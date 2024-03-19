# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import abc
import contextlib
from enum import Enum
import io
import itertools

from absl import logging
from absl.testing import absltest
from absl.testing import parameterized
from google.protobuf import message
import riegeli
from riegeli.records.tests import records_test_pb2
import tensorflow as tf


def combine_named_parameters(*testcase_sets):
  """Allows a parameterized test with multiple independent parameters.

  Example:
    combine_named_parameters([('BytesIO', BytesIOSpec), ('FileIO', FileIOSpec)],
                             [('serial', 0), ('parallel', 10)])
      yields the same elements as
    [('BytesIO_serial', BytesIOSpec, 0), ('BytesIO_parallel', BytesIOSpec, 10),
     ('FileIO_serial', FileIOSpec, 0), ('FileIO_parallel', FileIOSpec, 10)]
  """
  for combination in itertools.product(*testcase_sets):
    key = '_'.join(name for name, _ in combination)
    values = [value for _, value in combination]
    yield tuple([key] + values)


class RandomAccess(Enum):
  RANDOM_ACCESS = 1
  SEQUENTIAL_ACCESS_DETECTED = 2
  SEQUENTIAL_ACCESS_EXPLICIT = 3


class FakeFile:
  __slots__ = ('_random_access',)

  def __init__(self, random_access):
    self._random_access = random_access

  def seekable(self):
    return self._random_access

  def tell(self):
    if self._random_access:
      return 0
    raise NotImplementedError('tell()')

  def __getattr__(self, name):
    raise NotImplementedError(f'{name}()')


class UnseekableWrapper:
  __slots__ = ('_wrapped',)

  def __init__(self, wrapped):
    self._wrapped = wrapped

  def seekable(self):
    return False

  def tell(self, *args):
    raise NotImplementedError('tell()')

  def seek(self, *args):
    raise NotImplementedError('seek()')

  def __getattr__(self, name):
    return getattr(self._wrapped, name)


class FileSpecBase(metaclass=abc.ABCMeta):
  __slots__ = ('_random_access', '_file')

  def __init__(self, create_tempfile, random_access):
    self._random_access = random_access
    self._file = None

  @abc.abstractmethod
  def _open_for_writing(self):
    raise NotImplementedError('_open_for_writing()')

  def writing_open(self):
    self._open_for_writing()
    logging.debug('Opened %r for writing', self._file)
    if self._random_access is RandomAccess.RANDOM_ACCESS:
      return self._file
    else:
      return UnseekableWrapper(self._file)

  @property
  def writing_should_close(self):
    return True

  @property
  def writing_assumed_pos(self):
    if self._random_access is RandomAccess.SEQUENTIAL_ACCESS_EXPLICIT:
      return 0
    return None

  @abc.abstractmethod
  def _open_for_reading(self):
    raise NotImplementedError('_open_for_reading()')

  def reading_open(self):
    self._open_for_reading()
    logging.debug('Opened %r for reading', self._file)
    if self._random_access is RandomAccess.RANDOM_ACCESS:
      return self._file
    else:
      return UnseekableWrapper(self._file)

  @property
  def reading_should_close(self):
    return True

  @property
  def reading_assumed_pos(self):
    if self._random_access is RandomAccess.SEQUENTIAL_ACCESS_EXPLICIT:
      return 0
    return None

  def close(self):
    pass


class BytesIOSpec(FileSpecBase):
  __slots__ = ()

  def _open_for_writing(self):
    self._file = io.BytesIO()

  @property
  def writing_should_close(self):
    # If BytesIO is closed, it loses bytes written.
    return False

  def _open_for_reading(self):
    if self._file is None:
      raise ValueError('file was not set')
    self._file.seek(0)


class LocalFileSpecBase(FileSpecBase):
  __slots__ = ('_filename',)

  def __init__(self, create_tempfile, random_access):
    super().__init__(create_tempfile, random_access)
    self._filename = create_tempfile().full_path


class FileIOSpec(LocalFileSpecBase):
  __slots__ = ()

  def _open_for_writing(self):
    self._file = io.FileIO(self._filename, mode='wb')

  def _open_for_reading(self):
    self._file = io.FileIO(self._filename, mode='rb')


class BufferedIOSpec(LocalFileSpecBase):
  __slots__ = ()

  def _open_for_writing(self):
    self._file = io.open(self._filename, mode='wb')

  def _open_for_reading(self):
    self._file = io.open(self._filename, mode='rb')


class BuiltinFileSpec(LocalFileSpecBase):
  __slots__ = ()

  def _open_for_writing(self):
    self._file = open(self._filename, mode='wb')

  def _open_for_reading(self):
    self._file = open(self._filename, mode='rb')


class TensorFlowGFileSpec(LocalFileSpecBase):
  __slots__ = ()

  def _open_for_writing(self):
    self._file = tf.io.gfile.GFile(self._filename, mode='wb')

  def _open_for_reading(self):
    self._file = tf.io.gfile.GFile(self._filename, mode='rb')


def sample_string(i, size):
  piece = f'{i} '.encode()
  result = piece * -(-size // len(piece))  # len(result) >= size
  return result[:size]


def sample_message(i, size):
  return records_test_pb2.SimpleMessage(id=i, payload=sample_string(i, size))


def sample_message_id_only(i):
  return records_test_pb2.SimpleMessage(id=i)


def sample_invalid_message(size):
  return b'\xff' * size  # An unfinished varint.


def record_writer_options(parallelism, transpose=False, chunk_size=35000):
  return (
      f'{"transpose," if transpose else ""}uncompressed,'
      f'chunk_size:{chunk_size},parallelism:{parallelism}'
  )


# pyformat: disable
_FILE_SPEC_VALUES = (('BytesIO', BytesIOSpec),
                     ('FileIO', FileIOSpec),
                     ('BufferedIO', BufferedIOSpec),
                     ('BuiltinFile', BuiltinFileSpec),
                     ('TensorFlowGFile', TensorFlowGFileSpec))

_RANDOM_ACCESS_VALUES = (
    ('randomAccess', RandomAccess.RANDOM_ACCESS),
    ('sequentialAccessDetected', RandomAccess.SEQUENTIAL_ACCESS_DETECTED),
    ('sequentialAccessExplicit', RandomAccess.SEQUENTIAL_ACCESS_EXPLICIT))

_PARALLELISM_VALUES = (('serial', 0),
                       ('parallel', 10))
# pyformat: enable

_PARAMETERIZE_BY_FILE_SPEC = parameterized.named_parameters(*_FILE_SPEC_VALUES)

_PARAMETERIZE_BY_RANDOM_ACCESS = parameterized.named_parameters(
    *_RANDOM_ACCESS_VALUES
)

_PARAMETERIZE_BY_RANDOM_ACCESS_AND_PARALLELISM = parameterized.named_parameters(
    combine_named_parameters(_RANDOM_ACCESS_VALUES, _PARALLELISM_VALUES)
)

_PARAMETERIZE_BY_FILE_SPEC_AND_PARALLELISM = parameterized.named_parameters(
    combine_named_parameters(_FILE_SPEC_VALUES, _PARALLELISM_VALUES)
)

_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM = (
    parameterized.named_parameters(
        combine_named_parameters(
            _FILE_SPEC_VALUES, _RANDOM_ACCESS_VALUES, _PARALLELISM_VALUES
        )
    )
)


class RecordsTest(parameterized.TestCase):

  def corrupt_at(self, files, index):
    byte_reader = files.reading_open()
    contents1 = byte_reader.read(index)
    contents2 = byte_reader.read(1)
    contents2 = bytes([(contents2[0] + 1) % 256])
    contents3 = byte_reader.read()
    if files.reading_should_close:
      byte_reader.close()
    byte_writer = files.writing_open()
    byte_writer.write(contents1)
    byte_writer.write(contents2)
    byte_writer.write(contents3)
    if files.writing_should_close:
      byte_writer.close()

  @_PARAMETERIZE_BY_RANDOM_ACCESS_AND_PARALLELISM
  def test_record_writer_exception_from_file(self, random_access, parallelism):
    byte_writer = FakeFile(random_access is RandomAccess.RANDOM_ACCESS)
    with self.assertRaises(NotImplementedError):
      with riegeli.RecordWriter(
          byte_writer,
          assumed_pos=(
              0
              if random_access is RandomAccess.SEQUENTIAL_ACCESS_EXPLICIT
              else None
          ),
          options=record_writer_options(parallelism),
      ) as writer:
        writer.write_record(sample_string(0, 10000))

  @_PARAMETERIZE_BY_RANDOM_ACCESS
  def test_record_reader_exception_from_file(self, random_access):
    byte_reader = FakeFile(random_access is RandomAccess.RANDOM_ACCESS)
    with self.assertRaises(NotImplementedError):
      with riegeli.RecordReader(
          byte_reader,
          owns_src=False,
          assumed_pos=(
              0
              if random_access is RandomAccess.SEQUENTIAL_ACCESS_EXPLICIT
              else None
          ),
      ) as reader:
        reader.read_record()

  @_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM
  def test_write_read_record(self, file_spec, random_access, parallelism):
    with contextlib.closing(
        file_spec(self.create_tempfile, random_access)
    ) as files:
      positions = []
      with riegeli.RecordWriter(
          files.writing_open(),
          owns_dest=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism),
      ) as writer:
        for i in range(23):
          pos = writer.pos
          writer.write_record(sample_string(i, 10000))
          canonical_pos = writer.last_pos
          if positions:
            self.assertGreater(pos, positions[-1])
          self.assertLessEqual(pos, canonical_pos)
          positions.append(canonical_pos)
        writer.close()
        end_pos = writer.pos
        self.assertEqual(writer.last_pos, positions[-1])
      with riegeli.RecordReader(
          files.reading_open(),
          owns_src=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos,
      ) as reader:
        for i in range(23):
          pos = reader.pos
          self.assertEqual(reader.read_record(), sample_string(i, 10000))
          canonical_pos = reader.last_pos
          self.assertEqual(canonical_pos, positions[i])
          self.assertLessEqual(pos, canonical_pos)
        self.assertIsNone(reader.read_record())
        self.assertEqual(reader.pos, end_pos)
        reader.close()
        self.assertEqual(reader.pos, end_pos)

  @_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM
  def test_write_read_message(self, file_spec, random_access, parallelism):
    with contextlib.closing(
        file_spec(self.create_tempfile, random_access)
    ) as files:
      positions = []
      with riegeli.RecordWriter(
          files.writing_open(),
          owns_dest=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism),
      ) as writer:
        for i in range(23):
          pos = writer.pos
          writer.write_message(sample_message(i, 10000))
          canonical_pos = writer.last_pos
          if positions:
            self.assertGreater(pos, positions[-1])
          self.assertLessEqual(pos, canonical_pos)
          positions.append(canonical_pos)
        writer.close()
        end_pos = writer.pos
        self.assertEqual(writer.last_pos, positions[-1])
      with riegeli.RecordReader(
          files.reading_open(),
          owns_src=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos,
      ) as reader:
        for i in range(23):
          pos = reader.pos
          self.assertEqual(
              reader.read_message(records_test_pb2.SimpleMessage),
              sample_message(i, 10000),
          )
          canonical_pos = reader.last_pos
          self.assertEqual(canonical_pos, positions[i])
          self.assertLessEqual(pos, canonical_pos)
        self.assertIsNone(reader.read_message(records_test_pb2.SimpleMessage))
        self.assertEqual(reader.pos, end_pos)
        reader.close()
        self.assertEqual(reader.pos, end_pos)

  @_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM
  def test_write_read_records(self, file_spec, random_access, parallelism):
    with contextlib.closing(
        file_spec(self.create_tempfile, random_access)
    ) as files:
      with riegeli.RecordWriter(
          files.writing_open(),
          owns_dest=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism),
      ) as writer:
        writer.write_records(sample_string(i, 10000) for i in range(23))
      with riegeli.RecordReader(
          files.reading_open(),
          owns_src=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos,
      ) as reader:
        self.assertEqual(
            list(reader.read_records()),
            [sample_string(i, 10000) for i in range(23)],
        )

  @_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM
  def test_write_read_messages(self, file_spec, random_access, parallelism):
    with contextlib.closing(
        file_spec(self.create_tempfile, random_access)
    ) as files:
      with riegeli.RecordWriter(
          files.writing_open(),
          owns_dest=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism),
      ) as writer:
        writer.write_messages(sample_message(i, 10000) for i in range(23))
      with riegeli.RecordReader(
          files.reading_open(),
          owns_src=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos,
      ) as reader:
        self.assertEqual(
            list(reader.read_messages(records_test_pb2.SimpleMessage)),
            [sample_message(i, 10000) for i in range(23)],
        )

  @_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM
  def test_write_read_messages_with_field_projection(
      self, file_spec, random_access, parallelism
  ):
    with contextlib.closing(
        file_spec(self.create_tempfile, random_access)
    ) as files:
      with riegeli.RecordWriter(
          files.writing_open(),
          owns_dest=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism, transpose=True),
      ) as writer:
        writer.write_messages(sample_message(i, 10000) for i in range(23))
      with riegeli.RecordReader(
          files.reading_open(),
          owns_src=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos,
          field_projection=[[
              records_test_pb2.SimpleMessage.DESCRIPTOR.fields_by_name[
                  'id'
              ].number
          ]],
      ) as reader:
        self.assertEqual(
            list(reader.read_messages(records_test_pb2.SimpleMessage)),
            [sample_message_id_only(i) for i in range(23)],
        )

  @_PARAMETERIZE_BY_FILE_SPEC_AND_PARALLELISM
  def test_write_read_messages_with_field_projection_later(
      self, file_spec, parallelism
  ):
    with contextlib.closing(
        file_spec(
            self.create_tempfile, random_access=RandomAccess.RANDOM_ACCESS
        )
    ) as files:
      with riegeli.RecordWriter(
          files.writing_open(),
          owns_dest=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism, transpose=True),
      ) as writer:
        writer.write_messages(sample_message(i, 10000) for i in range(23))
      with riegeli.RecordReader(
          files.reading_open(),
          owns_src=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos,
      ) as reader:
        for i in range(4):
          self.assertEqual(
              reader.read_message(records_test_pb2.SimpleMessage),
              sample_message(i, 10000),
          )
        reader.set_field_projection([[
            records_test_pb2.SimpleMessage.DESCRIPTOR.fields_by_name[
                'id'
            ].number
        ]])
        for i in range(4, 14):
          self.assertEqual(
              reader.read_message(records_test_pb2.SimpleMessage),
              sample_message_id_only(i),
          )
        reader.set_field_projection(None)
        for i in range(14, 23):
          self.assertEqual(
              reader.read_message(records_test_pb2.SimpleMessage),
              sample_message(i, 10000),
          )
        self.assertIsNone(reader.read_message(records_test_pb2.SimpleMessage))

  @_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM
  def test_write_read_metadata(self, file_spec, random_access, parallelism):
    with contextlib.closing(
        file_spec(self.create_tempfile, random_access)
    ) as files:
      metadata_written = riegeli.RecordsMetadata()
      metadata_written.file_comment = 'Comment'
      riegeli.set_record_type(metadata_written, records_test_pb2.SimpleMessage)
      message_written = sample_message(7, 10)
      with riegeli.RecordWriter(
          files.writing_open(),
          owns_dest=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism),
          metadata=metadata_written,
      ) as writer:
        writer.write_message(message_written)
      with riegeli.RecordReader(
          files.reading_open(),
          owns_src=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos,
      ) as reader:
        metadata_read = reader.read_metadata()
        self.assertEqual(metadata_read, metadata_written)
        record_type = riegeli.get_record_type(metadata_read)
        assert record_type is not None
        self.assertEqual(
            record_type.DESCRIPTOR.full_name, 'riegeli.tests.SimpleMessage'
        )
        message_read = reader.read_message(record_type)
        assert message_read is not None
        # Serialize and deserialize because messages have descriptors of
        # different origins.
        self.assertEqual(
            records_test_pb2.SimpleMessage.FromString(
                message_read.SerializeToString()
            ),
            message_written,
        )

  @_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM
  def test_invalid_metadata_exception(
      self, file_spec, random_access, parallelism
  ):
    with contextlib.closing(
        file_spec(self.create_tempfile, random_access)
    ) as files:
      with riegeli.RecordWriter(
          files.writing_open(),
          owns_dest=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism),
          serialized_metadata=sample_invalid_message(100),
      ):
        pass
      with riegeli.RecordReader(
          files.reading_open(),
          owns_src=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos,
      ) as reader:
        with self.assertRaises(message.DecodeError):
          reader.read_metadata()

  @_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM
  def test_invalid_metadata_recovery(
      self, file_spec, random_access, parallelism
  ):
    with contextlib.closing(
        file_spec(self.create_tempfile, random_access)
    ) as files:
      with riegeli.RecordWriter(
          files.writing_open(),
          owns_dest=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism),
          serialized_metadata=sample_invalid_message(100),
      ):
        pass

      def recovery(skipped_region):
        pass

      with riegeli.RecordReader(
          files.reading_open(),
          owns_src=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos,
          recovery=recovery,
      ) as reader:
        self.assertEqual(reader.read_metadata(), riegeli.RecordsMetadata())

  @_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM
  def test_invalid_metadata_recovery_stop_iteration(
      self, file_spec, random_access, parallelism
  ):
    with contextlib.closing(
        file_spec(self.create_tempfile, random_access)
    ) as files:
      with riegeli.RecordWriter(
          files.writing_open(),
          owns_dest=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism),
          serialized_metadata=sample_invalid_message(100),
      ):
        pass

      def recovery(skipped_region):
        raise StopIteration

      with riegeli.RecordReader(
          files.reading_open(),
          owns_src=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos,
          recovery=recovery,
      ) as reader:
        self.assertIsNone(reader.read_metadata())

  @_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM
  def test_field_projection(self, file_spec, random_access, parallelism):
    with contextlib.closing(
        file_spec(self.create_tempfile, random_access)
    ) as files:
      with riegeli.RecordWriter(
          files.writing_open(),
          owns_dest=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=f'{record_writer_options(parallelism)},transpose',
      ) as writer:
        for i in range(23):
          writer.write_message(sample_message(i, 10000))
      with riegeli.RecordReader(
          files.reading_open(),
          owns_src=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos,
          field_projection=[[
              records_test_pb2.SimpleMessage.DESCRIPTOR.fields_by_name[
                  'id'
              ].number
          ]],
      ) as reader:
        for i in range(23):
          self.assertEqual(
              reader.read_message(records_test_pb2.SimpleMessage),
              records_test_pb2.SimpleMessage(id=i),
          )
        self.assertIsNone(reader.read_message(records_test_pb2.SimpleMessage))

  @_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM
  def test_field_projection_existence_only(
      self, file_spec, random_access, parallelism
  ):
    with contextlib.closing(
        file_spec(self.create_tempfile, random_access)
    ) as files:
      with riegeli.RecordWriter(
          files.writing_open(),
          owns_dest=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=f'{record_writer_options(parallelism)},transpose',
      ) as writer:
        for i in range(23):
          writer.write_message(sample_message(i, 10000))
      with riegeli.RecordReader(
          files.reading_open(),
          owns_src=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos,
          field_projection=[
              [
                  records_test_pb2.SimpleMessage.DESCRIPTOR.fields_by_name[
                      'id'
                  ].number
              ],
              [
                  records_test_pb2.SimpleMessage.DESCRIPTOR.fields_by_name[
                      'payload'
                  ].number,
                  riegeli.EXISTENCE_ONLY,
              ],
          ],
      ) as reader:
        for i in range(23):
          self.assertEqual(
              reader.read_message(records_test_pb2.SimpleMessage),
              records_test_pb2.SimpleMessage(id=i, payload=b''),
          )
        self.assertIsNone(reader.read_message(records_test_pb2.SimpleMessage))

  @_PARAMETERIZE_BY_FILE_SPEC_AND_PARALLELISM
  def test_seek(self, file_spec, parallelism):
    with contextlib.closing(
        file_spec(
            self.create_tempfile, random_access=RandomAccess.RANDOM_ACCESS
        )
    ) as files:
      positions = []
      with riegeli.RecordWriter(
          files.writing_open(),
          owns_dest=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism),
      ) as writer:
        for i in range(23):
          pos = writer.pos
          writer.write_record(sample_string(i, 10000))
          canonical_pos = writer.last_pos
          if positions:
            self.assertGreater(pos, positions[-1])
          self.assertLessEqual(pos, canonical_pos)
          positions.append(canonical_pos)
        writer.close()
        end_pos = writer.pos
      with riegeli.RecordReader(
          files.reading_open(),
          owns_src=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos,
      ) as reader:
        reader.seek(positions[9])
        self.assertGreater(reader.pos, positions[8])
        self.assertLessEqual(reader.pos, positions[9])
        reader.seek(positions[9])
        self.assertGreater(reader.pos, positions[8])
        self.assertLessEqual(reader.pos, positions[9])
        reader.seek(positions[11])
        self.assertGreater(reader.pos, positions[10])
        self.assertLessEqual(reader.pos, positions[11])
        self.assertEqual(reader.read_record(), sample_string(11, 10000))
        reader.seek(positions[9])
        self.assertGreater(reader.pos, positions[8])
        self.assertLessEqual(reader.pos, positions[9])
        self.assertEqual(reader.read_record(), sample_string(9, 10000))
        reader.seek(positions[11])
        self.assertGreater(reader.pos, positions[10])
        self.assertLessEqual(reader.pos, positions[11])
        self.assertEqual(reader.read_record(), sample_string(11, 10000))
        reader.seek(positions[13])
        self.assertGreater(reader.pos, positions[12])
        self.assertLessEqual(reader.pos, positions[13])
        self.assertEqual(reader.read_record(), sample_string(13, 10000))
        reader.seek(riegeli.RecordPosition(0, 0))
        self.assertLessEqual(reader.pos, positions[0])
        self.assertEqual(reader.read_record(), sample_string(0, 10000))
        reader.seek(end_pos)
        self.assertLessEqual(reader.pos, end_pos)
        self.assertIsNone(reader.read_record())
        reader.seek(positions[11])
        self.assertGreater(reader.pos, positions[10])
        self.assertLessEqual(reader.pos, positions[11])
        reader.close()
        self.assertGreater(reader.pos, positions[10])
        self.assertLessEqual(reader.pos, positions[11])

  @_PARAMETERIZE_BY_FILE_SPEC_AND_PARALLELISM
  def test_seek_numeric(self, file_spec, parallelism):
    with contextlib.closing(
        file_spec(
            self.create_tempfile, random_access=RandomAccess.RANDOM_ACCESS
        )
    ) as files:
      positions = []
      with riegeli.RecordWriter(
          files.writing_open(),
          owns_dest=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism),
      ) as writer:
        for i in range(23):
          pos = writer.pos
          writer.write_record(sample_string(i, 10000))
          canonical_pos = writer.last_pos
          if positions:
            self.assertGreater(pos, positions[-1])
          self.assertLessEqual(pos, canonical_pos)
          positions.append(canonical_pos)
        writer.close()
        end_pos = writer.pos
      with riegeli.RecordReader(
          files.reading_open(),
          owns_src=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos,
      ) as reader:
        reader.seek_numeric(positions[9].numeric)
        self.assertGreater(reader.pos, positions[8])
        self.assertLessEqual(reader.pos, positions[9])
        reader.seek_numeric(positions[9].numeric)
        self.assertGreater(reader.pos, positions[8])
        self.assertLessEqual(reader.pos, positions[9])
        reader.seek_numeric(positions[11].numeric)
        self.assertGreater(reader.pos, positions[10])
        self.assertLessEqual(reader.pos, positions[11])
        self.assertEqual(reader.read_record(), sample_string(11, 10000))
        reader.seek_numeric(positions[9].numeric)
        self.assertGreater(reader.pos, positions[8])
        self.assertLessEqual(reader.pos, positions[9])
        self.assertEqual(reader.read_record(), sample_string(9, 10000))
        reader.seek_numeric(positions[11].numeric)
        self.assertGreater(reader.pos, positions[10])
        self.assertLessEqual(reader.pos, positions[11])
        self.assertEqual(reader.read_record(), sample_string(11, 10000))
        reader.seek_numeric(positions[13].numeric)
        self.assertGreater(reader.pos, positions[12])
        self.assertLessEqual(reader.pos, positions[13])
        self.assertEqual(reader.read_record(), sample_string(13, 10000))
        reader.seek_numeric(0)
        self.assertLessEqual(reader.pos, positions[0])
        self.assertEqual(reader.read_record(), sample_string(0, 10000))
        reader.seek_numeric(end_pos.numeric)
        self.assertLessEqual(reader.pos, end_pos)
        self.assertIsNone(reader.read_record())
        reader.seek_numeric(positions[11].numeric)
        self.assertGreater(reader.pos, positions[10])
        self.assertLessEqual(reader.pos, positions[11])
        reader.close()
        self.assertGreater(reader.pos, positions[10])
        self.assertLessEqual(reader.pos, positions[11])

  @_PARAMETERIZE_BY_FILE_SPEC
  def test_seek_back(self, file_spec):
    with contextlib.closing(
        file_spec(
            self.create_tempfile, random_access=RandomAccess.RANDOM_ACCESS
        )
    ) as files:
      with riegeli.RecordWriter(
          files.writing_open(),
          owns_dest=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism=0),
      ) as writer:
        for i in range(23):
          writer.write_record(sample_string(i, 10000))
      with riegeli.RecordReader(
          files.reading_open(),
          owns_src=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos,
      ) as reader:
        reader.seek_numeric(reader.size())
        for i in reversed(range(23)):
          self.assertTrue(reader.seek_back())
          self.assertEqual(reader.read_record(), sample_string(i, 10000))
          self.assertTrue(reader.seek_back())
        self.assertFalse(reader.seek_back())

  @_PARAMETERIZE_BY_FILE_SPEC
  def test_search(self, file_spec):
    with contextlib.closing(
        file_spec(
            self.create_tempfile, random_access=RandomAccess.RANDOM_ACCESS
        )
    ) as files:
      with riegeli.RecordWriter(
          files.writing_open(),
          owns_dest=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism=0),
      ) as writer:
        positions = []
        for i in range(23):
          writer.write_message(sample_message(i, 10000))
          positions.append(writer.last_pos)
        writer.close()
        end_pos = writer.pos
      with riegeli.RecordReader(
          files.reading_open(),
          owns_src=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos,
      ) as reader:

        def test_function(search_target):
          def test(record_reader):
            msg = record_reader.read_message(records_test_pb2.SimpleMessage)
            return (msg.id > search_target) - (msg.id < search_target)

          return test

        self.assertEqual(reader.search(test_function(7)), 0)
        self.assertEqual(reader.pos, positions[7])
        self.assertEqual(reader.search(test_function(0)), 0)
        self.assertEqual(reader.pos, positions[0])
        self.assertEqual(reader.search(test_function(22)), 0)
        self.assertEqual(reader.pos, positions[22])
        self.assertEqual(reader.search(test_function(23)), -1)
        self.assertEqual(reader.pos, end_pos)

  @_PARAMETERIZE_BY_FILE_SPEC
  def test_search_for_record(self, file_spec):
    with contextlib.closing(
        file_spec(
            self.create_tempfile, random_access=RandomAccess.RANDOM_ACCESS
        )
    ) as files:
      with riegeli.RecordWriter(
          files.writing_open(),
          owns_dest=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism=0),
      ) as writer:
        positions = []
        for i in range(23):
          writer.write_message(sample_message(i, 10000))
          positions.append(writer.last_pos)
        writer.close()
        end_pos = writer.pos
      with riegeli.RecordReader(
          files.reading_open(),
          owns_src=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos,
      ) as reader:

        def test_function(search_target):
          def test(record):
            msg = records_test_pb2.SimpleMessage.FromString(record)
            return (msg.id > search_target) - (msg.id < search_target)

          return test

        self.assertEqual(reader.search_for_record(test_function(7)), 0)
        self.assertEqual(reader.pos, positions[7])
        self.assertEqual(reader.search_for_record(test_function(0)), 0)
        self.assertEqual(reader.pos, positions[0])
        self.assertEqual(reader.search_for_record(test_function(22)), 0)
        self.assertEqual(reader.pos, positions[22])
        self.assertEqual(reader.search_for_record(test_function(23)), -1)
        self.assertEqual(reader.pos, end_pos)

  @_PARAMETERIZE_BY_FILE_SPEC
  def test_search_for_record_stop_iteration(self, file_spec):
    with contextlib.closing(
        file_spec(
            self.create_tempfile, random_access=RandomAccess.RANDOM_ACCESS
        )
    ) as files:
      with riegeli.RecordWriter(
          files.writing_open(),
          owns_dest=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism=0),
      ) as writer:
        for i in range(23):
          writer.write_message(sample_message(i, 10000))
      with riegeli.RecordReader(
          files.reading_open(),
          owns_src=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos,
      ) as reader:

        def test(record):
          raise StopIteration

        self.assertIsNone(reader.search_for_record(test))

  @_PARAMETERIZE_BY_FILE_SPEC
  def test_search_for_message(self, file_spec):
    with contextlib.closing(
        file_spec(
            self.create_tempfile, random_access=RandomAccess.RANDOM_ACCESS
        )
    ) as files:
      with riegeli.RecordWriter(
          files.writing_open(),
          owns_dest=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism=0),
      ) as writer:
        positions = []
        for i in range(23):
          writer.write_message(sample_message(i, 10000))
          positions.append(writer.last_pos)
        writer.close()
        end_pos = writer.pos
      with riegeli.RecordReader(
          files.reading_open(),
          owns_src=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos,
      ) as reader:

        def test_function(search_target):
          def test(msg):
            return (msg.id > search_target) - (msg.id < search_target)

          return test

        self.assertEqual(
            reader.search_for_message(
                records_test_pb2.SimpleMessage, test_function(7)
            ),
            0,
        )
        self.assertEqual(reader.pos, positions[7])
        self.assertEqual(
            reader.search_for_message(
                records_test_pb2.SimpleMessage, test_function(0)
            ),
            0,
        )
        self.assertEqual(reader.pos, positions[0])
        self.assertEqual(
            reader.search_for_message(
                records_test_pb2.SimpleMessage, test_function(22)
            ),
            0,
        )
        self.assertEqual(reader.pos, positions[22])
        self.assertEqual(
            reader.search_for_message(
                records_test_pb2.SimpleMessage, test_function(23)
            ),
            -1,
        )
        self.assertEqual(reader.pos, end_pos)

  @_PARAMETERIZE_BY_FILE_SPEC
  def test_search_for_message_stop_iteration(self, file_spec):
    with contextlib.closing(
        file_spec(
            self.create_tempfile, random_access=RandomAccess.RANDOM_ACCESS
        )
    ) as files:
      with riegeli.RecordWriter(
          files.writing_open(),
          owns_dest=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism=0),
      ) as writer:
        for i in range(23):
          writer.write_message(sample_message(i, 10000))
      with riegeli.RecordReader(
          files.reading_open(),
          owns_src=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos,
      ) as reader:

        def test(record):
          raise StopIteration

        self.assertIsNone(
            reader.search_for_message(records_test_pb2.SimpleMessage, test)
        )

  @_PARAMETERIZE_BY_FILE_SPEC
  def test_search_for_invalid_message_exception(self, file_spec):
    with contextlib.closing(
        file_spec(
            self.create_tempfile, random_access=RandomAccess.RANDOM_ACCESS
        )
    ) as files:
      # Write 1 valid message, 1 invalid message, and 1 valid message, each in a
      # separate chunk.
      with riegeli.RecordWriter(
          files.writing_open(),
          owns_dest=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism=0, chunk_size=15000),
      ) as writer:
        writer.write_message(sample_message(0, 10000))
        writer.write_record(sample_invalid_message(10000))
        writer.write_message(sample_message(2, 10000))
      with riegeli.RecordReader(
          files.reading_open(),
          owns_src=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos,
      ) as reader:

        def test_function(search_target):
          def test(msg):
            return (msg.id > search_target) - (msg.id < search_target)

          return test

        with self.assertRaises(message.DecodeError):
          reader.search_for_message(
              records_test_pb2.SimpleMessage, test_function(1)
          )

  @_PARAMETERIZE_BY_FILE_SPEC
  def test_search_for_invalid_message_recovery(self, file_spec):
    with contextlib.closing(
        file_spec(
            self.create_tempfile, random_access=RandomAccess.RANDOM_ACCESS
        )
    ) as files:
      # Write 1 valid message, 1 invalid message, and 1 valid message, each in a
      # separate chunk.
      with riegeli.RecordWriter(
          files.writing_open(),
          owns_dest=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism=0, chunk_size=15000),
      ) as writer:
        positions = []
        writer.write_message(sample_message(0, 10000))
        positions.append(writer.last_pos)
        writer.write_record(sample_invalid_message(10000))
        positions.append(writer.last_pos)
        writer.write_message(sample_message(2, 10000))
        positions.append(writer.last_pos)
        writer.close()

      def recovery(skipped_region):
        pass

      with riegeli.RecordReader(
          files.reading_open(),
          owns_src=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos,
          recovery=recovery,
      ) as reader:

        def test_function(search_target):
          def test(msg):
            return (msg.id > search_target) - (msg.id < search_target)

          return test

        self.assertEqual(
            reader.search_for_message(
                records_test_pb2.SimpleMessage, test_function(1)
            ),
            1,
        )
        self.assertEqual(reader.pos, positions[2])

  @_PARAMETERIZE_BY_FILE_SPEC
  def test_search_for_invalid_message_recovery_stop_iteration(self, file_spec):
    with contextlib.closing(
        file_spec(
            self.create_tempfile, random_access=RandomAccess.RANDOM_ACCESS
        )
    ) as files:
      # Write 1 valid message, 1 invalid message, and 1 valid message, each in a
      # separate chunk.
      with riegeli.RecordWriter(
          files.writing_open(),
          owns_dest=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism=0, chunk_size=15000),
      ) as writer:
        positions = []
        writer.write_message(sample_message(0, 10000))
        positions.append(writer.last_pos)
        writer.write_record(sample_invalid_message(10000))
        positions.append(writer.last_pos)
        writer.write_message(sample_message(2, 10000))
        positions.append(writer.last_pos)
        writer.close()

      def recovery(skipped_region):
        raise StopIteration

      with riegeli.RecordReader(
          files.reading_open(),
          owns_src=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos,
          recovery=recovery,
      ) as reader:

        def test_function(search_target):
          def test(msg):
            return (msg.id > search_target) - (msg.id < search_target)

          return test

        self.assertIsNone(
            reader.search_for_message(
                records_test_pb2.SimpleMessage, test_function(1)
            )
        )

  @_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM
  def test_corruption_exception(self, file_spec, random_access, parallelism):
    with contextlib.closing(
        file_spec(self.create_tempfile, random_access)
    ) as files:
      positions = []
      with riegeli.RecordWriter(
          files.writing_open(),
          owns_dest=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism),
      ) as writer:
        for i in range(23):
          writer.write_record(sample_string(i, 10000))
          positions.append(writer.last_pos)
      # Corrupt the header of the chunk containing records [9..12).
      self.corrupt_at(files, positions[9].chunk_begin + 20)
      # Read records [0..9) successfully (all before the corrupted chunk).
      reader = riegeli.RecordReader(
          files.reading_open(),
          owns_src=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos,
      )
      for i in range(9):
        self.assertEqual(reader.read_record(), sample_string(i, 10000))
      with self.assertRaises(riegeli.RiegeliError):
        reader.read_record()
      with self.assertRaises(riegeli.RiegeliError):
        reader.close()

  @_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM
  def test_corruption_recovery(self, file_spec, random_access, parallelism):
    with contextlib.closing(
        file_spec(self.create_tempfile, random_access)
    ) as files:
      positions = []
      with riegeli.RecordWriter(
          files.writing_open(),
          owns_dest=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism),
      ) as writer:
        for i in range(23):
          writer.write_record(sample_string(i, 10000))
          positions.append(writer.last_pos)
      # Corrupt the header of the chunk containing records [9..12).
      self.corrupt_at(files, positions[9].chunk_begin + 20)
      # Read records [0..9) and [15..23) successfully (all except the corrupted
      # chunk and the next chunk which intersects the same block).
      skipped_regions = []
      with riegeli.RecordReader(
          files.reading_open(),
          owns_src=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos,
          recovery=skipped_regions.append,
      ) as reader:
        for i in range(9):
          self.assertEqual(reader.read_record(), sample_string(i, 10000))
        for i in range(15, 23):
          self.assertEqual(reader.read_record(), sample_string(i, 10000))
        self.assertIsNone(reader.read_record())
      self.assertLen(skipped_regions, 1)
      skipped_region = skipped_regions[0]
      self.assertEqual(skipped_region.begin, positions[9].numeric)
      self.assertEqual(skipped_region.end, positions[15].numeric)

  @_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM
  def test_corruption_recovery_stop_iteration(
      self, file_spec, random_access, parallelism
  ):
    with contextlib.closing(
        file_spec(self.create_tempfile, random_access)
    ) as files:
      positions = []
      with riegeli.RecordWriter(
          files.writing_open(),
          owns_dest=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism),
      ) as writer:
        for i in range(23):
          writer.write_record(sample_string(i, 10000))
          positions.append(writer.last_pos)
      # Corrupt the header of the chunk containing records [9..12).
      self.corrupt_at(files, positions[9].chunk_begin + 20)
      # Read records [0..9) successfully (all before the corrupted chunk).
      skipped_regions = []

      def recovery(skipped_region):
        skipped_regions.append(skipped_region)
        raise StopIteration

      with riegeli.RecordReader(
          files.reading_open(),
          owns_src=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos,
          recovery=recovery,
      ) as reader:
        for i in range(9):
          self.assertEqual(reader.read_record(), sample_string(i, 10000))
        self.assertIsNone(reader.read_record())
      self.assertLen(skipped_regions, 1)
      skipped_region = skipped_regions[0]
      self.assertEqual(skipped_region.begin, positions[9].numeric)
      self.assertEqual(skipped_region.end, positions[15].numeric)

  @_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM
  def test_corruption_recovery_exception(
      self, file_spec, random_access, parallelism
  ):
    with contextlib.closing(
        file_spec(self.create_tempfile, random_access)
    ) as files:
      positions = []
      with riegeli.RecordWriter(
          files.writing_open(),
          owns_dest=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism),
      ) as writer:
        for i in range(23):
          writer.write_record(sample_string(i, 10000))
          positions.append(writer.last_pos)
      # Corrupt the header of the chunk containing records [9..12).
      self.corrupt_at(files, positions[9].chunk_begin + 20)

      # Propagate exception from the recovery function
      def recovery(skipped_region):
        raise KeyboardInterrupt

      with riegeli.RecordReader(
          files.reading_open(),
          owns_src=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos,
          recovery=recovery,
      ) as reader:
        for i in range(9):
          self.assertEqual(reader.read_record(), sample_string(i, 10000))
        with self.assertRaises(KeyboardInterrupt):
          reader.read_record()

  @_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM
  def test_invalid_message_exception(
      self, file_spec, random_access, parallelism
  ):
    with contextlib.closing(
        file_spec(self.create_tempfile, random_access)
    ) as files:
      with riegeli.RecordWriter(
          files.writing_open(),
          owns_dest=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism),
      ) as writer:
        for i in range(9):
          writer.write_message(sample_message(i, 10000))
        for i in range(9, 10):
          writer.write_record(sample_invalid_message(10000))
        for i in range(10, 14):
          writer.write_message(sample_message(i, 10000))
        for i in range(14, 15):
          writer.write_record(sample_invalid_message(10000))
        for i in range(15, 23):
          writer.write_message(sample_message(i, 10000))
      # Read messages [0..9), [10..14), and [15, 23) successfully (all except
      # invalid messages), raising exceptions for invalid messages
      with riegeli.RecordReader(
          files.reading_open(),
          owns_src=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos,
      ) as reader:
        for i in range(9):
          self.assertEqual(
              reader.read_message(records_test_pb2.SimpleMessage),
              sample_message(i, 10000),
          )
        with self.assertRaises(message.DecodeError):
          reader.read_message(records_test_pb2.SimpleMessage)
        for i in range(10, 14):
          self.assertEqual(
              reader.read_message(records_test_pb2.SimpleMessage),
              sample_message(i, 10000),
          )
        with self.assertRaises(message.DecodeError):
          reader.read_message(records_test_pb2.SimpleMessage)
        for i in range(15, 23):
          self.assertEqual(
              reader.read_message(records_test_pb2.SimpleMessage),
              sample_message(i, 10000),
          )

  @_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM
  def test_invalid_message_recovery(
      self, file_spec, random_access, parallelism
  ):
    with contextlib.closing(
        file_spec(self.create_tempfile, random_access)
    ) as files:
      positions = []
      with riegeli.RecordWriter(
          files.writing_open(),
          owns_dest=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism),
      ) as writer:
        for i in range(9):
          writer.write_message(sample_message(i, 10000))
          positions.append(writer.last_pos)
        for i in range(9, 10):
          writer.write_record(sample_invalid_message(10000))
          positions.append(writer.last_pos)
        for i in range(10, 14):
          writer.write_message(sample_message(i, 10000))
          positions.append(writer.last_pos)
        for i in range(14, 15):
          writer.write_record(sample_invalid_message(10000))
          positions.append(writer.last_pos)
        for i in range(15, 23):
          writer.write_message(sample_message(i, 10000))
          positions.append(writer.last_pos)
      # Read messages [0..9), [10..14), and [15, 23) successfully (all except
      # invalid messages).
      skipped_regions = []
      with riegeli.RecordReader(
          files.reading_open(),
          owns_src=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos,
          recovery=skipped_regions.append,
      ) as reader:
        for i in range(9):
          self.assertEqual(
              reader.read_message(records_test_pb2.SimpleMessage),
              sample_message(i, 10000),
          )
        for i in range(10, 14):
          self.assertEqual(
              reader.read_message(records_test_pb2.SimpleMessage),
              sample_message(i, 10000),
          )
        for i in range(15, 23):
          self.assertEqual(
              reader.read_message(records_test_pb2.SimpleMessage),
              sample_message(i, 10000),
          )
        self.assertIsNone(reader.read_message(records_test_pb2.SimpleMessage))
      self.assertLen(skipped_regions, 2)
      skipped_region = skipped_regions[0]
      self.assertEqual(skipped_region.begin, positions[9].numeric)
      self.assertEqual(skipped_region.end, positions[10].numeric)
      skipped_region = skipped_regions[1]
      self.assertEqual(skipped_region.begin, positions[14].numeric)
      self.assertEqual(skipped_region.end, positions[15].numeric)

  @_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM
  def test_invalid_message_recovery_stop_iteration(
      self, file_spec, random_access, parallelism
  ):
    with contextlib.closing(
        file_spec(self.create_tempfile, random_access)
    ) as files:
      positions = []
      with riegeli.RecordWriter(
          files.writing_open(),
          owns_dest=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism),
      ) as writer:
        for i in range(9):
          writer.write_message(sample_message(i, 10000))
          positions.append(writer.last_pos)
        for i in range(9, 10):
          writer.write_record(sample_invalid_message(10000))
          positions.append(writer.last_pos)
        for i in range(10, 14):
          writer.write_message(sample_message(i, 10000))
          positions.append(writer.last_pos)
        for i in range(14, 15):
          writer.write_record(sample_invalid_message(10000))
          positions.append(writer.last_pos)
        for i in range(15, 23):
          writer.write_message(sample_message(i, 10000))
          positions.append(writer.last_pos)
      # Read messages [0..9) successfully (all before the first invalid
      # message).
      skipped_regions = []

      def recovery(skipped_region):
        skipped_regions.append(skipped_region)
        raise StopIteration

      with riegeli.RecordReader(
          files.reading_open(),
          owns_src=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos,
          recovery=recovery,
      ) as reader:
        for i in range(9):
          self.assertEqual(
              reader.read_message(records_test_pb2.SimpleMessage),
              sample_message(i, 10000),
          )
        self.assertIsNone(reader.read_message(records_test_pb2.SimpleMessage))
      self.assertLen(skipped_regions, 1)
      skipped_region = skipped_regions[0]
      self.assertEqual(skipped_region.begin, positions[9].numeric)
      self.assertEqual(skipped_region.end, positions[10].numeric)


if __name__ == '__main__':
  absltest.main()
