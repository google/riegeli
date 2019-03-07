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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import abc
import contextlib
import io
import itertools

from absl import logging
from absl.testing import absltest
from absl.testing import parameterized
from builtins import bytes  # pylint: disable=redefined-builtin
from builtins import range  # pylint: disable=redefined-builtin
import future.utils as future_utils
import riegeli
import tensorflow as tf

from riegeli.records.tests import records_test_pb2


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


class FakeFile(object):

  __slots__ = ('_random_access',)

  def __init__(self, random_access):
    self._random_access = random_access

  def tell(self):
    if self._random_access:
      return 0
    raise NotImplementedError('tell()')

  def __getattr__(self, name):
    raise NotImplementedError(name + '()')


class UnseekableWrapper(object):

  __slots__ = ('_wrapped',)

  def __init__(self, wrapped):
    self._wrapped = wrapped

  def tell(self, *args):
    raise NotImplementedError('tell()')

  def seek(self, *args):
    raise NotImplementedError('seek()')

  def __getattr__(self, name):
    return getattr(self._wrapped, name)


class FileSpecBase(future_utils.with_metaclass(abc.ABCMeta, object)):

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
    if self._random_access:
      return self._file
    else:
      return UnseekableWrapper(self._file)

  @property
  def writing_should_close(self):
    return True

  @property
  def writing_assumed_pos(self):
    if self._random_access:
      return None
    return 0

  @abc.abstractmethod
  def _open_for_reading(self):
    raise NotImplementedError('_open_for_reading()')

  def reading_open(self):
    self._open_for_reading()
    logging.debug('Opened %r for reading', self._file)
    if self._random_access:
      return self._file
    else:
      return UnseekableWrapper(self._file)

  @property
  def reading_should_close(self):
    return True

  @property
  def reading_assumed_pos(self):
    if self._random_access:
      return None
    return 0

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
    self._file.seek(0)


class LocalFileSpecBase(FileSpecBase):

  __slots__ = ('_filename',)

  def __init__(self, create_tempfile, random_access):
    super(LocalFileSpecBase, self).__init__(create_tempfile, random_access)
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
    self._file = tf.gfile.GFile(self._filename, mode='wb')

  def _open_for_reading(self):
    self._file = tf.gfile.GFile(self._filename, mode='rb')


def sample_string(i, size):
  piece = '{} '.format(i).encode()
  result = piece * -(-size // len(piece))  # len(result) >= size
  return result[:size]


def sample_message(i, size):
  return records_test_pb2.SimpleMessage(id=i, payload=sample_string(i, size))


def record_writer_options(parallelism):
  return 'uncompressed,chunk_size:35000,parallelism:{}'.format(parallelism)


# pyformat: disable
_FILE_SPEC_VALUES = (('BytesIO', BytesIOSpec),
                     ('FileIO', FileIOSpec),
                     ('BufferedIO', BufferedIOSpec),
                     ('BuiltinFile', BuiltinFileSpec),
                     ('TensorFlowGFile', TensorFlowGFileSpec))

_RANDOM_ACCESS_VALUES = (('randomAccess', True),
                         ('streamAccess', False))

_PARALLELISM_VALUES = (('serial', 0),
                       ('parallel', 10))
# pyformat: enable

_PARAMETERIZE_BY_RANDOM_ACCESS = (
    parameterized.named_parameters(*_RANDOM_ACCESS_VALUES))

_PARAMETERIZE_BY_RANDOM_ACCESS_AND_PARALLELISM = (
    parameterized.named_parameters(
        combine_named_parameters(_RANDOM_ACCESS_VALUES, _PARALLELISM_VALUES)))

_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM = (
    parameterized.named_parameters(
        combine_named_parameters(_FILE_SPEC_VALUES, _RANDOM_ACCESS_VALUES,
                                 _PARALLELISM_VALUES)))


class RecordsTest(parameterized.TestCase):

  def corrupt_at(self, files, index):
    byte_reader = files.reading_open()
    contents1 = byte_reader.read(index)
    contents2 = byte_reader.read(1)
    contents2 = bytes([(bytes(contents2)[0] + 1) % 256])
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
    byte_writer = FakeFile(random_access)
    with self.assertRaises(NotImplementedError):
      with riegeli.RecordWriter(
          byte_writer,
          assumed_pos=None if random_access else 0,
          options=record_writer_options(parallelism)) as writer:
        writer.write_record(sample_string(0, 10000))

  @_PARAMETERIZE_BY_RANDOM_ACCESS
  def test_record_reader_exception_from_file(self, random_access):
    byte_reader = FakeFile(random_access)
    with self.assertRaises(NotImplementedError):
      with riegeli.RecordReader(
          byte_reader, assumed_pos=None if random_access else 0,
          close=False) as reader:
        reader.read_record()

  @_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM
  def test_write_read_record(self, file_spec, random_access, parallelism):
    with contextlib.closing(file_spec(self.create_tempfile,
                                      random_access)) as files:
      with riegeli.RecordWriter(
          files.writing_open(),
          close=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism)) as writer:
        for i in range(23):
          writer.write_record(sample_string(i, 10000))
      with riegeli.RecordReader(
          files.reading_open(),
          close=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos) as reader:
        for i in range(23):
          self.assertEqual(reader.read_record(), sample_string(i, 10000))
        self.assertIsNone(reader.read_record())

  @_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM
  def test_write_read_record_with_key(self, file_spec, random_access,
                                      parallelism):
    with contextlib.closing(file_spec(self.create_tempfile,
                                      random_access)) as files:
      keys = []
      with riegeli.RecordWriter(
          files.writing_open(),
          close=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism)) as writer:
        for i in range(23):
          pos = writer.pos
          key = writer.write_record_with_key(sample_string(i, 10000))
          if keys:
            self.assertGreater(pos, keys[-1])
          self.assertLessEqual(pos, key)
          keys.append(key)
        writer.close()
        end_pos = writer.pos
      with riegeli.RecordReader(
          files.reading_open(),
          close=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos) as reader:
        for i in range(23):
          pos = reader.pos
          self.assertEqual(reader.read_record_with_key(),
                           (keys[i], sample_string(i, 10000)))
          self.assertLessEqual(pos, key)
        self.assertIsNone(reader.read_record_with_key())
        self.assertEqual(reader.pos, end_pos)
        reader.close()
        self.assertEqual(reader.pos, end_pos)

  @_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM
  def test_write_read_message(self, file_spec, random_access, parallelism):
    with contextlib.closing(file_spec(self.create_tempfile,
                                      random_access)) as files:
      with riegeli.RecordWriter(
          files.writing_open(),
          close=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism)) as writer:
        for i in range(23):
          writer.write_message(sample_message(i, 10000))
      with riegeli.RecordReader(
          files.reading_open(),
          close=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos) as reader:
        for i in range(23):
          self.assertEqual(
              reader.read_message(records_test_pb2.SimpleMessage),
              sample_message(i, 10000))
        self.assertIsNone(reader.read_message(records_test_pb2.SimpleMessage))

  @_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM
  def test_write_read_message_with_key(self, file_spec, random_access,
                                       parallelism):
    with contextlib.closing(file_spec(self.create_tempfile,
                                      random_access)) as files:
      keys = []
      with riegeli.RecordWriter(
          files.writing_open(),
          close=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism)) as writer:
        for i in range(23):
          pos = writer.pos
          key = writer.write_message_with_key(sample_message(i, 10000))
          if keys:
            self.assertGreater(pos, keys[-1])
          self.assertLessEqual(pos, key)
          keys.append(key)
        writer.close()
        end_pos = writer.pos
      with riegeli.RecordReader(
          files.reading_open(),
          close=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos) as reader:
        for i in range(23):
          pos = reader.pos
          self.assertEqual(
              reader.read_message_with_key(records_test_pb2.SimpleMessage),
              (keys[i], sample_message(i, 10000)))
          self.assertLessEqual(pos, key)
        self.assertIsNone(
            reader.read_message_with_key(records_test_pb2.SimpleMessage))
        self.assertEqual(reader.pos, end_pos)
        reader.close()
        self.assertEqual(reader.pos, end_pos)

  @_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM
  def test_write_read_records(self, file_spec, random_access, parallelism):
    with contextlib.closing(file_spec(self.create_tempfile,
                                      random_access)) as files:
      with riegeli.RecordWriter(
          files.writing_open(),
          close=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism)) as writer:
        writer.write_records(sample_string(i, 10000) for i in range(23))
      with riegeli.RecordReader(
          files.reading_open(),
          close=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos) as reader:
        self.assertEqual(
            list(reader.read_records()),
            [sample_string(i, 10000) for i in range(23)])

  @_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM
  def test_write_read_records_with_keys(self, file_spec, random_access,
                                        parallelism):
    with contextlib.closing(file_spec(self.create_tempfile,
                                      random_access)) as files:
      with riegeli.RecordWriter(
          files.writing_open(),
          close=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism)) as writer:
        keys = writer.write_records_with_keys(
            sample_string(i, 10000) for i in range(23))
      with riegeli.RecordReader(
          files.reading_open(),
          close=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos) as reader:
        self.assertEqual(
            list(reader.read_records_with_keys()),
            [(keys[i], sample_string(i, 10000)) for i in range(23)])

  @_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM
  def test_write_read_messages(self, file_spec, random_access, parallelism):
    with contextlib.closing(file_spec(self.create_tempfile,
                                      random_access)) as files:
      with riegeli.RecordWriter(
          files.writing_open(),
          close=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism)) as writer:
        writer.write_messages(sample_message(i, 10000) for i in range(23))
      with riegeli.RecordReader(
          files.reading_open(),
          close=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos) as reader:
        self.assertEqual(
            list(reader.read_messages(records_test_pb2.SimpleMessage)),
            [sample_message(i, 10000) for i in range(23)])

  @_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM
  def test_write_read_messages_with_keys(self, file_spec, random_access,
                                         parallelism):
    with contextlib.closing(file_spec(self.create_tempfile,
                                      random_access)) as files:
      with riegeli.RecordWriter(
          files.writing_open(),
          close=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism)) as writer:
        keys = writer.write_messages_with_keys(
            sample_message(i, 10000) for i in range(23))
      with riegeli.RecordReader(
          files.reading_open(),
          close=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos) as reader:
        self.assertEqual(
            list(
                reader.read_messages_with_keys(records_test_pb2.SimpleMessage)),
            [(keys[i], sample_message(i, 10000)) for i in range(23)])

  @_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM
  def test_write_read_metadata(self, file_spec, random_access, parallelism):
    with contextlib.closing(file_spec(self.create_tempfile,
                                      random_access)) as files:
      metadata_written = riegeli.RecordsMetadata()
      metadata_written.file_comment = 'Comment'
      riegeli.set_record_type(metadata_written, records_test_pb2.SimpleMessage)
      message_written = sample_message(7, 10)
      with riegeli.RecordWriter(
          files.writing_open(),
          close=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism),
          metadata=metadata_written) as writer:
        writer.write_message(message_written)
      with riegeli.RecordReader(
          files.reading_open(),
          close=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos) as reader:
        metadata_read = reader.read_metadata()
        self.assertEqual(metadata_read, metadata_written)
        record_type = riegeli.get_record_type(metadata_read)
        self.assertIsNotNone(record_type)
        self.assertEqual(record_type.DESCRIPTOR.full_name,
                         'riegeli.tests.SimpleMessage')
        message_read = reader.read_message(record_type)
        # Serialize and deserialize because messages have descriptors of
        # different origins.
        self.assertEqual(
            records_test_pb2.SimpleMessage.FromString(
                message_read.SerializeToString()), message_written)

  @_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM
  def test_field_projection(self, file_spec, random_access, parallelism):
    with contextlib.closing(file_spec(self.create_tempfile,
                                      random_access)) as files:
      with riegeli.RecordWriter(
          files.writing_open(),
          close=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism) + ',transpose') as writer:
        for i in range(23):
          writer.write_message(sample_message(i, 10000))
      with riegeli.RecordReader(
          files.reading_open(),
          close=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos,
          field_projection=[[
              records_test_pb2.SimpleMessage.DESCRIPTOR.fields_by_name['id']
              .number
          ]]) as reader:
        for i in range(23):
          projected_message = records_test_pb2.SimpleMessage()
          projected_message.id = i
          self.assertEqual(
              reader.read_message(records_test_pb2.SimpleMessage),
              projected_message)
        self.assertIsNone(reader.read_message(records_test_pb2.SimpleMessage))

  @_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM
  def test_seek(self, file_spec, random_access, parallelism):
    if not random_access:
      return
    with contextlib.closing(file_spec(self.create_tempfile,
                                      random_access)) as files:
      keys = []
      with riegeli.RecordWriter(
          files.writing_open(),
          close=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism)) as writer:
        for i in range(23):
          pos = writer.pos
          key = writer.write_record_with_key(sample_string(i, 10000))
          if keys:
            self.assertGreater(pos, keys[-1])
          self.assertLessEqual(pos, key)
          keys.append(key)
        writer.close()
        end_pos = writer.pos
      with riegeli.RecordReader(
          files.reading_open(),
          close=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos) as reader:
        reader.seek(keys[9])
        self.assertGreater(reader.pos, keys[8])
        self.assertLessEqual(reader.pos, keys[9])
        reader.seek(keys[9])
        self.assertGreater(reader.pos, keys[8])
        self.assertLessEqual(reader.pos, keys[9])
        reader.seek(keys[11])
        self.assertGreater(reader.pos, keys[10])
        self.assertLessEqual(reader.pos, keys[11])
        self.assertEqual(reader.read_record(), sample_string(11, 10000))
        reader.seek(keys[9])
        self.assertGreater(reader.pos, keys[8])
        self.assertLessEqual(reader.pos, keys[9])
        self.assertEqual(reader.read_record(), sample_string(9, 10000))
        reader.seek(keys[11])
        self.assertGreater(reader.pos, keys[10])
        self.assertLessEqual(reader.pos, keys[11])
        self.assertEqual(reader.read_record(), sample_string(11, 10000))
        reader.seek(keys[13])
        self.assertGreater(reader.pos, keys[12])
        self.assertLessEqual(reader.pos, keys[13])
        self.assertEqual(reader.read_record(), sample_string(13, 10000))
        reader.seek(riegeli.RecordPosition(0, 0))
        self.assertLessEqual(reader.pos, keys[0])
        self.assertEqual(reader.read_record(), sample_string(0, 10000))
        reader.seek(end_pos)
        self.assertLessEqual(reader.pos, end_pos)
        self.assertIsNone(reader.read_record())
        reader.seek(keys[11])
        self.assertGreater(reader.pos, keys[10])
        self.assertLessEqual(reader.pos, keys[11])
        reader.close()
        self.assertGreater(reader.pos, keys[10])
        self.assertLessEqual(reader.pos, keys[11])

  @_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM
  def test_seek_numeric(self, file_spec, random_access, parallelism):
    if not random_access:
      return
    with contextlib.closing(file_spec(self.create_tempfile,
                                      random_access)) as files:
      keys = []
      with riegeli.RecordWriter(
          files.writing_open(),
          close=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism)) as writer:
        for i in range(23):
          pos = writer.pos
          key = writer.write_record_with_key(sample_string(i, 10000))
          if keys:
            self.assertGreater(pos, keys[-1])
          self.assertLessEqual(pos, key)
          keys.append(key)
        writer.close()
        end_pos = writer.pos
      with riegeli.RecordReader(
          files.reading_open(),
          close=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos) as reader:
        reader.seek_numeric(keys[9].numeric)
        self.assertGreater(reader.pos, keys[8])
        self.assertLessEqual(reader.pos, keys[9])
        reader.seek_numeric(keys[9].numeric)
        self.assertGreater(reader.pos, keys[8])
        self.assertLessEqual(reader.pos, keys[9])
        reader.seek_numeric(keys[11].numeric)
        self.assertGreater(reader.pos, keys[10])
        self.assertLessEqual(reader.pos, keys[11])
        self.assertEqual(reader.read_record(), sample_string(11, 10000))
        reader.seek_numeric(keys[9].numeric)
        self.assertGreater(reader.pos, keys[8])
        self.assertLessEqual(reader.pos, keys[9])
        self.assertEqual(reader.read_record(), sample_string(9, 10000))
        reader.seek_numeric(keys[11].numeric)
        self.assertGreater(reader.pos, keys[10])
        self.assertLessEqual(reader.pos, keys[11])
        self.assertEqual(reader.read_record(), sample_string(11, 10000))
        reader.seek_numeric(keys[13].numeric)
        self.assertGreater(reader.pos, keys[12])
        self.assertLessEqual(reader.pos, keys[13])
        self.assertEqual(reader.read_record(), sample_string(13, 10000))
        reader.seek_numeric(0)
        self.assertLessEqual(reader.pos, keys[0])
        self.assertEqual(reader.read_record(), sample_string(0, 10000))
        reader.seek_numeric(end_pos.numeric)
        self.assertLessEqual(reader.pos, end_pos)
        self.assertIsNone(reader.read_record())
        reader.seek_numeric(keys[11].numeric)
        self.assertGreater(reader.pos, keys[10])
        self.assertLessEqual(reader.pos, keys[11])
        reader.close()
        self.assertGreater(reader.pos, keys[10])
        self.assertLessEqual(reader.pos, keys[11])

  @_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM
  def test_corruption_exception(self, file_spec, random_access, parallelism):
    with contextlib.closing(file_spec(self.create_tempfile,
                                      random_access)) as files:
      keys = []
      with riegeli.RecordWriter(
          files.writing_open(),
          close=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism)) as writer:
        for i in range(23):
          keys.append(writer.write_record_with_key(sample_string(i, 10000)))
      # Corrupt the header of the chunk containing records [9, 12).
      self.corrupt_at(files, keys[9].chunk_begin + 20)
      # Read records [0, 9) successfully (all before the corrupted chunk).
      reader = riegeli.RecordReader(
          files.reading_open(),
          close=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos)
      for i in range(9):
        self.assertEqual(reader.read_record(), sample_string(i, 10000))
      with self.assertRaises(riegeli.RiegeliError):
        reader.read_record()
      with self.assertRaises(riegeli.RiegeliError):
        reader.close()

  @_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM
  def test_corruption_recovery(self, file_spec, random_access, parallelism):
    with contextlib.closing(file_spec(self.create_tempfile,
                                      random_access)) as files:
      keys = []
      with riegeli.RecordWriter(
          files.writing_open(),
          close=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism)) as writer:
        for i in range(23):
          keys.append(writer.write_record_with_key(sample_string(i, 10000)))
      # Corrupt the header of the chunk containing records [9, 12).
      self.corrupt_at(files, keys[9].chunk_begin + 20)
      # Read records [0, 9) and [15, 23) successfully (all except the corrupted
      # chunk and the next chunk which intersects the same block).
      skipped_regions = []
      with riegeli.RecordReader(
          files.reading_open(),
          close=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos,
          recovery=skipped_regions.append) as reader:
        for i in range(9):
          self.assertEqual(reader.read_record(), sample_string(i, 10000))
        for i in range(15, 23):
          self.assertEqual(reader.read_record(), sample_string(i, 10000))
        self.assertIsNone(reader.read_record())
      self.assertLen(skipped_regions, 1)
      skipped_region = skipped_regions[0]
      self.assertEqual(skipped_region.begin, keys[9].numeric)
      self.assertEqual(skipped_region.end, keys[15].numeric)

  @_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM
  def test_corruption_recovery_stop_iteration(self, file_spec, random_access,
                                              parallelism):
    with contextlib.closing(file_spec(self.create_tempfile,
                                      random_access)) as files:
      keys = []
      with riegeli.RecordWriter(
          files.writing_open(),
          close=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism)) as writer:
        for i in range(23):
          keys.append(writer.write_record_with_key(sample_string(i, 10000)))
      # Corrupt the header of the chunk containing records [9, 12).
      self.corrupt_at(files, keys[9].chunk_begin + 20)
      # Read records [0, 9) successfully (all before the corrupted chunk).
      skipped_regions = []

      def recovery(skipped_region):
        skipped_regions.append(skipped_region)
        raise StopIteration

      with riegeli.RecordReader(
          files.reading_open(),
          close=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos,
          recovery=recovery) as reader:
        for i in range(9):
          self.assertEqual(reader.read_record(), sample_string(i, 10000))
        self.assertIsNone(reader.read_record())
      self.assertLen(skipped_regions, 1)
      skipped_region = skipped_regions[0]
      self.assertEqual(skipped_region.begin, keys[9].numeric)
      self.assertEqual(skipped_region.end, keys[15].numeric)

  @_PARAMETERIZE_BY_FILE_SPEC_AND_RANDOM_ACCESS_AND_PARALLELISM
  def test_corruption_recovery_exception(self, file_spec, random_access,
                                         parallelism):
    with contextlib.closing(file_spec(self.create_tempfile,
                                      random_access)) as files:
      keys = []
      with riegeli.RecordWriter(
          files.writing_open(),
          close=files.writing_should_close,
          assumed_pos=files.writing_assumed_pos,
          options=record_writer_options(parallelism)) as writer:
        for i in range(23):
          keys.append(writer.write_record_with_key(sample_string(i, 10000)))
      # Corrupt the header of the chunk containing records [9, 12).
      self.corrupt_at(files, keys[9].chunk_begin + 20)

      # Propagate exception from the recovery function
      def recovery(skipped_region):
        raise KeyboardInterrupt

      with riegeli.RecordReader(
          files.reading_open(),
          close=files.reading_should_close,
          assumed_pos=files.reading_assumed_pos,
          recovery=recovery) as reader:
        for i in range(9):
          self.assertEqual(reader.read_record(), sample_string(i, 10000))
        with self.assertRaises(KeyboardInterrupt):
          reader.read_record()


if __name__ == '__main__':
  absltest.main()
