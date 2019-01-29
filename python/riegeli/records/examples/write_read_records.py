# Copyright 2019 Google LLC
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
"""Simple example which writes and reads a Riegeli/records file."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import io

import riegeli
from riegeli.records.examples import records_test_pb2


def sample_string(i, size):
  piece = '{} '.format(i).encode()
  result = piece * -(-size // len(piece))  # length(result) >= size
  return result[:size]


def sample_message(i, size):
  return records_test_pb2.SimpleMessage(id=i, payload=sample_string(i, size))


def write_records(filename):
  print('Writing', filename)
  metadata = riegeli.RecordsMetadata()
  riegeli.set_record_type(metadata, records_test_pb2.SimpleMessage)
  with riegeli.RecordWriter(
      io.FileIO(filename, mode='wb'), options='transpose',
      metadata=metadata) as writer:
    writer.write_messages(sample_message(i, 100) for i in range(100))


def read_records(filename):
  print('Reading', filename)
  with riegeli.RecordReader(
      io.FileIO(filename, mode='rb'),
      field_projection=[[
          records_test_pb2.SimpleMessage.DESCRIPTOR.fields_by_name['id'].number
      ]]) as reader:
    print(' '.join(
        str(record.id)
        for record in reader.read_messages(records_test_pb2.SimpleMessage)))


def main():
  filename = '/tmp/riegeli_example'
  write_records(filename)
  read_records(filename)


if __name__ == '__main__':
  main()
