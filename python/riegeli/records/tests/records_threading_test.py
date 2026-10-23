# Copyright 2026 Google LLC
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

"""Exercises RecordWriter/RecordReader from several threads at once.

Riegeli lazily creates a few interned Python objects on first use (attribute
names, imported constants). This test releases all threads together before any
of them has touched riegeli, so that first use happens concurrently. It is
meant to run under ThreadSanitizer with free-threaded Python, but passes as a
plain round-trip test otherwise.
"""

import io
import threading

from absl.testing import absltest
import riegeli

_NUM_THREADS = 8
_NUM_RECORDS = 10
_BARRIER_TIMEOUT_SECONDS = 60


def _records(thread_index):
  return [
      f'thread {thread_index} record {i}'.encode() for i in range(_NUM_RECORDS)
  ]


class RecordsThreadingTest(absltest.TestCase):

  def test_concurrent_write_read(self):
    barrier = threading.Barrier(_NUM_THREADS)
    results: dict[int, list[bytes]] = {}
    errors: dict[int, Exception] = {}

    def run(thread_index):
      try:
        barrier.wait(_BARRIER_TIMEOUT_SECONDS)
        dest = io.BytesIO()
        with riegeli.RecordWriter(dest, owns_dest=False) as writer:
          writer.write_records(_records(thread_index))
          writer.flush()
        with riegeli.RecordReader(
            io.BytesIO(dest.getvalue()), owns_src=False
        ) as reader:
          results[thread_index] = list(reader.read_records())
      except Exception as e:  # pylint: disable=broad-exception-caught
        errors[thread_index] = e

    threads = [
        threading.Thread(target=run, args=(i,)) for i in range(_NUM_THREADS)
    ]
    for thread in threads:
      thread.start()
    for thread in threads:
      thread.join()

    self.assertEmpty(errors)
    self.assertEqual(results, {i: _records(i) for i in range(_NUM_THREADS)})


if __name__ == '__main__':
  absltest.main()
