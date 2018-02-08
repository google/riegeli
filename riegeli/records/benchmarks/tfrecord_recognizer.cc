// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "riegeli/records/benchmarks/tfrecord_recognizer.h"

#include <stdint.h>

#include "riegeli/base/base.h"
#include "riegeli/base/endian.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/zlib_reader.h"
#include "tensorflow/core/lib/hash/crc32c.h"
#include "tensorflow/core/lib/io/record_reader.h"

namespace riegeli {

void TFRecordDetector::Done() { byte_reader_ = nullptr; }

bool TFRecordDetector::CheckFileFormat(
    tensorflow::io::RecordReaderOptions* record_reader_options) {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  if (RIEGELI_UNLIKELY(!byte_reader_->Pull())) {
    if (RIEGELI_LIKELY(byte_reader_->healthy())) {
      // Empty file: return false but leave healthy() as true. This mimics the
      // behavior of reading functions at end of file.
      return false;
    }
    return Fail(*byte_reader_);
  }

  const Position pos_before = byte_reader_->pos();
  ZLibReader decompressor(byte_reader_);
  Reader* reader;
  if (!decompressor.Pull()) {
    if (decompressor.Close()) return false;
    if (RIEGELI_UNLIKELY(!byte_reader_->Seek(pos_before))) {
      if (byte_reader_->healthy()) return Fail("Seeking failed");
      return Fail(*byte_reader_);
    }
    record_reader_options->compression_type =
        tensorflow::io::RecordReaderOptions::NONE;
    reader = byte_reader_;
  } else {
    record_reader_options->compression_type =
        tensorflow::io::RecordReaderOptions::ZLIB_COMPRESSION;
    record_reader_options->zlib_options =
        tensorflow::io::ZlibCompressionOptions::DEFAULT();
    record_reader_options->zlib_options.window_bits = 32;
    reader = &decompressor;
  }

  char header[sizeof(uint64_t)];
  uint32_t masked_crc;
  if (RIEGELI_UNLIKELY(!reader->Read(header, sizeof(header))) ||
      RIEGELI_UNLIKELY(!reader->Read(reinterpret_cast<char*>(&masked_crc),
                                     sizeof(masked_crc)))) {
    if (reader->healthy()) return Fail("Truncated TFRecord file");
    return Fail(*reader);
  }
  if (tensorflow::crc32c::Unmask(ReadLittleEndian32(masked_crc)) !=
      tensorflow::crc32c::Value(header, sizeof(header))) {
    return Fail("Corrupted TFRecord file");
  }
  return true;
}

}  // namespace riegeli
