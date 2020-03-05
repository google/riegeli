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

#include "riegeli/records/tools/tfrecord_recognizer.h"

#include <stdint.h>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/endian_reading.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/zlib_reader.h"
#include "tensorflow/core/lib/hash/crc32c.h"
#include "tensorflow/core/lib/io/record_reader.h"

namespace riegeli {

bool TFRecordRecognizer::CheckFileFormat(
    tensorflow::io::RecordReaderOptions* record_reader_options) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(!byte_reader_->Pull())) {
    if (ABSL_PREDICT_FALSE(!byte_reader_->healthy())) {
      return Fail(*byte_reader_);
    }
    // Empty file: return `false` but leave `healthy()` as `true`. This mimics
    // the behavior of reading functions at end of file.
    return false;
  }

  const Position pos_before = byte_reader_->pos();
  ZlibReader<> decompressor(byte_reader_);
  Reader* reader;
  if (!decompressor.Pull()) {
    if (decompressor.Close()) return false;
    if (ABSL_PREDICT_FALSE(!byte_reader_->Seek(pos_before))) {
      return Fail(*byte_reader_, absl::InternalError("Seeking failed"));
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

  if (ABSL_PREDICT_FALSE(!reader->Pull(sizeof(uint64_t) + sizeof(uint32_t)))) {
    if (ABSL_PREDICT_FALSE(!reader->healthy())) return Fail(*reader);
    return Fail(absl::DataLossError("Truncated TFRecord file"));
  }
  if (tensorflow::crc32c::Unmask(
          ReadLittleEndian32(reader->cursor() + sizeof(uint64_t))) !=
      tensorflow::crc32c::Value(reader->cursor(), sizeof(uint64_t))) {
    return Fail(absl::DataLossError("Corrupted TFRecord file"));
  }
  return true;
}

}  // namespace riegeli
