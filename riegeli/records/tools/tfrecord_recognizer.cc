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
#include "riegeli/base/any_dependency.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/endian/endian_reading.h"
#include "riegeli/zlib/zlib_reader.h"
#include "tensorflow/core/lib/hash/crc32c.h"
#include "tensorflow/core/lib/io/record_reader.h"

namespace riegeli {

bool TFRecordRecognizer::CheckFileFormat(
    tensorflow::io::RecordReaderOptions& record_reader_options) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (ABSL_PREDICT_FALSE(!byte_reader_->Pull())) {
    if (ABSL_PREDICT_FALSE(!byte_reader_->ok())) {
      return Fail(byte_reader_->status());
    }
    // Empty file: return `false` but leave `ok()` as `true`. This mimics the
    // behavior of reading functions at end of file.
    return false;
  }

  AnyDependency<Reader*>::Inlining<ZlibReader<>> reader;
  if (RecognizeZlib(*byte_reader_)) {
    record_reader_options.compression_type =
        tensorflow::io::RecordReaderOptions::ZLIB_COMPRESSION;
    record_reader_options.zlib_options =
        tensorflow::io::ZlibCompressionOptions::DEFAULT();
    record_reader_options.zlib_options.window_bits = 32;
    reader.Emplace<ZlibReader<>>(byte_reader_);
  } else {
    record_reader_options.compression_type =
        tensorflow::io::RecordReaderOptions::NONE;
    reader = byte_reader_;
  }

  if (ABSL_PREDICT_FALSE(!reader->Pull(sizeof(uint64_t) + sizeof(uint32_t)))) {
    if (ABSL_PREDICT_FALSE(!reader->ok())) return Fail(reader->status());
    return Fail(absl::InvalidArgumentError("Truncated TFRecord file"));
  }
  if (tensorflow::crc32c::Unmask(
          ReadLittleEndian32(reader->cursor() + sizeof(uint64_t))) !=
      tensorflow::crc32c::Value(reader->cursor(), sizeof(uint64_t))) {
    return Fail(absl::InvalidArgumentError("Corrupted TFRecord file"));
  }
  return true;
}

}  // namespace riegeli
