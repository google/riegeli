// Copyright 2017 Google LLC
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

#include "riegeli/bytes/brotli_writer.h"

#include <stddef.h>
#include <stdint.h>
#include <limits>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "brotli/encode.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

BrotliWriter::BrotliWriter(Writer* dest, Options options)
    : BufferedWriter(options.buffer_size_),
      dest_(RIEGELI_ASSERT_NOTNULL(dest)),
      compressor_(BrotliEncoderCreateInstance(nullptr, nullptr, nullptr)) {
  if (ABSL_PREDICT_FALSE(compressor_ == nullptr)) {
    Fail("BrotliEncoderCreateInstance() failed");
    return;
  }
  if (ABSL_PREDICT_FALSE(!BrotliEncoderSetParameter(
          compressor_.get(), BROTLI_PARAM_QUALITY,
          IntCast<uint32_t>(options.compression_level_)))) {
    Fail("BrotliEncoderSetParameter(BROTLI_PARAM_QUALITY) failed");
    return;
  }
  if (ABSL_PREDICT_FALSE(!BrotliEncoderSetParameter(
          compressor_.get(), BROTLI_PARAM_LARGE_WINDOW,
          uint32_t{options.window_log_ > BROTLI_MAX_WINDOW_BITS}))) {
    Fail("BrotliEncoderSetParameter(BROTLI_PARAM_LARGE_WINDOW) failed");
    return;
  }
  if (ABSL_PREDICT_FALSE(
          !BrotliEncoderSetParameter(compressor_.get(), BROTLI_PARAM_LGWIN,
                                     IntCast<uint32_t>(options.window_log_)))) {
    Fail("BrotliEncoderSetParameter(BROTLI_PARAM_LGWIN) failed");
    return;
  }
  if (options.size_hint_ > 0) {
    // Ignore errors from tuning.
    BrotliEncoderSetParameter(
        compressor_.get(), BROTLI_PARAM_SIZE_HINT,
        UnsignedMin(options.size_hint_, std::numeric_limits<uint32_t>::max()));
  }
}

void BrotliWriter::Done() {
  if (ABSL_PREDICT_TRUE(healthy())) {
    const size_t buffered_length = written_to_buffer();
    cursor_ = start_;
    WriteInternal(absl::string_view(start_, buffered_length),
                  BROTLI_OPERATION_FINISH);
  }
  if (owned_dest_ != nullptr) {
    if (ABSL_PREDICT_TRUE(healthy())) {
      if (ABSL_PREDICT_FALSE(!owned_dest_->Close())) Fail(*owned_dest_);
    }
    owned_dest_.reset();
  }
  dest_ = nullptr;
  compressor_.reset();
  BufferedWriter::Done();
}

bool BrotliWriter::Flush(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  const size_t buffered_length = written_to_buffer();
  cursor_ = start_;
  if (ABSL_PREDICT_FALSE(
          !WriteInternal(absl::string_view(start_, buffered_length),
                         BROTLI_OPERATION_FLUSH))) {
    return false;
  }
  if (ABSL_PREDICT_FALSE(!dest_->Flush(flush_type))) {
    if (dest_->healthy()) return false;
    limit_ = start_;
    return Fail(*dest_);
  }
  return true;
}

bool BrotliWriter::WriteInternal(absl::string_view src) {
  RIEGELI_ASSERT(!src.empty())
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "nothing to write";
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "Object unhealthy";
  RIEGELI_ASSERT_EQ(written_to_buffer(), 0u)
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "buffer not cleared";
  return WriteInternal(src, BROTLI_OPERATION_PROCESS);
}

inline bool BrotliWriter::WriteInternal(absl::string_view src,
                                        BrotliEncoderOperation op) {
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of BrotliWriter::WriteInternal(): "
         "Object unhealthy";
  RIEGELI_ASSERT_EQ(written_to_buffer(), 0u)
      << "Failed precondition of BrotliWriter::WriteInternal(): "
         "buffer not cleared";
  if (ABSL_PREDICT_FALSE(src.size() >
                         std::numeric_limits<Position>::max() - limit_pos())) {
    limit_ = start_;
    return FailOverflow();
  }
  size_t available_in = src.size();
  const uint8_t* next_in = reinterpret_cast<const uint8_t*>(src.data());
  size_t available_out = 0;
  for (;;) {
    if (ABSL_PREDICT_FALSE(!BrotliEncoderCompressStream(
            compressor_.get(), op, &available_in, &next_in, &available_out,
            nullptr, nullptr))) {
      limit_ = start_;
      return Fail("BrotliEncoderCompressStream() failed");
    }
    size_t length = 0;
    const char* const data = reinterpret_cast<const char*>(
        BrotliEncoderTakeOutput(compressor_.get(), &length));
    if (length > 0) {
      if (ABSL_PREDICT_FALSE(!dest_->Write(absl::string_view(data, length)))) {
        limit_ = start_;
        return Fail(*dest_);
      }
    } else if (available_in == 0) {
      start_pos_ += src.size();
      return true;
    }
  }
}

}  // namespace riegeli
