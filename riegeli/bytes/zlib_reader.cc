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

#include "riegeli/bytes/zlib_reader.h"

#include <stddef.h>
#include <limits>
#include <string>

#include "absl/base/optimization.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/reader.h"
#include "zconf.h"
#include "zlib.h"

namespace riegeli {

ZlibReader::ZlibReader(Reader* src, Options options)
    : BufferedReader(options.buffer_size_), src_(RIEGELI_ASSERT_NOTNULL(src)) {
  decompressor_present_ = true;
  decompressor_.next_in = nullptr;
  decompressor_.avail_in = 0;
  decompressor_.zalloc = nullptr;
  decompressor_.zfree = nullptr;
  decompressor_.opaque = nullptr;
  if (ABSL_PREDICT_FALSE(inflateInit2(&decompressor_, options.window_bits_) !=
                         Z_OK)) {
    FailOperation("inflateInit2()");
  }
}

void ZlibReader::Done() {
  if (ABSL_PREDICT_FALSE(truncated_)) Fail("Truncated zlib-compressed stream");
  if (owned_src_ != nullptr) {
    if (ABSL_PREDICT_FALSE(!owned_src_->Close())) Fail(*owned_src_);
  }
  if (decompressor_present_) {
    decompressor_present_ = false;
    const int result = inflateEnd(&decompressor_);
    RIEGELI_ASSERT_EQ(result, Z_OK) << "inflateEnd() failed";
  }
  BufferedReader::Done();
}

inline bool ZlibReader::FailOperation(absl::string_view operation) {
  std::string message = absl::StrCat(operation, " failed");
  if (decompressor_.msg != nullptr) {
    absl::StrAppend(&message, ": ", decompressor_.msg);
  }
  return Fail(message);
}

bool ZlibReader::PullSlow() {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of Reader::PullSlow(): "
         "data available, use Pull() instead";
  // After all data have been decompressed, skip BufferedReader::PullSlow()
  // to avoid allocating the buffer in case it was not allocated yet.
  if (ABSL_PREDICT_FALSE(!decompressor_present_)) return false;
  return BufferedReader::PullSlow();
}

bool ZlibReader::ReadInternal(char* dest, size_t min_length,
                              size_t max_length) {
  RIEGELI_ASSERT_GT(min_length, 0u)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "nothing to read";
  RIEGELI_ASSERT_GE(max_length, min_length)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "max_length < min_length";
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of BufferedReader::ReadInternal(): " << message();
  if (ABSL_PREDICT_FALSE(!decompressor_present_)) return false;
  truncated_ = false;
  decompressor_.next_out = reinterpret_cast<Bytef*>(dest);
  for (;;) {
    decompressor_.avail_out =
        UnsignedMin(max_length - PtrDistance(reinterpret_cast<Bytef*>(dest),
                                             decompressor_.next_out),
                    std::numeric_limits<uInt>::max());
    decompressor_.next_in = const_cast<z_const Bytef*>(
        reinterpret_cast<const Bytef*>(src_->cursor()));
    decompressor_.avail_in =
        UnsignedMin(src_->available(), std::numeric_limits<uInt>::max());
    int result = inflate(&decompressor_, Z_NO_FLUSH);
    src_->set_cursor(reinterpret_cast<const char*>(decompressor_.next_in));
    const size_t length_read =
        PtrDistance(dest, reinterpret_cast<char*>(decompressor_.next_out));
    switch (result) {
      case Z_OK:
        if (length_read >= min_length) {
          limit_pos_ += length_read;
          return true;
        }
        RIEGELI_ASSERT_EQ(decompressor_.avail_in, 0u)
            << "inflate() returned but there are still input data and output "
               "space";
        if (ABSL_PREDICT_FALSE(!src_->Pull())) {
          limit_pos_ += length_read;
          if (ABSL_PREDICT_TRUE(src_->healthy())) {
            truncated_ = true;
            return false;
          }
          return Fail(*src_);
        }
        continue;
      case Z_STREAM_END:
        decompressor_present_ = false;
        result = inflateEnd(&decompressor_);
        RIEGELI_ASSERT_EQ(result, Z_OK) << "inflateEnd() failed";
        limit_pos_ += length_read;
        return length_read >= min_length;
      default:
        FailOperation("inflate()");
        limit_pos_ += length_read;
        return length_read >= min_length;
    }
  }
}

}  // namespace riegeli
