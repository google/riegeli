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

#include "absl/base/macros.h"
#include "absl/base/optimization.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/reader.h"
#include "zconf.h"
#include "zlib.h"

namespace riegeli {

void ZlibReaderBase::Initialize(int window_bits) {
  decompressor_.reset(new z_stream());
  if (ABSL_PREDICT_FALSE(inflateInit2(decompressor_.get(), window_bits) !=
                         Z_OK)) {
    FailOperation("inflateInit2()");
  }
}

void ZlibReaderBase::Done() {
  if (ABSL_PREDICT_FALSE(truncated_)) Fail("Truncated zlib-compressed stream");
  decompressor_.reset();
  BufferedReader::Done();
}

inline bool ZlibReaderBase::FailOperation(absl::string_view operation) {
  std::string message = absl::StrCat(operation, " failed");
  if (decompressor_->msg != nullptr) {
    absl::StrAppend(&message, ": ", decompressor_->msg);
  }
  return Fail(message);
}

bool ZlibReaderBase::PullSlow() {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of Reader::PullSlow(): "
         "data available, use Pull() instead";
  // After all data have been decompressed, skip BufferedReader::PullSlow()
  // to avoid allocating the buffer in case it was not allocated yet.
  if (ABSL_PREDICT_FALSE(decompressor_ == nullptr)) return false;
  return BufferedReader::PullSlow();
}

bool ZlibReaderBase::ReadInternal(char* dest, size_t min_length,
                                  size_t max_length) {
  RIEGELI_ASSERT_GT(min_length, 0u)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "nothing to read";
  RIEGELI_ASSERT_GE(max_length, min_length)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "max_length < min_length";
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of BufferedReader::ReadInternal(): " << message();
  if (ABSL_PREDICT_FALSE(decompressor_ == nullptr)) return false;
  Reader* const src = src_reader();
  truncated_ = false;
  decompressor_->next_out = reinterpret_cast<Bytef*>(dest);
  for (;;) {
    decompressor_->avail_out = UnsignedMin(
        PtrDistance(reinterpret_cast<char*>(decompressor_->next_out),
                    dest + max_length),
        std::numeric_limits<uInt>::max());
    decompressor_->next_in = const_cast<z_const Bytef*>(
        reinterpret_cast<const Bytef*>(src->cursor()));
    decompressor_->avail_in =
        UnsignedMin(src->available(), std::numeric_limits<uInt>::max());
    const int result = inflate(decompressor_.get(), Z_NO_FLUSH);
    src->set_cursor(reinterpret_cast<const char*>(decompressor_->next_in));
    const size_t length_read =
        PtrDistance(dest, reinterpret_cast<char*>(decompressor_->next_out));
    switch (result) {
      case Z_OK:
        if (length_read >= min_length) break;
        ABSL_FALLTHROUGH_INTENDED;
      case Z_BUF_ERROR:
        RIEGELI_ASSERT_EQ(decompressor_->avail_in, 0u)
            << "inflate() returned but there are still input data";
        if (ABSL_PREDICT_FALSE(!src->Pull())) {
          limit_pos_ += length_read;
          if (ABSL_PREDICT_FALSE(!src->healthy())) return Fail(*src);
          truncated_ = true;
          return false;
        }
        continue;
      case Z_STREAM_END:
        decompressor_.reset();
        break;
      default:
        FailOperation("inflate()");
        break;
    }
    limit_pos_ += length_read;
    return length_read >= min_length;
  }
}

template class ZlibReader<Reader*>;
template class ZlibReader<std::unique_ptr<Reader>>;

}  // namespace riegeli
