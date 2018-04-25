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

// Make ZSTD_DCtx_setMaxWindowSize() and ZSTD_WINDOWLOG_MAX available.
#define ZSTD_STATIC_LINKING_ONLY

#include "riegeli/bytes/zstd_reader.h"

#include <stddef.h>
#include <limits>

#include "absl/base/optimization.h"
#include "absl/strings/str_cat.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/reader.h"
#include "zstd.h"

namespace riegeli {

ZstdReader::ZstdReader(Reader* src, Options options)
    : BufferedReader(options.buffer_size_),
      src_(RIEGELI_ASSERT_NOTNULL(src)),
      decompressor_(ZSTD_createDStream()) {
  if (ABSL_PREDICT_FALSE(decompressor_ == nullptr)) {
    Fail("ZSTD_createDStream() failed");
    return;
  }
  {
    const size_t result = ZSTD_initDStream(decompressor_.get());
    if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
      Fail(absl::StrCat("ZSTD_initDStream() failed: ",
                        ZSTD_getErrorName(result)));
    }
  }
  {
    const size_t result = ZSTD_DCtx_setMaxWindowSize(
        decompressor_.get(), size_t{1} << ZSTD_WINDOWLOG_MAX);
    if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
      Fail(absl::StrCat("ZSTD_DCtx_setMaxWindowSize() failed: ",
                        ZSTD_getErrorName(result)));
    }
  }
}

void ZstdReader::Done() {
  if (!Pull() && ABSL_PREDICT_FALSE(decompressor_ != nullptr)) {
    Fail("Truncated Zstd-compressed stream");
  }
  if (owned_src_ != nullptr) {
    if (ABSL_PREDICT_TRUE(healthy())) {
      if (ABSL_PREDICT_FALSE(!owned_src_->Close())) Fail(*owned_src_);
    }
    owned_src_.reset();
  }
  src_ = nullptr;
  decompressor_.reset();
  BufferedReader::Done();
}

bool ZstdReader::PullSlow() {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of Reader::PullSlow(): "
         "data available, use Pull() instead";
  // After all data have been decompressed, skip BufferedReader::PullSlow()
  // to avoid allocating the buffer in case it was not allocated yet.
  if (ABSL_PREDICT_FALSE(decompressor_ == nullptr)) return false;
  return BufferedReader::PullSlow();
}

bool ZstdReader::ReadInternal(char* dest, size_t min_length,
                              size_t max_length) {
  RIEGELI_ASSERT_GT(min_length, 0u)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "nothing to read";
  RIEGELI_ASSERT_GE(max_length, min_length)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "max_length < min_length";
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of BufferedReader::ReadInternal(): " << message();
  if (ABSL_PREDICT_FALSE(max_length >
                         std::numeric_limits<Position>::max() - limit_pos_)) {
    return FailOverflow();
  }
  if (ABSL_PREDICT_FALSE(decompressor_ == nullptr)) return false;
  ZSTD_outBuffer output = {dest, max_length, 0};
  for (;;) {
    ZSTD_inBuffer input = {src_->cursor(), src_->available(), 0};
    const size_t result =
        ZSTD_decompressStream(decompressor_.get(), &output, &input);
    src_->set_cursor(static_cast<const char*>(input.src) + input.pos);
    if (ABSL_PREDICT_FALSE(result == 0)) {
      decompressor_.reset();
      limit_pos_ += output.pos;
      return output.pos >= min_length;
    }
    if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
      Fail(absl::StrCat("ZSTD_decompressStream() failed: ",
                        ZSTD_getErrorName(result)));
      limit_pos_ += output.pos;
      return output.pos >= min_length;
    }
    if (output.pos >= min_length) {
      limit_pos_ += output.pos;
      return true;
    }
    RIEGELI_ASSERT_EQ(input.pos, input.size)
        << "ZSTD_decompressStream() returned but there are still input data "
           "and output space";
    if (ABSL_PREDICT_FALSE(!src_->Pull())) {
      limit_pos_ += output.pos;
      if (ABSL_PREDICT_TRUE(src_->HopeForMore())) return false;
      if (src_->healthy()) return Fail("Truncated Zstd-compressed stream");
      return Fail(*src_);
    }
  }
}

bool ZstdReader::HopeForMoreSlow() const {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of Reader::HopeForMoreSlow(): "
         "data available, use HopeForMore() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  return decompressor_ != nullptr;
}

}  // namespace riegeli
