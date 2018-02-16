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

// Make ZSTD_initCStream_advanced() available, which allows specifying a size
// hint.
#define ZSTD_STATIC_LINKING_ONLY

#include "riegeli/bytes/zstd_writer.h"

#include <stddef.h>
#include <limits>

#include "riegeli/base/base.h"
#include "riegeli/base/str_cat.h"
#include "riegeli/base/string_view.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/writer.h"
#include "zstd.h"

namespace riegeli {

ZstdWriter::ZstdWriter(Writer* dest, Options options)
    : BufferedWriter(options.buffer_size_),
      dest_(RIEGELI_ASSERT_NOTNULL(dest)),
      compressor_(ZSTD_createCStream()) {
  if (RIEGELI_UNLIKELY(compressor_ == nullptr)) {
    Fail("ZSTD_createCStream() failed");
    return;
  }
  const size_t result = ZSTD_initCStream_advanced(
      compressor_.get(), nullptr, 0,
      ZSTD_getParams(options.compression_level_,
                     IntCast<unsigned long long>(options.size_hint_), 0),
      ZSTD_CONTENTSIZE_UNKNOWN);
  if (RIEGELI_UNLIKELY(ZSTD_isError(result))) {
    Fail(StrCat("ZSTD_initCStream_advanced() failed: ",
                ZSTD_getErrorName(result)));
  }
}

void ZstdWriter::Done() {
  PushInternal();
  RIEGELI_ASSERT_EQ(written_to_buffer(), 0u)
      << "BufferedWriter::PushInternal() did not empty the buffer";
  if (RIEGELI_LIKELY(healthy())) {
    FlushInternal(ZSTD_endStream, "ZSTD_endStream()");
  }
  if (owned_dest_ != nullptr) {
    if (RIEGELI_LIKELY(healthy())) {
      if (RIEGELI_UNLIKELY(!owned_dest_->Close())) Fail(*owned_dest_);
    }
    owned_dest_.reset();
  }
  dest_ = nullptr;
  compressor_.reset();
  BufferedWriter::Done();
}

bool ZstdWriter::Flush(FlushType flush_type) {
  if (RIEGELI_UNLIKELY(!PushInternal())) return false;
  RIEGELI_ASSERT_EQ(written_to_buffer(), 0u)
      << "BufferedWriter::PushInternal() did not empty the buffer";
  if (RIEGELI_UNLIKELY(
          !FlushInternal(ZSTD_flushStream, "ZSTD_flushStream()"))) {
    return false;
  }
  if (RIEGELI_UNLIKELY(!dest_->Flush(flush_type))) {
    if (dest_->healthy()) return false;
    limit_ = start_;
    return Fail(*dest_);
  }
  return true;
}

bool ZstdWriter::WriteInternal(string_view src) {
  RIEGELI_ASSERT(!src.empty())
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "nothing to write";
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "Object unhealthy";
  RIEGELI_ASSERT_EQ(written_to_buffer(), 0u)
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "buffer not cleared";
  if (RIEGELI_UNLIKELY(src.size() >
                       std::numeric_limits<Position>::max() - limit_pos())) {
    limit_ = start_;
    return FailOverflow();
  }
  ZSTD_inBuffer input = {src.data(), src.size(), 0};
  for (;;) {
    ZSTD_outBuffer output = {dest_->cursor(), dest_->available(), 0};
    const size_t result =
        ZSTD_compressStream(compressor_.get(), &output, &input);
    dest_->set_cursor(static_cast<char*>(output.dst) + output.pos);
    if (RIEGELI_UNLIKELY(ZSTD_isError(result))) {
      limit_ = start_;
      return Fail(
          StrCat("ZSTD_compressStream() failed: ", ZSTD_getErrorName(result)));
    }
    if (output.pos < output.size) {
      RIEGELI_ASSERT_EQ(input.pos, input.size)
          << "ZSTD_compressStream() returned but there are still input data "
             "and output space";
      start_pos_ += input.pos;
      return true;
    }
    if (RIEGELI_UNLIKELY(!dest_->Push())) {
      limit_ = start_;
      return Fail(*dest_);
    }
  }
}

template <typename Function>
bool ZstdWriter::FlushInternal(Function function, string_view function_name) {
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of ZstdWriter::FlushInternal(): "
         "Object unhealthy";
  RIEGELI_ASSERT_EQ(written_to_buffer(), 0u)
      << "Failed precondition of ZstdWriter::FlushInternal(): "
         "buffer not cleared";
  for (;;) {
    ZSTD_outBuffer output = {dest_->cursor(), dest_->available(), 0};
    const size_t result = function(compressor_.get(), &output);
    dest_->set_cursor(static_cast<char*>(output.dst) + output.pos);
    if (result == 0) return true;
    if (RIEGELI_UNLIKELY(ZSTD_isError(result))) {
      limit_ = start_;
      return Fail(
          StrCat(function_name, " failed: ", ZSTD_getErrorName(result)));
    }
    RIEGELI_ASSERT_EQ(output.pos, output.size)
        << function_name << " returned but there is still output space";
    if (RIEGELI_UNLIKELY(!dest_->Push())) {
      limit_ = start_;
      return Fail(*dest_);
    }
  }
}

}  // namespace riegeli
