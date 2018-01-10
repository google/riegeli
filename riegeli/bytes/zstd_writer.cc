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
#include <memory>
#include <string>
#include <utility>

#include "riegeli/base/assert.h"
#include "riegeli/base/base.h"
#include "riegeli/base/string_view.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/writer.h"
#include "zstd.h"

namespace riegeli {

ZstdWriter::ZstdWriter() : dest_(nullptr), compressor_(nullptr) {
  MarkCancelled();
}

ZstdWriter::ZstdWriter(std::unique_ptr<Writer> dest, Options options)
    : ZstdWriter(dest.get(), options) {
  owned_dest_ = std::move(dest);
}

ZstdWriter::ZstdWriter(Writer* dest, Options options)
    : BufferedWriter(options.buffer_size_),
      dest_(RIEGELI_ASSERT_NOTNULL(dest)),
      compressor_(ZSTD_createCStream()) {
  if (RIEGELI_UNLIKELY(compressor_ == nullptr)) {
    Fail("ZSTD_createCStream() failed");
    return;
  }
  const unsigned long long size_hint = UnsignedMin(
      options.size_hint_, std::numeric_limits<unsigned long long>::max());
  ZSTD_parameters params =
      ZSTD_getParams(options.compression_level_, size_hint, 0);
  params.fParams.contentSizeFlag = options.size_hint_ > 0 ? 1 : 0;
  const size_t result =
      ZSTD_initCStream_advanced(compressor_, nullptr, 0, params, size_hint);
  if (RIEGELI_UNLIKELY(ZSTD_isError(result))) {
    Fail(std::string("ZSTD_initCStream_advanced() failed: ") +
         ZSTD_getErrorName(result));
  }
}

ZstdWriter::ZstdWriter(ZstdWriter&& src) noexcept
    : BufferedWriter(std::move(src)),
      dest_(riegeli::exchange(src.dest_, nullptr)),
      owned_dest_(std::move(src.owned_dest_)),
      compressor_(riegeli::exchange(src.compressor_, nullptr)) {}

ZstdWriter& ZstdWriter::operator=(ZstdWriter&& src) noexcept {
  if (&src != this) {
    BufferedWriter::operator=(std::move(src));
    dest_ = riegeli::exchange(src.dest_, nullptr);
    owned_dest_ = std::move(src.owned_dest_);
    compressor_ = riegeli::exchange(src.compressor_, nullptr);
  }
  return *this;
}

ZstdWriter::~ZstdWriter() { Cancel(); }

void ZstdWriter::Done() {
  PushInternal();
  if (RIEGELI_LIKELY(healthy())) {
    FlushInternal(ZSTD_endStream, "ZSTD_endStream()");
  }
  if (RIEGELI_LIKELY(healthy())) {
    if (owned_dest_ != nullptr) {
      if (RIEGELI_UNLIKELY(!owned_dest_->Close())) Fail(owned_dest_->Message());
    }
  } else {
    dest_->Cancel();
  }
  dest_ = nullptr;
  owned_dest_.reset();
  ZSTD_freeCStream(compressor_);
  compressor_ = nullptr;
  BufferedWriter::Done();
}

bool ZstdWriter::Flush(FlushType flush_type) {
  if (RIEGELI_UNLIKELY(!PushInternal())) return false;
  if (RIEGELI_UNLIKELY(
          !FlushInternal(ZSTD_flushStream, "ZSTD_flushStream()"))) {
    return false;
  }
  if (RIEGELI_UNLIKELY(!dest_->Flush(flush_type))) {
    if (dest_->healthy()) return false;
    return Fail(dest_->Message());
  }
  return true;
}

bool ZstdWriter::WriteInternal(string_view src) {
  RIEGELI_ASSERT(!src.empty());
  RIEGELI_ASSERT(healthy());
  ZSTD_inBuffer input = {src.data(), src.size(), 0};
  for (;;) {
    ZSTD_outBuffer output = {dest_->cursor(), dest_->available(), 0};
    const size_t result = ZSTD_compressStream(compressor_, &output, &input);
    dest_->set_cursor(static_cast<char*>(output.dst) + output.pos);
    if (RIEGELI_UNLIKELY(ZSTD_isError(result))) {
      return Fail(std::string("ZSTD_compressStream() failed: ") +
                  ZSTD_getErrorName(result));
    }
    if (output.pos < output.size) {
      RIEGELI_ASSERT_EQ(input.pos, input.size);
      start_pos_ += input.pos;
      return true;
    }
    if (RIEGELI_UNLIKELY(!dest_->Push())) {
      RIEGELI_ASSERT(!dest_->healthy());
      return Fail(dest_->Message());
    }
  }
}

template <typename Function>
bool ZstdWriter::FlushInternal(Function function, const char* function_name) {
  RIEGELI_ASSERT(healthy());
  for (;;) {
    ZSTD_outBuffer output = {dest_->cursor(), dest_->available(), 0};
    const size_t result = function(compressor_, &output);
    dest_->set_cursor(static_cast<char*>(output.dst) + output.pos);
    if (result == 0) return true;
    if (RIEGELI_UNLIKELY(ZSTD_isError(result))) {
      return Fail(std::string(function_name) +
                  " failed: " + ZSTD_getErrorName(result));
    }
    RIEGELI_ASSERT_EQ(output.pos, output.size);
    if (RIEGELI_UNLIKELY(!dest_->Push())) {
      RIEGELI_ASSERT(!dest_->healthy());
      return Fail(dest_->Message());
    }
  }
}

}  // namespace riegeli
