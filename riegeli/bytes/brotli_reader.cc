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

#include "riegeli/bytes/brotli_reader.h"

#include <stddef.h>
#include <stdint.h>
#include <memory>
#include <string>
#include <utility>

#include "brotli/decode.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

inline void BrotliReader::BrotliDecoderStateDeleter::operator()(
    BrotliDecoderState* ptr) const {
  BrotliDecoderDestroyInstance(ptr);
}

BrotliReader::BrotliReader() noexcept { MarkClosed(); }

BrotliReader::BrotliReader(std::unique_ptr<Reader> src, Options options)
    : BrotliReader(src.get(), options) {
  owned_src_ = std::move(src);
}

BrotliReader::BrotliReader(Reader* src, Options options)
    : src_(RIEGELI_ASSERT_NOTNULL(src)),
      decompressor_(BrotliDecoderCreateInstance(nullptr, nullptr, nullptr)) {
  if (RIEGELI_UNLIKELY(decompressor_ == nullptr)) {
    Fail("BrotliDecoderCreateInstance() failed");
  }
}

BrotliReader::BrotliReader(BrotliReader&& src) noexcept
    : Reader(std::move(src)),
      owned_src_(std::move(src.owned_src_)),
      src_(riegeli::exchange(src.src_, nullptr)),
      decompressor_(std::move(src.decompressor_)) {}

BrotliReader& BrotliReader::operator=(BrotliReader&& src) noexcept {
  Reader::operator=(std::move(src));
  owned_src_ = std::move(src.owned_src_);
  src_ = riegeli::exchange(src.src_, nullptr);
  decompressor_ = std::move(src.decompressor_);
  return *this;
}

BrotliReader::~BrotliReader() = default;

void BrotliReader::Done() {
  if (RIEGELI_UNLIKELY(!Pull() && decompressor_ != nullptr)) {
    Fail("Truncated Brotli-compressed stream");
  }
  if (owned_src_ != nullptr) {
    if (RIEGELI_LIKELY(healthy())) {
      if (RIEGELI_UNLIKELY(!owned_src_->Close())) Fail(*owned_src_);
    }
    owned_src_.reset();
  }
  src_ = nullptr;
  decompressor_.reset();
  Reader::Done();
}

bool BrotliReader::PullSlow() {
  RIEGELI_ASSERT_EQ(available(), 0u);
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  if (RIEGELI_UNLIKELY(decompressor_ == nullptr)) return false;
  size_t available_out = 0;
  for (;;) {
    size_t available_in = src_->available();
    const uint8_t* next_in = reinterpret_cast<const uint8_t*>(src_->cursor());
    const BrotliDecoderResult result = BrotliDecoderDecompressStream(
        decompressor_.get(), &available_in, &next_in, &available_out, nullptr,
        nullptr);
    src_->set_cursor(reinterpret_cast<const char*>(next_in));
    if (RIEGELI_UNLIKELY(result == BROTLI_DECODER_RESULT_ERROR)) {
      Fail(std::string("BrotliDecoderDecompressStream() failed: ") +
           BrotliDecoderErrorString(
               BrotliDecoderGetErrorCode(decompressor_.get())));
    }
    // Take the output first even if BrotliDecoderDecompressStream() returned
    // BROTLI_DECODER_RESULT_NEEDS_MORE_INPUT, in order to be able to read data
    // which have been written before a Flush() without waiting for data to be
    // written after the Flush().
    size_t length = 0;
    const char* const data = reinterpret_cast<const char*>(
        BrotliDecoderTakeOutput(decompressor_.get(), &length));
    if (length > 0) {
      start_ = data;
      cursor_ = data;
      limit_ = data + length;
      limit_pos_ += length;
    }
    switch (result) {
      case BROTLI_DECODER_RESULT_ERROR:
        return length > 0;
      case BROTLI_DECODER_RESULT_SUCCESS:
        decompressor_.reset();
        return length > 0;
      case BROTLI_DECODER_RESULT_NEEDS_MORE_INPUT:
        if (length > 0) return true;
        if (RIEGELI_UNLIKELY(!src_->Pull())) {
          if (RIEGELI_LIKELY(src_->HopeForMore())) return false;
          if (src_->healthy()) {
            return Fail("Truncated Brotli-compressed stream");
          }
          return Fail(*src_);
        }
        continue;
      case BROTLI_DECODER_RESULT_NEEDS_MORE_OUTPUT:
        RIEGELI_ASSERT_GT(length, 0u)
            << "BrotliDecoderDecompressStream() returned "
               "BROTLI_DECODER_RESULT_NEEDS_MORE_OUTPUT but "
               "BrotliDecoderTakeOutput() returned no data";
        return true;
    }
    RIEGELI_UNREACHABLE() << "Unknown BrotliDecoderResult: "
                          << static_cast<int>(result);
  }
}

bool BrotliReader::HopeForMoreSlow() const {
  RIEGELI_ASSERT_EQ(available(), 0u);
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  return decompressor_ != nullptr;
}

}  // namespace riegeli
