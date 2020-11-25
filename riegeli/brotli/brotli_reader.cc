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

#include "riegeli/brotli/brotli_reader.h"

#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <memory>
#include <string>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "brotli/decode.h"
#include "riegeli/base/base.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/pullable_reader.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

void BrotliReaderBase::Initialize(Reader* src) {
  RIEGELI_ASSERT(src != nullptr)
      << "Failed precondition of BrotliReader: null Reader pointer";
  if (ABSL_PREDICT_FALSE(!src->healthy()) && src->available() == 0) {
    Fail(*src);
    return;
  }
  decompressor_.reset(BrotliDecoderCreateInstance(
      allocator_.alloc_func(), allocator_.free_func(), allocator_.opaque()));
  if (ABSL_PREDICT_FALSE(decompressor_ == nullptr)) {
    Fail(absl::InternalError("BrotliDecoderCreateInstance() failed"));
    return;
  }
  if (ABSL_PREDICT_FALSE(!BrotliDecoderSetParameter(
          decompressor_.get(), BROTLI_DECODER_PARAM_LARGE_WINDOW,
          uint32_t{true}))) {
    Fail(absl::InternalError(
        "BrotliDecoderSetParameter(BROTLI_DECODER_PARAM_LARGE_WINDOW) failed"));
  }
}

void BrotliReaderBase::Done() {
  if (ABSL_PREDICT_FALSE(truncated_)) {
    Reader& src = *src_reader();
    Fail(Annotate(absl::DataLossError("Truncated Brotli-compressed stream"),
                  absl::StrCat("at byte ", src.pos())));
  }
  decompressor_.reset();
  PullableReader::Done();
}

bool BrotliReaderBase::Fail(absl::Status status) {
  RIEGELI_ASSERT(!status.ok())
      << "Failed precondition of Object::Fail(): status not failed";
  return FailWithoutAnnotation(
      Annotate(status, absl::StrCat("at uncompressed byte ", pos())));
}

bool BrotliReaderBase::PullSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Reader::PullSlow(): "
         "enough data available, use Pull() instead";
  if (ABSL_PREDICT_FALSE(!PullUsingScratch(min_length, recommended_length))) {
    return available() >= min_length;
  }
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(decompressor_ == nullptr)) return false;
  Reader& src = *src_reader();
  truncated_ = false;
  size_t available_out = 0;
  for (;;) {
    size_t available_in = src.available();
    const uint8_t* next_in = reinterpret_cast<const uint8_t*>(src.cursor());
    const BrotliDecoderResult result = BrotliDecoderDecompressStream(
        decompressor_.get(), &available_in, &next_in, &available_out, nullptr,
        nullptr);
    src.set_cursor(reinterpret_cast<const char*>(next_in));
    switch (result) {
      case BROTLI_DECODER_RESULT_ERROR:
        set_buffer();
        return Fail(
            Annotate(absl::DataLossError(absl::StrCat(
                         "BrotliDecoderDecompressStream() failed: ",
                         BrotliDecoderErrorString(
                             BrotliDecoderGetErrorCode(decompressor_.get())))),
                     absl::StrCat("at byte ", src.pos())));
      case BROTLI_DECODER_RESULT_SUCCESS:
        set_buffer();
        decompressor_.reset();
        return false;
      case BROTLI_DECODER_RESULT_NEEDS_MORE_INPUT:
      case BROTLI_DECODER_RESULT_NEEDS_MORE_OUTPUT: {
        // Take the output first even if `BrotliDecoderDecompressStream()`
        // returned `BROTLI_DECODER_RESULT_NEEDS_MORE_INPUT`, in order to be
        // able to read data which have been written before a `Flush()` without
        // waiting for data to be written after the `Flush()`.
        size_t length = 0;
        const char* const data = reinterpret_cast<const char*>(
            BrotliDecoderTakeOutput(decompressor_.get(), &length));
        if (length > 0) {
          if (ABSL_PREDICT_FALSE(length > std::numeric_limits<Position>::max() -
                                              limit_pos())) {
            set_buffer();
            return FailOverflow();
          }
          set_buffer(data, length);
          move_limit_pos(available());
          return true;
        }
        RIEGELI_ASSERT_EQ(result, BROTLI_DECODER_RESULT_NEEDS_MORE_INPUT)
            << "BrotliDecoderDecompressStream() returned "
               "BROTLI_DECODER_RESULT_NEEDS_MORE_OUTPUT but "
               "BrotliDecoderTakeOutput() returned no data";
        if (ABSL_PREDICT_FALSE(!src.Pull())) {
          set_buffer();
          if (ABSL_PREDICT_FALSE(!src.healthy())) return Fail(src);
          truncated_ = true;
          return false;
        }
        continue;
      }
    }
    RIEGELI_ASSERT_UNREACHABLE()
        << "Unknown BrotliDecoderResult: " << static_cast<int>(result);
  }
}

}  // namespace riegeli
