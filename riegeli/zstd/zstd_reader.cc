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

// Enables the experimental zstd API:
//  * `ZSTD_FRAMEHEADERSIZE_PREFIX()`
//  * `ZSTD_FRAMEHEADERSIZE_MAX`
//  * `ZSTD_f_zstd1`
//  * `ZSTD_d_stableOutBuffer`
//  * `ZSTD_skippableFrame`
//  * `ZSTD_frameHeader`
//  * `ZSTD_getFrameHeader()`
#define ZSTD_STATIC_LINKING_ONLY

#include "riegeli/zstd/zstd_reader.h"

#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <memory>
#include <optional>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/recycling_pool.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/reader.h"
#include "zstd.h"

namespace riegeli {

void ZstdReaderBase::Initialize(Reader* src) {
  RIEGELI_ASSERT_NE(src, nullptr)
      << "Failed precondition of ZstdReader: null Reader pointer";
  if (ABSL_PREDICT_FALSE(!src->ok()) && src->available() == 0) {
    FailWithoutAnnotation(AnnotateOverSrc(src->status()));
    return;
  }
  initial_compressed_pos_ = src->pos();
  InitializeDecompressor(*src);
}

inline void ZstdReaderBase::InitializeDecompressor(Reader& src) {
  decompressor_ =
      RecyclingPool<ZSTD_DCtx, ZSTD_DCtxDeleter>::global(
          recycling_pool_options_)
          .Get(
              [] {
                return std::unique_ptr<ZSTD_DCtx, ZSTD_DCtxDeleter>(
                    ZSTD_createDCtx());
              },
              [](ZSTD_DCtx* decompressor) {
                {
                  const size_t result = ZSTD_DCtx_reset(
                      decompressor, ZSTD_reset_session_and_parameters);
                  RIEGELI_ASSERT(!ZSTD_isError(result))
                      << "ZSTD_DCtx_reset() failed: "
                      << ZSTD_getErrorName(result);
                }
#if ZSTD_VERSION_NUMBER <= 10405
                // Workaround for https://github.com/facebook/zstd/issues/2331
                {
                  const size_t result = ZSTD_DCtx_setParameter(
                      decompressor, ZSTD_d_stableOutBuffer, 0);
                  RIEGELI_ASSERT(!ZSTD_isError(result))
                      << "ZSTD_DCtx_setParameter(ZSTD_d_stableOutBuffer) "
                         "failed: "
                      << ZSTD_getErrorName(result);
                }
#endif
              });
  if (ABSL_PREDICT_FALSE(decompressor_ == nullptr)) {
    Fail(absl::InternalError("ZSTD_createDCtx() failed"));
    return;
  }
  {
    // Maximum window size could also be found with
    // `ZSTD_dParam_getBounds(ZSTD_d_windowLogMax)`.
    const size_t result =
        ZSTD_DCtx_setParameter(decompressor_.get(), ZSTD_d_windowLogMax,
                               sizeof(size_t) == 4 ? 30 : 31);
    if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
      Fail(absl::InternalError(
          absl::StrCat("ZSTD_DCtx_setParameter(ZSTD_d_windowLogMax) failed: ",
                       ZSTD_getErrorName(result))));
      return;
    }
  }
  if (!dictionary_.empty()) {
    const ZSTD_DDict* const decompression_dictionary =
        dictionary_.PrepareDecompressionDictionary();
    if (ABSL_PREDICT_FALSE(decompression_dictionary == nullptr)) {
      Fail(absl::InternalError("ZSTD_createDDict_advanced() failed"));
      return;
    }
    const size_t result =
        ZSTD_DCtx_refDDict(decompressor_.get(), decompression_dictionary);
    if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
      Fail(absl::InternalError(absl::StrCat("ZSTD_DCtx_refDDict() failed: ",
                                            ZSTD_getErrorName(result))));
      return;
    }
  }
  if (!concatenate_ && exact_size() == std::nullopt) {
    set_exact_size(ZstdUncompressedSize(src));
  }
  just_initialized_ = true;
}

void ZstdReaderBase::Done() {
  if (ABSL_PREDICT_FALSE(truncated_) && growing_source_) {
    Reader& src = *SrcReader();
    FailWithoutAnnotation(AnnotateOverSrc(src.AnnotateStatus(
        absl::InvalidArgumentError("Truncated Zstd-compressed stream"))));
  }
  BufferedReader::Done();
  decompressor_.reset();
  dictionary_ = ZstdDictionary();
}

absl::Status ZstdReaderBase::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) {
    if (ABSL_PREDICT_FALSE(truncated_)) {
      status = Annotate(status, "reading truncated Zstd-compressed stream");
    }
    Reader& src = *SrcReader();
    status = src.AnnotateStatus(std::move(status));
  }
  // The status might have been annotated by `src` with the compressed position.
  // Clarify that the current position is the uncompressed position instead of
  // delegating to `BufferedReader::AnnotateStatusImpl()`.
  return AnnotateOverSrc(std::move(status));
}

absl::Status ZstdReaderBase::AnnotateOverSrc(absl::Status status) {
  if (is_open()) {
    return Annotate(status, absl::StrCat("at uncompressed byte ", pos()));
  }
  return status;
}

bool ZstdReaderBase::ReadInternal(size_t min_length, size_t max_length,
                                  char* dest) {
  RIEGELI_ASSERT_GT(min_length, 0u)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "nothing to read";
  RIEGELI_ASSERT_GE(max_length, min_length)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "max_length < min_length";
  RIEGELI_ASSERT_OK(*this)
      << "Failed precondition of BufferedReader::ReadInternal()";
  Reader& src = *SrcReader();
  truncated_ = false;
  if (just_initialized_ && !concatenate_ && exact_size() == std::nullopt) {
    // Try again in case the source has grown.
    set_exact_size(ZstdUncompressedSize(src));
  }
  size_t effective_min_length = min_length;
  if (just_initialized_ && !growing_source_ && exact_size() != std::nullopt &&
      max_length >= *exact_size()) {
    // Avoid a memory copy from an internal buffer of the Zstd engine to `dest`
    // by promising to decompress all remaining data to `dest`.
    const size_t result =
        ZSTD_DCtx_setParameter(decompressor_.get(), ZSTD_d_stableOutBuffer, 1);
    if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
      return Fail(absl::InternalError(absl::StrCat(
          "ZSTD_DCtx_setParameter(ZSTD_d_stableOutBuffer) failed: ",
          ZSTD_getErrorName(result))));
    }
    effective_min_length = std::numeric_limits<size_t>::max();
  }
  just_initialized_ = false;
  max_length = UnsignedMin(max_length,
                           std::numeric_limits<Position>::max() - limit_pos());
  ZSTD_outBuffer output = {dest, max_length, 0};
  for (;;) {
    ZSTD_inBuffer input = {src.cursor(), src.available(), 0};
    const size_t result =
        ZSTD_decompressStream(decompressor_.get(), &output, &input);
    src.set_cursor(static_cast<const char*>(input.src) + input.pos);
    if (ABSL_PREDICT_FALSE(result == 0)) {
      if (concatenate_) {
        if (output.pos >= min_length) {
          move_limit_pos(output.pos);
          return true;
        }
        continue;
      }
      decompressor_.reset();
      move_limit_pos(output.pos);
      // Avoid `BufferedReader` allocating another buffer.
      set_exact_size(limit_pos());
      return output.pos >= min_length;
    }
    if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) {
      Fail(absl::InvalidArgumentError(absl::StrCat(
          "ZSTD_decompressStream() failed: ", ZSTD_getErrorName(result))));
      move_limit_pos(output.pos);
      return output.pos >= min_length;
    }
    if (output.pos >= effective_min_length) {
      move_limit_pos(output.pos);
      return true;
    }
    if (ABSL_PREDICT_FALSE(input.pos < input.size)) {
      RIEGELI_ASSERT_EQ(output.pos, output.size)
          << "ZSTD_decompressStream() returned but there are still "
             "input data and output space";
      RIEGELI_ASSERT_EQ(output.pos,
                        std::numeric_limits<Position>::max() - limit_pos())
          << "The position does not overflow but the output buffer is full, "
             "while less than min_length was output, which implies that "
             "ZSTD_decompressStream() wants to output more than the "
             "expected decompressed size to a flat buffer";
      move_limit_pos(output.pos);
      return FailOverflow();
    }
    if (ABSL_PREDICT_FALSE(!src.Pull(1, result))) {
      move_limit_pos(output.pos);
      if (ABSL_PREDICT_FALSE(!src.ok())) {
        FailWithoutAnnotation(AnnotateOverSrc(src.status()));
      } else if (ABSL_PREDICT_FALSE(!concatenate_ || input.pos > 0)) {
        if (!growing_source_) {
          Fail(absl::InvalidArgumentError("Truncated Zstd-compressed stream"));
        }
        truncated_ = true;
      }
      return output.pos >= min_length;
    }
  }
}

void ZstdReaderBase::ExactSizeReached() {
  if (decompressor_ == nullptr) return;
  // This is especially important if `exact_size() == 0` because in that case
  // `ReadInternal()` was never called and the compressed stream was not
  // consumed.
  char buffer[1];
  if (ABSL_PREDICT_FALSE(ZstdReaderBase::ReadInternal(1, 1, buffer))) {
    decompressor_.reset();
    Fail(absl::FailedPreconditionError(
        "Uncompressed size reached but more data can be decompressed, "
        "which implies that seeking back and reading again encountered "
        "changed Zstd-compressed data"));
  }
}

bool ZstdReaderBase::ToleratesReadingAhead() {
  Reader* const src = SrcReader();
  return src != nullptr && src->ToleratesReadingAhead();
}

bool ZstdReaderBase::SupportsRewind() {
  Reader* const src = SrcReader();
  return src != nullptr && src->SupportsRewind();
}

bool ZstdReaderBase::SeekBehindBuffer(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of BufferedReader::SeekBehindBuffer(): "
         "position in the buffer, use Seek() instead";
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedReader::SeekBehindBuffer(): "
         "buffer not empty";
  if (new_pos <= limit_pos()) {
    // Seeking backwards.
    if (ABSL_PREDICT_FALSE(!ok())) return false;
    Reader& src = *SrcReader();
    truncated_ = false;
    set_buffer();
    set_limit_pos(0);
    decompressor_.reset();
    if (ABSL_PREDICT_FALSE(!src.Seek(initial_compressed_pos_))) {
      return FailWithoutAnnotation(AnnotateOverSrc(src.StatusOrAnnotate(
          absl::DataLossError("Zstd-compressed stream got truncated"))));
    }
    InitializeDecompressor(src);
    if (ABSL_PREDICT_FALSE(!ok())) return false;
    if (new_pos == 0) return true;
  }
  return BufferedReader::SeekBehindBuffer(new_pos);
}

bool ZstdReaderBase::SupportsNewReader() {
  Reader* const src = SrcReader();
  return src != nullptr && src->SupportsNewReader();
}

std::unique_ptr<Reader> ZstdReaderBase::NewReaderImpl(Position initial_pos) {
  if (ABSL_PREDICT_FALSE(!ok())) return nullptr;
  // `NewReaderImpl()` is thread-safe from this point
  // if `SrcReader()->SupportsNewReader()`.
  Reader& src = *SrcReader();
  std::unique_ptr<Reader> compressed_reader =
      src.NewReader(initial_compressed_pos_);
  if (ABSL_PREDICT_FALSE(compressed_reader == nullptr)) {
    FailWithoutAnnotation(AnnotateOverSrc(src.status()));
    return nullptr;
  }
  std::unique_ptr<Reader> reader =
      std::make_unique<ZstdReader<std::unique_ptr<Reader>>>(
          std::move(compressed_reader),
          ZstdReaderBase::Options()
              .set_growing_source(growing_source_)
              .set_concatenate(concatenate_)
              .set_dictionary(dictionary_)
              .set_buffer_options(buffer_options())
              .set_recycling_pool_options(recycling_pool_options_));
  reader->Seek(initial_pos);
  return reader;
}

namespace {

inline bool GetFrameHeader(Reader& src, ZSTD_frameHeader& header) {
  if (ABSL_PREDICT_FALSE(!src.Pull(ZSTD_FRAMEHEADERSIZE_PREFIX(ZSTD_f_zstd1),
                                   ZSTD_FRAMEHEADERSIZE_MAX))) {
    return false;
  }
  for (;;) {
    const size_t result =
        ZSTD_getFrameHeader(&header, src.cursor(), src.available());
    if (ABSL_PREDICT_TRUE(result == 0)) return true;
    if (ABSL_PREDICT_FALSE(ZSTD_isError(result))) return false;
    if (ABSL_PREDICT_FALSE(!src.Pull(result))) return false;
  }
}

}  // namespace

bool RecognizeZstd(Reader& src) {
  ZSTD_frameHeader header;
  return GetFrameHeader(src, header);
}

std::optional<Position> ZstdUncompressedSize(Reader& src) {
  ZSTD_frameHeader header;
  if (ABSL_PREDICT_FALSE(!GetFrameHeader(src, header))) return std::nullopt;
  if (header.frameType == ZSTD_skippableFrame) return 0;
  if (header.frameContentSize == ZSTD_CONTENTSIZE_UNKNOWN) return std::nullopt;
  return IntCast<Position>(header.frameContentSize);
}

std::optional<uint32_t> ZstdDictId(Reader& src) {
  ZSTD_frameHeader header;
  if (ABSL_PREDICT_FALSE(!GetFrameHeader(src, header))) return std::nullopt;
  return IntCast<uint32_t>(header.dictID);
}

}  // namespace riegeli
