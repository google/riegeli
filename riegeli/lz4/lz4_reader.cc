// Copyright 2022 Google LLC
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

// Enables the experimental lz4 API:
//  * `LZ4F_decompress_usingDict()`
#define LZ4F_STATIC_LINKING_ONLY

#include "riegeli/lz4/lz4_reader.h"

#include <stddef.h>

#include <limits>
#include <memory>
#include <optional>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "lz4frame.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/recycling_pool.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

void Lz4ReaderBase::Initialize(Reader* src) {
  RIEGELI_ASSERT_NE(src, nullptr)
      << "Failed precondition of Lz4Reader: null Reader pointer";
  if (ABSL_PREDICT_FALSE(!src->ok()) && src->available() == 0) {
    FailWithoutAnnotation(AnnotateOverSrc(src->status()));
    return;
  }
  initial_compressed_pos_ = src->pos();
  InitializeDecompressor(*src);
}

inline void Lz4ReaderBase::InitializeDecompressor(Reader& src) {
  LZ4F_errorCode_t result = 0;
  decompressor_ = RecyclingPool<LZ4F_dctx, LZ4F_dctxDeleter>::global(
                      recycling_pool_options_)
                      .Get(
                          [&result] {
                            LZ4F_dctx* decompressor = nullptr;
                            result = LZ4F_createDecompressionContext(
                                &decompressor, LZ4F_VERSION);
                            return std::unique_ptr<LZ4F_dctx, LZ4F_dctxDeleter>(
                                decompressor);
                          },
                          [](LZ4F_dctx* decompressor) {
                            LZ4F_resetDecompressionContext(decompressor);
                          });
  if (ABSL_PREDICT_FALSE(LZ4F_isError(result))) {
    Fail(absl::InternalError(
        absl::StrCat("LZ4F_createDecompressionContext() failed: ",
                     LZ4F_getErrorName(result))));
    return;
  }
  ReadHeader(src);
}

inline bool Lz4ReaderBase::ReadHeader(Reader& src) {
  if (ABSL_PREDICT_FALSE(!src.Pull(LZ4F_MIN_SIZE_TO_KNOW_HEADER_LENGTH,
                                   LZ4F_HEADER_SIZE_MAX))) {
    if (ABSL_PREDICT_FALSE(!src.ok())) {
      return FailWithoutAnnotation(AnnotateOverSrc(src.status()));
    }
    if (ABSL_PREDICT_FALSE(!concatenate_)) {
      if (!growing_source_) {
        Fail(absl::InvalidArgumentError("Truncated Lz4-compressed stream"));
      }
      truncated_ = true;
    }
    return false;
  }
  const size_t header_size = LZ4F_headerSize(src.cursor(), src.available());
  if (ABSL_PREDICT_FALSE(LZ4F_isError(header_size))) {
    return Fail(absl::InvalidArgumentError(absl::StrCat(
        "LZ4F_headerSize() failed: ", LZ4F_getErrorName(header_size))));
  }
  if (ABSL_PREDICT_FALSE(!src.Pull(header_size))) {
    if (ABSL_PREDICT_FALSE(!src.ok())) {
      return FailWithoutAnnotation(AnnotateOverSrc(src.status()));
    }
    if (ABSL_PREDICT_FALSE(!concatenate_)) {
      if (!growing_source_) {
        Fail(absl::InvalidArgumentError("Truncated Lz4-compressed stream"));
      }
      truncated_ = true;
    }
    return false;
  }
  LZ4F_frameInfo_t frame_info;
  size_t length = src.available();
  const size_t result = LZ4F_getFrameInfo(decompressor_.get(), &frame_info,
                                          src.cursor(), &length);
  if (ABSL_PREDICT_FALSE(LZ4F_isError(result))) {
    return Fail(absl::InvalidArgumentError(absl::StrCat(
        "LZ4F_getFrameInfo() failed: ", LZ4F_getErrorName(result))));
  }
  src.move_cursor(length);
  header_read_ = true;

  if (!concatenate_ && frame_info.contentSize > 0) {
    set_exact_size(frame_info.contentSize);
  }
  if (frame_info.dictID > 0 &&
      ABSL_PREDICT_FALSE(frame_info.dictID != dictionary_.dict_id())) {
    if (dictionary_.empty()) {
      return Fail(absl::InvalidArgumentError(absl::StrCat(
          "Missing dictionary: expected dict_id ", frame_info.dictID)));
    }
    if (dictionary_.dict_id() > 0) {
      return Fail(absl::InvalidArgumentError(
          absl::StrCat("Wrong dictionary: expected dict_id ", frame_info.dictID,
                       ", have dict_id ", dictionary_.dict_id())));
    }
    // Dictionary is present but has `dict_id() == 0`. Hopefully it is the right
    // dictionary.
  }
  return true;
}

void Lz4ReaderBase::Done() {
  if (ABSL_PREDICT_FALSE(truncated_) && growing_source_) {
    Reader& src = *SrcReader();
    FailWithoutAnnotation(AnnotateOverSrc(src.AnnotateStatus(
        absl::InvalidArgumentError("Truncated Lz4-compressed stream"))));
  }
  BufferedReader::Done();
  decompressor_.reset();
  dictionary_ = Lz4Dictionary();
}

absl::Status Lz4ReaderBase::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) {
    if (ABSL_PREDICT_FALSE(truncated_)) {
      status = Annotate(status, "reading truncated Lz4-compressed stream");
    }
    Reader& src = *SrcReader();
    status = src.AnnotateStatus(std::move(status));
  }
  // The status might have been annotated by `src` with the compressed position.
  // Clarify that the current position is the uncompressed position instead of
  // delegating to `BufferedReader::AnnotateStatusImpl()`.
  return AnnotateOverSrc(std::move(status));
}

absl::Status Lz4ReaderBase::AnnotateOverSrc(absl::Status status) {
  if (is_open()) {
    return Annotate(status, absl::StrCat("at uncompressed byte ", pos()));
  }
  return status;
}

bool Lz4ReaderBase::ReadInternal(size_t min_length, size_t max_length,
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
  if (ABSL_PREDICT_FALSE(!header_read_)) {
    if (ABSL_PREDICT_FALSE(!ReadHeader(src))) return false;
  }
  LZ4F_decompressOptions_t decompress_options{};
  size_t effective_min_length = min_length;
  if (!growing_source_ && exact_size() != std::nullopt &&
      max_length >= SaturatingSub(*exact_size(), limit_pos())) {
    // Avoid a memory copy from an internal buffer of the Lz4 engine to `dest`
    // by promising to decompress all remaining data to `dest`.
    decompress_options.stableDst = 1;
    effective_min_length = std::numeric_limits<size_t>::max();
  }
  max_length = UnsignedMin(max_length,
                           std::numeric_limits<Position>::max() - limit_pos());
  for (;;) {
    size_t src_length = src.available();
    size_t dest_length = max_length;
    const size_t result = LZ4F_decompress_usingDict(
        decompressor_.get(), dest, &dest_length, src.cursor(), &src_length,
        dictionary_.data().data(), dictionary_.data().size(),
        &decompress_options);
    src.move_cursor(src_length);
    move_limit_pos(dest_length);
    if (ABSL_PREDICT_FALSE(result == 0)) {
      if (concatenate_) {
        if (dest_length >= min_length) return true;
        dest += dest_length;
        min_length -= dest_length;
        max_length -= dest_length;
        effective_min_length -= dest_length;
        header_read_ = false;
        if (ABSL_PREDICT_FALSE(!ReadHeader(src))) return false;
        continue;
      }
      decompressor_.reset();
      // Avoid `BufferedReader` allocating another buffer.
      set_exact_size(limit_pos());
      return dest_length >= min_length;
    }
    if (ABSL_PREDICT_FALSE(LZ4F_isError(result))) {
      Fail(absl::InvalidArgumentError(absl::StrCat(
          "LZ4F_decompress_usingDict() failed: ", LZ4F_getErrorName(result))));
      return dest_length >= min_length;
    }
    if (dest_length >= effective_min_length) return true;
    if (ABSL_PREDICT_FALSE(src.available() > 0)) {
      RIEGELI_ASSERT_EQ(dest_length, max_length)
          << "LZ4F_decompress_usingDict() returned but there are still "
             "input data and output space";
      RIEGELI_ASSERT_EQ(dest_length,
                        std::numeric_limits<Position>::max() - limit_pos())
          << "The position does not overflow but the output buffer is full, "
             "while less than min_length was output, which implies that "
             "LZ4F_decompress_usingDict() wants to output more than the "
             "expected decompressed size to a flat buffer";
      return FailOverflow();
    }
    if (ABSL_PREDICT_FALSE(!src.Pull(1, result))) {
      if (ABSL_PREDICT_FALSE(!src.ok())) {
        FailWithoutAnnotation(AnnotateOverSrc(src.status()));
      } else {
        if (!growing_source_) {
          Fail(absl::InvalidArgumentError("Truncated Lz4-compressed stream"));
        }
        truncated_ = true;
      }
      return dest_length >= min_length;
    }
    dest += dest_length;
    min_length = SaturatingSub(min_length, dest_length);
    max_length -= dest_length;
    effective_min_length -= dest_length;
  }
}

void Lz4ReaderBase::ExactSizeReached() {
  if (decompressor_ == nullptr) return;
  char buffer[1];
  if (ABSL_PREDICT_FALSE(Lz4ReaderBase::ReadInternal(1, 1, buffer))) {
    decompressor_.reset();
    Fail(absl::FailedPreconditionError(
        "Uncompressed size reached but more data can be decompressed, "
        "which implies that seeking back and reading again encountered "
        "changed Lz4-compressed data"));
  }
}

bool Lz4ReaderBase::ToleratesReadingAhead() {
  Reader* const src = SrcReader();
  return src != nullptr && src->ToleratesReadingAhead();
}

bool Lz4ReaderBase::SupportsRewind() {
  Reader* const src = SrcReader();
  return src != nullptr && src->SupportsRewind();
}

bool Lz4ReaderBase::SeekBehindBuffer(Position new_pos) {
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
          absl::DataLossError("Lz4-compressed stream got truncated"))));
    }
    InitializeDecompressor(src);
    if (ABSL_PREDICT_FALSE(!ok())) return false;
    if (new_pos == 0) return true;
  }
  return BufferedReader::SeekBehindBuffer(new_pos);
}

bool Lz4ReaderBase::SupportsNewReader() {
  Reader* const src = SrcReader();
  return src != nullptr && src->SupportsNewReader();
}

std::unique_ptr<Reader> Lz4ReaderBase::NewReaderImpl(Position initial_pos) {
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
      std::make_unique<Lz4Reader<std::unique_ptr<Reader>>>(
          std::move(compressed_reader),
          Lz4ReaderBase::Options()
              .set_growing_source(growing_source_)
              .set_concatenate(concatenate_)
              .set_dictionary(dictionary_)
              .set_buffer_options(buffer_options())
              .set_recycling_pool_options(recycling_pool_options_));
  reader->Seek(initial_pos);
  return reader;
}

namespace lz4_internal {

inline bool GetFrameInfo(Reader& src, LZ4F_frameInfo_t& frame_info,
                         const RecyclingPoolOptions& recycling_pool_options) {
  using LZ4F_dctxDeleter = Lz4ReaderBase::LZ4F_dctxDeleter;
  RecyclingPool<LZ4F_dctx, LZ4F_dctxDeleter>::Handle decompressor;
  {
    LZ4F_errorCode_t result = 0;
    decompressor =
        RecyclingPool<LZ4F_dctx, LZ4F_dctxDeleter>::global(
            recycling_pool_options)
            .Get(
                [&result] {
                  LZ4F_dctx* decompressor = nullptr;
                  result = LZ4F_createDecompressionContext(&decompressor,
                                                           LZ4F_VERSION);
                  return std::unique_ptr<LZ4F_dctx, LZ4F_dctxDeleter>(
                      decompressor);
                },
                [](LZ4F_dctx* decompressor) {
                  LZ4F_resetDecompressionContext(decompressor);
                });
    if (ABSL_PREDICT_FALSE(LZ4F_isError(result))) return false;
  }
  if (ABSL_PREDICT_FALSE(!src.Pull(LZ4F_MIN_SIZE_TO_KNOW_HEADER_LENGTH,
                                   LZ4F_HEADER_SIZE_MAX))) {
    return false;
  }
  const size_t header_size = LZ4F_headerSize(src.cursor(), src.available());
  if (ABSL_PREDICT_FALSE(LZ4F_isError(header_size))) return false;
  if (ABSL_PREDICT_FALSE(!src.Pull(header_size))) return false;
  size_t length;
  const size_t result =
      LZ4F_getFrameInfo(decompressor.get(), &frame_info, src.cursor(), &length);
  return !LZ4F_isError(result);
}

}  // namespace lz4_internal

bool RecognizeLz4(Reader& src,
                  const RecyclingPoolOptions& recycling_pool_options) {
  LZ4F_frameInfo_t frame_info;
  return lz4_internal::GetFrameInfo(src, frame_info, recycling_pool_options);
}

std::optional<Position> Lz4UncompressedSize(
    Reader& src, const RecyclingPoolOptions& recycling_pool_options) {
  LZ4F_frameInfo_t frame_info;
  if (!lz4_internal::GetFrameInfo(src, frame_info, recycling_pool_options)) {
    return std::nullopt;
  }
  if (frame_info.contentSize > 0) return frame_info.contentSize;
  return std::nullopt;
}

}  // namespace riegeli
