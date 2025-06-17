// Copyright 2023 Google LLC
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

#include "riegeli/xz/xz_reader.h"

#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <memory>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "lzma.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/recycling_pool.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/xz/xz_error.h"

namespace riegeli {

void XzReaderBase::Initialize(Reader* src) {
  RIEGELI_ASSERT_NE(src, nullptr)
      << "Failed precondition of XzReader: null Reader pointer";
  if (ABSL_PREDICT_FALSE(!src->ok()) && src->available() == 0) {
    FailWithoutAnnotation(AnnotateOverSrc(src->status()));
    return;
  }
  initial_compressed_pos_ = src->pos();
  InitializeDecompressor();
}

inline void XzReaderBase::InitializeDecompressor() {
  decompressor_ =
      KeyedRecyclingPool<lzma_stream, LzmaStreamKey, LzmaStreamDeleter>::global(
          recycling_pool_options_)
          .Get(LzmaStreamKey(container_),
               [] { return riegeli::Maker<lzma_stream>(); });
  switch (container_) {
    case Container::kXz: {
      const lzma_ret liblzma_code = lzma_stream_decoder(
          decompressor_.get(), std::numeric_limits<uint64_t>::max(), flags_);
      if (ABSL_PREDICT_FALSE(liblzma_code != LZMA_OK)) {
        FailOperation("lzma_stream_decoder()", liblzma_code);
      }
      return;
    }
    case Container::kLzma: {
      const lzma_ret liblzma_code = lzma_alone_decoder(
          decompressor_.get(), std::numeric_limits<uint64_t>::max());
      if (ABSL_PREDICT_FALSE(liblzma_code != LZMA_OK)) {
        FailOperation("lzma_alone_decoder()", liblzma_code);
      }
      return;
    }
    case Container::kXzOrLzma: {
      const lzma_ret liblzma_code = lzma_auto_decoder(
          decompressor_.get(), std::numeric_limits<uint64_t>::max(), flags_);
      if (ABSL_PREDICT_FALSE(liblzma_code != LZMA_OK)) {
        FailOperation("lzma_auto_decoder()", liblzma_code);
      }
      return;
    }
  }
  RIEGELI_ASSUME_UNREACHABLE()
      << "Unknown container format: " << static_cast<int>(container_);
}

void XzReaderBase::Done() {
  if (ABSL_PREDICT_FALSE(TruncatedAtClose())) {
    Reader& src = *SrcReader();
    FailWithoutAnnotation(AnnotateOverSrc(src.AnnotateStatus(
        absl::InvalidArgumentError("Truncated Xz-compressed stream"))));
  }
  BufferedReader::Done();
  decompressor_.reset();
}

inline bool XzReaderBase::FailOperation(absl::string_view operation,
                                        lzma_ret liblzma_code) {
  RIEGELI_ASSERT_NE(liblzma_code, LZMA_OK)
      << "Failed precondition of XzReaderBase::FailOperation(): "
         "liblzma error code not failed";
  RIEGELI_ASSERT(is_open())
      << "Failed precondition of XzReaderBase::FailOperation(): "
         "Object closed";
  return Fail(xz_internal::XzErrorToStatus(operation, liblzma_code));
}

absl::Status XzReaderBase::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) {
    if (ABSL_PREDICT_FALSE(truncated_ && (flags_ & LZMA_CONCATENATED) == 0)) {
      status = Annotate(status, "reading truncated Xz-compressed stream");
    }
    Reader& src = *SrcReader();
    status = src.AnnotateStatus(std::move(status));
  }
  // The status might have been annotated by `src` with the compressed position.
  // Clarify that the current position is the uncompressed position instead of
  // delegating to `BufferedReader::AnnotateStatusImpl()`.
  return AnnotateOverSrc(std::move(status));
}

absl::Status XzReaderBase::AnnotateOverSrc(absl::Status status) {
  if (is_open()) {
    return Annotate(status, absl::StrCat("at uncompressed byte ", pos()));
  }
  return status;
}

inline bool XzReaderBase::TruncatedAtClose() {
  if (!truncated_) return false;
  if ((flags_ & LZMA_CONCATENATED) == 0) return true;
  Reader& src = *SrcReader();
  if (src.pos() == initial_compressed_pos_) {
    // Empty concatenated stream.
    return false;
  }
  // Check if the stream ends cleanly.
  decompressor_->next_out = nullptr;
  decompressor_->avail_out = 0;
  decompressor_->next_in = nullptr;
  decompressor_->avail_in = 0;
  const lzma_ret liblzma_code = lzma_code(decompressor_.get(), LZMA_FINISH);
  switch (liblzma_code) {
    case LZMA_OK:
      RIEGELI_ASSUME_UNREACHABLE()
          << "lzma_code(LZMA_FINISH) with no buffer returned LZMA_OK";
    case LZMA_BUF_ERROR:
      return true;
    case LZMA_STREAM_END:
      return false;
    default:
      FailOperation("lzma_code()", liblzma_code);
      return false;
  }
}

bool XzReaderBase::ReadInternal(size_t min_length, size_t max_length,
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
  max_length = UnsignedMin(max_length,
                           std::numeric_limits<Position>::max() - limit_pos());
  decompressor_->next_out = reinterpret_cast<uint8_t*>(dest);
  for (;;) {
    decompressor_->avail_out = PtrDistance(
        reinterpret_cast<char*>(decompressor_->next_out), dest + max_length);
    decompressor_->next_in = reinterpret_cast<const uint8_t*>(src.cursor());
    decompressor_->avail_in = src.available();
    const lzma_ret liblzma_code = lzma_code(decompressor_.get(), LZMA_RUN);
    src.set_cursor(reinterpret_cast<const char*>(decompressor_->next_in));
    const size_t length_read =
        PtrDistance(dest, reinterpret_cast<char*>(decompressor_->next_out));
    switch (liblzma_code) {
      case LZMA_OK:
        if (length_read >= min_length) break;
        ABSL_FALLTHROUGH_INTENDED;
      case LZMA_BUF_ERROR:
        if (ABSL_PREDICT_FALSE(decompressor_->avail_in > 0)) {
          RIEGELI_ASSERT_EQ(decompressor_->avail_out, 0u)
              << "lzma_code() returned but there are still input data "
                 "and output space";
          RIEGELI_ASSERT_EQ(length_read,
                            std::numeric_limits<Position>::max() - limit_pos())
              << "The position does not overflow but the output buffer is "
                 "full, while less than min_length was output, which is "
                 "impossible because the buffer has size max_length which is "
                 "at least min_length if the position does not overflow";
          move_limit_pos(length_read);
          return FailOverflow();
        }
        if (ABSL_PREDICT_FALSE(!src.Pull())) {
          move_limit_pos(length_read);
          if (ABSL_PREDICT_FALSE(!src.ok())) {
            return FailWithoutAnnotation(AnnotateOverSrc(src.status()));
          }
          truncated_ = true;
          return false;
        }
        continue;
      case LZMA_STREAM_END:
        decompressor_.reset();
        move_limit_pos(length_read);
        // Avoid `BufferedReader` allocating another buffer.
        set_exact_size(limit_pos());
        return length_read >= min_length;
      default:
        FailOperation("lzma_code()", liblzma_code);
        break;
    }
    move_limit_pos(length_read);
    return length_read >= min_length;
  }
}

void XzReaderBase::ExactSizeReached() {
  if (decompressor_ == nullptr) return;
  char buffer[1];
  if (ABSL_PREDICT_FALSE(XzReaderBase::ReadInternal(1, 1, buffer))) {
    decompressor_.reset();
    Fail(absl::FailedPreconditionError(
        "Uncompressed size reached but more data can be decompressed, "
        "which implies that seeking back and reading again encountered "
        "changed Xz-compressed data"));
  }
}

bool XzReaderBase::ToleratesReadingAhead() {
  Reader* const src = SrcReader();
  return src != nullptr && src->ToleratesReadingAhead();
}

bool XzReaderBase::SupportsRewind() {
  Reader* const src = SrcReader();
  return src != nullptr && src->SupportsRewind();
}

bool XzReaderBase::SeekBehindBuffer(Position new_pos) {
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
          absl::DataLossError("Xz-compressed stream got truncated"))));
    }
    InitializeDecompressor();
    if (ABSL_PREDICT_FALSE(!ok())) return false;
    if (new_pos == 0) return true;
  }
  return BufferedReader::SeekBehindBuffer(new_pos);
}

bool XzReaderBase::SupportsNewReader() {
  Reader* const src = SrcReader();
  return src != nullptr && src->SupportsNewReader();
}

std::unique_ptr<Reader> XzReaderBase::NewReaderImpl(Position initial_pos) {
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
      std::make_unique<XzReader<std::unique_ptr<Reader>>>(
          std::move(compressed_reader),
          XzReaderBase::Options()
              .set_container(container_)
              .set_concatenate((flags_ & LZMA_CONCATENATED) != 0)
              .set_buffer_options(buffer_options())
              .set_recycling_pool_options(recycling_pool_options_));
  reader->Seek(initial_pos);
  return reader;
}

bool RecognizeXz(Reader& src) {
  static constexpr char kMagic[] = {'\xfd', '7', 'z', 'X', 'Z', '\x00'};
  return src.Pull(sizeof(kMagic)) &&
         absl::string_view(src.cursor(), sizeof(kMagic)) ==
             absl::string_view(kMagic, sizeof(kMagic));
}

}  // namespace riegeli
