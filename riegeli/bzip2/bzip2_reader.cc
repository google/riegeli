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

#include "riegeli/bzip2/bzip2_reader.h"

#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <memory>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "bzlib.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bzip2/bzip2_error.h"

namespace riegeli {

void Bzip2ReaderBase::Initialize(Reader* src) {
  RIEGELI_ASSERT(src != nullptr)
      << "Failed precondition of Bzip2Reader: null Reader pointer";
  if (ABSL_PREDICT_FALSE(!src->ok()) && src->available() == 0) {
    FailWithoutAnnotation(AnnotateOverSrc(src->status()));
    return;
  }
  initial_compressed_pos_ = src->pos();
  InitializeDecompressor();
}

inline void Bzip2ReaderBase::InitializeDecompressor() {
  decompressor_.reset(new bz_stream());
  const int bzlib_code = BZ2_bzDecompressInit(decompressor_.get(), 0, 0);
  if (ABSL_PREDICT_FALSE(bzlib_code != BZ_OK)) {
    delete decompressor_.release();  // Skip `BZ2_bzDecompressEnd()`.
    FailOperation("BZ2_bzDecompressInit()", bzlib_code);
  }
}

void Bzip2ReaderBase::Done() {
  if (ABSL_PREDICT_FALSE(truncated_)) {
    Reader& src = *SrcReader();
    FailWithoutAnnotation(AnnotateOverSrc(src.AnnotateStatus(
        absl::InvalidArgumentError("Truncated Bzip2-compressed stream"))));
  }
  BufferedReader::Done();
  decompressor_.reset();
}

inline bool Bzip2ReaderBase::FailOperation(absl::string_view operation,
                                           int bzlib_code) {
  RIEGELI_ASSERT(bzlib_code != BZ_OK && bzlib_code != BZ_RUN_OK &&
                 bzlib_code != BZ_FLUSH_OK && bzlib_code != BZ_FINISH_OK)
      << "Failed precondition of Bzip2ReaderBase::FailOperation(): "
         "bzlib error code not failed";
  RIEGELI_ASSERT(is_open())
      << "Failed precondition of Bzip2ReaderBase::FailOperation(): "
         "Object closed";
  return Fail(bzip2_internal::Bzip2ErrorToStatus(operation, bzlib_code));
}

absl::Status Bzip2ReaderBase::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) {
    if (ABSL_PREDICT_FALSE(truncated_)) {
      status = Annotate(status, "reading truncated Bzip2-compressed stream");
    }
    Reader& src = *SrcReader();
    status = src.AnnotateStatus(std::move(status));
  }
  // The status might have been annotated by `src` with the compressed position.
  // Clarify that the current position is the uncompressed position instead of
  // delegating to `BufferedReader::AnnotateStatusImpl()`.
  return AnnotateOverSrc(std::move(status));
}

absl::Status Bzip2ReaderBase::AnnotateOverSrc(absl::Status status) {
  if (is_open()) {
    return Annotate(status, absl::StrCat("at uncompressed byte ", pos()));
  }
  return status;
}

bool Bzip2ReaderBase::ReadInternal(size_t min_length, size_t max_length,
                                   char* dest) {
  RIEGELI_ASSERT_GT(min_length, 0u)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "nothing to read";
  RIEGELI_ASSERT_GE(max_length, min_length)
      << "Failed precondition of BufferedReader::ReadInternal(): "
         "max_length < min_length";
  RIEGELI_ASSERT(ok())
      << "Failed precondition of BufferedReader::ReadInternal(): " << status();
  Reader& src = *SrcReader();
  truncated_ = false;
  max_length = UnsignedMin(max_length,
                           std::numeric_limits<Position>::max() - limit_pos());
  decompressor_->next_out = dest;
  for (;;) {
    decompressor_->avail_out = SaturatingIntCast<unsigned int>(
        PtrDistance(decompressor_->next_out, dest + max_length));
    decompressor_->next_in = const_cast<char*>(src.cursor());
    decompressor_->avail_in = SaturatingIntCast<unsigned int>(src.available());
    if (decompressor_->avail_in > 0) stream_had_data_ = true;
    int bzlib_code = BZ2_bzDecompress(decompressor_.get());
    src.set_cursor(decompressor_->next_in);
    const size_t length_read = PtrDistance(dest, decompressor_->next_out);
    switch (bzlib_code) {
      case BZ_OK:
        if (length_read >= min_length) break;
        if (ABSL_PREDICT_FALSE(decompressor_->avail_in > 0)) {
          RIEGELI_ASSERT_EQ(decompressor_->avail_out, 0u)
              << "BZ2_bzDecompress() returned but there are still input data "
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
          if (ABSL_PREDICT_FALSE(!concatenate_ || stream_had_data_)) {
            truncated_ = true;
          }
          return false;
        }
        continue;
      case BZ_STREAM_END:
        if (concatenate_) {
          bzlib_code = BZ2_bzDecompressEnd(decompressor_.get());
          if (ABSL_PREDICT_FALSE(bzlib_code != BZ_OK)) {
            FailOperation("BZ2_bzDecompressEnd()", bzlib_code);
            break;
          }
          bzlib_code = BZ2_bzDecompressInit(decompressor_.get(), 0, 0);
          if (ABSL_PREDICT_FALSE(bzlib_code != BZ_OK)) {
            delete decompressor_.release();  // Skip `BZ2_bzDecompressEnd()`.
            FailOperation("BZ2_bzDecompressInit()", bzlib_code);
            break;
          }
          stream_had_data_ = false;
          if (length_read >= min_length) break;
          continue;
        }
        decompressor_.reset();
        move_limit_pos(length_read);
        // Avoid `BufferedReader` allocating another buffer.
        set_exact_size(limit_pos());
        return length_read >= min_length;
      default:
        FailOperation("BZ2_bzDecompress()", bzlib_code);
        break;
    }
    move_limit_pos(length_read);
    return length_read >= min_length;
  }
}

void Bzip2ReaderBase::ExactSizeReached() {
  if (decompressor_ == nullptr) return;
  char buffer[1];
  if (ABSL_PREDICT_FALSE(Bzip2ReaderBase::ReadInternal(1, 1, buffer))) {
    decompressor_.reset();
    Fail(absl::FailedPreconditionError(
        "Uncompressed size reached but more data can be decompressed, "
        "which implies that seeking back and reading again encountered "
        "changed Bzip2-compressed data"));
  }
}

bool Bzip2ReaderBase::ToleratesReadingAhead() {
  Reader* const src = SrcReader();
  return src != nullptr && src->ToleratesReadingAhead();
}

bool Bzip2ReaderBase::SupportsRewind() {
  Reader* const src = SrcReader();
  return src != nullptr && src->SupportsRewind();
}

bool Bzip2ReaderBase::SeekBehindBuffer(Position new_pos) {
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
    stream_had_data_ = false;
    set_buffer();
    set_limit_pos(0);
    decompressor_.reset();
    if (ABSL_PREDICT_FALSE(!src.Seek(initial_compressed_pos_))) {
      return FailWithoutAnnotation(AnnotateOverSrc(src.StatusOrAnnotate(
          absl::DataLossError("Bzip2-compressed stream got truncated"))));
    }
    InitializeDecompressor();
    if (ABSL_PREDICT_FALSE(!ok())) return false;
    if (new_pos == 0) return true;
  }
  return BufferedReader::SeekBehindBuffer(new_pos);
}

bool Bzip2ReaderBase::SupportsNewReader() {
  Reader* const src = SrcReader();
  return src != nullptr && src->SupportsNewReader();
}

std::unique_ptr<Reader> Bzip2ReaderBase::NewReaderImpl(Position initial_pos) {
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
      std::make_unique<Bzip2Reader<std::unique_ptr<Reader>>>(
          std::move(compressed_reader),
          Bzip2ReaderBase::Options()
              .set_concatenate(concatenate_)
              .set_buffer_options(buffer_options()));
  reader->Seek(initial_pos);
  return reader;
}

namespace {

inline bool GetByte(Reader& src, size_t offset, uint8_t& value) {
  if (!src.Pull(offset + 1, 8)) return false;
  value = static_cast<uint8_t>(src.cursor()[offset]);
  return true;
}

}  // namespace

bool RecognizeBzip2(Reader& src) {
  // Based on https://github.com/dsnet/compress/blob/master/doc/bzip2-format.pdf
  uint8_t byte;
  // HeaderMagic: "BZh1" to "BZh9".
  if (!(GetByte(src, 0, byte) && byte == 'B' && GetByte(src, 1, byte) &&
        byte == 'Z' && GetByte(src, 2, byte) && byte == 'h' &&
        GetByte(src, 3, byte) && byte >= '1' && byte <= '9' &&
        GetByte(src, 4, byte))) {
    return false;
  }
  switch (byte) {
    case 0x31:
      // BlockMagic: 0x31, 0x41, 0x59, 0x26.
      return GetByte(src, 5, byte) && byte == 0x41 && GetByte(src, 6, byte) &&
             byte == 0x59 && GetByte(src, 7, byte) && byte == 0x26;
    case 0x17:
      // FooterMagic: 0x17, 0x72, 0x45, 0x38.
      return GetByte(src, 5, byte) && byte == 0x72 && GetByte(src, 6, byte) &&
             byte == 0x45 && GetByte(src, 7, byte) && byte == 0x38;
    default:
      return false;
  }
}

}  // namespace riegeli
