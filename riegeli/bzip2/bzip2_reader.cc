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
#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "bzlib.h"
#include "riegeli/base/base.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/reader.h"

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
    FailOperation(absl::StatusCode::kInternal, "BZ2_bzDecompressInit()",
                  bzlib_code);
  }
}

void Bzip2ReaderBase::Done() {
  if (ABSL_PREDICT_FALSE(truncated_)) {
    Reader& src = *src_reader();
    FailWithoutAnnotation(AnnotateOverSrc(src.AnnotateStatus(
        absl::InvalidArgumentError("Truncated bzip2-compressed stream"))));
  }
  BufferedReader::Done();
  decompressor_.reset();
}

inline bool Bzip2ReaderBase::FailOperation(absl::StatusCode code,
                                           absl::string_view operation,
                                           int bzlib_code) {
  RIEGELI_ASSERT_NE(code, absl::StatusCode::kOk)
      << "Failed precondition of Bzip2ReaderBase::FailOperation(): "
         "status code not failed";
  RIEGELI_ASSERT(is_open())
      << "Failed precondition of Bzip2ReaderBase::FailOperation(): "
         "Object closed";
  std::string message = absl::StrCat(operation, " failed");
  const char* details = nullptr;
  switch (bzlib_code) {
    case BZ_SEQUENCE_ERROR:
      details = "sequence error";
      break;
    case BZ_PARAM_ERROR:
      details = "parameter error";
      break;
    case BZ_MEM_ERROR:
      details = "memory error";
      break;
    case BZ_DATA_ERROR:
      details = "data error";
      break;
    case BZ_DATA_ERROR_MAGIC:
      details = "data error (magic)";
      break;
    case BZ_IO_ERROR:
      details = "I/O error";
      break;
    case BZ_UNEXPECTED_EOF:
      details = "unexpected EOF";
      break;
    case BZ_OUTBUFF_FULL:
      details = "output buffer full";
      break;
    case BZ_CONFIG_ERROR:
      details = "config error";
      break;
  }
  if (details != nullptr) absl::StrAppend(&message, ": ", details);
  return Fail(absl::Status(code, message));
}

absl::Status Bzip2ReaderBase::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) {
    if (ABSL_PREDICT_FALSE(truncated_)) {
      status = Annotate(status, "reading truncated bzip2-compressed stream");
    }
    Reader& src = *src_reader();
    status = src.AnnotateStatus(std::move(status));
  }
  // The status might have been annotated by `*src->reader()` with the
  // compressed position. Clarify that the current position is the uncompressed
  // position instead of delegating to `BufferedReader::AnnotateStatusImpl()`.
  return AnnotateOverSrc(std::move(status));
}

absl::Status Bzip2ReaderBase::AnnotateOverSrc(absl::Status status) {
  if (is_open()) {
    return Annotate(status, absl::StrCat("at uncompressed byte ", pos()));
  }
  return status;
}

bool Bzip2ReaderBase::PullSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Reader::PullSlow(): "
         "enough data available, use Pull() instead";
  // After all data have been decompressed, skip `BufferedReader::PullSlow()`
  // to avoid allocating the buffer in case it was not allocated yet.
  if (ABSL_PREDICT_FALSE(decompressor_ == nullptr)) return false;
  return BufferedReader::PullSlow(min_length, recommended_length);
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
  if (ABSL_PREDICT_FALSE(decompressor_ == nullptr)) return false;
  Reader& src = *src_reader();
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
            FailOperation(absl::StatusCode::kInternal, "BZ2_bzDecompressEnd()",
                          bzlib_code);
            break;
          }
          bzlib_code = BZ2_bzDecompressInit(decompressor_.get(), 0, 0);
          if (ABSL_PREDICT_FALSE(bzlib_code != BZ_OK)) {
            delete decompressor_.release();  // Skip `BZ2_bzDecompressEnd()`.
            FailOperation(absl::StatusCode::kInternal, "BZ2_bzDecompressInit()",
                          bzlib_code);
            break;
          }
          stream_had_data_ = false;
          if (length_read >= min_length) break;
          continue;
        }
        decompressor_.reset();
        break;
      case BZ_DATA_ERROR:
      case BZ_DATA_ERROR_MAGIC:
        FailOperation(absl::StatusCode::kInvalidArgument, "BZ2_bzDecompress()",
                      bzlib_code);
        break;
      default:
        FailOperation(absl::StatusCode::kInternal, "BZ2_bzDecompress()",
                      bzlib_code);
        break;
    }
    move_limit_pos(length_read);
    return length_read >= min_length;
  }
}

bool Bzip2ReaderBase::ToleratesReadingAhead() {
  Reader* const src = src_reader();
  return src != nullptr && src->ToleratesReadingAhead();
}

bool Bzip2ReaderBase::SupportsRewind() {
  Reader* const src = src_reader();
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
    Reader& src = *src_reader();
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
  Reader* const src = src_reader();
  return src != nullptr && src->SupportsNewReader();
}

std::unique_ptr<Reader> Bzip2ReaderBase::NewReaderImpl(Position initial_pos) {
  if (ABSL_PREDICT_FALSE(!ok())) return nullptr;
  // `NewReaderImpl()` is thread-safe from this point
  // if `src_reader()->SupportsNewReader()`.
  Reader& src = *src_reader();
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
