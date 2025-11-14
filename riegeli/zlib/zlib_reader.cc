// Copyright 2018 Google LLC
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

#include "riegeli/zlib/zlib_reader.h"

#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <memory>
#include <optional>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/recycling_pool.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/endian/endian_reading.h"
#include "riegeli/zlib/zlib_error.h"
#include "zconf.h"
#include "zlib.h"

namespace riegeli {

static_assert(ZlibReaderBase::Options::kMaxWindowLog == MAX_WBITS,
              "Mismatched constant");
static_assert(ZlibReaderBase::Options::kDefaultWindowLog == MAX_WBITS,
              "Mismatched constant");

void ZlibReaderBase::ZStreamDeleter::operator()(void* ptr) const {
  z_stream* const z_stream_ptr = static_cast<z_stream*>(ptr);
  const int zlib_code = inflateEnd(z_stream_ptr);
  RIEGELI_ASSERT_EQ(zlib_code, Z_OK) << "inflateEnd() failed";
  delete z_stream_ptr;
}

void ZlibReaderBase::Initialize(Reader* src) {
  RIEGELI_ASSERT_NE(src, nullptr)
      << "Failed precondition of ZlibReader: null Reader pointer";
  if (ABSL_PREDICT_FALSE(!src->ok()) && src->available() == 0) {
    FailWithoutAnnotation(AnnotateOverSrc(src->status()));
    return;
  }
  initial_compressed_pos_ = src->pos();
  InitializeDecompressor();
}

inline void ZlibReaderBase::InitializeDecompressor() {
  decompressor_ =
      RecyclingPool<void, ZStreamDeleter>::global(recycling_pool_options_)
          .Get(
              [&] {
                auto ptr =
                    riegeli::Maker<z_stream>().UniquePtr<ZStreamDeleter>();
                const int zlib_code = inflateInit2(ptr.get(), window_bits_);
                if (ABSL_PREDICT_FALSE(zlib_code != Z_OK)) {
                  FailOperation("inflateInit2()", zlib_code);
                }
                return ptr;
              },
              [&](void* ptr) {
                z_stream* const z_stream_ptr = static_cast<z_stream*>(ptr);
                const int zlib_code = inflateReset2(z_stream_ptr, window_bits_);
                if (ABSL_PREDICT_FALSE(zlib_code != Z_OK)) {
                  FailOperation("inflateReset2()", zlib_code);
                }
              });
}

void ZlibReaderBase::Done() {
  if (ABSL_PREDICT_FALSE(truncated_)) {
    Reader& src = *SrcReader();
    FailWithoutAnnotation(AnnotateOverSrc(src.AnnotateStatus(
        absl::InvalidArgumentError("Truncated Zlib-compressed stream"))));
  }
  BufferedReader::Done();
  decompressor_.reset();
  dictionary_ = ZlibDictionary();
}

inline bool ZlibReaderBase::FailOperation(absl::string_view operation,
                                          int zlib_code) {
  RIEGELI_ASSERT_NE(zlib_code, Z_OK)
      << "Failed precondition of ZlibReaderBase::FailOperation(): "
         "zlib error code not failed";
  RIEGELI_ASSERT(is_open())
      << "Failed precondition of ZlibReaderBase::FailOperation(): "
         "Object closed";
  z_stream* const z_stream_ptr = static_cast<z_stream*>(decompressor_.get());
  return Fail(zlib_internal::ZlibErrorToStatus(operation, zlib_code,
                                               z_stream_ptr->msg));
}

absl::Status ZlibReaderBase::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) {
    if (ABSL_PREDICT_FALSE(truncated_)) {
      status = Annotate(status, "reading truncated Zlib-compressed stream");
    }
    Reader& src = *SrcReader();
    status = src.AnnotateStatus(std::move(status));
  }
  // The status might have been annotated by `src` with the compressed position.
  // Clarify that the current position is the uncompressed position instead of
  // delegating to `BufferedReader::AnnotateStatusImpl()`.
  return AnnotateOverSrc(std::move(status));
}

absl::Status ZlibReaderBase::AnnotateOverSrc(absl::Status status) {
  if (is_open()) {
    return Annotate(status, absl::StrCat("at uncompressed byte ", pos()));
  }
  return status;
}

bool ZlibReaderBase::ReadInternal(size_t min_length, size_t max_length,
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
  z_stream* const z_stream_ptr = static_cast<z_stream*>(decompressor_.get());
  z_stream_ptr->next_out = reinterpret_cast<Bytef*>(dest);
  for (;;) {
    z_stream_ptr->avail_out = SaturatingIntCast<uInt>(PtrDistance(
        reinterpret_cast<char*>(z_stream_ptr->next_out), dest + max_length));
    z_stream_ptr->next_in = const_cast<z_const Bytef*>(
        reinterpret_cast<const Bytef*>(src.cursor()));
    z_stream_ptr->avail_in = SaturatingIntCast<uInt>(src.available());
    if (z_stream_ptr->avail_in > 0) stream_had_data_ = true;
    int zlib_code = inflate(z_stream_ptr, Z_NO_FLUSH);
    src.set_cursor(reinterpret_cast<const char*>(z_stream_ptr->next_in));
    const size_t length_read =
        PtrDistance(dest, reinterpret_cast<char*>(z_stream_ptr->next_out));
    switch (zlib_code) {
      case Z_OK:
        if (length_read >= min_length) break;
        ABSL_FALLTHROUGH_INTENDED;
      case Z_BUF_ERROR:
        if (ABSL_PREDICT_FALSE(z_stream_ptr->avail_in > 0)) {
          RIEGELI_ASSERT_EQ(z_stream_ptr->avail_out, 0u)
              << "inflate() returned but there are still input data "
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
      case Z_STREAM_END:
        if (concatenate_) {
          const int zlib_code = inflateReset(z_stream_ptr);
          if (ABSL_PREDICT_FALSE(zlib_code != Z_OK)) {
            FailOperation("inflateReset()", zlib_code);
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
      case Z_NEED_DICT:
        if (ABSL_PREDICT_TRUE(!dictionary_.empty())) {
          zlib_code = inflateSetDictionary(
              z_stream_ptr,
              const_cast<z_const Bytef*>(
                  reinterpret_cast<const Bytef*>(dictionary_.data().data())),
              SaturatingIntCast<uInt>(dictionary_.data().size()));
          if (ABSL_PREDICT_FALSE(zlib_code != Z_OK)) {
            FailOperation("inflateSetDictionary()", zlib_code);
            break;
          }
          continue;
        }
        ABSL_FALLTHROUGH_INTENDED;
      default:
        FailOperation("inflate()", zlib_code);
        break;
    }
    move_limit_pos(length_read);
    return length_read >= min_length;
  }
}

void ZlibReaderBase::ExactSizeReached() {
  if (decompressor_ == nullptr) return;
  char buffer[1];
  if (ABSL_PREDICT_FALSE(ZlibReaderBase::ReadInternal(1, 1, buffer))) {
    decompressor_.reset();
    Fail(absl::FailedPreconditionError(
        "Uncompressed size reached but more data can be decompressed, "
        "which implies that seeking back and reading again encountered "
        "changed Zlib-compressed data"));
  }
}

bool ZlibReaderBase::ToleratesReadingAhead() {
  Reader* const src = SrcReader();
  return src != nullptr && src->ToleratesReadingAhead();
}

bool ZlibReaderBase::SupportsRewind() {
  Reader* const src = SrcReader();
  return src != nullptr && src->SupportsRewind();
}

bool ZlibReaderBase::SeekBehindBuffer(Position new_pos) {
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
          absl::DataLossError("Zlib-compressed stream got truncated"))));
    }
    InitializeDecompressor();
    if (ABSL_PREDICT_FALSE(!ok())) return false;
    if (new_pos == 0) return true;
  }
  return BufferedReader::SeekBehindBuffer(new_pos);
}

bool ZlibReaderBase::SupportsNewReader() {
  Reader* const src = SrcReader();
  return src != nullptr && src->SupportsNewReader();
}

std::unique_ptr<Reader> ZlibReaderBase::NewReaderImpl(Position initial_pos) {
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
      std::make_unique<ZlibReader<std::unique_ptr<Reader>>>(
          std::move(compressed_reader),
          ZlibReaderBase::Options()
              .set_header(window_bits_ < 0
                              ? Header::kRaw
                              : static_cast<Header>(window_bits_ & ~15))
              .set_window_log(window_bits_ < 0 ? -window_bits_
                                               : window_bits_ & 15)
              .set_concatenate(concatenate_)
              .set_dictionary(dictionary_)
              .set_buffer_options(buffer_options())
              .set_recycling_pool_options(recycling_pool_options_));
  reader->Seek(initial_pos);
  return reader;
}

bool RecognizeZlib(Reader& src, ZlibReaderBase::Header header,
                   const RecyclingPoolOptions& recycling_pool_options) {
  RIEGELI_ASSERT_NE(header, ZlibReaderBase::Header::kRaw)
      << "Failed precondition of RecognizeZlib(): "
         "Header::kRaw cannot be reliably detected";
  using ZStreamDeleter = ZlibReaderBase::ZStreamDeleter;
  // If `header == Header::kRaw` then `window_bits == -1`, which causes
  // `inflateInit2()` or `inflateReset2()` to fail.
  const int window_bits = static_cast<int>(header);
  int zlib_code;
  const RecyclingPool<z_stream, ZStreamDeleter>::Handle decompressor =
      RecyclingPool<z_stream, ZStreamDeleter>::global(recycling_pool_options)
          .Get(
              [&] {
                auto ptr =
                    riegeli::Maker<z_stream>().UniquePtr<ZStreamDeleter>();
                zlib_code = inflateInit2(ptr.get(), window_bits);
                return ptr;
              },
              [&](z_stream* ptr) {
                zlib_code = inflateReset2(ptr, window_bits);
              });
  if (ABSL_PREDICT_FALSE(zlib_code != Z_OK)) return false;

  char dest[1];
  size_t cursor_index = 0;
  decompressor->next_out = reinterpret_cast<Bytef*>(dest);
  decompressor->avail_out = 1;
  for (;;) {
    decompressor->next_in = const_cast<z_const Bytef*>(
        reinterpret_cast<const Bytef*>(src.cursor() + cursor_index));
    decompressor->avail_in =
        SaturatingIntCast<uInt>(src.available() - cursor_index);
    // `Z_BLOCK` stops after decoding the header.
    switch (inflate(decompressor.get(), Z_BLOCK)) {
      case Z_OK:
        if (
            // Decoded the header.
            (decompressor->data_type & 128) != 0 ||
            // Output a byte. This is impossible if `header != Header::kRaw`;
            // kept for robustness.
            decompressor->avail_out < 1) {
          return true;
        }
        ABSL_FALLTHROUGH_INTENDED;
      case Z_BUF_ERROR:
        RIEGELI_ASSERT_EQ(decompressor->avail_in, 0u)
            << "inflate() returned but there are still input data";
        cursor_index = src.available();
        if (ABSL_PREDICT_FALSE(!src.Pull(cursor_index + 1))) return false;
        continue;
      case Z_STREAM_END:  // This is impossible if `header != Header::kRaw`;
                          // kept for robustness.
      case Z_NEED_DICT:
        return true;
      default:
        return false;
    }
  }
}

std::optional<uint32_t> GzipUncompressedSizeModulo4G(Reader& src) {
  RIEGELI_ASSERT(src.SupportsRandomAccess())
      << "Failed precondition of GzipUncompressedSizeModulo4G(): "
         "Reader does not support random access";
  const std::optional<Position> compressed_size = src.Size();
  if (ABSL_PREDICT_FALSE(compressed_size == std::nullopt ||
                         *compressed_size < 20)) {
    return std::nullopt;
  }
  const Position pos_before = src.pos();
  uint32_t uncompressed_size;
  if (ABSL_PREDICT_FALSE(!src.Seek(*compressed_size - sizeof(uint32_t)) ||
                         !ReadLittleEndian32(src, uncompressed_size) ||
                         !src.Seek(pos_before))) {
    return std::nullopt;
  }
  return uncompressed_size;
}

}  // namespace riegeli
