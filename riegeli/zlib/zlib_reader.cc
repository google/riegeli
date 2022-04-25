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

#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/recycling_pool.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/reader.h"
#include "zconf.h"
#include "zlib.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if __cplusplus < 201703
constexpr int ZlibReaderBase::Options::kMinWindowLog;
constexpr int ZlibReaderBase::Options::kMaxWindowLog;
constexpr ZlibReaderBase::Header ZlibReaderBase::Options::kDefaultHeader;
#endif

void ZlibReaderBase::Initialize(Reader* src) {
  RIEGELI_ASSERT(src != nullptr)
      << "Failed precondition of ZlibReader: null Reader pointer";
  if (ABSL_PREDICT_FALSE(!src->ok()) && src->available() == 0) {
    FailWithoutAnnotation(AnnotateOverSrc(src->status()));
    return;
  }
  initial_compressed_pos_ = src->pos();
  InitializeDecompressor();
}

inline void ZlibReaderBase::InitializeDecompressor() {
  decompressor_ = RecyclingPool<z_stream, ZStreamDeleter>::global().Get(
      [&] {
        std::unique_ptr<z_stream, ZStreamDeleter> ptr(new z_stream());
        const int zlib_code = inflateInit2(ptr.get(), window_bits_);
        if (ABSL_PREDICT_FALSE(zlib_code != Z_OK)) {
          FailOperation(absl::StatusCode::kInternal, "inflateInit2()",
                        zlib_code);
        }
        return ptr;
      },
      [&](z_stream* ptr) {
        const int zlib_code = inflateReset2(ptr, window_bits_);
        if (ABSL_PREDICT_FALSE(zlib_code != Z_OK)) {
          FailOperation(absl::StatusCode::kInternal, "inflateReset2()",
                        zlib_code);
        }
      });
}

void ZlibReaderBase::Done() {
  if (ABSL_PREDICT_FALSE(truncated_)) {
    Reader& src = *src_reader();
    FailWithoutAnnotation(AnnotateOverSrc(src.AnnotateStatus(
        absl::InvalidArgumentError("Truncated zlib-compressed stream"))));
  }
  BufferedReader::Done();
  decompressor_.reset();
  dictionary_ = ZlibDictionary();
}

inline bool ZlibReaderBase::FailOperation(absl::StatusCode code,
                                          absl::string_view operation,
                                          int zlib_code) {
  RIEGELI_ASSERT_NE(code, absl::StatusCode::kOk)
      << "Failed precondition of ZlibReaderBase::FailOperation(): "
         "status code not failed";
  RIEGELI_ASSERT(is_open())
      << "Failed precondition of ZlibReaderBase::FailOperation(): "
         "Object closed";
  std::string message = absl::StrCat(operation, " failed");
  const char* details = decompressor_->msg;
  if (details == nullptr) {
    switch (zlib_code) {
      case Z_STREAM_END:
        details = "stream end";
        break;
      case Z_NEED_DICT:
        details = "need dictionary";
        break;
      case Z_ERRNO:
        details = "file error";
        break;
      case Z_STREAM_ERROR:
        details = "stream error";
        break;
      case Z_DATA_ERROR:
        details = "data error";
        break;
      case Z_MEM_ERROR:
        details = "insufficient memory";
        break;
      case Z_BUF_ERROR:
        details = "buffer error";
        break;
      case Z_VERSION_ERROR:
        details = "incompatible version";
        break;
    }
  }
  if (details != nullptr) absl::StrAppend(&message, ": ", details);
  return Fail(absl::Status(code, message));
}

absl::Status ZlibReaderBase::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) {
    if (ABSL_PREDICT_FALSE(truncated_)) {
      status = Annotate(status, "reading truncated zlib-compressed stream");
    }
    Reader& src = *src_reader();
    status = src.AnnotateStatus(std::move(status));
  }
  // The status might have been annotated by `*src->reader()` with the
  // compressed position. Clarify that the current position is the uncompressed
  // position instead of delegating to `BufferedReader::AnnotateStatusImpl()`.
  return AnnotateOverSrc(std::move(status));
}

absl::Status ZlibReaderBase::AnnotateOverSrc(absl::Status status) {
  if (is_open()) {
    return Annotate(status, absl::StrCat("at uncompressed byte ", pos()));
  }
  return status;
}

bool ZlibReaderBase::PullSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Reader::PullSlow(): "
         "enough data available, use Pull() instead";
  // After all data have been decompressed, skip `BufferedReader::PullSlow()`
  // to avoid allocating the buffer in case it was not allocated yet.
  if (ABSL_PREDICT_FALSE(decompressor_ == nullptr)) return false;
  return BufferedReader::PullSlow(min_length, recommended_length);
}

bool ZlibReaderBase::ReadInternal(size_t min_length, size_t max_length,
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
  if (ABSL_PREDICT_FALSE(max_length >
                         std::numeric_limits<Position>::max() - limit_pos())) {
    max_length = std::numeric_limits<Position>::max() - limit_pos();
    if (ABSL_PREDICT_FALSE(max_length < min_length)) return FailOverflow();
  }
  decompressor_->next_out = reinterpret_cast<Bytef*>(dest);
  for (;;) {
    decompressor_->avail_out = SaturatingIntCast<uInt>(PtrDistance(
        reinterpret_cast<char*>(decompressor_->next_out), dest + max_length));
    decompressor_->next_in = const_cast<z_const Bytef*>(
        reinterpret_cast<const Bytef*>(src.cursor()));
    decompressor_->avail_in = SaturatingIntCast<uInt>(src.available());
    if (decompressor_->avail_in > 0) stream_had_data_ = true;
    const int result = inflate(decompressor_.get(), Z_NO_FLUSH);
    src.set_cursor(reinterpret_cast<const char*>(decompressor_->next_in));
    const size_t length_read =
        PtrDistance(dest, reinterpret_cast<char*>(decompressor_->next_out));
    switch (result) {
      case Z_OK:
        if (length_read >= min_length) break;
        ABSL_FALLTHROUGH_INTENDED;
      case Z_BUF_ERROR:
        RIEGELI_ASSERT_EQ(decompressor_->avail_in, 0u)
            << "inflate() returned but there are still input data";
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
          const int zlib_code = inflateReset(decompressor_.get());
          if (ABSL_PREDICT_FALSE(zlib_code != Z_OK)) {
            FailOperation(absl::StatusCode::kInternal, "inflateReset()",
                          zlib_code);
            break;
          }
          stream_had_data_ = false;
          if (length_read >= min_length) break;
          continue;
        }
        decompressor_.reset();
        break;
      case Z_NEED_DICT:
        if (ABSL_PREDICT_TRUE(!dictionary_.empty())) {
          const int zlib_code = inflateSetDictionary(
              decompressor_.get(),
              const_cast<z_const Bytef*>(
                  reinterpret_cast<const Bytef*>(dictionary_.data().data())),
              SaturatingIntCast<uInt>(dictionary_.data().size()));
          if (ABSL_PREDICT_FALSE(zlib_code != Z_OK)) {
            FailOperation(absl::StatusCode::kInvalidArgument,
                          "inflateSetDictionary()", zlib_code);
            break;
          }
          continue;
        }
        ABSL_FALLTHROUGH_INTENDED;
      case Z_DATA_ERROR:
        FailOperation(absl::StatusCode::kInvalidArgument, "inflate()", result);
        break;
      default:
        FailOperation(absl::StatusCode::kInternal, "inflate()", result);
        break;
    }
    move_limit_pos(length_read);
    return length_read >= min_length;
  }
}

bool ZlibReaderBase::SupportsRewind() {
  Reader* const src = src_reader();
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
    Reader& src = *src_reader();
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
  Reader* const src = src_reader();
  return src != nullptr && src->SupportsNewReader();
}

std::unique_ptr<Reader> ZlibReaderBase::NewReaderImpl(Position initial_pos) {
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
      std::make_unique<ZlibReader<std::unique_ptr<Reader>>>(
          std::move(compressed_reader),
          ZlibReaderBase::Options()
              .set_window_log((window_bits_ & 15) == 0
                                  ? absl::nullopt
                                  : absl::make_optional(
                                        window_bits_ < 0 ? -window_bits_
                                                         : window_bits_ & 15))
              .set_header(window_bits_ < 0
                              ? Header::kRaw
                              : static_cast<Header>(window_bits_ & ~15))
              .set_dictionary(dictionary_)
              .set_concatenate(concatenate_)
              .set_buffer_options(buffer_options()));
  reader->Seek(initial_pos);
  return reader;
}

}  // namespace riegeli
