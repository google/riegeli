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

#include <memory>
#include <string>

#include "absl/base/macros.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/recycling_pool.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/buffered_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/zlib/zlib_dictionary.h"
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

void ZlibReaderBase::Initialize(Reader* src, int window_bits) {
  RIEGELI_ASSERT(src != nullptr)
      << "Failed precondition of ZlibReader: null Reader pointer";
  if (ABSL_PREDICT_FALSE(!src->healthy()) && src->available() == 0) {
    Fail(*src);
    return;
  }
  decompressor_ = RecyclingPool<z_stream, ZStreamDeleter>::global().Get(
      [&] {
        std::unique_ptr<z_stream, ZStreamDeleter> ptr(new z_stream());
        const int zlib_code = inflateInit2(ptr.get(), window_bits);
        if (ABSL_PREDICT_FALSE(zlib_code != Z_OK)) {
          FailOperation(absl::StatusCode::kInternal, "inflateInit2()",
                        zlib_code);
        }
        return ptr;
      },
      [&](z_stream* ptr) {
        const int zlib_code = inflateReset2(ptr, window_bits);
        if (ABSL_PREDICT_FALSE(zlib_code != Z_OK)) {
          FailOperation(absl::StatusCode::kInternal, "inflateReset2()",
                        zlib_code);
        }
      });
}

void ZlibReaderBase::Done() {
  if (ABSL_PREDICT_FALSE(truncated_)) {
    Reader& src = *src_reader();
    Fail(Annotate(absl::DataLossError("Truncated zlib-compressed stream"),
                  absl::StrCat("at byte ", src.pos())));
  }
  decompressor_.reset();
  BufferedReader::Done();
}

inline bool ZlibReaderBase::FailOperation(absl::StatusCode code,
                                          absl::string_view operation,
                                          int zlib_code) {
  RIEGELI_ASSERT_NE(code, absl::StatusCode::kOk)
      << "Failed precondition of ZlibReaderBase::FailOperation(): "
         "status code not failed";
  RIEGELI_ASSERT(!closed())
      << "Failed precondition of ZlibReaderBase::FailOperation(): "
         "Object closed";
  Reader& src = *src_reader();
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
  return Fail(Annotate(absl::Status(code, message),
                       absl::StrCat("at byte ", src.pos())));
}

bool ZlibReaderBase::Fail(absl::Status status) {
  RIEGELI_ASSERT(!status.ok())
      << "Failed precondition of Object::Fail(): status not failed";
  return FailWithoutAnnotation(
      Annotate(status, absl::StrCat("at uncompressed byte ", pos())));
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
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of BufferedReader::ReadInternal(): " << status();
  if (ABSL_PREDICT_FALSE(decompressor_ == nullptr)) return false;
  Reader& src = *src_reader();
  truncated_ = false;
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
          if (ABSL_PREDICT_FALSE(!src.healthy())) return Fail(src);
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
            FailOperation(absl::StatusCode::kDataLoss, "inflateSetDictionary()",
                          zlib_code);
            break;
          }
          continue;
        }
        ABSL_FALLTHROUGH_INTENDED;
      case Z_DATA_ERROR:
        FailOperation(absl::StatusCode::kDataLoss, "inflate()", result);
        break;
      default:
        FailOperation(absl::StatusCode::kInternal, "inflate()", result);
        break;
    }
    move_limit_pos(length_read);
    return length_read >= min_length;
  }
}

}  // namespace riegeli
