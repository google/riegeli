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

#include "riegeli/zlib/zlib_writer.h"

#include <stddef.h>

#include <limits>
#include <memory>
#include <string>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/recycling_pool.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/writer.h"
#include "zconf.h"
#include "zlib.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if __cplusplus < 201703
constexpr int ZlibWriterBase::Options::kMinCompressionLevel;
constexpr int ZlibWriterBase::Options::kMaxCompressionLevel;
constexpr int ZlibWriterBase::Options::kDefaultCompressionLevel;
constexpr int ZlibWriterBase::Options::kMinWindowLog;
constexpr int ZlibWriterBase::Options::kMaxWindowLog;
constexpr int ZlibWriterBase::Options::kDefaultWindowLog;
constexpr ZlibWriterBase::Header ZlibWriterBase::Options::kDefaultHeader;
#endif

void ZlibWriterBase::Initialize(Writer* dest, int compression_level,
                                int window_bits) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of ZlibWriter: null Writer pointer";
  if (ABSL_PREDICT_FALSE(!dest->healthy())) {
    Fail(*dest);
    return;
  }
  // Do not reduce `window_log` based on `size_hint`. An unexpected reduction
  // of `window_log` would break concatenation of compressed streams, because
  // Zlib decompressor rejects `window_log` in a subsequent header greater than
  // in the first header.
  compressor_ =
      KeyedRecyclingPool<z_stream, ZStreamKey, ZStreamDeleter>::global().Get(
          ZStreamKey{compression_level, window_bits},
          [&] {
            std::unique_ptr<z_stream, ZStreamDeleter> ptr(new z_stream());
            const int zlib_code =
                deflateInit2(ptr.get(), compression_level, Z_DEFLATED,
                             window_bits, 8, Z_DEFAULT_STRATEGY);
            if (ABSL_PREDICT_FALSE(zlib_code != Z_OK)) {
              FailOperation("deflateInit2()", zlib_code);
            }
            return ptr;
          },
          [&](z_stream* ptr) {
            const int zlib_code = deflateReset(ptr);
            if (ABSL_PREDICT_FALSE(zlib_code != Z_OK)) {
              FailOperation("deflateReset()", zlib_code);
            }
          });
  if (!dictionary_.empty()) {
    const int zlib_code = deflateSetDictionary(
        compressor_.get(),
        const_cast<z_const Bytef*>(
            reinterpret_cast<const Bytef*>(dictionary_.data().data())),
        SaturatingIntCast<uInt>(dictionary_.data().size()));
    if (ABSL_PREDICT_FALSE(zlib_code != Z_OK)) {
      FailOperation("deflateSetDictionary()", zlib_code);
    }
  }
}

void ZlibWriterBase::DoneBehindBuffer(absl::string_view src) {
  RIEGELI_ASSERT_EQ(buffer_size(), 0u)
      << "Failed precondition of BufferedWriter::DoneBehindBuffer():"
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!healthy())) return;
  Writer& dest = *dest_writer();
  WriteInternal(src, dest, Z_FINISH);
}

void ZlibWriterBase::Done() {
  BufferedWriter::Done();
  compressor_.reset();
}

bool ZlibWriterBase::FailOperation(absl::string_view operation, int zlib_code) {
  RIEGELI_ASSERT(is_open())
      << "Failed precondition of ZlibWriterBase::FailOperation(): "
         "Object closed";
  Writer& dest = *dest_writer();
  std::string message = absl::StrCat(operation, " failed");
  const char* details = compressor_->msg;
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
  return Fail(Annotate(absl::InternalError(message),
                       absl::StrCat("at byte ", dest.pos())));
}

void ZlibWriterBase::DefaultAnnotateStatus() {
  RIEGELI_ASSERT(!not_failed())
      << "Failed precondition of Object::DefaultAnnotateStatus(): "
         "Object not failed";
  if (is_open()) AnnotateStatus(absl::StrCat("at uncompressed byte ", pos()));
}

bool ZlibWriterBase::WriteInternal(absl::string_view src) {
  RIEGELI_ASSERT(!src.empty())
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "nothing to write";
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of BufferedWriter::WriteInternal(): " << status();
  Writer& dest = *dest_writer();
  return WriteInternal(src, dest, Z_NO_FLUSH);
}

inline bool ZlibWriterBase::WriteInternal(absl::string_view src, Writer& dest,
                                          int flush) {
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of ZlibWriterBase::WriteInternal(): " << status();
  if (ABSL_PREDICT_FALSE(src.size() >
                         std::numeric_limits<Position>::max() - start_pos())) {
    return FailOverflow();
  }
  compressor_->next_in =
      const_cast<z_const Bytef*>(reinterpret_cast<const Bytef*>(src.data()));
  for (;;) {
    // If `compressor_->avail_out == 0` then `deflate()` returns `Z_BUF_ERROR`,
    // so `dest.Push()` first.
    if (ABSL_PREDICT_FALSE(!dest.Push())) return Fail(dest);
    size_t avail_in =
        PtrDistance(reinterpret_cast<const char*>(compressor_->next_in),
                    src.data() + src.size());
    int op = flush;
    if (ABSL_PREDICT_FALSE(avail_in > std::numeric_limits<uInt>::max())) {
      avail_in = size_t{std::numeric_limits<uInt>::max()};
      op = Z_NO_FLUSH;
    }
    compressor_->avail_in = IntCast<uInt>(avail_in);
    compressor_->next_out = reinterpret_cast<Bytef*>(dest.cursor());
    compressor_->avail_out = SaturatingIntCast<uInt>(dest.available());
    const int result = deflate(compressor_.get(), op);
    dest.set_cursor(reinterpret_cast<char*>(compressor_->next_out));
    const size_t length_written = PtrDistance(
        src.data(), reinterpret_cast<const char*>(compressor_->next_in));
    switch (result) {
      case Z_OK:
        if (compressor_->avail_out == 0 ||
            ABSL_PREDICT_FALSE(length_written < src.size())) {
          continue;
        }
        break;
      case Z_STREAM_END:
        break;
      case Z_BUF_ERROR:
        RIEGELI_ASSERT_EQ(op, Z_SYNC_FLUSH)
            << "deflate() returned an unexpected Z_BUF_ERROR";
        break;
      default:
        return FailOperation("deflate()", result);
    }
    RIEGELI_ASSERT_EQ(length_written, src.size())
        << "deflate() returned but there are still input data";
    move_start_pos(length_written);
    return true;
  }
}

bool ZlibWriterBase::FlushBehindBuffer(absl::string_view src,
                                       FlushType flush_type) {
  RIEGELI_ASSERT_EQ(buffer_size(), 0u)
      << "Failed precondition of BufferedWriter::DoneBehindBuffer():"
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Writer& dest = *dest_writer();
  return WriteInternal(src, dest, Z_SYNC_FLUSH);
}

}  // namespace riegeli
