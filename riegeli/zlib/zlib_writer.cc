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
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/recycling_pool.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/zlib/zlib_reader.h"
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

void ZlibWriterBase::Initialize(Writer* dest, int compression_level) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of ZlibWriter: null Writer pointer";
  if (ABSL_PREDICT_FALSE(!dest->ok())) {
    FailWithoutAnnotation(AnnotateOverDest(dest->status()));
    return;
  }
  initial_compressed_pos_ = dest->pos();
  compressor_ =
      KeyedRecyclingPool<z_stream, ZStreamKey, ZStreamDeleter>::global().Get(
          ZStreamKey{compression_level, window_bits_},
          [&] {
            std::unique_ptr<z_stream, ZStreamDeleter> ptr(new z_stream());
            const int zlib_code =
                deflateInit2(ptr.get(), compression_level, Z_DEFLATED,
                             window_bits_, 8, Z_DEFAULT_STRATEGY);
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
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::DoneBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!ok())) return;
  Writer& dest = *dest_writer();
  WriteInternal(src, dest, Z_FINISH);
}

void ZlibWriterBase::Done() {
  BufferedWriter::Done();
  compressor_.reset();
  dictionary_ = ZlibDictionary();
  associated_reader_.Reset();
}

bool ZlibWriterBase::FailOperation(absl::string_view operation, int zlib_code) {
  RIEGELI_ASSERT(is_open())
      << "Failed precondition of ZlibWriterBase::FailOperation(): "
         "Object closed";
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
  return Fail(absl::InternalError(message));
}

absl::Status ZlibWriterBase::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) {
    Writer& dest = *dest_writer();
    status = dest.AnnotateStatus(std::move(status));
  }
  // The status might have been annotated by `*dest->writer()` with the
  // compressed position. Clarify that the current position is the uncompressed
  // position instead of delegating to `BufferedWriter::AnnotateStatusImpl()`.
  return AnnotateOverDest(std::move(status));
}

absl::Status ZlibWriterBase::AnnotateOverDest(absl::Status status) {
  if (is_open()) {
    return Annotate(status, absl::StrCat("at uncompressed byte ", pos()));
  }
  return status;
}

bool ZlibWriterBase::WriteInternal(absl::string_view src) {
  RIEGELI_ASSERT(!src.empty())
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "nothing to write";
  RIEGELI_ASSERT(ok())
      << "Failed precondition of BufferedWriter::WriteInternal(): " << status();
  Writer& dest = *dest_writer();
  return WriteInternal(src, dest, Z_NO_FLUSH);
}

inline bool ZlibWriterBase::WriteInternal(absl::string_view src, Writer& dest,
                                          int flush) {
  RIEGELI_ASSERT(ok())
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
    if (ABSL_PREDICT_FALSE(!dest.Push())) {
      return FailWithoutAnnotation(AnnotateOverDest(dest.status()));
    }
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
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::DoneBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Writer& dest = *dest_writer();
  return WriteInternal(src, dest, Z_SYNC_FLUSH);
}

bool ZlibWriterBase::SupportsReadMode() {
  Writer* const dest = dest_writer();
  return dest != nullptr && dest->SupportsReadMode();
}

Reader* ZlibWriterBase::ReadModeBehindBuffer(Position initial_pos) {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::ReadModeBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!ZlibWriterBase::FlushBehindBuffer(
          absl::string_view(), FlushType::kFromObject))) {
    return nullptr;
  }
  Writer& dest = *dest_writer();
  Reader* const compressed_reader = dest.ReadMode(initial_compressed_pos_);
  if (ABSL_PREDICT_FALSE(compressed_reader == nullptr)) {
    FailWithoutAnnotation(AnnotateOverDest(dest.status()));
    return nullptr;
  }
  ZlibReader<>* const reader = associated_reader_.ResetReader(
      compressed_reader,
      ZlibReaderBase::Options()
          .set_window_log(window_bits_ < 0 ? -window_bits_ : window_bits_ & 15)
          .set_header(window_bits_ < 0 ? ZlibReaderBase::Header::kRaw
                                       : static_cast<ZlibReaderBase::Header>(
                                             window_bits_ & ~15))
          .set_dictionary(dictionary_)
          .set_buffer_options(buffer_options()));
  reader->Seek(initial_pos);
  return reader;
}

}  // namespace riegeli
