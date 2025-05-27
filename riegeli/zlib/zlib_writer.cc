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
#include <utility>

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
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/zlib/zlib_error.h"
#include "riegeli/zlib/zlib_reader.h"
#include "zconf.h"
#include "zlib.h"

namespace riegeli {

static_assert(ZlibWriterBase::Options::kMinCompressionLevel == Z_NO_COMPRESSION,
              "Mismatched constant");
static_assert(ZlibWriterBase::Options::kMaxCompressionLevel ==
                  Z_BEST_COMPRESSION,
              "Mismatched constant");
static_assert(ZlibWriterBase::Options::kMaxWindowLog == MAX_WBITS,
              "Mismatched constant");
static_assert(ZlibWriterBase::Options::kDefaultWindowLog == MAX_WBITS,
              "Mismatched constant");

void ZlibWriterBase::ZStreamDeleter::operator()(z_stream* ptr) const {
  const int zlib_code = deflateEnd(ptr);
  RIEGELI_ASSERT(zlib_code == Z_OK || zlib_code == Z_DATA_ERROR)
      << "deflateEnd() failed: " << zlib_code;
  delete ptr;
}

void ZlibWriterBase::Initialize(Writer* dest, int compression_level) {
  RIEGELI_ASSERT_NE(dest, nullptr)
      << "Failed precondition of ZlibWriter: null Writer pointer";
  if (ABSL_PREDICT_FALSE(!dest->ok())) {
    FailWithoutAnnotation(AnnotateOverDest(dest->status()));
    return;
  }
  initial_compressed_pos_ = dest->pos();
  compressor_ =
      KeyedRecyclingPool<z_stream, ZStreamKey, ZStreamDeleter>::global(
          recycling_pool_options_)
          .Get(
              ZStreamKey(compression_level, window_bits_),
              [&] {
                auto ptr =
                    riegeli::Maker<z_stream>().UniquePtr<ZStreamDeleter>();
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
  Writer& dest = *DestWriter();
  WriteInternal(src, dest, Z_FINISH);
}

void ZlibWriterBase::Done() {
  BufferedWriter::Done();
  compressor_.reset();
  dictionary_ = ZlibDictionary();
  associated_reader_.Reset();
}

inline bool ZlibWriterBase::FailOperation(absl::string_view operation,
                                          int zlib_code) {
  RIEGELI_ASSERT_NE(zlib_code, Z_OK)
      << "Failed precondition of ZlibWriterBase::FailOperation(): "
         "zlib error code not failed";
  RIEGELI_ASSERT(is_open())
      << "Failed precondition of ZlibWriterBase::FailOperation(): "
         "Object closed";
  return Fail(
      zlib_internal::ZlibErrorToStatus(operation, zlib_code, compressor_->msg));
}

absl::Status ZlibWriterBase::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) {
    Writer& dest = *DestWriter();
    status = dest.AnnotateStatus(std::move(status));
  }
  // The status might have been annotated by `dest` with the compressed
  // position. Clarify that the current position is the uncompressed position
  // instead of delegating to `BufferedWriter::AnnotateStatusImpl()`.
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
  RIEGELI_ASSERT_OK(*this)
      << "Failed precondition of BufferedWriter::WriteInternal()";
  Writer& dest = *DestWriter();
  return WriteInternal(src, dest, Z_NO_FLUSH);
}

inline bool ZlibWriterBase::WriteInternal(absl::string_view src, Writer& dest,
                                          int flush) {
  RIEGELI_ASSERT_OK(*this)
      << "Failed precondition of ZlibWriterBase::WriteInternal()";
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
    const int zlib_code = deflate(compressor_.get(), op);
    dest.set_cursor(reinterpret_cast<char*>(compressor_->next_out));
    const size_t length_written = PtrDistance(
        src.data(), reinterpret_cast<const char*>(compressor_->next_in));
    switch (zlib_code) {
      case Z_OK:
        if (compressor_->avail_out == 0) continue;
        RIEGELI_ASSERT_EQ(compressor_->avail_in, 0u)
            << "deflate() returned but there are still input data "
               "and output space";
        if (ABSL_PREDICT_FALSE(length_written < src.size())) continue;
        break;
      case Z_STREAM_END:
        break;
      case Z_BUF_ERROR:
        RIEGELI_ASSERT_EQ(op, Z_SYNC_FLUSH)
            << "deflate() returned an unexpected Z_BUF_ERROR";
        break;
      default:
        return FailOperation("deflate()", zlib_code);
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
      << "Failed precondition of BufferedWriter::FlushBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Writer& dest = *DestWriter();
  return WriteInternal(src, dest, Z_SYNC_FLUSH);
}

bool ZlibWriterBase::SupportsReadMode() {
  Writer* const dest = DestWriter();
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
  Writer& dest = *DestWriter();
  Reader* const compressed_reader = dest.ReadMode(initial_compressed_pos_);
  if (ABSL_PREDICT_FALSE(compressed_reader == nullptr)) {
    FailWithoutAnnotation(AnnotateOverDest(dest.status()));
    return nullptr;
  }
  ZlibReader<>* const reader = associated_reader_.ResetReader(
      compressed_reader,
      ZlibReaderBase::Options()
          .set_header(window_bits_ < 0 ? ZlibReaderBase::Header::kRaw
                                       : static_cast<ZlibReaderBase::Header>(
                                             window_bits_ & ~15))
          .set_window_log(window_bits_ < 0 ? -window_bits_ : window_bits_ & 15)
          .set_dictionary(dictionary_)
          .set_buffer_options(buffer_options())
          .set_recycling_pool_options(recycling_pool_options_));
  reader->Seek(initial_pos);
  return reader;
}

}  // namespace riegeli
