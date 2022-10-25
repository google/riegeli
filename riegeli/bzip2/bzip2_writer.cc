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

#include "riegeli/bzip2/bzip2_writer.h"

#include <stddef.h>

#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "bzlib.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/object.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffered_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if __cplusplus < 201703
constexpr int Bzip2WriterBase::Options::kMinCompressionLevel;
constexpr int Bzip2WriterBase::Options::kMaxCompressionLevel;
constexpr int Bzip2WriterBase::Options::kDefaultCompressionLevel;
#endif

void Bzip2WriterBase::Initialize(Writer* dest, int compression_level) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of Bzip2Writer: null Writer pointer";
  if (ABSL_PREDICT_FALSE(!dest->ok())) {
    FailWithoutAnnotation(AnnotateOverDest(dest->status()));
    return;
  }
  initial_compressed_pos_ = dest->pos();
  compressor_.reset(new bz_stream());
  const int bzlib_code =
      BZ2_bzCompressInit(compressor_.get(), compression_level, 0, 0);
  if (ABSL_PREDICT_FALSE(bzlib_code != BZ_OK)) {
    delete compressor_.release();  // Skip `BZ2_bzCompressEnd()`.
    FailOperation("BZ2_bzCompressInit()", bzlib_code);
  }
}

void Bzip2WriterBase::DoneBehindBuffer(absl::string_view src) {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::DoneBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!ok())) return;
  Writer& dest = *DestWriter();
  WriteInternal(src, dest, BZ_FINISH);
}

void Bzip2WriterBase::Done() {
  BufferedWriter::Done();
  compressor_.reset();
}

bool Bzip2WriterBase::FailOperation(absl::string_view operation,
                                    int bzlib_code) {
  RIEGELI_ASSERT(is_open())
      << "Failed precondition of Bzip2WriterBase::FailOperation(): "
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
  return Fail(absl::InternalError(message));
}

absl::Status Bzip2WriterBase::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) {
    Writer& dest = *DestWriter();
    status = dest.AnnotateStatus(std::move(status));
  }
  // The status might have been annotated by `*dest->writer()` with the
  // compressed position. Clarify that the current position is the uncompressed
  // position instead of delegating to `BufferedWriter::AnnotateStatusImpl()`.
  return AnnotateOverDest(std::move(status));
}

absl::Status Bzip2WriterBase::AnnotateOverDest(absl::Status status) {
  if (is_open()) {
    return Annotate(status, absl::StrCat("at uncompressed byte ", pos()));
  }
  return status;
}

bool Bzip2WriterBase::WriteInternal(absl::string_view src) {
  RIEGELI_ASSERT(!src.empty())
      << "Failed precondition of BufferedWriter::WriteInternal(): "
         "nothing to write";
  RIEGELI_ASSERT(ok())
      << "Failed precondition of BufferedWriter::WriteInternal(): " << status();
  Writer& dest = *DestWriter();
  return WriteInternal(src, dest, BZ_RUN);
}

inline bool Bzip2WriterBase::WriteInternal(absl::string_view src, Writer& dest,
                                           int flush) {
  RIEGELI_ASSERT(ok())
      << "Failed precondition of Bzip2WriterBase::WriteInternal(): "
      << status();
  if (ABSL_PREDICT_FALSE(src.size() >
                         std::numeric_limits<Position>::max() - start_pos())) {
    return FailOverflow();
  }
  compressor_->next_in = const_cast<char*>(src.data());
  for (;;) {
    // If no progress was made, e.g. `compressor_->avail_out == 0` but
    // `BZ2_bzCompress()` wants to output compressed data, then
    // `BZ2_bzCompress()` returns `BZ_PARAM_ERROR`, so `dest.Push()` first.
    if (ABSL_PREDICT_FALSE(!dest.Push())) {
      return FailWithoutAnnotation(AnnotateOverDest(dest.status()));
    }
    size_t avail_in =
        PtrDistance(compressor_->next_in, src.data() + src.size());
    int action = flush;
    if (ABSL_PREDICT_FALSE(avail_in >
                           std::numeric_limits<unsigned int>::max())) {
      avail_in = size_t{std::numeric_limits<unsigned int>::max()};
      action = BZ_RUN;
    }
    compressor_->avail_in = IntCast<unsigned int>(avail_in);
    compressor_->next_out = dest.cursor();
    compressor_->avail_out = SaturatingIntCast<unsigned int>(dest.available());
    const int bzlib_code = BZ2_bzCompress(compressor_.get(), action);
    dest.set_cursor(compressor_->next_out);
    const size_t length_written = PtrDistance(src.data(), compressor_->next_in);
    switch (bzlib_code) {
      case BZ_RUN_OK:
        if (length_written < src.size()) {
          RIEGELI_ASSERT(compressor_->avail_in == 0 ||
                         compressor_->avail_out == 0)
              << "BZ2_bzCompress() returned but there are still input data "
                 "and output space";
          continue;
        }
        break;
      case BZ_FLUSH_OK:
      case BZ_FINISH_OK:
        RIEGELI_ASSERT_EQ(compressor_->avail_out, 0u)
            << "BZ2_bzCompress() is "
            << (bzlib_code == BZ_FLUSH_OK ? "flushing" : "finishing")
            << " but there is still output space";
        continue;
      case BZ_STREAM_END:
        break;
      default:
        return FailOperation("BZ2_bzCompress()", bzlib_code);
    }
    RIEGELI_ASSERT_EQ(length_written, src.size())
        << "BZ2_bzCompress() returned but there are still input data";
    move_start_pos(length_written);
    return true;
  }
}

bool Bzip2WriterBase::FlushBehindBuffer(absl::string_view src,
                                        FlushType flush_type) {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::DoneBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Writer& dest = *DestWriter();
  return WriteInternal(src, dest, BZ_FLUSH);
}

}  // namespace riegeli
