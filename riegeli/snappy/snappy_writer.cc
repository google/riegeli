// Copyright 2019 Google LLC
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

#include "riegeli/snappy/snappy_writer.h"

#include <stddef.h>
#include <stdint.h>

#include <cstring>
#include <limits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/byte_fill.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/external_ref.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/snappy/snappy_streams.h"
#include "snappy.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if !__cpp_inline_variables
constexpr size_t SnappyWriterBase::kBlockSize;
#endif

void SnappyWriterBase::Done() {
  if (ABSL_PREDICT_TRUE(ok())) SyncBuffer();
  Writer::Done();
  if (ABSL_PREDICT_TRUE(ok())) {
    Writer& dest = *DestWriter();
    {
      absl::Status status = SnappyCompress(
          ChainReader<>(&uncompressed_), dest,
          SnappyCompressOptions().set_compression_level(compression_level_));
      if (ABSL_PREDICT_FALSE(!status.ok())) {
        FailWithoutAnnotation(std::move(status));
      }
    }
  }
  uncompressed_ = Chain();
  associated_reader_.Reset();
}

inline size_t SnappyWriterBase::MaxBytesToCopy() const {
  const size_t max_bytes_to_copy = ~IntCast<size_t>(pos()) & (kBlockSize - 1);
  if (options_.size_hint() != absl::nullopt &&
      IntCast<size_t>(pos()) < *options_.size_hint()) {
    return UnsignedMin(*options_.size_hint() - IntCast<size_t>(pos()) - 1,
                       max_bytes_to_copy);
  }
  return max_bytes_to_copy;
}

inline bool SnappyWriterBase::SyncBuffer() {
  set_start_pos(pos());
  uncompressed_.RemoveSuffix(available());
  set_buffer();
  if (ABSL_PREDICT_FALSE(IntCast<size_t>(start_pos()) >
                         std::numeric_limits<uint32_t>::max())) {
    return FailOverflow();
  }
  return true;
}

absl::Status SnappyWriterBase::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) {
    Writer& dest = *DestWriter();
    status = dest.AnnotateStatus(std::move(status));
  }
  // The status might have been annotated by `dest` with the compressed
  // position. Clarify that the current position is the uncompressed position
  // instead of delegating to `Writer::AnnotateStatusImpl()`.
  return AnnotateOverDest(std::move(status));
}

absl::Status SnappyWriterBase::AnnotateOverDest(absl::Status status) {
  if (is_open()) {
    return Annotate(status, absl::StrCat("at uncompressed byte ", pos()));
  }
  return status;
}

void SnappyWriterBase::SetWriteSizeHintImpl(
    absl::optional<Position> write_size_hint) {
  if (write_size_hint == absl::nullopt) {
    options_.set_size_hint(absl::nullopt);
  } else {
    options_.set_size_hint(
        SaturatingIntCast<size_t>(SaturatingAdd(pos(), *write_size_hint)));
  }
}

bool SnappyWriterBase::PushSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Writer::PushSlow(): "
         "enough space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (ABSL_PREDICT_FALSE(!SyncBuffer())) return false;
  if (ABSL_PREDICT_FALSE(min_length > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(start_pos()))) {
    return FailOverflow();
  }
  const absl::Span<char> buffer = uncompressed_.AppendFixedBuffer(
      UnsignedMax(
          ApplySizeHint(
              RoundUp<kBlockSize>(IntCast<size_t>(start_pos()) + min_length) -
                  IntCast<size_t>(start_pos()),
              options_.size_hint(), IntCast<size_t>(start_pos())),
          min_length),
      options_);
  set_buffer(buffer.data(), buffer.size());
  return true;
}

bool SnappyWriterBase::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain): "
         "enough space available, use Write(Chain) instead";
  if (src.size() <= MaxBytesToCopy()) return Writer::WriteSlow(src);
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (ABSL_PREDICT_FALSE(!SyncBuffer())) return false;
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<uint32_t>::max() -
                                          IntCast<size_t>(start_pos()))) {
    return FailOverflow();
  }
  move_start_pos(src.size());
  uncompressed_.Append(src, options_);
  return true;
}

bool SnappyWriterBase::WriteSlow(ByteFill src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(ByteFill): "
         "enough space available, use Write(ByteFill) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (ABSL_PREDICT_FALSE(IntCast<size_t>(pos()) >
                             std::numeric_limits<uint32_t>::max() ||
                         src.size() > std::numeric_limits<uint32_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  const size_t first_length = UnsignedMin(
      RoundUp<kBlockSize>(IntCast<size_t>(pos())) - IntCast<size_t>(pos()),
      IntCast<size_t>(src.size()));
  if (first_length > 0) {
    if (ABSL_PREDICT_FALSE(!Push(first_length))) return false;
    std::memset(cursor(), src.fill(), first_length);
    move_cursor(first_length);
    src.Extract(first_length);
  }
  Write(src.Extract(RoundDown<kBlockSize>(IntCast<size_t>(src.size()))));
  const size_t last_length = IntCast<size_t>(src.size());
  if (last_length > 0) {
    if (ABSL_PREDICT_FALSE(!Push(last_length))) return false;
    std::memset(cursor(), src.fill(), last_length);
    move_cursor(last_length);
  }
  return true;
}

bool SnappyWriterBase::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain&&): "
         "enough space available, use Write(Chain&&) instead";
  if (src.size() <= MaxBytesToCopy()) {
    // Not `std::move(src)`: forward to `Writer::WriteSlow(const Chain&)`,
    // because `Writer::WriteSlow(Chain&&)` would forward to
    // `SnappyWriterBase::WriteSlow(const Chain&)`.
    return Writer::WriteSlow(src);
  }
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (ABSL_PREDICT_FALSE(!SyncBuffer())) return false;
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<uint32_t>::max() -
                                          IntCast<size_t>(start_pos()))) {
    return FailOverflow();
  }
  move_start_pos(src.size());
  uncompressed_.Append(std::move(src), options_);
  return true;
}

bool SnappyWriterBase::WriteSlow(const absl::Cord& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Cord): "
         "enough space available, use Write(Cord) instead";
  if (src.size() <= MaxBytesToCopy()) return Writer::WriteSlow(src);
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (ABSL_PREDICT_FALSE(!SyncBuffer())) return false;
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<uint32_t>::max() -
                                          IntCast<size_t>(start_pos()))) {
    return FailOverflow();
  }
  move_start_pos(src.size());
  uncompressed_.Append(src, options_);
  return true;
}

bool SnappyWriterBase::WriteSlow(absl::Cord&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Cord&&): "
         "enough space available, use Write(Cord&&) instead";
  if (src.size() <= MaxBytesToCopy()) {
    // Not `std::move(src)`: forward to `Writer::WriteSlow(const absl::Cord&)`,
    // because `Writer::WriteSlow(absl::Cord&&)` would forward to
    // `SnappyWriterBase::WriteSlow(const absl::Cord&)`.
    return Writer::WriteSlow(src);
  }
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (ABSL_PREDICT_FALSE(!SyncBuffer())) return false;
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<uint32_t>::max() -
                                          IntCast<size_t>(start_pos()))) {
    return FailOverflow();
  }
  move_start_pos(src.size());
  uncompressed_.Append(std::move(src), options_);
  return true;
}

bool SnappyWriterBase::WriteSlow(ExternalRef src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(ExternalRef): "
         "enough space available, use Write(ExternalRef) instead";
  if (src.size() <= MaxBytesToCopy()) return Writer::WriteSlow(std::move(src));
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (ABSL_PREDICT_FALSE(!SyncBuffer())) return false;
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<uint32_t>::max() -
                                          IntCast<size_t>(start_pos()))) {
    return FailOverflow();
  }
  move_start_pos(src.size());
  uncompressed_.Append(std::move(src), options_);
  return true;
}

Reader* SnappyWriterBase::ReadModeImpl(Position initial_pos) {
  if (ABSL_PREDICT_FALSE(!ok())) return nullptr;
  if (ABSL_PREDICT_FALSE(!SyncBuffer())) return nullptr;
  ChainReader<>* const reader = associated_reader_.ResetReader(&uncompressed_);
  reader->Seek(initial_pos);
  return reader;
}

namespace snappy_internal {

absl::Status SnappyCompressImpl(Reader& src, Writer& dest,
                                SnappyCompressOptions options) {
  const absl::optional<Position> size = src.Size();
  if (ABSL_PREDICT_FALSE(size == absl::nullopt)) return src.status();
  if (ABSL_PREDICT_FALSE(*size > std::numeric_limits<uint32_t>::max())) {
    return absl::ResourceExhaustedError(absl::StrCat(
        "Uncompressed data too large for snappy compression: ", *size, " > ",
        std::numeric_limits<uint32_t>::max()));
  }
  ReaderSnappySource source(&src, *size);
  WriterSnappySink sink(&dest);
  snappy::Compress(&source, &sink, {/*level=*/options.compression_level()});
  if (ABSL_PREDICT_FALSE(!dest.ok())) return dest.status();
  if (ABSL_PREDICT_FALSE(!src.ok())) return src.status();
  return absl::OkStatus();
}

}  // namespace snappy_internal

size_t SnappyMaxCompressedSize(size_t uncompressed_size) {
  return snappy::MaxCompressedLength(uncompressed_size);
}

}  // namespace riegeli
