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

#include <cstring>
#include <limits>
#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/snappy/snappy_streams.h"
#include "snappy.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if __cplusplus < 201703
constexpr size_t SnappyWriterBase::kBlockSize;
#endif

void SnappyWriterBase::Done() {
  if (ABSL_PREDICT_TRUE(healthy())) SyncBuffer();
  Writer::Done();
  if (ABSL_PREDICT_TRUE(healthy())) {
    Writer& dest = *dest_writer();
    {
      absl::Status status = SnappyCompress(ChainReader<>(&uncompressed_), dest);
      if (ABSL_PREDICT_FALSE(!status.ok())) {
        Fail(std::move(status));
      }
    }
  }
}

bool SnappyWriterBase::Fail(absl::Status status) {
  RIEGELI_ASSERT(!status.ok())
      << "Failed precondition of Object::Fail(): status not failed";
  return FailWithoutAnnotation(
      Annotate(status, absl::StrCat("at uncompressed byte ", pos())));
}

bool SnappyWriterBase::PushSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Writer::PushSlow(): "
         "enough space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(min_length > std::numeric_limits<size_t>::max() -
                                          uncompressed_.size())) {
    return FailOverflow();
  }
  SyncBuffer();
  const absl::Span<char> buffer = uncompressed_.AppendFixedBuffer(
      BufferLength(min_length,
                   RoundUp<kBlockSize>(uncompressed_.size() + min_length) -
                       uncompressed_.size(),
                   options_.size_hint(), uncompressed_.size()),
      options_);
  set_buffer(buffer.data(), buffer.size());
  return true;
}

bool SnappyWriterBase::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain): "
         "enough space available, use Write(Chain) instead";
  if (src.size() < MinBytesToShare()) return Writer::WriteSlow(src);
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  SyncBuffer();
  move_start_pos(src.size());
  uncompressed_.Append(src, options_);
  return true;
}

bool SnappyWriterBase::WriteZerosSlow(Position length) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Writer::WriteZerosSlow(): "
         "enough space available, use WriteZeros() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(length > std::numeric_limits<size_t>::max() -
                                      IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  const size_t first_length =
      UnsignedMin(RoundUp<kBlockSize>(pos()) - pos(), IntCast<size_t>(length));
  if (first_length > 0) {
    if (ABSL_PREDICT_FALSE(!Push(first_length))) return false;
    std::memset(cursor(), '\0', first_length);
    move_cursor(first_length);
    length -= first_length;
  }
  const size_t middle_length = RoundDown<kBlockSize>(IntCast<size_t>(length));
  if (middle_length > 0) {
    Write(ChainOfZeros(middle_length));
    length -= middle_length;
  }
  const size_t last_length = IntCast<size_t>(length);
  if (last_length > 0) {
    if (ABSL_PREDICT_FALSE(!Push(last_length))) return false;
    std::memset(cursor(), '\0', last_length);
    move_cursor(last_length);
  }
  return true;
}

bool SnappyWriterBase::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain&&): "
         "enough space available, use Write(Chain&&) instead";
  if (src.size() < MinBytesToShare()) {
    // Not `std::move(src)`: forward to `Writer::WriteSlow(const Chain&)`,
    // because `Writer::WriteSlow(Chain&&)` would forward to
    // `SnappyWriterBase::WriteSlow(const Chain&)`.
    return Writer::WriteSlow(src);
  }
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  SyncBuffer();
  move_start_pos(src.size());
  uncompressed_.Append(std::move(src), options_);
  return true;
}

bool SnappyWriterBase::WriteSlow(const absl::Cord& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Cord): "
         "enough space available, use Write(Cord) instead";
  if (src.size() < MinBytesToShare()) return Writer::WriteSlow(src);
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  SyncBuffer();
  move_start_pos(src.size());
  uncompressed_.Append(src, options_);
  return true;
}

bool SnappyWriterBase::WriteSlow(absl::Cord&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Cord&&): "
         "enough space available, use Write(Cord&&) instead";
  if (src.size() < MinBytesToShare()) {
    // Not `std::move(src)`: forward to `Writer::WriteSlow(const absl::Cord&)`,
    // because `Writer::WriteSlow(absl::Cord&&)` would forward to
    // `SnappyWriterBase::WriteSlow(const absl::Cord&)`.
    return Writer::WriteSlow(src);
  }
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  SyncBuffer();
  move_start_pos(src.size());
  uncompressed_.Append(std::move(src), options_);
  return true;
}

inline size_t SnappyWriterBase::MinBytesToShare() const {
  const Position next_block_begin = RoundUp<kBlockSize>(pos());
  Position length_in_next_block = kBlockSize;
  if (next_block_begin < options_.size_hint() && next_block_begin == pos()) {
    length_in_next_block = UnsignedMin(options_.size_hint() - next_block_begin,
                                       length_in_next_block);
  }
  return IntCast<size_t>(next_block_begin + length_in_next_block - pos());
}

inline void SnappyWriterBase::SyncBuffer() {
  set_start_pos(pos());
  uncompressed_.RemoveSuffix(available());
  set_buffer();
}

namespace internal {

absl::Status SnappyCompressImpl(Reader& src, Writer& dest,
                                SnappyCompressOptions options) {
  ReaderSnappySource source(&src, options.assumed_size());
  WriterSnappySink sink(&dest);
  snappy::Compress(&source, &sink);
  if (ABSL_PREDICT_FALSE(!dest.healthy())) return dest.status();
  if (ABSL_PREDICT_FALSE(!src.healthy())) return src.status();
  return absl::OkStatus();
}

}  // namespace internal

size_t SnappyMaxCompressedSize(size_t uncompressed_size) {
  return snappy::MaxCompressedLength(uncompressed_size);
}

}  // namespace riegeli
