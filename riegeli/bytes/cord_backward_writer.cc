// Copyright 2017 Google LLC
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

#include "riegeli/bytes/cord_backward_writer.h"

#include <stddef.h>

#include <cstring>
#include <limits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/memory.h"
#include "riegeli/bytes/backward_writer.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if __cplusplus < 201703
constexpr size_t CordBackwardWriterBase::kShortBufferSize;
#endif

void CordBackwardWriterBase::Done() {
  CordBackwardWriterBase::FlushImpl(FlushType::kFromObject);
  BackwardWriter::Done();
  buffer_ = Buffer();
}

bool CordBackwardWriterBase::PushSlow(size_t min_length,
                                      size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of BackwardWriter::PushSlow(): "
         "enough space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  absl::Cord& dest = *dest_cord();
  RIEGELI_ASSERT_EQ(start_pos(), dest.size())
      << "CordBackwardWriter destination changed unexpectedly";
  if (limit() == short_buffer_) {
    const size_t cursor_index = start_to_cursor();
    if (ABSL_PREDICT_FALSE(
            min_length > std::numeric_limits<size_t>::max() - cursor_index ||
            cursor_index + min_length >
                std::numeric_limits<size_t>::max() - dest.size())) {
      return FailOverflow();
    }
    buffer_.Reset(BufferLength(
        cursor_index + min_length,
        UnsignedMax(SaturatingAdd(cursor_index, recommended_length),
                    start_pos(), min_block_size_),
        max_block_size_, size_hint_, start_pos()));
    const size_t length = UnsignedMin(
        buffer_.capacity(), std::numeric_limits<size_t>::max() - dest.size());
    std::memcpy(buffer_.data() + length - cursor_index, cursor(), cursor_index);
    set_buffer(buffer_.data(), length, cursor_index);
  } else {
    SyncBuffer(dest);
    if (ABSL_PREDICT_FALSE(min_length >
                           std::numeric_limits<size_t>::max() - dest.size())) {
      return FailOverflow();
    }
    buffer_.Reset(BufferLength(
        min_length,
        UnsignedMax(recommended_length, start_pos(), min_block_size_),
        max_block_size_, size_hint_, start_pos()));
    set_buffer(buffer_.data(),
               UnsignedMin(buffer_.capacity(),
                           std::numeric_limits<size_t>::max() - dest.size()));
  }
  return true;
}

bool CordBackwardWriterBase::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Chain): "
         "enough space available, use Write(Chain) instead";
  if (src.size() <= kMaxBytesToCopy) return BackwardWriter::WriteSlow(src);
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  absl::Cord& dest = *dest_cord();
  RIEGELI_ASSERT_EQ(start_pos(), dest.size())
      << "CordBackwardWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  SyncBuffer(dest);
  move_start_pos(src.size());
  src.PrependTo(dest);
  return true;
}

bool CordBackwardWriterBase::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Chain&&): "
         "enough space available, use Write(Chain&&) instead";
  if (src.size() <= kMaxBytesToCopy) {
    // Not `std::move(src)`: forward to
    // `BackwardWriter::WriteSlow(const Chain&)`, because
    // `BackwardWriter::WriteSlow(Chain&&)` would forward to
    // `CordBackwardWriterBase::WriteSlow(const Chain&)`.
    return BackwardWriter::WriteSlow(src);
  }
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  absl::Cord& dest = *dest_cord();
  RIEGELI_ASSERT_EQ(start_pos(), dest.size())
      << "CordBackwardWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  SyncBuffer(dest);
  move_start_pos(src.size());
  std::move(src).PrependTo(dest);
  return true;
}

bool CordBackwardWriterBase::WriteSlow(const absl::Cord& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Cord): "
         "enough space available, use Write(Cord) instead";
  if (src.size() <= kMaxBytesToCopy) return BackwardWriter::WriteSlow(src);
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  absl::Cord& dest = *dest_cord();
  RIEGELI_ASSERT_EQ(start_pos(), dest.size())
      << "CordBackwardWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  SyncBuffer(dest);
  move_start_pos(src.size());
  dest.Prepend(src);
  return true;
}

bool CordBackwardWriterBase::WriteSlow(absl::Cord&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Cord&&): "
         "enough space available, use Write(Cord&&) instead";
  if (src.size() <= kMaxBytesToCopy) {
    // Not `std::move(src)`: forward to
    // `BackwardWriter::WriteSlow(const absl::Cord&)`, because
    // `BackwardWriter::WriteSlow(absl::Cord&&)` would forward to
    // `CordBackwardWriterBase::WriteSlow(const absl::Cord&)`.
    return BackwardWriter::WriteSlow(src);
  }
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  absl::Cord& dest = *dest_cord();
  RIEGELI_ASSERT_EQ(start_pos(), dest.size())
      << "CordBackwardWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  SyncBuffer(dest);
  move_start_pos(src.size());
  dest.Prepend(std::move(src));
  return true;
}

bool CordBackwardWriterBase::WriteZerosSlow(Position length) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of BackwardWriter::WriteZerosSlow(): "
         "enough space available, use WriteZeros() instead";
  if (length <= kMaxBytesToCopy) return BackwardWriter::WriteZerosSlow(length);
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  absl::Cord& dest = *dest_cord();
  RIEGELI_ASSERT_EQ(start_pos(), dest.size())
      << "CordBackwardWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(length > std::numeric_limits<size_t>::max() -
                                      IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  SyncBuffer(dest);
  move_start_pos(length);
  dest.Prepend(CordOfZeros(IntCast<size_t>(length)));
  return true;
}

bool CordBackwardWriterBase::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  absl::Cord& dest = *dest_cord();
  RIEGELI_ASSERT_EQ(start_pos(), dest.size())
      << "CordBackwardWriter destination changed unexpectedly";
  SyncBuffer(dest);
  return true;
}

bool CordBackwardWriterBase::TruncateImpl(Position new_size) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  absl::Cord& dest = *dest_cord();
  RIEGELI_ASSERT_EQ(start_pos(), dest.size())
      << "CordBackwardWriter destination changed unexpectedly";
  if (new_size >= start_pos()) {
    if (ABSL_PREDICT_FALSE(new_size > pos())) return false;
    set_cursor(start() - (new_size - start_pos()));
    return true;
  }
  set_start_pos(new_size);
  dest.RemovePrefix(dest.size() - IntCast<size_t>(new_size));
  set_cursor(start());
  return true;
}

inline void CordBackwardWriterBase::SyncBuffer(absl::Cord& dest) {
  if (limit() == nullptr) return;
  set_start_pos(pos());
  const absl::string_view data(cursor(), start_to_cursor());
  if (limit() == short_buffer_) {
    dest.Prepend(data);
  } else {
    std::move(buffer_).PrependSubstrTo(data, dest);
  }
  set_buffer();
}

}  // namespace riegeli
