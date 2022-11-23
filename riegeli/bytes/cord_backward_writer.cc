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
#include "absl/strings/cord_buffer.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/cord_utils.h"
#include "riegeli/base/types.h"
#include "riegeli/base/zeros.h"
#include "riegeli/bytes/backward_writer.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if __cplusplus < 201703
constexpr size_t CordBackwardWriterBase::kCordBufferBlockSize;
constexpr size_t CordBackwardWriterBase::kCordBufferMaxSize;
#endif

void CordBackwardWriterBase::Done() {
  CordBackwardWriterBase::FlushImpl(FlushType::kFromObject);
  BackwardWriter::Done();
  cord_buffer_ = absl::CordBuffer();
  buffer_ = Buffer();
}

inline void CordBackwardWriterBase::SyncBuffer(absl::Cord& dest) {
  if (limit() == nullptr) return;
  set_start_pos(pos());
  const absl::string_view data(cursor(), start_to_cursor());
  if (limit() == cord_buffer_.data()) {
    const size_t prefix_to_remove =
        PtrDistance(cord_buffer_.data(), data.data());
    if (prefix_to_remove == 0) {
      dest.Prepend(std::move(cord_buffer_));
    } else if (!Wasteful(cord_buffer_.capacity(), data.size()) &&
               data.size() > MaxBytesToCopyToCord(dest)) {
      dest.Prepend(std::move(cord_buffer_));
      dest.RemovePrefix(prefix_to_remove);
    } else {
      PrependToBlockyCord(data, dest);
    }
  } else {
    std::move(buffer_).PrependSubstrTo(data, dest);
  }
  set_buffer();
}

void CordBackwardWriterBase::SetWriteSizeHintImpl(
    absl::optional<Position> write_size_hint) {
  size_hint_ =
      write_size_hint == absl::nullopt
          ? absl::nullopt
          : absl::make_optional(SaturatingAdd(pos(), *write_size_hint));
}

bool CordBackwardWriterBase::PushSlow(size_t min_length,
                                      size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of BackwardWriter::PushSlow(): "
         "enough space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (pos() == 0) {
    Position needed_length = UnsignedMax(min_length, recommended_length);
    if (size_hint_ != absl::nullopt) {
      needed_length = UnsignedMax(needed_length, *size_hint_);
    }
    if (needed_length <= cord_buffer_.capacity()) {
      // Use the initial capacity of `cord_buffer_`, even if it is smaller than
      // `min_block_size_`, because this avoids allocation.
      cord_buffer_.SetLength(cord_buffer_.capacity());
      set_buffer(cord_buffer_.data(), cord_buffer_.length());
      return true;
    }
  }
  if (ABSL_PREDICT_FALSE(min_length > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  absl::Cord& dest = *DestCord();
  RIEGELI_ASSERT_EQ(start_pos(), dest.size())
      << "CordBackwardWriter destination changed unexpectedly";
  if (start_to_cursor() >= min_block_size_) SyncBuffer(dest);
  const size_t cursor_index = start_to_cursor();
  const size_t buffer_length = UnsignedClamp(
      UnsignedMax(ApplyWriteSizeHint(UnsignedMax(start_pos(), min_block_size_),
                                     size_hint_, start_pos()),
                  SaturatingAdd(cursor_index, recommended_length)),
      cursor_index + min_length, max_block_size_);
  if (buffer_length <= kCordBufferMaxSize) {
    RIEGELI_ASSERT(cord_buffer_.capacity() < buffer_length ||
                   limit() != cord_buffer_.data())
        << "Failed invariant of CordBackwardWriter: "
           "cord_buffer_ has enough capacity but was used only partially";
    absl::CordBuffer new_cord_buffer =
        cord_buffer_.capacity() >= buffer_length
            ? std::move(cord_buffer_)
            : absl::CordBuffer::CreateWithCustomLimit(kCordBufferBlockSize,
                                                      buffer_length);
    if (new_cord_buffer.capacity() >= cursor_index + min_length) {
      new_cord_buffer.SetLength(
          UnsignedMin(new_cord_buffer.capacity(),
                      std::numeric_limits<size_t>::max() - dest.size()));
      if (
          // `std::memcpy(_, nullptr, 0)` is undefined.
          cursor_index > 0) {
        std::memcpy(
            new_cord_buffer.data() + new_cord_buffer.length() - cursor_index,
            cursor(), cursor_index);
      }
      cord_buffer_ = std::move(new_cord_buffer);
      set_buffer(cord_buffer_.data(), cord_buffer_.length(), cursor_index);
      return true;
    }
  }
  RIEGELI_ASSERT(buffer_.capacity() < buffer_length ||
                 limit() != buffer_.data())
      << "Failed invariant of CordBackwardWriter: "
         "buffer_ has enough capacity but was used only partially";
  Buffer new_buffer = buffer_.capacity() >= buffer_length
                          ? std::move(buffer_)
                          : Buffer(buffer_length);
  const size_t length = UnsignedMin(
      new_buffer.capacity(), std::numeric_limits<size_t>::max() - dest.size());
  if (
      // `std::memcpy(_, nullptr, 0)` is undefined.
      cursor_index > 0) {
    std::memcpy(new_buffer.data() + length - cursor_index, cursor(),
                cursor_index);
  }
  buffer_ = std::move(new_buffer);
  set_buffer(buffer_.data(), length, cursor_index);
  return true;
}

bool CordBackwardWriterBase::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Chain): "
         "enough space available, use Write(Chain) instead";
  if (src.size() <= kMaxBytesToCopy) return BackwardWriter::WriteSlow(src);
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  absl::Cord& dest = *DestCord();
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
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  absl::Cord& dest = *DestCord();
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
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  absl::Cord& dest = *DestCord();
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
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  absl::Cord& dest = *DestCord();
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
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  absl::Cord& dest = *DestCord();
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
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  absl::Cord& dest = *DestCord();
  RIEGELI_ASSERT_EQ(start_pos(), dest.size())
      << "CordBackwardWriter destination changed unexpectedly";
  SyncBuffer(dest);
  return true;
}

bool CordBackwardWriterBase::TruncateImpl(Position new_size) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  absl::Cord& dest = *DestCord();
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

}  // namespace riegeli
