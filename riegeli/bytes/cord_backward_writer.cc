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

#include <limits>
#include <optional>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/strings/cord_buffer.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/byte_fill.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/cord_utils.h"
#include "riegeli/base/external_ref.h"
#include "riegeli/base/null_safe_memcpy.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/backward_writer.h"

namespace riegeli {

void CordBackwardWriterBase::Done() {
  CordBackwardWriterBase::FlushImpl(FlushType::kFromObject);
  BackwardWriter::Done();
  cord_buffer_ = absl::CordBuffer();
  buffer_ = Buffer();
}

inline size_t CordBackwardWriterBase::MaxBytesToCopy() const {
  if (size_hint_ != std::nullopt && pos() < *size_hint_) {
    return UnsignedClamp(*size_hint_ - pos() - 1,
                         cord_internal::kMaxBytesToCopyToEmptyCord,
                         cord_internal::kMaxBytesToCopyToNonEmptyCord);
  }
  if (pos() == 0) return cord_internal::kMaxBytesToCopyToEmptyCord;
  return cord_internal::kMaxBytesToCopyToNonEmptyCord;
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
    } else if (Wasteful(cord_internal::kFlatOverhead + cord_buffer_.capacity(),
                        data.size()) ||
               data.size() <= MaxBytesToCopy()) {
      cord_internal::PrependToBlockyCord(data, dest);
    } else {
      dest.Prepend(std::move(cord_buffer_));
      dest.RemovePrefix(prefix_to_remove);
    }
  } else {
    ExternalRef(std::move(buffer_), data).PrependTo(dest);
  }
  set_buffer();
}

void CordBackwardWriterBase::SetWriteSizeHintImpl(
    std::optional<Position> write_size_hint) {
  if (write_size_hint == std::nullopt) {
    size_hint_ = std::nullopt;
  } else {
    size_hint_ = SaturatingAdd(pos(), *write_size_hint);
  }
}

bool CordBackwardWriterBase::PushSlow(size_t min_length,
                                      size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of BackwardWriter::PushSlow(): "
         "enough space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (pos() == 0) {
    Position needed_length = UnsignedMax(min_length, recommended_length);
    if (size_hint_ != std::nullopt) {
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
  absl::Cord& dest = *DestCord();
  RIEGELI_ASSERT_EQ(start_pos(), dest.size())
      << "CordBackwardWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(min_length > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  if (start_to_cursor() >= min_block_size_) SyncBuffer(dest);
  const size_t cursor_index = start_to_cursor();
  const size_t buffer_length = ApplyBufferConstraints(
      ApplySizeHint(UnsignedMax(start_pos(), min_block_size_), size_hint_,
                    start_pos()),
      cursor_index + min_length,
      SaturatingAdd(cursor_index, recommended_length), max_block_size_);
  if (buffer_length <= cord_internal::kCordBufferMaxSize) {
    RIEGELI_ASSERT(cord_buffer_.capacity() < buffer_length ||
                   limit() != cord_buffer_.data())
        << "Failed invariant of CordBackwardWriter: "
           "cord_buffer_ has enough capacity but was used only partially";
    const size_t predicted_cord_buffer_size =
        cord_internal::CordBufferSizeForCapacity(buffer_length);
    if (predicted_cord_buffer_size >= cursor_index + min_length) {
      // Reuse the existing `cord_buffer_` if it has at least the same capacity
      // as a new one would have.
      absl::CordBuffer new_cord_buffer =
          cord_buffer_.capacity() >= predicted_cord_buffer_size
              ? std::move(cord_buffer_)
              : absl::CordBuffer::CreateWithCustomLimit(
                    cord_internal::kCordBufferBlockSize, buffer_length);
      if (ABSL_PREDICT_FALSE(new_cord_buffer.capacity() <
                             cursor_index + min_length)) {
        // The size prediction turned out to be wrong, and the actual size is
        // insufficient even for what is required. Ignore `new_cord_buffer`.
      } else {
        new_cord_buffer.SetLength(
            UnsignedMin(new_cord_buffer.capacity(),
                        std::numeric_limits<size_t>::max() - dest.size()));
        riegeli::null_safe_memcpy(
            new_cord_buffer.data() + new_cord_buffer.length() - cursor_index,
            cursor(), cursor_index);
        cord_buffer_ = std::move(new_cord_buffer);
        set_buffer(cord_buffer_.data(), cord_buffer_.length(), cursor_index);
        return true;
      }
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
  riegeli::null_safe_memcpy(new_buffer.data() + length - cursor_index, cursor(),
                            cursor_index);
  buffer_ = std::move(new_buffer);
  set_buffer(buffer_.data(), length, cursor_index);
  return true;
}

bool CordBackwardWriterBase::WriteSlow(ExternalRef src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(ExternalRef): "
         "enough space available, use Write(ExternalRef) instead";
  if (src.size() <= MaxBytesToCopy()) {
    return BackwardWriter::WriteSlow(std::move(src));
  }
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  absl::Cord& dest = *DestCord();
  RIEGELI_ASSERT_EQ(start_pos(), dest.size())
      << "CordBackwardWriter destination changed unexpectedly";
  SyncBuffer(dest);
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(start_pos()))) {
    return FailOverflow();
  }
  move_start_pos(src.size());
  std::move(src).PrependTo(dest);
  return true;
}

bool CordBackwardWriterBase::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Chain): "
         "enough space available, use Write(Chain) instead";
  if (src.size() <= MaxBytesToCopy()) return BackwardWriter::WriteSlow(src);
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  absl::Cord& dest = *DestCord();
  RIEGELI_ASSERT_EQ(start_pos(), dest.size())
      << "CordBackwardWriter destination changed unexpectedly";
  SyncBuffer(dest);
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(start_pos()))) {
    return FailOverflow();
  }
  move_start_pos(src.size());
  src.PrependTo(dest);
  return true;
}

bool CordBackwardWriterBase::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Chain&&): "
         "enough space available, use Write(Chain&&) instead";
  if (src.size() <= MaxBytesToCopy()) {
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
  SyncBuffer(dest);
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(start_pos()))) {
    return FailOverflow();
  }
  move_start_pos(src.size());
  std::move(src).PrependTo(dest);
  return true;
}

bool CordBackwardWriterBase::WriteSlow(const absl::Cord& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Cord): "
         "enough space available, use Write(Cord) instead";
  if (src.size() <= MaxBytesToCopy()) return BackwardWriter::WriteSlow(src);
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  absl::Cord& dest = *DestCord();
  RIEGELI_ASSERT_EQ(start_pos(), dest.size())
      << "CordBackwardWriter destination changed unexpectedly";
  SyncBuffer(dest);
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(start_pos()))) {
    return FailOverflow();
  }
  move_start_pos(src.size());
  dest.Prepend(src);
  return true;
}

bool CordBackwardWriterBase::WriteSlow(absl::Cord&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Cord&&): "
         "enough space available, use Write(Cord&&) instead";
  if (src.size() <= MaxBytesToCopy()) {
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
  SyncBuffer(dest);
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(start_pos()))) {
    return FailOverflow();
  }
  move_start_pos(src.size());
  dest.Prepend(std::move(src));
  return true;
}

bool CordBackwardWriterBase::WriteSlow(ByteFill src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(ByteFill): "
         "enough space available, use Write(ByteFill) instead";
  if (src.size() <= MaxBytesToCopy()) return BackwardWriter::WriteSlow(src);
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  absl::Cord& dest = *DestCord();
  RIEGELI_ASSERT_EQ(start_pos(), dest.size())
      << "CordBackwardWriter destination changed unexpectedly";
  SyncBuffer(dest);
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(start_pos()))) {
    return FailOverflow();
  }
  move_start_pos(src.size());
  src.PrependTo(dest);
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
