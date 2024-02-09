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

#include "riegeli/bytes/cord_writer.h"

#include <stddef.h>

#include <cstring>
#include <limits>
#include <memory>
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
#include "riegeli/bytes/cord_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if !__cpp_inline_variables
constexpr size_t CordWriterBase::kCordBufferBlockSize;
constexpr size_t CordWriterBase::kCordBufferMaxSize;
#endif

void CordWriterBase::Done() {
  CordWriterBase::FlushImpl(FlushType::kFromObject);
  Writer::Done();
  cord_buffer_ = absl::CordBuffer();
  buffer_ = Buffer();
  tail_.reset();
  associated_reader_.Reset();
}

inline void CordWriterBase::SyncBuffer(absl::Cord& dest) {
  if (ABSL_PREDICT_FALSE(HasAppendedTail(dest))) {
    ExtractTail(dest);
    return;
  }
  if (start() == nullptr) return;
  ShrinkTail(start_to_cursor());
  set_start_pos(pos());
  const absl::string_view data(start(), start_to_cursor());
  if (start() == cord_buffer_.data()) {
    cord_buffer_.SetLength(data.size());
    if (!Wasteful(kFlatCordOverhead + cord_buffer_.capacity(),
                  cord_buffer_.length())) {
      dest.Append(std::move(cord_buffer_));
    } else {
      AppendToBlockyCord(data, dest);
    }
  } else {
    std::move(buffer_).AppendSubstrTo(data.data(), data.size(), dest);
  }
  set_buffer();
}

inline void CordWriterBase::MoveFromTail(size_t length, absl::Cord& dest) {
  RIEGELI_ASSERT(tail_ != nullptr)
      << "Failed precondition of CordWriterBase::MoveFromTail(): no tail";
  RIEGELI_ASSERT(length <= tail_->size())
      << "Failed precondition of CordWriterBase::MoveFromTail(): "
         "length longer than the tail";
  if (length == tail_->size()) {
    dest.Append(std::move(*tail_));
    tail_->Clear();
    return;
  }
  dest.Append(tail_->Subcord(0, length));
  tail_->RemovePrefix(length);
}

inline void CordWriterBase::MoveToTail(size_t length, absl::Cord& dest) {
  RIEGELI_ASSERT(length <= dest.size())
      << "Failed precondition of CordWriterBase::MoveToTail(): "
         "length longer than the destination";
  if (tail_ == nullptr) tail_ = std::make_unique<absl::Cord>();
  if (length == dest.size()) {
    tail_->Prepend(std::move(dest));
    dest.Clear();
    return;
  }
  tail_->Prepend(dest.Subcord(dest.size() - length, length));
  dest.RemoveSuffix(length);
}

inline bool CordWriterBase::HasAppendedTail(const absl::Cord& dest) const {
  return start_pos() < dest.size();
}

inline void CordWriterBase::ExtractTail(absl::Cord& dest) {
  RIEGELI_ASSERT(HasAppendedTail(dest))
      << "Failed precondition of CordWriterBase::ExtractTail(): "
         "the tail is not appended";
  RIEGELI_ASSERT(start() == nullptr)
      << "Failed invariant of CordWriterBase: "
         "both a buffer and the appended tail are present";
  MoveToTail(dest.size() - IntCast<size_t>(start_pos()), dest);
}

inline void CordWriterBase::AppendTail(absl::Cord& dest) {
  RIEGELI_ASSERT(!HasAppendedTail(dest))
      << "Failed precondition of CordWriterBase::AppendTail(): "
         "the tail is appended";
  if (ABSL_PREDICT_FALSE(tail_ != nullptr)) {
    dest.Append(std::move(*tail_));
    tail_->Clear();
  }
}

inline void CordWriterBase::ShrinkTail(size_t length) {
  if (ABSL_PREDICT_FALSE(tail_ != nullptr)) {
    tail_->RemovePrefix(UnsignedMin(length, tail_->size()));
  }
}

void CordWriterBase::SetWriteSizeHintImpl(
    absl::optional<Position> write_size_hint) {
  size_hint_ =
      write_size_hint == absl::nullopt
          ? absl::nullopt
          : absl::make_optional(SaturatingAdd(pos(), *write_size_hint));
}

bool CordWriterBase::PushSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Writer::PushSlow(): "
         "enough space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  absl::Cord& dest = *DestCord();
  RIEGELI_ASSERT_LE(start_pos(), dest.size())
      << "CordWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(HasAppendedTail(dest))) {
    ExtractTail(dest);
  } else if (pos() == 0) {
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
  if (start_to_cursor() >= min_block_size_) SyncBuffer(dest);
  const size_t cursor_index = start_to_cursor();
  const size_t buffer_length = ApplyBufferConstraints(
      ApplySizeHint(UnsignedMax(start_pos(), min_block_size_), size_hint_,
                    start_pos()),
      cursor_index + min_length,
      SaturatingAdd(cursor_index, recommended_length), max_block_size_);
  if (buffer_length <= kCordBufferMaxSize) {
    RIEGELI_ASSERT(cord_buffer_.capacity() < buffer_length ||
                   start() != cord_buffer_.data())
        << "Failed invariant of CordWriter: "
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
      // `std::memcpy(_, nullptr, 0)` is undefined.
      if (cursor_index > 0) {
        std::memcpy(new_cord_buffer.data(), start(), cursor_index);
      }
      cord_buffer_ = std::move(new_cord_buffer);
      set_buffer(cord_buffer_.data(), cord_buffer_.length(), cursor_index);
      return true;
    }
  }
  RIEGELI_ASSERT(buffer_.capacity() < buffer_length ||
                 start() != buffer_.data())
      << "Failed invariant of CordWriter: "
         "buffer_ has enough capacity but was used only partially";
  Buffer new_buffer = buffer_.capacity() >= buffer_length
                          ? std::move(buffer_)
                          : Buffer(buffer_length);
  // `std::memcpy(_, nullptr, 0)` is undefined.
  if (cursor_index > 0) std::memcpy(new_buffer.data(), start(), cursor_index);
  buffer_ = std::move(new_buffer);
  set_buffer(buffer_.data(),
             UnsignedMin(buffer_.capacity(),
                         std::numeric_limits<size_t>::max() - dest.size()),
             cursor_index);
  return true;
}

bool CordWriterBase::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain): "
         "enough space available, use Write(Chain) instead";
  if (src.size() <= kMaxBytesToCopy) return Writer::WriteSlow(src);
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  absl::Cord& dest = *DestCord();
  RIEGELI_ASSERT_LE(start_pos(), dest.size())
      << "CordWriter destination changed unexpectedly";
  SyncBuffer(dest);
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(start_pos()))) {
    return FailOverflow();
  }
  ShrinkTail(src.size());
  move_start_pos(src.size());
  src.AppendTo(dest);
  return true;
}

bool CordWriterBase::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain&&): "
         "enough space available, use Write(Chain&&) instead";
  if (src.size() <= kMaxBytesToCopy) {
    // Not `std::move(src)`: forward to `Writer::WriteSlow(const Chain&)`,
    // because `Writer::WriteSlow(Chain&&)` would forward to
    // `CordWriterBase::WriteSlow(const Chain&)`.
    return Writer::WriteSlow(src);
  }
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  absl::Cord& dest = *DestCord();
  RIEGELI_ASSERT_LE(start_pos(), dest.size())
      << "CordWriter destination changed unexpectedly";
  SyncBuffer(dest);
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(start_pos()))) {
    return FailOverflow();
  }
  ShrinkTail(src.size());
  move_start_pos(src.size());
  std::move(src).AppendTo(dest);
  return true;
}

bool CordWriterBase::WriteSlow(const absl::Cord& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Cord): "
         "enough space available, use Write(Cord) instead";
  if (src.size() <= kMaxBytesToCopy) return Writer::WriteSlow(src);
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  absl::Cord& dest = *DestCord();
  RIEGELI_ASSERT_LE(start_pos(), dest.size())
      << "CordWriter destination changed unexpectedly";
  SyncBuffer(dest);
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(start_pos()))) {
    return FailOverflow();
  }
  ShrinkTail(src.size());
  move_start_pos(src.size());
  dest.Append(src);
  return true;
}

bool CordWriterBase::WriteSlow(absl::Cord&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Cord&&): "
         "enough space available, use Write(Cord&&) instead";
  if (src.size() <= kMaxBytesToCopy) {
    // Not `std::move(src)`: forward to `Writer::WriteSlow(const absl::Cord&)`,
    // because `Writer::WriteSlow(absl::Cord&&)` would forward to
    // `CordWriterBase::WriteSlow(const absl::Cord&)`.
    return Writer::WriteSlow(src);
  }
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  absl::Cord& dest = *DestCord();
  RIEGELI_ASSERT_LE(start_pos(), dest.size())
      << "CordWriter destination changed unexpectedly";
  SyncBuffer(dest);
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(start_pos()))) {
    return FailOverflow();
  }
  ShrinkTail(src.size());
  move_start_pos(src.size());
  dest.Append(std::move(src));
  return true;
}

bool CordWriterBase::WriteZerosSlow(Position length) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Writer::WriteZerosSlow(): "
         "enough space available, use WriteZeros() instead";
  if (length <= kMaxBytesToCopy) return Writer::WriteZerosSlow(length);
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  absl::Cord& dest = *DestCord();
  RIEGELI_ASSERT_LE(start_pos(), dest.size())
      << "CordWriter destination changed unexpectedly";
  SyncBuffer(dest);
  if (ABSL_PREDICT_FALSE(length > std::numeric_limits<size_t>::max() -
                                      IntCast<size_t>(start_pos()))) {
    return FailOverflow();
  }
  ShrinkTail(length);
  move_start_pos(length);
  dest.Append(CordOfZeros(IntCast<size_t>(length)));
  return true;
}

bool CordWriterBase::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  absl::Cord& dest = *DestCord();
  RIEGELI_ASSERT_LE(start_pos(), dest.size())
      << "CordWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(HasAppendedTail(dest))) {
    RIEGELI_ASSERT(start() == nullptr)
        << "Failed invariant of CordWriterBase: "
           "both a buffer and the appended tail are present";
    RIEGELI_ASSERT(tail_ == nullptr || tail_->empty())
        << "Failed invariant of CordWriterBase: "
           "the tail is both appended and separated";
    return true;
  }
  SyncBuffer(dest);
  AppendTail(dest);
  return true;
}

bool CordWriterBase::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT_NE(new_pos, pos())
      << "Failed precondition of Writer::SeekSlow(): "
         "position unchanged, use Seek() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  absl::Cord& dest = *DestCord();
  RIEGELI_ASSERT_LE(start_pos(), dest.size())
      << "CordWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(HasAppendedTail(dest))) {
    RIEGELI_ASSERT(start() == nullptr)
        << "Failed invariant of CordWriterBase: "
           "both a buffer and the appended tail are present";
    RIEGELI_ASSERT(tail_ == nullptr || tail_->empty())
        << "Failed invariant of CordWriterBase: "
           "the tail is both appended and separated";
    if (ABSL_PREDICT_FALSE(new_pos > dest.size())) {
      set_start_pos(dest.size());
      return false;
    }
    MoveToTail(dest.size() - IntCast<size_t>(new_pos), dest);
    set_start_pos(new_pos);
    return true;
  }
  if (new_pos > pos()) {
    if (ABSL_PREDICT_TRUE(tail_ == nullptr) || tail_->empty()) return false;
    SyncBuffer(dest);
    if (ABSL_PREDICT_FALSE(new_pos > dest.size() + tail_->size())) {
      AppendTail(dest);
      set_start_pos(dest.size());
      return false;
    }
    MoveFromTail(IntCast<size_t>(new_pos) - dest.size(), dest);
  } else {
    SyncBuffer(dest);
    MoveToTail(dest.size() - IntCast<size_t>(new_pos), dest);
  }
  set_start_pos(new_pos);
  return true;
}

absl::optional<Position> CordWriterBase::SizeImpl() {
  if (ABSL_PREDICT_FALSE(!ok())) return absl::nullopt;
  absl::Cord& dest = *DestCord();
  RIEGELI_ASSERT_LE(start_pos(), dest.size())
      << "CordWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(HasAppendedTail(dest))) {
    RIEGELI_ASSERT(start() == nullptr)
        << "Failed invariant of CordWriterBase: "
           "both a buffer and the appended tail are present";
    RIEGELI_ASSERT(tail_ == nullptr || tail_->empty())
        << "Failed invariant of CordWriterBase: "
           "the tail is both appended and separated";
    return dest.size();
  }
  if (ABSL_PREDICT_FALSE(tail_ != nullptr)) {
    return UnsignedMax(pos(), start_pos() + tail_->size());
  }
  return pos();
}

bool CordWriterBase::TruncateImpl(Position new_size) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  absl::Cord& dest = *DestCord();
  RIEGELI_ASSERT_LE(start_pos(), dest.size())
      << "CordWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(HasAppendedTail(dest))) {
    RIEGELI_ASSERT(start() == nullptr)
        << "Failed invariant of CordWriterBase: "
           "both a buffer and the appended tail are present";
    RIEGELI_ASSERT(tail_ == nullptr || tail_->empty())
        << "Failed invariant of CordWriterBase: "
           "the tail is both appended and separated";
    if (ABSL_PREDICT_FALSE(new_size > dest.size())) {
      set_start_pos(dest.size());
      return false;
    }
  } else if (new_size > pos()) {
    if (ABSL_PREDICT_TRUE(tail_ == nullptr) || tail_->empty()) return false;
    SyncBuffer(dest);
    if (ABSL_PREDICT_FALSE(new_size > dest.size() + tail_->size())) {
      move_start_pos(tail_->size());
      AppendTail(dest);
      return false;
    }
    set_start_pos(new_size);
    tail_->RemoveSuffix(dest.size() + tail_->size() -
                        IntCast<size_t>(new_size));
    AppendTail(dest);
    return true;
  } else {
    if (ABSL_PREDICT_FALSE(tail_ != nullptr)) tail_->Clear();
    if (new_size >= start_pos()) {
      set_cursor(start() + (new_size - start_pos()));
      return true;
    }
  }
  RIEGELI_ASSERT(tail_ == nullptr || tail_->empty());
  RIEGELI_ASSERT_LE(new_size, dest.size());
  set_start_pos(new_size);
  dest.RemoveSuffix(dest.size() - IntCast<size_t>(new_size));
  set_cursor(start());
  return true;
}

Reader* CordWriterBase::ReadModeImpl(Position initial_pos) {
  if (ABSL_PREDICT_FALSE(!CordWriterBase::FlushImpl(FlushType::kFromObject))) {
    return nullptr;
  }
  absl::Cord& dest = *DestCord();
  CordReader<>* const reader = associated_reader_.ResetReader(&dest);
  reader->Seek(initial_pos);
  return reader;
}

}  // namespace riegeli
