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
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/memory.h"
#include "riegeli/bytes/cord_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if __cplusplus < 201703
constexpr size_t CordWriterBase::kShortBufferSize;
#endif

void CordWriterBase::Done() {
  CordWriterBase::FlushImpl(FlushType::kFromObject);
  Writer::Done();
  buffer_ = Buffer();
  associated_reader_.Reset();
}

bool CordWriterBase::PushSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Writer::PushSlow(): "
         "enough space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (pos() == 0) {
    Position needed_length = UnsignedMax(min_length, recommended_length);
    if (size_hint_ != absl::nullopt) {
      needed_length = UnsignedMax(needed_length, *size_hint_);
    }
    if (needed_length <= kShortBufferSize) {
      // Use `short_buffer_`, even if it is smaller than `min_block_size_`,
      // because this avoids allocation.
      set_buffer(short_buffer_, kShortBufferSize);
      return true;
    }
  }
  if (ABSL_PREDICT_FALSE(min_length > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  absl::Cord& dest = *dest_cord();
  RIEGELI_ASSERT_EQ(start_pos(), dest.size())
      << "CordWriter destination changed unexpectedly";
  if (start() == short_buffer_) {
    const size_t cursor_index = start_to_cursor();
    buffer_.Reset(UnsignedClamp(
        UnsignedMax(
            ApplyWriteSizeHint(UnsignedMax(start_pos(), min_block_size_),
                               size_hint_, start_pos()),
            SaturatingAdd(cursor_index, recommended_length)),
        // Ensure at least `kShortBufferSize` of length, so that `short_buffer_`
        // can be copied using a fixed length `std::memcpy()`.
        UnsignedMax(cursor_index + min_length, kShortBufferSize),
        max_block_size_));
    std::memcpy(buffer_.data(), short_buffer_, kShortBufferSize);
    set_buffer(buffer_.data(),
               UnsignedMin(buffer_.capacity(),
                           std::numeric_limits<size_t>::max() - dest.size()),
               cursor_index);
  } else {
    SyncBuffer(dest);
    buffer_.Reset(UnsignedClamp(
        UnsignedMax(
            ApplyWriteSizeHint(UnsignedMax(start_pos(), min_block_size_),
                               size_hint_, start_pos()),
            recommended_length),
        min_length, max_block_size_));
    set_buffer(buffer_.data(),
               UnsignedMin(buffer_.capacity(),
                           std::numeric_limits<size_t>::max() - dest.size()));
  }
  return true;
}

bool CordWriterBase::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain): "
         "enough space available, use Write(Chain) instead";
  if (src.size() <= kMaxBytesToCopy) return Writer::WriteSlow(src);
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  absl::Cord& dest = *dest_cord();
  RIEGELI_ASSERT_EQ(start_pos(), dest.size())
      << "CordWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  SyncBuffer(dest);
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
  absl::Cord& dest = *dest_cord();
  RIEGELI_ASSERT_EQ(start_pos(), dest.size())
      << "CordWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  SyncBuffer(dest);
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
  absl::Cord& dest = *dest_cord();
  RIEGELI_ASSERT_EQ(start_pos(), dest.size())
      << "CordWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  SyncBuffer(dest);
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
  absl::Cord& dest = *dest_cord();
  RIEGELI_ASSERT_EQ(start_pos(), dest.size())
      << "CordWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  SyncBuffer(dest);
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
  absl::Cord& dest = *dest_cord();
  RIEGELI_ASSERT_EQ(start_pos(), dest.size())
      << "CordWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(length > std::numeric_limits<size_t>::max() -
                                      IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  SyncBuffer(dest);
  move_start_pos(length);
  dest.Append(CordOfZeros(IntCast<size_t>(length)));
  return true;
}

bool CordWriterBase::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  absl::Cord& dest = *dest_cord();
  RIEGELI_ASSERT_EQ(start_pos(), dest.size())
      << "CordWriter destination changed unexpectedly";
  SyncBuffer(dest);
  return true;
}

absl::optional<Position> CordWriterBase::SizeImpl() {
  if (ABSL_PREDICT_FALSE(!ok())) return absl::nullopt;
  return pos();
}

bool CordWriterBase::TruncateImpl(Position new_size) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  absl::Cord& dest = *dest_cord();
  RIEGELI_ASSERT_EQ(start_pos(), dest.size())
      << "CordWriter destination changed unexpectedly";
  if (new_size >= start_pos()) {
    if (ABSL_PREDICT_FALSE(new_size > pos())) return false;
    set_cursor(start() + (new_size - start_pos()));
    return true;
  }
  set_start_pos(new_size);
  dest.RemoveSuffix(dest.size() - IntCast<size_t>(new_size));
  set_cursor(start());
  return true;
}

Reader* CordWriterBase::ReadModeImpl(Position initial_pos) {
  if (ABSL_PREDICT_FALSE(!ok())) return nullptr;
  absl::Cord& dest = *dest_cord();
  SyncBuffer(dest);
  CordReader<>* const reader = associated_reader_.ResetReader(&dest);
  reader->Seek(initial_pos);
  return reader;
}

inline void CordWriterBase::SyncBuffer(absl::Cord& dest) {
  if (start() == nullptr) return;
  set_start_pos(pos());
  const absl::string_view data(start(), start_to_cursor());
  if (start() == short_buffer_) {
    dest.Append(data);
  } else {
    std::move(buffer_).AppendSubstrTo(data, dest);
  }
  set_buffer();
}

}  // namespace riegeli
