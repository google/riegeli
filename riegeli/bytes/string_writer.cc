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

#include "riegeli/bytes/string_writer.h"

#include <stddef.h>

#include <cstring>
#include <limits>
#include <optional>
#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/byte_fill.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/cord_utils.h"
#include "riegeli/base/external_ref.h"
#include "riegeli/base/string_utils.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/string_reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void StringWriterBase::Done() {
  StringWriterBase::FlushImpl(FlushType::kFromObject);
  Writer::Done();
  secondary_buffer_ = Chain();
  associated_reader_.Reset();
}

inline size_t StringWriterBase::used_size() const {
  return UnsignedMax(IntCast<size_t>(pos()), written_size_);
}

inline size_t StringWriterBase::used_dest_size() const {
  if (uses_secondary_buffer()) {
    RIEGELI_ASSERT_EQ(available(), 0u)
        << "Failed precondition of StringWriterBase::used_dest_size(): "
        << "secondary buffer has free space";
  }
  RIEGELI_ASSERT_GE(used_size(), secondary_buffer_.size())
      << "Failed invariant of StringWriterBase: "
         "negative destination size";
  return used_size() - secondary_buffer_.size();
}

inline void StringWriterBase::MakeDestBuffer(std::string& dest,
                                             size_t cursor_index) {
  RIEGELI_ASSERT(!uses_secondary_buffer())
      << "Failed precondition in StringWriterBase::MakeDestBuffer(): "
         "secondary buffer is used";
  set_buffer(dest.data(), dest.size(), cursor_index);
  set_start_pos(0);
}

inline void StringWriterBase::GrowDestAndMakeBuffer(std::string& dest,
                                                    size_t cursor_index,
                                                    size_t new_size) {
  if (uses_secondary_buffer()) {
    RIEGELI_ASSERT_EQ(available(), 0u)
        << "Failed precondition of StringWriterBase::GrowDestAndMakeBuffer(): "
        << "secondary buffer has free space";
  }
  const size_t old_size = dest.size();
  if (ABSL_PREDICT_TRUE(new_size > old_size)) {
    StringResizeAmortized(dest, new_size);
    MarkPoisoned(dest.data() + old_size, new_size - old_size);
  }
  set_buffer(dest.data(), dest.size(), cursor_index);
  set_start_pos(0);
}

inline bool StringWriterBase::GrowDestUnderCapacityAndMakeBuffer(
    std::string& dest, size_t cursor_index, size_t min_length) {
  RIEGELI_ASSERT(!uses_secondary_buffer())
      << "Failed precondition in "
         "StringWriterBase::GrowDestUnderCapacityAndMakeBuffer(): "
         "secondary buffer is used";
  RIEGELI_ASSERT_LE(min_length,
                    std::numeric_limits<size_t>::max() - cursor_index)
      << "Failed precondition of "
         "StringWriterBase::GrowDestUnderCapacityAndMakeBuffer(): "
         "Writer position overflow";
  size_t new_size = cursor_index + min_length;
  if (new_size > dest.capacity()) return false;
  new_size = UnsignedClamp(
      dest.size() + UnsignedClamp(dest.size(), kDefaultMinBlockSize,
                                  kDefaultMaxBlockSize),
      new_size, dest.capacity());
  const size_t old_size = dest.size();
  if (ABSL_PREDICT_TRUE(new_size > old_size)) {
    dest.resize(new_size);
    MarkPoisoned(dest.data() + old_size, new_size - old_size);
  }
  set_buffer(dest.data(), dest.size(), cursor_index);
  set_start_pos(0);
  return true;
}

inline void StringWriterBase::SyncSecondaryBuffer() {
  set_start_pos(pos());
  secondary_buffer_.RemoveSuffix(available(), options_);
  set_buffer();
}

inline void StringWriterBase::MakeSecondaryBuffer(size_t min_length,
                                                  size_t recommended_length) {
  const absl::Span<char> buffer = secondary_buffer_.AppendBuffer(
      min_length, recommended_length, Chain::kAnyLength, options_);
  set_buffer(buffer.data(), buffer.size());
}

void StringWriterBase::SetWriteSizeHintImpl(
    std::optional<Position> write_size_hint) {
  if (write_size_hint == std::nullopt || ABSL_PREDICT_FALSE(!ok())) return;
  std::string& dest = *DestString();
  RIEGELI_ASSERT_GE(dest.size(), limit_pos() - secondary_buffer_.size())
      << "StringWriter destination changed unexpectedly";
  const size_t cursor_index = IntCast<size_t>(pos());
  const size_t size_hint = UnsignedMax(
      SaturatingAdd(cursor_index, SaturatingIntCast<size_t>(*write_size_hint)),
      written_size_);
  if (!uses_secondary_buffer()) {
    GrowDestAndMakeBuffer(dest, cursor_index, size_hint);
    return;
  }
  SyncSecondaryBuffer();
  dest.erase(used_dest_size());
  StringReserveAmortized(dest, size_hint);
  std::move(secondary_buffer_).AppendTo(dest);
  secondary_buffer_.Clear();
  MakeDestBuffer(dest, cursor_index);
}

bool StringWriterBase::PushSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Writer::PushSlow(): "
         "enough space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  std::string& dest = *DestString();
  RIEGELI_ASSERT_GE(dest.size(), limit_pos() - secondary_buffer_.size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(min_length > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  if (!uses_secondary_buffer()) {
    const size_t cursor_index = IntCast<size_t>(pos());
    if (cursor_index == 0 || ABSL_PREDICT_FALSE(written_size_ > cursor_index)) {
      // Allocate the first block directly in `dest`. It is possible that it
      // will not need to be copied if it turns out to be the only block,
      // although this decision might cause it to remain wasteful if less data
      // are written than space requested.
      //
      // Resize `dest` also if data follow the current position, to make the
      // data available for partial overwriting.
      const size_t size_hint = SaturatingAdd(
          cursor_index, UnsignedMax(min_length, recommended_length));
      StringReserveAmortized(dest, size_hint);
    }
    if (GrowDestUnderCapacityAndMakeBuffer(dest, cursor_index, min_length)) {
      return true;
    }
    set_start_pos(cursor_index);
    set_buffer();
    written_size_ = 0;
  } else {
    SyncSecondaryBuffer();
  }
  MakeSecondaryBuffer(min_length, recommended_length);
  return true;
}

bool StringWriterBase::WriteSlow(absl::string_view src) {
  RIEGELI_ASSERT_LT(available(), src.size())
      << "Failed precondition of Writer::WriteSlow(string_view): "
         "enough space available, use Write(string_view) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  std::string& dest = *DestString();
  RIEGELI_ASSERT_GE(dest.size(), limit_pos() - secondary_buffer_.size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  if (!uses_secondary_buffer()) {
    const size_t cursor_index = IntCast<size_t>(pos());
    const size_t new_cursor_index = cursor_index + src.size();
    if (cursor_index == 0) {
      // Allocate the first block directly in `dest`. It is possible that it
      // will not need to be copied if it turns out to be the only block,
      // although this decision might cause it to remain wasteful if less data
      // are written than space requested.
      StringReserveAmortized(dest, new_cursor_index);
    }
    if (new_cursor_index <= dest.capacity()) {
      if (ABSL_PREDICT_FALSE(new_cursor_index <= dest.size())) {
        std::memcpy(dest.data() + cursor_index, src.data(), src.size());
      } else {
        dest.erase(cursor_index);
        // TODO: When `absl::string_view` becomes C++17
        // `std::string_view`: `dest.append(src)`
        dest.append(src.data(), src.size());
      }
      GrowDestUnderCapacityAndMakeBuffer(dest, new_cursor_index);
      return true;
    }
    const size_t prefix_size = dest.capacity() - cursor_index;
    dest.erase(cursor_index);
    dest.append(src.data(), prefix_size);
    src.remove_prefix(prefix_size);
    set_start_pos(cursor_index + prefix_size);
    set_buffer();
    written_size_ = 0;
  } else {
    SyncSecondaryBuffer();
  }
  move_start_pos(src.size());
  secondary_buffer_.Append(src, options_);
  MakeSecondaryBuffer();
  return true;
}

bool StringWriterBase::WriteSlow(ExternalRef src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(ExternalRef): "
         "enough space available, use Write(ExternalRef) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  std::string& dest = *DestString();
  RIEGELI_ASSERT_GE(dest.size(), limit_pos() - secondary_buffer_.size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  if (!uses_secondary_buffer()) {
    const size_t cursor_index = IntCast<size_t>(pos());
    const size_t new_cursor_index = cursor_index + src.size();
    if (new_cursor_index <= dest.capacity()) {
      if (ABSL_PREDICT_FALSE(new_cursor_index <= dest.size())) {
        std::memcpy(dest.data() + cursor_index, src.data(), src.size());
      } else {
        dest.erase(cursor_index);
        // TODO: When `absl::string_view` becomes C++17
        // `std::string_view`: `dest.append(absl::string_view(src))`
        dest.append(src.data(), src.size());
      }
      GrowDestUnderCapacityAndMakeBuffer(dest, new_cursor_index);
      return true;
    }
    set_start_pos(cursor_index);
    set_buffer();
    written_size_ = 0;
  } else {
    SyncSecondaryBuffer();
  }
  move_start_pos(src.size());
  secondary_buffer_.Append(std::move(src), options_);
  MakeSecondaryBuffer();
  return true;
}

bool StringWriterBase::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain): "
         "enough space available, use Write(Chain) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  std::string& dest = *DestString();
  RIEGELI_ASSERT_GE(dest.size(), limit_pos() - secondary_buffer_.size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  if (!uses_secondary_buffer()) {
    const size_t cursor_index = IntCast<size_t>(pos());
    const size_t new_cursor_index = cursor_index + src.size();
    if (new_cursor_index <= dest.capacity()) {
      if (ABSL_PREDICT_FALSE(new_cursor_index <= dest.size())) {
        src.CopyTo(dest.data() + cursor_index);
      } else {
        dest.erase(cursor_index);
        src.AppendTo(dest);
      }
      GrowDestUnderCapacityAndMakeBuffer(dest, new_cursor_index);
      return true;
    }
    set_start_pos(cursor_index);
    set_buffer();
    written_size_ = 0;
  } else {
    SyncSecondaryBuffer();
  }
  move_start_pos(src.size());
  secondary_buffer_.Append(src, options_);
  MakeSecondaryBuffer();
  return true;
}

bool StringWriterBase::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain&&): "
         "enough space available, use Write(Chain) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  std::string& dest = *DestString();
  RIEGELI_ASSERT_GE(dest.size(), limit_pos() - secondary_buffer_.size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  if (!uses_secondary_buffer()) {
    const size_t cursor_index = IntCast<size_t>(pos());
    const size_t new_cursor_index = cursor_index + src.size();
    if (new_cursor_index <= dest.capacity()) {
      if (ABSL_PREDICT_FALSE(new_cursor_index <= dest.size())) {
        src.CopyTo(dest.data() + cursor_index);
      } else {
        dest.erase(cursor_index);
        std::move(src).AppendTo(dest);
      }
      GrowDestUnderCapacityAndMakeBuffer(dest, new_cursor_index);
      return true;
    }
    set_start_pos(cursor_index);
    set_buffer();
    written_size_ = 0;
  } else {
    SyncSecondaryBuffer();
  }
  move_start_pos(src.size());
  secondary_buffer_.Append(std::move(src), options_);
  MakeSecondaryBuffer();
  return true;
}

bool StringWriterBase::WriteSlow(const absl::Cord& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Cord): "
         "enough space available, use Write(Cord) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  std::string& dest = *DestString();
  RIEGELI_ASSERT_GE(dest.size(), limit_pos() - secondary_buffer_.size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  if (!uses_secondary_buffer()) {
    const size_t cursor_index = IntCast<size_t>(pos());
    const size_t new_cursor_index = cursor_index + src.size();
    if (new_cursor_index <= dest.capacity()) {
      if (ABSL_PREDICT_FALSE(new_cursor_index <= dest.size())) {
        cord_internal::CopyCordToArray(src, dest.data() + cursor_index);
      } else {
        dest.erase(cursor_index);
        absl::AppendCordToString(src, &dest);
      }
      GrowDestUnderCapacityAndMakeBuffer(dest, new_cursor_index);
      return true;
    }
    set_start_pos(cursor_index);
    set_buffer();
    written_size_ = 0;
  } else {
    SyncSecondaryBuffer();
  }
  move_start_pos(src.size());
  secondary_buffer_.Append(src, options_);
  MakeSecondaryBuffer();
  return true;
}

bool StringWriterBase::WriteSlow(absl::Cord&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Cord&&): "
         "enough space available, use Write(Cord&&) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  std::string& dest = *DestString();
  RIEGELI_ASSERT_GE(dest.size(), limit_pos() - secondary_buffer_.size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  if (!uses_secondary_buffer()) {
    const size_t cursor_index = IntCast<size_t>(pos());
    const size_t new_cursor_index = cursor_index + src.size();
    if (new_cursor_index <= dest.capacity()) {
      if (ABSL_PREDICT_FALSE(new_cursor_index <= dest.size())) {
        cord_internal::CopyCordToArray(src, dest.data() + cursor_index);
      } else {
        dest.erase(cursor_index);
        absl::AppendCordToString(src, &dest);
      }
      GrowDestUnderCapacityAndMakeBuffer(dest, new_cursor_index);
      return true;
    }
    set_start_pos(cursor_index);
    set_buffer();
    written_size_ = 0;
  } else {
    SyncSecondaryBuffer();
  }
  move_start_pos(src.size());
  secondary_buffer_.Append(std::move(src), options_);
  MakeSecondaryBuffer();
  return true;
}

bool StringWriterBase::WriteSlow(ByteFill src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(ByteFill): "
         "enough space available, use Write(ByteFill) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  std::string& dest = *DestString();
  RIEGELI_ASSERT_GE(dest.size(), limit_pos() - secondary_buffer_.size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  if (!uses_secondary_buffer()) {
    const size_t cursor_index = IntCast<size_t>(pos());
    const size_t new_cursor_index = cursor_index + IntCast<size_t>(src.size());
    if (new_cursor_index <= dest.capacity()) {
      if (ABSL_PREDICT_FALSE(new_cursor_index <= dest.size())) {
        std::memset(dest.data() + cursor_index, src.fill(),
                    IntCast<size_t>(src.size()));
      } else {
        dest.erase(cursor_index);
        dest.append(IntCast<size_t>(src.size()), src.fill());
      }
      GrowDestUnderCapacityAndMakeBuffer(dest, new_cursor_index);
      return true;
    }
    set_start_pos(cursor_index);
    set_buffer();
    written_size_ = 0;
  } else {
    SyncSecondaryBuffer();
  }
  move_start_pos(src.size());
  src.AppendTo(secondary_buffer_, options_);
  MakeSecondaryBuffer();
  return true;
}

bool StringWriterBase::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  std::string& dest = *DestString();
  RIEGELI_ASSERT_GE(dest.size(), limit_pos() - secondary_buffer_.size())
      << "StringWriter destination changed unexpectedly";
  const size_t cursor_index = IntCast<size_t>(pos());
  if (!uses_secondary_buffer()) {
    dest.erase(used_size());
  } else {
    SyncSecondaryBuffer();
    dest.erase(used_dest_size());
    std::move(secondary_buffer_).AppendTo(dest);
    secondary_buffer_.Clear();
  }
  set_buffer();
  set_start_pos(cursor_index);
  return true;
}

bool StringWriterBase::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT_NE(new_pos, pos())
      << "Failed precondition of Writer::SeekSlow(): "
         "position unchanged, use Seek() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  std::string& dest = *DestString();
  RIEGELI_ASSERT_GE(dest.size(), limit_pos() - secondary_buffer_.size())
      << "StringWriter destination changed unexpectedly";
  if (new_pos > pos()) {
    if (ABSL_PREDICT_FALSE(uses_secondary_buffer())) return false;
    if (ABSL_PREDICT_FALSE(new_pos > used_size())) {
      MakeDestBuffer(dest, used_size());
      return false;
    }
  } else {
    if (uses_secondary_buffer()) {
      SyncSecondaryBuffer();
      dest.erase(used_dest_size());
      std::move(secondary_buffer_).AppendTo(dest);
      secondary_buffer_.Clear();
    }
    written_size_ = used_size();
  }
  MakeDestBuffer(dest, IntCast<size_t>(new_pos));
  return true;
}

std::optional<Position> StringWriterBase::SizeImpl() {
  if (ABSL_PREDICT_FALSE(!ok())) return std::nullopt;
  return used_size();
}

bool StringWriterBase::TruncateImpl(Position new_size) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  std::string& dest = *DestString();
  RIEGELI_ASSERT_GE(dest.size(), limit_pos() - secondary_buffer_.size())
      << "StringWriter destination changed unexpectedly";
  if (new_size > pos()) {
    if (ABSL_PREDICT_FALSE(uses_secondary_buffer())) return false;
    if (ABSL_PREDICT_FALSE(new_size > used_size())) {
      MakeDestBuffer(dest, used_size());
      return false;
    }
  } else if (new_size > limit_pos() - secondary_buffer_.size()) {
    secondary_buffer_.RemoveSuffix(
        IntCast<size_t>(limit_pos()) - IntCast<size_t>(new_size), options_);
    set_start_pos(new_size);
    set_buffer();
    return true;
  } else {
    secondary_buffer_.Clear();
  }
  written_size_ = 0;
  MakeDestBuffer(dest, IntCast<size_t>(new_size));
  return true;
}

Reader* StringWriterBase::ReadModeImpl(Position initial_pos) {
  if (ABSL_PREDICT_FALSE(!ok())) return nullptr;
  std::string& dest = *DestString();
  RIEGELI_ASSERT_GE(dest.size(), limit_pos() - secondary_buffer_.size())
      << "StringWriter destination changed unexpectedly";
  if (uses_secondary_buffer()) {
    SyncSecondaryBuffer();
    dest.erase(used_dest_size());
    std::move(secondary_buffer_).AppendTo(dest);
    secondary_buffer_.Clear();
  }
  StringReader<>* const reader =
      associated_reader_.ResetReader(dest.data(), used_size());
  reader->Seek(initial_pos);
  return reader;
}

}  // namespace riegeli
