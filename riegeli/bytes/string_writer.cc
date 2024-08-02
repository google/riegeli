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
#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/byte_fill.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/cord_utils.h"
#include "riegeli/base/external_ref.h"
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

inline void StringWriterBase::SyncDestBuffer(std::string& dest) {
  RIEGELI_ASSERT(!uses_secondary_buffer())
      << "Failed precondition in StringWriterBase::SyncDestBuffer(): "
         "secondary buffer is used";
  const size_t new_size = used_size();
  set_start_pos(pos());
  dest.erase(new_size);
  set_buffer();
}

inline void StringWriterBase::MakeDestBuffer(std::string& dest,
                                             size_t cursor_index) {
  RIEGELI_ASSERT(!uses_secondary_buffer())
      << "Failed precondition in StringWriterBase::MakeDestBuffer(): "
         "secondary buffer is used";
  set_buffer(&dest[0], dest.size(), cursor_index);
  set_start_pos(0);
}

inline void StringWriterBase::GrowDestToCapacityAndMakeBuffer(
    std::string& dest, size_t cursor_index) {
  RIEGELI_ASSERT(!uses_secondary_buffer())
      << "Failed precondition in "
         "StringWriterBase::GrowDestToCapacityAndMakeBuffer(): "
         "secondary buffer is used";
  dest.resize(dest.capacity());
  MakeDestBuffer(dest, cursor_index);
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
    absl::optional<Position> write_size_hint) {
  if (write_size_hint == absl::nullopt || ABSL_PREDICT_FALSE(!ok())) return;
  std::string& dest = *DestString();
  RIEGELI_ASSERT_EQ(UnsignedMax(limit_pos(), written_size_),
                    dest.size() + secondary_buffer_.size())
      << "StringWriter destination changed unexpectedly";
  const size_t size_hint = SaturatingAdd(pos(), *write_size_hint);
  if (!uses_secondary_buffer()) {
    SyncDestBuffer(dest);
    if (dest.capacity() < size_hint) dest.reserve(size_hint);
  } else {
    if (dest.capacity() < size_hint) dest.reserve(size_hint);
    SyncSecondaryBuffer();
    std::move(secondary_buffer_).AppendTo(dest);
    secondary_buffer_.Clear();
  }
  GrowDestToCapacityAndMakeBuffer(dest, IntCast<size_t>(start_pos()));
}

bool StringWriterBase::PushSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Writer::PushSlow(): "
         "enough space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  std::string& dest = *DestString();
  RIEGELI_ASSERT_EQ(UnsignedMax(limit_pos(), written_size_),
                    dest.size() + secondary_buffer_.size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(min_length > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  if (!uses_secondary_buffer()) {
    SyncDestBuffer(dest);
    const size_t cursor_index = IntCast<size_t>(start_pos());
    if (dest.empty() || ABSL_PREDICT_FALSE(written_size_ > cursor_index)) {
      // Allocate the first block directly in `dest`. It is possible that it
      // will not need to be copied if it turns out to be the only block,
      // although this decision might cause it to remain wasteful if less data
      // are written than space requested.
      //
      // Resize `dest` also if data follow the current position.
      const size_t size_hint =
          cursor_index + UnsignedMax(min_length, recommended_length);
      if (dest.capacity() < size_hint) dest.reserve(size_hint);
    }
    if (min_length <= dest.capacity() - cursor_index) {
      GrowDestToCapacityAndMakeBuffer(dest, cursor_index);
      return true;
    }
    written_size_ = 0;
  } else {
    SyncSecondaryBuffer();
  }
  MakeSecondaryBuffer(min_length, recommended_length);
  return true;
}

bool StringWriterBase::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain): "
         "enough space available, use Write(Chain) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  std::string& dest = *DestString();
  RIEGELI_ASSERT_EQ(UnsignedMax(limit_pos(), written_size_),
                    dest.size() + secondary_buffer_.size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  if (!uses_secondary_buffer()) {
    SyncDestBuffer(dest);
    const size_t cursor_index = IntCast<size_t>(start_pos());
    const size_t new_cursor_index = cursor_index + src.size();
    if (new_cursor_index <= dest.capacity()) {
      if (ABSL_PREDICT_FALSE(new_cursor_index <= dest.size())) {
        src.CopyTo(&dest[cursor_index]);
      } else {
        dest.erase(cursor_index);
        src.AppendTo(dest);
      }
      GrowDestToCapacityAndMakeBuffer(dest, new_cursor_index);
      return true;
    }
    dest.erase(cursor_index);
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
  RIEGELI_ASSERT_EQ(UnsignedMax(limit_pos(), written_size_),
                    dest.size() + secondary_buffer_.size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  if (!uses_secondary_buffer()) {
    SyncDestBuffer(dest);
    const size_t cursor_index = IntCast<size_t>(start_pos());
    const size_t new_cursor_index = cursor_index + src.size();
    if (new_cursor_index <= dest.capacity()) {
      if (ABSL_PREDICT_FALSE(new_cursor_index <= dest.size())) {
        src.CopyTo(&dest[cursor_index]);
      } else {
        dest.erase(cursor_index);
        std::move(src).AppendTo(dest);
      }
      GrowDestToCapacityAndMakeBuffer(dest, new_cursor_index);
      return true;
    }
    dest.erase(cursor_index);
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
  RIEGELI_ASSERT_EQ(UnsignedMax(limit_pos(), written_size_),
                    dest.size() + secondary_buffer_.size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  if (!uses_secondary_buffer()) {
    SyncDestBuffer(dest);
    const size_t cursor_index = IntCast<size_t>(start_pos());
    const size_t new_cursor_index = cursor_index + src.size();
    if (new_cursor_index <= dest.capacity()) {
      if (ABSL_PREDICT_FALSE(new_cursor_index <= dest.size())) {
        cord_internal::CopyCordToArray(src, &dest[cursor_index]);
      } else {
        dest.erase(cursor_index);
        cord_internal::AppendCordToString(src, dest);
      }
      GrowDestToCapacityAndMakeBuffer(dest, new_cursor_index);
      return true;
    }
    dest.erase(cursor_index);
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
  RIEGELI_ASSERT_EQ(UnsignedMax(limit_pos(), written_size_),
                    dest.size() + secondary_buffer_.size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  if (!uses_secondary_buffer()) {
    SyncDestBuffer(dest);
    const size_t cursor_index = IntCast<size_t>(start_pos());
    const size_t new_cursor_index = cursor_index + src.size();
    if (new_cursor_index <= dest.capacity()) {
      if (ABSL_PREDICT_FALSE(new_cursor_index <= dest.size())) {
        cord_internal::CopyCordToArray(src, &dest[cursor_index]);
      } else {
        dest.erase(cursor_index);
        cord_internal::AppendCordToString(src, dest);
      }
      GrowDestToCapacityAndMakeBuffer(dest, new_cursor_index);
      return true;
    }
    dest.erase(cursor_index);
    written_size_ = 0;
  } else {
    SyncSecondaryBuffer();
  }
  move_start_pos(src.size());
  secondary_buffer_.Append(std::move(src), options_);
  MakeSecondaryBuffer();
  return true;
}

bool StringWriterBase::WriteSlow(ExternalRef src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(ExternalRef): "
         "enough space available, use Write(ExternalRef) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  std::string& dest = *DestString();
  RIEGELI_ASSERT_EQ(UnsignedMax(limit_pos(), written_size_),
                    dest.size() + secondary_buffer_.size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  if (!uses_secondary_buffer()) {
    SyncDestBuffer(dest);
    const size_t cursor_index = IntCast<size_t>(start_pos());
    const size_t new_cursor_index = cursor_index + src.size();
    if (new_cursor_index <= dest.capacity()) {
      if (ABSL_PREDICT_FALSE(new_cursor_index <= dest.size())) {
        std::memcpy(&dest[cursor_index], src.data(), src.size());
      } else {
        dest.erase(cursor_index);
        // TODO: When `absl::string_view` becomes C++17
        // `std::string_view`: `dest.append(absl::string_view(src))`
        dest.append(src.data(), src.size());
      }
      GrowDestToCapacityAndMakeBuffer(dest, new_cursor_index);
      return true;
    }
    dest.erase(cursor_index);
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
  RIEGELI_ASSERT_EQ(UnsignedMax(limit_pos(), written_size_),
                    dest.size() + secondary_buffer_.size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  if (!uses_secondary_buffer()) {
    SyncDestBuffer(dest);
    const size_t cursor_index = IntCast<size_t>(start_pos());
    const size_t new_cursor_index = cursor_index + IntCast<size_t>(src.size());
    if (new_cursor_index <= dest.capacity()) {
      if (ABSL_PREDICT_FALSE(new_cursor_index <= dest.size())) {
        std::memset(&dest[cursor_index], src.fill(),
                    IntCast<size_t>(src.size()));
      } else {
        dest.erase(cursor_index);
        dest.append(IntCast<size_t>(src.size()), src.fill());
      }
      GrowDestToCapacityAndMakeBuffer(dest, new_cursor_index);
      return true;
    }
    dest.erase(cursor_index);
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
  RIEGELI_ASSERT_EQ(UnsignedMax(limit_pos(), written_size_),
                    dest.size() + secondary_buffer_.size())
      << "StringWriter destination changed unexpectedly";
  if (!uses_secondary_buffer()) {
    SyncDestBuffer(dest);
  } else {
    SyncSecondaryBuffer();
    std::move(secondary_buffer_).AppendTo(dest);
    secondary_buffer_.Clear();
  }
  return true;
}

bool StringWriterBase::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT_NE(new_pos, pos())
      << "Failed precondition of Writer::SeekSlow(): "
         "position unchanged, use Seek() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  std::string& dest = *DestString();
  RIEGELI_ASSERT_EQ(UnsignedMax(limit_pos(), written_size_),
                    dest.size() + secondary_buffer_.size())
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
      std::move(secondary_buffer_).AppendTo(dest);
      secondary_buffer_.Clear();
    }
    written_size_ = used_size();
  }
  MakeDestBuffer(dest, IntCast<size_t>(new_pos));
  return true;
}

absl::optional<Position> StringWriterBase::SizeImpl() {
  if (ABSL_PREDICT_FALSE(!ok())) return absl::nullopt;
  return used_size();
}

bool StringWriterBase::TruncateImpl(Position new_size) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  std::string& dest = *DestString();
  RIEGELI_ASSERT_EQ(UnsignedMax(limit_pos(), written_size_),
                    dest.size() + secondary_buffer_.size())
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
  RIEGELI_ASSERT_EQ(UnsignedMax(limit_pos(), written_size_),
                    dest.size() + secondary_buffer_.size())
      << "StringWriter destination changed unexpectedly";
  if (uses_secondary_buffer()) {
    SyncSecondaryBuffer();
    std::move(secondary_buffer_).AppendTo(dest);
    secondary_buffer_.Clear();
  }
  StringReader<>* const reader =
      associated_reader_.ResetReader(dest.data(), used_size());
  reader->Seek(initial_pos);
  return reader;
}

}  // namespace riegeli
