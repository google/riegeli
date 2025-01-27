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

#include "riegeli/bytes/resizable_writer.h"

#include <stddef.h>

#include <cstring>
#include <limits>
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

void ResizableWriterBase::Done() {
  ResizableWriterBase::FlushImpl(FlushType::kFromObject);
  Writer::Done();
  secondary_buffer_ = Chain();
  associated_reader_.Reset();
}

inline void ResizableWriterBase::SyncSecondaryBuffer() {
  set_start_pos(pos());
  secondary_buffer_.RemoveSuffix(available(), options_);
  set_buffer();
}

inline void ResizableWriterBase::MakeSecondaryBuffer(
    size_t min_length, size_t recommended_length) {
  const absl::Span<char> buffer = secondary_buffer_.AppendBuffer(
      min_length, recommended_length, Chain::kAnyLength, options_);
  set_buffer(buffer.data(), buffer.size());
}

void ResizableWriterBase::SetWriteSizeHintImpl(
    absl::optional<Position> write_size_hint) {
  if (write_size_hint == absl::nullopt || ABSL_PREDICT_FALSE(!ok())) return;
  const size_t size_hint =
      UnsignedMax(SaturatingAdd(IntCast<size_t>(pos()),
                                SaturatingIntCast<size_t>(*write_size_hint)),
                  written_size_);
  if (!uses_secondary_buffer()) {
    GrowDestAndMakeBuffer(size_hint);
    return;
  }
  SyncSecondaryBuffer();
  if (ABSL_PREDICT_FALSE(!GrowDestAndMakeBuffer(size_hint))) return;
  secondary_buffer_.CopyTo(cursor() - secondary_buffer_.size());
  secondary_buffer_.Clear();
}

bool ResizableWriterBase::PushSlow(size_t min_length,
                                   size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Writer::PushSlow(): "
         "enough space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (ABSL_PREDICT_FALSE(min_length > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  if (!uses_secondary_buffer()) {
    if (pos() == 0 || ABSL_PREDICT_FALSE(written_size_ > pos())) {
      // Allocate the first block directly in the destination. It is possible
      // that it will not need to be copied if it turns out to be the only
      // block, although this decision might cause it to remain wasteful if less
      // data are written than space requested.
      //
      // Resize the destination also if data follow the current position.
      return GrowDestAndMakeBuffer(IntCast<size_t>(pos()) +
                                   UnsignedMax(min_length, recommended_length));
    }
    GrowDestToCapacityAndMakeBuffer();
    if (min_length <= available()) return true;
    set_start_pos(pos());
    set_buffer();
    written_size_ = 0;
  } else {
    SyncSecondaryBuffer();
  }
  MakeSecondaryBuffer(min_length, recommended_length);
  return true;
}

bool ResizableWriterBase::WriteSlow(ExternalRef src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(ExternalRef): "
         "enough space available, use Write(ExternalRef) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  if (!uses_secondary_buffer()) {
    GrowDestToCapacityAndMakeBuffer();
    if (src.size() <= available()) {
      std::memcpy(cursor(), src.data(), src.size());
      move_cursor(src.size());
      return true;
    }
    set_start_pos(pos());
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

bool ResizableWriterBase::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain): "
         "enough space available, use Write(Chain) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  if (!uses_secondary_buffer()) {
    GrowDestToCapacityAndMakeBuffer();
    if (src.size() <= available()) {
      src.CopyTo(cursor());
      move_cursor(src.size());
      return true;
    }
    set_start_pos(pos());
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

bool ResizableWriterBase::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain&&): "
         "enough space available, use Write(Chain) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  if (!uses_secondary_buffer()) {
    GrowDestToCapacityAndMakeBuffer();
    if (src.size() <= available()) {
      src.CopyTo(cursor());
      move_cursor(src.size());
      return true;
    }
    set_start_pos(pos());
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

bool ResizableWriterBase::WriteSlow(const absl::Cord& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Cord): "
         "enough space available, use Write(Cord) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  if (!uses_secondary_buffer()) {
    GrowDestToCapacityAndMakeBuffer();
    if (src.size() <= available()) {
      cord_internal::CopyCordToArray(src, cursor());
      move_cursor(src.size());
      return true;
    }
    set_start_pos(pos());
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

bool ResizableWriterBase::WriteSlow(absl::Cord&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Cord&&): "
         "enough space available, use Write(Cord&&) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  if (!uses_secondary_buffer()) {
    GrowDestToCapacityAndMakeBuffer();
    if (src.size() <= available()) {
      cord_internal::CopyCordToArray(src, cursor());
      move_cursor(src.size());
      return true;
    }
    set_start_pos(pos());
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

bool ResizableWriterBase::WriteSlow(ByteFill src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(ByteFill): "
         "enough space available, use Write(ByteFill) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  if (!uses_secondary_buffer()) {
    GrowDestToCapacityAndMakeBuffer();
    if (src.size() <= available()) {
      std::memset(cursor(), src.fill(), IntCast<size_t>(src.size()));
      move_cursor(IntCast<size_t>(src.size()));
      return true;
    }
    set_start_pos(pos());
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

bool ResizableWriterBase::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (!uses_secondary_buffer()) return ResizeDest();
  SyncSecondaryBuffer();
  if (ABSL_PREDICT_FALSE(!ResizeDest())) return false;
  secondary_buffer_.CopyTo(cursor() - secondary_buffer_.size());
  secondary_buffer_.Clear();
  return true;
}

bool ResizableWriterBase::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT_NE(new_pos, pos())
      << "Failed precondition of Writer::SeekSlow(): "
         "position unchanged, use Seek() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (new_pos > pos()) {
    if (ABSL_PREDICT_FALSE(uses_secondary_buffer())) return false;
    if (ABSL_PREDICT_FALSE(new_pos > used_size())) {
      MakeDestBuffer(used_size());
      return false;
    }
  } else {
    if (uses_secondary_buffer()) {
      SyncSecondaryBuffer();
      if (ABSL_PREDICT_FALSE(!GrowDestAndMakeBuffer(IntCast<size_t>(pos())))) {
        return false;
      }
      secondary_buffer_.CopyTo(cursor() - secondary_buffer_.size());
      secondary_buffer_.Clear();
    }
    written_size_ = used_size();
  }
  MakeDestBuffer(IntCast<size_t>(new_pos));
  return true;
}

absl::optional<Position> ResizableWriterBase::SizeImpl() {
  if (ABSL_PREDICT_FALSE(!ok())) return absl::nullopt;
  return used_size();
}

bool ResizableWriterBase::TruncateImpl(Position new_size) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (new_size > pos()) {
    if (ABSL_PREDICT_FALSE(uses_secondary_buffer())) return false;
    if (ABSL_PREDICT_FALSE(new_size > used_size())) {
      MakeDestBuffer(used_size());
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
  MakeDestBuffer(IntCast<size_t>(new_size));
  return true;
}

Reader* ResizableWriterBase::ReadModeImpl(Position initial_pos) {
  if (ABSL_PREDICT_FALSE(!ok())) return nullptr;
  if (!uses_secondary_buffer()) {
    MakeDestBuffer(IntCast<size_t>(pos()));
  } else {
    SyncSecondaryBuffer();
    if (ABSL_PREDICT_FALSE(!GrowDestAndMakeBuffer(IntCast<size_t>(pos())))) {
      return nullptr;
    }
    secondary_buffer_.CopyTo(cursor() - secondary_buffer_.size());
    secondary_buffer_.Clear();
  }
  StringReader<>* const reader =
      associated_reader_.ResetReader(start(), used_dest_size());
  reader->Seek(initial_pos);
  return reader;
}

}  // namespace riegeli
