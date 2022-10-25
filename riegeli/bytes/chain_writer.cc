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

#include "riegeli/bytes/chain_writer.h"

#include <stddef.h>

#include <limits>
#include <memory>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void ChainWriterBase::Done() {
  ChainWriterBase::FlushImpl(FlushType::kFromObject);
  Writer::Done();
  tail_.reset();
  associated_reader_.Reset();
}

inline void ChainWriterBase::SyncBuffer(Chain& dest) {
  if (ABSL_PREDICT_FALSE(HasAppendedTail(dest))) {
    ExtractTail(dest);
    return;
  }
  ShrinkTail(start_to_cursor());
  set_start_pos(pos());
  dest.RemoveSuffix(available(), options_);
  set_buffer();
}

inline void ChainWriterBase::MakeBuffer(Chain& dest, size_t min_length,
                                        size_t recommended_length) {
  const absl::Span<char> buffer = dest.AppendBuffer(
      min_length, recommended_length, Chain::kAnyLength, options_);
  set_buffer(buffer.data(), buffer.size());
}

inline void ChainWriterBase::MoveFromTail(size_t length, Chain& dest) {
  RIEGELI_ASSERT(tail_ != nullptr)
      << "Failed precondition of ChainWriterBase::MoveFromTail(): no tail";
  RIEGELI_ASSERT(length <= tail_->size())
      << "Failed precondition of ChainWriterBase::MoveFromTail(): "
         "length longer than the tail";
  if (length == tail_->size()) {
    dest.Append(std::move(*tail_), options_);
    tail_->Clear();
    return;
  }
  Chain::BlockIterator iter = tail_->blocks().cbegin();
  size_t remaining = length;
  while (remaining > iter->size()) {
    iter.AppendTo(dest, options_);
    remaining -= iter->size();
    ++iter;
  }
  iter.AppendSubstrTo(absl::string_view(iter->data(), remaining), dest,
                      options_);
  tail_->RemovePrefix(length, options_);
}

inline void ChainWriterBase::MoveToTail(size_t length, Chain& dest) {
  RIEGELI_ASSERT(length <= dest.size())
      << "Failed precondition of ChainWriterBase::MoveToTail(): "
         "length longer than the destination";
  if (tail_ == nullptr) tail_ = std::make_unique<Chain>();
  if (length == dest.size()) {
    tail_->Prepend(std::move(dest), options_);
    dest.Clear();
    return;
  }
  Chain::BlockIterator iter = dest.blocks().cend();
  size_t remaining = length;
  for (;;) {
    --iter;
    if (remaining <= iter->size()) break;
    iter.PrependTo(*tail_, options_);
    remaining -= iter->size();
  }
  iter.PrependSubstrTo(
      absl::string_view(iter->data() + iter->size() - remaining, remaining),
      *tail_, options_);
  dest.RemoveSuffix(length, options_);
}

inline bool ChainWriterBase::HasAppendedTail(const Chain& dest) const {
  return limit_pos() < dest.size();
}

inline void ChainWriterBase::ExtractTail(Chain& dest) {
  RIEGELI_ASSERT(HasAppendedTail(dest))
      << "Failed precondition of ChainWriterBase::ExtractTail(): "
         "the tail is not appended";
  RIEGELI_ASSERT(start() == nullptr)
      << "Failed invariant of ChainWriterBase: "
         "both a buffer and the appended tail are present";
  MoveToTail(dest.size() - IntCast<size_t>(start_pos()), dest);
}

inline void ChainWriterBase::AppendTail(Chain& dest) {
  RIEGELI_ASSERT(!HasAppendedTail(dest))
      << "Failed precondition of ChainWriterBase::AppendTail(): "
         "the tail is appended";
  if (ABSL_PREDICT_FALSE(tail_ != nullptr)) {
    dest.Append(std::move(*tail_), options_);
    tail_->Clear();
  }
}

inline void ChainWriterBase::ShrinkTail(size_t length) {
  if (ABSL_PREDICT_FALSE(tail_ != nullptr)) {
    tail_->RemovePrefix(UnsignedMin(length, tail_->size()), options_);
  }
}

void ChainWriterBase::SetWriteSizeHintImpl(
    absl::optional<Position> write_size_hint) {
  options_.set_size_hint(
      write_size_hint == absl::nullopt
          ? 0
          : SaturatingIntCast<size_t>(SaturatingAdd(pos(), *write_size_hint)));
}

bool ChainWriterBase::PushSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Writer::PushSlow(): "
         "enough space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Chain& dest = *DestChain();
  RIEGELI_ASSERT_LE(limit_pos(), dest.size())
      << "ChainWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(min_length >
                         std::numeric_limits<size_t>::max() - dest.size())) {
    return FailOverflow();
  }
  SyncBuffer(dest);
  MakeBuffer(dest, min_length, recommended_length);
  return true;
}

bool ChainWriterBase::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain): "
         "enough space available, use Write(Chain) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Chain& dest = *DestChain();
  RIEGELI_ASSERT_LE(limit_pos(), dest.size())
      << "ChainWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  SyncBuffer(dest);
  ShrinkTail(src.size());
  move_start_pos(src.size());
  dest.Append(src, options_);
  MakeBuffer(dest);
  return true;
}

bool ChainWriterBase::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain&&): "
         "enough space available, use Write(Chain&&) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Chain& dest = *DestChain();
  RIEGELI_ASSERT_LE(limit_pos(), dest.size())
      << "ChainWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  SyncBuffer(dest);
  ShrinkTail(src.size());
  move_start_pos(src.size());
  dest.Append(std::move(src), options_);
  MakeBuffer(dest);
  return true;
}

bool ChainWriterBase::WriteSlow(const absl::Cord& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Cord): "
         "enough space available, use Write(Cord) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Chain& dest = *DestChain();
  RIEGELI_ASSERT_LE(limit_pos(), dest.size())
      << "ChainWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  SyncBuffer(dest);
  ShrinkTail(src.size());
  move_start_pos(src.size());
  dest.Append(src, options_);
  MakeBuffer(dest);
  return true;
}

bool ChainWriterBase::WriteSlow(absl::Cord&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Cord&&): "
         "enough space available, use Write(Cord&&) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Chain& dest = *DestChain();
  RIEGELI_ASSERT_LE(limit_pos(), dest.size())
      << "ChainWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  SyncBuffer(dest);
  ShrinkTail(src.size());
  move_start_pos(src.size());
  dest.Append(std::move(src), options_);
  MakeBuffer(dest);
  return true;
}

bool ChainWriterBase::WriteZerosSlow(Position length) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Writer::WriteZerosSlow(): "
         "enough space available, use WriteZeros() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Chain& dest = *DestChain();
  RIEGELI_ASSERT_LE(limit_pos(), dest.size())
      << "ChainWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(length > std::numeric_limits<size_t>::max() -
                                      IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  SyncBuffer(dest);
  ShrinkTail(length);
  move_start_pos(length);
  dest.Append(ChainOfZeros(IntCast<size_t>(length)), options_);
  MakeBuffer(dest);
  return true;
}

bool ChainWriterBase::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Chain& dest = *DestChain();
  RIEGELI_ASSERT_LE(limit_pos(), dest.size())
      << "ChainWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(HasAppendedTail(dest))) {
    RIEGELI_ASSERT(start() == nullptr)
        << "Failed invariant of ChainWriterBase: "
           "both a buffer and the appended tail are present";
    RIEGELI_ASSERT(tail_ == nullptr || tail_->empty())
        << "Failed invariant of ChainWriterBase: "
           "the tail is both appended and separated";
    return true;
  }
  SyncBuffer(dest);
  AppendTail(dest);
  return true;
}

bool ChainWriterBase::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT_NE(new_pos, pos())
      << "Failed precondition of Writer::SeekSlow(): "
         "position unchanged, use Seek() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Chain& dest = *DestChain();
  RIEGELI_ASSERT_LE(limit_pos(), dest.size())
      << "ChainWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(HasAppendedTail(dest))) {
    RIEGELI_ASSERT(start() == nullptr)
        << "Failed invariant of ChainWriterBase: "
           "both a buffer and the appended tail are present";
    RIEGELI_ASSERT(tail_ == nullptr || tail_->empty())
        << "Failed invariant of ChainWriterBase: "
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

absl::optional<Position> ChainWriterBase::SizeImpl() {
  if (ABSL_PREDICT_FALSE(!ok())) return absl::nullopt;
  Chain& dest = *DestChain();
  RIEGELI_ASSERT_LE(limit_pos(), dest.size())
      << "ChainWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(HasAppendedTail(dest))) {
    RIEGELI_ASSERT(start() == nullptr)
        << "Failed invariant of ChainWriterBase: "
           "both a buffer and the appended tail are present";
    RIEGELI_ASSERT(tail_ == nullptr || tail_->empty())
        << "Failed invariant of ChainWriterBase: "
           "the tail is both appended and separated";
    return dest.size();
  }
  if (ABSL_PREDICT_FALSE(tail_ != nullptr)) {
    return UnsignedMax(pos(), start_pos() + tail_->size());
  }
  return pos();
}

bool ChainWriterBase::TruncateImpl(Position new_size) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Chain& dest = *DestChain();
  RIEGELI_ASSERT_LE(limit_pos(), dest.size())
      << "ChainWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(HasAppendedTail(dest))) {
    RIEGELI_ASSERT(start() == nullptr)
        << "Failed invariant of ChainWriterBase: "
           "both a buffer and the appended tail are present";
    RIEGELI_ASSERT(tail_ == nullptr || tail_->empty())
        << "Failed invariant of ChainWriterBase: "
           "the tail is both appended and separated";
    if (ABSL_PREDICT_FALSE(new_size > dest.size())) return false;
  } else if (new_size > pos()) {
    if (ABSL_PREDICT_TRUE(tail_ == nullptr) || tail_->empty()) return false;
    SyncBuffer(dest);
    if (ABSL_PREDICT_FALSE(new_size > dest.size() + tail_->size())) {
      move_start_pos(tail_->size());
      AppendTail(dest);
      return false;
    }
    set_start_pos(new_size);
    tail_->RemoveSuffix(dest.size() + tail_->size() - IntCast<size_t>(new_size),
                        options_);
    AppendTail(dest);
    return true;
  } else {
    if (ABSL_PREDICT_FALSE(tail_ != nullptr)) tail_->Clear();
    if (new_size >= start_pos()) {
      set_cursor(start() + (new_size - start_pos()));
      return true;
    }
  }
  set_start_pos(new_size);
  dest.RemoveSuffix(dest.size() - IntCast<size_t>(new_size), options_);
  set_buffer();
  return true;
}

Reader* ChainWriterBase::ReadModeImpl(Position initial_pos) {
  if (ABSL_PREDICT_FALSE(!ChainWriterBase::FlushImpl(FlushType::kFromObject))) {
    return nullptr;
  }
  Chain& dest = *DestChain();
  ChainReader<>* const reader = associated_reader_.ResetReader(&dest);
  reader->Seek(initial_pos);
  return reader;
}

}  // namespace riegeli
