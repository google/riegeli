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

#include "riegeli/bytes/chain_reader.h"

#include <stddef.h>
#include <limits>
#include <utility>

#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/string_view.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void ChainReader::Done() {
  owned_src_ = Chain();
  src_ = &owned_src_;
  iter_ = src_->blocks().cbegin();
  Reader::Done();
}

bool ChainReader::PullSlow() {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of Reader::PullSlow(): "
         "data available, use Pull() instead";
  RIEGELI_ASSERT_LE(limit_pos_, src_->size())
      << "ChainReader source changed unexpectedly";
  if (RIEGELI_UNLIKELY(iter_ == src_->blocks().cend())) return false;
  while (++iter_ != src_->blocks().cend()) {
    if (RIEGELI_LIKELY(!iter_->empty())) {
      RIEGELI_ASSERT_LE(iter_->size(), src_->size() - limit_pos_)
          << "ChainReader source changed unexpectedly";
      start_ = iter_->data();
      cursor_ = iter_->data();
      limit_ = iter_->data() + iter_->size();
      limit_pos_ += iter_->size();
      return true;
    }
  }
  start_ = nullptr;
  cursor_ = nullptr;
  limit_ = nullptr;
  return false;
}

bool ChainReader::ReadSlow(Chain* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy()))
      << "Failed precondition of Reader::ReadSlow(Chain*): "
         "length too small, use Read(Chain*) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Reader::ReadSlow(Chain*): "
         "Chain size overflow";
  RIEGELI_ASSERT_LE(limit_pos_, src_->size())
      << "ChainReader source changed unexpectedly";
  const size_t size_hint = dest->size() + length;
  if (length <= available()) {
    iter_.AppendSubstrTo(string_view(cursor_, length), dest, size_hint);
    cursor_ += length;
    return true;
  }
  if (RIEGELI_UNLIKELY(iter_ == src_->blocks().cend())) return false;
  iter_.AppendSubstrTo(string_view(cursor_, available()), dest, size_hint);
  length -= available();
  for (;;) {
    if (RIEGELI_UNLIKELY(++iter_ == src_->blocks().cend())) {
      start_ = nullptr;
      cursor_ = nullptr;
      limit_ = nullptr;
      return false;
    }
    RIEGELI_ASSERT_LE(iter_->size(), src_->size() - limit_pos_)
        << "ChainReader source changed unexpectedly";
    limit_pos_ += iter_->size();
    if (length <= iter_->size()) {
      start_ = iter_->data();
      cursor_ = iter_->data() + length;
      limit_ = iter_->data() + iter_->size();
      iter_.AppendSubstrTo(string_view(start_, length), dest, size_hint);
      return true;
    }
    iter_.AppendTo(dest, size_hint);
    length -= iter_->size();
  }
}

bool ChainReader::CopyToSlow(Writer* dest, Position length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy()))
      << "Failed precondition of Reader::CopyToSlow(Writer*): "
         "length too small, use CopyTo(Writer*) instead";
  RIEGELI_ASSERT_LE(limit_pos_, src_->size())
      << "ChainReader source changed unexpectedly";
  const Position length_to_copy = UnsignedMin(length, src_->size() - pos());
  bool ok;
  if (length_to_copy == src_->size()) {
    if (!Skip(length_to_copy)) {
      RIEGELI_ASSERT_UNREACHABLE() << "ChainReader::Skip() failed";
    }
    ok = dest->Write(*src_);
  } else if (length_to_copy <= kMaxBytesToCopy()) {
    char buffer[kMaxBytesToCopy()];
    if (!Read(buffer, IntCast<size_t>(length_to_copy))) {
      RIEGELI_ASSERT_UNREACHABLE() << "ChainReader::Read(char*) failed";
    }
    ok = dest->Write(string_view(buffer, IntCast<size_t>(length_to_copy)));
  } else {
    Chain data;
    if (!Read(&data, IntCast<size_t>(length_to_copy))) {
      RIEGELI_ASSERT_UNREACHABLE() << "ChainReader::Read(Chain*) failed";
    }
    ok = dest->Write(std::move(data));
  }
  return ok && length_to_copy == length;
}

bool ChainReader::CopyToSlow(BackwardWriter* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy()))
      << "Failed precondition of Reader::CopyToSlow(BackwardWriter*): "
         "length too small, use CopyTo(BackwardWriter*) instead";
  RIEGELI_ASSERT_LE(limit_pos_, src_->size())
      << "ChainReader source changed unexpectedly";
  if (RIEGELI_UNLIKELY(length > src_->size() - pos())) {
    if (!Seek(src_->size())) {
      RIEGELI_ASSERT_UNREACHABLE() << "ChainReader::Seek() failed";
    }
    return false;
  }
  if (length == src_->size()) {
    if (!Skip(length)) {
      RIEGELI_ASSERT_UNREACHABLE() << "ChainReader::Skip() failed";
    }
    return dest->Write(*src_);
  }
  if (length <= kMaxBytesToCopy()) {
    char buffer[kMaxBytesToCopy()];
    if (!Reader::ReadSlow(buffer, length)) {
      RIEGELI_ASSERT_UNREACHABLE() << "ChainReader::ReadSlow(char*) failed";
    }
    return dest->Write(string_view(buffer, length));
  }
  Chain data;
  if (!ReadSlow(&data, length)) {
    RIEGELI_ASSERT_UNREACHABLE() << "ChainReader::ReadSlow(Chain*) failed";
  }
  return dest->Write(std::move(data));
}

bool ChainReader::HopeForMoreSlow() const {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of Reader::HopeForMoreSlow(): "
         "data available, use HopeForMore() instead";
  return iter_ != src_->blocks().cend();
}

bool ChainReader::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos_)
      << "Failed precondition of Reader::SeekSlow(): "
         "position in the buffer, use Seek() instead";
  RIEGELI_ASSERT_LE(limit_pos_, src_->size())
      << "ChainReader source changed unexpectedly";
  if (new_pos > limit_pos_) {
    // Seeking forwards.
    if (RIEGELI_UNLIKELY(new_pos > src_->size())) {
      // Source ends.
      iter_ = src_->blocks().cend();
      start_ = nullptr;
      cursor_ = nullptr;
      limit_ = nullptr;
      limit_pos_ = src_->size();
      return false;
    }
    RIEGELI_ASSERT(healthy()) << "Failed invariant of ChainReader: "
                                 "unhealthy but has non-zero source size";
    if (src_->size() - new_pos < new_pos - limit_pos_) {
      // Iterate backwards from the end, it is closer.
      iter_ = src_->blocks().cend();
      limit_pos_ = src_->size();
      RIEGELI_ASSERT(iter_ != src_->blocks().cbegin()) << "Malformed Chain";
      --iter_;
      RIEGELI_ASSERT_LE(iter_->size(), limit_pos_) << "Malformed Chain";
      Position block_begin = limit_pos_ - iter_->size();
      while (new_pos < block_begin) {
        limit_pos_ = block_begin;
        RIEGELI_ASSERT(iter_ != src_->blocks().cbegin()) << "Malformed Chain";
        --iter_;
        RIEGELI_ASSERT_LE(iter_->size(), block_begin) << "Malformed Chain";
        block_begin -= iter_->size();
      }
    } else {
      // Iterate forwards from the current position, it is closer.
      do {
        RIEGELI_ASSERT(iter_ != src_->blocks().cend())
            << "ChainReader source changed unexpectedly or is malformed";
        ++iter_;
        RIEGELI_ASSERT_LE(iter_->size(), src_->size() - limit_pos_)
            << "ChainReader source changed unexpectedly or is malformed";
        limit_pos_ += iter_->size();
      } while (new_pos > limit_pos_);
    }
  } else {
    // Seeking backwards.
    RIEGELI_ASSERT(healthy()) << "Failed invariant of ChainReader: "
                                 "unhealthy but has non-zero position";
    Position block_begin = start_pos();
    if (new_pos < block_begin - new_pos) {
      // Iterate forwards from the beginning, it is closer.
      iter_ = src_->blocks().cbegin();
      limit_pos_ = iter_->size();
      while (new_pos > limit_pos_) {
        RIEGELI_ASSERT(iter_ != src_->blocks().cend()) << "Malformed Chain";
        ++iter_;
        RIEGELI_ASSERT_LE(iter_->size(), src_->size() - limit_pos_)
            << "Malformed Chain";
        limit_pos_ += iter_->size();
      }
    } else {
      // Iterate backwards from the current position, it is closer.
      do {
        limit_pos_ = block_begin;
        RIEGELI_ASSERT(iter_ != src_->blocks().cbegin())
            << "ChainReader source changed unexpectedly or is malformed";
        --iter_;
        RIEGELI_ASSERT_LE(iter_->size(), block_begin)
            << "ChainReader source changed unexpectedly or is malformed";
        block_begin -= iter_->size();
      } while (new_pos < block_begin);
    }
  }
  start_ = iter_->data();
  limit_ = iter_->data() + iter_->size();
  RIEGELI_ASSERT_GE(limit_pos_, new_pos)
      << "ChainReader::SeekSlow() stopped before the target";
  const Position available_length = limit_pos_ - new_pos;
  RIEGELI_ASSERT_LE(available_length, buffer_size())
      << "ChainReader::SeekSlow() stopped after the target";
  cursor_ = limit_ - available_length;
  return true;
}

}  // namespace riegeli
