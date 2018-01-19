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
#include <utility>

#include "riegeli/base/assert.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/string_view.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

ChainReader::ChainReader() : src_(&owned_src_), iter_(src_->blocks().cbegin()) {
  MarkClosed();
}

ChainReader::ChainReader(Chain src)
    : owned_src_(std::move(src)),
      src_(&owned_src_),
      iter_(src_->blocks().cbegin()) {
  if (iter_ != src_->blocks().cend()) {
    start_ = iter_->data();
    cursor_ = iter_->data();
    limit_ = iter_->data() + iter_->size();
    limit_pos_ = iter_->size();
  }
}

ChainReader::ChainReader(const Chain* src)
    : src_(src), iter_(src_->blocks().cbegin()) {
  if (iter_ != src_->blocks().cend()) {
    start_ = iter_->data();
    cursor_ = iter_->data();
    limit_ = iter_->data() + iter_->size();
    limit_pos_ = iter_->size();
  }
}

ChainReader::ChainReader(ChainReader&& src) noexcept
    : ChainReader(
          std::move(src),
          static_cast<size_t>(src.iter_ - src.src_->blocks().cbegin())) {}

// block_index is computed early because if src.src_ == &src.owned_src_ then
// *src.src_ is moved, which invalidates src.iter_, and block_index depends on
// src.iter_.
ChainReader::ChainReader(ChainReader&& src, size_t block_index)
    : Reader(std::move(src)),
      owned_src_(std::move(src.owned_src_)),
      src_(src.src_ == &src.owned_src_
               ? &owned_src_
               : riegeli::exchange(src.src_, &src.owned_src_)),
      iter_(src_->blocks().cbegin() + block_index) {
  src.iter_ = src.src_->blocks().cbegin();
}

ChainReader& ChainReader::operator=(ChainReader&& src) noexcept {
  if (&src != this) {
    // block_index is computed early because if src.src_ == &src.owned_src_ then
    // *src.src_ is moved, which invalidates src.iter_, and block_index depends
    // on src.iter_.
    const size_t block_index =
        static_cast<size_t>(src.iter_ - src.src_->blocks().cbegin());
    Reader::operator=(std::move(src));
    owned_src_ = std::move(src.owned_src_);
    src_ = src.src_ == &src.owned_src_
               ? &owned_src_
               : riegeli::exchange(src.src_, &src.owned_src_);
    iter_ = src_->blocks().cbegin() + block_index;
    src.iter_ = src.src_->blocks().cbegin();
  }
  return *this;
}

ChainReader::~ChainReader() = default;

void ChainReader::Done() {
  owned_src_ = Chain();
  src_ = &owned_src_;
  iter_ = src_->blocks().cbegin();
  Reader::Done();
}

bool ChainReader::PullSlow() {
  RIEGELI_ASSERT_EQ(available(), 0u);
  if (RIEGELI_UNLIKELY(iter_ == src_->blocks().cend())) return false;
  while (++iter_ != src_->blocks().cend()) {
    if (RIEGELI_LIKELY(!iter_->empty())) {
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
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy()));
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
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy()));
  const Position length_to_copy = UnsignedMin(length, src_->size() - pos());
  bool ok;
  if (length_to_copy == src_->size()) {
    if (!Seek(length_to_copy)) RIEGELI_UNREACHABLE();
    ok = dest->Write(*src_);
  } else if (length_to_copy <= kMaxBytesToCopy()) {
    char buffer[kMaxBytesToCopy()];
    if (!Read(buffer, length_to_copy)) RIEGELI_UNREACHABLE();
    ok = dest->Write(string_view(buffer, length_to_copy));
  } else {
    Chain data;
    if (!Read(&data, length_to_copy)) RIEGELI_UNREACHABLE();
    ok = dest->Write(std::move(data));
  }
  return ok && length_to_copy == length;
}

bool ChainReader::CopyToSlow(BackwardWriter* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy()));
  if (RIEGELI_UNLIKELY(length > src_->size() - pos())) {
    if (!Seek(src_->size())) RIEGELI_UNREACHABLE();
    return false;
  }
  if (length == src_->size()) {
    if (!Seek(length)) RIEGELI_UNREACHABLE();
    return dest->Write(*src_);
  }
  if (length <= kMaxBytesToCopy()) {
    char buffer[kMaxBytesToCopy()];
    if (!Reader::ReadSlow(buffer, length)) RIEGELI_UNREACHABLE();
    return dest->Write(string_view(buffer, length));
  }
  Chain data;
  if (!ReadSlow(&data, length)) RIEGELI_UNREACHABLE();
  return dest->Write(std::move(data));
}

bool ChainReader::HopeForMoreSlow() const {
  RIEGELI_ASSERT_EQ(available(), 0u);
  return iter_ != src_->blocks().cend();
}

bool ChainReader::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos_);
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
    if (src_->size() - new_pos < new_pos - limit_pos_) {
      // Iterate backwards from the end, it is closer.
      iter_ = src_->blocks().cend();
      limit_pos_ = src_->size();
      --iter_;
      Position block_begin = limit_pos_ - iter_->size();
      while (new_pos < block_begin) {
        limit_pos_ = block_begin;
        --iter_;
        block_begin -= iter_->size();
      }
    } else {
      // Iterate forwards from the current position, it is closer.
      do {
        ++iter_;
        limit_pos_ += iter_->size();
      } while (new_pos > limit_pos_);
    }
  } else {
    // Seeking backwards.
    Position block_begin = start_pos();
    if (new_pos < block_begin - new_pos) {
      // Iterate forwards from the beginning, it is closer.
      iter_ = src_->blocks().cbegin();
      limit_pos_ = iter_->size();
      while (new_pos > limit_pos_) {
        ++iter_;
        limit_pos_ += iter_->size();
      }
    } else {
      // Iterate backwards from the current position, it is closer.
      do {
        limit_pos_ = block_begin;
        --iter_;
        block_begin -= iter_->size();
      } while (new_pos < block_begin);
    }
  }
  start_ = iter_->data();
  limit_ = iter_->data() + iter_->size();
  cursor_ = limit_ - (limit_pos_ - new_pos);
  return true;
}

}  // namespace riegeli
