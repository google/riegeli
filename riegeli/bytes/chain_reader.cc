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

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/pullable_reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void ChainReaderBase::Done() {
  PullableReader::Done();
  iter_ = Chain::BlockIterator();
}

bool ChainReaderBase::PullSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_GT(min_length, available())
      << "Failed precondition of Reader::PullSlow(): "
         "length too small, use Pull() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(!PullUsingScratch(min_length))) {
    return available() >= min_length;
  }
  const Chain* const src = iter_.chain();
  RIEGELI_ASSERT_LE(limit_pos_, src->size())
      << "ChainReader source changed unexpectedly";
  if (ABSL_PREDICT_FALSE(iter_ == src->blocks().cend())) return false;
  while (++iter_ != src->blocks().cend()) {
    if (ABSL_PREDICT_TRUE(!iter_->empty())) {
      RIEGELI_ASSERT_LE(iter_->size(), src->size() - limit_pos_)
          << "ChainReader source changed unexpectedly";
      start_ = iter_->data();
      cursor_ = start_;
      limit_ = start_ + iter_->size();
      limit_pos_ += available();
      return true;
    }
  }
  start_ = nullptr;
  cursor_ = nullptr;
  limit_ = nullptr;
  return false;
}

bool ChainReaderBase::ReadSlow(Chain* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::ReadSlow(Chain*): "
         "length too small, use Read(Chain*) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Reader::ReadSlow(Chain*): "
         "Chain size overflow";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(!ReadScratch(dest, &length))) return length == 0;
  const Chain* const src = iter_.chain();
  RIEGELI_ASSERT_LE(limit_pos_, src->size())
      << "ChainReader source changed unexpectedly";
  if (length <= available()) {
    iter_.AppendSubstrTo(absl::string_view(cursor_, length), dest);
    cursor_ += length;
    return true;
  }
  if (ABSL_PREDICT_FALSE(iter_ == src->blocks().cend())) return false;
  iter_.AppendSubstrTo(absl::string_view(cursor_, available()), dest);
  length -= available();
  for (;;) {
    if (ABSL_PREDICT_FALSE(++iter_ == src->blocks().cend())) {
      start_ = nullptr;
      cursor_ = nullptr;
      limit_ = nullptr;
      return false;
    }
    RIEGELI_ASSERT_LE(iter_->size(), src->size() - limit_pos_)
        << "ChainReader source changed unexpectedly";
    limit_pos_ += iter_->size();
    if (length <= iter_->size()) {
      start_ = iter_->data();
      cursor_ = start_ + length;
      limit_ = start_ + iter_->size();
      iter_.AppendSubstrTo(absl::string_view(start_, length), dest);
      return true;
    }
    iter_.AppendTo(dest);
    length -= iter_->size();
  }
}

bool ChainReaderBase::CopyToSlow(Writer* dest, Position length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::CopyToSlow(Writer*): "
         "length too small, use CopyTo(Writer*) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  const Chain* const src = iter_.chain();
  RIEGELI_ASSERT_LE(limit_pos_, src->size())
      << "ChainReader source changed unexpectedly";
  const Position length_to_copy = UnsignedMin(length, src->size() - pos());
  bool ok;
  if (length_to_copy == src->size()) {
    if (!Skip(length_to_copy)) {
      RIEGELI_ASSERT_UNREACHABLE() << "ChainReader::Skip() failed";
    }
    ok = dest->Write(*src);
  } else if (length_to_copy <= kMaxBytesToCopy) {
    if (ABSL_PREDICT_FALSE(!dest->Push(IntCast<size_t>(length_to_copy)))) {
      return false;
    }
    if (!Read(dest->cursor(), IntCast<size_t>(length_to_copy))) {
      RIEGELI_ASSERT_UNREACHABLE() << "ChainReader::Read(char*) failed";
    }
    dest->set_cursor(dest->cursor() + IntCast<size_t>(length_to_copy));
    ok = true;
  } else {
    Chain data;
    if (!Read(&data, IntCast<size_t>(length_to_copy))) {
      RIEGELI_ASSERT_UNREACHABLE() << "ChainReader::Read(Chain*) failed";
    }
    ok = dest->Write(std::move(data));
  }
  return ok && length_to_copy == length;
}

bool ChainReaderBase::CopyToSlow(BackwardWriter* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::CopyToSlow(BackwardWriter*): "
         "length too small, use CopyTo(BackwardWriter*) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  const Chain* const src = iter_.chain();
  RIEGELI_ASSERT_LE(limit_pos_, src->size())
      << "ChainReader source changed unexpectedly";
  if (ABSL_PREDICT_FALSE(length > src->size() - pos())) {
    if (!Seek(src->size())) {
      RIEGELI_ASSERT_UNREACHABLE() << "ChainReader::Seek() failed";
    }
    return false;
  }
  if (length == src->size()) {
    if (!Skip(length)) {
      RIEGELI_ASSERT_UNREACHABLE() << "ChainReader::Skip() failed";
    }
    return dest->Write(*src);
  }
  if (length <= kMaxBytesToCopy) {
    if (ABSL_PREDICT_FALSE(!dest->Push(length))) return false;
    dest->set_cursor(dest->cursor() - length);
    if (ABSL_PREDICT_FALSE(!ReadSlow(dest->cursor(), length))) {
      dest->set_cursor(dest->cursor() + length);
      return false;
    }
    return true;
  }
  Chain data;
  if (!ReadSlow(&data, length)) {
    RIEGELI_ASSERT_UNREACHABLE() << "ChainReader::ReadSlow(Chain*) failed";
  }
  return dest->Write(std::move(data));
}

bool ChainReaderBase::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos_)
      << "Failed precondition of Reader::SeekSlow(): "
         "position in the buffer, use Seek() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  SyncScratch();
  const Chain* const src = iter_.chain();
  RIEGELI_ASSERT_LE(limit_pos_, src->size())
      << "ChainReader source changed unexpectedly";
  if (new_pos > limit_pos_) {
    // Seeking forwards.
    if (ABSL_PREDICT_FALSE(new_pos > src->size())) {
      // Source ends.
      iter_ = src->blocks().cend();
      start_ = nullptr;
      cursor_ = nullptr;
      limit_ = nullptr;
      limit_pos_ = src->size();
      return false;
    }
    if (src->size() - new_pos < new_pos - limit_pos_) {
      // Iterate backwards from the end, it is closer.
      iter_ = src->blocks().cend();
      limit_pos_ = src->size();
      RIEGELI_ASSERT(iter_ != src->blocks().cbegin()) << "Malformed Chain";
      --iter_;
      RIEGELI_ASSERT_LE(iter_->size(), limit_pos_) << "Malformed Chain";
      Position block_begin = limit_pos_ - iter_->size();
      while (new_pos < block_begin) {
        limit_pos_ = block_begin;
        RIEGELI_ASSERT(iter_ != src->blocks().cbegin()) << "Malformed Chain";
        --iter_;
        RIEGELI_ASSERT_LE(iter_->size(), block_begin) << "Malformed Chain";
        block_begin -= iter_->size();
      }
    } else {
      // Iterate forwards from the current position, it is closer.
      do {
        RIEGELI_ASSERT(iter_ != src->blocks().cend())
            << "ChainReader source changed unexpectedly or is malformed";
        ++iter_;
        RIEGELI_ASSERT_LE(iter_->size(), src->size() - limit_pos_)
            << "ChainReader source changed unexpectedly or is malformed";
        limit_pos_ += iter_->size();
      } while (new_pos > limit_pos_);
    }
  } else {
    // Seeking backwards.
    Position block_begin = start_pos();
    if (new_pos < block_begin - new_pos) {
      // Iterate forwards from the beginning, it is closer.
      iter_ = src->blocks().cbegin();
      limit_pos_ = iter_->size();
      while (new_pos > limit_pos_) {
        RIEGELI_ASSERT(iter_ != src->blocks().cend()) << "Malformed Chain";
        ++iter_;
        RIEGELI_ASSERT_LE(iter_->size(), src->size() - limit_pos_)
            << "Malformed Chain";
        limit_pos_ += iter_->size();
      }
    } else {
      // Iterate backwards from the current position, it is closer.
      do {
        limit_pos_ = block_begin;
        RIEGELI_ASSERT(iter_ != src->blocks().cbegin())
            << "ChainReader source changed unexpectedly or is malformed";
        --iter_;
        RIEGELI_ASSERT_LE(iter_->size(), block_begin)
            << "ChainReader source changed unexpectedly or is malformed";
        block_begin -= iter_->size();
      } while (new_pos < block_begin);
    }
  }
  start_ = iter_->data();
  limit_ = start_ + iter_->size();
  RIEGELI_ASSERT_GE(limit_pos_, new_pos)
      << "ChainReader::SeekSlow() stopped before the target";
  const Position available_length = limit_pos_ - new_pos;
  RIEGELI_ASSERT_LE(available_length, buffer_size())
      << "ChainReader::SeekSlow() stopped after the target";
  cursor_ = limit_ - available_length;
  return true;
}

bool ChainReaderBase::Size(Position* size) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  const Chain* const src = iter_.chain();
  *size = src->size();
  return true;
}

template class ChainReader<const Chain*>;
template class ChainReader<Chain>;

}  // namespace riegeli
