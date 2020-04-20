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
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
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
  if (ABSL_PREDICT_FALSE(!PullUsingScratch(min_length, recommended_length))) {
    return available() >= min_length;
  }
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  const Chain* const src = iter_.chain();
  RIEGELI_ASSERT_LE(limit_pos(), src->size())
      << "ChainReader source changed unexpectedly";
  if (ABSL_PREDICT_FALSE(iter_ == src->blocks().cend())) return false;
  while (++iter_ != src->blocks().cend()) {
    if (ABSL_PREDICT_TRUE(!iter_->empty())) {
      RIEGELI_ASSERT_LE(iter_->size(), src->size() - limit_pos())
          << "ChainReader source changed unexpectedly";
      set_buffer(iter_->data(), iter_->size());
      move_limit_pos(available());
      return true;
    }
  }
  set_buffer();
  return false;
}

bool ChainReaderBase::ReadSlow(Chain* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::ReadSlow(Chain*): "
         "length too small, use Read(Chain*) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Reader::ReadSlow(Chain*): "
         "Chain size overflow";
  if (ABSL_PREDICT_FALSE(!ReadScratch(dest, &length))) return length == 0;
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  const Chain* const src = iter_.chain();
  RIEGELI_ASSERT_LE(limit_pos(), src->size())
      << "ChainReader source changed unexpectedly";
  if (length <= available()) {
    iter_.AppendSubstrTo(absl::string_view(cursor(), length), dest);
    move_cursor(length);
    return true;
  }
  if (ABSL_PREDICT_FALSE(iter_ == src->blocks().cend())) return false;
  iter_.AppendSubstrTo(absl::string_view(cursor(), available()), dest);
  length -= available();
  while (++iter_ != src->blocks().cend()) {
    RIEGELI_ASSERT_LE(iter_->size(), src->size() - limit_pos())
        << "ChainReader source changed unexpectedly";
    move_limit_pos(iter_->size());
    if (length <= iter_->size()) {
      set_buffer(iter_->data(), iter_->size(), length);
      iter_.AppendSubstrTo(absl::string_view(start(), length), dest);
      return true;
    }
    iter_.AppendTo(dest);
    length -= iter_->size();
  }
  set_buffer();
  return false;
}

bool ChainReaderBase::ReadSlow(absl::Cord* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::ReadSlow(Cord*): "
         "length too small, use Read(Cord*) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Reader::ReadSlow(Cord*): "
         "Cord size overflow";
  if (ABSL_PREDICT_FALSE(!ReadScratch(dest, &length))) return length == 0;
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  const Chain* const src = iter_.chain();
  RIEGELI_ASSERT_LE(limit_pos(), src->size())
      << "ChainReader source changed unexpectedly";
  if (length <= available()) {
    iter_.AppendSubstrTo(absl::string_view(cursor(), length), dest);
    move_cursor(length);
    return true;
  }
  if (ABSL_PREDICT_FALSE(iter_ == src->blocks().cend())) return false;
  iter_.AppendSubstrTo(absl::string_view(cursor(), available()), dest);
  length -= available();
  while (++iter_ != src->blocks().cend()) {
    RIEGELI_ASSERT_LE(iter_->size(), src->size() - limit_pos())
        << "ChainReader source changed unexpectedly";
    move_limit_pos(iter_->size());
    if (length <= iter_->size()) {
      set_buffer(iter_->data(), iter_->size(), length);
      iter_.AppendSubstrTo(absl::string_view(start(), length), dest);
      return true;
    }
    iter_.AppendTo(dest);
    length -= iter_->size();
  }
  set_buffer();
  return false;
}

bool ChainReaderBase::CopyToSlow(Writer* dest, Position length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::CopyToSlow(Writer*): "
         "length too small, use CopyTo(Writer*) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  const Chain* const src = iter_.chain();
  RIEGELI_ASSERT_LE(limit_pos(), src->size())
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
    dest->move_cursor(IntCast<size_t>(length_to_copy));
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
  RIEGELI_ASSERT_LE(limit_pos(), src->size())
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
    dest->move_cursor(length);
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
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of Reader::SeekSlow(): "
         "position in the buffer, use Seek() instead";
  if (ABSL_PREDICT_FALSE(!SeekUsingScratch(new_pos))) return true;
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  const Chain* const src = iter_.chain();
  RIEGELI_ASSERT_LE(limit_pos(), src->size())
      << "ChainReader source changed unexpectedly";
  if (new_pos >= src->size()) {
    // Source ends.
    iter_ = src->blocks().cend();
    set_limit_pos(src->size());
    set_buffer();
    return new_pos == src->size();
  }
  const Chain::CharPosition char_pos = src->FindPosition(new_pos);
  iter_ = char_pos.block_iter;
  set_buffer(iter_->data(), iter_->size(), char_pos.char_index);
  set_limit_pos(new_pos + available());
  return true;
}

absl::optional<Position> ChainReaderBase::Size() {
  if (ABSL_PREDICT_FALSE(!healthy())) return absl::nullopt;
  const Chain* const src = iter_.chain();
  return src->size();
}

}  // namespace riegeli
