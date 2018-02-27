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

#include "riegeli/bytes/limiting_reader.h"

#include <stddef.h>
#include <limits>

#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

LimitingReader::LimitingReader(Reader* src, Position size_limit)
    : Reader(State::kOpen),
      src_(RIEGELI_ASSERT_NOTNULL(src)),
      size_limit_(size_limit) {
  RIEGELI_ASSERT_GE(size_limit, src_->pos())
      << "Failed precondition of LimitingReader::LimitingReader(): "
         "size limit smaller than current position";
  if (src_->GetTypeId() == TypeId::For<LimitingReader>() &&
      RIEGELI_LIKELY(src_->healthy())) {
    wrapped_ = static_cast<LimitingReader*>(src_);
    // src is already a LimitingReader: refer to its source instead, so that
    // creating a stack of LimitingReaders avoids iterating through the stack
    // in each virtual function call.
    wrapped_->src_->set_cursor(wrapped_->cursor_);
    src_ = wrapped_->src_;
    size_limit_ = UnsignedMin(size_limit_, wrapped_->size_limit_);
  }
  SyncBuffer();
}

void LimitingReader::Done() {
  if (RIEGELI_LIKELY(healthy())) src_->set_cursor(cursor_);
  if (wrapped_ != nullptr) {
    wrapped_->SyncBuffer();
    wrapped_ = nullptr;
  }
  src_ = nullptr;
  size_limit_ = 0;
  Reader::Done();
}

TypeId LimitingReader::GetTypeId() const {
  return TypeId::For<LimitingReader>();
}

bool LimitingReader::PullSlow() {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of Reader::PullSlow(): "
         "data available, use Pull() instead";
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  src_->set_cursor(cursor_);
  if (RIEGELI_UNLIKELY(limit_pos_ == size_limit_)) return false;
  const bool ok = src_->Pull();
  SyncBuffer();
  return ok;
}

bool LimitingReader::ReadSlow(char* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, available())
      << "Failed precondition of Reader::ReadSlow(char*): "
         "length too small, use Read(char*) instead";
  return ReadInternal(dest, length);
}

bool LimitingReader::ReadSlow(Chain* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy()))
      << "Failed precondition of Reader::ReadSlow(Chain*): "
         "length too small, use Read(Chain*) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Reader::ReadSlow(Chain*): "
         "Chain size overflow";
  return ReadInternal(dest, length);
}

template <typename Dest>
bool LimitingReader::ReadInternal(Dest* dest, size_t length) {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  src_->set_cursor(cursor_);
  RIEGELI_ASSERT_LE(pos(), size_limit_)
      << "Failed invariant of LimitingReader: position exceeds size limit";
  const size_t length_to_read = UnsignedMin(length, size_limit_ - pos());
  const bool ok = src_->Read(dest, length_to_read);
  SyncBuffer();
  return ok && length_to_read == length;
}

bool LimitingReader::CopyToSlow(Writer* dest, Position length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy()))
      << "Failed precondition of Reader::CopyToSlow(Writer*): "
         "length too small, use CopyTo(Writer*) instead";
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  src_->set_cursor(cursor_);
  RIEGELI_ASSERT_LE(pos(), size_limit_)
      << "Failed invariant of LimitingReader: position exceeds size limit";
  const Position length_to_copy = UnsignedMin(length, size_limit_ - pos());
  const bool ok = src_->CopyTo(dest, length_to_copy);
  SyncBuffer();
  return ok && length_to_copy == length;
}

bool LimitingReader::CopyToSlow(BackwardWriter* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy()))
      << "Failed precondition of Reader::CopyToSlow(BackwardWriter*): "
         "length too small, use CopyTo(BackwardWriter*) instead";
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  src_->set_cursor(cursor_);
  RIEGELI_ASSERT_LE(pos(), size_limit_)
      << "Failed invariant of LimitingReader: position exceeds size limit";
  if (RIEGELI_UNLIKELY(length > size_limit_ - pos())) {
    src_->Seek(size_limit_);
    SyncBuffer();
    return false;
  }
  const bool ok = src_->CopyTo(dest, length);
  SyncBuffer();
  return ok;
}

bool LimitingReader::HopeForMoreSlow() const {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of Reader::HopeForMoreSlow(): "
         "data available, use HopeForMore() instead";
  return pos() < size_limit_ && src_->HopeForMore();
}

bool LimitingReader::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos_)
      << "Failed precondition of Reader::SeekSlow(): "
         "position in the buffer, use Seek() instead";
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  src_->set_cursor(cursor_);
  const Position pos_to_seek = UnsignedMin(new_pos, size_limit_);
  const bool ok = src_->Seek(pos_to_seek);
  SyncBuffer();
  return ok && pos_to_seek == new_pos;
}

inline void LimitingReader::SyncBuffer() {
  start_ = src_->start();
  cursor_ = src_->cursor();
  limit_ = src_->limit();
  limit_pos_ = src_->pos() + src_->available();  // src_->limit_pos_
  if (RIEGELI_UNLIKELY(limit_pos_ > size_limit_)) {
    limit_ -= IntCast<size_t>(limit_pos_ - size_limit_);
    limit_pos_ = size_limit_;
  }
  if (RIEGELI_UNLIKELY(!src_->healthy())) Fail(*src_);
}

}  // namespace riegeli
