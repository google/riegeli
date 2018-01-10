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
#include <utility>

#include "riegeli/base/assert.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

LimitingReader::LimitingReader() : src_(nullptr), size_limit_(0) {
  MarkCancelled();
}

LimitingReader::LimitingReader(Reader* src, Position size_limit)
    : src_(RIEGELI_ASSERT_NOTNULL(src)), size_limit_(size_limit) {
  RIEGELI_ASSERT_GE(size_limit, src_->pos());
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

LimitingReader::LimitingReader(LimitingReader&& src) noexcept
    : Reader(std::move(src)),
      src_(riegeli::exchange(src.src_, nullptr)),
      size_limit_(riegeli::exchange(src.size_limit_, 0)),
      wrapped_(riegeli::exchange(src.wrapped_, nullptr)) {}

LimitingReader& LimitingReader::operator=(LimitingReader&& src) noexcept {
  if (&src != this) {
    Reader::operator=(std::move(src));
    src_ = riegeli::exchange(src.src_, nullptr);
    size_limit_ = riegeli::exchange(src.size_limit_, 0);
    wrapped_ = riegeli::exchange(src.wrapped_, nullptr);
  }
  return *this;
}

LimitingReader::~LimitingReader() { Cancel(); }

void LimitingReader::Done() {
  src_->set_cursor(cursor_);
  if (wrapped_ != nullptr) wrapped_->SyncBuffer();
  src_ = nullptr;
  size_limit_ = 0;
  wrapped_ = nullptr;
  Reader::Done();
}

TypeId LimitingReader::GetTypeId() const {
  return TypeId::For<LimitingReader>();
}

bool LimitingReader::PullSlow() {
  RIEGELI_ASSERT_EQ(available(), 0u);
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  src_->set_cursor(cursor_);
  if (RIEGELI_UNLIKELY(limit_pos_ == size_limit_)) return false;
  const bool ok = src_->Pull();
  SyncBuffer();
  return ok;
}

bool LimitingReader::ReadSlow(char* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, available());
  return ReadInternal(dest, length);
}

bool LimitingReader::ReadSlow(Chain* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy()));
  return ReadInternal(dest, length);
}

template <typename Dest>
bool LimitingReader::ReadInternal(Dest* dest, size_t length) {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  src_->set_cursor(cursor_);
  const size_t length_to_read = UnsignedMin(length, size_limit_ - pos());
  const bool ok = src_->Read(dest, length_to_read);
  SyncBuffer();
  return ok && length_to_read == length;
}

bool LimitingReader::CopyToSlow(Writer* dest, Position length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy()));
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  src_->set_cursor(cursor_);
  const Position length_to_copy = UnsignedMin(length, size_limit_ - pos());
  const bool ok = src_->CopyTo(dest, length_to_copy);
  SyncBuffer();
  return ok && length_to_copy == length;
}

bool LimitingReader::CopyToSlow(BackwardWriter* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy()));
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  src_->set_cursor(cursor_);
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
  RIEGELI_ASSERT_EQ(available(), 0u);
  return pos() < size_limit_ && src_->HopeForMore();
}

bool LimitingReader::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos_);
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
    limit_ -= limit_pos_ - size_limit_;
    limit_pos_ = size_limit_;
  }
  if (RIEGELI_UNLIKELY(!src_->healthy())) Fail(src_->Message());
}

}  // namespace riegeli
