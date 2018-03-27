// Copyright 2018 Google LLC
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

#include "riegeli/bytes/limiting_backward_writer.h"

#include <stddef.h>
#include <string>
#include <utility>

#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/backward_writer.h"

namespace riegeli {

LimitingBackwardWriter::LimitingBackwardWriter(BackwardWriter* dest,
                                               Position size_limit)
    : BackwardWriter(State::kOpen),
      dest_(RIEGELI_ASSERT_NOTNULL(dest)),
      size_limit_(size_limit) {
  RIEGELI_ASSERT_GE(size_limit, dest_->pos())
      << "Failed precondition of "
         "LimitingBackwardWriter::LimitingBackwardWriter(): "
         "size limit smaller than current position";
  SyncBuffer();
}

void LimitingBackwardWriter::Done() {
  if (RIEGELI_LIKELY(healthy())) dest_->set_cursor(cursor_);
  dest_ = nullptr;
  size_limit_ = 0;
  BackwardWriter::Done();
}

bool LimitingBackwardWriter::PushSlow() {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of BackwardWriter::PushSlow(): "
         "space available, use Push() instead";
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  if (RIEGELI_UNLIKELY(pos() == size_limit_)) {
    cursor_ = start_;
    limit_ = start_;
    return FailOverflow();
  }
  dest_->set_cursor(cursor_);
  const bool ok = dest_->Push();
  SyncBuffer();
  return ok;
}

bool LimitingBackwardWriter::WriteSlow(absl::string_view src) {
  RIEGELI_ASSERT_GT(src.size(), available())
      << "Failed precondition of BackwardWriter::WriteSlow(string_view): "
         "length too small, use Write(string_view) instead";
  return WriteInternal(src);
}

bool LimitingBackwardWriter::WriteSlow(std::string&& src) {
  RIEGELI_ASSERT_GT(src.size(), available())
      << "Failed precondition of BackwardWriter::WriteSlow(string_view): "
         "length too small, use Write(string_view) instead";
  return WriteInternal(std::move(src));
}

bool LimitingBackwardWriter::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_GT(src.size(), UnsignedMin(available(), kMaxBytesToCopy()))
      << "Failed precondition of BackwardWriter::WriteSlow(Chain): "
         "length too small, use Write(Chain) instead";
  return WriteInternal(src);
}

bool LimitingBackwardWriter::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_GT(src.size(), UnsignedMin(available(), kMaxBytesToCopy()))
      << "Failed precondition of BackwardWriter::WriteSlow(Chain&&): "
         "length too small, use Write(Chain&&) instead";
  return WriteInternal(std::move(src));
}

template <typename Src>
bool LimitingBackwardWriter::WriteInternal(Src&& src) {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  RIEGELI_ASSERT_LE(pos(), size_limit_)
      << "Failed invariant of LimitingBackwardWriter: "
         "position exceeds size limit";
  if (RIEGELI_UNLIKELY(src.size() > size_limit_ - pos())) {
    cursor_ = start_;
    limit_ = start_;
    return FailOverflow();
  }
  dest_->set_cursor(cursor_);
  const bool ok = dest_->Write(std::forward<Src>(src));
  SyncBuffer();
  return ok;
}

inline void LimitingBackwardWriter::SyncBuffer() {
  start_ = dest_->start();
  cursor_ = dest_->cursor();
  limit_ = dest_->limit();
  start_pos_ = dest_->pos() - dest_->written_to_buffer();  // dest_->start_pos_
  if (RIEGELI_UNLIKELY(limit_pos() > size_limit_)) {
    limit_ += IntCast<size_t>(limit_pos() - size_limit_);
  }
  if (RIEGELI_UNLIKELY(!dest_->healthy())) Fail(*dest_);
}

}  // namespace riegeli
