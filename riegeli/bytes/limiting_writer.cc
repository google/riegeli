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

#include "riegeli/bytes/limiting_writer.h"

#include <stddef.h>
#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

LimitingWriter::LimitingWriter(Writer* dest, Position size_limit)
    : Writer(State::kOpen),
      dest_(RIEGELI_ASSERT_NOTNULL(dest)),
      size_limit_(size_limit) {
  RIEGELI_ASSERT_GE(size_limit, dest_->pos())
      << "Failed precondition of LimitingWriter::LimitingWriter(): "
         "size limit smaller than current position";
  MakeBuffer();
}

void LimitingWriter::Done() {
  if (ABSL_PREDICT_TRUE(healthy())) SyncBuffer();
  Writer::Done();
}

bool LimitingWriter::PushSlow() {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of Writer::PushSlow(): "
         "space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(pos() == size_limit_)) {
    cursor_ = start_;
    limit_ = start_;
    return FailOverflow();
  }
  SyncBuffer();
  const bool ok = dest_->Push();
  MakeBuffer();
  return ok;
}

bool LimitingWriter::WriteSlow(absl::string_view src) {
  RIEGELI_ASSERT_GT(src.size(), available())
      << "Failed precondition of Writer::WriteSlow(string_view): "
         "length too small, use Write(string_view) instead";
  return WriteInternal(src);
}

bool LimitingWriter::WriteSlow(std::string&& src) {
  RIEGELI_ASSERT_GT(src.size(), available())
      << "Failed precondition of Writer::WriteSlow(string_view): "
         "length too small, use Write(string_view) instead";
  return WriteInternal(std::move(src));
}

bool LimitingWriter::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_GT(src.size(), UnsignedMin(available(), kMaxBytesToCopy()))
      << "Failed precondition of Writer::WriteSlow(Chain): "
         "length too small, use Write(Chain) instead";
  return WriteInternal(src);
}

bool LimitingWriter::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_GT(src.size(), UnsignedMin(available(), kMaxBytesToCopy()))
      << "Failed precondition of Writer::WriteSlow(Chain&&): "
         "length too small, use Write(Chain&&) instead";
  return WriteInternal(std::move(src));
}

template <typename Src>
bool LimitingWriter::WriteInternal(Src&& src) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  RIEGELI_ASSERT_LE(pos(), size_limit_)
      << "Failed invariant of LimitingWriter: position exceeds size limit";
  if (ABSL_PREDICT_FALSE(src.size() > size_limit_ - pos())) {
    cursor_ = start_;
    limit_ = start_;
    return FailOverflow();
  }
  SyncBuffer();
  const bool ok = dest_->Write(std::forward<Src>(src));
  MakeBuffer();
  return ok;
}

bool LimitingWriter::SupportsRandomAccess() const {
  return dest_ != nullptr && dest_->SupportsRandomAccess();
}

bool LimitingWriter::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos_ || new_pos > pos())
      << "Failed precondition of Writer::SeekSlow(): "
         "position in the buffer, use Seek() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  SyncBuffer();
  const Position pos_to_seek = UnsignedMin(new_pos, size_limit_);
  const bool ok = dest_->Seek(pos_to_seek);
  MakeBuffer();
  return ok && pos_to_seek == new_pos;
}

bool LimitingWriter::Flush(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  SyncBuffer();
  const bool ok = dest_->Flush(flush_type);
  MakeBuffer();
  return ok;
}

bool LimitingWriter::Size(Position* size) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  SyncBuffer();
  const bool ok = dest_->Size(size);
  MakeBuffer();
  if (ABSL_PREDICT_FALSE(!ok)) return false;
  *size = UnsignedMin(*size, size_limit_);
  return true;
}

bool LimitingWriter::SupportsTruncate() const {
  return dest_ != nullptr && dest_->SupportsTruncate();
}

bool LimitingWriter::Truncate(Position new_size) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  SyncBuffer();
  const bool ok = dest_->Truncate(new_size);
  MakeBuffer();
  return ok;
}

inline void LimitingWriter::SyncBuffer() { dest_->set_cursor(cursor_); }

inline void LimitingWriter::MakeBuffer() {
  start_ = dest_->start();
  cursor_ = dest_->cursor();
  limit_ = dest_->limit();
  start_pos_ = dest_->pos() - dest_->written_to_buffer();  // dest_->start_pos_
  if (ABSL_PREDICT_FALSE(limit_pos() > size_limit_)) {
    limit_ -= IntCast<size_t>(limit_pos() - size_limit_);
  }
  if (ABSL_PREDICT_FALSE(!dest_->healthy())) Fail(*dest_);
}

}  // namespace riegeli
