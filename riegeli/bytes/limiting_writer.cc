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

#include <memory>
#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if __cplusplus < 201703
constexpr Position LimitingWriterBase::kNoSizeLimit;
#endif

void LimitingWriterBase::Done() {
  if (ABSL_PREDICT_TRUE(healthy())) {
    Writer* const dest = dest_writer();
    SyncBuffer(dest);
  }
  Writer::Done();
}

bool LimitingWriterBase::PushSlow() {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of Writer::PushSlow(): "
         "space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Writer* const dest = dest_writer();
  if (ABSL_PREDICT_FALSE(pos() == size_limit_)) return FailOverflow();
  SyncBuffer(dest);
  const bool ok = dest->Push();
  MakeBuffer(dest);
  return ok;
}

bool LimitingWriterBase::WriteSlow(absl::string_view src) {
  RIEGELI_ASSERT_GT(src.size(), available())
      << "Failed precondition of Writer::WriteSlow(string_view): "
         "length too small, use Write(string_view) instead";
  return WriteInternal(src);
}

bool LimitingWriterBase::WriteSlow(std::string&& src) {
  RIEGELI_ASSERT_GT(src.size(), available())
      << "Failed precondition of Writer::WriteSlow(string&&): "
         "length too small, use Write(string&&) instead";
  return WriteInternal(std::move(src));
}

bool LimitingWriterBase::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_GT(src.size(), UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Writer::WriteSlow(Chain): "
         "length too small, use Write(Chain) instead";
  return WriteInternal(src);
}

bool LimitingWriterBase::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_GT(src.size(), UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Writer::WriteSlow(Chain&&): "
         "length too small, use Write(Chain&&) instead";
  return WriteInternal(std::move(src));
}

template <typename Src>
inline bool LimitingWriterBase::WriteInternal(Src&& src) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Writer* const dest = dest_writer();
  RIEGELI_ASSERT_LE(pos(), size_limit_)
      << "Failed invariant of LimitingWriter: position exceeds size limit";
  if (ABSL_PREDICT_FALSE(src.size() > size_limit_ - pos())) {
    return FailOverflow();
  }
  SyncBuffer(dest);
  const bool ok = dest->Write(std::forward<Src>(src));
  MakeBuffer(dest);
  return ok;
}

bool LimitingWriterBase::SupportsRandomAccess() const {
  const Writer* const dest = dest_writer();
  return dest != nullptr && dest->SupportsRandomAccess();
}

bool LimitingWriterBase::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos_ || new_pos > pos())
      << "Failed precondition of Writer::SeekSlow(): "
         "position in the buffer, use Seek() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Writer* const dest = dest_writer();
  SyncBuffer(dest);
  const Position pos_to_seek = UnsignedMin(new_pos, size_limit_);
  const bool ok = dest->Seek(pos_to_seek);
  MakeBuffer(dest);
  return ok && pos_to_seek == new_pos;
}

bool LimitingWriterBase::Flush(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Writer* const dest = dest_writer();
  SyncBuffer(dest);
  const bool ok = dest->Flush(flush_type);
  MakeBuffer(dest);
  return ok;
}

bool LimitingWriterBase::Size(Position* size) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Writer* const dest = dest_writer();
  SyncBuffer(dest);
  const bool ok = dest->Size(size);
  MakeBuffer(dest);
  if (ABSL_PREDICT_FALSE(!ok)) return false;
  *size = UnsignedMin(*size, size_limit_);
  return true;
}

bool LimitingWriterBase::SupportsTruncate() const {
  const Writer* const dest = dest_writer();
  return dest != nullptr && dest->SupportsTruncate();
}

bool LimitingWriterBase::Truncate(Position new_size) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Writer* const dest = dest_writer();
  SyncBuffer(dest);
  const bool ok = dest->Truncate(new_size);
  MakeBuffer(dest);
  return ok;
}

template class LimitingWriter<Writer*>;
template class LimitingWriter<std::unique_ptr<Writer>>;

}  // namespace riegeli
