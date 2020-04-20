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

#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if __cplusplus < 201703
constexpr Position LimitingReaderBase::kNoSizeLimit;
#endif

void LimitingReaderBase::Done() {
  if (ABSL_PREDICT_TRUE(healthy())) {
    Reader* const src = src_reader();
    SyncBuffer(src);
  }
  Reader::Done();
}

bool LimitingReaderBase::PullSlow(size_t min_length,
                                  size_t recommended_length) {
  RIEGELI_ASSERT_GT(min_length, available())
      << "Failed precondition of Reader::PullSlow(): "
         "length too small, use Pull() instead";
  RIEGELI_ASSERT_LE(pos(), size_limit_)
      << "Failed invariant of LimitingReaderBase: position exceeds size limit";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader* const src = src_reader();
  SyncBuffer(src);
  const size_t min_length_to_pull =
      UnsignedMin(min_length, size_limit_ - pos());
  const bool ok = src->Pull(min_length_to_pull, recommended_length);
  MakeBuffer(src);
  return ok && min_length_to_pull == min_length;
}

bool LimitingReaderBase::ReadSlow(char* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, available())
      << "Failed precondition of Reader::ReadSlow(char*): "
         "length too small, use Read(char*) instead";
  RIEGELI_ASSERT_LE(pos(), size_limit_)
      << "Failed invariant of LimitingReaderBase: position exceeds size limit";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader* const src = src_reader();
  SyncBuffer(src);
  const size_t length_to_read = UnsignedMin(length, size_limit_ - pos());
  const bool ok = src->Read(dest, length_to_read);
  MakeBuffer(src);
  return ok && length_to_read == length;
}

bool LimitingReaderBase::ReadSlow(Chain* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::ReadSlow(Chain*): "
         "length too small, use Read(Chain*) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Reader::ReadSlow(Chain*): "
         "Chain size overflow";
  return ReadInternal(dest, length);
}

bool LimitingReaderBase::ReadSlow(absl::Cord* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::ReadSlow(Cord*): "
         "length too small, use Read(Cord*) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Reader::ReadSlow(Cord*): "
         "Cord size overflow";
  return ReadInternal(dest, length);
}

template <typename Dest>
inline bool LimitingReaderBase::ReadInternal(Dest* dest, size_t length) {
  RIEGELI_ASSERT_LE(pos(), size_limit_)
      << "Failed invariant of LimitingReaderBase: position exceeds size limit";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader* const src = src_reader();
  SyncBuffer(src);
  const size_t length_to_read = UnsignedMin(length, size_limit_ - pos());
  const bool ok = src->ReadAndAppend(dest, length_to_read);
  MakeBuffer(src);
  return ok && length_to_read == length;
}

bool LimitingReaderBase::CopyToSlow(Writer* dest, Position length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::CopyToSlow(Writer*): "
         "length too small, use CopyTo(Writer*) instead";
  RIEGELI_ASSERT_LE(pos(), size_limit_)
      << "Failed invariant of LimitingReaderBase: position exceeds size limit";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader* const src = src_reader();
  SyncBuffer(src);
  const Position length_to_copy = UnsignedMin(length, size_limit_ - pos());
  const bool ok = src->CopyTo(dest, length_to_copy);
  MakeBuffer(src);
  return ok && length_to_copy == length;
}

bool LimitingReaderBase::CopyToSlow(BackwardWriter* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::CopyToSlow(BackwardWriter*): "
         "length too small, use CopyTo(BackwardWriter*) instead";
  RIEGELI_ASSERT_LE(pos(), size_limit_)
      << "Failed invariant of LimitingReaderBase: position exceeds size limit";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader* const src = src_reader();
  SyncBuffer(src);
  if (ABSL_PREDICT_FALSE(length > size_limit_ - pos())) {
    src->Seek(size_limit_);
    MakeBuffer(src);
    return false;
  }
  const bool ok = src->CopyTo(dest, length);
  MakeBuffer(src);
  return ok;
}

void LimitingReaderBase::ReadHintSlow(size_t length) {
  RIEGELI_ASSERT_GT(length, available())
      << "Failed precondition of Reader::ReadHintSlow(): "
         "length too small, use ReadHint() instead";
  RIEGELI_ASSERT_LE(pos(), size_limit_)
      << "Failed invariant of LimitingReaderBase: position exceeds size limit";
  if (ABSL_PREDICT_FALSE(!healthy())) return;
  Reader* const src = src_reader();
  SyncBuffer(src);
  src->ReadHint(UnsignedMin(length, size_limit_ - pos()));
  MakeBuffer(src);
}

bool LimitingReaderBase::Sync() {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader* const src = src_reader();
  SyncBuffer(src);
  const bool ok = src->Sync();
  MakeBuffer(src);
  return ok;
}

bool LimitingReaderBase::SupportsRandomAccess() const {
  const Reader* const src = src_reader();
  return src != nullptr && src->SupportsRandomAccess();
}

bool LimitingReaderBase::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of Reader::SeekSlow(): "
         "position in the buffer, use Seek() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader* const src = src_reader();
  SyncBuffer(src);
  const Position pos_to_seek = UnsignedMin(new_pos, size_limit_);
  const bool ok = src->Seek(pos_to_seek);
  MakeBuffer(src);
  return ok && pos_to_seek == new_pos;
}

bool LimitingReaderBase::SupportsSize() const {
  const Reader* const src = src_reader();
  return src != nullptr && src->SupportsSize();
}

absl::optional<Position> LimitingReaderBase::Size() {
  if (ABSL_PREDICT_FALSE(!healthy())) return absl::nullopt;
  Reader* const src = src_reader();
  SyncBuffer(src);
  const absl::optional<Position> size = src->Size();
  MakeBuffer(src);
  if (ABSL_PREDICT_FALSE(size == absl::nullopt)) return absl::nullopt;
  return UnsignedMin(*size, size_limit_);
}

}  // namespace riegeli
