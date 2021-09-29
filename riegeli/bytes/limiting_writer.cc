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
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void LimitingWriterBase::Done() {
  if (ABSL_PREDICT_TRUE(healthy())) {
    Writer& dest = *dest_writer();
    SyncBuffer(dest);
  }
  if (ABSL_PREDICT_FALSE(exact_ && pos() < max_pos_)) {
    FailWithoutAnnotation(absl::InvalidArgumentError(absl::StrCat(
        "Not enough data: expected ", max_pos_, ", have ", pos())));
  }
  Writer::Done();
}

bool LimitingWriterBase::FailLimitExceeded() {
  return Fail(absl::ResourceExhaustedError(
      absl::StrCat("Position limit exceeded: ", max_pos_)));
}

void LimitingWriterBase::FailLengthOverflow(Position max_length) {
  FailWithoutAnnotation(absl::InvalidArgumentError(
      absl::StrCat("Not enough data: expected ", pos(), " + ", max_length,
                   " which overflows the Writer position")));
}

bool LimitingWriterBase::PushSlow(size_t min_length,
                                  size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Writer::PushSlow(): "
         "enough space available, use Push() instead";
  RIEGELI_ASSERT_LE(start_pos(), max_pos_)
      << "Failed invariant of LimitingWriterBase: "
         "position already exceeds its limit";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Writer& dest = *dest_writer();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(dest))) return false;
  const bool ok = dest.Push(min_length, recommended_length);
  MakeBuffer(dest);
  return ok;
}

bool LimitingWriterBase::WriteSlow(absl::string_view src) {
  RIEGELI_ASSERT_LT(available(), src.size())
      << "Failed precondition of Writer::WriteSlow(string_view): "
         "enough space available, use Write(string_view) instead";
  return WriteInternal(src);
}

bool LimitingWriterBase::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain): "
         "enough space available, use Write(Chain) instead";
  return WriteInternal(src);
}

bool LimitingWriterBase::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain&&): "
         "enough space available, use Write(Chain&&) instead";
  return WriteInternal(std::move(src));
}

bool LimitingWriterBase::WriteSlow(const absl::Cord& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Cord): "
         "enough space available, use Write(Cord) instead";
  return WriteInternal(src);
}

bool LimitingWriterBase::WriteSlow(absl::Cord&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Cord&&): "
         "enough space available, use Write(Cord&&) instead";
  return WriteInternal(std::move(src));
}

template <typename Src>
inline bool LimitingWriterBase::WriteInternal(Src&& src) {
  RIEGELI_ASSERT_LE(start_pos(), max_pos_)
      << "Failed invariant of LimitingWriterBase: "
         "position already exceeds its limit";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Writer& dest = *dest_writer();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(dest))) return false;
  if (ABSL_PREDICT_FALSE(src.size() > max_pos_ - pos())) {
    return FailLimitExceeded();
  }
  const bool ok = dest.Write(std::forward<Src>(src));
  MakeBuffer(dest);
  return ok;
}

bool LimitingWriterBase::WriteZerosSlow(Position length) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Writer::WriteZerosSlow(): "
         "enough space available, use WriteZeros() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Writer& dest = *dest_writer();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(dest))) return false;
  if (ABSL_PREDICT_FALSE(length > max_pos_ - pos())) {
    return FailLimitExceeded();
  }
  const bool ok = dest.WriteZeros(length);
  MakeBuffer(dest);
  return ok;
}

bool LimitingWriterBase::SupportsRandomAccess() {
  Writer* const dest = dest_writer();
  return dest != nullptr && dest->SupportsRandomAccess();
}

bool LimitingWriterBase::SeekImpl(Position new_pos) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Writer& dest = *dest_writer();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(dest))) return false;
  const Position pos_to_seek = UnsignedMin(new_pos, max_pos_);
  const bool ok = dest.Seek(pos_to_seek);
  MakeBuffer(dest);
  return ok && pos_to_seek == new_pos;
}

bool LimitingWriterBase::PrefersCopying() const {
  const Writer* const dest = dest_writer();
  return dest != nullptr && dest->PrefersCopying();
}

absl::optional<Position> LimitingWriterBase::SizeImpl() {
  if (ABSL_PREDICT_FALSE(!healthy())) return absl::nullopt;
  Writer& dest = *dest_writer();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(dest))) return absl::nullopt;
  const absl::optional<Position> size = dest.Size();
  MakeBuffer(dest);
  if (ABSL_PREDICT_FALSE(size == absl::nullopt)) return absl::nullopt;
  return UnsignedMin(*size, max_pos_);
}

bool LimitingWriterBase::SupportsTruncate() {
  Writer* const dest = dest_writer();
  return dest != nullptr && dest->SupportsTruncate();
}

bool LimitingWriterBase::TruncateImpl(Position new_size) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Writer& dest = *dest_writer();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(dest))) return false;
  const bool ok = dest.Truncate(new_size);
  MakeBuffer(dest);
  return ok;
}

}  // namespace riegeli
