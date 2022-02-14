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

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/backward_writer.h"

namespace riegeli {

void LimitingBackwardWriterBase::Done() {
  BackwardWriter& dest = *dest_writer();
  if (ABSL_PREDICT_TRUE(ok())) SyncBuffer(dest);
  if (ABSL_PREDICT_FALSE(exact_ && pos() < max_pos_)) {
    // Do not call `Fail()` because `AnnotateStatusImpl()` synchronizes the
    // buffer again.
    FailWithoutAnnotation(dest.AnnotateStatus(absl::InvalidArgumentError(
        absl::StrCat("Not enough data: expected ", max_pos_))));
  }
  BackwardWriter::Done();
}

bool LimitingBackwardWriterBase::FailLimitExceeded(BackwardWriter& dest) {
  dest.set_cursor(cursor() + IntCast<size_t>(pos() - max_pos_));
  // Do not call `Fail()` because `AnnotateStatusImpl()` synchronizes the buffer
  // again.
  return FailWithoutAnnotation(dest.AnnotateStatus(
      absl::ResourceExhaustedError(absl::StrCat("Position limit exceeded"))));
}

void LimitingBackwardWriterBase::FailLengthOverflow(Position max_length) {
  Fail(absl::InvalidArgumentError(
      absl::StrCat("Not enough data: expected ", pos(), " + ", max_length,
                   " which overflows the BackwardWriter position")));
}

absl::Status LimitingBackwardWriterBase::AnnotateStatusImpl(
    absl::Status status) {
  // Fully delegate annotations to `*dest_writer()`.
  if (is_open()) {
    BackwardWriter& dest = *dest_writer();
    const bool sync_buffer_ok = SyncBuffer(dest);
    status = dest.AnnotateStatus(std::move(status));
    if (ABSL_PREDICT_TRUE(sync_buffer_ok)) MakeBuffer(dest);
  }
  return status;
}

bool LimitingBackwardWriterBase::PushSlow(size_t min_length,
                                          size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of BackwardWriter::PushSlow(): "
         "enough space available, use Push() instead";
  RIEGELI_ASSERT_LE(start_pos(), max_pos_)
      << "Failed invariant of LimitingBackwardWriterBase: "
         "position already exceeds its limit";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  BackwardWriter& dest = *dest_writer();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(dest))) return false;
  const bool push_ok = dest.Push(min_length, recommended_length);
  MakeBuffer(dest);
  return push_ok;
}

bool LimitingBackwardWriterBase::WriteSlow(absl::string_view src) {
  RIEGELI_ASSERT_LT(available(), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(string_view): "
         "enough space available, use Write(string_view) instead";
  return WriteInternal(src);
}

bool LimitingBackwardWriterBase::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Chain): "
         "enough space available, use Write(Chain) instead";
  return WriteInternal(src);
}

bool LimitingBackwardWriterBase::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Chain&&): "
         "enough space available, use Write(Chain&&) instead";
  return WriteInternal(std::move(src));
}

bool LimitingBackwardWriterBase::WriteSlow(const absl::Cord& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Cord): "
         "enough space available, use Write(Cord) instead";
  return WriteInternal(src);
}

bool LimitingBackwardWriterBase::WriteSlow(absl::Cord&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Cord&&): "
         "enough space available, use Write(Cord&&) instead";
  return WriteInternal(std::move(src));
}

template <typename Src>
inline bool LimitingBackwardWriterBase::WriteInternal(Src&& src) {
  RIEGELI_ASSERT_LE(start_pos(), max_pos_)
      << "Failed invariant of LimitingBackwardWriterBase: "
         "position already exceeds its limit";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  BackwardWriter& dest = *dest_writer();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(dest))) return false;
  if (ABSL_PREDICT_FALSE(src.size() > max_pos_ - pos())) {
    return FailLimitExceeded(dest);
  }
  const bool write_ok = dest.Write(std::forward<Src>(src));
  MakeBuffer(dest);
  return write_ok;
}

bool LimitingBackwardWriterBase::WriteZerosSlow(Position length) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of BackwardWriter::WriteZerosSlow(): "
         "enough space available, use WriteZeros() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  BackwardWriter& dest = *dest_writer();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(dest))) return false;
  if (ABSL_PREDICT_FALSE(length > max_pos_ - pos())) {
    return FailLimitExceeded(dest);
  }
  const bool write_ok = dest.WriteZeros(length);
  MakeBuffer(dest);
  return write_ok;
}

bool LimitingBackwardWriterBase::PrefersCopying() const {
  const BackwardWriter* const dest = dest_writer();
  return dest != nullptr && dest->PrefersCopying();
}

bool LimitingBackwardWriterBase::SupportsTruncate() {
  BackwardWriter* const dest = dest_writer();
  return dest != nullptr && dest->SupportsTruncate();
}

bool LimitingBackwardWriterBase::TruncateImpl(Position new_size) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  BackwardWriter& dest = *dest_writer();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(dest))) return false;
  const bool truncate_ok = dest.Truncate(new_size);
  MakeBuffer(dest);
  return truncate_ok;
}

}  // namespace riegeli
