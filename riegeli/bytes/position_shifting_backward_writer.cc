// Copyright 2021 Google LLC
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

#include "riegeli/bytes/position_shifting_backward_writer.h"

#include <stddef.h>

#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/byte_fill.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/external_ref.h"
#include "riegeli/base/object.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/backward_writer.h"

namespace riegeli {

void PositionShiftingBackwardWriterBase::Done() {
  if (ABSL_PREDICT_TRUE(ok())) {
    BackwardWriter& dest = *DestWriter();
    SyncBuffer(dest);
  }
  BackwardWriter::Done();
}

bool PositionShiftingBackwardWriterBase::FailUnderflow(Position new_pos,
                                                       Object& object) {
  return object.Fail(absl::InvalidArgumentError(
      absl::StrCat("PositionShiftingBackwardWriter does not support "
                   "truncating before the base position: ",
                   new_pos, " < ", base_pos_)));
}

absl::Status PositionShiftingBackwardWriterBase::AnnotateStatusImpl(
    absl::Status status) {
  if (is_open()) {
    BackwardWriter& dest = *DestWriter();
    SyncBuffer(dest);
    status = dest.AnnotateStatus(std::move(status));
    MakeBuffer(dest);
  }
  // The status might have been annotated by `dest` with the original position.
  // Clarify that the current position is the relative position instead of
  // delegating to `BackwardWriter::AnnotateStatusImpl()`.
  return AnnotateOverDest(std::move(status));
}

absl::Status PositionShiftingBackwardWriterBase::AnnotateOverDest(
    absl::Status status) {
  if (is_open() && base_pos_ > 0) {
    return Annotate(status,
                    absl::StrCat("with relative position at byte ", pos()));
  }
  return status;
}

bool PositionShiftingBackwardWriterBase::PushSlow(size_t min_length,
                                                  size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of BackwardWriter::PushSlow(): "
         "enough space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  BackwardWriter& dest = *DestWriter();
  SyncBuffer(dest);
  const bool push_ok = dest.Push(min_length, recommended_length);
  return MakeBuffer(dest, min_length) && push_ok;
}

bool PositionShiftingBackwardWriterBase::WriteSlow(absl::string_view src) {
  RIEGELI_ASSERT_LT(available(), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(string_view): "
         "enough space available, use Write(string_view) instead";
  return WriteInternal(src);
}

bool PositionShiftingBackwardWriterBase::WriteSlow(ExternalRef src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(ExternalRef): "
         "enough space available, use Write(ExternalRef) instead";
  return WriteInternal(std::move(src));
}

bool PositionShiftingBackwardWriterBase::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Chain): "
         "enough space available, use Write(Chain) instead";
  return WriteInternal(src);
}

bool PositionShiftingBackwardWriterBase::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Chain&&): "
         "enough space available, use Write(Chain&&) instead";
  return WriteInternal(std::move(src));
}

bool PositionShiftingBackwardWriterBase::WriteSlow(const absl::Cord& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Cord): "
         "enough space available, use Write(Cord) instead";
  return WriteInternal(src);
}

bool PositionShiftingBackwardWriterBase::WriteSlow(absl::Cord&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Cord&&): "
         "enough space available, use Write(Cord&&) instead";
  return WriteInternal(std::move(src));
}

bool PositionShiftingBackwardWriterBase::WriteSlow(ByteFill src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(ByteFill): "
         "enough space available, use Write(ByteFill) instead";
  return WriteInternal(src);
}

template <typename Src>
inline bool PositionShiftingBackwardWriterBase::WriteInternal(Src&& src) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  BackwardWriter& dest = *DestWriter();
  SyncBuffer(dest);
  const bool write_ok = dest.Write(std::forward<Src>(src));
  return MakeBuffer(dest) && write_ok;
}

bool PositionShiftingBackwardWriterBase::SupportsTruncate() {
  BackwardWriter* const dest = DestWriter();
  return dest != nullptr && dest->SupportsTruncate();
}

bool PositionShiftingBackwardWriterBase::TruncateImpl(Position new_size) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (ABSL_PREDICT_FALSE(new_size < base_pos_)) {
    return FailUnderflow(new_size, *this);
  }
  BackwardWriter& dest = *DestWriter();
  SyncBuffer(dest);
  const bool truncate_ok = dest.Truncate(new_size - base_pos_);
  return MakeBuffer(dest) && truncate_ok;
}

}  // namespace riegeli
