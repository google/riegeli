// Copyright 2025 Google LLC
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

#include "riegeli/messages/serialized_message_backward_writer.h"

#include <stddef.h>
#include <stdint.h>

#include <utility>

#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "riegeli/base/any.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/cord_iterator_span.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/copy_all.h"
#include "riegeli/bytes/reader.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

absl::Status SerializedMessageBackwardWriter::LengthOverflowError(
    Position length) {
  return absl::ResourceExhaustedError(
      absl::StrCat("Failed to write length-delimited field "
                   "because its size must be smaller than 2GiB: ",
                   length));
}

absl::Status SerializedMessageBackwardWriter::WriteStringFailed(
    Reader& src, BackwardWriter& dest) {
  return !dest.ok()
             ? dest.status()
             : src.StatusOrAnnotate(absl::InvalidArgumentError(
                   "Could not read contents for a length-delimited field"));
}

absl::Status SerializedMessageBackwardWriter::WriteString(int field_number,
                                                          AnyRef<Reader*> src) {
  if (src.IsOwning()) src->SetReadAllHint(true);
  const Position pos_after = writer().pos();
  if (absl::Status status = CopyAll(std::move(src), writer());
      ABSL_PREDICT_FALSE(!status.ok())) {
    return status;
  }
  RIEGELI_ASSERT_GE(writer().pos(), pos_after)
      << "CopyAll() decreased dest.pos()";
  return WriteLengthUnchecked(field_number, writer().pos() - pos_after);
}

absl::Status SerializedMessageBackwardWriter::WriteString(
    int field_number, CordIteratorSpan src) {
  if (src.length() <= kMaxBytesToCopy) {
    if (ABSL_PREDICT_FALSE(!writer().Push(src.length()))) {
      return writer().status();
    }
    writer().move_cursor(src.length());
    CordIteratorSpan::Read(src.iterator(), src.length(), writer().cursor());
    return WriteLengthUnchecked(field_number, src.length());
  }
  return WriteString(field_number, std::move(src).ToCord());
}

void SerializedMessageBackwardWriter::OpenLengthDelimited() {
  submessages_.push_back(writer().pos());
}

absl::Status SerializedMessageBackwardWriter::CloseLengthDelimited(
    int field_number) {
  RIEGELI_ASSERT(!submessages_.empty())
      << "Failed precondition of "
         "SerializedMessageBackwardWriter::CloseLengthDelimited(): "
         "no matching OpenLengthDelimited() call";
  RIEGELI_ASSERT_GE(writer().pos(), submessages_.back())
      << "Failed precondition of "
         "SerializedMessageBackwardWriter::CloseLengthDelimited(): "
         "writer().pos() decreased since OpenLengthDelimited()";
  const Position length = writer().pos() - submessages_.back();
  submessages_.pop_back();
  return WriteLengthUnchecked(field_number, length);
}

absl::Status SerializedMessageBackwardWriter::CloseOptionalLengthDelimited(
    int field_number) {
  RIEGELI_ASSERT(!submessages_.empty())
      << "Failed precondition of "
         "SerializedMessageBackwardWriter::CloseOptionalLengthDelimited(): "
         "no matching OpenLengthDelimited() call";
  RIEGELI_ASSERT_GE(writer().pos(), submessages_.back())
      << "Failed precondition of "
         "SerializedMessageBackwardWriter::CloseOptionalLengthDelimited(): "
         "writer().pos() decreased since OpenLengthDelimited()";
  const Position length = writer().pos() - submessages_.back();
  submessages_.pop_back();
  if (length > 0) return WriteLengthUnchecked(field_number, length);
  return absl::OkStatus();
}

}  // namespace riegeli
