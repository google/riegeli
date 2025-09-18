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

#include <limits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "riegeli/base/any.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/copy_all.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/varint/varint_writing.h"

namespace riegeli {

absl::Status SerializedMessageBackwardWriter::LengthOverflowError(
    Position length) {
  return absl::ResourceExhaustedError(
      absl::StrCat("Failed to write length-delimited field "
                   "because its size must be smaller than 2GiB: ",
                   length));
}

absl::Status SerializedMessageBackwardWriter::CopyString(int field_number,
                                                         Reader& src,
                                                         Position length) {
  if (ABSL_PREDICT_FALSE(length >
                         uint32_t{std::numeric_limits<int32_t>::max()})) {
    return LengthOverflowError(length);
  }
  if (ABSL_PREDICT_FALSE(!src.Copy(IntCast<size_t>(length), writer()))) {
    return !writer().ok() ? writer().status()
                          : src.StatusOrAnnotate(absl::InvalidArgumentError(
                                "Could not read a length-delimited field"));
  }
  return WriteLengthUnchecked(field_number, length);
}

absl::Status SerializedMessageBackwardWriter::CopyString(int field_number,
                                                         AnyRef<Reader*> src) {
  if (src.IsOwning()) src->SetReadAllHint(true);
  const Position pos_after = dest_->pos();
  if (absl::Status status = CopyAll(std::move(src), *dest_);
      ABSL_PREDICT_FALSE(!status.ok())) {
    return status;
  }
  RIEGELI_ASSERT_GE(dest_->pos(), pos_after)
      << "CopyAll() decreased dest.pos()";
  return WriteLengthUnchecked(field_number, dest_->pos() - pos_after);
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
  RIEGELI_ASSERT_GE(dest_->pos(), submessages_.back())
      << "Failed precondition of "
         "SerializedMessageBackwardWriter::CloseLengthDelimited(): "
         "writer().pos() decreased since OpenLengthDelimited()";
  const Position length = dest_->pos() - submessages_.back();
  submessages_.pop_back();
  return WriteLengthUnchecked(field_number, length);
}

absl::Status SerializedMessageBackwardWriter::CloseOptionalLengthDelimited(
    int field_number) {
  RIEGELI_ASSERT(!submessages_.empty())
      << "Failed precondition of "
         "SerializedMessageBackwardWriter::CloseOptionalLengthDelimited(): "
         "no matching OpenLengthDelimited() call";
  RIEGELI_ASSERT_GE(dest_->pos(), submessages_.back())
      << "Failed precondition of "
         "SerializedMessageBackwardWriter::CloseOptionalLengthDelimited(): "
         "writer().pos() decreased since OpenLengthDelimited()";
  const Position length = dest_->pos() - submessages_.back();
  submessages_.pop_back();
  if (length > 0) return WriteLengthUnchecked(field_number, length);
  return absl::OkStatus();
}

inline absl::Status SerializedMessageBackwardWriter::WriteTag(uint32_t tag) {
  if (ABSL_PREDICT_FALSE(!WriteVarint32(tag, writer()))) {
    return writer().status();
  }
  return absl::OkStatus();
}

}  // namespace riegeli
