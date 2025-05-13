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

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/chain_writer.h"
#include "riegeli/messages/message_wire_format.h"
#include "riegeli/varint/varint_reading.h"
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
                                                         Position length,
                                                         Reader& src) {
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
  if (ABSL_PREDICT_FALSE(length >
                         uint32_t{std::numeric_limits<int32_t>::max()})) {
    return LengthOverflowError(length);
  }
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

absl::Status SerializedMessageBackwardWriter::CopyFieldFrom(uint32_t tag,
                                                            Reader& src) {
  switch (GetTagWireType(tag)) {
    case WireType::kVarint: {
      uint64_t value;
      if (ABSL_PREDICT_FALSE(!ReadVarint64(src, value))) {
        return src.StatusOrAnnotate(
            absl::InvalidArgumentError("Could not read a varint field"));
      }
      if (ABSL_PREDICT_FALSE(!WriteVarint64(value, writer()))) {
        return writer().status();
      }
      return WriteTag(tag);
    }
    case WireType::kFixed32:
      if (ABSL_PREDICT_FALSE(!src.Copy(sizeof(uint32_t), writer()))) {
        return !writer().ok() ? writer().status()
                              : src.StatusOrAnnotate(absl::InvalidArgumentError(
                                    "Could not read a fixed32 field"));
      }
      return WriteTag(tag);
    case WireType::kFixed64:
      if (ABSL_PREDICT_FALSE(!src.Copy(sizeof(uint64_t), writer()))) {
        return !writer().ok() ? writer().status()
                              : src.StatusOrAnnotate(absl::InvalidArgumentError(
                                    "Could not read a fixed64 field"));
      }
      return WriteTag(tag);
    case WireType::kLengthDelimited: {
      uint32_t length;
      if (ABSL_PREDICT_FALSE(!ReadVarint32(src, length))) {
        return src.StatusOrAnnotate(absl::InvalidArgumentError(
            "Could not read a length-delimited field length"));
      }
      if (ABSL_PREDICT_FALSE(!src.Copy(length, writer()))) {
        return !writer().ok() ? writer().status()
                              : src.StatusOrAnnotate(absl::InvalidArgumentError(
                                    "Could not read a length-delimited field"));
      }
      if (ABSL_PREDICT_FALSE(!WriteVarint32(length, writer()))) {
        return writer().status();
      }
      return WriteTag(tag);
    }
    case WireType::kStartGroup:
    case WireType::kEndGroup:
      return WriteTag(tag);
  }
  return src.StatusOrAnnotate(absl::InvalidArgumentError(
      absl::StrCat("Invalid wire type: ", GetTagWireType(tag))));
}

}  // namespace riegeli
