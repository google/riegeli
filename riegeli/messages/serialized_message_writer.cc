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

#include "riegeli/messages/serialized_message_writer.h"

#include <stddef.h>
#include <stdint.h>

#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/types/optional.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/chain_writer.h"
#include "riegeli/messages/message_wire_format.h"
#include "riegeli/varint/varint_reading.h"
#include "riegeli/varint/varint_writing.h"

namespace riegeli {

absl::Status SerializedMessageWriter::CopyString(int field_number,
                                                 Position length, Reader& src) {
  {
    absl::Status status = WriteLengthUnchecked(field_number, length);
    if (ABSL_PREDICT_FALSE(!status.ok())) {
      return status;
    }
  }
  if (ABSL_PREDICT_FALSE(!src.Copy(length, writer()))) {
    return !writer().ok() ? writer().status()
                          : src.StatusOrAnnotate(absl::InvalidArgumentError(
                                "Could not read a length-delimited field"));
  }
  return absl::OkStatus();
}

void SerializedMessageWriter::OpenLengthDelimited() {
  writer_ = &submessages_.emplace_back();
}

SerializedMessageWriter SerializedMessageWriter::NewLengthDelimited()
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  SerializedMessageWriter message(&writer());
  message.OpenLengthDelimited();
  return message;
}

absl::Status SerializedMessageWriter::CloseLengthDelimited(int field_number) {
  RIEGELI_ASSERT(!submessages_.empty())
      << "Failed precondition of "
         "SerializedMessageWriter::CloseLengthDelimited(): "
         "no matching OpenLengthDelimited() call";
  RIEGELI_ASSERT_EQ(writer_, &submessages_.back())
      << "Failed invariant of SerializedMessageWriter: "
         "writer() does not point to the most recently open submessage";
  ChainWriter<Chain>& submessage = submessages_.back();
  if (ABSL_PREDICT_FALSE(!submessage.Close())) return submessage.status();
  writer_ = submessages_.size() > 1 ? &submessages_.end()[-2] : dest_;
  if (ABSL_PREDICT_FALSE(!WriteLengthWithTag(field_number,
                                             submessage.dest().size(),
                                             *writer_) ||
                         !writer_->Write(std::move(submessage.dest())))) {
    return writer_->status();
  }
  submessages_.pop_back();
  return absl::OkStatus();
}

absl::Status SerializedMessageWriter::CloseOptionalLengthDelimited(
    int field_number) {
  RIEGELI_ASSERT(!submessages_.empty())
      << "Failed precondition of "
         "SerializedMessageWriter::CloseOptionalLengthDelimited(): "
         "no matching OpenLengthDelimited() call";
  RIEGELI_ASSERT_EQ(writer_, &submessages_.back())
      << "Failed invariant of SerializedMessageWriter: "
         "writer() does not point to the most recently open submessage";
  ChainWriter<Chain>& submessage = submessages_.back();
  if (ABSL_PREDICT_FALSE(!submessage.Close())) return submessage.status();
  writer_ = submessages_.size() > 1 ? &submessages_.end()[-2] : dest_;
  if (!submessage.dest().empty()) {
    if (ABSL_PREDICT_FALSE(!WriteLengthWithTag(field_number,
                                               submessage.dest().size(),
                                               *writer_) ||
                           !writer_->Write(std::move(submessage.dest())))) {
      return writer_->status();
    }
  }
  submessages_.pop_back();
  return absl::OkStatus();
}

absl::Status SerializedMessageWriter::CopyFieldFrom(uint32_t tag, Reader& src) {
  if (ABSL_PREDICT_FALSE(!WriteVarint32(tag, writer()))) {
    return writer().status();
  }
  switch (GetTagWireType(tag)) {
    case WireType::kVarint: {
      if (ABSL_PREDICT_FALSE(!writer().Push(kMaxLengthVarint64))) {
        return writer().status();
      }
      const absl::optional<size_t> length =
          CopyVarint64(src, writer().cursor());
      if (ABSL_PREDICT_FALSE(length == absl::nullopt)) {
        return src.StatusOrAnnotate(
            absl::InvalidArgumentError("Could not read a varint field"));
      }
      writer().move_cursor(*length);
      return absl::OkStatus();
    }
    case WireType::kFixed32:
      if (ABSL_PREDICT_FALSE(!src.Copy(sizeof(uint32_t), writer()))) {
        return !writer().ok() ? writer().status()
                              : src.StatusOrAnnotate(absl::InvalidArgumentError(
                                    "Could not read a fixed32 field"));
      }
      return absl::OkStatus();
    case WireType::kFixed64:
      if (ABSL_PREDICT_FALSE(!src.Copy(sizeof(uint64_t), writer()))) {
        return !writer().ok() ? writer().status()
                              : src.StatusOrAnnotate(absl::InvalidArgumentError(
                                    "Could not read a fixed64 field"));
      }
      return absl::OkStatus();
    case WireType::kLengthDelimited: {
      uint32_t length;
      if (ABSL_PREDICT_FALSE(!ReadVarint32(src, length))) {
        return src.StatusOrAnnotate(absl::InvalidArgumentError(
            "Could not read a length-delimited field length"));
      }
      if (ABSL_PREDICT_FALSE(!WriteVarint32(length, writer()))) {
        return writer().status();
      }
      if (ABSL_PREDICT_FALSE(!src.Copy(length, writer()))) {
        return !writer().ok() ? writer().status()
                              : src.StatusOrAnnotate(absl::InvalidArgumentError(
                                    "Could not read a length-delimited field"));
      }
      return absl::OkStatus();
    }
    case WireType::kStartGroup:
    case WireType::kEndGroup:
      return absl::OkStatus();
  }
  return src.StatusOrAnnotate(absl::InvalidArgumentError(
      absl::StrCat("Invalid wire type: ", GetTagWireType(tag))));
}

}  // namespace riegeli
