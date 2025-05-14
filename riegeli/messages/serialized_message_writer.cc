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
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/chain_writer.h"

namespace riegeli {

absl::Status SerializedMessageWriter::LengthOverflowError(Position length) {
  return absl::ResourceExhaustedError(
      absl::StrCat("Failed to write length-delimited field "
                   "because its size must be smaller than 2GiB: ",
                   length));
}

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
  {
    absl::Status status =
        WriteLengthUnchecked(field_number, submessage.dest().size());
    if (ABSL_PREDICT_FALSE(!status.ok())) {
      return status;
    }
  }
  if (ABSL_PREDICT_FALSE(!writer_->Write(std::move(submessage.dest())))) {
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
    {
      absl::Status status =
          WriteLengthUnchecked(field_number, submessage.dest().size());
      if (ABSL_PREDICT_FALSE(!status.ok())) {
        return status;
      }
    }
    if (ABSL_PREDICT_FALSE(!writer_->Write(std::move(submessage.dest())))) {
      return writer_->status();
    }
  }
  submessages_.pop_back();
  return absl::OkStatus();
}

}  // namespace riegeli
