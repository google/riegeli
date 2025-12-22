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

#include <optional>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "riegeli/base/any.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/cord_iterator_span.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/cord_writer.h"
#include "riegeli/bytes/limiting_reader.h"
#include "riegeli/bytes/read_all.h"
#include "riegeli/bytes/reader.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

absl::Status SerializedMessageWriter::LengthOverflowError(Position length) {
  return absl::ResourceExhaustedError(
      absl::StrCat("Failed to write length-delimited field "
                   "because its size must be smaller than 2GiB: ",
                   length));
}

absl::Status SerializedMessageWriter::WriteStringFailed(Reader& src,
                                                        Writer& dest) {
  return !dest.ok()
             ? dest.status()
             : src.StatusOrAnnotate(absl::InvalidArgumentError(
                   "Could not read contents for a length-delimited field"));
}

absl::Status SerializedMessageWriter::WriteString(int field_number,
                                                  AnyRef<Reader*> src) {
  if (src.IsOwning()) src->SetReadAllHint(true);
  if (src->SupportsSize()) {
    const std::optional<Position> size = src->Size();
    if (ABSL_PREDICT_FALSE(size == std::nullopt)) return src->status();
    if (absl::Status status = WriteString(
            field_number,
            ReaderSpan(src.get(), SaturatingSub(*size, src->pos())));
        ABSL_PREDICT_FALSE(!status.ok())) {
      return status;
    }
    if (src.IsOwning()) {
      if (ABSL_PREDICT_FALSE(!src->Close())) return src->status();
    }
    return absl::OkStatus();
  } else {
    absl::Cord contents;
    if (absl::Status status = ReadAll(std::move(src), contents);
        ABSL_PREDICT_FALSE(!status.ok())) {
      return status;
    }
    return WriteString(field_number, std::move(contents));
  }
}

absl::Status SerializedMessageWriter::WriteString(int field_number,
                                                  CordIteratorSpan src) {
  if (src.length() <= kMaxBytesToCopy) {
    if (absl::Status status = WriteLengthUnchecked(field_number, src.length());
        ABSL_PREDICT_FALSE(!status.ok())) {
      return status;
    }
    if (ABSL_PREDICT_FALSE(!writer().Push(src.length()))) {
      return writer().status();
    }
    CordIteratorSpan::Read(src.iterator(), src.length(), writer().cursor());
    writer().move_cursor(src.length());
    return absl::OkStatus();
  }
  return WriteString(field_number, std::move(src).ToCord());
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
  RIEGELI_ASSERT(writer_ == &submessages_.back())
      << "Failed invariant of SerializedMessageWriter: "
         "writer() does not point to the most recently open submessage";
  CordWriter<absl::Cord>& submessage = submessages_.back();
  if (ABSL_PREDICT_FALSE(!submessage.Close())) return submessage.status();
  writer_ = submessages_.size() > 1 ? &submessages_.end()[-2] : dest_;
  if (absl::Status status =
          WriteLengthUnchecked(field_number, submessage.dest().size());
      ABSL_PREDICT_FALSE(!status.ok())) {
    return status;
  }
  RIEGELI_ASSERT(writer_ != nullptr)
      << "Failed precondition of CloseLengthDelimited(): "
         "set_dest() not called before the last CloseLengthDelimited()";
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
  RIEGELI_ASSERT(writer_ == &submessages_.back())
      << "Failed invariant of SerializedMessageWriter: "
         "writer() does not point to the most recently open submessage";
  CordWriter<absl::Cord>& submessage = submessages_.back();
  if (ABSL_PREDICT_FALSE(!submessage.Close())) return submessage.status();
  writer_ = submessages_.size() > 1 ? &submessages_.end()[-2] : dest_;
  if (!submessage.dest().empty()) {
    if (absl::Status status =
            WriteLengthUnchecked(field_number, submessage.dest().size());
        ABSL_PREDICT_FALSE(!status.ok())) {
      return status;
    }
    RIEGELI_ASSERT(writer_ != nullptr)
        << "Failed precondition of CloseOptionalLengthDelimited(): "
           "set_dest() not called before the last "
           "CloseOptionalLengthDelimited()";
    if (ABSL_PREDICT_FALSE(!writer_->Write(std::move(submessage.dest())))) {
      return writer_->status();
    }
  }
  submessages_.pop_back();
  return absl::OkStatus();
}

}  // namespace riegeli
