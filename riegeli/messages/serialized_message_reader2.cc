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

#include "riegeli/messages/serialized_message_reader2.h"

#include <stdint.h>

#include <utility>

#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/limiting_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/messages/message_wire_format.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli::serialized_message_reader_internal {

absl::Status AnnotateWithFieldNumberSlow(absl::Status status,
                                         int field_number) {
  if (absl::IsCancelled(status)) return status;
  return riegeli::Annotate(status,
                           absl::StrCat("field number: ", field_number));
}

absl::Status AnnotateWithSourceAndFieldNumberSlow(absl::Status status,
                                                  Reader& src,
                                                  int field_number) {
  if (absl::IsCancelled(status)) return status;
  return AnnotateWithFieldNumberSlow(src.StatusOrAnnotate(std::move(status)),
                                     field_number);
}

absl::Status ReadTagError(Reader& src) {
  return src.StatusOrAnnotate(
      absl::InvalidArgumentError("Could not read field tag"));
}

absl::Status ReadVarintError(Reader& src, int field_number) {
  return AnnotateWithSourceAndFieldNumberSlow(
      absl::InvalidArgumentError("Could not read a varint field"), src,
      field_number);
}

absl::Status ReadFixed32Error(Reader& src, int field_number) {
  return AnnotateWithSourceAndFieldNumberSlow(
      absl::InvalidArgumentError("Could not read a fixed32 field"), src,
      field_number);
}

absl::Status ReadFixed64Error(Reader& src, int field_number) {
  return AnnotateWithSourceAndFieldNumberSlow(
      absl::InvalidArgumentError("Could not read a fixed64 field"), src,
      field_number);
}

absl::Status NotEnoughError(LimitingReaderBase& src, int field_number,
                            uint32_t expected_length) {
  return AnnotateWithSourceAndFieldNumberSlow(
      absl::InvalidArgumentError(
          absl::StrCat("Not enough data: expected at least ", expected_length,
                       " more, will have at most ", src.max_length(), " more")),
      src, field_number);
}

absl::Status ReadLengthDelimitedLengthError(Reader& src, int field_number) {
  return AnnotateWithSourceAndFieldNumberSlow(
      absl::InvalidArgumentError(
          "Could not read a length-delimited field length"),
      src, field_number);
}

absl::Status ReadLengthDelimitedValueError(Reader& src, int field_number) {
  return AnnotateWithFieldNumberSlow(ReadLengthDelimitedValueError(src),
                                     field_number);
}

absl::Status ReadLengthDelimitedValueError(Reader& src) {
  return src.StatusOrAnnotate(
      absl::InvalidArgumentError("Could not read a length-delimited field"));
}

absl::Status InvalidWireTypeError(Reader& src, uint32_t tag) {
  return src.StatusOrAnnotate(InvalidWireTypeError(tag));
}

absl::Status InvalidWireTypeError(uint32_t tag) {
  return absl::InvalidArgumentError(absl::StrCat(
      "Invalid wire type: ", static_cast<int>(GetTagWireType(tag))));
}

}  // namespace riegeli::serialized_message_reader_internal
