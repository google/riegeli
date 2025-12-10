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

#include <stddef.h>
#include <stdint.h>

#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/limiting_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/messages/message_wire_format.h"
#include "riegeli/messages/serialized_message_reader_internal.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli::serialized_message_reader_internal {

namespace {

ABSL_ATTRIBUTE_COLD absl::Status ReadVarintError() {
  return absl::InvalidArgumentError("Could not read a varint field");
}

ABSL_ATTRIBUTE_COLD absl::Status ReadFixed32Error() {
  return absl::InvalidArgumentError("Could not read a fixed32 field");
}

ABSL_ATTRIBUTE_COLD absl::Status ReadFixed64Error() {
  return absl::InvalidArgumentError("Could not read a fixed64 field");
}

ABSL_ATTRIBUTE_COLD absl::Status NotEnoughError(uint32_t expected_length,
                                                Position available) {
  return absl::InvalidArgumentError(
      absl::StrCat("Not enough data: expected at least ", expected_length,
                   " more, will have at most ", available, " more"));
}

ABSL_ATTRIBUTE_COLD
absl::Status ReadLengthDelimitedLengthError() {
  return absl::InvalidArgumentError(
      "Could not read a length-delimited field length");
}

}  // namespace

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

absl::Status ReadTagError() {
  return absl::InvalidArgumentError("Could not read field tag");
}

absl::Status ReadTagError(Reader& src) {
  return src.StatusOrAnnotate(ReadTagError());
}

absl::Status ReadVarintError(int field_number) {
  return AnnotateWithFieldNumberSlow(ReadVarintError(), field_number);
}

absl::Status ReadVarintError(Reader& src, int field_number) {
  return AnnotateWithSourceAndFieldNumberSlow(ReadVarintError(), src,
                                              field_number);
}

absl::Status ReadFixed32Error(int field_number) {
  return AnnotateWithFieldNumberSlow(ReadFixed32Error(), field_number);
}

absl::Status ReadFixed32Error(Reader& src, int field_number) {
  return AnnotateWithSourceAndFieldNumberSlow(ReadFixed32Error(), src,
                                              field_number);
}

absl::Status ReadFixed64Error(int field_number) {
  return AnnotateWithFieldNumberSlow(ReadFixed64Error(), field_number);
}

absl::Status ReadFixed64Error(Reader& src, int field_number) {
  return AnnotateWithSourceAndFieldNumberSlow(ReadFixed64Error(), src,
                                              field_number);
}

absl::Status NotEnoughError(int field_number, uint32_t expected_length,
                            size_t available) {
  return AnnotateWithFieldNumberSlow(NotEnoughError(expected_length, available),
                                     field_number);
}

absl::Status NotEnoughError(LimitingReaderBase& src, int field_number,
                            uint32_t expected_length) {
  return AnnotateWithSourceAndFieldNumberSlow(
      NotEnoughError(expected_length, src.max_length()), src, field_number);
}

absl::Status ReadLengthDelimitedLengthError(int field_number) {
  return AnnotateWithFieldNumberSlow(ReadLengthDelimitedLengthError(),
                                     field_number);
}

absl::Status ReadLengthDelimitedLengthError(Reader& src, int field_number) {
  return AnnotateWithSourceAndFieldNumberSlow(ReadLengthDelimitedLengthError(),
                                              src, field_number);
}

absl::Status ReadLengthDelimitedValueError(Reader& src) {
  return src.StatusOrAnnotate(
      absl::InvalidArgumentError("Could not read a length-delimited field"));
}

absl::Status ReadLengthDelimitedValueError(Reader& src, int field_number) {
  return AnnotateWithFieldNumberSlow(ReadLengthDelimitedValueError(src),
                                     field_number);
}

absl::Status InvalidWireTypeError(uint32_t tag) {
  return absl::InvalidArgumentError(absl::StrCat(
      "Invalid wire type: ", static_cast<int>(GetTagWireType(tag))));
}

absl::Status InvalidWireTypeError(Reader& src, uint32_t tag) {
  return src.StatusOrAnnotate(InvalidWireTypeError(tag));
}

}  // namespace riegeli::serialized_message_reader_internal
