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

#include "riegeli/messages/field_handlers.h"

#include <stdint.h>

#include <utility>

#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "riegeli/bytes/reader.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli::field_handlers_internal {

absl::Status AnnotateByReader(absl::Status status, Reader& reader) {
  if (absl::IsCancelled(status)) {
    return status;
  } else {
    return reader.StatusOrAnnotate(std::move(status));
  }
}

absl::Status ReadPackedVarintError(Reader& src) {
  return src.StatusOrAnnotate(absl::InvalidArgumentError(
      "Could not read a varint element of a packed repeated field"));
}

absl::Status ReadPackedFixed32Error(Reader& src) {
  return src.StatusOrAnnotate(absl::InvalidArgumentError(
      "Could not read a fixed32 element of a packed repeated field"));
}

absl::Status ReadPackedFixed64Error(Reader& src) {
  return src.StatusOrAnnotate(absl::InvalidArgumentError(
      "Could not read a fixed64 element of a packed repeated field"));
}

absl::Status InvalidEnumError(uint64_t repr) {
  return absl::InvalidArgumentError(
      absl::StrCat("enum field overflow: ", repr));
}

absl::Status Int32Traits::InvalidError(Reader& src, uint64_t repr) {
  return src.StatusOrAnnotate(InvalidError(repr));
}

absl::Status Int32Traits::InvalidError(uint64_t repr) {
  return absl::InvalidArgumentError(
      absl::StrCat("int32 field overflow: ", repr));
}

absl::Status UInt32Traits::InvalidError(Reader& src, uint64_t repr) {
  return src.StatusOrAnnotate(InvalidError(repr));
}

absl::Status UInt32Traits::InvalidError(uint64_t repr) {
  return absl::InvalidArgumentError(
      absl::StrCat("uint32 field overflow: ", repr));
}

absl::Status SInt32Traits::InvalidError(Reader& src, uint64_t repr) {
  return src.StatusOrAnnotate(InvalidError(repr));
}

absl::Status SInt32Traits::InvalidError(uint64_t repr) {
  return absl::InvalidArgumentError(
      absl::StrCat("sint32 field overflow: ", repr));
}

absl::Status BoolTraits::InvalidError(Reader& src, uint64_t repr) {
  return src.StatusOrAnnotate(InvalidError(repr));
}

absl::Status BoolTraits::InvalidError(uint64_t repr) {
  return absl::InvalidArgumentError(
      absl::StrCat("bool field overflow: ", repr));
}

}  // namespace riegeli::field_handlers_internal
