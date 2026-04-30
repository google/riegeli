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

#include <stddef.h>
#include <stdint.h>

#include <utility>

#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
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

absl::Status ReadPackedVarintError() {
  return absl::InvalidArgumentError(
      "Could not read a varint element of a packed repeated field");
}

absl::Status ReadPackedVarintError(Reader& src) {
  return src.StatusOrAnnotate(ReadPackedVarintError());
}

template <>
absl::Status ReadPackedFixedError<sizeof(uint32_t)>() {
  return absl::InvalidArgumentError(
      "Could not read a fixed32 element of a packed repeated field");
}

template <>
absl::Status ReadPackedFixedError<sizeof(uint64_t)>() {
  return absl::InvalidArgumentError(
      "Could not read a fixed64 element of a packed repeated field");
}

template <>
absl::Status ReadPackedFixedError<sizeof(uint32_t)>(Reader& src) {
  return src.StatusOrAnnotate(ReadPackedFixedError<sizeof(uint32_t)>());
}

template <>
absl::Status ReadPackedFixedError<sizeof(uint64_t)>(Reader& src) {
  return src.StatusOrAnnotate(ReadPackedFixedError<sizeof(uint64_t)>());
}

}  // namespace riegeli::field_handlers_internal
