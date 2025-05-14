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

#include "riegeli/messages/serialized_message_rewriter.h"

#include <cstddef>
#include <cstdint>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/types/optional.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/messages/message_wire_format.h"
#include "riegeli/varint/varint_reading.h"
#include "riegeli/varint/varint_writing.h"

namespace riegeli {
namespace serialized_message_rewriter_internal {

absl::Status CopyField(uint32_t tag, Reader& src, Writer& dest) {
  if (ABSL_PREDICT_FALSE(!WriteVarint32(tag, dest))) {
    return dest.status();
  }
  switch (GetTagWireType(tag)) {
    case WireType::kVarint: {
      if (ABSL_PREDICT_FALSE(!dest.Push(kMaxLengthVarint64))) {
        return dest.status();
      }
      const absl::optional<size_t> length = CopyVarint64(src, dest.cursor());
      if (ABSL_PREDICT_FALSE(length == absl::nullopt)) {
        return src.StatusOrAnnotate(
            absl::InvalidArgumentError("Could not read a varint field"));
      }
      dest.move_cursor(*length);
      return absl::OkStatus();
    }
    case WireType::kFixed32:
      if (ABSL_PREDICT_FALSE(!src.Copy(sizeof(uint32_t), dest))) {
        return !dest.ok() ? dest.status()
                          : src.StatusOrAnnotate(absl::InvalidArgumentError(
                                "Could not read a fixed32 field"));
      }
      return absl::OkStatus();
    case WireType::kFixed64:
      if (ABSL_PREDICT_FALSE(!src.Copy(sizeof(uint64_t), dest))) {
        return !dest.ok() ? dest.status()
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
      if (ABSL_PREDICT_FALSE(!WriteVarint32(length, dest))) {
        return dest.status();
      }
      if (ABSL_PREDICT_FALSE(!src.Copy(length, dest))) {
        return !dest.ok() ? dest.status()
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

}  // namespace serialized_message_rewriter_internal
}  // namespace riegeli
