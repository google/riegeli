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

#include <stddef.h>
#include <stdint.h>

#include <limits>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/messages/message_wire_format.h"
#include "riegeli/varint/varint_reading.h"
#include "riegeli/varint/varint_writing.h"

namespace riegeli::serialized_message_rewriter_internal {

absl::Status MessageReaderContextBase::ExtendUnchanged(Reader& src) {
  RIEGELI_ASSERT(supports_random_access())
      << "Failed precondition of MessageReaderContextBase::ExtendUnchanged(): "
         "source does not support random access";
  end_unchanged_ = src.pos();
  RIEGELI_ASSERT_LE(begin_unchanged_, end_unchanged_)
      << "Failed precondition of MessageReaderContextBase::ExtendUnchanged(): "
         "source position moved backward";
  return absl::OkStatus();
}

absl::Status MessageReaderContextBase::CommitUnchanged(
    Reader& src, Position pending_length) {
  const Position original_pos = src.pos();
  const Position length_unchanged = end_unchanged_ - begin_unchanged_;
  if (length_unchanged > 0) {
    if (ABSL_PREDICT_FALSE(!src.Seek(begin_unchanged_))) {
      return src.StatusOrAnnotate(absl::InternalError(
          "Failed to seek to the beginning of the unchanged region"));
    }
    if (ABSL_PREDICT_FALSE(
            !src.Copy(length_unchanged, message_writer().writer()))) {
      return !message_writer().writer().ok()
                 ? message_writer().writer().status()
                 : src.StatusOrAnnotate(absl::InternalError(
                       "Failed to copy the unchanged region"));
    }
    if (ABSL_PREDICT_FALSE(!src.Seek(original_pos))) {
      return src.StatusOrAnnotate(
          absl::InternalError("Failed to seek to the original position"));
    }
  }
  begin_unchanged_ = original_pos + pending_length;
  end_unchanged_ = begin_unchanged_;
  return absl::OkStatus();
}

absl::Status CopyUnchangedField(uint32_t tag, Reader& src,
                                MessageReaderContextBase& context) {
  if (context.supports_random_access()) {
    switch (GetTagWireType(tag)) {
      case WireType::kVarint: {
        if (ABSL_PREDICT_FALSE(!SkipVarint64(src))) {
          return src.StatusOrAnnotate(
              absl::InvalidArgumentError("Could not read a varint field"));
        }
        return context.ExtendUnchanged(src);
      }
      case WireType::kFixed32:
        if (ABSL_PREDICT_FALSE(!src.Skip(sizeof(uint32_t)))) {
          return src.StatusOrAnnotate(
              absl::InvalidArgumentError("Could not read a fixed32 field"));
        }
        return context.ExtendUnchanged(src);
      case WireType::kFixed64:
        if (ABSL_PREDICT_FALSE(!src.Skip(sizeof(uint64_t)))) {
          return src.StatusOrAnnotate(
              absl::InvalidArgumentError("Could not read a fixed64 field"));
        }
        return context.ExtendUnchanged(src);
      case WireType::kLengthDelimited: {
        uint32_t length;
        if (ABSL_PREDICT_FALSE(
                !ReadVarint32(src, length) ||
                length > uint32_t{std::numeric_limits<int32_t>::max()})) {
          return src.StatusOrAnnotate(absl::InvalidArgumentError(
              "Could not read a length-delimited field length"));
        }
        if (ABSL_PREDICT_FALSE(!src.Skip(length))) {
          return src.StatusOrAnnotate(absl::InvalidArgumentError(
              "Could not read a length-delimited field"));
        }
        return context.ExtendUnchanged(src);
      }
      case WireType::kStartGroup:
      case WireType::kEndGroup:
        return context.ExtendUnchanged(src);
      case WireType::kInvalid6:
      case WireType::kInvalid7:
        return src.StatusOrAnnotate(absl::InvalidArgumentError(
            absl::StrCat("Invalid wire type: ", GetTagWireType(tag))));
    }
  } else {
    Writer& dest = context.message_writer().writer();
    if (ABSL_PREDICT_FALSE(!WriteVarint32(tag, dest))) return dest.status();
    switch (GetTagWireType(tag)) {
      case WireType::kVarint: {
        if (ABSL_PREDICT_FALSE(!dest.Push(kMaxLengthVarint64))) {
          return dest.status();
        }
        const size_t length = CopyVarint64(src, dest.cursor());
        if (ABSL_PREDICT_FALSE(length == 0)) {
          return src.StatusOrAnnotate(
              absl::InvalidArgumentError("Could not read a varint field"));
        }
        dest.move_cursor(length);
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
        if (ABSL_PREDICT_FALSE(
                !ReadVarint32(src, length) ||
                length > uint32_t{std::numeric_limits<int32_t>::max()})) {
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
      case WireType::kInvalid6:
      case WireType::kInvalid7:
        return src.StatusOrAnnotate(absl::InvalidArgumentError(
            absl::StrCat("Invalid wire type: ", GetTagWireType(tag))));
    }
  }
  RIEGELI_ASSUME_UNREACHABLE()
      << "Impossible wire type: " << static_cast<int>(GetTagWireType(tag));
}

}  // namespace riegeli::serialized_message_rewriter_internal
