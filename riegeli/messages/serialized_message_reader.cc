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

#include "riegeli/messages/serialized_message_reader.h"

#include <stddef.h>
#include <stdint.h>

#include <functional>
#include <string>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/any.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/type_erased_ref.h"
#include "riegeli/bytes/chain_writer.h"
#include "riegeli/bytes/limiting_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/endian/endian_reading.h"
#include "riegeli/messages/message_wire_format.h"
#include "riegeli/varint/varint_reading.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if !__cpp_inline_variables
constexpr uint32_t SerializedMessageReaderBase::kNumDefinedWireTypes;
#endif

absl::Status SerializedMessageReaderBase::SkipField(
    uint32_t tag, LimitingReaderBase& src,
    ABSL_ATTRIBUTE_UNUSED TypeErasedRef context) {
  switch (GetTagWireType(tag)) {
    case WireType::kVarint: {
      uint64_t value;
      if (ABSL_PREDICT_FALSE(!ReadVarint64(src, value))) {
        return src.StatusOrAnnotate(
            absl::InvalidArgumentError("Could not read a varint field"));
      }
      return absl::OkStatus();
    }
    case WireType::kFixed32:
      if (ABSL_PREDICT_FALSE(!src.Skip(sizeof(uint32_t)))) {
        return src.StatusOrAnnotate(
            absl::InvalidArgumentError("Could not read a fixed32 field"));
      }
      return absl::OkStatus();
    case WireType::kFixed64:
      if (ABSL_PREDICT_FALSE(!src.Skip(sizeof(uint64_t)))) {
        return src.StatusOrAnnotate(
            absl::InvalidArgumentError("Could not read a fixed64 field"));
      }
      return absl::OkStatus();
    case WireType::kLengthDelimited: {
      uint32_t length;
      if (ABSL_PREDICT_FALSE(!ReadVarint32(src, length))) {
        return src.StatusOrAnnotate(absl::InvalidArgumentError(
            "Could not read a length-delimited field length"));
      }
      if (ABSL_PREDICT_FALSE(!src.Skip(length))) {
        return src.StatusOrAnnotate(absl::InvalidArgumentError(
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

absl::Status SerializedMessageReaderBase::NoActionForSubmessage(
    ABSL_ATTRIBUTE_UNUSED int field_number,
    ABSL_ATTRIBUTE_UNUSED TypeErasedRef context) {
  return absl::OkStatus();
}

absl::Status SerializedMessageReaderBase::NoActionForRoot(
    ABSL_ATTRIBUTE_UNUSED TypeErasedRef context) {
  return absl::OkStatus();
}

void SerializedMessageReaderBase::OnInt32(
    absl::Span<const int> field_path,
    std::function<absl::Status(int32_t value, TypeErasedRef context)> action) {
  SetAction(field_path, WireType::kVarint,
            [action](LimitingReaderBase& src, TypeErasedRef context) {
              uint64_t repr;
              if (ABSL_PREDICT_FALSE(!ReadVarint64(src, repr))) {
                return src.StatusOrAnnotate(absl::InvalidArgumentError(
                    "Could not read a varint field"));
              }
              const int32_t value = static_cast<int32_t>(repr);
              if (ABSL_PREDICT_FALSE(static_cast<uint64_t>(value) != repr)) {
                return src.StatusOrAnnotate(absl::InvalidArgumentError(
                    absl::StrCat("int32 field overflow: ", repr)));
              }
              return action(value, context);
            });
  OnLengthDelimited(
      field_path, [action = std::move(action)](LimitingReaderBase& src,
                                               TypeErasedRef context) {
        uint64_t repr;
        while (ReadVarint64(src, repr)) {
          const int32_t value = static_cast<int32_t>(repr);
          if (ABSL_PREDICT_FALSE(static_cast<uint64_t>(value) != repr)) {
            return src.StatusOrAnnotate(absl::InvalidArgumentError(
                absl::StrCat("int32 field overflow: ", repr)));
          }
          if (absl::Status status = action(value, context);
              ABSL_PREDICT_FALSE(!status.ok())) {
            return status;
          }
        }
        if (ABSL_PREDICT_FALSE(src.pos() < src.max_pos())) {
          return src.StatusOrAnnotate(absl::InvalidArgumentError(
              "Could not read a varint element of a packed repeated field"));
        }
        return absl::OkStatus();
      });
}

void SerializedMessageReaderBase::OnUInt32(
    absl::Span<const int> field_path,
    std::function<absl::Status(uint32_t value, TypeErasedRef context)> action) {
  SetAction(field_path, WireType::kVarint,
            [action](LimitingReaderBase& src, TypeErasedRef context) {
              uint64_t repr;
              if (ABSL_PREDICT_FALSE(!ReadVarint64(src, repr))) {
                return src.StatusOrAnnotate(absl::InvalidArgumentError(
                    "Could not read a varint field"));
              }
              const uint32_t value = static_cast<uint32_t>(repr);
              if (ABSL_PREDICT_FALSE(static_cast<uint64_t>(value) != repr)) {
                return src.StatusOrAnnotate(absl::InvalidArgumentError(
                    absl::StrCat("uint32 field overflow: ", repr)));
              }
              return action(value, context);
            });
  OnLengthDelimited(
      field_path, [action = std::move(action)](LimitingReaderBase& src,
                                               TypeErasedRef context) {
        uint64_t repr;
        while (ReadVarint64(src, repr)) {
          const uint32_t value = static_cast<uint32_t>(repr);
          if (ABSL_PREDICT_FALSE(static_cast<uint64_t>(value) != repr)) {
            return src.StatusOrAnnotate(absl::InvalidArgumentError(
                absl::StrCat("uint32 field overflow: ", repr)));
          }
          if (absl::Status status = action(value, context);
              ABSL_PREDICT_FALSE(!status.ok())) {
            return status;
          }
        }
        if (ABSL_PREDICT_FALSE(src.pos() < src.max_pos())) {
          return src.StatusOrAnnotate(absl::InvalidArgumentError(
              "Could not read a varint element of a packed repeated field"));
        }
        return absl::OkStatus();
      });
}

void SerializedMessageReaderBase::OnUInt64(
    absl::Span<const int> field_path,
    std::function<absl::Status(uint64_t value, TypeErasedRef context)> action) {
  SetAction(field_path, WireType::kVarint,
            [action](LimitingReaderBase& src, TypeErasedRef context) {
              uint64_t value;
              if (ABSL_PREDICT_FALSE(!ReadVarint64(src, value))) {
                return src.StatusOrAnnotate(absl::InvalidArgumentError(
                    "Could not read a varint field"));
              }
              return action(value, context);
            });
  OnLengthDelimited(
      field_path, [action = std::move(action)](LimitingReaderBase& src,
                                               TypeErasedRef context) {
        uint64_t value;
        while (ReadVarint64(src, value)) {
          if (absl::Status status = action(value, context);
              ABSL_PREDICT_FALSE(!status.ok())) {
            return status;
          }
        }
        if (ABSL_PREDICT_FALSE(src.pos() < src.max_pos())) {
          return src.StatusOrAnnotate(absl::InvalidArgumentError(
              "Could not read a varint element of a packed repeated field"));
        }
        return absl::OkStatus();
      });
}

void SerializedMessageReaderBase::OnBool(
    absl::Span<const int> field_path,
    std::function<absl::Status(bool value, TypeErasedRef context)> action) {
  SetAction(field_path, WireType::kVarint,
            [action](LimitingReaderBase& src, TypeErasedRef context) {
              uint64_t repr;
              if (ABSL_PREDICT_FALSE(!ReadVarint64(src, repr))) {
                return src.StatusOrAnnotate(absl::InvalidArgumentError(
                    "Could not read a varint field"));
              }
              if (ABSL_PREDICT_FALSE(repr > 1)) {
                return src.StatusOrAnnotate(absl::InvalidArgumentError(
                    absl::StrCat("Invalid bool value: ", repr)));
              }
              return action(repr != 0, context);
            });
  OnLengthDelimited(
      field_path, [action = std::move(action)](LimitingReaderBase& src,
                                               TypeErasedRef context) {
        uint64_t repr;
        while (ReadVarint64(src, repr)) {
          if (ABSL_PREDICT_FALSE(repr > 1)) {
            return src.StatusOrAnnotate(absl::InvalidArgumentError(
                absl::StrCat("Invalid bool value: ", repr)));
          }
          if (absl::Status status = action(repr != 0, context);
              ABSL_PREDICT_FALSE(!status.ok())) {
            return status;
          }
        }
        if (ABSL_PREDICT_FALSE(src.pos() < src.max_pos())) {
          return src.StatusOrAnnotate(absl::InvalidArgumentError(
              "Could not read a varint element of a packed repeated field"));
        }
        return absl::OkStatus();
      });
}

void SerializedMessageReaderBase::OnFixed32(
    absl::Span<const int> field_path,
    std::function<absl::Status(uint32_t value, TypeErasedRef context)> action) {
  SetAction(field_path, WireType::kFixed32,
            [action](LimitingReaderBase& src, TypeErasedRef context) {
              uint32_t value;
              if (ABSL_PREDICT_FALSE(!ReadLittleEndian32(src, value))) {
                return src.StatusOrAnnotate(absl::InvalidArgumentError(
                    "Could not read a fixed32 field"));
              }
              return action(value, context);
            });
  OnLengthDelimited(
      field_path, [action = std::move(action)](LimitingReaderBase& src,
                                               TypeErasedRef context) {
        uint32_t value;
        while (ReadLittleEndian32(src, value)) {
          if (absl::Status status = action(value, context);
              ABSL_PREDICT_FALSE(!status.ok())) {
            return status;
          }
        }
        if (ABSL_PREDICT_FALSE(src.pos() < src.max_pos())) {
          return src.StatusOrAnnotate(absl::InvalidArgumentError(
              "Could not read a fixed32 element of a packed repeated field"));
        }
        return absl::OkStatus();
      });
}

void SerializedMessageReaderBase::OnFixed64(
    absl::Span<const int> field_path,
    std::function<absl::Status(uint64_t value, TypeErasedRef context)> action) {
  SetAction(field_path, WireType::kFixed64,
            [action](LimitingReaderBase& src, TypeErasedRef context) {
              uint64_t value;
              if (ABSL_PREDICT_FALSE(!ReadLittleEndian64(src, value))) {
                return src.StatusOrAnnotate(absl::InvalidArgumentError(
                    "Could not read a fixed64 field"));
              }
              return action(value, context);
            });
  OnLengthDelimited(
      field_path, [action = std::move(action)](LimitingReaderBase& src,
                                               TypeErasedRef context) {
        uint64_t value;
        while (ReadLittleEndian64(src, value)) {
          if (absl::Status status = action(value, context);
              ABSL_PREDICT_FALSE(!status.ok())) {
            return status;
          }
        }
        if (ABSL_PREDICT_FALSE(src.pos() < src.max_pos())) {
          return src.StatusOrAnnotate(absl::InvalidArgumentError(
              "Could not read a fixed64 element of a packed repeated field"));
        }
        return absl::OkStatus();
      });
}

void SerializedMessageReaderBase::OnStringView(
    absl::Span<const int> field_path,
    std::function<absl::Status(absl::string_view value, TypeErasedRef context)>
        action) {
  OnLengthUnchecked(field_path, [action = std::move(action)](
                                    size_t length, LimitingReaderBase& src,
                                    TypeErasedRef context) {
    absl::string_view value;
    if (ABSL_PREDICT_FALSE(!src.Read(length, value))) {
      return src.StatusOrAnnotate(absl::InvalidArgumentError(
          "Could not read a length-delimited field"));
    }
    return action(value, context);
  });
}

void SerializedMessageReaderBase::OnString(
    absl::Span<const int> field_path,
    std::function<absl::Status(std::string&& value, TypeErasedRef context)>
        action) {
  OnLengthUnchecked(field_path, [action = std::move(action)](
                                    size_t length, LimitingReaderBase& src,
                                    TypeErasedRef context) {
    std::string value;
    if (ABSL_PREDICT_FALSE(!src.Read(length, value))) {
      return src.StatusOrAnnotate(absl::InvalidArgumentError(
          "Could not read a length-delimited field"));
    }
    return action(std::move(value), context);
  });
}

void SerializedMessageReaderBase::OnChain(
    absl::Span<const int> field_path,
    std::function<absl::Status(Chain&& value, TypeErasedRef context)> action) {
  OnLengthUnchecked(field_path, [action = std::move(action)](
                                    size_t length, LimitingReaderBase& src,
                                    TypeErasedRef context) {
    Chain value;
    if (ABSL_PREDICT_FALSE(!src.Read(length, value))) {
      return src.StatusOrAnnotate(absl::InvalidArgumentError(
          "Could not read a length-delimited field"));
    }
    return action(std::move(value), context);
  });
}

void SerializedMessageReaderBase::OnCord(
    absl::Span<const int> field_path,
    std::function<absl::Status(absl::Cord&& value, TypeErasedRef context)>
        action) {
  OnLengthUnchecked(field_path, [action = std::move(action)](
                                    size_t length, LimitingReaderBase& src,
                                    TypeErasedRef context) {
    absl::Cord value;
    if (ABSL_PREDICT_FALSE(!src.Read(length, value))) {
      return src.StatusOrAnnotate(absl::InvalidArgumentError(
          "Could not read a length-delimited field"));
    }
    return action(std::move(value), context);
  });
}

void SerializedMessageReaderBase::OnLengthDelimited(
    absl::Span<const int> field_path,
    std::function<absl::Status(LimitingReaderBase& src, TypeErasedRef context)>
        action) {
  OnLengthUnchecked(field_path, [action = std::move(action)](
                                    size_t length, LimitingReaderBase& src,
                                    TypeErasedRef context) {
    ScopedLimiter scoped_limiter(
        &src, ScopedLimiter::Options().set_exact_length(length));
    absl::Status status = action(src, context);
    src.Seek(src.max_pos());
    return status;
  });
}

void SerializedMessageReaderBase::OnLengthUnchecked(
    absl::Span<const int> field_path,
    std::function<absl::Status(size_t length, LimitingReaderBase& src,
                               TypeErasedRef context)>
        action) {
  SetAction(field_path, WireType::kLengthDelimited,
            [action = std::move(action)](LimitingReaderBase& src,
                                         TypeErasedRef context) {
              uint32_t length;
              if (ABSL_PREDICT_FALSE(!ReadVarint32(src, length))) {
                return src.StatusOrAnnotate(absl::InvalidArgumentError(
                    "Could not read a length-delimited field length"));
              }
              return action(size_t{length}, src, context);
            });
}

void SerializedMessageReaderBase::BeforeMessage(
    absl::Span<const int> field_path,
    std::function<absl::Status(TypeErasedRef context)> action) {
  if (field_path.empty()) {
    before_root_ = std::move(action);
  } else {
    GetField(field_path)->before_message = std::move(action);
  }
}

void SerializedMessageReaderBase::AfterMessage(
    absl::Span<const int> field_path,
    std::function<absl::Status(TypeErasedRef context)> action) {
  if (field_path.empty()) {
    after_root_ = std::move(action);
  } else {
    GetField(field_path)->after_message = std::move(action);
  }
}

inline void SerializedMessageReaderBase::SetAction(
    absl::Span<const int> field_path, WireType wire_type,
    std::function<absl::Status(LimitingReaderBase& src, TypeErasedRef context)>
        action) {
  RIEGELI_ASSERT(static_cast<uint32_t>(wire_type) < kNumDefinedWireTypes)
      << "Failed precondition of SerializedMessageReaderBase::SetAction(): "
         "invalid wire type: "
      << static_cast<uint32_t>(wire_type);
  GetField(field_path)->actions[static_cast<uint32_t>(wire_type)] =
      std::move(action);
}

inline SerializedMessageReaderBase::Field*
SerializedMessageReaderBase::GetField(absl::Span<const int> field_path) {
  RIEGELI_ASSERT(!field_path.empty())
      << "Failed precondition of SerializedMessageReaderBase::SetAction*(): "
         "empty field path";
  absl::flat_hash_map<int, Field>* fields = &root_;
  Field* field;
  for (const int field_number : field_path) {
    field = &(*fields)[field_number];
    fields = &field->children;
  }
  return field;
}

absl::Status SerializedMessageReaderBase::Read(AnyRef<Reader*> src,
                                               TypeErasedRef context) const {
  if (src.IsOwning()) src->SetReadAllHint(true);
  LimitingReader<AnyRef<Reader*>> src_limiting(std::move(src));
  absl::Status status = ReadRootMessage(src_limiting, context);
  if (ABSL_PREDICT_TRUE(status.ok())) src_limiting.VerifyEnd();
  if (ABSL_PREDICT_FALSE(!src_limiting.Close())) {
    status.Update(src_limiting.status());
  }
  return status;
}

inline absl::Status SerializedMessageReaderBase::ReadRootMessage(
    LimitingReaderBase& src, TypeErasedRef context) const {
  {
    absl::Status status = before_root_(context);
    if (ABSL_PREDICT_FALSE(!status.ok())) {
      return status;
    }
  }
  {
    absl::Status status = ReadMessage(src, root_, context);
    if (ABSL_PREDICT_FALSE(!status.ok())) {
      return status;
    }
  }
  {
    absl::Status status = after_root_(context);
    if (ABSL_PREDICT_FALSE(!status.ok())) {
      return status;
    }
  }
  return absl::OkStatus();
}

absl::Status SerializedMessageReaderBase::ReadMessage(
    LimitingReaderBase& src, const absl::flat_hash_map<int, Field>& fields,
    TypeErasedRef context) const {
  uint32_t tag;
  while (ReadVarint32(src, tag)) {
    const WireType wire_type = GetTagWireType(tag);
    if (ABSL_PREDICT_FALSE(static_cast<uint32_t>(wire_type) >=
                           kNumDefinedWireTypes)) {
      return src.AnnotateStatus(absl::InvalidArgumentError(
          absl::StrCat("Invalid wire type: ", wire_type)));
    }
    {
      const auto iter = fields.find(GetTagFieldNumber(tag));
      if (iter != fields.end()) {
        const std::function<absl::Status(LimitingReaderBase & src,
                                         TypeErasedRef context)>& action =
            iter->second.actions[static_cast<uint32_t>(wire_type)];
        if (action != nullptr) {
          absl::Status status = action(src, context);
          if (ABSL_PREDICT_FALSE(!status.ok())) return status;
          continue;
        }
        if (wire_type == WireType::kLengthDelimited &&
            (iter->second.before_message != nullptr ||
             iter->second.after_message != nullptr ||
             !iter->second.children.empty())) {
          uint32_t length;
          if (ABSL_PREDICT_FALSE(!ReadVarint32(src, length))) {
            return src.StatusOrAnnotate(absl::InvalidArgumentError(
                "Could not read a length-delimited field length"));
          }
          ScopedLimiter scoped_limiter(
              &src, ScopedLimiter::Options().set_exact_length(size_t{length}));
          if (absl::Status status =
                  iter->second.before_message != nullptr
                      ? iter->second.before_message(context)
                      : before_other_message_(GetTagFieldNumber(tag), context);
              ABSL_PREDICT_FALSE(!status.ok())) {
            return status;
          }
          if (absl::Status status =
                  ReadMessage(src, iter->second.children, context);
              ABSL_PREDICT_FALSE(!status.ok())) {
            return status;
          }
          if (absl::Status status =
                  iter->second.after_message != nullptr
                      ? iter->second.after_message(context)
                      : after_other_message_(GetTagFieldNumber(tag), context);
              ABSL_PREDICT_FALSE(!status.ok())) {
            return status;
          }
          continue;
        }
      }
    }
    absl::Status status = on_other_(tag, src, context);
    if (ABSL_PREDICT_FALSE(!status.ok())) return status;
  }
  return absl::OkStatus();
}

}  // namespace riegeli
