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

#ifndef RIEGELI_MESSAGES_FIELD_HANDLERS_H_
#define RIEGELI_MESSAGES_FIELD_HANDLERS_H_

#include <stddef.h>
#include <stdint.h>

#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/casts.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/cord_iterator_span.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/limiting_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/endian/endian_reading.h"
#include "riegeli/messages/map_entry_field.h"  // IWYU pragma: export
#include "riegeli/messages/serialized_message_reader2.h"
#include "riegeli/messages/serialized_message_reader_internal.h"
#include "riegeli/varint/varint_reading.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

namespace field_handlers {

// The kind of a varint field, assuming that its C++ type is known.
enum class VarintKind {
  kPlain,   // `int32`, `int64`, `uint32`, `uint64`, `bool`
  kSigned,  // `sint32`, `sint64`
  kEnum,    // `enum`
};

// Class types of common field handlers. Usually it is enough to use `auto`
// instead of spelling these types.
template <typename Value, VarintKind kind, int field_number, typename Action>
class OnOptionalVarintType;
template <typename Value, VarintKind kind, int field_number, typename Action>
class OnRepeatedVarintType;
template <typename Value, int field_number, typename Action>
class OnOptionalFixedType;
template <typename Value, int field_number, typename Action>
class OnRepeatedFixedType;
template <int field_number, typename Action>
class OnLengthDelimitedType;
template <int field_number, typename Action>
class BeforeGroupType;
template <int field_number, typename Action>
class AfterGroupType;

// Common field handlers for `SerializedMessageReader2`, `DynamicFieldHandler`,
// and `FieldHandlerMap`.
//
// For a `MessageType` with generated code, the field number of a field named
// `foo_bar` can be obtained as `MessageType::kFooBarFieldNumber`.
//
// For numeric fields, `Optional` and `Repeated` variants of field handlers are
// provided. An `Optional` variant is intended for singular fields, and it
// will be called also for elements of a non-packed repeated field (which have
// the same wire representation). A `Repeated` variant is intended for repeated
// fields (packed or not), and it will be called also for singular fields
// (which have the same wire representation as a non-packed repeated field).
//
// For varint fields, in contrast to native proto parsing, 64-bit values which
// overflow the provided 32-bit type are reported as errors instead of being
// silently truncated.
//
// Two kinds of field handlers are provided by these functions:
//
//  * Static, which handle a single field number known at compile time.
//    The `field_number` template parameter must be specified and positive.
//
//  * Unbound, which are meant to be registered in a `FieldHandlerMap` with a
//    field number specified during registration. The `field_number` template
//    parameter must not be specified.

// Field handler of a singular `int32` field. The value is provided as
// `int32_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnOptionalVarintType<int32_t, VarintKind::kPlain, field_number,
                               std::decay_t<Action>>
OnOptionalInt32(Action&& action) {
  return OnOptionalVarintType<int32_t, VarintKind::kPlain, field_number,
                              std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of an element of a repeated `int32` field. The value is
// provided as `int32_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnRepeatedVarintType<int32_t, VarintKind::kPlain, field_number,
                               std::decay_t<Action>>
OnRepeatedInt32(Action&& action) {
  return OnRepeatedVarintType<int32_t, VarintKind::kPlain, field_number,
                              std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of a singular `int64` field. The value is provided as
// `int64_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnOptionalVarintType<int64_t, VarintKind::kPlain, field_number,
                               std::decay_t<Action>>
OnOptionalInt64(Action&& action) {
  return OnOptionalVarintType<int64_t, VarintKind::kPlain, field_number,
                              std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of an element of a repeated `int64` field. The value is
// provided as `int64_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnRepeatedVarintType<int64_t, VarintKind::kPlain, field_number,
                               std::decay_t<Action>>
OnRepeatedInt64(Action&& action) {
  return OnRepeatedVarintType<int64_t, VarintKind::kPlain, field_number,
                              std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of a singular `uint32` field. The value is provided as
// `uint32_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnOptionalVarintType<uint32_t, VarintKind::kPlain, field_number,
                               std::decay_t<Action>>
OnOptionalUInt32(Action&& action) {
  return OnOptionalVarintType<uint32_t, VarintKind::kPlain, field_number,
                              std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of an element of a repeated `uint32` field. The value is
// provided as `uint32_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnRepeatedVarintType<uint32_t, VarintKind::kPlain, field_number,
                               std::decay_t<Action>>
OnRepeatedUInt32(Action&& action) {
  return OnRepeatedVarintType<uint32_t, VarintKind::kPlain, field_number,
                              std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of a singular `uint64` field. The value is provided as
// `uint64_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnOptionalVarintType<uint64_t, VarintKind::kPlain, field_number,
                               std::decay_t<Action>>
OnOptionalUInt64(Action&& action) {
  return OnOptionalVarintType<uint64_t, VarintKind::kPlain, field_number,
                              std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of an element of a repeated `uint64` field. The value is
// provided as `uint64_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnRepeatedVarintType<uint64_t, VarintKind::kPlain, field_number,
                               std::decay_t<Action>>
OnRepeatedUInt64(Action&& action) {
  return OnRepeatedVarintType<uint64_t, VarintKind::kPlain, field_number,
                              std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of a singular `sint32` field. The value is provided as
// `int32_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnOptionalVarintType<int32_t, VarintKind::kSigned, field_number,
                               std::decay_t<Action>>
OnOptionalSInt32(Action&& action) {
  return OnOptionalVarintType<int32_t, VarintKind::kSigned, field_number,
                              std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of an element of a repeated `sint32` field. The value is
// provided as `int32_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnRepeatedVarintType<int32_t, VarintKind::kSigned, field_number,
                               std::decay_t<Action>>
OnRepeatedSInt32(Action&& action) {
  return OnRepeatedVarintType<int32_t, VarintKind::kSigned, field_number,
                              std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of a singular `sint64` field. The value is provided as
// `int64_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnOptionalVarintType<int64_t, VarintKind::kSigned, field_number,
                               std::decay_t<Action>>
OnOptionalSInt64(Action&& action) {
  return OnOptionalVarintType<int64_t, VarintKind::kSigned, field_number,
                              std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of an element of a repeated `sint64` field. The value is
// provided as `int64_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnRepeatedVarintType<int64_t, VarintKind::kSigned, field_number,
                               std::decay_t<Action>>
OnRepeatedSInt64(Action&& action) {
  return OnRepeatedVarintType<int64_t, VarintKind::kSigned, field_number,
                              std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of a singular `bool` field. The value is provided as `bool`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnOptionalVarintType<bool, VarintKind::kPlain, field_number,
                               std::decay_t<Action>>
OnOptionalBool(Action&& action) {
  return OnOptionalVarintType<bool, VarintKind::kPlain, field_number,
                              std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of an element of a repeated `bool` field. The value is provided
// as `bool`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnRepeatedVarintType<bool, VarintKind::kPlain, field_number,
                               std::decay_t<Action>>
OnRepeatedBool(Action&& action) {
  return OnRepeatedVarintType<bool, VarintKind::kPlain, field_number,
                              std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of a singular `fixed32` field. The value is provided as
// `uint32_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnOptionalFixedType<uint32_t, field_number, std::decay_t<Action>>
OnOptionalFixed32(Action&& action) {
  return OnOptionalFixedType<uint32_t, field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of an element of a repeated `fixed32` field. The value is
// provided as `uint32_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnRepeatedFixedType<uint32_t, field_number, std::decay_t<Action>>
OnRepeatedFixed32(Action&& action) {
  return OnRepeatedFixedType<uint32_t, field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of a singular `fixed64` field. The value is provided as
// `uint64_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnOptionalFixedType<uint64_t, field_number, std::decay_t<Action>>
OnOptionalFixed64(Action&& action) {
  return OnOptionalFixedType<uint64_t, field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of an element of a repeated `fixed64` field. The value is
// provided as `uint64_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnRepeatedFixedType<uint64_t, field_number, std::decay_t<Action>>
OnRepeatedFixed64(Action&& action) {
  return OnRepeatedFixedType<uint64_t, field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of a singular `sfixed32` field. The value is provided as
// `int32_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnOptionalFixedType<int32_t, field_number, std::decay_t<Action>>
OnOptionalSFixed32(Action&& action) {
  return OnOptionalFixedType<int32_t, field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of an element of a repeated `sfixed32` field. The value is
// provided as `int32_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnRepeatedFixedType<int32_t, field_number, std::decay_t<Action>>
OnRepeatedSFixed32(Action&& action) {
  return OnRepeatedFixedType<int32_t, field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of a singular `sfixed64` field. The value is provided as
// `int64_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnOptionalFixedType<int64_t, field_number, std::decay_t<Action>>
OnOptionalSFixed64(Action&& action) {
  return OnOptionalFixedType<int64_t, field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of an element of a repeated `sfixed64` field. The value is
// provided as `int64_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnRepeatedFixedType<int64_t, field_number, std::decay_t<Action>>
OnRepeatedSFixed64(Action&& action) {
  return OnRepeatedFixedType<int64_t, field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of a singular `float` field. The value is provided as `float`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnOptionalFixedType<float, field_number, std::decay_t<Action>>
OnOptionalFloat(Action&& action) {
  return OnOptionalFixedType<float, field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of an element of a repeated `float` field. The value is
// provided as `float`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnRepeatedFixedType<float, field_number, std::decay_t<Action>>
OnRepeatedFloat(Action&& action) {
  return OnRepeatedFixedType<float, field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of a singular `double` field. The value is provided as
// `double`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnOptionalFixedType<double, field_number, std::decay_t<Action>>
OnOptionalDouble(Action&& action) {
  return OnOptionalFixedType<double, field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of an element of a repeated `double` field. The value is
// provided as `double`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnRepeatedFixedType<double, field_number, std::decay_t<Action>>
OnRepeatedDouble(Action&& action) {
  return OnRepeatedFixedType<double, field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of a singular enum field. The value is provided as an enum type
// (C++ or proto enum) or an integral type.
template <
    typename EnumType, int field_number = kUnboundFieldNumber, typename Action,
    std::enable_if_t<
        std::disjunction_v<std::is_enum<EnumType>, std::is_integral<EnumType>>,
        int> = 0>
constexpr OnOptionalVarintType<EnumType, VarintKind::kEnum, field_number,
                               std::decay_t<Action>>
OnOptionalEnum(Action&& action) {
  return OnOptionalVarintType<EnumType, VarintKind::kEnum, field_number,
                              std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of an element of a repeated enum field. The value is provided
// as an enum type (C++ or proto enum) or an integral type.
template <
    typename EnumType, int field_number = kUnboundFieldNumber, typename Action,
    std::enable_if_t<
        std::disjunction_v<std::is_enum<EnumType>, std::is_integral<EnumType>>,
        int> = 0>
constexpr OnRepeatedVarintType<EnumType, VarintKind::kEnum, field_number,
                               std::decay_t<Action>>
OnRepeatedEnum(Action&& action) {
  return OnRepeatedVarintType<EnumType, VarintKind::kEnum, field_number,
                              std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of a singular or an element of a repeated `string`, `bytes`,
// or submessage field.
//
// For a `Reader` source, the value is provided as `ReaderSpan<>`,
// `absl::string_view`, `std::string&&`, `Chain&&`, or `absl::Cord&&`,
// depending on what is accepted.
//
// For a `Cord` source, the value is provided as `CordIteratorSpan`,
// `absl::string_view`, `std::string&&`, `Chain&&`, or `absl::Cord&&`,
// depending on what is accepted.
//
// For a string source, the value is provided as `absl::string_view`,
// `std::string&&`, `Chain&&`, or `absl::Cord&&`, depending on what is accepted.
//
// For a string source, if the action accepts `absl::string_view`, then the
// value is guaranteed to be a substring of the original string. This guarantee
// is absent for a `Reader` or `Cord` source.
//
// If the action accepts the first candidate for each source type, i.e.
// `ReaderSpan<>`, `CordIteratorSpan`, and `absl::string_view`, then the action
// parameter can be declared as `auto`. This is convenient if the implementation
// can treat these types uniformly, e.g. when the value is passed to
// `riegeli::ParseMessage()`, `SerializedMessageReader2::ReadMessage()`, or
// `SerializedMessageWriter::WriteString()`.
//
// Alternatively, the action can use `absl::Overload{}` to provide variants with
// separate implementations for these parameter types.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnLengthDelimitedType<field_number, std::decay_t<Action>>
OnLengthDelimited(Action&& action) {
  return OnLengthDelimitedType<field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler called before the given group.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr BeforeGroupType<field_number, std::decay_t<Action>> BeforeGroup(
    Action&& action) {
  return BeforeGroupType<field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler called after the given group.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr AfterGroupType<field_number, std::decay_t<Action>> AfterGroup(
    Action&& action) {
  return AfterGroupType<field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

}  // namespace field_handlers

// Implementation details follow.

namespace field_handlers_internal {

ABSL_ATTRIBUTE_COLD absl::Status AnnotateByReader(absl::Status status,
                                                  Reader& reader);

ABSL_ATTRIBUTE_COLD absl::Status EnumOverflowError(uint64_t repr);

template <typename Value, field_handlers::VarintKind kind>
absl::Status VarintOverflowError(uint64_t repr) {
  RIEGELI_ASSERT(kind == field_handlers::VarintKind::kEnum)
      << "Remaining VarintOverflowError() instantiations should be for enums";
  return EnumOverflowError(repr);
}
template <>
ABSL_ATTRIBUTE_COLD absl::Status
VarintOverflowError<int32_t, field_handlers::VarintKind::kPlain>(uint64_t repr);
template <>
ABSL_ATTRIBUTE_COLD absl::Status
VarintOverflowError<uint32_t, field_handlers::VarintKind::kPlain>(
    uint64_t repr);
template <>
ABSL_ATTRIBUTE_COLD absl::Status
VarintOverflowError<int32_t, field_handlers::VarintKind::kSigned>(
    uint64_t repr);
template <>
ABSL_ATTRIBUTE_COLD absl::Status
VarintOverflowError<bool, field_handlers::VarintKind::kPlain>(uint64_t repr);

ABSL_ATTRIBUTE_COLD absl::Status EnumOverflowError(Reader& src, uint64_t repr);

template <typename Value, field_handlers::VarintKind kind>
absl::Status VarintOverflowError(Reader& src, uint64_t repr) {
  RIEGELI_ASSERT(kind == field_handlers::VarintKind::kEnum)
      << "Remaining VarintOverflowError() instantiations should be for enums";
  return EnumOverflowError(src, repr);
}
template <>
ABSL_ATTRIBUTE_COLD absl::Status
VarintOverflowError<int32_t, field_handlers::VarintKind::kPlain>(Reader& src,
                                                                 uint64_t repr);
template <>
ABSL_ATTRIBUTE_COLD absl::Status
VarintOverflowError<uint32_t, field_handlers::VarintKind::kPlain>(
    Reader& src, uint64_t repr);
template <>
ABSL_ATTRIBUTE_COLD absl::Status
VarintOverflowError<int32_t, field_handlers::VarintKind::kSigned>(
    Reader& src, uint64_t repr);
template <>
ABSL_ATTRIBUTE_COLD absl::Status
VarintOverflowError<bool, field_handlers::VarintKind::kPlain>(Reader& src,
                                                              uint64_t repr);

ABSL_ATTRIBUTE_COLD absl::Status ReadPackedVarintError();

ABSL_ATTRIBUTE_COLD absl::Status ReadPackedVarintError(Reader& src);

template <size_t size>
absl::Status ReadPackedFixedError();
template <>
ABSL_ATTRIBUTE_COLD absl::Status ReadPackedFixedError<sizeof(uint32_t)>();
template <>
ABSL_ATTRIBUTE_COLD absl::Status ReadPackedFixedError<sizeof(uint64_t)>();

template <size_t size>
absl::Status ReadPackedFixedError(Reader& src);
template <>
ABSL_ATTRIBUTE_COLD absl::Status ReadPackedFixedError<sizeof(uint32_t)>(
    Reader& src);
template <>
ABSL_ATTRIBUTE_COLD absl::Status ReadPackedFixedError<sizeof(uint64_t)>(
    Reader& src);

template <typename Value, field_handlers::VarintKind kind>
inline bool VarintIsValid(uint64_t repr) {
  if constexpr (std::disjunction_v<std::is_same<Value, uint64_t>,
                                   std::is_same<Value, int64_t>>) {
    return true;
  } else if constexpr (kind == field_handlers::VarintKind::kSigned) {
    return uint64_t{static_cast<std::make_unsigned_t<Value>>(repr)} == repr;
  } else if constexpr (std::is_enum_v<Value>) {
    static_assert(kind == field_handlers::VarintKind::kEnum);
    return static_cast<uint64_t>(
               static_cast<std::underlying_type_t<Value>>(repr)) == repr;
  } else {
    return static_cast<uint64_t>(static_cast<Value>(repr)) == repr;
  }
}

template <typename Value, field_handlers::VarintKind kind>
inline Value DecodeVarint(uint64_t repr) {
  if constexpr (kind == field_handlers::VarintKind::kSigned) {
    return static_cast<Value>(DecodeVarintSigned64(repr));
  } else if constexpr (std::is_enum_v<Value>) {
    static_assert(kind == field_handlers::VarintKind::kEnum);
    // Casting an out of range value to an enum has undefined behavior.
    // Casting such a value to an integral type wraps around.
    return static_cast<Value>(static_cast<std::underlying_type_t<Value>>(repr));
  } else {
    return static_cast<Value>(repr);
  }
}

}  // namespace field_handlers_internal

namespace field_handlers {

template <typename Value, VarintKind kind, int field_number, typename Action>
class OnOptionalVarintType {
 public:
  static constexpr int kFieldNumber = field_number;

  template <typename ActionInitializer,
            std::enable_if_t<std::is_convertible_v<ActionInitializer&&, Action>,
                             int> = 0>
  explicit constexpr OnOptionalVarintType(ActionInitializer&& action)
      : action_(std::forward<ActionInitializer>(action)) {}

  template <
      typename... Context,
      std::enable_if_t<std::is_invocable_v<const Action&, Value, Context&...>,
                       int> = 0>
  absl::Status HandleVarint(uint64_t repr, Context&... context) const {
    if (ABSL_PREDICT_FALSE(
            (!field_handlers_internal::VarintIsValid<Value, kind>(repr)))) {
      return field_handlers_internal::VarintOverflowError<Value, kind>(repr);
    }
    return action_(field_handlers_internal::DecodeVarint<Value, kind>(repr),
                   context...);
  }

 protected:
  const Action& action() const { return action_; }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Action action_;
};

template <typename Value, VarintKind kind, int field_number, typename Action>
class OnRepeatedVarintType
    : public OnOptionalVarintType<Value, kind, field_number, Action> {
 public:
  using OnRepeatedVarintType::OnOptionalVarintType::OnOptionalVarintType;

  template <
      typename... Context,
      std::enable_if_t<std::is_invocable_v<const Action&, Value, Context&...>,
                       int> = 0>
  absl::Status HandleLengthDelimitedFromReader(ReaderSpan<> repr,
                                               Context&... context) const;

  template <
      typename... Context,
      std::enable_if_t<std::is_invocable_v<const Action&, Value, Context&...>,
                       int> = 0>
  absl::Status HandleLengthDelimitedFromCord(CordIteratorSpan repr,
                                             std::string& scratch,
                                             Context&... context) const;

  template <
      typename... Context,
      std::enable_if_t<std::is_invocable_v<const Action&, Value, Context&...>,
                       int> = 0>
  absl::Status HandleLengthDelimitedFromString(absl::string_view repr,
                                               Context&... context) const;
};

template <typename Value, int field_number, typename Action>
class OnOptionalFixedType {
 public:
  static constexpr int kFieldNumber = field_number;

  template <typename ActionInitializer,
            std::enable_if_t<std::is_convertible_v<ActionInitializer&&, Action>,
                             int> = 0>
  explicit constexpr OnOptionalFixedType(ActionInitializer&& action)
      : action_(std::forward<ActionInitializer>(action)) {}

  template <typename... Context,
            std::enable_if_t<
                std::conjunction_v<
                    std::bool_constant<sizeof(Value) == sizeof(uint32_t)>,
                    std::is_invocable<const Action&, Value, Context&...>>,
                int> = 0>
  absl::Status HandleFixed32(uint32_t repr, Context&... context) const {
    return action_(absl::bit_cast<Value>(repr), context...);
  }

  template <typename... Context,
            std::enable_if_t<
                std::conjunction_v<
                    std::bool_constant<sizeof(Value) == sizeof(uint64_t)>,
                    std::is_invocable<const Action&, Value, Context&...>>,
                int> = 0>
  absl::Status HandleFixed64(uint64_t repr, Context&... context) const {
    return action_(absl::bit_cast<Value>(repr), context...);
  }

 protected:
  const Action& action() const { return action_; }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Action action_;
};

template <typename Value, int field_number, typename Action>
class OnRepeatedFixedType
    : public OnOptionalFixedType<Value, field_number, Action> {
 public:
  using OnRepeatedFixedType::OnOptionalFixedType::OnOptionalFixedType;

  template <
      typename... Context,
      std::enable_if_t<std::is_invocable_v<const Action&, Value, Context&...>,
                       int> = 0>
  absl::Status HandleLengthDelimitedFromReader(ReaderSpan<> repr,
                                               Context&... context) const;

  template <
      typename... Context,
      std::enable_if_t<std::is_invocable_v<const Action&, Value, Context&...>,
                       int> = 0>
  absl::Status HandleLengthDelimitedFromCord(CordIteratorSpan repr,
                                             std::string& scratch,
                                             Context&... context) const;

  template <
      typename... Context,
      std::enable_if_t<std::is_invocable_v<const Action&, Value, Context&...>,
                       int> = 0>
  absl::Status HandleLengthDelimitedFromString(absl::string_view repr,
                                               Context&... context) const;
};

template <int field_number, typename Action>
class OnLengthDelimitedType {
 public:
  static constexpr int kFieldNumber = field_number;

  template <typename ActionInitializer,
            std::enable_if_t<std::is_convertible_v<ActionInitializer&&, Action>,
                             int> = 0>
  explicit constexpr OnLengthDelimitedType(ActionInitializer&& action)
      : action_(std::forward<ActionInitializer>(action)) {}

  template <
      typename... Context,
      std::enable_if_t<
          std::disjunction_v<
              std::is_invocable<const Action&, ReaderSpan<>, Context&...>,
              std::is_invocable<const Action&, absl::string_view, Context&...>,
              std::is_invocable<const Action&, std::string&&, Context&...>,
              std::is_invocable<const Action&, Chain&&, Context&...>,
              std::is_invocable<const Action&, absl::Cord&&, Context&...>>,
          int> = 0>
  absl::Status HandleLengthDelimitedFromReader(ReaderSpan<> repr,
                                               Context&... context) const {
    if constexpr (std::is_invocable_v<const Action&, ReaderSpan<>,
                                      Context&...>) {
      return SkipLengthDelimitedFromReader(
          repr, [&] { return action_(std::move(repr), context...); });
    } else if constexpr (std::is_invocable_v<const Action&, absl::string_view,
                                             Context&...>) {
      return HandleString<absl::string_view>(std::move(repr), context...);
    } else if constexpr (std::is_invocable_v<const Action&, std::string&&,
                                             Context&...>) {
      return HandleString<std::string>(std::move(repr), context...);
    } else if constexpr (std::is_invocable_v<const Action&, Chain&&,
                                             Context&...>) {
      return HandleString<Chain>(std::move(repr), context...);
    } else if constexpr (std::is_invocable_v<const Action&, absl::Cord&&,
                                             Context&...>) {
      return HandleString<absl::Cord>(std::move(repr), context...);
    } else {
      static_assert(false, "No string-like type accepted");
    }
  }

  template <
      typename... Context,
      std::enable_if_t<
          std::disjunction_v<
              std::is_invocable<const Action&, CordIteratorSpan, Context&...>,
              std::is_invocable<const Action&, absl::string_view, Context&...>,
              std::is_invocable<const Action&, std::string&&, Context&...>,
              std::is_invocable<const Action&, Chain, Context&...>,
              std::is_invocable<const Action&, absl::Cord, Context&...>>,
          int> = 0>
  absl::Status HandleLengthDelimitedFromCord(CordIteratorSpan repr,
                                             std::string& scratch,
                                             Context&... context) const {
    if constexpr (std::is_invocable_v<const Action&, CordIteratorSpan,
                                      Context&...>) {
      return SkipLengthDelimitedFromCord(
          repr, [&] { return action_(std::move(repr), context...); });
    } else if constexpr (std::is_invocable_v<const Action&, absl::string_view,
                                             Context&...>) {
      return action_(std::move(repr).ToStringView(scratch), context...);
    } else if constexpr (std::is_invocable_v<const Action&, std::string&&,
                                             Context&...>) {
      std::move(repr).ToString(scratch);
      return action_(std::move(scratch), context...);
    } else if constexpr (std::is_invocable_v<const Action&, Chain,
                                             Context&...>) {
      return action_(Chain(std::move(repr).ToCord()), context...);
    } else if constexpr (std::is_invocable_v<const Action&, absl::Cord,
                                             Context&...>) {
      return action_(std::move(repr).ToCord(), context...);
    } else {
      static_assert(false, "No string-like type accepted");
    }
  }

  template <
      typename... Context,
      std::enable_if_t<
          std::disjunction_v<
              std::is_invocable<const Action&, absl::string_view, Context&...>,
              std::is_invocable<const Action&, std::string&&, Context&...>,
              std::is_invocable<const Action&, Chain&&, Context&...>,
              std::is_invocable<const Action&, absl::Cord&&, Context&...>>,
          int> = 0>
  absl::Status HandleLengthDelimitedFromString(absl::string_view repr,
                                               Context&... context) const {
    if constexpr (std::is_invocable_v<const Action&, absl::string_view,
                                      Context&...>) {
      return action_(repr, context...);
    } else if constexpr (std::is_invocable_v<const Action&, std::string&&,
                                             Context&...>) {
      return action_(std::string(repr), context...);
    } else if constexpr (std::is_invocable_v<const Action&, Chain&&,
                                             Context&...>) {
      return action_(Chain(repr), context...);
    } else if constexpr (std::is_invocable_v<const Action&, absl::Cord&&,
                                             Context&...>) {
      return action_(absl::Cord(repr), context...);
    } else {
      static_assert(false, "No string-like type accepted");
    }
  }

 private:
  template <typename StringType, typename... Context>
  absl::Status HandleString(ReaderSpan<> repr, Context&... context) const {
    StringType value;
    if (ABSL_PREDICT_FALSE(
            !repr.reader().Read(IntCast<size_t>(repr.length()), value))) {
      return serialized_message_reader_internal::ReadLengthDelimitedValueError(
          repr.reader());
    }
    absl::Status status = action_(std::move(value), context...);
    // Comparison against `absl::CancelledError()` is a fast path of
    // `absl::IsCancelled()`.
    if (ABSL_PREDICT_FALSE(!status.ok() && status != absl::CancelledError())) {
      status = field_handlers_internal::AnnotateByReader(std::move(status),
                                                         repr.reader());
    }
    return status;
  }

  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Action action_;
};

template <int field_number, typename Action>
class BeforeGroupType {
 public:
  static constexpr int kFieldNumber = field_number;

  template <typename ActionInitializer,
            std::enable_if_t<std::is_convertible_v<ActionInitializer&&, Action>,
                             int> = 0>
  explicit constexpr BeforeGroupType(ActionInitializer&& action)
      : action_(std::forward<ActionInitializer>(action)) {}

  template <typename... Context,
            std::enable_if_t<std::is_invocable_v<const Action&, Context&...>,
                             int> = 0>
  absl::Status HandleStartGroup(Context&... context) const {
    return action_(context...);
  }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Action action_;
};

template <int field_number, typename Action>
class AfterGroupType {
 public:
  static constexpr int kFieldNumber = field_number;

  template <typename ActionInitializer,
            std::enable_if_t<std::is_convertible_v<ActionInitializer&&, Action>,
                             int> = 0>
  explicit constexpr AfterGroupType(ActionInitializer&& action)
      : action_(std::forward<ActionInitializer>(action)) {}

  template <typename... Context,
            std::enable_if_t<std::is_invocable_v<const Action&, Context&...>,
                             int> = 0>
  absl::Status HandleEndGroup(Context&... context) const {
    return action_(context...);
  }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Action action_;
};

template <typename Value, VarintKind kind, int field_number, typename Action>
template <typename... Context,
          std::enable_if_t<
              std::is_invocable_v<const Action&, Value, Context&...>, int>>
absl::Status OnRepeatedVarintType<Value, kind, field_number, Action>::
    HandleLengthDelimitedFromReader(ReaderSpan<> repr,
                                    Context&... context) const {
  ScopedLimiter scoped_limiter(repr);
  uint64_t element;
  while (ReadVarint64(repr.reader(), element)) {
    if (ABSL_PREDICT_FALSE(
            (!field_handlers_internal::VarintIsValid<Value, kind>(element)))) {
      return field_handlers_internal::VarintOverflowError<Value, kind>(
          repr.reader(), element);
    }
    if (absl::Status status = this->action()(
            field_handlers_internal::DecodeVarint<Value, kind>(element),
            context...);
        ABSL_PREDICT_FALSE(!status.ok())) {
      // Comparison against `absl::CancelledError()` is a fast path of
      // `absl::IsCancelled()`.
      if (ABSL_PREDICT_FALSE(status != absl::CancelledError())) {
        status = field_handlers_internal::AnnotateByReader(std::move(status),
                                                           repr.reader());
      }
      return status;
    }
  }
  if (ABSL_PREDICT_FALSE(repr.reader().pos() < repr.reader().max_pos())) {
    return field_handlers_internal::ReadPackedVarintError(repr.reader());
  }
  return absl::OkStatus();
}

template <typename Value, VarintKind kind, int field_number, typename Action>
template <typename... Context,
          std::enable_if_t<
              std::is_invocable_v<const Action&, Value, Context&...>, int>>
absl::Status OnRepeatedVarintType<Value, kind, field_number, Action>::
    HandleLengthDelimitedFromCord(CordIteratorSpan repr,
                                  ABSL_ATTRIBUTE_UNUSED std::string& scratch,
                                  Context&... context) const {
  const size_t limit =
      CordIteratorSpan::Remaining(repr.iterator()) - repr.length();
  uint64_t element;
  while (ReadVarint64(repr.iterator(),
                      CordIteratorSpan::Remaining(repr.iterator()) - limit,
                      element)) {
    if (ABSL_PREDICT_FALSE(
            (!field_handlers_internal::VarintIsValid<Value, kind>(element)))) {
      return field_handlers_internal::VarintOverflowError<Value, kind>(element);
    }
    if (absl::Status status = this->action()(
            field_handlers_internal::DecodeVarint<Value, kind>(element),
            context...);
        ABSL_PREDICT_FALSE(!status.ok())) {
      return status;
    }
  }
  if (ABSL_PREDICT_FALSE(CordIteratorSpan::Remaining(repr.iterator()) >
                         limit)) {
    return field_handlers_internal::ReadPackedVarintError();
  }
  return absl::OkStatus();
}

template <typename Value, VarintKind kind, int field_number, typename Action>
template <typename... Context,
          std::enable_if_t<
              std::is_invocable_v<const Action&, Value, Context&...>, int>>
absl::Status OnRepeatedVarintType<Value, kind, field_number, Action>::
    HandleLengthDelimitedFromString(absl::string_view repr,
                                    Context&... context) const {
  const char* cursor = repr.data();
  const char* const limit = repr.data() + repr.size();
  uint64_t element;
  while (const size_t length =
             ReadVarint64(cursor, PtrDistance(cursor, limit), element)) {
    cursor += length;
    if (ABSL_PREDICT_FALSE(
            (!field_handlers_internal::VarintIsValid<Value, kind>(element)))) {
      return field_handlers_internal::VarintOverflowError<Value, kind>(element);
    }
    if (absl::Status status = this->action()(
            field_handlers_internal::DecodeVarint<Value, kind>(element),
            context...);
        ABSL_PREDICT_FALSE(!status.ok())) {
      return status;
    }
  }
  if (ABSL_PREDICT_FALSE(cursor < limit)) {
    return field_handlers_internal::ReadPackedVarintError();
  }
  return absl::OkStatus();
}

template <typename Value, int field_number, typename Action>
template <typename... Context,
          std::enable_if_t<
              std::is_invocable_v<const Action&, Value, Context&...>, int>>
absl::Status OnRepeatedFixedType<Value, field_number, Action>::
    HandleLengthDelimitedFromReader(ReaderSpan<> repr,
                                    Context&... context) const {
  if (ABSL_PREDICT_FALSE(repr.length() % sizeof(Value) > 0)) {
    return field_handlers_internal::ReadPackedFixedError<sizeof(Value)>(
        repr.reader());
  }
  Position length = repr.length();
  while (length > 0) {
    Value element;
    if (ABSL_PREDICT_FALSE(!ReadLittleEndian<Value>(repr.reader(), element))) {
      return field_handlers_internal::ReadPackedFixedError<sizeof(Value)>(
          repr.reader());
    }
    if (absl::Status status = this->action()(element, context...);
        ABSL_PREDICT_FALSE(!status.ok())) {
      // Comparison against `absl::CancelledError()` is a fast path of
      // `absl::IsCancelled()`.
      if (ABSL_PREDICT_FALSE(status != absl::CancelledError())) {
        status = field_handlers_internal::AnnotateByReader(std::move(status),
                                                           repr.reader());
      }
      return status;
    }
    length -= sizeof(Value);
  }
  return absl::OkStatus();
}

template <typename Value, int field_number, typename Action>
template <typename... Context,
          std::enable_if_t<
              std::is_invocable_v<const Action&, Value, Context&...>, int>>
absl::Status
OnRepeatedFixedType<Value, field_number, Action>::HandleLengthDelimitedFromCord(
    CordIteratorSpan repr, ABSL_ATTRIBUTE_UNUSED std::string& scratch,
    Context&... context) const {
  if (ABSL_PREDICT_FALSE(repr.length() % sizeof(Value) > 0)) {
    return field_handlers_internal::ReadPackedFixedError<sizeof(Value)>();
  }
  Position length = repr.length();
  while (length > 0) {
    char buffer[sizeof(Value)];
    CordIteratorSpan::Read(repr.iterator(), sizeof(Value), buffer);
    const Value element = ReadLittleEndian<Value>(buffer);
    if (absl::Status status = this->action()(element, context...);
        ABSL_PREDICT_FALSE(!status.ok())) {
      return status;
    }
    length -= sizeof(Value);
  }
  return absl::OkStatus();
}

template <typename Value, int field_number, typename Action>
template <typename... Context,
          std::enable_if_t<
              std::is_invocable_v<const Action&, Value, Context&...>, int>>
absl::Status OnRepeatedFixedType<Value, field_number, Action>::
    HandleLengthDelimitedFromString(absl::string_view repr,
                                    Context&... context) const {
  if (ABSL_PREDICT_FALSE(repr.size() % sizeof(Value) > 0)) {
    return field_handlers_internal::ReadPackedFixedError<sizeof(Value)>();
  }
  const char* cursor = repr.data();
  const char* const limit = repr.data() + repr.size();
  while (cursor < limit) {
    const Value element = ReadLittleEndian<Value>(cursor);
    if (absl::Status status = this->action()(element, context...);
        ABSL_PREDICT_FALSE(!status.ok())) {
      return status;
    }
    cursor += sizeof(Value);
  }
  return absl::OkStatus();
}

}  // namespace field_handlers

}  // namespace riegeli

#endif  // RIEGELI_MESSAGES_FIELD_HANDLERS_H_
