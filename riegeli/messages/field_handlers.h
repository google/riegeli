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
#include "riegeli/base/chain.h"
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

namespace field_handlers_internal {

// Decoders from a wire type representation to field types.
struct Int32Traits;
struct Int64Traits;
struct UInt32Traits;
struct UInt64Traits;
struct SInt32Traits;
struct SInt64Traits;
struct BoolTraits;
struct Fixed32Traits;
struct Fixed64Traits;
struct SFixed32Traits;
struct SFixed64Traits;
struct FloatTraits;
struct DoubleTraits;
template <typename EnumType>
struct EnumTraits;

// Class types of common field handlers. Clients should use the corresponding
// type aliases defined below.
template <typename Traits, int field_number, typename Action>
class OnOptionalVarintImpl;
template <typename Traits, int field_number, typename Action>
class OnRepeatedVarintImpl;
template <typename Traits, int field_number, typename Action>
class OnOptionalFixed32Impl;
template <typename Traits, int field_number, typename Action>
class OnRepeatedFixed32Impl;
template <typename Traits, int field_number, typename Action>
class OnOptionalFixed64Impl;
template <typename Traits, int field_number, typename Action>
class OnRepeatedFixed64Impl;
template <int field_number, typename Action>
class OnLengthDelimitedImpl;
template <int field_number, typename Action>
class OnStartGroupImpl;
template <int field_number, typename Action>
class OnEndGroupImpl;

}  // namespace field_handlers_internal

namespace field_handlers {

// Types of common field handlers, to use if the caller prefers to spell them.
// Usually it is enough to use `auto` instead of spelling these types.

template <int field_number, typename Action>
using OnOptionalInt32Type = field_handlers_internal::OnOptionalVarintImpl<
    field_handlers_internal::Int32Traits, field_number, Action>;

template <int field_number, typename Action>
using OnRepeatedInt32Type = field_handlers_internal::OnRepeatedVarintImpl<
    field_handlers_internal::Int32Traits, field_number, Action>;

template <int field_number, typename Action>
using OnOptionalInt64Type = field_handlers_internal::OnOptionalVarintImpl<
    field_handlers_internal::Int64Traits, field_number, Action>;

template <int field_number, typename Action>
using OnRepeatedInt64Type = field_handlers_internal::OnRepeatedVarintImpl<
    field_handlers_internal::Int64Traits, field_number, Action>;

template <int field_number, typename Action>
using OnOptionalUInt32Type = field_handlers_internal::OnOptionalVarintImpl<
    field_handlers_internal::UInt32Traits, field_number, Action>;

template <int field_number, typename Action>
using OnRepeatedUInt32Type = field_handlers_internal::OnRepeatedVarintImpl<
    field_handlers_internal::UInt32Traits, field_number, Action>;

template <int field_number, typename Action>
using OnOptionalUInt64Type = field_handlers_internal::OnOptionalVarintImpl<
    field_handlers_internal::UInt64Traits, field_number, Action>;

template <int field_number, typename Action>
using OnRepeatedUInt64Type = field_handlers_internal::OnRepeatedVarintImpl<
    field_handlers_internal::UInt64Traits, field_number, Action>;

template <int field_number, typename Action>
using OnOptionalSInt32Type = field_handlers_internal::OnOptionalVarintImpl<
    field_handlers_internal::SInt32Traits, field_number, Action>;

template <int field_number, typename Action>
using OnRepeatedSInt32Type = field_handlers_internal::OnRepeatedVarintImpl<
    field_handlers_internal::SInt32Traits, field_number, Action>;

template <int field_number, typename Action>
using OnOptionalSInt64Type = field_handlers_internal::OnOptionalVarintImpl<
    field_handlers_internal::SInt64Traits, field_number, Action>;

template <int field_number, typename Action>
using OnRepeatedSInt64Type = field_handlers_internal::OnRepeatedVarintImpl<
    field_handlers_internal::SInt64Traits, field_number, Action>;

template <int field_number, typename Action>
using OnOptionalBoolType = field_handlers_internal::OnOptionalVarintImpl<
    field_handlers_internal::BoolTraits, field_number, Action>;

template <int field_number, typename Action>
using OnRepeatedBoolType = field_handlers_internal::OnRepeatedVarintImpl<
    field_handlers_internal::BoolTraits, field_number, Action>;

template <int field_number, typename Action>
using OnOptionalFixed32Type = field_handlers_internal::OnOptionalFixed32Impl<
    field_handlers_internal::Fixed32Traits, field_number, Action>;

template <int field_number, typename Action>
using OnRepeatedFixed32Type = field_handlers_internal::OnRepeatedFixed32Impl<
    field_handlers_internal::Fixed32Traits, field_number, Action>;

template <int field_number, typename Action>
using OnOptionalFixed64Type = field_handlers_internal::OnOptionalFixed64Impl<
    field_handlers_internal::Fixed64Traits, field_number, Action>;

template <int field_number, typename Action>
using OnRepeatedFixed64Type = field_handlers_internal::OnRepeatedFixed64Impl<
    field_handlers_internal::Fixed64Traits, field_number, Action>;

template <int field_number, typename Action>
using OnOptionalSFixed32Type = field_handlers_internal::OnOptionalFixed32Impl<
    field_handlers_internal::SFixed32Traits, field_number, Action>;

template <int field_number, typename Action>
using OnRepeatedSFixed32Type = field_handlers_internal::OnRepeatedFixed32Impl<
    field_handlers_internal::SFixed32Traits, field_number, Action>;

template <int field_number, typename Action>
using OnOptionalSFixed64Type = field_handlers_internal::OnOptionalFixed64Impl<
    field_handlers_internal::SFixed64Traits, field_number, Action>;

template <int field_number, typename Action>
using OnRepeatedSFixed64Type = field_handlers_internal::OnRepeatedFixed64Impl<
    field_handlers_internal::SFixed64Traits, field_number, Action>;

template <int field_number, typename Action>
using OnOptionalFloatType = field_handlers_internal::OnOptionalFixed32Impl<
    field_handlers_internal::FloatTraits, field_number, Action>;

template <int field_number, typename Action>
using OnRepeatedFloatType = field_handlers_internal::OnRepeatedFixed32Impl<
    field_handlers_internal::FloatTraits, field_number, Action>;

template <int field_number, typename Action>
using OnOptionalDoubleType = field_handlers_internal::OnOptionalFixed64Impl<
    field_handlers_internal::DoubleTraits, field_number, Action>;

template <int field_number, typename Action>
using OnRepeatedDoubleType = field_handlers_internal::OnRepeatedFixed64Impl<
    field_handlers_internal::DoubleTraits, field_number, Action>;

template <typename EnumType, int field_number, typename Action>
using OnOptionalEnumType = field_handlers_internal::OnOptionalVarintImpl<
    field_handlers_internal::EnumTraits<EnumType>, field_number, Action>;

template <typename EnumType, int field_number, typename Action>
using OnRepeatedEnumType = field_handlers_internal::OnRepeatedVarintImpl<
    field_handlers_internal::EnumTraits<EnumType>, field_number, Action>;

template <int field_number, typename Action>
using OnLengthDelimitedType =
    field_handlers_internal::OnLengthDelimitedImpl<field_number, Action>;

template <int field_number, typename Action>
using BeforeGroupType =
    field_handlers_internal::OnStartGroupImpl<field_number, Action>;

template <int field_number, typename Action>
using AfterGroupType =
    field_handlers_internal::OnEndGroupImpl<field_number, Action>;

// Common field handlers for `SerializedMessageReader2` and `FieldHandlerMap`.
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
constexpr OnOptionalInt32Type<field_number, std::decay_t<Action>>
OnOptionalInt32(Action&& action) {
  return OnOptionalInt32Type<field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of an element of a repeated `int32` field. The value is
// provided as `int32_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnRepeatedInt32Type<field_number, std::decay_t<Action>>
OnRepeatedInt32(Action&& action) {
  return OnRepeatedInt32Type<field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of a singular `int64` field. The value is provided as
// `int64_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnOptionalInt64Type<field_number, std::decay_t<Action>>
OnOptionalInt64(Action&& action) {
  return OnOptionalInt64Type<field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of an element of a repeated `int64` field. The value is
// provided as `int64_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnRepeatedInt64Type<field_number, std::decay_t<Action>>
OnRepeatedInt64(Action&& action) {
  return OnRepeatedInt64Type<field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of a singular `uint32` field. The value is provided as
// `uint32_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnOptionalUInt32Type<field_number, std::decay_t<Action>>
OnOptionalUInt32(Action&& action) {
  return OnOptionalUInt32Type<field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of an element of a repeated `uint32` field. The value is
// provided as `uint32_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnRepeatedUInt32Type<field_number, std::decay_t<Action>>
OnRepeatedUInt32(Action&& action) {
  return OnRepeatedUInt32Type<field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of a singular `uint64` field. The value is provided as
// `uint64_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnOptionalUInt64Type<field_number, std::decay_t<Action>>
OnOptionalUInt64(Action&& action) {
  return OnOptionalUInt64Type<field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of an element of a repeated `uint64` field. The value is
// provided as `uint64_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnRepeatedUInt64Type<field_number, std::decay_t<Action>>
OnRepeatedUInt64(Action&& action) {
  return OnRepeatedUInt64Type<field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of a singular `sint32` field. The value is provided as
// `int32_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnOptionalSInt32Type<field_number, std::decay_t<Action>>
OnOptionalSInt32(Action&& action) {
  return OnOptionalSInt32Type<field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of an element of a repeated `sint32` field. The value is
// provided as `int32_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnRepeatedSInt32Type<field_number, std::decay_t<Action>>
OnRepeatedSInt32(Action&& action) {
  return OnRepeatedSInt32Type<field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of a singular `sint64` field. The value is provided as
// `int64_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnOptionalSInt64Type<field_number, std::decay_t<Action>>
OnOptionalSInt64(Action&& action) {
  return OnOptionalSInt64Type<field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of an element of a repeated `sint64` field. The value is
// provided as `int64_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnRepeatedSInt64Type<field_number, std::decay_t<Action>>
OnRepeatedSInt64(Action&& action) {
  return OnRepeatedSInt64Type<field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of a singular `bool` field. The value is provided as `bool`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnOptionalBoolType<field_number, std::decay_t<Action>> OnOptionalBool(
    Action&& action) {
  return OnOptionalBoolType<field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of an element of a repeated `bool` field. The value is provided
// as `bool`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnRepeatedBoolType<field_number, std::decay_t<Action>> OnRepeatedBool(
    Action&& action) {
  return OnRepeatedBoolType<field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of a singular `fixed32` field. The value is provided as
// `uint32_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnOptionalFixed32Type<field_number, std::decay_t<Action>>
OnOptionalFixed32(Action&& action) {
  return OnOptionalFixed32Type<field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of an element of a repeated `fixed32` field. The value is
// provided as `uint32_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnRepeatedFixed32Type<field_number, std::decay_t<Action>>
OnRepeatedFixed32(Action&& action) {
  return OnRepeatedFixed32Type<field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of a singular `fixed64` field. The value is provided as
// `uint64_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnOptionalFixed64Type<field_number, std::decay_t<Action>>
OnOptionalFixed64(Action&& action) {
  return OnOptionalFixed64Type<field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of an element of a repeated `fixed64` field. The value is
// provided as `uint64_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnRepeatedFixed64Type<field_number, std::decay_t<Action>>
OnRepeatedFixed64(Action&& action) {
  return OnRepeatedFixed64Type<field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of a singular `sfixed32` field. The value is provided as
// `int32_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnOptionalSFixed32Type<field_number, std::decay_t<Action>>
OnOptionalSFixed32(Action&& action) {
  return OnOptionalSFixed32Type<field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of an element of a repeated `sfixed32` field. The value is
// provided as `int32_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnRepeatedSFixed32Type<field_number, std::decay_t<Action>>
OnRepeatedSFixed32(Action&& action) {
  return OnRepeatedSFixed32Type<field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of a singular `sfixed64` field. The value is provided as
// `int64_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnOptionalSFixed64Type<field_number, std::decay_t<Action>>
OnOptionalSFixed64(Action&& action) {
  return OnOptionalSFixed64Type<field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of an element of a repeated `sfixed64` field. The value is
// provided as `int64_t`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnRepeatedSFixed64Type<field_number, std::decay_t<Action>>
OnRepeatedSFixed64(Action&& action) {
  return OnRepeatedSFixed64Type<field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of a singular `float` field. The value is provided as `float`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnOptionalFloatType<field_number, std::decay_t<Action>>
OnOptionalFloat(Action&& action) {
  return OnOptionalFloatType<field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of an element of a repeated `float` field. The value is
// provided as `float`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnRepeatedFloatType<field_number, std::decay_t<Action>>
OnRepeatedFloat(Action&& action) {
  return OnRepeatedFloatType<field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of a singular `double` field. The value is provided as
// `double`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnOptionalDoubleType<field_number, std::decay_t<Action>>
OnOptionalDouble(Action&& action) {
  return OnOptionalDoubleType<field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of an element of a repeated `double` field. The value is
// provided as `double`.
template <int field_number = kUnboundFieldNumber, typename Action>
constexpr OnRepeatedDoubleType<field_number, std::decay_t<Action>>
OnRepeatedDouble(Action&& action) {
  return OnRepeatedDoubleType<field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of a singular enum field. The value is provided as an enum type
// (C++ or proto enum) or an integral type.
template <
    typename EnumType, int field_number = kUnboundFieldNumber, typename Action,
    std::enable_if_t<
        std::disjunction_v<std::is_enum<EnumType>, std::is_integral<EnumType>>,
        int> = 0>
constexpr OnOptionalEnumType<EnumType, field_number, std::decay_t<Action>>
OnOptionalEnum(Action&& action) {
  return OnOptionalEnumType<EnumType, field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of an element of a repeated enum field. The value is provided
// as an enum type (C++ or proto enum) or an integral type.
template <
    typename EnumType, int field_number = kUnboundFieldNumber, typename Action,
    std::enable_if_t<
        std::disjunction_v<std::is_enum<EnumType>, std::is_integral<EnumType>>,
        int> = 0>
constexpr OnRepeatedEnumType<EnumType, field_number, std::decay_t<Action>>
OnRepeatedEnum(Action&& action) {
  return OnRepeatedEnumType<EnumType, field_number, std::decay_t<Action>>(
      std::forward<Action>(action));
}

// Field handler of a singular or an element of a repeated `string`, `bytes`,
// or submessage field.
//
// For a `Reader` source, the value is provided as `ReaderSpan<>`,
// `absl::string_view`, `std::string&&`, `Chain&&`, or `absl::Cord&&`,
// depending on what the action accepts.
//
// For a string source, the value is provided as one of the string-like types
// above, except for `ReaderSpan<>`. If the action accepts `absl::string_view`,
// the value is guaranteed to be a substring of the original string. This
// guarantee is absent for a `Reader` source.
//
// If the action accepts `ReaderSpan<>`, it must read to the end of it or fail.
// `SkipLengthDelimited()` can be used to seek to the end of the field.
//
// If the action accepts either `ReaderSpan<>` or one of the string-like types
// above, the value is provided as `ReaderSpan<>` for a `Reader` source, and as
// the string-like type for a string source.
//
// Among string-like types, `absl::string_view` is checked before `std::string`.
// This means that if the action accepts either `ReaderSpan<>` or
// `absl::string_view`, the action parameter can be declared as `auto`.
// This is convenient if the action body can treat `ReaderSpan<>`
// and `absl::string_view` uniformly, e.g. when it is passed to
// `riegeli::ParseMessage()` or `SerializedMessageReader2::ReadMessage()`.
//
// Alternatively, the action can use `absl::Overload{}` to provide two variants
// with separate implementations.
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

template <typename T, typename Enable = void>
struct TraitsHaveIsValid : std::false_type {};

template <typename T>
struct TraitsHaveIsValid<
    T, std::enable_if_t<std::is_convertible_v<
           decltype(std::declval<const T&>().IsValid()), bool>>>
    : std::true_type {};

ABSL_ATTRIBUTE_COLD absl::Status AnnotateByReader(absl::Status status,
                                                  Reader& reader);
ABSL_ATTRIBUTE_COLD absl::Status ReadPackedVarintError(Reader& src);
ABSL_ATTRIBUTE_COLD absl::Status ReadPackedVarintError();
ABSL_ATTRIBUTE_COLD absl::Status ReadPackedFixed32Error(Reader& src);
ABSL_ATTRIBUTE_COLD absl::Status ReadPackedFixed32Error();
ABSL_ATTRIBUTE_COLD absl::Status ReadPackedFixed64Error(Reader& src);
ABSL_ATTRIBUTE_COLD absl::Status ReadPackedFixed64Error();
ABSL_ATTRIBUTE_COLD absl::Status InvalidEnumError(uint64_t repr);

struct Int32Traits {
  static bool IsValid(uint64_t repr) {
    return static_cast<uint64_t>(static_cast<int32_t>(repr)) == repr;
  }
  ABSL_ATTRIBUTE_COLD static absl::Status InvalidError(Reader& src,
                                                       uint64_t repr);
  ABSL_ATTRIBUTE_COLD static absl::Status InvalidError(uint64_t repr);
  static int32_t Decode(uint64_t repr) { return static_cast<int32_t>(repr); }
};

struct Int64Traits {
  static int64_t Decode(uint64_t repr) { return static_cast<int64_t>(repr); }
};

struct UInt32Traits {
  static bool IsValid(uint64_t repr) {
    return static_cast<uint64_t>(static_cast<uint32_t>(repr)) == repr;
  }
  ABSL_ATTRIBUTE_COLD static absl::Status InvalidError(Reader& src,
                                                       uint64_t repr);
  ABSL_ATTRIBUTE_COLD static absl::Status InvalidError(uint64_t repr);
  static uint32_t Decode(uint64_t repr) { return static_cast<uint32_t>(repr); }
};

struct UInt64Traits {
  static uint64_t Decode(uint64_t repr) { return repr; }
};

struct SInt32Traits {
  static bool IsValid(uint64_t repr) {
    return static_cast<uint64_t>(static_cast<uint32_t>(repr)) == repr;
  }
  ABSL_ATTRIBUTE_COLD static absl::Status InvalidError(Reader& src,
                                                       uint64_t repr);
  ABSL_ATTRIBUTE_COLD static absl::Status InvalidError(uint64_t repr);
  static int32_t Decode(uint64_t repr) {
    return DecodeVarintSigned32(static_cast<uint32_t>(repr));
  }
};

struct SInt64Traits {
  static int64_t Decode(uint64_t repr) { return DecodeVarintSigned64(repr); }
};

struct Fixed32Traits {
  static uint32_t Decode(uint32_t repr) { return repr; }
};

struct Fixed64Traits {
  static uint64_t Decode(uint64_t repr) { return repr; }
};

struct SFixed32Traits {
  static int32_t Decode(uint64_t repr) { return static_cast<int32_t>(repr); }
};

struct SFixed64Traits {
  static int64_t Decode(uint64_t repr) { return static_cast<int64_t>(repr); }
};

struct BoolTraits {
  static bool IsValid(uint64_t repr) { return repr <= 1; }
  ABSL_ATTRIBUTE_COLD static absl::Status InvalidError(Reader& src,
                                                       uint64_t repr);
  ABSL_ATTRIBUTE_COLD static absl::Status InvalidError(uint64_t repr);
  static bool Decode(uint64_t repr) { return repr != 0; }
};

struct FloatTraits {
  static float Decode(uint32_t repr) { return absl::bit_cast<float>(repr); }
};

struct DoubleTraits {
  static double Decode(uint64_t repr) { return absl::bit_cast<double>(repr); }
};

template <typename EnumType>
struct EnumTraits {
  static bool IsValid(uint64_t repr) {
    return static_cast<uint64_t>(Decode(repr)) == repr;
  }
  static absl::Status InvalidError(Reader& src, uint64_t repr) {
    return src.StatusOrAnnotate(InvalidError(repr));
  }
  static absl::Status InvalidError(uint64_t repr) {
    return InvalidEnumError(repr);
  }
  static EnumType Decode(uint64_t repr) {
    if constexpr (std::is_enum_v<EnumType>) {
      // Casting an out of range value to an enum has undefined behavior.
      // Casting such a value to an integral type wraps around.
      return static_cast<EnumType>(
          static_cast<std::underlying_type_t<EnumType>>(repr));
    } else {
      return static_cast<EnumType>(repr);
    }
  }
};

template <typename Traits, int field_number, typename Action>
class OnOptionalVarintImpl {
 public:
  static constexpr int kFieldNumber = field_number;

  template <typename ActionInitializer,
            std::enable_if_t<
                std::is_constructible_v<Action, ActionInitializer&&>, int> = 0>
  explicit constexpr OnOptionalVarintImpl(ActionInitializer&& action)
      : action_(std::forward<ActionInitializer>(action)) {}

  template <
      typename... Context,
      std::enable_if_t<
          std::is_invocable_r_v<
              absl::Status, const Action&,
              decltype(Traits::Decode(std::declval<uint64_t>())), Context&...>,
          int> = 0>
  absl::Status HandleVarint(uint64_t repr, Context&... context) const {
    if constexpr (TraitsHaveIsValid<Traits>::value) {
      if (ABSL_PREDICT_FALSE(!Traits::IsValid(repr))) {
        return Traits::InvalidError(repr);
      }
    }
    return action_(Traits::Decode(repr), context...);
  }

 protected:
  const Action& action() const { return action_; }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Action action_;
};

template <typename Traits, int field_number, typename Action>
class OnRepeatedVarintImpl
    : public OnOptionalVarintImpl<Traits, field_number, Action> {
 public:
  using OnRepeatedVarintImpl::OnOptionalVarintImpl::OnOptionalVarintImpl;

  template <
      typename... Context,
      std::enable_if_t<
          std::is_invocable_r_v<
              absl::Status, const Action&,
              decltype(Traits::Decode(std::declval<uint64_t>())), Context&...>,
          int> = 0>
  absl::Status HandleLengthDelimitedFromReader(ReaderSpan<> value,
                                               Context&... context) const {
    ScopedLimiter scoped_limiter(value);
    uint64_t repr;
    while (ReadVarint64(value.reader(), repr)) {
      if constexpr (TraitsHaveIsValid<Traits>::value) {
        if (ABSL_PREDICT_FALSE(!Traits::IsValid(repr))) {
          return Traits::InvalidError(value, repr);
        }
      }
      if (absl::Status status =
              this->action()(Traits::Decode(repr), context...);
          ABSL_PREDICT_FALSE(!status.ok())) {
        // Comparison against `absl::CancelledError()` is a fast path of
        // `absl::IsCancelled()`.
        if (ABSL_PREDICT_FALSE(status != absl::CancelledError())) {
          status = AnnotateByReader(std::move(status), value.reader());
        }
        return status;
      }
    }
    if (ABSL_PREDICT_FALSE(value.reader().pos() < value.reader().max_pos())) {
      return ReadPackedVarintError(value.reader());
    }
    return absl::OkStatus();
  }

  template <
      typename... Context,
      std::enable_if_t<
          std::is_invocable_r_v<
              absl::Status, const Action&,
              decltype(Traits::Decode(std::declval<uint64_t>())), Context&...>,
          int> = 0>
  absl::Status HandleLengthDelimitedFromString(absl::string_view value,
                                               Context&... context) const {
    const char* cursor = value.data();
    const char* const limit = value.data() + value.size();
    uint64_t repr;
    while (const size_t length =
               ReadVarint64(cursor, PtrDistance(cursor, limit), repr)) {
      cursor += length;
      if constexpr (TraitsHaveIsValid<Traits>::value) {
        if (ABSL_PREDICT_FALSE(!Traits::IsValid(repr))) {
          return Traits::InvalidError(repr);
        }
      }
      if (absl::Status status =
              this->action()(Traits::Decode(repr), context...);
          ABSL_PREDICT_FALSE(!status.ok())) {
        return status;
      }
    }
    if (ABSL_PREDICT_FALSE(cursor < limit)) return ReadPackedVarintError();
    return absl::OkStatus();
  }
};

template <typename Traits, int field_number, typename Action>
class OnOptionalFixed32Impl {
 public:
  static constexpr int kFieldNumber = field_number;

  template <typename ActionInitializer,
            std::enable_if_t<
                std::is_constructible_v<Action, ActionInitializer&&>, int> = 0>
  explicit constexpr OnOptionalFixed32Impl(ActionInitializer&& action)
      : action_(std::forward<ActionInitializer>(action)) {}

  template <
      typename... Context,
      std::enable_if_t<
          std::is_invocable_r_v<
              absl::Status, const Action&,
              decltype(Traits::Decode(std::declval<uint32_t>())), Context&...>,
          int> = 0>
  absl::Status HandleFixed32(uint32_t repr, Context&... context) const {
    return action_(Traits::Decode(repr), context...);
  }

 protected:
  const Action& action() const { return action_; }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Action action_;
};

template <typename Traits, int field_number, typename Action>
class OnRepeatedFixed32Impl
    : public OnOptionalFixed32Impl<Traits, field_number, Action> {
 public:
  static constexpr int kFieldNumber = field_number;

  using OnRepeatedFixed32Impl::OnOptionalFixed32Impl::OnOptionalFixed32Impl;

  template <
      typename... Context,
      std::enable_if_t<
          std::is_invocable_r_v<
              absl::Status, const Action&,
              decltype(Traits::Decode(std::declval<uint32_t>())), Context&...>,
          int> = 0>
  absl::Status HandleLengthDelimitedFromReader(ReaderSpan<> value,
                                               Context&... context) const {
    if (ABSL_PREDICT_FALSE(value.length() % sizeof(uint32_t) > 0)) {
      return ReadPackedFixed32Error(value.reader());
    }
    Position length = value.length();
    while (length > 0) {
      uint32_t repr;
      if (ABSL_PREDICT_FALSE(!ReadLittleEndian32(value.reader(), repr))) {
        return ReadPackedFixed32Error(value.reader());
      }
      length -= sizeof(uint32_t);
      if (absl::Status status =
              this->action()(Traits::Decode(repr), context...);
          ABSL_PREDICT_FALSE(!status.ok())) {
        // Comparison against `absl::CancelledError()` is a fast path of
        // `absl::IsCancelled()`.
        if (ABSL_PREDICT_FALSE(status != absl::CancelledError())) {
          status = AnnotateByReader(std::move(status), value.reader());
        }
        return status;
      }
    }
    return absl::OkStatus();
  }

  template <
      typename... Context,
      std::enable_if_t<
          std::is_invocable_r_v<
              absl::Status, const Action&,
              decltype(Traits::Decode(std::declval<uint32_t>())), Context&...>,
          int> = 0>
  absl::Status HandleLengthDelimitedFromString(absl::string_view value,
                                               Context&... context) const {
    if (ABSL_PREDICT_FALSE(value.size() % sizeof(uint32_t) > 0)) {
      return ReadPackedFixed32Error();
    }
    const char* cursor = value.data();
    const char* const limit = value.data() + value.size();
    while (cursor < limit) {
      const uint32_t repr = ReadLittleEndian32(cursor);
      cursor += sizeof(uint32_t);
      if (absl::Status status =
              this->action()(Traits::Decode(repr), context...);
          ABSL_PREDICT_FALSE(!status.ok())) {
        return status;
      }
    }
    return absl::OkStatus();
  }
};

template <typename Traits, int field_number, typename Action>
class OnOptionalFixed64Impl {
 public:
  static constexpr int kFieldNumber = field_number;

  template <typename ActionInitializer,
            std::enable_if_t<
                std::is_constructible_v<Action, ActionInitializer&&>, int> = 0>
  explicit constexpr OnOptionalFixed64Impl(ActionInitializer&& action)
      : action_(std::forward<ActionInitializer>(action)) {}

  template <
      typename... Context,
      std::enable_if_t<
          std::is_invocable_r_v<
              absl::Status, const Action&,
              decltype(Traits::Decode(std::declval<uint64_t>())), Context&...>,
          int> = 0>
  absl::Status HandleFixed64(uint64_t repr, Context&... context) const {
    return action_(Traits::Decode(repr), context...);
  }

 protected:
  const Action& action() const { return action_; }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Action action_;
};

template <typename Traits, int field_number, typename Action>
class OnRepeatedFixed64Impl
    : public OnOptionalFixed64Impl<Traits, field_number, Action> {
 public:
  using OnRepeatedFixed64Impl::OnOptionalFixed64Impl::OnOptionalFixed64Impl;

  template <
      typename... Context,
      std::enable_if_t<
          std::is_invocable_r_v<
              absl::Status, const Action&,
              decltype(Traits::Decode(std::declval<uint64_t>())), Context&...>,
          int> = 0>
  absl::Status HandleLengthDelimitedFromReader(ReaderSpan<> value,
                                               Context&... context) const {
    if (ABSL_PREDICT_FALSE(value.length() % sizeof(uint64_t) > 0)) {
      return ReadPackedFixed64Error(value.reader());
    }
    Position length = value.length();
    while (length > 0) {
      uint64_t repr;
      if (ABSL_PREDICT_FALSE(!ReadLittleEndian64(value.reader(), repr))) {
        return ReadPackedFixed64Error(value.reader());
      }
      length -= sizeof(uint64_t);
      if (absl::Status status =
              this->action()(Traits::Decode(repr), context...);
          ABSL_PREDICT_FALSE(!status.ok())) {
        // Comparison against `absl::CancelledError()` is a fast path of
        // `absl::IsCancelled()`.
        if (ABSL_PREDICT_FALSE(status != absl::CancelledError())) {
          status = AnnotateByReader(std::move(status), value.reader());
        }
        return status;
      }
    }
    return absl::OkStatus();
  }

  template <
      typename... Context,
      std::enable_if_t<
          std::is_invocable_r_v<
              absl::Status, const Action&,
              decltype(Traits::Decode(std::declval<uint64_t>())), Context&...>,
          int> = 0>
  absl::Status HandleLengthDelimitedFromString(absl::string_view value,
                                               Context&... context) const {
    if (ABSL_PREDICT_FALSE(value.size() % sizeof(uint64_t) > 0)) {
      return ReadPackedFixed64Error();
    }
    const char* cursor = value.data();
    const char* const limit = value.data() + value.size();
    while (cursor < limit) {
      const uint64_t repr = ReadLittleEndian64(cursor);
      cursor += sizeof(uint64_t);
      if (absl::Status status =
              this->action()(Traits::Decode(repr), context...);
          ABSL_PREDICT_FALSE(!status.ok())) {
        return status;
      }
    }
    return absl::OkStatus();
  }
};

template <int field_number, typename Action>
class OnLengthDelimitedImpl {
 public:
  static constexpr int kFieldNumber = field_number;

  template <typename ActionInitializer,
            std::enable_if_t<
                std::is_constructible_v<Action, ActionInitializer&&>, int> = 0>
  explicit constexpr OnLengthDelimitedImpl(ActionInitializer&& action)
      : action_(std::forward<ActionInitializer>(action)) {}

  template <
      typename... Context,
      std::enable_if_t<std::disjunction_v<
                           std::is_invocable_r<absl::Status, const Action&,
                                               ReaderSpan<>, Context&...>,
                           std::is_invocable_r<absl::Status, const Action&,
                                               absl::string_view, Context&...>,
                           std::is_invocable_r<absl::Status, const Action&,
                                               std::string&&, Context&...>,
                           std::is_invocable_r<absl::Status, const Action&,
                                               Chain&&, Context&...>,
                           std::is_invocable_r<absl::Status, const Action&,
                                               absl::Cord&&, Context&...>>,
                       int> = 0>
  absl::Status HandleLengthDelimitedFromReader(ReaderSpan<> value,
                                               Context&... context) const {
    if constexpr (std::is_invocable_v<const Action&, ReaderSpan<>,
                                      Context&...>) {
      return action_(value, context...);
    } else if constexpr (std::is_invocable_v<const Action&, absl::string_view,
                                             Context&...>) {
      return HandleString<absl::string_view>(value, context...);
    } else if constexpr (std::is_invocable_v<const Action&, std::string&&,
                                             Context&...>) {
      return HandleString<std::string>(value, context...);
    } else if constexpr (std::is_invocable_v<const Action&, Chain&&,
                                             Context&...>) {
      return HandleString<Chain>(value, context...);
    } else if constexpr (std::is_invocable_v<const Action&, absl::Cord&&,
                                             Context&...>) {
      return HandleString<absl::Cord>(value, context...);
    } else {
      static_assert(sizeof(Action) == 0, "No string-like type accepted");
    }
  }

  template <
      typename... Context,
      std::enable_if_t<std::disjunction_v<
                           std::is_invocable_r<absl::Status, const Action&,
                                               absl::string_view, Context&...>,
                           std::is_invocable_r<absl::Status, const Action&,
                                               std::string&&, Context&...>,
                           std::is_invocable_r<absl::Status, const Action&,
                                               Chain&&, Context&...>,
                           std::is_invocable_r<absl::Status, const Action&,
                                               absl::Cord&&, Context&...>>,
                       int> = 0>
  absl::Status HandleLengthDelimitedFromString(absl::string_view value,
                                               Context&... context) const {
    if constexpr (std::is_invocable_v<const Action&, absl::string_view,
                                      Context&...>) {
      return action_(value, context...);
    } else if constexpr (std::is_invocable_v<const Action&, std::string&&,
                                             Context&...>) {
      return action_(std::string(value), context...);
    } else if constexpr (std::is_invocable_v<const Action&, Chain&&,
                                             Context&...>) {
      return action_(Chain(value), context...);
    } else if constexpr (std::is_invocable_v<const Action&, absl::Cord&&,
                                             Context&...>) {
      return action_(absl::Cord(value), context...);
    } else {
      static_assert(sizeof(Action) == 0, "No string-like type accepted");
    }
  }

 private:
  template <typename StringType, typename... Context>
  absl::Status HandleString(ReaderSpan<> value, Context&... context) const {
    StringType value_string;
    if (ABSL_PREDICT_FALSE(!value.reader().Read(IntCast<size_t>(value.length()),
                                                value_string))) {
      return serialized_message_reader_internal::ReadLengthDelimitedValueError(
          value.reader());
    }
    absl::Status status = action_(std::move(value_string), context...);
    // Comparison against `absl::CancelledError()` is a fast path of
    // `absl::IsCancelled()`.
    if (ABSL_PREDICT_FALSE(!status.ok() && status != absl::CancelledError())) {
      status = AnnotateByReader(std::move(status), value.reader());
    }
    return status;
  }

  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Action action_;
};

template <int field_number, typename Action>
class OnStartGroupImpl {
 public:
  static constexpr int kFieldNumber = field_number;

  template <typename ActionInitializer,
            std::enable_if_t<
                std::is_constructible_v<Action, ActionInitializer&&>, int> = 0>
  explicit constexpr OnStartGroupImpl(ActionInitializer&& action)
      : action_(std::forward<ActionInitializer>(action)) {}

  template <typename... Context,
            std::enable_if_t<
                std::is_invocable_r_v<absl::Status, const Action&, Context&...>,
                int> = 0>
  absl::Status HandleStartGroup(Context&... context) const {
    return action_(context...);
  }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Action action_;
};

template <int field_number, typename Action>
class OnEndGroupImpl {
 public:
  static constexpr int kFieldNumber = field_number;

  template <typename ActionInitializer,
            std::enable_if_t<
                std::is_constructible_v<Action, ActionInitializer&&>, int> = 0>
  explicit constexpr OnEndGroupImpl(ActionInitializer&& action)
      : action_(std::forward<ActionInitializer>(action)) {}

  template <typename... Context,
            std::enable_if_t<
                std::is_invocable_r_v<absl::Status, const Action&, Context&...>,
                int> = 0>
  absl::Status HandleEndGroup(Context&... context) const {
    return action_(context...);
  }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Action action_;
};

}  // namespace field_handlers_internal

}  // namespace riegeli

#endif  // RIEGELI_MESSAGES_FIELD_HANDLERS_H_
