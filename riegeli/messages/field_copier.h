// Copyright 2026 Google LLC
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

#ifndef RIEGELI_MESSAGES_FIELD_COPIER_H_
#define RIEGELI_MESSAGES_FIELD_COPIER_H_

#include <stdint.h>

#include <optional>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/cord_iterator_span.h"
#include "riegeli/bytes/limiting_reader.h"
#include "riegeli/messages/message_wire_format.h"
#include "riegeli/messages/serialized_message_reader.h"
#include "riegeli/messages/serialized_message_writer.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

// The type returned by `FieldCopier()`.
template <int field_number, WireTypeSet wire_types = AllWireTypes>
class FieldCopierType;

// The type returned by `DynamicFieldCopier()`.
template <typename Accept, WireTypeSet wire_types = AllWireTypes>
class DynamicFieldCopierType;

// The type of the `accept` function used by `AnyFieldCopier()`.
struct AcceptAnyField;

// The type returned by `AnyFieldCopier()`.
using AnyFieldCopierType = DynamicFieldCopierType<AcceptAnyField>;

// A field handler for `SerializedMessageReader` which copies the given field to
// a `SerializedMessageWriter`.
//
// As an optimization, `wire_types` constrains the set of wire types to handle.
// This yields smaller and faster code.
//
// `Context...` types must contain exactly one occurrence of
// `SerializedMessageWriter`. Use `ContextProjection()` to select the
// `SerializedMessageWriter` if this is not the case.
template <int field_number, WireTypeSet wire_types = AllWireTypes>
constexpr FieldCopierType<field_number, wire_types> FieldCopier() {
  return FieldCopierType<field_number, wire_types>();
}

// A field handler for `SerializedMessageReader` which copies the given field to
// a `SerializedMessageWriter`, with the predicate over field numbers specified
// at runtime, possibly mapping them to other field numbers.
//
// `accept` is an invocable taking the field number as `int` and returning
// `std::optional<int>`. If it returns a value other than `std::nullopt`,
// the field is copied, and the returned value is used as the new field number.
//
// `Context...` types must contain exactly one occurrence of
// `SerializedMessageWriter`. Use `ContextProjection()` to select the
// `SerializedMessageWriter` if this is not the case.
template <WireTypeSet wire_types = AllWireTypes, typename Accept>
constexpr DynamicFieldCopierType<std::decay_t<Accept>, wire_types>
DynamicFieldCopier(Accept&& accept) {
  return DynamicFieldCopierType<std::decay_t<Accept>, wire_types>(
      std::forward<Accept>(accept));
}

// A field handler for `SerializedMessageReader` which copies any field to a
// `SerializedMessageWriter`.
//
// It is meant to be used as the last field handler, so that remaining fields
// not handled by previous field handlers will be copied unchanged.
//
// `Context...` types must contain exactly one occurrence of
// `SerializedMessageWriter`. Use `ContextProjection()` to select the
// `SerializedMessageWriter` if this is not the case.
constexpr AnyFieldCopierType AnyFieldCopier();

// Implementation details follow.

template <int field_number, WireTypeSet wire_types>
class FieldCopierType {
 public:
  static constexpr int kFieldNumber = field_number;

  constexpr FieldCopierType() = default;

  FieldCopierType(const FieldCopierType& that) = default;
  FieldCopierType& operator=(const FieldCopierType& that) = default;

  template <typename... Context, WireTypeSet dependent_wire_types = wire_types,
            std::enable_if_t<(dependent_wire_types & WireTypeSet::kVarint) ==
                                 WireTypeSet::kVarint,
                             int> = 0>
  absl::Status HandleVarint(uint64_t repr, Context&... context) const {
    return message_writer(context...).WriteUInt64(field_number, repr);
  }

  template <typename... Context, WireTypeSet dependent_wire_types = wire_types,
            std::enable_if_t<(dependent_wire_types & WireTypeSet::kFixed32) ==
                                 WireTypeSet::kFixed32,
                             int> = 0>
  absl::Status HandleFixed32(uint32_t repr, Context&... context) const {
    return message_writer(context...).WriteFixed32(field_number, repr);
  }

  template <typename... Context, WireTypeSet dependent_wire_types = wire_types,
            std::enable_if_t<(dependent_wire_types & WireTypeSet::kFixed64) ==
                                 WireTypeSet::kFixed64,
                             int> = 0>
  absl::Status HandleFixed64(uint64_t repr, Context&... context) const {
    return message_writer(context...).WriteFixed64(field_number, repr);
  }

  template <
      typename... Context, WireTypeSet dependent_wire_types = wire_types,
      std::enable_if_t<(dependent_wire_types & WireTypeSet::kLengthDelimited) ==
                           WireTypeSet::kLengthDelimited,
                       int> = 0>
  absl::Status HandleLengthDelimitedFromReader(ReaderSpan<> repr,
                                               Context&... context) const {
    return message_writer(context...)
        .WriteString(field_number, std::move(repr));
  }

  template <
      typename... Context, WireTypeSet dependent_wire_types = wire_types,
      std::enable_if_t<(dependent_wire_types & WireTypeSet::kLengthDelimited) ==
                           WireTypeSet::kLengthDelimited,
                       int> = 0>
  absl::Status HandleLengthDelimitedFromCord(
      CordIteratorSpan repr, ABSL_ATTRIBUTE_UNUSED std::string& scratch,
      Context&... context) const {
    return message_writer(context...)
        .WriteString(field_number, std::move(repr));
  }

  template <
      typename... Context, WireTypeSet dependent_wire_types = wire_types,
      std::enable_if_t<(dependent_wire_types & WireTypeSet::kLengthDelimited) ==
                           WireTypeSet::kLengthDelimited,
                       int> = 0>
  absl::Status HandleLengthDelimitedFromString(absl::string_view repr,
                                               Context&... context) const {
    return message_writer(context...).WriteString(field_number, repr);
  }

  template <
      typename... Context, WireTypeSet dependent_wire_types = wire_types,
      std::enable_if_t<(dependent_wire_types & WireTypeSet::kStartGroup) ==
                           WireTypeSet::kStartGroup,
                       int> = 0>
  absl::Status HandleStartGroup(Context&... context) const {
    return message_writer(context...).OpenGroup(field_number);
  }

  template <typename... Context, WireTypeSet dependent_wire_types = wire_types,
            std::enable_if_t<(dependent_wire_types & WireTypeSet::kEndGroup) ==
                                 WireTypeSet::kEndGroup,
                             int> = 0>
  absl::Status HandleEndGroup(Context&... context) const {
    return message_writer(context...).CloseGroup(field_number);
  }

 private:
  template <typename... Context>
  static SerializedMessageWriter& message_writer(Context&... context) {
    return std::get<SerializedMessageWriter&>(
        std::tuple<Context&...>(context...));
  }
};

template <typename Accept, WireTypeSet wire_types>
class DynamicFieldCopierType {
 public:
  static constexpr int kFieldNumber = kDynamicFieldNumber;

  template <typename AcceptInitializer,
            std::enable_if_t<std::is_convertible_v<AcceptInitializer&&, Accept>,
                             int> = 0>
  explicit constexpr DynamicFieldCopierType(AcceptInitializer&& accept)
      : accept_(std::forward<AcceptInitializer>(accept)) {}

  DynamicFieldCopierType() = default;

  DynamicFieldCopierType(const DynamicFieldCopierType& that) = default;
  DynamicFieldCopierType& operator=(const DynamicFieldCopierType& that) =
      default;

  template <typename... Context, WireTypeSet dependent_wire_types = wire_types,
            std::enable_if_t<(dependent_wire_types & WireTypeSet::kVarint) ==
                                 WireTypeSet::kVarint,
                             int> = 0>
  std::optional<int> AcceptVarint(int field_number) const {
    return accept_(field_number);
  }

  template <typename... Context, WireTypeSet dependent_wire_types = wire_types,
            std::enable_if_t<(dependent_wire_types & WireTypeSet::kVarint) ==
                                 WireTypeSet::kVarint,
                             int> = 0>
  absl::Status DynamicHandleVarint(int field_number, uint64_t repr,
                                   Context&... context) const {
    return message_writer(context...).WriteUInt64(field_number, repr);
  }

  template <typename... Context, WireTypeSet dependent_wire_types = wire_types,
            std::enable_if_t<(dependent_wire_types & WireTypeSet::kFixed32) ==
                                 WireTypeSet::kFixed32,
                             int> = 0>
  std::optional<int> AcceptFixed32(int field_number) const {
    return accept_(field_number);
  }

  template <typename... Context, WireTypeSet dependent_wire_types = wire_types,
            std::enable_if_t<(dependent_wire_types & WireTypeSet::kFixed32) ==
                                 WireTypeSet::kFixed32,
                             int> = 0>
  absl::Status DynamicHandleFixed32(int field_number, uint32_t repr,
                                    Context&... context) const {
    return message_writer(context...).WriteFixed32(field_number, repr);
  }

  template <typename... Context, WireTypeSet dependent_wire_types = wire_types,
            std::enable_if_t<(dependent_wire_types & WireTypeSet::kFixed64) ==
                                 WireTypeSet::kFixed64,
                             int> = 0>
  std::optional<int> AcceptFixed64(int field_number) const {
    return accept_(field_number);
  }

  template <typename... Context, WireTypeSet dependent_wire_types = wire_types,
            std::enable_if_t<(dependent_wire_types & WireTypeSet::kFixed64) ==
                                 WireTypeSet::kFixed64,
                             int> = 0>
  absl::Status DynamicHandleFixed64(int field_number, uint64_t repr,
                                    Context&... context) const {
    return message_writer(context...).WriteFixed64(field_number, repr);
  }

  template <typename... Context>
  std::optional<int> AcceptLengthDelimited(int field_number) const {
    return accept_(field_number);
  }

  template <
      typename... Context, WireTypeSet dependent_wire_types = wire_types,
      std::enable_if_t<(dependent_wire_types & WireTypeSet::kLengthDelimited) ==
                           WireTypeSet::kLengthDelimited,
                       int> = 0>
  absl::Status DynamicHandleLengthDelimitedFromReader(
      int field_number, ReaderSpan<> repr, Context&... context) const {
    return message_writer(context...)
        .WriteString(field_number, std::move(repr));
  }

  template <
      typename... Context, WireTypeSet dependent_wire_types = wire_types,
      std::enable_if_t<(dependent_wire_types & WireTypeSet::kLengthDelimited) ==
                           WireTypeSet::kLengthDelimited,
                       int> = 0>
  absl::Status DynamicHandleLengthDelimitedFromCord(
      int field_number, CordIteratorSpan repr,
      ABSL_ATTRIBUTE_UNUSED std::string& scratch, Context&... context) const {
    return message_writer(context...)
        .WriteString(field_number, std::move(repr));
  }

  template <
      typename... Context, WireTypeSet dependent_wire_types = wire_types,
      std::enable_if_t<(dependent_wire_types & WireTypeSet::kLengthDelimited) ==
                           WireTypeSet::kLengthDelimited,
                       int> = 0>
  absl::Status DynamicHandleLengthDelimitedFromString(
      int field_number, absl::string_view repr, Context&... context) const {
    return message_writer(context...).WriteString(field_number, repr);
  }

  template <
      typename... Context, WireTypeSet dependent_wire_types = wire_types,
      std::enable_if_t<(dependent_wire_types & WireTypeSet::kStartGroup) ==
                           WireTypeSet::kStartGroup,
                       int> = 0>
  std::optional<int> AcceptStartGroup(int field_number) const {
    return accept_(field_number);
  }

  template <
      typename... Context, WireTypeSet dependent_wire_types = wire_types,
      std::enable_if_t<(dependent_wire_types & WireTypeSet::kStartGroup) ==
                           WireTypeSet::kStartGroup,
                       int> = 0>
  absl::Status DynamicHandleStartGroup(int field_number,
                                       Context&... context) const {
    return message_writer(context...).OpenGroup(field_number);
  }

  template <typename... Context, WireTypeSet dependent_wire_types = wire_types,
            std::enable_if_t<(dependent_wire_types & WireTypeSet::kEndGroup) ==
                                 WireTypeSet::kEndGroup,
                             int> = 0>
  std::optional<int> AcceptEndGroup(int field_number) const {
    return accept_(field_number);
  }

  template <typename... Context, WireTypeSet dependent_wire_types = wire_types,
            std::enable_if_t<(dependent_wire_types & WireTypeSet::kEndGroup) ==
                                 WireTypeSet::kEndGroup,
                             int> = 0>
  absl::Status DynamicHandleEndGroup(int field_number,
                                     Context&... context) const {
    return message_writer(context...).CloseGroup(field_number);
  }

 private:
  template <typename... Context>
  static SerializedMessageWriter& message_writer(Context&... context) {
    return std::get<SerializedMessageWriter&>(
        std::tuple<Context&...>(context...));
  }

  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Accept accept_;
};

struct AcceptAnyField {
  std::optional<int> operator()(int field_number) const { return field_number; }
};

constexpr AnyFieldCopierType AnyFieldCopier() {
  return AnyFieldCopierType(AcceptAnyField());
}

}  // namespace riegeli

#endif  // RIEGELI_MESSAGES_FIELD_COPIER_H_
