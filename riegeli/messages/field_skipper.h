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

#ifndef RIEGELI_MESSAGES_FIELD_SKIPPER_H_
#define RIEGELI_MESSAGES_FIELD_SKIPPER_H_

#include <stdint.h>

#include <string>
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

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

// The type returned by `FieldSkipper()` and `UnboundFieldSkipper()`.
template <int field_number, WireTypeSet wire_types = AllWireTypes>
class FieldSkipperType;

// The type returned by `DynamicFieldSkipper()`.
template <typename Accept, WireTypeSet wire_types = AllWireTypes>
class DynamicFieldSkipperType;

// A field handler for `SerializedMessageReader` which ignores the given field.
//
// This can be used together with `FieldTracker()` or
// `OnlyFirstFieldThenCancel()` to detect field presence without otherwise
// processing its value.
//
// As an optimization, `wire_types` constrains the set of wire types to handle.
// This yields smaller and faster code.
template <int field_number, WireTypeSet wire_types = AllWireTypes>
constexpr FieldSkipperType<field_number, wire_types> FieldSkipper() {
  return FieldSkipperType<field_number, wire_types>();
}

// A field handler for `SerializedMessageReader` which ignores the given field,
// with the predicate over field numbers specified at runtime.
//
// This can be used together with `FieldTracker()` or
// `OnlyFirstFieldThenCancel()` to detect field presence without otherwise
// processing its value.
//
// `accept` is an invocable taking the field number as `int` and returning
// `bool`. If it returns `true`, the field is ignored.
template <WireTypeSet wire_types = AllWireTypes, typename Accept>
constexpr DynamicFieldSkipperType<std::decay_t<Accept>, wire_types>
DynamicFieldSkipper(Accept&& accept) {
  return DynamicFieldSkipperType<std::decay_t<Accept>, wire_types>(
      std::forward<Accept>(accept));
}

// An unbound field handler for a `DynamicFieldHandler` or `FieldHandlerMap`
// which ignores the current field, with the field number specified at runtime.
//
// This can be used together with `FieldTracker()` or
// `OnlyFirstFieldThenCancel()` to detect field presence without otherwise
// processing its value.
//
// As an optimization, `wire_types` constrains the set of wire types to handle.
// This yields smaller and faster code.
template <WireTypeSet wire_types = AllWireTypes>
constexpr FieldSkipperType<kUnboundFieldNumber, wire_types>
UnboundFieldSkipper() {
  return FieldSkipperType<kUnboundFieldNumber, wire_types>();
}

// Implementation details follow.

template <int field_number, WireTypeSet wire_types>
class FieldSkipperType {
 public:
  static constexpr int kFieldNumber = field_number;

  constexpr FieldSkipperType() = default;

  FieldSkipperType(const FieldSkipperType& that) = default;
  FieldSkipperType& operator=(const FieldSkipperType& that) = default;

  template <typename... Context, WireTypeSet dependent_wire_types = wire_types,
            std::enable_if_t<(dependent_wire_types & WireTypeSet::kVarint) ==
                                 WireTypeSet::kVarint,
                             int> = 0>
  absl::Status HandleVarint(uint64_t /*repr*/, Context&... /*context*/) const {
    return absl::OkStatus();
  }

  template <typename... Context, WireTypeSet dependent_wire_types = wire_types,
            std::enable_if_t<(dependent_wire_types & WireTypeSet::kFixed32) ==
                                 WireTypeSet::kFixed32,
                             int> = 0>
  absl::Status HandleFixed32(uint32_t /*repr*/, Context&... /*context*/) const {
    return absl::OkStatus();
  }

  template <typename... Context, WireTypeSet dependent_wire_types = wire_types,
            std::enable_if_t<(dependent_wire_types & WireTypeSet::kFixed64) ==
                                 WireTypeSet::kFixed64,
                             int> = 0>
  absl::Status HandleFixed64(uint64_t /*repr*/, Context&... /*context*/) const {
    return absl::OkStatus();
  }

  template <
      typename... Context, WireTypeSet dependent_wire_types = wire_types,
      std::enable_if_t<(dependent_wire_types & WireTypeSet::kLengthDelimited) ==
                           WireTypeSet::kLengthDelimited,
                       int> = 0>
  absl::Status HandleLengthDelimitedFromReader(ReaderSpan<> repr,
                                               Context&... /*context*/) const {
    return riegeli::SkipLengthDelimitedFromReader(std::move(repr));
  }

  template <
      typename... Context, WireTypeSet dependent_wire_types = wire_types,
      std::enable_if_t<(dependent_wire_types & WireTypeSet::kLengthDelimited) ==
                           WireTypeSet::kLengthDelimited,
                       int> = 0>
  absl::Status HandleLengthDelimitedFromCord(CordIteratorSpan repr,
                                             std::string& /*scratch*/,
                                             Context&... /*context*/) const {
    return riegeli::SkipLengthDelimitedFromCord(std::move(repr));
  }

  template <
      typename... Context, WireTypeSet dependent_wire_types = wire_types,
      std::enable_if_t<(dependent_wire_types & WireTypeSet::kLengthDelimited) ==
                           WireTypeSet::kLengthDelimited,
                       int> = 0>
  absl::Status HandleLengthDelimitedFromString(absl::string_view /*repr*/,
                                               Context&... /*context*/) const {
    return absl::OkStatus();
  }

  template <
      typename... Context, WireTypeSet dependent_wire_types = wire_types,
      std::enable_if_t<(dependent_wire_types & WireTypeSet::kStartGroup) ==
                           WireTypeSet::kStartGroup,
                       int> = 0>
  absl::Status HandleStartGroup(Context&... /*context*/) const {
    return absl::OkStatus();
  }

  template <typename... Context, WireTypeSet dependent_wire_types = wire_types,
            std::enable_if_t<(dependent_wire_types & WireTypeSet::kEndGroup) ==
                                 WireTypeSet::kEndGroup,
                             int> = 0>
  absl::Status HandleEndGroup(Context&... /*context*/) const {
    return absl::OkStatus();
  }
};

template <typename Accept, WireTypeSet wire_types>
class DynamicFieldSkipperType {
 public:
  static constexpr int kFieldNumber = kDynamicFieldNumber;

  template <typename AcceptInitializer,
            std::enable_if_t<std::is_convertible_v<AcceptInitializer&&, Accept>,
                             int> = 0>
  explicit constexpr DynamicFieldSkipperType(AcceptInitializer&& accept)
      : accept_(std::forward<AcceptInitializer>(accept)) {}

  DynamicFieldSkipperType() = default;

  DynamicFieldSkipperType(const DynamicFieldSkipperType& that) = default;
  DynamicFieldSkipperType& operator=(const DynamicFieldSkipperType& that) =
      default;

  template <typename... Context, WireTypeSet dependent_wire_types = wire_types,
            std::enable_if_t<(dependent_wire_types & WireTypeSet::kVarint) ==
                                 WireTypeSet::kVarint,
                             int> = 0>
  FieldAcceptedIf AcceptVarint(int field_number) const {
    return FieldAcceptedIf(accept_(field_number));
  }

  template <typename... Context, WireTypeSet dependent_wire_types = wire_types,
            std::enable_if_t<(dependent_wire_types & WireTypeSet::kVarint) ==
                                 WireTypeSet::kVarint,
                             int> = 0>
  absl::Status DynamicHandleVarint(FieldAccepted, uint64_t /*repr*/,
                                   Context&... /*context*/) const {
    return absl::OkStatus();
  }

  template <typename... Context, WireTypeSet dependent_wire_types = wire_types,
            std::enable_if_t<(dependent_wire_types & WireTypeSet::kFixed32) ==
                                 WireTypeSet::kFixed32,
                             int> = 0>
  FieldAcceptedIf AcceptFixed32(int field_number) const {
    return FieldAcceptedIf(accept_(field_number));
  }

  template <typename... Context, WireTypeSet dependent_wire_types = wire_types,
            std::enable_if_t<(dependent_wire_types & WireTypeSet::kFixed32) ==
                                 WireTypeSet::kFixed32,
                             int> = 0>
  absl::Status DynamicHandleFixed32(FieldAccepted, uint32_t /*repr*/,
                                    Context&... /*context*/) const {
    return absl::OkStatus();
  }

  template <typename... Context, WireTypeSet dependent_wire_types = wire_types,
            std::enable_if_t<(dependent_wire_types & WireTypeSet::kFixed64) ==
                                 WireTypeSet::kFixed64,
                             int> = 0>
  FieldAcceptedIf AcceptFixed64(int field_number) const {
    return FieldAcceptedIf(accept_(field_number));
  }

  template <typename... Context, WireTypeSet dependent_wire_types = wire_types,
            std::enable_if_t<(dependent_wire_types & WireTypeSet::kFixed64) ==
                                 WireTypeSet::kFixed64,
                             int> = 0>
  absl::Status DynamicHandleFixed64(FieldAccepted, uint64_t /*repr*/,
                                    Context&... /*context*/) const {
    return absl::OkStatus();
  }

  template <typename... Context>
  FieldAcceptedIf AcceptLengthDelimited(int field_number) const {
    return FieldAcceptedIf(accept_(field_number));
  }

  template <
      typename... Context, WireTypeSet dependent_wire_types = wire_types,
      std::enable_if_t<(dependent_wire_types & WireTypeSet::kLengthDelimited) ==
                           WireTypeSet::kLengthDelimited,
                       int> = 0>
  absl::Status DynamicHandleLengthDelimitedFromReader(
      FieldAccepted, ReaderSpan<> repr, Context&... /*context*/) const {
    return riegeli::SkipLengthDelimitedFromReader(std::move(repr));
  }

  template <
      typename... Context, WireTypeSet dependent_wire_types = wire_types,
      std::enable_if_t<(dependent_wire_types & WireTypeSet::kLengthDelimited) ==
                           WireTypeSet::kLengthDelimited,
                       int> = 0>
  absl::Status DynamicHandleLengthDelimitedFromCord(
      FieldAccepted, CordIteratorSpan repr, std::string& /*scratch*/,
      Context&... /*context*/) const {
    return riegeli::SkipLengthDelimitedFromCord(std::move(repr));
  }

  template <
      typename... Context, WireTypeSet dependent_wire_types = wire_types,
      std::enable_if_t<(dependent_wire_types & WireTypeSet::kLengthDelimited) ==
                           WireTypeSet::kLengthDelimited,
                       int> = 0>
  absl::Status DynamicHandleLengthDelimitedFromString(
      FieldAccepted, absl::string_view /*repr*/,
      Context&... /*context*/) const {
    return absl::OkStatus();
  }

  template <
      typename... Context, WireTypeSet dependent_wire_types = wire_types,
      std::enable_if_t<(dependent_wire_types & WireTypeSet::kStartGroup) ==
                           WireTypeSet::kStartGroup,
                       int> = 0>
  FieldAcceptedIf AcceptStartGroup(int field_number) const {
    return FieldAcceptedIf(accept_(field_number));
  }

  template <
      typename... Context, WireTypeSet dependent_wire_types = wire_types,
      std::enable_if_t<(dependent_wire_types & WireTypeSet::kStartGroup) ==
                           WireTypeSet::kStartGroup,
                       int> = 0>
  absl::Status DynamicHandleStartGroup(FieldAccepted,
                                       Context&... /*context*/) const {
    return absl::OkStatus();
  }

  template <typename... Context, WireTypeSet dependent_wire_types = wire_types,
            std::enable_if_t<(dependent_wire_types & WireTypeSet::kEndGroup) ==
                                 WireTypeSet::kEndGroup,
                             int> = 0>
  FieldAcceptedIf AcceptEndGroup(int field_number) const {
    return FieldAcceptedIf(accept_(field_number));
  }

  template <typename... Context, WireTypeSet dependent_wire_types = wire_types,
            std::enable_if_t<(dependent_wire_types & WireTypeSet::kEndGroup) ==
                                 WireTypeSet::kEndGroup,
                             int> = 0>
  absl::Status DynamicHandleEndGroup(FieldAccepted,
                                     Context&... /*context*/) const {
    return absl::OkStatus();
  }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Accept accept_;
};

}  // namespace riegeli

#endif  // RIEGELI_MESSAGES_FIELD_SKIPPER_H_
