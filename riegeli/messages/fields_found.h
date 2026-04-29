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

#ifndef RIEGELI_MESSAGES_FIELDS_FOUND_H_
#define RIEGELI_MESSAGES_FIELDS_FOUND_H_

#include <stddef.h>
#include <stdint.h>

#include <bitset>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/cord_iterator_span.h"
#include "riegeli/bytes/limiting_reader.h"
#include "riegeli/messages/serialized_message_reader.h"
#include "riegeli/messages/serialized_message_reader_internal.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

namespace fields_found_internal {

template <bool matches>
struct MatchingIndex {
  /*implicit*/ constexpr MatchingIndex(size_t index) : index(index) {}
  size_t index;
};

template <typename... Context>
constexpr size_t NumbersFoundIndexFromContext();

template <typename... Context>
auto& NumbersFoundFromContext(Context&... context);

}  // namespace fields_found_internal

// Tracks which fields among some tracked subset have been found in a message,
// or possibly which fields have been marked as found once some condition is
// satisfied.
//
// A variable of this type is usually passed to field handlers as one of the
// `Context` parameters. There should be exactly one context parameter of this
// type.
//
// The fields are identified by their field numbers, or possibly by other `int`
// constants agreed upon by callers.
template <int... numbers>
class NumbersFound {
 public:
  // Marks no fields as found.
  NumbersFound() = default;

  NumbersFound(const NumbersFound& that) = default;
  NumbersFound& operator=(const NumbersFound& that) = default;

  // Returns the number of fields being tracked.
  static constexpr size_t size() { return sizeof...(numbers); }

  // Checks if this field has been found.
  template <int number>
  bool at() const {
    return bitset_[IndexOfNumber<number>()];
  }

  // Returns a `std::bitset::reference` to the bit corresponding to this field.
  //
  // This can be used to check if this field has been found, or to mark it as
  // found.
  template <int number>
  typename std::bitset<sizeof...(numbers)>::reference at() {
    return bitset_[IndexOfNumber<number>()];
  }

  // Returns `true` if all tracked fields have been found.
  bool all() const { return bitset_.all(); }

 private:
  template <int number, size_t... indices>
  static constexpr size_t IndexOfNumberImpl(std::index_sequence<indices...>) {
    return std::get<fields_found_internal::MatchingIndex<true>>(
               std::tuple<
                   fields_found_internal::MatchingIndex<numbers == number>...>(
                   indices...))
        .index;
  }

  template <int number>
  static constexpr size_t IndexOfNumber() {
    return IndexOfNumberImpl<number>(
        std::make_index_sequence<sizeof...(numbers)>());
  }

  std::bitset<sizeof...(numbers)> bitset_;
};

// The type returned by `FieldHandlerWrapper()`.
template <typename ActionWrapper, typename FieldHandler>
class FieldHandlerWrapperImpl;

// Implements field handler wrappers below. Users of defined field handler
// wrappers do not need to be concerned with this. This is relevant for writing
// custom field handler wrappers.
//
// Wraps a field handler into a handler, which wraps an action into a call to
// an `ActionWrapper` with two parameters: the original action, and the action
// of skipping the field. `ActionWrapper` should call exactly one of these
// parameters.
//
// The `ActionWrapper` is default-constructed.
template <typename ActionWrapper, typename FieldHandler>
constexpr FieldHandlerWrapperImpl<ActionWrapper, std::decay_t<FieldHandler>>
FieldHandlerWrapper(FieldHandler&& field_handler) {
  return FieldHandlerWrapperImpl<ActionWrapper, std::decay_t<FieldHandler>>(
      std::forward<FieldHandler>(field_handler));
}

// Implements field handler wrappers below. Users of defined field handler
// wrappers do not need to be concerned with this. This is relevant for writing
// custom field handler wrappers.
//
// Wraps a field handler into a handler, which wraps an action into a call to
// an `ActionWrapper` with two parameters: the original action, and the action
// of skipping the field. `ActionWrapper` should call exactly one of these
// parameters.
//
// The `ActionWrapper` is passed explicitly.
template <typename ActionWrapper, typename FieldHandler>
constexpr FieldHandlerWrapperImpl<std::decay_t<ActionWrapper>,
                                  std::decay_t<FieldHandler>>
FieldHandlerWrapper(ActionWrapper&& action_wrapper,
                    FieldHandler&& field_handler) {
  return FieldHandlerWrapperImpl<std::decay_t<ActionWrapper>,
                                 std::decay_t<FieldHandler>>(
      std::forward<ActionWrapper>(action_wrapper),
      std::forward<FieldHandler>(field_handler));
}

// The type used in the result of `FieldTracker()`.
template <int field_number>
struct FieldTrackerActionWrapper;

// Wraps a field handler into a handler which tracks whether the given field has
// been found.
//
// The field number is passed as a template parameter.
template <int field_number, typename FieldHandler>
constexpr FieldHandlerWrapperImpl<FieldTrackerActionWrapper<field_number>,
                                  std::decay_t<FieldHandler>>
FieldTracker(FieldHandler&& field_handler) {
  return FieldHandlerWrapper<FieldTrackerActionWrapper<field_number>>(
      std::forward<FieldHandler>(field_handler));
}

// Wraps a field handler into a handler which tracks whether the given field has
// been found.
//
// The field number is extracted from the field handler.
template <typename FieldHandler>
constexpr FieldHandlerWrapperImpl<
    FieldTrackerActionWrapper<std::decay_t<FieldHandler>::kFieldNumber>,
    std::decay_t<FieldHandler>>
FieldTracker(FieldHandler&& field_handler) {
  static_assert(std::decay_t<FieldHandler>::kFieldNumber > 0);
  return FieldTracker<std::decay_t<FieldHandler>::kFieldNumber>(
      std::forward<FieldHandler>(field_handler));
}

// The type used in the result of `UntilFieldFound()`.
template <int field_number>
struct UntilFieldFoundActionWrapper;

// Wraps a field handler into a handler which performs its action unless the
// given field has been marked as found. Marking the field as found is the
// responsibility of field handlers.
//
// If a non-repeated field occurs multiple times, this violates the proto
// semantics of merging, where the next occurrence should override or be merged
// with the previous value. Multiple occurrences can happen if serialized
// messages have been concatenated, or if messages have been merged while this
// field was unknown, or if this field used to be repeated.
//
// The field number is passed as a template parameter.
template <int field_number, typename FieldHandler>
constexpr FieldHandlerWrapperImpl<UntilFieldFoundActionWrapper<field_number>,
                                  std::decay_t<FieldHandler>>
UntilFieldFound(FieldHandler&& field_handler) {
  return FieldHandlerWrapper<UntilFieldFoundActionWrapper<field_number>>(
      std::forward<FieldHandler>(field_handler));
}

// Wraps a field handler into a handler which performs its action unless the
// given field has been marked as found. Marking the field as found is the
// responsibility of field handlers.
//
// If a non-repeated field occurs multiple times, this violates the proto
// semantics of merging, where the next occurrence should override or be merged
// with the previous value. Multiple occurrences can happen if serialized
// messages have been concatenated, or if messages have been merged while this
// field was unknown, or if this field used to be repeated.
//
// The field number is extracted from the field handler.
template <typename FieldHandler>
constexpr FieldHandlerWrapperImpl<
    UntilFieldFoundActionWrapper<std::decay_t<FieldHandler>::kFieldNumber>,
    std::decay_t<FieldHandler>>
UntilFieldFound(FieldHandler&& field_handler) {
  static_assert(std::decay_t<FieldHandler>::kFieldNumber > 0);
  return UntilFieldFound<std::decay_t<FieldHandler>::kFieldNumber>(
      std::forward<FieldHandler>(field_handler));
}

// The type used in the result of `UntilFieldFoundThenCancel()`.
template <int field_number>
struct UntilFieldFoundThenCancelActionWrapper;

// Wraps a field handler into a handler which performs its action unless the
// given field has been marked as found. Marking the field as found is the
// responsibility of field handlers. Once all tracked fields have been found,
// reading the message is cancelled with `absl::CancelledError()`.
//
// To simplify checking the outcome of
// `SerializedMessageReader::ReadMessage()`, store its status, then handle
// `found.all()`. Only if that was `false`, check the stored status, without
// having to check it for cancellation.
//
// If a non-repeated field occurs multiple times, this violates the proto
// semantics of merging, where the next occurrence should override or be merged
// with the previous value. Multiple occurrences can happen if serialized
// messages have been concatenated, or if messages have been merged while this
// field was unknown, or if this field used to be repeated.
//
// The field number is passed as a template parameter.
template <int field_number, typename FieldHandler>
constexpr FieldHandlerWrapperImpl<
    UntilFieldFoundThenCancelActionWrapper<field_number>,
    std::decay_t<FieldHandler>>
UntilFieldFoundThenCancel(FieldHandler&& field_handler) {
  return FieldHandlerWrapper<
      UntilFieldFoundThenCancelActionWrapper<field_number>>(
      std::forward<FieldHandler>(field_handler));
}

// Wraps a field handler into a handler which performs its action unless the
// given field has been marked as found. Marking the field as found is the
// responsibility of field handlers. Once all tracked fields have been found,
// reading the message is cancelled with `absl::CancelledError()`.
//
// To simplify checking the outcome of
// `SerializedMessageReader::ReadMessage()`, store its status, then handle
// `found.all()`. Only if that was `false`, check the stored status, without
// having to check it for cancellation.
//
// If a non-repeated field occurs multiple times, this violates the proto
// semantics of merging, where the next occurrence should override or be merged
// with the previous value. Multiple occurrences can happen if serialized
// messages have been concatenated, or if messages have been merged while this
// field was unknown, or if this field used to be repeated.
//
// The field number is extracted from the field handler.
template <typename FieldHandler>
constexpr FieldHandlerWrapperImpl<UntilFieldFoundThenCancelActionWrapper<
                                      std::decay_t<FieldHandler>::kFieldNumber>,
                                  std::decay_t<FieldHandler>>
UntilFieldFoundThenCancel(FieldHandler&& field_handler) {
  static_assert(std::decay_t<FieldHandler>::kFieldNumber > 0);
  return UntilFieldFoundThenCancel<std::decay_t<FieldHandler>::kFieldNumber>(
      std::forward<FieldHandler>(field_handler));
}

// The type used in the result of `OnlyFirstFieldThenCancel()`.
template <int field_number>
struct OnlyFirstFieldActionWrapper;

// Wraps a field handler into a handler which performs its action only for the
// first occurrence of the given field.
//
// If a non-repeated field occurs multiple times, this violates the proto
// semantics of merging, where the next occurrence should override or be merged
// with the previous value. Multiple occurrences can happen if serialized
// messages have been concatenated, or if messages have been merged while this
// field was unknown, or if this field used to be repeated.
//
// The field number is passed as a template parameter.
template <int field_number, typename FieldHandler>
constexpr FieldHandlerWrapperImpl<OnlyFirstFieldActionWrapper<field_number>,
                                  std::decay_t<FieldHandler>>
OnlyFirstField(FieldHandler&& field_handler) {
  return FieldHandlerWrapper<OnlyFirstFieldActionWrapper<field_number>>(
      std::forward<FieldHandler>(field_handler));
}

// Wraps a field handler into a handler which performs an action only the
// first occurrence of the given field.
//
// If a non-repeated field occurs multiple times, this violates the proto
// semantics of merging, where the next occurrence should override or be merged
// with the previous value. Multiple occurrences can happen if serialized
// messages have been concatenated, or if messages have been merged while this
// field was unknown, or if this field used to be repeated.
//
// The field number is extracted from the field handler.
template <typename FieldHandler>
constexpr FieldHandlerWrapperImpl<
    OnlyFirstFieldActionWrapper<std::decay_t<FieldHandler>::kFieldNumber>,
    std::decay_t<FieldHandler>>
OnlyFirstField(FieldHandler&& field_handler) {
  static_assert(std::decay_t<FieldHandler>::kFieldNumber > 0);
  return OnlyFirstField<std::decay_t<FieldHandler>::kFieldNumber>(
      std::forward<FieldHandler>(field_handler));
}

// The type used in the result of `OnlyFirstFieldThenCancel()`.
template <int field_number>
struct OnlyFirstFieldThenCancelActionWrapper;

// Wraps a field handler into a handler which performs its action only for the
// first occurrence of the given field. Once all tracked fields have been found,
// reading the message is cancelled with `absl::CancelledError()`.
//
// To simplify checking the outcome of
// `SerializedMessageReader::ReadMessage()`, store its status, then handle
// `found.all()`. Only if that was `false`, check the stored status, without
// having to check it for cancellation.
//
// If a non-repeated field occurs multiple times, this violates the proto
// semantics of merging, where the next occurrence should override or be merged
// with the previous value. Multiple occurrences can happen if serialized
// messages have been concatenated, or if messages have been merged while this
// field was unknown, or if this field used to be repeated.
//
// The field number is passed as a template parameter.
template <int field_number, typename FieldHandler>
constexpr FieldHandlerWrapperImpl<
    OnlyFirstFieldThenCancelActionWrapper<field_number>,
    std::decay_t<FieldHandler>>
OnlyFirstFieldThenCancel(FieldHandler&& field_handler) {
  return FieldHandlerWrapper<
      OnlyFirstFieldThenCancelActionWrapper<field_number>>(
      std::forward<FieldHandler>(field_handler));
}

// Wraps a field handler into a handler which performs its action only for the
// first occurrence of the given field. Once all tracked fields have been found,
// reading the message is cancelled with `absl::CancelledError()`.
//
// To simplify checking the outcome of
// `SerializedMessageReader::ReadMessage()`, store its status, then handle
// `found.all()`. Only if that was `false`, check the stored status, without
// having to check it for cancellation.
//
// If a non-repeated field occurs multiple times, this violates the proto
// semantics of merging, where the next occurrence should override or be merged
// with the previous value. Multiple occurrences can happen if serialized
// messages have been concatenated, or if messages have been merged while this
// field was unknown, or if this field used to be repeated.
//
// The field number is extracted from the field handler.
template <typename FieldHandler>
constexpr FieldHandlerWrapperImpl<OnlyFirstFieldThenCancelActionWrapper<
                                      std::decay_t<FieldHandler>::kFieldNumber>,
                                  std::decay_t<FieldHandler>>
OnlyFirstFieldThenCancel(FieldHandler&& field_handler) {
  static_assert(std::decay_t<FieldHandler>::kFieldNumber > 0);
  return OnlyFirstFieldThenCancel<std::decay_t<FieldHandler>::kFieldNumber>(
      std::forward<FieldHandler>(field_handler));
}

// Implementation details follow.

namespace fields_found_internal {

template <typename T>
struct IsNumbersFound : std::false_type {};

template <int... numbers>
struct IsNumbersFound<NumbersFound<numbers...>> : std::true_type {};

template <typename... Context, size_t... indices>
constexpr size_t NumbersFoundIndexFromContextImpl(
    std::index_sequence<indices...>) {
  return std::get<MatchingIndex<true>>(
             std::tuple<MatchingIndex<IsNumbersFound<Context>::value>...>(
                 indices...))
      .index;
}

template <typename... Context>
constexpr size_t NumbersFoundIndexFromContext() {
  return NumbersFoundIndexFromContextImpl<Context...>(
      std::index_sequence_for<Context...>());
}

template <typename... Context>
auto& NumbersFoundFromContext(Context&... context) {
  return std::get<NumbersFoundIndexFromContext<Context...>()>(
      std::tuple<Context&...>(context...));
}

}  // namespace fields_found_internal

template <typename ActionWrapper, typename FieldHandler>
class FieldHandlerWrapperImpl {
 public:
  template <typename FieldHandlerInitializer,
            std::enable_if_t<
                std::is_convertible_v<FieldHandlerInitializer&&, FieldHandler>,
                int> = 0>
  explicit constexpr FieldHandlerWrapperImpl(
      FieldHandlerInitializer&& field_handler)
      : action_wrapper_(),
        field_handler_(std::forward<FieldHandlerInitializer>(field_handler)) {}

  template <
      typename ActionWrapperInitializer, typename FieldHandlerInitializer,
      std::enable_if_t<
          std::conjunction_v<
              std::is_convertible<ActionWrapperInitializer&&, ActionWrapper>,
              std::is_convertible<FieldHandlerInitializer&&, FieldHandler>>,
          int> = 0>
  explicit constexpr FieldHandlerWrapperImpl(
      ActionWrapperInitializer&& action_wrapper,
      FieldHandlerInitializer&& field_handler)
      : action_wrapper_(std::forward<ActionWrapperInitializer>(action_wrapper)),
        field_handler_(std::forward<FieldHandlerInitializer>(field_handler)) {}

  static constexpr int kFieldNumber = FieldHandler::kFieldNumber;

  template <
      typename... Context,
      std::enable_if_t<
          serialized_message_reader_internal::IsStaticFieldHandlerForVarint<
              FieldHandler, Context...>::value,
          int> = 0>
  absl::Status HandleVarint(uint64_t repr, Context&... context) const {
    return action_wrapper_(
        [&] { return field_handler_.HandleVarint(repr, context...); },
        [] { return absl::OkStatus(); }, context...);
  }

  template <typename DependentFieldHandler = FieldHandler,
            std::enable_if_t<serialized_message_reader_internal::
                                 IsDynamicFieldHandlerForVarintSomeContext<
                                     DependentFieldHandler>::value,
                             int> = 0>
  auto AcceptVarint(int field_number) const {
    return field_handler_.AcceptVarint(field_number);
  }

  template <
      typename Accepted, typename... Context,
      std::enable_if_t<
          serialized_message_reader_internal::IsDynamicFieldHandlerForVarint<
              FieldHandler, Context...>::value,
          int> = 0>
  absl::Status DynamicHandleVarint(Accepted&& accepted, uint64_t repr,
                                   Context&... context) const {
    return action_wrapper_(
        [&] {
          return field_handler_.HandleVarint(std::forward<Accepted>(accepted),
                                             repr, context...);
        },
        [] { return absl::OkStatus(); }, context...);
  }

  template <
      typename... Context,
      std::enable_if_t<
          serialized_message_reader_internal::IsStaticFieldHandlerForFixed32<
              FieldHandler, Context...>::value,
          int> = 0>
  absl::Status HandleFixed32(uint32_t repr, Context&... context) const {
    return action_wrapper_(
        [&] { return field_handler_.HandleFixed32(repr, context...); },
        [] { return absl::OkStatus(); }, context...);
  }

  template <typename DependentFieldHandler = FieldHandler,
            std::enable_if_t<serialized_message_reader_internal::
                                 IsDynamicFieldHandlerForFixed32SomeContext<
                                     DependentFieldHandler>::value,
                             int> = 0>
  auto AcceptFixed32(int field_number) const {
    return field_handler_.AcceptFixed32(field_number);
  }

  template <
      typename Accepted, typename... Context,
      std::enable_if_t<
          serialized_message_reader_internal::IsDynamicFieldHandlerForFixed32<
              FieldHandler, Context...>::value,
          int> = 0>
  absl::Status DynamicHandleFixed32(Accepted&& accepted, uint32_t repr,
                                    Context&... context) const {
    return action_wrapper_(
        [&] {
          return field_handler_.HandleFixed32(std::forward<Accepted>(accepted),
                                              repr, context...);
        },
        [] { return absl::OkStatus(); }, context...);
  }

  template <
      typename... Context,
      std::enable_if_t<
          serialized_message_reader_internal::IsStaticFieldHandlerForFixed64<
              FieldHandler, Context...>::value,
          int> = 0>
  absl::Status HandleFixed64(uint64_t repr, Context&... context) const {
    return action_wrapper_(
        [&] { return field_handler_.HandleFixed64(repr, context...); },
        [] { return absl::OkStatus(); }, context...);
  }

  template <typename DependentFieldHandler = FieldHandler,
            std::enable_if_t<serialized_message_reader_internal::
                                 IsDynamicFieldHandlerForFixed64SomeContext<
                                     DependentFieldHandler>::value,
                             int> = 0>
  auto AcceptFixed64(int field_number) const {
    return field_handler_.AcceptFixed64(field_number);
  }

  template <
      typename Accepted, typename... Context,
      std::enable_if_t<
          serialized_message_reader_internal::IsDynamicFieldHandlerForFixed64<
              FieldHandler, Context...>::value,
          int> = 0>
  absl::Status DynamicHandleFixed64(Accepted&& accepted, uint64_t repr,
                                    Context&... context) const {
    return action_wrapper_(
        [&] {
          return field_handler_.HandleFixed64(std::forward<Accepted>(accepted),
                                              repr, context...);
        },
        [] { return absl::OkStatus(); }, context...);
  }

  template <
      typename... Context,
      std::enable_if_t<serialized_message_reader_internal::
                           IsStaticFieldHandlerForLengthDelimitedFromReader<
                               FieldHandler, Context...>::value,
                       int> = 0>
  absl::Status HandleLengthDelimitedFromReader(ReaderSpan<> repr,
                                               Context&... context) const {
    return action_wrapper_(
        [&] {
          return field_handler_.HandleLengthDelimitedFromReader(std::move(repr),
                                                                context...);
        },
        [&] { return SkipLengthDelimitedFromReader(std::move(repr)); },
        context...);
  }

  template <typename... Context,
            std::enable_if_t<serialized_message_reader_internal::
                                 IsStaticFieldHandlerForLengthDelimitedFromCord<
                                     FieldHandler, Context...>::value,
                             int> = 0>
  absl::Status HandleLengthDelimitedFromCord(CordIteratorSpan repr,
                                             std::string& scratch,
                                             Context&... context) const {
    return action_wrapper_(
        [&] {
          return field_handler_.HandleLengthDelimitedFromCord(
              std::move(repr), scratch, context...);
        },
        [&] { return SkipLengthDelimitedFromCord(std::move(repr)); },
        context...);
  }

  template <
      typename... Context,
      std::enable_if_t<serialized_message_reader_internal::
                           IsStaticFieldHandlerForLengthDelimitedFromString<
                               FieldHandler, Context...>::value,
                       int> = 0>
  absl::Status HandleLengthDelimitedFromString(absl::string_view repr,
                                               Context&... context) const {
    return action_wrapper_(
        [&] {
          return field_handler_.HandleLengthDelimitedFromString(repr,
                                                                context...);
        },
        [] { return absl::OkStatus(); }, context...);
  }

  template <
      typename DependentFieldHandler = FieldHandler,
      std::enable_if_t<serialized_message_reader_internal::
                           IsDynamicFieldHandlerForLengthDelimitedSomeContext<
                               DependentFieldHandler>::value,
                       int> = 0>
  auto AcceptLengthDelimited(int field_number) const {
    return field_handler_.AcceptLengthDelimited(field_number);
  }

  template <
      typename Accepted, typename... Context,
      std::enable_if_t<serialized_message_reader_internal::
                           IsDynamicFieldHandlerForLengthDelimitedFromReader<
                               FieldHandler, Context...>::value,
                       int> = 0>
  absl::Status DynamicHandleLengthDelimitedFromReader(
      Accepted&& accepted, ReaderSpan<> repr, Context&... context) const {
    return action_wrapper_(
        [&] {
          return field_handler_.HandleLengthDelimitedFromReader(
              std::forward<Accepted>(accepted), std::move(repr), context...);
        },
        [&] { return SkipLengthDelimitedFromReader(std::move(repr)); },
        context...);
  }

  template <
      typename Accepted, typename... Context,
      std::enable_if_t<serialized_message_reader_internal::
                           IsDynamicFieldHandlerForLengthDelimitedFromCord<
                               FieldHandler, Context...>::value,
                       int> = 0>
  absl::Status DynamicHandleLengthDelimitedFromCord(Accepted&& accepted,
                                                    CordIteratorSpan repr,
                                                    std::string& scratch,
                                                    Context&... context) const {
    return action_wrapper_(
        [&] {
          return field_handler_.HandleLengthDelimitedFromCord(
              std::forward<Accepted>(accepted), std::move(repr), scratch,
              context...);
        },
        [&] { return SkipLengthDelimitedFromCord(std::move(repr)); },
        context...);
  }

  template <
      typename Accepted, typename... Context,
      std::enable_if_t<serialized_message_reader_internal::
                           IsDynamicFieldHandlerForLengthDelimitedFromString<
                               FieldHandler, Context...>::value,
                       int> = 0>
  absl::Status DynamicHandleLengthDelimitedFromString(
      Accepted&& accepted, absl::string_view repr, Context&... context) const {
    return action_wrapper_(
        [&] {
          return field_handler_.HandleLengthDelimitedFromString(
              std::forward<Accepted>(accepted), repr, context...);
        },
        [] { return absl::OkStatus(); }, context...);
  }

  template <
      typename... Context,
      std::enable_if_t<
          serialized_message_reader_internal::IsStaticFieldHandlerForStartGroup<
              FieldHandler, Context...>::value,
          int> = 0>
  absl::Status HandleStartGroup(Context&... context) const {
    return action_wrapper_(
        [&] { return field_handler_.HandleStartGroup(context...); },
        [] { return absl::OkStatus(); }, context...);
  }

  template <typename DependentFieldHandler = FieldHandler,
            std::enable_if_t<serialized_message_reader_internal::
                                 IsDynamicFieldHandlerForStartGroupSomeContext<
                                     DependentFieldHandler>::value,
                             int> = 0>
  auto AcceptStartGroup(int field_number) const {
    return field_handler_.AcceptStartGroup(field_number);
  }

  template <typename Accepted, typename... Context,
            std::enable_if_t<serialized_message_reader_internal::
                                 IsDynamicFieldHandlerForStartGroup<
                                     FieldHandler, Context...>::value,
                             int> = 0>
  absl::Status DynamicHandleStartGroup(Accepted&& accepted,
                                       Context&... context) const {
    return action_wrapper_(
        [&] {
          return field_handler_.HandleStartGroup(
              std::forward<Accepted>(accepted), context...);
        },
        [] { return absl::OkStatus(); }, context...);
  }

  template <
      typename... Context,
      std::enable_if_t<
          serialized_message_reader_internal::IsStaticFieldHandlerForEndGroup<
              FieldHandler, Context...>::value,
          int> = 0>
  absl::Status HandleEndGroup(Context&... context) const {
    return action_wrapper_(
        [&] { return field_handler_.HandleEndGroup(context...); },
        [] { return absl::OkStatus(); }, context...);
  }

  template <typename DependentFieldHandler = FieldHandler,
            std::enable_if_t<serialized_message_reader_internal::
                                 IsDynamicFieldHandlerForEndGroupSomeContext<
                                     DependentFieldHandler>::value,
                             int> = 0>
  auto AcceptEndGroup(int field_number) const {
    return field_handler_.AcceptEndGroup(field_number);
  }

  template <
      typename Accepted, typename... Context,
      std::enable_if_t<
          serialized_message_reader_internal::IsDynamicFieldHandlerForEndGroup<
              FieldHandler, Context...>::value,
          int> = 0>
  absl::Status DynamicHandleEndGroup(Accepted&& accepted,
                                     Context&... context) const {
    return action_wrapper_(
        [&] {
          return field_handler_.HandleEndGroup(std::forward<Accepted>(accepted),
                                               context...);
        },
        [] { return absl::OkStatus(); }, context...);
  }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS ActionWrapper action_wrapper_;
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS FieldHandler field_handler_;
};

template <int field_number>
struct FieldTrackerActionWrapper {
  template <typename Action, typename Skip, typename... Context>
  absl::Status operator()(Action&& action, ABSL_ATTRIBUTE_UNUSED Skip&& skip,
                          Context&... context) const {
    if (absl::Status status = std::forward<Action>(action)();
        ABSL_PREDICT_FALSE(!status.ok())) {
      return status;
    }
    fields_found_internal::NumbersFoundFromContext(context...)
        .template at<field_number>() = true;
    return absl::OkStatus();
  }
};

template <int field_number>
struct UntilFieldFoundActionWrapper {
  template <typename Action, typename Skip, typename... Context>
  absl::Status operator()(Action&& action, Skip&& skip,
                          Context&... context) const {
    if (fields_found_internal::NumbersFoundFromContext(context...)
            .template at<field_number>()) {
      return std::forward<Skip>(skip)();
    }
    return std::forward<Action>(action)();
  }
};

template <int field_number>
struct UntilFieldFoundThenCancelActionWrapper {
  template <typename Action, typename Skip, typename... Context>
  absl::Status operator()(Action&& action, Skip&& skip,
                          Context&... context) const {
    if constexpr (std::tuple_element_t<
                      fields_found_internal::NumbersFoundIndexFromContext<
                          Context...>(),
                      std::tuple<Context...>>::size() > 1) {
      if (fields_found_internal::NumbersFoundFromContext(context...)
              .template at<field_number>()) {
        return std::forward<Skip>(skip)();
      }
    }
    if (absl::Status status = std::forward<Action>(action)();
        ABSL_PREDICT_FALSE(!status.ok())) {
      return status;
    }
    if (fields_found_internal::NumbersFoundFromContext(context...).all()) {
      return absl::CancelledError();
    }
    return absl::OkStatus();
  }
};

template <int field_number>
struct OnlyFirstFieldActionWrapper {
  template <typename Action, typename Skip, typename... Context>
  absl::Status operator()(Action&& action, Skip&& skip,
                          Context&... context) const {
    if (fields_found_internal::NumbersFoundFromContext(context...)
            .template at<field_number>()) {
      return std::forward<Skip>(skip)();
    }
    if (absl::Status status = std::forward<Action>(action)();
        ABSL_PREDICT_FALSE(!status.ok())) {
      return status;
    }
    fields_found_internal::NumbersFoundFromContext(context...)
        .template at<field_number>() = true;
    return absl::OkStatus();
  }
};

template <int field_number>
struct OnlyFirstFieldThenCancelActionWrapper {
  template <typename Action, typename Skip, typename... Context>
  absl::Status operator()(Action&& action, Skip&& skip,
                          Context&... context) const {
    if constexpr (std::tuple_element_t<
                      fields_found_internal::NumbersFoundIndexFromContext<
                          Context...>(),
                      std::tuple<Context...>>::size() > 1) {
      if (fields_found_internal::NumbersFoundFromContext(context...)
              .template at<field_number>()) {
        return std::forward<Skip>(skip)();
      }
    }
    if (absl::Status status = std::forward<Action>(action)();
        ABSL_PREDICT_FALSE(!status.ok())) {
      return status;
    }
    fields_found_internal::NumbersFoundFromContext(context...)
        .template at<field_number>() = true;
    if constexpr (std::tuple_element_t<
                      fields_found_internal::NumbersFoundIndexFromContext<
                          Context...>(),
                      std::tuple<Context...>>::size() > 1) {
      if (fields_found_internal::NumbersFoundFromContext(context...).all()) {
        return absl::CancelledError();
      }
      return absl::OkStatus();
    } else {
      return absl::CancelledError();
    }
  }
};

}  // namespace riegeli

#endif  // RIEGELI_MESSAGES_FIELDS_FOUND_H_
