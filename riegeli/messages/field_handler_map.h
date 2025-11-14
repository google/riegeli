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

#ifndef RIEGELI_MESSAGES_FIELD_HANDLER_MAP_H_
#define RIEGELI_MESSAGES_FIELD_HANDLER_MAP_H_

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <tuple>
#include <type_traits>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/container/flat_hash_map.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/bytes/limiting_reader.h"
#include "riegeli/messages/serialized_message_reader2.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

// A map of field handlers for `SerializedMessageReader2` registered at runtime.
//
// A pointer to a `FieldHandlerMap` is itself a dynamic field handler, to be
// used with `SerializedMessageReader2` with compatible `Context` types.
//
// Registered field handlers must be unbound, not yet associated with a field
// number. The field number is specified during registration.
//
// An action for a parent submessage field can be associated with some
// `ParentState` initialized during registration. `ParentState` always includes
// a `FieldHandlerMap` for children of the submessage, and may include values
// of other types specified as `ExtraParentState...`. They must be
// default-constructible. These type parameters of `FieldHandlerMap` are wrapped
// in `std::tuple` to distinguish them from `Context...`.
//
// `FieldHandlerMap` is not directly applicable to a string source.
// If `SerializedMessageReader2::Read()` is called with a string, the source
// is wrapped in a `StringReader`.

template <typename ExtraParentState, typename... Context>
class FieldHandlerMap;

template <typename... ExtraParentState, typename... Context>
class FieldHandlerMap<std::tuple<ExtraParentState...>, Context...> {
 public:
  // The state associated with a parent submessage field.
  using ParentState = std::tuple<FieldHandlerMap, ExtraParentState...>;

 private:
  template <typename... Value>
  using FieldAction =
      absl::AnyInvocable<absl::Status(Value..., Context&...) const>;

  struct LengthDelimitedHandler {
    template <
        typename Action,
        std::enable_if_t<std::is_invocable_r_v<absl::Status, const Action&,
                                               const ParentState* absl_nullable,
                                               ReaderSpan<>, Context&...>,
                         int> = 0>
    explicit LengthDelimitedHandler(Action&& action)
        : action(std::forward<Action>(action)) {}

    FieldAction<const ParentState* absl_nullable, ReaderSpan<>> action;
    absl_nullable std::unique_ptr<ParentState> parent_state;
  };

  struct DefaultParentAction;

 public:
  FieldHandlerMap() = default;

  FieldHandlerMap(FieldHandlerMap&& that) = default;
  FieldHandlerMap& operator=(FieldHandlerMap&& that) = default;

  // Registers a field handler for a field with the given `field_number`.
  // The field handler must be unbound, not yet associated with a field number.
  //
  // Returns `true` if successful, or `false` if the corresponding field number
  // was already registered.
  template <typename FieldHandler,
            std::enable_if_t<IsUnboundFieldHandler<std::decay_t<FieldHandler>,
                                                   Context...>::value,
                             int> = 0>
  bool Register(int field_number, FieldHandler&& field_handler);

  // Registers a length-delimited field as a parent submessage, creating the
  // associated `ParentState`, and registering an action for the field.
  //
  // `parent_action` is invoked with the field number, `const FieldHandlerMap&`
  // for the children, `const ExtraParentState&...`, `ReaderSpan<>` with
  // submessage contents, and `Context&...`. It must read to the end of the
  // `ReaderSpan<>` or fail.
  //
  // The default `parent_action` is:
  // ```
  //   return riegeli::SerializedMessageReader2<Context...>(&children).Read(
  //       value, context...);
  // ```
  //
  // If the field number was not registered yet, creates a default-constructed
  // `ParentState`, registers `parent_action`, and returns a mutable pointer
  // to the `ParentState`.
  //
  // If the field number was already registered as a parent submessage,
  // returns a pointer to the existing `ParentState`. The newly specified
  // `parent_action` is ignored; it is expected to be the same as the existing
  // action.
  //
  // If the field number was already registered as a regular length-delimited
  // field, returns `nullptr`.
  template <
      typename ParentAction,
      std::enable_if_t<
          std::is_invocable_r_v<
              absl::Status, const ParentAction&, int, const FieldHandlerMap&,
              const ExtraParentState&..., ReaderSpan<>, Context&...>,
          int> = 0>
  ParentState* absl_nullable RegisterParent(
      int field_number, ParentAction&& parent_action = DefaultParentAction());

  // Implement the field handler protocol.

  static constexpr int kFieldNumber = kDynamicFieldNumber;

  const FieldAction<uint64_t>* absl_nullable AcceptVarint(
      int field_number) const {
    const auto iter = varint_handlers_.find(field_number);
    if (iter == varint_handlers_.end()) return nullptr;
    return &iter->second;
  }

  absl::Status HandleVarint(const FieldAction<uint64_t>& handler,
                            uint64_t value, Context&... context) const {
    return handler(value, context...);
  }

  const FieldAction<uint32_t>* absl_nullable AcceptFixed32(
      int field_number) const {
    const auto iter = fixed32_handlers_.find(field_number);
    if (iter == fixed32_handlers_.end()) return nullptr;
    return &iter->second;
  }

  absl::Status HandleFixed32(const FieldAction<uint32_t>& handler,
                             uint32_t value, Context&... context) const {
    return handler(value, context...);
  }

  const FieldAction<uint64_t>* absl_nullable AcceptFixed64(
      int field_number) const {
    const auto iter = fixed64_handlers_.find(field_number);
    if (iter == fixed64_handlers_.end()) return nullptr;
    return &iter->second;
  }

  absl::Status HandleFixed64(const FieldAction<uint64_t>& handler,
                             uint64_t value, Context&... context) const {
    return handler(value, context...);
  }

  const LengthDelimitedHandler* absl_nullable AcceptLengthDelimited(
      int field_number) const {
    const auto iter = length_delimited_handlers_.find(field_number);
    if (iter == length_delimited_handlers_.end()) return nullptr;
    return &iter->second;
  }

  absl::Status HandleLengthDelimitedFromReader(
      const LengthDelimitedHandler& handler, ReaderSpan<> value,
      Context&... context) const {
    return handler.action(handler.parent_state.get(), value, context...);
  }

  const FieldAction<>* absl_nullable AcceptStartGroup(int field_number) const {
    const auto iter = start_group_handlers_.find(field_number);
    if (iter == start_group_handlers_.end()) return nullptr;
    return &iter->second;
  }

  absl::Status HandleStartGroup(const FieldAction<>& handler,
                                Context&... context) const {
    return handler(context...);
  }

  const FieldAction<>* absl_nullable AcceptEndGroup(int field_number) const {
    const auto iter = end_group_handlers_.find(field_number);
    if (iter == end_group_handlers_.end()) return nullptr;
    return &iter->second;
  }

  absl::Status HandleEndGroup(const FieldAction<>& handler,
                              Context&... context) const {
    return handler(context...);
  }

 private:
  absl::flat_hash_map<int, FieldAction<uint64_t>> varint_handlers_;
  absl::flat_hash_map<int, FieldAction<uint32_t>> fixed32_handlers_;
  absl::flat_hash_map<int, FieldAction<uint64_t>> fixed64_handlers_;
  absl::flat_hash_map<int, LengthDelimitedHandler> length_delimited_handlers_;
  absl::flat_hash_map<int, FieldAction<>> start_group_handlers_;
  absl::flat_hash_map<int, FieldAction<>> end_group_handlers_;
};

// Implementation details follow.

template <typename... ExtraParentState, typename... Context>
struct FieldHandlerMap<std::tuple<ExtraParentState...>,
                       Context...>::DefaultParentAction {
  absl::Status operator()(ABSL_ATTRIBUTE_UNUSED int field_number,
                          const ParentState& parent_state, ReaderSpan<> value,
                          Context&... context) const {
    return riegeli::SerializedMessageReader2<Context...>(
               std::get<0>(parent_state))
        .Read(value, context...);
  }
};

template <typename... ExtraParentState, typename... Context>
template <typename FieldHandler,
          std::enable_if_t<IsUnboundFieldHandler<std::decay_t<FieldHandler>,
                                                 Context...>::value,
                           int>>
bool FieldHandlerMap<std::tuple<ExtraParentState...>, Context...>::Register(
    int field_number, FieldHandler&& field_handler) {
  bool all_registered = true;
  if constexpr (serialized_message_reader_internal::
                    IsStaticFieldHandlerForVarint<std::decay_t<FieldHandler>,
                                                  Context...>::value) {
    if (ABSL_PREDICT_FALSE(
            !varint_handlers_
                 .try_emplace(
                     field_number,
                     [field_handler](uint64_t value, Context&... context) {
                       return field_handler.HandleVarint(value, context...);
                     })
                 .second)) {
      all_registered = false;
    }
  }
  if constexpr (serialized_message_reader_internal::
                    IsStaticFieldHandlerForFixed32<std::decay_t<FieldHandler>,
                                                   Context...>::value) {
    if (ABSL_PREDICT_FALSE(
            !fixed32_handlers_
                 .try_emplace(
                     field_number,
                     [field_handler](uint32_t value, Context&... context) {
                       return field_handler.HandleFixed32(value, context...);
                     })
                 .second)) {
      all_registered = false;
    }
  }
  if constexpr (serialized_message_reader_internal::
                    IsStaticFieldHandlerForFixed64<std::decay_t<FieldHandler>,
                                                   Context...>::value) {
    if (ABSL_PREDICT_FALSE(
            !fixed64_handlers_
                 .try_emplace(
                     field_number,
                     [field_handler](uint64_t value, Context&... context) {
                       return field_handler.HandleFixed64(value, context...);
                     })
                 .second)) {
      all_registered = false;
    }
  }
  if constexpr (serialized_message_reader_internal::
                    IsStaticFieldHandlerForLengthDelimitedFromReader<
                        std::decay_t<FieldHandler>, Context...>::value) {
    if (ABSL_PREDICT_FALSE(
            !length_delimited_handlers_
                 .try_emplace(
                     field_number,
                     [field_handler](ABSL_ATTRIBUTE_UNUSED const ParentState
                                         * absl_nullable parent_state,
                                     ReaderSpan<> value, Context&... context) {
                       return field_handler.HandleLengthDelimitedFromReader(
                           value, context...);
                     })
                 .second)) {
      all_registered = false;
    }
  } else if constexpr (serialized_message_reader_internal::
                           IsStaticFieldHandlerForLengthDelimitedFromString<
                               std::decay_t<FieldHandler>, Context...>::value) {
    if (ABSL_PREDICT_FALSE(
            !length_delimited_handlers_
                 .try_emplace(
                     field_number,
                     [field_handler](ABSL_ATTRIBUTE_UNUSED const ParentState
                                         * absl_nullable parent_state,
                                     ReaderSpan<> value, Context&... context) {
                       absl::string_view value_string;
                       if (ABSL_PREDICT_FALSE(!value.reader().Read(
                               IntCast<size_t>(value.length()),
                               value_string))) {
                         return serialized_message_reader_internal::
                             ReadLengthDelimitedValueError(value.reader());
                       }
                       return field_handler.HandleLengthDelimitedFromString(
                           value_string, context...);
                     })
                 .second)) {
      all_registered = false;
    }
  }
  if constexpr (serialized_message_reader_internal::
                    IsStaticFieldHandlerForStartGroup<
                        std::decay_t<FieldHandler>, Context...>::value) {
    if (ABSL_PREDICT_FALSE(
            !start_group_handlers_
                 .try_emplace(field_number,
                              [field_handler](Context&... context) {
                                return field_handler.HandleStartGroup(
                                    context...);
                              })
                 .second)) {
      all_registered = false;
    }
  }
  if constexpr (serialized_message_reader_internal::
                    IsStaticFieldHandlerForEndGroup<std::decay_t<FieldHandler>,
                                                    Context...>::value) {
    if (ABSL_PREDICT_FALSE(
            !end_group_handlers_
                 .try_emplace(field_number,
                              [field_handler](Context&... context) {
                                return field_handler.HandleEndGroup(context...);
                              })
                 .second)) {
      all_registered = false;
    }
  }
  return all_registered;
}

template <typename... ExtraParentState, typename... Context>
template <
    typename ParentAction,
    std::enable_if_t<
        std::is_invocable_r_v<
            absl::Status, const ParentAction&, int,
            const FieldHandlerMap<std::tuple<ExtraParentState...>, Context...>&,
            const ExtraParentState&..., ReaderSpan<>, Context&...>,
        int>>
auto FieldHandlerMap<std::tuple<ExtraParentState...>,
                     Context...>::RegisterParent(int field_number,
                                                 ParentAction&& parent_action)
    -> ParentState* absl_nullable {
  auto inserted = length_delimited_handlers_.try_emplace(
      field_number,
      [field_number, parent_action = std::forward<ParentAction>(parent_action)](
          const ParentState* absl_nullable parent_state, ReaderSpan<> value,
          Context&... context) {
        RIEGELI_ASSERT(parent_state != nullptr)
            << "parent_state must have been initialized "
               "before parent_action can be invoked";
        return std::apply(
            [&](const FieldHandlerMap& children,
                const ExtraParentState&... extra_parent_state) {
              return parent_action(field_number, children,
                                   extra_parent_state..., value, context...);
            },
            *parent_state);
      });
  if (inserted.second) {
    inserted.first->second.parent_state = std::make_unique<ParentState>();
  }
  return inserted.first->second.parent_state.get();
}

}  // namespace riegeli

#endif  // RIEGELI_MESSAGES_FIELD_HANDLER_MAP_H_
