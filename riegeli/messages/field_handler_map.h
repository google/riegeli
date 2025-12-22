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
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/container/flat_hash_map.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/bytes/limiting_reader.h"
#include "riegeli/messages/serialized_message_reader2.h"
#include "riegeli/messages/serialized_message_reader_internal.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

template <typename Associated, typename... Context>
class FieldHandlerMapImpl;

// A map of field handlers for `SerializedMessageReader2` registered at runtime.
//
// A `FieldHandlerMap` is itself a dynamic field handler, used with
// `SerializedMessageReader2` with compatible `Context...`.
//
// Registered field handlers must be unbound, not yet associated with a field
// number. The field number is specified during registration.
//
// A handler for a length-delimited field holds an optional `FieldHandlerMap`
// for its children. A `FieldHandlerMap` is effectively a tree of field
// handlers.
//
// `FieldHandlerMap<Context...>::With<Associated>` is a `FieldHandlerMap`
// with some `Associated` data. Having these data in the `FieldHandlerMap`,
// including children of a length-delimited field, can avoid maintaining
// a parallel map of associated data. `void` indicates no associated data.
// Otherwise `Associated` must be default-constructible.
//
// `FieldHandlerMap` is not directly applicable to a string source.
// If `SerializedMessageReader2::ReadMessage()` is called with a string, the
// source is wrapped in a `StringReader`.

template <typename... Context>
using FieldHandlerMap = FieldHandlerMapImpl<void, Context...>;

template <typename Associated, typename... Context>
class FieldHandlerMapImpl {
 private:
  template <typename... Value>
  using FieldAction =
      absl::AnyInvocable<absl::Status(Value..., Context&...) const>;

  struct LengthDelimitedHandler {
    template <typename Action,
              std::enable_if_t<std::is_invocable_r_v<
                                   absl::Status, const Action&,
                                   const FieldHandlerMapImpl* absl_nullable,
                                   ReaderSpan<>, Context&...>,
                               int> = 0>
    explicit LengthDelimitedHandler(Action&& action)
        : action(std::forward<Action>(action)) {}

    FieldAction<const FieldHandlerMapImpl* absl_nullable, ReaderSpan<>> action;
    absl_nullable std::unique_ptr<FieldHandlerMapImpl> children;
  };

  struct DefaultParentAction;

 public:
  template <typename OtherAssociated>
  using With = FieldHandlerMapImpl<OtherAssociated, Context...>;

  // Field number and associated data for a registered parent submessage.
  // See `RegisterParent()`.
  struct Submessage {
    int field_number;
    FieldHandlerMapImpl* children;
  };

  // Creates an empty `FieldHandlerMap` with value-initialized `associated()`.
  FieldHandlerMapImpl() = default;

  FieldHandlerMapImpl(FieldHandlerMapImpl&& that) = default;
  FieldHandlerMapImpl& operator=(FieldHandlerMapImpl&& that) = default;

  // Registers a field handler for a field with the given `field_number`.
  // The field handler must be unbound, not yet associated with a field number.
  //
  // Returns `true` if successful, or `false` if the corresponding field number
  // was already registered.
  template <typename FieldHandler,
            std::enable_if_t<IsUnboundFieldHandler<std::decay_t<FieldHandler>,
                                                   Context...>::value,
                             int> = 0>
  bool RegisterField(int field_number, FieldHandler&& field_handler);

  // Registers an action for a submessage field with the given `field_number`.
  //
  // `parent_action` is invoked with the field number, `const FieldHandlerMap&`
  // for the children, `ReaderSpan<>` with submessage contents, and
  // `Context&...`.
  //
  // The default `parent_action` is:
  // ```
  //   return riegeli::SerializedMessageReader2<Context...>(&children)
  //       .ReadMessage(std::move(repr), context...);
  // ```
  //
  // If the field number was not registered yet, creates a default-constructed
  // `FieldHandlerMap`, registers `parent_action`, and returns a mutable pointer
  // to the `FieldHandlerMap`.
  //
  // If the field number was already registered as a parent submessage,
  // returns a pointer to the existing `FieldHandlerMap`. The newly specified
  // `parent_action` is ignored; it is expected to be the same as the existing
  // action.
  //
  // If the field number was already registered as a regular length-delimited
  // field, returns `nullptr`.
  template <
      typename ParentAction,
      std::enable_if_t<std::is_invocable_r_v<absl::Status, const ParentAction&,
                                             int, const FieldHandlerMapImpl&,
                                             ReaderSpan<>, Context&...>,
                       int> = 0>
  FieldHandlerMapImpl* absl_nullable RegisterParent(
      int field_number, ParentAction&& parent_action = DefaultParentAction());

  // If `Associated` is not `void`, returns `Associated` data of this map.
  template <typename DependentAssociated = Associated,
            std::enable_if_t<!std::is_void_v<DependentAssociated>, int> = 0>
  DependentAssociated& associated() ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return associated_;
  }
  template <typename DependentAssociated = Associated,
            std::enable_if_t<!std::is_void_v<DependentAssociated>, int> = 0>
  const DependentAssociated& associated() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return associated_;
  }

  // Returns field numbers and associated data for all parent submessages
  // registered so far. See `RegisterParent()`.
  absl::Span<const Submessage> submessages() const { return submessages_; }

  // Returns `true` if no handlers are registered.
  bool empty() const {
    return varint_handlers_.empty() && fixed32_handlers_.empty() &&
           fixed64_handlers_.empty() && length_delimited_handlers_.empty() &&
           start_group_handlers_.empty() && end_group_handlers_.empty();
  }

  // Implement the field handler protocol.

  static constexpr int kFieldNumber = kDynamicFieldNumber;

  const FieldAction<uint64_t>* absl_nullable AcceptVarint(
      int field_number) const {
    const auto iter = varint_handlers_.find(field_number);
    if (iter == varint_handlers_.end()) return nullptr;
    return &iter->second;
  }

  absl::Status DynamicHandleVarint(const FieldAction<uint64_t>& handler,
                                   uint64_t repr, Context&... context) const {
    return handler(repr, context...);
  }

  const FieldAction<uint32_t>* absl_nullable AcceptFixed32(
      int field_number) const {
    const auto iter = fixed32_handlers_.find(field_number);
    if (iter == fixed32_handlers_.end()) return nullptr;
    return &iter->second;
  }

  absl::Status DynamicHandleFixed32(const FieldAction<uint32_t>& handler,
                                    uint32_t repr, Context&... context) const {
    return handler(repr, context...);
  }

  const FieldAction<uint64_t>* absl_nullable AcceptFixed64(
      int field_number) const {
    const auto iter = fixed64_handlers_.find(field_number);
    if (iter == fixed64_handlers_.end()) return nullptr;
    return &iter->second;
  }

  absl::Status DynamicHandleFixed64(const FieldAction<uint64_t>& handler,
                                    uint64_t repr, Context&... context) const {
    return handler(repr, context...);
  }

  const LengthDelimitedHandler* absl_nullable AcceptLengthDelimited(
      int field_number) const {
    const auto iter = length_delimited_handlers_.find(field_number);
    if (iter == length_delimited_handlers_.end()) return nullptr;
    return &iter->second;
  }

  absl::Status DynamicHandleLengthDelimitedFromReader(
      const LengthDelimitedHandler& handler, ReaderSpan<> repr,
      Context&... context) const {
    return handler.action(handler.children.get(), std::move(repr), context...);
  }

  const FieldAction<>* absl_nullable AcceptStartGroup(int field_number) const {
    const auto iter = start_group_handlers_.find(field_number);
    if (iter == start_group_handlers_.end()) return nullptr;
    return &iter->second;
  }

  absl::Status DynamicHandleStartGroup(const FieldAction<>& handler,
                                       Context&... context) const {
    return handler(context...);
  }

  const FieldAction<>* absl_nullable AcceptEndGroup(int field_number) const {
    const auto iter = end_group_handlers_.find(field_number);
    if (iter == end_group_handlers_.end()) return nullptr;
    return &iter->second;
  }

  absl::Status DynamicHandleEndGroup(const FieldAction<>& handler,
                                     Context&... context) const {
    return handler(context...);
  }

 private:
  struct Empty {};

  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS
  std::conditional_t<std::is_void_v<Associated>, Empty, Associated>
      associated_{};
  absl::flat_hash_map<int, FieldAction<uint64_t>> varint_handlers_;
  absl::flat_hash_map<int, FieldAction<uint32_t>> fixed32_handlers_;
  absl::flat_hash_map<int, FieldAction<uint64_t>> fixed64_handlers_;
  absl::flat_hash_map<int, LengthDelimitedHandler> length_delimited_handlers_;
  absl::flat_hash_map<int, FieldAction<>> start_group_handlers_;
  absl::flat_hash_map<int, FieldAction<>> end_group_handlers_;
  std::vector<Submessage> submessages_;
};

// Implementation details follow.

template <typename Associated, typename... Context>
struct FieldHandlerMapImpl<Associated, Context...>::DefaultParentAction {
  absl::Status operator()(ABSL_ATTRIBUTE_UNUSED int field_number,
                          const FieldHandlerMapImpl& children,
                          ReaderSpan<> repr, Context&... context) const {
    return riegeli::SerializedMessageReader2<Context...>(&children).ReadMessage(
        std::move(repr), context...);
  }
};

template <typename Associated, typename... Context>
template <typename FieldHandler,
          std::enable_if_t<IsUnboundFieldHandler<std::decay_t<FieldHandler>,
                                                 Context...>::value,
                           int>>
bool FieldHandlerMapImpl<Associated, Context...>::RegisterField(
    int field_number, FieldHandler&& field_handler) {
  bool all_registered = true;
  if constexpr (serialized_message_reader_internal::
                    IsStaticFieldHandlerForVarint<std::decay_t<FieldHandler>,
                                                  Context...>::value) {
    if (ABSL_PREDICT_FALSE(
            !varint_handlers_
                 .try_emplace(
                     field_number,
                     [field_handler](uint64_t repr, Context&... context) {
                       return field_handler.HandleVarint(repr, context...);
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
                     [field_handler](uint32_t repr, Context&... context) {
                       return field_handler.HandleFixed32(repr, context...);
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
                     [field_handler](uint64_t repr, Context&... context) {
                       return field_handler.HandleFixed64(repr, context...);
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
                     [field_handler](
                         ABSL_ATTRIBUTE_UNUSED const FieldHandlerMapImpl
                             * absl_nullable children,
                         ReaderSpan<> repr, Context&... context) {
                       return field_handler.HandleLengthDelimitedFromReader(
                           std::move(repr), context...);
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
                     [field_handler](
                         ABSL_ATTRIBUTE_UNUSED const FieldHandlerMapImpl
                             * absl_nullable children,
                         ReaderSpan<> repr, Context&... context) {
                       absl::string_view value;
                       if (ABSL_PREDICT_FALSE(!repr.reader().Read(
                               IntCast<size_t>(repr.length()), value))) {
                         return serialized_message_reader_internal::
                             ReadLengthDelimitedValueError(repr.reader());
                       }
                       return field_handler.HandleLengthDelimitedFromString(
                           value, context...);
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

template <typename Associated, typename... Context>
template <
    typename ParentAction,
    std::enable_if_t<std::is_invocable_r_v<
                         absl::Status, const ParentAction&, int,
                         const FieldHandlerMapImpl<Associated, Context...>&,
                         ReaderSpan<>, Context&...>,
                     int>>
auto FieldHandlerMapImpl<Associated, Context...>::RegisterParent(
    int field_number, ParentAction&& parent_action)
    -> FieldHandlerMapImpl* absl_nullable {
  auto inserted = length_delimited_handlers_.try_emplace(
      field_number,
      [field_number, parent_action = std::forward<ParentAction>(parent_action)](
          const FieldHandlerMapImpl* absl_nullable children, ReaderSpan<> repr,
          Context&... context) {
        RIEGELI_ASSERT(children != nullptr)
            << "children must have been initialized "
               "before parent_action can be invoked";
        return SkipLengthDelimitedFromReader(repr, [&] {
          return parent_action(field_number, *children, std::move(repr),
                               context...);
        });
      });
  if (inserted.second) {
    inserted.first->second.children = std::make_unique<FieldHandlerMapImpl>();
    submessages_.push_back(
        Submessage{field_number, inserted.first->second.children.get()});
  }
  return inserted.first->second.children.get();
}

}  // namespace riegeli

#endif  // RIEGELI_MESSAGES_FIELD_HANDLER_MAP_H_
