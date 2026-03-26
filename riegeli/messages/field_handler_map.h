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

#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/container/flat_hash_map.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/cord_iterator_span.h"
#include "riegeli/base/hybrid_direct_map.h"
#include "riegeli/base/initializer.h"
#include "riegeli/bytes/limiting_reader.h"
#include "riegeli/messages/serialized_message_reader.h"
#include "riegeli/messages/serialized_message_reader_internal.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

template <typename... Context>
class FieldHandlerMapBuilder;

// A map of field handlers for `SerializedMessageReader` registered at runtime.
//
// A `FieldHandlerMap` is itself a dynamic field handler, used with
// `SerializedMessageReader` with compatible `Context...`.
//
// `FieldHandlerMap` is created from `FieldHandlerMap::Builder`, with which
// field handlers are registered.
//
// Registered field handlers must be unbound, not yet associated with a field
// number. The field number is specified during registration.
//
// Field handlers for length-delimited fields must be directly applicable to a
// string source. This makes `FieldHandlerMap` itself directly applicable to a
// string or `Cord` source.
//
// `DynamicFieldHandler` is similar to a `FieldHandlerMap` with a single
// registered field handler, but more efficient and without type erasure.
template <typename... Context>
class FieldHandlerMap {
 private:
  template <typename... Value>
  using FieldAction =
      absl::AnyInvocable<absl::Status(Value..., Context&...) const>;

  struct LengthDelimitedActions;

 public:
  // Prepares a `FieldHandlerMap`.
  using Builder = FieldHandlerMapBuilder<Context...>;

  // Creates an empty `FieldHandlerMap`. Designed for `Reset()`.
  FieldHandlerMap() = default;

  // Builds a `FieldHandlerMap`.
  explicit FieldHandlerMap(Builder&& builder);

  FieldHandlerMap(FieldHandlerMap&& that) = default;
  FieldHandlerMap& operator=(FieldHandlerMap&& that) = default;

  // Makes `*this` equivalent to a newly constructed `FieldHandlerMap`.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset();
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Builder&& builder);

  // Implement the field handler protocol.

  static constexpr int kFieldNumber = kDynamicFieldNumber;

  ABSL_ATTRIBUTE_ALWAYS_INLINE
  const FieldAction<uint64_t>* absl_nullable AcceptVarint(
      int field_number) const {
    return varint_handlers_.Find(field_number);
  }

  absl::Status DynamicHandleVarint(const FieldAction<uint64_t>& handler,
                                   uint64_t repr, Context&... context) const {
    return handler(repr, context...);
  }

  ABSL_ATTRIBUTE_ALWAYS_INLINE
  const FieldAction<uint32_t>* absl_nullable AcceptFixed32(
      int field_number) const {
    return fixed32_handlers_.Find(field_number);
  }

  absl::Status DynamicHandleFixed32(const FieldAction<uint32_t>& handler,
                                    uint32_t repr, Context&... context) const {
    return handler(repr, context...);
  }

  ABSL_ATTRIBUTE_ALWAYS_INLINE
  const FieldAction<uint64_t>* absl_nullable AcceptFixed64(
      int field_number) const {
    return fixed64_handlers_.Find(field_number);
  }

  absl::Status DynamicHandleFixed64(const FieldAction<uint64_t>& handler,
                                    uint64_t repr, Context&... context) const {
    return handler(repr, context...);
  }

  ABSL_ATTRIBUTE_ALWAYS_INLINE
  const LengthDelimitedActions* absl_nullable AcceptLengthDelimited(
      int field_number) const {
    return length_delimited_handlers_.Find(field_number);
  }

  absl::Status DynamicHandleLengthDelimitedFromReader(
      const LengthDelimitedActions& handler, ReaderSpan<> repr,
      Context&... context) const {
    return handler.action_from_reader(std::move(repr), context...);
  }

  absl::Status DynamicHandleLengthDelimitedFromCord(
      const LengthDelimitedActions& handler, CordIteratorSpan repr,
      std::string& scratch, Context&... context) const {
    return handler.action_from_cord(std::move(repr), scratch, context...);
  }

  absl::Status DynamicHandleLengthDelimitedFromString(
      const LengthDelimitedActions& handler, absl::string_view repr,
      Context&... context) const {
    return handler.action_from_string(repr, context...);
  }

  ABSL_ATTRIBUTE_ALWAYS_INLINE
  const FieldAction<>* absl_nullable AcceptStartGroup(int field_number) const {
    return start_group_handlers_.Find(field_number);
  }

  absl::Status DynamicHandleStartGroup(const FieldAction<>& handler,
                                       Context&... context) const {
    return handler(context...);
  }

  ABSL_ATTRIBUTE_ALWAYS_INLINE
  const FieldAction<>* absl_nullable AcceptEndGroup(int field_number) const {
    return end_group_handlers_.Find(field_number);
  }

  absl::Status DynamicHandleEndGroup(const FieldAction<>& handler,
                                     Context&... context) const {
    return handler(context...);
  }

 private:
  // For `FieldAction` and `LengthDelimitedActions`.
  friend class FieldHandlerMapBuilder<Context...>;

  HybridDirectMap<int, FieldAction<uint64_t>, 1> varint_handlers_;
  HybridDirectMap<int, FieldAction<uint32_t>, 1> fixed32_handlers_;
  HybridDirectMap<int, FieldAction<uint64_t>, 1> fixed64_handlers_;
  HybridDirectMap<int, LengthDelimitedActions, 1> length_delimited_handlers_;
  HybridDirectMap<int, FieldAction<>, 1> start_group_handlers_;
  HybridDirectMap<int, FieldAction<>, 1> end_group_handlers_;
};

template <typename... Context>
explicit FieldHandlerMap(FieldHandlerMapBuilder<Context...>&& builder)
    -> FieldHandlerMap<Context...>;

template <typename... Context>
class FieldHandlerMapBuilder {
 public:
  // Creates an empty `Builder`.
  FieldHandlerMapBuilder() = default;

  FieldHandlerMapBuilder(FieldHandlerMapBuilder&& that) = default;
  FieldHandlerMapBuilder& operator=(FieldHandlerMapBuilder&& that) = default;

  // Makes `*this` equivalent to a newly constructed `Builder`.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset();

  // Registers a field handler for a field with the given `field_number`.
  // The field handler must be unbound, not yet associated with a field number.
  // A field handler for a length-delimited field must be directly applicable
  // to a string source.
  //
  // Returns `true` if successful, or `false` if `field_number` was already
  // registered.
  template <typename FieldHandler,
            std::enable_if_t<IsUnboundFieldHandlerFromString<
                                 TargetT<FieldHandler>, Context...>::value,
                             int> = 0>
  bool RegisterField(int field_number, const FieldHandler& field_handler);

 private:
  friend class FieldHandlerMap<Context...>;  // For member variables.

  template <typename... Value>
  using FieldAction =
      typename FieldHandlerMap<Context...>::template FieldAction<Value...>;
  using LengthDelimitedActions =
      typename FieldHandlerMap<Context...>::LengthDelimitedActions;

  absl::flat_hash_map<int, FieldAction<uint64_t>> varint_handlers_;
  absl::flat_hash_map<int, FieldAction<uint32_t>> fixed32_handlers_;
  absl::flat_hash_map<int, FieldAction<uint64_t>> fixed64_handlers_;
  absl::flat_hash_map<int, LengthDelimitedActions> length_delimited_handlers_;
  absl::flat_hash_map<int, FieldAction<>> start_group_handlers_;
  absl::flat_hash_map<int, FieldAction<>> end_group_handlers_;
};

// Implementation details follow.

template <typename... Context>
void FieldHandlerMapBuilder<Context...>::Reset() {
  varint_handlers_.clear();
  fixed32_handlers_.clear();
  fixed64_handlers_.clear();
  length_delimited_handlers_.clear();
  start_group_handlers_.clear();
  end_group_handlers_.clear();
}

template <typename... Context>
template <typename FieldHandler,
          std::enable_if_t<IsUnboundFieldHandlerFromString<
                               TargetT<FieldHandler>, Context...>::value,
                           int>>
bool FieldHandlerMapBuilder<Context...>::RegisterField(
    int field_number, const FieldHandler& field_handler) {
  bool all_registered = true;
  if constexpr (serialized_message_reader_internal::
                    IsStaticFieldHandlerForVarint<TargetT<FieldHandler>,
                                                  Context...>::value) {
    if (ABSL_PREDICT_FALSE(
            !varint_handlers_
                 .try_emplace(
                     field_number,
                     [field_handler = TargetT<FieldHandler>(field_handler)](
                         uint64_t repr, Context&... context) {
                       return field_handler.HandleVarint(repr, context...);
                     })
                 .second)) {
      all_registered = false;
    }
  }
  if constexpr (serialized_message_reader_internal::
                    IsStaticFieldHandlerForFixed32<TargetT<FieldHandler>,
                                                   Context...>::value) {
    if (ABSL_PREDICT_FALSE(
            !fixed32_handlers_
                 .try_emplace(
                     field_number,
                     [field_handler = TargetT<FieldHandler>(field_handler)](
                         uint32_t repr, Context&... context) {
                       return field_handler.HandleFixed32(repr, context...);
                     })
                 .second)) {
      all_registered = false;
    }
  }
  if constexpr (serialized_message_reader_internal::
                    IsStaticFieldHandlerForFixed64<TargetT<FieldHandler>,
                                                   Context...>::value) {
    if (ABSL_PREDICT_FALSE(
            !fixed64_handlers_
                 .try_emplace(
                     field_number,
                     [field_handler = TargetT<FieldHandler>(field_handler)](
                         uint64_t repr, Context&... context) {
                       return field_handler.HandleFixed64(repr, context...);
                     })
                 .second)) {
      all_registered = false;
    }
  }
  if constexpr (serialized_message_reader_internal::
                    IsStaticFieldHandlerForLengthDelimitedFromString<
                        TargetT<FieldHandler>, Context...>::value) {
    if (ABSL_PREDICT_FALSE(
            !length_delimited_handlers_
                 .try_emplace(
                     field_number,
                     [field_handler = TargetT<FieldHandler>(field_handler)](
                         ReaderSpan<> repr, Context&... context) {
                       if constexpr (
                           serialized_message_reader_internal::
                               IsStaticFieldHandlerForLengthDelimitedFromReader<
                                   TargetT<FieldHandler>, Context...>::value) {
                         return field_handler.HandleLengthDelimitedFromReader(
                             std::move(repr), context...);
                       } else {
                         absl::string_view value;
                         if (ABSL_PREDICT_FALSE(!repr.reader().Read(
                                 IntCast<size_t>(repr.length()), value))) {
                           return serialized_message_reader_internal::
                               ReadLengthDelimitedValueError(repr.reader());
                         }
                         return field_handler.HandleLengthDelimitedFromString(
                             value, context...);
                       }
                     },
                     [field_handler = TargetT<FieldHandler>(field_handler)](
                         CordIteratorSpan repr, std::string& scratch,
                         Context&... context) {
                       if constexpr (
                           serialized_message_reader_internal::
                               IsStaticFieldHandlerForLengthDelimitedFromCord<
                                   TargetT<FieldHandler>, Context...>::value) {
                         return field_handler.HandleLengthDelimitedFromCord(
                             std::move(repr), scratch, context...);
                       } else {
                         return field_handler.HandleLengthDelimitedFromString(
                             std::move(repr).ToStringView(scratch), context...);
                       }
                     },
                     [field_handler = TargetT<FieldHandler>(field_handler)](
                         absl::string_view repr, Context&... context) {
                       return field_handler.HandleLengthDelimitedFromString(
                           repr, context...);
                     })
                 .second)) {
      all_registered = false;
    }
  }
  if constexpr (serialized_message_reader_internal::
                    IsStaticFieldHandlerForStartGroup<TargetT<FieldHandler>,
                                                      Context...>::value) {
    if (ABSL_PREDICT_FALSE(
            !start_group_handlers_
                 .try_emplace(field_number,
                              [field_handler = TargetT<FieldHandler>(
                                   field_handler)](Context&... context) {
                                return field_handler.HandleStartGroup(
                                    context...);
                              })
                 .second)) {
      all_registered = false;
    }
  }
  if constexpr (serialized_message_reader_internal::
                    IsStaticFieldHandlerForEndGroup<TargetT<FieldHandler>,
                                                    Context...>::value) {
    if (ABSL_PREDICT_FALSE(
            !end_group_handlers_
                 .try_emplace(field_number,
                              [field_handler = TargetT<FieldHandler>(
                                   field_handler)](Context&... context) {
                                return field_handler.HandleEndGroup(context...);
                              })
                 .second)) {
      all_registered = false;
    }
  }
  return all_registered;
}

template <typename... Context>
struct FieldHandlerMap<Context...>::LengthDelimitedActions {
#if !__cpp_aggregate_paren_init
  LengthDelimitedActions() = default;

  template <
      typename ActionFromReader, typename ActionFromCord,
      typename ActionFromString,
      std::enable_if_t<
          std::conjunction_v<
              std::is_convertible<ActionFromReader&&,
                                  FieldAction<ReaderSpan<>>>,
              std::is_convertible<ActionFromCord&&,
                                  FieldAction<CordIteratorSpan, std::string&>>,
              std::is_convertible<ActionFromString&&,
                                  FieldAction<absl::string_view>>>,
          int> = 0>
  explicit LengthDelimitedActions(ActionFromReader&& action_from_reader,
                                  ActionFromCord&& action_from_cord,
                                  ActionFromString&& action_from_string)
      : action_from_reader(std::forward<ActionFromReader>(action_from_reader)),
        action_from_cord(std::forward<ActionFromCord>(action_from_cord)),
        action_from_string(std::forward<ActionFromString>(action_from_string)) {
  }

  LengthDelimitedActions(LengthDelimitedActions&& that) = default;
  LengthDelimitedActions& operator=(LengthDelimitedActions&& that) = default;
#endif

  FieldAction<ReaderSpan<>> action_from_reader;
  FieldAction<CordIteratorSpan, std::string&> action_from_cord;
  FieldAction<absl::string_view> action_from_string;
};

template <typename... Context>
FieldHandlerMap<Context...>::FieldHandlerMap(Builder&& builder)
    : varint_handlers_(std::move(builder.varint_handlers_)),
      fixed32_handlers_(std::move(builder.fixed32_handlers_)),
      fixed64_handlers_(std::move(builder.fixed64_handlers_)),
      length_delimited_handlers_(std::move(builder.length_delimited_handlers_)),
      start_group_handlers_(std::move(builder.start_group_handlers_)),
      end_group_handlers_(std::move(builder.end_group_handlers_)) {}

template <typename... Context>
void FieldHandlerMap<Context...>::Reset() {
  varint_handlers_.Reset();
  fixed32_handlers_.Reset();
  fixed64_handlers_.Reset();
  length_delimited_handlers_.Reset();
  start_group_handlers_.Reset();
  end_group_handlers_.Reset();
}

template <typename... Context>
void FieldHandlerMap<Context...>::Reset(Builder&& builder) {
  varint_handlers_.Reset(std::move(builder.varint_handlers_));
  fixed32_handlers_.Reset(std::move(builder.fixed32_handlers_));
  fixed64_handlers_.Reset(std::move(builder.fixed64_handlers_));
  length_delimited_handlers_.Reset(
      std::move(builder.length_delimited_handlers_));
  start_group_handlers_.Reset(std::move(builder.start_group_handlers_));
  end_group_handlers_.Reset(std::move(builder.end_group_handlers_));
}

}  // namespace riegeli

#endif  // RIEGELI_MESSAGES_FIELD_HANDLER_MAP_H_
