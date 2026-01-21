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
#include <optional>
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
#include "riegeli/base/assert.h"
#include "riegeli/base/cord_iterator_span.h"
#include "riegeli/base/initializer.h"
#include "riegeli/bytes/limiting_reader.h"
#include "riegeli/messages/serialized_message_reader2.h"
#include "riegeli/messages/serialized_message_reader_internal.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

template <typename... Context>
class FieldHandlerMapBuilder;

// A map of field handlers for `SerializedMessageReader2` registered at runtime.
//
// A `FieldHandlerMap` is itself a dynamic field handler, used with
// `SerializedMessageReader2` with compatible `Context...`.
//
// `FieldHandlerMap` is created from `FieldHandlerMap::Builder`, with which
// field handlers are registered.
//
// Registered field handlers must be unbound, not yet associated with a field
// number. The field number is specified during registration.
//
// Field handlers for length-delimited fields must be directly applicable to a
// string source. This makes `FieldHandlerMap` itself is directly applicable to
// a string and `Cord` source.
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

  // An optimized version of `absl::flat_hash_map<int, Value>` for keys being
  // field numbers.
  template <typename Value>
  class FieldMap;

  FieldMap<FieldAction<uint64_t>> varint_handlers_;
  FieldMap<FieldAction<uint32_t>> fixed32_handlers_;
  FieldMap<FieldAction<uint64_t>> fixed64_handlers_;
  FieldMap<LengthDelimitedActions> length_delimited_handlers_;
  FieldMap<FieldAction<>> start_group_handlers_;
  FieldMap<FieldAction<>> end_group_handlers_;
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

// An optimized version of `absl::flat_hash_map<int, Value>` for keys being
// field numbers.
//
// Stores an initial portion of the map in an array indexed by field number
// minus 1. This relies on the assumption that typical field numbers are small.
//
// Non-positive field numbers are meaningless, but `FieldMap` works correctly
// with them.
template <typename... Context>
template <typename Value>
class FieldHandlerMap<Context...>::FieldMap {
 public:
  FieldMap() = default;

  // Builds `FieldMap` from an `absl::flat_hash_map`.
  explicit FieldMap(absl::flat_hash_map<int, Value>&& map) {
    if (!map.empty()) Optimize(std::move(map));
  }

  FieldMap(FieldMap&& that) noexcept;
  FieldMap& operator=(FieldMap&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `FieldMap`.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset();
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      absl::flat_hash_map<int, Value>&& map) {
    Reset();
    if (!map.empty()) Optimize(std::move(map));
  }

  ABSL_ATTRIBUTE_ALWAYS_INLINE
  const Value* absl_nullable Find(int field_number) const {
    RIEGELI_ASSERT(field_number != kPoisonedNumSmallKeys ||
                   small_map_ != nullptr)
        << "Moved-from FieldHandlerMap";
    if (static_cast<size_t>(field_number - 1) < num_small_keys_) {
      return small_map_[field_number - 1];
    }
    if (ABSL_PREDICT_TRUE(large_map_ == std::nullopt)) return nullptr;
    const auto iter = large_map_->find(field_number);
    if (iter == large_map_->end()) return nullptr;
    return &iter->second;
  }

 private:
  // A moved-from `FieldMap` has `num_small_keys_ == kPoisonedNumSmallKeys`
  // and `small_map_ == nullptr`. In debug mode this asserts against using a
  // moved-from object. In non-debug mode, if the field number is not too large,
  // then this triggers a null pointer dereference with an offset up to 1MB,
  // which is assumed to reliably crash.
  static constexpr size_t kPoisonedNumSmallKeys =
      (size_t{1} << 20) / sizeof(const Value*);

  void Optimize(absl::flat_hash_map<int, Value>&& map);

  // The size of `small_map_`, or `kPoisonedNumSmallKeys` for a moved-from
  // `FieldMap`.
  size_t num_small_keys_ = 0;
  // Indexed by field number minus 1, where the field number is between 1 and
  // `num_small_keys_`. Elements corresponding to registered fields point to
  // elements of `small_values_`. The remaining elements are `nullptr`.
  absl_nullable std::unique_ptr<const Value* absl_nullable[]> small_map_;
  // Stores values for `small_map_`, in no particular order.
  absl_nullable std::unique_ptr<Value[]> small_values_;
  // If not `std::nullopt`, stores the mapping for field numbers too large for
  // `small_map_`.
  std::optional<absl::flat_hash_map<int, Value>> large_map_;
};

template <typename... Context>
template <typename Value>
FieldHandlerMap<Context...>::FieldMap<Value>::FieldMap(FieldMap&& that) noexcept
    : num_small_keys_(
          std::exchange(that.num_small_keys_, kPoisonedNumSmallKeys)),
      small_map_(std::move(that.small_map_)),
      small_values_(std::move(that.small_values_)),
      large_map_(std::move(that.large_map_)) {}

template <typename... Context>
template <typename Value>
auto FieldHandlerMap<Context...>::FieldMap<Value>::operator=(
    FieldMap&& that) noexcept -> FieldMap& {
  num_small_keys_ = std::exchange(that.num_small_keys_, kPoisonedNumSmallKeys);
  small_map_ = std::move(that.small_map_);
  small_values_ = std::move(that.small_values_);
  large_map_ = std::move(that.large_map_);
  return *this;
}

template <typename... Context>
template <typename Value>
void FieldHandlerMap<Context...>::FieldMap<Value>::Reset() {
  num_small_keys_ = 0;
  small_map_.reset();
  small_values_.reset();
  large_map_.reset();
}

template <typename... Context>
template <typename Value>
void FieldHandlerMap<Context...>::FieldMap<Value>::Optimize(
    absl::flat_hash_map<int, Value>&& map) {
  RIEGELI_ASSERT(!map.empty())
      << "Failed precondition of FieldHandlerMap::FieldMap::Optimize(): "
         "an empty map must have been handled before";
  size_t max_key = 0;
  for (const auto& entry : map) {
    max_key = UnsignedMax(max_key, static_cast<size_t>(entry.first - 1));
  }
  // Prevent `small_map_` from wasting too much memory. A field number not
  // larger than 128 is suitable for `small_map_`. If `map` has many elements,
  // `small_map_` can cover more field numbers if it is at least 25% full.
  const size_t max_num_small_keys = UnsignedMax(size_t{128}, map.size() * 4);
  if (max_key < max_num_small_keys) {
    // All field numbers are suitable for `small_map_`. `large_map_` is not
    // used.
    //
    // There is no need for `small_map_` to cover keys larger than `max_key`,
    // because their lookup is fast if `large_map_` is `std::nullopt`.
    num_small_keys_ = max_key + 1;
    small_map_ =
        std::make_unique<const Value* absl_nullable[]>(num_small_keys_);
    small_values_ = std::make_unique<Value[]>(map.size());
    size_t small_values_index = 0;
    for (auto& entry : map) {
      Value* const value = &small_values_[small_values_index++];
      small_map_[IntCast<size_t>(entry.first - 1)] = value;
      *value = std::move(entry.second);
    }
  } else {
    // Some field numbers are too large for `small_map_`. `large_map_` is used.
    //
    // `small_map_` covers all keys below `max_num_small_keys` rather than only
    // to `max_key`, to reduce lookups in `large_map_`.
    num_small_keys_ = max_num_small_keys;
    small_map_ =
        std::make_unique<const Value* absl_nullable[]>(max_num_small_keys);
    size_t num_small_values = 0;
    for (const auto& entry : map) {
      num_small_values +=
          static_cast<size_t>(entry.first - 1) < max_num_small_keys ? 1 : 0;
    }
    if (num_small_values > 0) {
      small_values_ = std::make_unique<Value[]>(num_small_values);
    }
    large_map_.emplace();
    large_map_->reserve(map.size() - num_small_values);
    size_t small_values_index = 0;
    for (auto& entry : map) {
      if (static_cast<size_t>(entry.first - 1) < max_num_small_keys) {
        Value* const value = &small_values_[small_values_index++];
        small_map_[IntCast<size_t>(entry.first - 1)] = value;
        *value = std::move(entry.second);
      } else {
        large_map_->try_emplace(entry.first, std::move(entry.second));
      }
    }
    RIEGELI_ASSERT_EQ(small_values_index, num_small_values)
        << "The whole small_values_ array should have been filled";
  }
#if RIEGELI_DEBUG
  // Detect using a moved-from `FieldHandlerMap::Builder` if using a moved-from
  // `absl::flat_hash_map` is detected.
  ABSL_ATTRIBUTE_UNUSED absl::flat_hash_map<int, Value> moved = std::move(map);
#endif
}

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
