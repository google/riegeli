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

#ifndef RIEGELI_BASE_HYBRID_DIRECT_MAP_H_
#define RIEGELI_BASE_HYBRID_DIRECT_MAP_H_

#include <stddef.h>

#include <functional>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/container/flat_hash_map.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/hybrid_direct_common.h"  // IWYU pragma: export
#include "riegeli/base/hybrid_direct_internal.h"
#include "riegeli/base/iterable.h"
#include "riegeli/base/type_traits.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

namespace hybrid_direct_internal {

// Part of `HybridDirectMap` excluding constructors and assignment. This is
// separated to make copy and move constructors and assignment available
// conditionally.
template <typename Key, typename Value, typename Traits>
class HybridDirectMapImpl {
 public:
  static size_t max_size();

  ABSL_ATTRIBUTE_REINITIALIZES void Reset();

  Value* absl_nullable Find(Key key);
  const Value* absl_nullable Find(Key key) const;

 protected:
  HybridDirectMapImpl() = default;

  HybridDirectMapImpl(const HybridDirectMapImpl& that) noexcept;
  HybridDirectMapImpl& operator=(const HybridDirectMapImpl& that) noexcept;

  HybridDirectMapImpl(HybridDirectMapImpl&& that) = default;
  HybridDirectMapImpl& operator=(HybridDirectMapImpl&& that) = default;

  template <typename Src, typename KeyProjection, typename ValueProjection>
  void Initialize(Src&& src, const KeyProjection& key_projection,
                  const ValueProjection& value_projection,
                  size_t direct_capacity);

 private:
  using RawKey = std::decay_t<decltype(Traits::ToRawKey(std::declval<Key>()))>;
  static_assert(std::is_unsigned_v<RawKey>);

  using DirectValues = SizedArray<DelayedConstructor<Value>>;
  using DirectMap = SizedArray<Value* absl_nullable>;
  using SlowMap = absl::flat_hash_map<RawKey, Value>;

  static constexpr int kInverseMinLoadFactor = 4;  // 25%.

  template <typename Src, typename Iterator, typename KeyProjection,
            typename ValueProjection>
  void Optimize(Iterator first, Iterator last, size_t size,
                const KeyProjection& key_projection,
                const ValueProjection& value_projection,
                size_t direct_capacity);

  absl_nullable DirectValues CopyDirectValues() const;
  absl_nullable DirectMap
  CopyDirectMap(DelayedConstructor<Value>* absl_nullable dest_values) const;
  absl_nullable std::unique_ptr<SlowMap> CopySlowMap() const;

  // Stores values for `direct_map_`, in no particular order.
  absl_nullable DirectValues direct_values_;
  // Indexed by raw key below `direct_map_.get_deleter().size()`. Elements
  // corresponding to present values point to elements of `direct_values_`.
  // The remaining elements are `nullptr`.
  absl_nullable DirectMap direct_map_;
  // If not `nullptr`, stores the mapping for keys too large for `direct_map_`.
  // Uses `std::unique_ptr` rather than `std::optional` to reduce memory usage
  // in the common case when `slow_map_` is not used.
  //
  // Invariant: if `slow_map_ != nullptr` then `!slow_map_->empty()`.
  absl_nullable std::unique_ptr<SlowMap> slow_map_;
};

}  // namespace hybrid_direct_internal

// `HybridDirectMap` is a map optimized for keys being small integers.
// It supports only lookups, but no incremental building nor iteration.
//
// It stores a part of the map covering some range of small keys in an array.
// The remaining keys are stored in an `absl::flat_hash_map`.
//
// `Traits` specifies a mapping of keys to an unsigned integer type. It must
// support at least the following static member:
//
// ```
//   // Translates the key to a raw key, which is an unsigned integer type.
//   // Small raw keys are put in the array.
//   static RawKey ToRawKey(Key key);
// ```
//
// `direct_capacity`, if specified during building, is the intended capacity
// of the array part. The actual capacity can be smaller if all keys fit
// in the array, or larger if the array remains at least 25% full. Default:
// `kHybridDirectDefaultDirectCapacity` (128).
template <typename Key, typename Value,
          typename Traits = HybridDirectTraits<Key>>
class HybridDirectMap
    : public hybrid_direct_internal::HybridDirectMapImpl<Key, Value, Traits>,
      private ConditionallyConstructible<std::is_copy_constructible_v<Value>,
                                         true>,
      private ConditionallyAssignable<std::is_copy_constructible_v<Value>,
                                      true> {
 private:
  template <typename Src, typename Enable = void>
  struct HasCompatibleKeys : std::false_type {};
  template <typename Src>
  struct HasCompatibleKeys<
      Src, std::enable_if_t<std::is_convertible_v<
               decltype(std::declval<ElementTypeT<const Src&>>().first), Key>>>
      : std::true_type {};

  template <typename Src, typename KeyProjection, typename Enable = void>
  struct HasProjectibleKeys : std::false_type {};
  template <typename Src, typename KeyProjection>
  struct HasProjectibleKeys<
      Src, KeyProjection,
      std::enable_if_t<std::is_convertible_v<
          std::invoke_result_t<const KeyProjection&, ElementTypeT<const Src&>>,
          Key>>> : std::true_type {};

  template <typename Src, typename Enable = void>
  struct HasCompatibleValues : std::false_type {};
  template <typename Src>
  struct HasCompatibleValues<
      Src, std::enable_if_t<std::is_convertible_v<
               decltype(std::declval<ElementTypeT<Src>>().second), Value>>>
      : std::true_type {};

  template <typename Src, typename ValueProjection, typename Enable = void>
  struct HasProjectibleValues : std::false_type {};
  template <typename Src, typename ValueProjection>
  struct HasProjectibleValues<
      Src, ValueProjection,
      std::enable_if_t<std::is_convertible_v<
          std::invoke_result_t<const ValueProjection&, ElementTypeT<Src>>,
          Value>>> : std::true_type {};

  template <typename Src>
  struct DefaultKeyProjection {
    Key operator()(ElementTypeT<const Src&> entry) const { return entry.first; }
  };

  template <typename Src>
  struct DefaultValueProjection {
    auto&& operator()(ElementTypeT<Src>&& entry) const {
      return std::forward<ElementTypeT<Src>>(entry).second;
    }
  };

 public:
  // Constructs an empty `HybridDirectMap`.
  HybridDirectMap() = default;

  // Builds `HybridDirectMap` from an iterable `src`. Moves values if `src` is
  // an rvalue which owns its elements.
  template <typename Src,
            std::enable_if_t<
                std::conjunction_v<
                    NotSameRef<HybridDirectMap, Src>, IsForwardIterable<Src>,
                    HasCompatibleKeys<Src>, HasCompatibleValues<Src>>,
                int> = 0>
  explicit HybridDirectMap(Src&& src) {
    this->Initialize(std::forward<Src>(src), DefaultKeyProjection<Src>(),
                     DefaultValueProjection<Src>(),
                     kHybridDirectDefaultDirectCapacity);
  }
  template <typename Src,
            std::enable_if_t<std::conjunction_v<IsForwardIterable<Src>,
                                                HasCompatibleKeys<Src>,
                                                HasCompatibleValues<Src>>,
                             int> = 0>
  explicit HybridDirectMap(Src&& src, size_t direct_capacity) {
    this->Initialize(std::forward<Src>(src), DefaultKeyProjection<Src>(),
                     DefaultValueProjection<Src>(), direct_capacity);
  }

  // Builds `HybridDirectMap` from an initializer list.
  /*implicit*/ HybridDirectMap(
      std::initializer_list<std::pair<Key, Value>> src,
      size_t direct_capacity = kHybridDirectDefaultDirectCapacity) {
    this->Initialize(src, DefaultKeyProjection<decltype(src)>(),
                     DefaultValueProjection<decltype(src)>(), direct_capacity);
  }

  // Builds `HybridDirectMap` from an iterable `src`. Moves values if `src` is
  // an rvalue which owns its elements.
  //
  // Keys and values are extracted using `key_projection()` and
  // `value_projection()` rather than `.first` and `.second`. `key_projection()`
  // may be called multiple times for each entry so it should be efficient.
  // `value_projection()` is called once for each entry so it can be expensive.
  template <
      typename Src, typename KeyProjection = DefaultKeyProjection<Src>,
      std::enable_if_t<
          std::conjunction_v<
              std::negation<std::is_convertible<KeyProjection, size_t>>,
              IsForwardIterable<Src>, HasProjectibleKeys<Src, KeyProjection>,
              HasCompatibleValues<Src>>,
          int> = 0>
  explicit HybridDirectMap(
      Src&& src, const KeyProjection& key_projection,
      size_t direct_capacity = kHybridDirectDefaultDirectCapacity) {
    this->Initialize(std::forward<Src>(src), key_projection,
                     DefaultValueProjection<Src>(), direct_capacity);
  }
  template <
      typename Src, typename KeyProjection = DefaultKeyProjection<Src>,
      typename ValueProjection = DefaultValueProjection<Src>,
      std::enable_if_t<
          std::conjunction_v<
              std::negation<std::is_convertible<KeyProjection, size_t>>,
              std::negation<std::is_convertible<ValueProjection, size_t>>,
              IsForwardIterable<Src>, HasProjectibleKeys<Src, KeyProjection>,
              HasProjectibleValues<Src, ValueProjection>>,
          int> = 0>
  explicit HybridDirectMap(
      Src&& src, const KeyProjection& key_projection,
      const ValueProjection& value_projection,
      size_t direct_capacity = kHybridDirectDefaultDirectCapacity) {
    this->Initialize(std::forward<Src>(src), key_projection, value_projection,
                     direct_capacity);
  }

  HybridDirectMap(const HybridDirectMap& that) = default;
  HybridDirectMap& operator=(const HybridDirectMap& that) = default;

  HybridDirectMap(HybridDirectMap&& that) = default;
  HybridDirectMap& operator=(HybridDirectMap&& that) = default;

  // Makes `*this` equivalent to a newly constructed `HybridDirectMap`.
  using HybridDirectMap::HybridDirectMapImpl::Reset;
  template <typename Src,
            std::enable_if_t<std::conjunction_v<IsForwardIterable<Src>,
                                                HasCompatibleKeys<Src>,
                                                HasCompatibleValues<Src>>,
                             int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      Src&& src, size_t direct_capacity = kHybridDirectDefaultDirectCapacity) {
    this->Reset();
    this->Initialize(std::forward<Src>(src), DefaultKeyProjection<Src>(),
                     DefaultValueProjection<Src>(), direct_capacity);
  }
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      std::initializer_list<std::pair<Key, Value>> src,
      size_t direct_capacity = kHybridDirectDefaultDirectCapacity) {
    this->Reset();
    this->Initialize(src, DefaultKeyProjection<decltype(src)>(),
                     DefaultValueProjection<decltype(src)>(), direct_capacity);
  }
  template <
      typename Src, typename KeyProjection = DefaultKeyProjection<Src>,
      std::enable_if_t<
          std::conjunction_v<
              std::negation<std::is_convertible<KeyProjection, size_t>>,
              IsForwardIterable<Src>, HasProjectibleKeys<Src, KeyProjection>,
              HasCompatibleValues<Src>>,
          int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      Src&& src, const KeyProjection& key_projection,
      size_t direct_capacity = kHybridDirectDefaultDirectCapacity) {
    this->Reset();
    this->Initialize(std::forward<Src>(src), key_projection,
                     DefaultValueProjection<Src>(), direct_capacity);
  }
  template <
      typename Src, typename KeyProjection = DefaultKeyProjection<Src>,
      typename ValueProjection = DefaultValueProjection<Src>,
      std::enable_if_t<
          std::conjunction_v<
              std::negation<std::is_convertible<KeyProjection, size_t>>,
              std::negation<std::is_convertible<ValueProjection, size_t>>,
              IsForwardIterable<Src>, HasProjectibleKeys<Src, KeyProjection>,
              HasProjectibleValues<Src, ValueProjection>>,
          int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      Src&& src, const KeyProjection& key_projection,
      const ValueProjection& value_projection,
      size_t direct_capacity = kHybridDirectDefaultDirectCapacity) {
    this->Reset();
    this->Initialize(std::forward<Src>(src), key_projection, value_projection,
                     direct_capacity);
  }
};

// Implementation details follow.

namespace hybrid_direct_internal {

template <typename Key, typename Value, typename Traits>
inline size_t HybridDirectMapImpl<Key, Value, Traits>::max_size() {
  return UnsignedMin(SizedDeleter<Value* absl_nullable>::max_size(),
                     SizedDeleter<DelayedConstructor<Value>>::max_size()) /
         kInverseMinLoadFactor;
}

template <typename Key, typename Value, typename Traits>
void HybridDirectMapImpl<Key, Value, Traits>::Reset() {
  direct_values_ = DirectValues();
  direct_map_ = DirectMap();
  slow_map_.reset();
}

template <typename Key, typename Value, typename Traits>
template <typename Src, typename KeyProjection, typename ValueProjection>
void HybridDirectMapImpl<Key, Value, Traits>::Initialize(
    Src&& src, const KeyProjection& key_projection,
    const ValueProjection& value_projection, size_t direct_capacity) {
  using std::begin;
  using std::end;
  if constexpr (IterableHasSize<Src>::value) {
    using std::size;
    const size_t src_size = size(src);
    RIEGELI_ASSERT_EQ(src_size,
                      IntCast<size_t>(std::distance(begin(src), end(src))))
        << "Failed precondition of HybridDirectMap initialization: "
           "size does not match the distance between iterators";
    if (src_size > 0) {
      Optimize<Src>(begin(src), end(src), src_size, key_projection,
                    value_projection, direct_capacity);
    }
  } else {
    auto first = begin(src);
    auto last = end(src);
    const size_t src_size = IntCast<size_t>(std::distance(first, last));
    if (src_size > 0) {
      Optimize<Src>(first, last, src_size, key_projection, value_projection,
                    direct_capacity);
    }
  }
#if RIEGELI_DEBUG
  // Detect building `HybridDirectMap` from a moved-from `src` if possible.
  if constexpr (std::conjunction_v<std::negation<std::is_reference<Src>>,
                                   std::is_move_constructible<Src>>) {
    ABSL_ATTRIBUTE_UNUSED Src moved = std::forward<Src>(src);
  }
#endif
}

template <typename Key, typename Value, typename Traits>
template <typename Src, typename Iterator, typename KeyProjection,
          typename ValueProjection>
void HybridDirectMapImpl<Key, Value, Traits>::Optimize(
    Iterator first, Iterator last, size_t size,
    const KeyProjection& key_projection,
    const ValueProjection& value_projection, size_t direct_capacity) {
  RIEGELI_ASSERT_GE(size, 0u)
      << "Failed precondition of HybridDirectMapImpl::Optimize(): "
         "an empty map must have been handled before";
  RIEGELI_CHECK_LE(size, max_size())
      << "Failed precondition of HybridDirectMap initialization: "
         "size overflow";
  RawKey max_raw_key = 0;
  for (auto iter = first; iter != last; ++iter) {
    max_raw_key = UnsignedMax(
        max_raw_key, Traits::ToRawKey(std::invoke(key_projection, *iter)));
  }
  const size_t max_num_direct_keys =
      UnsignedMax(direct_capacity, size * kInverseMinLoadFactor);
  size_t direct_values_index;
  if (max_raw_key < max_num_direct_keys) {
    // All keys are suitable for `direct_map_`. `slow_map_` is not used.
    //
    // There is no need for `direct_map_` to cover raw keys larger than
    // `max_raw_key` because their lookup is fast if `slow_map_` is `nullptr`.
    RIEGELI_ASSUME_EQ(direct_values_, nullptr) << "Initialization";
    direct_values_ = MakeSizedArray<DelayedConstructor<Value>>(size);
    RIEGELI_ASSUME_EQ(direct_map_, nullptr) << "Initialization";
    direct_map_ =
        MakeSizedArray<Value* absl_nullable>(IntCast<size_t>(max_raw_key) + 1);
    direct_values_index = 0;
    for (auto iter = first; iter != last; ++iter) {
      const RawKey raw_key =
          Traits::ToRawKey(std::invoke(key_projection, *iter));
      RIEGELI_ASSERT_EQ(direct_map_[raw_key], nullptr)
          << "Failed precondition of HybridDirectMap initialization: "
             "duplicate key: "
          << riegeli::Debug(std::invoke(key_projection, *iter));
      direct_map_[raw_key] = &direct_values_[direct_values_index++].emplace(
          std::invoke(value_projection, *MaybeMakeMoveIterator<Src>(iter)));
    }
  } else {
    // Some keys are too large for `direct_map_`. `slow_map_` is used.
    //
    // `direct_map_` covers all raw keys below `max_num_direct_keys` rather than
    // only up to `max_raw_key`, to reduce lookups in `slow_map_`.
    size_t num_direct_values = 0;
    for (auto iter = first; iter != last; ++iter) {
      num_direct_values += Traits::ToRawKey(std::invoke(
                               key_projection, *iter)) < max_num_direct_keys
                               ? 1
                               : 0;
    }
    RIEGELI_ASSERT_LT(num_direct_values, size)
        << "Some keys should have been too large for direct_map_";
    RIEGELI_ASSUME_EQ(direct_values_, nullptr) << "Initialization";
    if (num_direct_values > 0) {
      direct_values_ =
          MakeSizedArray<DelayedConstructor<Value>>(num_direct_values);
    }
    RIEGELI_ASSUME_EQ(direct_map_, nullptr) << "Initialization";
    direct_map_ = MakeSizedArray<Value* absl_nullable>(max_num_direct_keys);
    RIEGELI_ASSUME_EQ(slow_map_, nullptr) << "Initialization";
    slow_map_ = std::make_unique<SlowMap>();
    slow_map_->reserve(size - num_direct_values);
    direct_values_index = 0;
    for (auto iter = first; iter != last; ++iter) {
      const RawKey raw_key =
          Traits::ToRawKey(std::invoke(key_projection, *iter));
      if (raw_key < max_num_direct_keys) {
        RIEGELI_ASSERT_EQ(direct_map_[raw_key], nullptr)
            << "Failed precondition of HybridDirectMap initialization: "
               "duplicate key: "
            << riegeli::Debug(std::invoke(key_projection, *iter));
        direct_map_[raw_key] = &direct_values_[direct_values_index++].emplace(
            std::invoke(value_projection, *MaybeMakeMoveIterator<Src>(iter)));
      } else {
        const auto inserted = slow_map_->try_emplace(
            raw_key,
            std::invoke(value_projection, *MaybeMakeMoveIterator<Src>(iter)));
        RIEGELI_ASSERT(inserted.second)
            << "Failed precondition of HybridDirectMap initialization: "
               "duplicate key: "
            << riegeli::Debug(std::invoke(key_projection, *iter));
      }
    }
  }
  RIEGELI_ASSERT_EQ(direct_values_index, direct_values_.get_deleter().size())
      << "The whole direct_values_ array should have been filled";
}

template <typename Key, typename Value, typename Traits>
HybridDirectMapImpl<Key, Value, Traits>::HybridDirectMapImpl(
    const HybridDirectMapImpl& that) noexcept
    : direct_values_(that.CopyDirectValues()),
      direct_map_(that.CopyDirectMap(direct_values_.get())),
      slow_map_(that.CopySlowMap()) {}

template <typename Key, typename Value, typename Traits>
HybridDirectMapImpl<Key, Value, Traits>&
HybridDirectMapImpl<Key, Value, Traits>::operator=(
    const HybridDirectMapImpl& that) noexcept {
  absl_nullable DirectValues new_direct_values = that.CopyDirectValues();
  direct_map_ = that.CopyDirectMap(new_direct_values.get());
  direct_values_ = std::move(new_direct_values);
  slow_map_ = that.CopySlowMap();
  return *this;
}

template <typename Key, typename Value, typename Traits>
auto HybridDirectMapImpl<Key, Value, Traits>::CopyDirectValues() const ->
    absl_nullable DirectValues {
  if (direct_values_ == nullptr) return nullptr;
  DirectValues dest_ptr = MakeSizedArray<DelayedConstructor<Value>>(
      direct_values_.get_deleter().size());
  DelayedConstructor<Value>* src_iter = direct_values_.get();
  DelayedConstructor<Value>* const end =
      dest_ptr.get() + dest_ptr.get_deleter().size();
  for (DelayedConstructor<Value>* dest_iter = dest_ptr.get(); dest_iter != end;
       ++dest_iter) {
    dest_iter->emplace(**src_iter);
    ++src_iter;
  }
  return dest_ptr;
}

template <typename Key, typename Value, typename Traits>
auto HybridDirectMapImpl<Key, Value, Traits>::CopyDirectMap(
    DelayedConstructor<Value>* absl_nullable dest_values) const ->
    absl_nullable DirectMap {
  if (direct_map_ == nullptr) return nullptr;
  DelayedConstructor<Value>* const absl_nullable src_values =
      direct_values_.get();
  DirectMap dest_ptr =
      MakeSizedArray<Value* absl_nullable>(direct_map_.get_deleter().size());
  Value* absl_nullable* src_iter = direct_map_.get();
  Value* absl_nullable* const end =
      dest_ptr.get() + dest_ptr.get_deleter().size();
  for (Value* absl_nullable* dest_iter = dest_ptr.get(); dest_iter != end;
       ++dest_iter) {
    if (*src_iter != nullptr) {
      *dest_iter =
          reinterpret_cast<Value*>(reinterpret_cast<char*>(dest_values) +
                                   ((reinterpret_cast<char*>(*src_iter) -
                                     reinterpret_cast<char*>(src_values))));
    }
    ++src_iter;
  }
  return dest_ptr;
}

template <typename Key, typename Value, typename Traits>
auto HybridDirectMapImpl<Key, Value, Traits>::CopySlowMap() const ->
    absl_nullable std::unique_ptr<SlowMap> {
  if (slow_map_ == nullptr) return nullptr;
  return std::make_unique<SlowMap>(*slow_map_);
}

template <typename Key, typename Value, typename Traits>
ABSL_ATTRIBUTE_ALWAYS_INLINE Value* absl_nullable
HybridDirectMapImpl<Key, Value, Traits>::Find(Key key) {
  return const_cast<Value*>(std::as_const(*this).Find(key));
}

template <typename Key, typename Value, typename Traits>
ABSL_ATTRIBUTE_ALWAYS_INLINE const Value* absl_nullable
HybridDirectMapImpl<Key, Value, Traits>::Find(Key key) const {
  RIEGELI_ASSERT(!direct_map_.get_deleter().IsMovedFromIfNull() ||
                 direct_map_ != nullptr)
      << "Moved-from HybridDirectMap";
  const RawKey raw_key = Traits::ToRawKey(key);
  if (raw_key < direct_map_.get_deleter().size()) return direct_map_[raw_key];
  if (ABSL_PREDICT_TRUE(slow_map_ == nullptr)) return nullptr;
  const auto iter = slow_map_->find(raw_key);
  if (iter == slow_map_->end()) return nullptr;
  return &iter->second;
}

}  // namespace hybrid_direct_internal

}  // namespace riegeli

#endif  // RIEGELI_BASE_HYBRID_DIRECT_MAP_H_
