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
#include <limits>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/container/flat_hash_map.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/debug.h"
#include "riegeli/base/hybrid_direct_common.h"  // IWYU pragma: export
#include "riegeli/base/hybrid_direct_internal.h"
#include "riegeli/base/invoker.h"
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
 private:
  template <bool is_const>
  class IteratorImpl;

 public:
  using key_type = Key;
  using mapped_type = Value;
  using value_type = std::pair<const Key, Value>;
  using reference = ReferencePair<const Key, Value&>;
  using const_reference = ReferencePair<const Key, const Value&>;
  using pointer = ArrowProxy<reference>;
  using const_pointer = ArrowProxy<const_reference>;
  using iterator = IteratorImpl<false>;
  using const_iterator = IteratorImpl<true>;
  using size_type = size_t;
  using difference_type = ptrdiff_t;

  static size_t max_size();

  ABSL_ATTRIBUTE_REINITIALIZES void Reset();

  // Returns a pointer to the value associated with `key`, or `nullptr` if `key`
  // is absent.
  //
  // This can be a bit faster than `find()`.
  Value* absl_nullable FindOrNull(Key key) ABSL_ATTRIBUTE_LIFETIME_BOUND;
  const Value* absl_nullable FindOrNull(Key key) const
      ABSL_ATTRIBUTE_LIFETIME_BOUND;

  // Returns a reference to the value associated with `key`, or a reference to
  // `default_value` if `key` is absent.
  const Value& FindOrDefault(
      Key key, const Value& default_value ABSL_ATTRIBUTE_LIFETIME_BOUND = {})
      const ABSL_ATTRIBUTE_LIFETIME_BOUND;

  iterator find(Key key) ABSL_ATTRIBUTE_LIFETIME_BOUND;
  const_iterator find(Key key) const ABSL_ATTRIBUTE_LIFETIME_BOUND;

  bool contains(Key key) const;

  Value& at(Key key) ABSL_ATTRIBUTE_LIFETIME_BOUND;
  const Value& at(Key key) const ABSL_ATTRIBUTE_LIFETIME_BOUND;

  bool empty() const {
    return direct_values_.get_deleter().size() == 0 &&
           ABSL_PREDICT_TRUE(slow_map_ == nullptr);
  }
  size_t size() const;

  iterator begin() ABSL_ATTRIBUTE_LIFETIME_BOUND;
  const_iterator begin() const ABSL_ATTRIBUTE_LIFETIME_BOUND;
  const_iterator cbegin() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return begin();
  }
  iterator end() ABSL_ATTRIBUTE_LIFETIME_BOUND;
  const_iterator end() const ABSL_ATTRIBUTE_LIFETIME_BOUND;
  const_iterator cend() const ABSL_ATTRIBUTE_LIFETIME_BOUND { return end(); }

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

  template <typename Index, typename KeyProjection, typename ValueProjection>
  void InitializeByIndex(Index size, const KeyProjection& key_projection,
                         const ValueProjection& value_projection,
                         size_t direct_capacity);

  static bool Equal(const HybridDirectMapImpl& a, const HybridDirectMapImpl& b);

 private:
  using RawKey = std::decay_t<decltype(Traits::ToRawKey(std::declval<Key>()))>;
  static_assert(std::is_unsigned_v<RawKey>);

  using DirectValues =
      SizedArray<DelayedConstructor<Value>, /*supports_abandon=*/true>;
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

  ABSL_ATTRIBUTE_NORETURN static void KeyNotFound(Key key);

  size_t FirstRawKey() const;

  size_t capacity() const {
    return direct_map_.get_deleter().size() +
           (slow_map_ == nullptr ? 0 : slow_map_->capacity());
  }

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

// `HybridDirectMap` is a map optimized for keys being mostly small integers,
// especially dense near zero. It supports only lookups and iteration, but no
// incremental modification.
//
// It stores a part of the map covering some range of small keys in an array.
// The remaining keys are stored in an `absl::flat_hash_map`.
//
// `Traits` specifies a mapping of keys to an unsigned integer type. It must
// support at least the following static members:
//
// ```
//   // Translates the key to a raw key, which is an unsigned integer type.
//   // Small raw keys are put in the array.
//   static RawKey ToRawKey(Key key);
//
//   // Translates the raw key back to a key.
//   //
//   // This is optional. Needed only for iterators.
//   static Key FromRawKey(RawKey raw_key);
// ```
//
// `direct_capacity`, if specified during building, is the intended capacity
// of the array part. The actual capacity can be smaller if all keys fit
// in the array, or larger if the array remains at least 25% full. Default:
// `kHybridDirectDefaultDirectCapacity` (128).
//
// In the case of duplicate keys, the first value wins.
template <typename Key, typename Value,
          typename Traits = HybridDirectTraits<Key>>
class HybridDirectMap
    : public hybrid_direct_internal::HybridDirectMapImpl<Key, Value, Traits>,
      public ConditionallyConstructible<std::is_copy_constructible_v<Value>,
                                        true>,
      public ConditionallyAssignable<std::is_copy_constructible_v<Value>, true>,
      public WithEqual<HybridDirectMap<Key, Value, Traits>> {
 private:
  template <typename Src, typename Enable = void>
  struct HasCompatibleKeys : std::false_type {};
  template <typename Src>
  struct HasCompatibleKeys<
      Src, std::enable_if_t<std::is_convertible_v<
               decltype(std::declval<ElementTypeT<const Src&>>().first), Key>>>
      : std::true_type {};

  template <typename Src, typename Enable = void>
  struct HasCompatibleValues : std::false_type {};
  template <typename Src>
  struct HasCompatibleValues<
      Src, std::enable_if_t<std::is_convertible_v<
               decltype(std::declval<ElementTypeT<Src>>().second), Value>>>
      : std::true_type {};

  template <typename Src, typename KeyProjection, typename Enable = void>
  struct HasProjectableKeys : std::false_type {};
  template <typename Src, typename KeyProjection>
  struct HasProjectableKeys<
      Src, KeyProjection,
      std::enable_if_t<std::is_convertible_v<
          std::invoke_result_t<const KeyProjection&, ElementTypeT<const Src&>>,
          Key>>> : std::true_type {};

  template <typename Src, typename ValueProjection, typename Enable = void>
  struct HasProjectableValues : std::false_type {};
  template <typename Src, typename ValueProjection>
  struct HasProjectableValues<
      Src, ValueProjection,
      std::enable_if_t<std::is_convertible_v<
          std::invoke_result_t<const ValueProjection&, ElementTypeT<Src>>,
          Value>>> : std::true_type {};

  template <typename Index, typename KeyProjection, typename Enable = void>
  struct HasGeneratableKeys : std::false_type {};
  template <typename Index, typename KeyProjection>
  struct HasGeneratableKeys<
      Index, KeyProjection,
      std::enable_if_t<std::is_convertible_v<
          std::invoke_result_t<const KeyProjection&, Index>, Key>>>
      : std::true_type {};

  template <typename Index, typename ValueProjection, typename Enable = void>
  struct HasGeneratableValues : std::false_type {};
  template <typename Index, typename ValueProjection>
  struct HasGeneratableValues<
      Index, ValueProjection,
      std::enable_if_t<std::is_convertible_v<
          std::invoke_result_t<const ValueProjection&, Index>, Value>>>
      : std::true_type {};

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
              IsForwardIterable<Src>, HasProjectableKeys<Src, KeyProjection>,
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
              IsForwardIterable<Src>, HasProjectableKeys<Src, KeyProjection>,
              HasProjectableValues<Src, ValueProjection>>,
          int> = 0>
  explicit HybridDirectMap(
      Src&& src, const KeyProjection& key_projection,
      const ValueProjection& value_projection,
      size_t direct_capacity = kHybridDirectDefaultDirectCapacity) {
    this->Initialize(std::forward<Src>(src), key_projection, value_projection,
                     direct_capacity);
  }

  // Builds `HybridDirectMap` from keys and values computed by invoking
  // `key_projection()` and `value_projection()` with indices from [0..`size`).
  //
  // `key_projection()` may be called multiple times for each index so it should
  // be efficient. `value_projection()` is called once for each index so it can
  // be expensive.
  template <typename Index, typename KeyProjection, typename ValueProjection,
            std::enable_if_t<
                std::conjunction_v<
                    std::is_integral<Index>,
                    std::negation<std::is_convertible<KeyProjection, size_t>>,
                    std::negation<std::is_convertible<ValueProjection, size_t>>,
                    HasGeneratableKeys<Index, KeyProjection>,
                    HasGeneratableValues<Index, ValueProjection>>,
                int> = 0>
  explicit HybridDirectMap(
      Index size, const KeyProjection& key_projection,
      const ValueProjection& value_projection,
      size_t direct_capacity = kHybridDirectDefaultDirectCapacity) {
    this->InitializeByIndex(size, key_projection, value_projection,
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
              IsForwardIterable<Src>, HasProjectableKeys<Src, KeyProjection>,
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
              IsForwardIterable<Src>, HasProjectableKeys<Src, KeyProjection>,
              HasProjectableValues<Src, ValueProjection>>,
          int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      Src&& src, const KeyProjection& key_projection,
      const ValueProjection& value_projection,
      size_t direct_capacity = kHybridDirectDefaultDirectCapacity) {
    this->Reset();
    this->Initialize(std::forward<Src>(src), key_projection, value_projection,
                     direct_capacity);
  }
  template <typename Index, typename KeyProjection, typename ValueProjection,
            std::enable_if_t<
                std::conjunction_v<
                    std::is_integral<Index>,
                    std::negation<std::is_convertible<KeyProjection, size_t>>,
                    std::negation<std::is_convertible<ValueProjection, size_t>>,
                    HasGeneratableKeys<Index, KeyProjection>,
                    HasGeneratableValues<Index, ValueProjection>>,
                int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      Index size, const KeyProjection& key_projection,
      const ValueProjection& value_projection,
      size_t direct_capacity = kHybridDirectDefaultDirectCapacity) {
    this->Reset();
    this->InitializeByIndex(size, key_projection, value_projection,
                            direct_capacity);
  }

  friend bool operator==(const HybridDirectMap& a, const HybridDirectMap& b) {
    return HybridDirectMap::HybridDirectMapImpl::Equal(a, b);
  }
};

namespace hybrid_direct_internal {

template <typename Key, typename Value, typename Traits>
template <bool is_const>
class HybridDirectMapImpl<Key, Value, Traits>::IteratorImpl
    : public WithEqual<IteratorImpl<is_const>> {
 public:
  using iterator_concept = std::forward_iterator_tag;
  // `iterator_category` is only `std::input_iterator_tag` because the
  // `LegacyForwardIterator` requirement and above require `reference` to be
  // a true reference type.
  using iterator_category = std::input_iterator_tag;
  using value_type = std::pair<const Key, Value>;
  using reference =
      ReferencePair<const Key,
                    std::conditional_t<is_const, const Value&, Value&>>;
  using pointer = ArrowProxy<reference>;
  using difference_type = ptrdiff_t;

  IteratorImpl() = default;

  // Conversion from `iterator` to `const_iterator`.
  template <bool that_is_const,
            std::enable_if_t<is_const && !that_is_const, int> = 0>
  /*implicit*/ IteratorImpl(IteratorImpl<that_is_const> that) noexcept
      : direct_map_end_(that.direct_map_end_),
        direct_map_size_(that.direct_map_size_),
        raw_key_complement_(that.raw_key_complement_),
        slow_map_iter_(that.slow_map_iter_) {}

  IteratorImpl(const IteratorImpl& that) = default;
  IteratorImpl& operator=(const IteratorImpl& that) = default;

  reference operator*() const {
    if (ABSL_PREDICT_TRUE(raw_key_complement_ > 0)) {
      return reference{Traits::FromRawKey(IntCast<RawKey>(direct_map_size_ -
                                                          raw_key_complement_)),
                       **(direct_map_end_ - raw_key_complement_)};
    }
    const auto iter = *slow_map_iter_;
    return reference{Traits::FromRawKey(iter->first), iter->second};
  }
  pointer operator->() const { return pointer(**this); }
  IteratorImpl& operator++() {
    if (ABSL_PREDICT_TRUE(raw_key_complement_ > 0)) {
      do {
        --raw_key_complement_;
        if (ABSL_PREDICT_FALSE(raw_key_complement_ == 0)) break;
      } while (*(direct_map_end_ - raw_key_complement_) == nullptr);
    } else {
      ++*slow_map_iter_;
    }
    return *this;
  }
  IteratorImpl operator++(int) {
    IteratorImpl result = *this;
    ++*this;
    return result;
  }

  template <bool that_is_const>
  friend bool operator==(IteratorImpl a, IteratorImpl<that_is_const> b) {
    RIEGELI_ASSERT_EQ(a.direct_map_end_, b.direct_map_end_)
        << "Failed precondition of operator==(HybridDirectMap::iterator): "
           "incomparable iterators";
    RIEGELI_ASSERT_EQ(a.direct_map_size_, b.direct_map_size_)
        << "Failed precondition of operator==(HybridDirectMap::iterator): "
           "incomparable iterators";
    RIEGELI_ASSERT_EQ(a.slow_map_iter_ != std::nullopt,
                      b.slow_map_iter_ != std::nullopt)
        << "Failed precondition of operator==(HybridDirectMap::iterator): "
           "incomparable iterators";
    if (a.raw_key_complement_ != b.raw_key_complement_) return false;
    if (ABSL_PREDICT_TRUE(a.slow_map_iter_ == std::nullopt)) return true;
    return *a.slow_map_iter_ == *b.slow_map_iter_;
  }

 private:
  friend class HybridDirectMapImpl;

  explicit IteratorImpl(std::conditional_t<is_const, const HybridDirectMapImpl*,
                                           HybridDirectMapImpl*>
                            map ABSL_ATTRIBUTE_LIFETIME_BOUND,
                        size_t raw_key_complement)
      : direct_map_end_(map->direct_map_.get() +
                        map->direct_map_.get_deleter().size()),
        direct_map_size_(map->direct_map_.get_deleter().size()),
        raw_key_complement_(raw_key_complement) {}

  explicit IteratorImpl(
      std::conditional_t<is_const, const HybridDirectMapImpl*,
                         HybridDirectMapImpl*>
          map ABSL_ATTRIBUTE_LIFETIME_BOUND,
      size_t raw_key_complement,
      std::conditional_t<is_const, typename SlowMap::const_iterator,
                         typename SlowMap::iterator>
          slow_map_iter)
      : direct_map_end_(map->direct_map_.get() +
                        map->direct_map_.get_deleter().size()),
        direct_map_size_(map->direct_map_.get_deleter().size()),
        raw_key_complement_(raw_key_complement),
        slow_map_iter_(slow_map_iter) {}

  // The end of the `direct_map_` array.
  //
  // Counting backwards simplifies checking for iteration over `direct_map_`.
  absl_nullable const std::conditional_t<
      is_const, const Value*, Value*>* absl_nullable direct_map_end_ = nullptr;
  // `direct_map_.get_deleter().size()`.
  size_t direct_map_size_ = 0;
  // `direct_map_size_ - raw_key` when iterating over `direct_map_`, otherwise
  // 0.
  //
  // Invariant: if `raw_key_complement > 0` then
  // `*(direct_map_end_ - raw_key_complement_) != nullptr`.
  //
  // Counting backwards simplifies checking for iteration over `direct_map_`.
  size_t raw_key_complement_ = 0;
  // Iterator over `*slow_map_` when `slow_map_ != nullptr`, otherwise
  // `std::nullopt`.
  //
  // Invariant: if `raw_key_complement_ > 0` then
  // `slow_map_iter_ == std::nullopt` or
  // `slow_map_iter_ == slow_map_->begin()`.
  //
  // Distinguishing `std::nullopt` instead of using the default-constructed
  // `SlowMap::iterator` makes the common case of `operator==` faster by
  // reducing usage of `SlowMap` iterators.
  std::optional<std::conditional_t<is_const, typename SlowMap::const_iterator,
                                   typename SlowMap::iterator>>
      slow_map_iter_;
};

}  // namespace hybrid_direct_internal

// Implementation details follow.

namespace hybrid_direct_internal {

template <typename Key, typename Value, typename Traits>
inline size_t HybridDirectMapImpl<Key, Value, Traits>::max_size() {
  return UnsignedMin(SizedDeleter<Value* absl_nullable>::max_size(),
                     SizedDeleter<DelayedConstructor<Value>,
                                  /*supports_abandon=*/true>::max_size()) /
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
template <typename Index, typename KeyProjection, typename ValueProjection>
void HybridDirectMapImpl<Key, Value, Traits>::InitializeByIndex(
    Index size, const KeyProjection& key_projection,
    const ValueProjection& value_projection, size_t direct_capacity) {
  if (size > 0) {
    RIEGELI_CHECK_LE(UnsignedCast(size), std::numeric_limits<size_t>::max())
        << "Failed precondition of HybridDirectMap initialization: "
           "size overflow";
    // The template parameter of `Optimize()` serves only to determine whether
    // to apply `std::move_iterator`.
    Optimize<const Index[1]>(hybrid_direct_internal::IndexIterator<Index>(0),
                             hybrid_direct_internal::IndexIterator<Index>(size),
                             IntCast<size_t>(size), key_projection,
                             value_projection, direct_capacity);
  }
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
    hybrid_direct_internal::AssignToAssumedNull(
        direct_values_,
        MakeSizedArray<DelayedConstructor<Value>, /*supports_abandon=*/true>(
            size));
    hybrid_direct_internal::AssignToAssumedNull(
        direct_map_,
        MakeSizedArray<Value* absl_nullable>(IntCast<size_t>(max_raw_key) + 1));
    direct_values_index = 0;
    for (auto iter = first; iter != last; ++iter) {
      const RawKey raw_key =
          Traits::ToRawKey(std::invoke(key_projection, *iter));
      if (ABSL_PREDICT_FALSE(direct_map_[raw_key] != nullptr)) continue;
      direct_map_[raw_key] =
          &direct_values_[direct_values_index++].emplace(riegeli::Invoker(
              value_projection, *MaybeMakeMoveIterator<Src>(iter)));
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
    if (num_direct_values > 0) {
      hybrid_direct_internal::AssignToAssumedNull(
          direct_values_,
          MakeSizedArray<DelayedConstructor<Value>, /*supports_abandon=*/true>(
              num_direct_values));
    }
    hybrid_direct_internal::AssignToAssumedNull(
        direct_map_, MakeSizedArray<Value* absl_nullable>(max_num_direct_keys));
    hybrid_direct_internal::AssignToAssumedNull(slow_map_,
                                                std::make_unique<SlowMap>());
    slow_map_->reserve(size - num_direct_values);
    direct_values_index = 0;
    for (auto iter = first; iter != last; ++iter) {
      const RawKey raw_key =
          Traits::ToRawKey(std::invoke(key_projection, *iter));
      if (raw_key < max_num_direct_keys) {
        if (ABSL_PREDICT_FALSE(direct_map_[raw_key] != nullptr)) continue;
        direct_map_[raw_key] =
            &direct_values_[direct_values_index++].emplace(riegeli::Invoker(
                value_projection, *MaybeMakeMoveIterator<Src>(iter)));
      } else {
        slow_map_->try_emplace(
            raw_key, riegeli::Invoker(value_projection,
                                      *MaybeMakeMoveIterator<Src>(iter)));
      }
    }
  }
  direct_values_.get_deleter().AbandonAfter(direct_values_.get(),
                                            direct_values_index);
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
  DirectValues dest_ptr =
      MakeSizedArray<DelayedConstructor<Value>, /*supports_abandon=*/true>(
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
  DirectMap dest_ptr = MakeSizedArrayForOverwrite<Value* absl_nullable>(
      direct_map_.get_deleter().size());
  Value* absl_nullable* src_iter = direct_map_.get();
  Value* absl_nullable* const end =
      dest_ptr.get() + dest_ptr.get_deleter().size();
  for (Value* absl_nullable* dest_iter = dest_ptr.get(); dest_iter != end;
       ++dest_iter) {
    *dest_iter =
        *src_iter == nullptr
            ? nullptr
            : reinterpret_cast<Value*>(reinterpret_cast<char*>(dest_values) +
                                       ((reinterpret_cast<char*>(*src_iter) -
                                         reinterpret_cast<char*>(src_values))));
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
ABSL_ATTRIBUTE_ALWAYS_INLINE inline Value* absl_nullable
HybridDirectMapImpl<Key, Value, Traits>::FindOrNull(Key key)
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  return const_cast<Value*>(std::as_const(*this).FindOrNull(key));
}

template <typename Key, typename Value, typename Traits>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline const Value* absl_nullable
HybridDirectMapImpl<Key, Value, Traits>::FindOrNull(Key key) const
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
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

template <typename Key, typename Value, typename Traits>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline const Value&
HybridDirectMapImpl<Key, Value, Traits>::FindOrDefault(
    Key key, const Value& default_value ABSL_ATTRIBUTE_LIFETIME_BOUND) const
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  RIEGELI_ASSERT(!direct_map_.get_deleter().IsMovedFromIfNull() ||
                 direct_map_ != nullptr)
      << "Moved-from HybridDirectMap";
  const RawKey raw_key = Traits::ToRawKey(key);
  if (raw_key < direct_map_.get_deleter().size()) {
    const Value* const absl_nullable value = direct_map_[raw_key];
    if (value == nullptr) return default_value;
    return *value;
  }
  if (ABSL_PREDICT_TRUE(slow_map_ == nullptr)) return default_value;
  const auto iter = slow_map_->find(raw_key);
  if (iter == slow_map_->end()) return default_value;
  return iter->second;
}

template <typename Key, typename Value, typename Traits>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline auto
HybridDirectMapImpl<Key, Value, Traits>::find(Key key)
    ABSL_ATTRIBUTE_LIFETIME_BOUND -> iterator {
  RIEGELI_ASSERT(!direct_map_.get_deleter().IsMovedFromIfNull() ||
                 direct_map_ != nullptr)
      << "Moved-from HybridDirectMap";
  const RawKey raw_key = Traits::ToRawKey(key);
  if (raw_key < direct_map_.get_deleter().size()) {
    if (ABSL_PREDICT_TRUE(slow_map_ == nullptr)) {
      return iterator(this, direct_map_[raw_key] == nullptr
                                ? 0
                                : direct_map_.get_deleter().size() - raw_key);
    }
    if (direct_map_[raw_key] == nullptr) {
      return iterator(this, 0, slow_map_->end());
    }
    return iterator(this, direct_map_.get_deleter().size() - raw_key,
                    slow_map_->begin());
  }
  if (ABSL_PREDICT_TRUE(slow_map_ == nullptr)) return iterator(this, 0);
  return iterator(this, 0, slow_map_->find(raw_key));
}

template <typename Key, typename Value, typename Traits>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline auto
HybridDirectMapImpl<Key, Value, Traits>::find(Key key) const
    ABSL_ATTRIBUTE_LIFETIME_BOUND -> const_iterator {
  RIEGELI_ASSERT(!direct_map_.get_deleter().IsMovedFromIfNull() ||
                 direct_map_ != nullptr)
      << "Moved-from HybridDirectMap";
  const RawKey raw_key = Traits::ToRawKey(key);
  if (raw_key < direct_map_.get_deleter().size()) {
    if (ABSL_PREDICT_TRUE(slow_map_ == nullptr)) {
      return const_iterator(this,
                            direct_map_[raw_key] == nullptr
                                ? 0
                                : direct_map_.get_deleter().size() - raw_key);
    }
    if (direct_map_[raw_key] == nullptr) {
      return const_iterator(this, 0, slow_map_->cend());
    }
    return const_iterator(this, direct_map_.get_deleter().size() - raw_key,
                          slow_map_->cbegin());
  }
  if (ABSL_PREDICT_TRUE(slow_map_ == nullptr)) return const_iterator(this, 0);
  return const_iterator(this, 0, std::as_const(*slow_map_).find(raw_key));
}

template <typename Key, typename Value, typename Traits>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool
HybridDirectMapImpl<Key, Value, Traits>::contains(Key key) const {
  RIEGELI_ASSERT(!direct_map_.get_deleter().IsMovedFromIfNull() ||
                 direct_map_ != nullptr)
      << "Moved-from HybridDirectMap";
  const RawKey raw_key = Traits::ToRawKey(key);
  if (raw_key < direct_map_.get_deleter().size()) {
    return direct_map_[raw_key] != nullptr;
  }
  if (ABSL_PREDICT_TRUE(slow_map_ == nullptr)) return false;
  return slow_map_->contains(raw_key);
}

template <typename Key, typename Value, typename Traits>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline Value&
HybridDirectMapImpl<Key, Value, Traits>::at(Key key)
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  return const_cast<Value&>(std::as_const(*this).at(key));
}

template <typename Key, typename Value, typename Traits>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline const Value&
HybridDirectMapImpl<Key, Value, Traits>::at(Key key) const
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  RIEGELI_ASSERT(!direct_map_.get_deleter().IsMovedFromIfNull() ||
                 direct_map_ != nullptr)
      << "Moved-from HybridDirectMap";
  const RawKey raw_key = Traits::ToRawKey(key);
  if (raw_key < direct_map_.get_deleter().size()) {
    const Value* const absl_nullable value = direct_map_[raw_key];
    if (ABSL_PREDICT_FALSE(value == nullptr)) KeyNotFound(key);
    return *value;
  }
  if (ABSL_PREDICT_FALSE(slow_map_ == nullptr)) KeyNotFound(key);
  const auto iter = slow_map_->find(raw_key);
  if (ABSL_PREDICT_FALSE(iter == slow_map_->end())) KeyNotFound(key);
  return iter->second;
}

template <typename Key, typename Value, typename Traits>
ABSL_ATTRIBUTE_NORETURN void
HybridDirectMapImpl<Key, Value, Traits>::KeyNotFound(Key key) {
  RIEGELI_CHECK_UNREACHABLE()
      << "HybridDirectMap key not found: " << riegeli::Debug(key);
}

template <typename Key, typename Value, typename Traits>
inline size_t HybridDirectMapImpl<Key, Value, Traits>::FirstRawKey() const {
  const size_t direct_map_size = direct_map_.get_deleter().size();
  for (size_t raw_key = 0; raw_key < direct_map_size; ++raw_key) {
    if (direct_map_[raw_key] != nullptr) return raw_key;
  }
  return direct_map_size;
}

template <typename Key, typename Value, typename Traits>
inline size_t HybridDirectMapImpl<Key, Value, Traits>::size() const {
  return direct_values_.get_deleter().size() +
         (ABSL_PREDICT_TRUE(slow_map_ == nullptr) ? 0 : slow_map_->size());
}

template <typename Key, typename Value, typename Traits>
inline auto HybridDirectMapImpl<Key, Value, Traits>::begin()
    ABSL_ATTRIBUTE_LIFETIME_BOUND -> iterator {
  const size_t raw_key_complement =
      direct_map_.get_deleter().size() - FirstRawKey();
  if (ABSL_PREDICT_TRUE(slow_map_ == nullptr)) {
    return iterator(this, raw_key_complement);
  }
  return iterator(this, raw_key_complement, slow_map_->begin());
}

template <typename Key, typename Value, typename Traits>
inline auto HybridDirectMapImpl<Key, Value, Traits>::begin() const
    ABSL_ATTRIBUTE_LIFETIME_BOUND -> const_iterator {
  const size_t raw_key_complement =
      direct_map_.get_deleter().size() - FirstRawKey();
  if (ABSL_PREDICT_TRUE(slow_map_ == nullptr)) {
    return const_iterator(this, raw_key_complement);
  }
  return const_iterator(this, raw_key_complement, slow_map_->cbegin());
}

template <typename Key, typename Value, typename Traits>
inline auto HybridDirectMapImpl<Key, Value, Traits>::end()
    ABSL_ATTRIBUTE_LIFETIME_BOUND -> iterator {
  if (ABSL_PREDICT_TRUE(slow_map_ == nullptr)) return iterator(this, 0);
  return iterator(this, 0, slow_map_->end());
}

template <typename Key, typename Value, typename Traits>
inline auto HybridDirectMapImpl<Key, Value, Traits>::end() const
    ABSL_ATTRIBUTE_LIFETIME_BOUND -> const_iterator {
  if (ABSL_PREDICT_TRUE(slow_map_ == nullptr)) return const_iterator(this, 0);
  return const_iterator(this, 0, slow_map_->cend());
}

template <typename Key, typename Value, typename Traits>
bool HybridDirectMapImpl<Key, Value, Traits>::Equal(
    const HybridDirectMapImpl& a, const HybridDirectMapImpl& b) {
  if (a.size() != b.size()) return false;
  const HybridDirectMapImpl* outer;
  const HybridDirectMapImpl* inner;
  if (a.capacity() <= b.capacity()) {
    outer = &a;
    inner = &b;
  } else {
    outer = &b;
    inner = &a;
  }
  for (const_reference entry : *outer) {
    const auto* const found = inner->FindOrNull(entry.first);
    if (found == nullptr || *found != entry.second) return false;
  }
  return true;
}

}  // namespace hybrid_direct_internal

}  // namespace riegeli

#endif  // RIEGELI_BASE_HYBRID_DIRECT_MAP_H_
