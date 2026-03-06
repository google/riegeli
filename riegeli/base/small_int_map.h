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

#ifndef RIEGELI_BASE_SMALL_INT_MAP_H_
#define RIEGELI_BASE_SMALL_INT_MAP_H_

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/container/flat_hash_map.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

namespace small_int_map_internal {

template <typename T>
class DelayedConstructor;

}  // namespace small_int_map_internal

// `SmallIntMap` is a map optimized for keys being small integers. It supports
// only lookups, but no incremental building nor iteration.
//
// It stores a part of the map covering some range of keys starting from
// `expected_min_key` in an array.
//
// At least `array_capacity` possible keys starting from `expected_min_key` are
// suitable for array lookup. If all present keys are suitable, the stored array
// can be smaller than `array_capacity`, covering the range to the largest key.
// If the map is large, the stored array can be larger than `array_capacity`,
// as long as it is at least 25% full.
template <typename Key, typename Value, Key expected_min_key = 0,
          size_t array_capacity = 128>
class SmallIntMap {
 public:
  SmallIntMap() = default;

  // Builds `SmallIntMap` from a pair of iterators over pairs of key and value
  // with no duplicate keys.
  template <typename Iter>
  explicit SmallIntMap(Iter first, Iter last, size_t size) {
    RIEGELI_ASSERT_EQ(size, std::distance(first, last))
        << "Failed precondition of SmallIntMap initialization: "
           "size does not match the distance between iterators";
    if (size > 0) Optimize(first, last, size);
  }

  SmallIntMap(SmallIntMap&& that) noexcept;
  SmallIntMap& operator=(SmallIntMap&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `SmallIntMap`.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset();
  template <typename Iter>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Iter first, Iter last, size_t size) {
    RIEGELI_ASSERT_EQ(size, std::distance(first, last))
        << "Failed precondition of SmallIntMap initialization: "
           "size does not match the distance between iterators";
    Reset();
    if (size > 0) Optimize(first, last, size);
  }

  const Value* absl_nullable Find(Key key) const;

 private:
  // The raw key corresponding to a key is the key minus `expected_min_key`,
  // represented in an unsigned type with wrap-around.
  using RawKey = std::common_type_t<std::make_unsigned_t<Key>, size_t>;

  // A moved-from `SmallIntMap` has `num_small_keys_ == kPoisonedNumSmallKeys`
  // and `small_map_ == nullptr`. In debug mode this asserts against using a
  // moved-from object. In non-debug mode, if the key is not too large,
  // then this triggers a null pointer dereference with an offset up to 1MB,
  // which is assumed to reliably crash.
  static constexpr size_t kPoisonedNumSmallKeys =
      (size_t{1} << 20) / sizeof(const Value*);

  static RawKey ToRawKey(Key key) {
    // Wrap-around is not an error.
    return static_cast<RawKey>(key) - static_cast<RawKey>(expected_min_key);
  }

  template <typename Iter>
  void Optimize(Iter first, Iter last, size_t size);

  const Value* absl_nullable FindSlow(Key key) const;

  // The size of `small_map_`, or `kPoisonedNumSmallKeys` for a moved-from
  // `SmallIntMap`.
  size_t num_small_keys_ = 0;
  // Indexed by raw key, with the size of `num_small_keys_`. Elements
  // corresponding to present values point to elements of `small_values_`.
  // The remaining elements are `nullptr`.
  absl_nullable std::unique_ptr<const Value* absl_nullable[]> small_map_;
  // Stores values for `small_map_`, in no particular order.
  absl_nullable
  std::unique_ptr<small_int_map_internal::DelayedConstructor<Value>[]>
      small_values_;
  // If not `nullptr`, stores the mapping for keys too large for `small_map_`.
  // Uses `std::unique_ptr` rather than `std::optional` to reduce memory usage
  // in the common case when `large_map_` is not used.
  absl_nullable std::unique_ptr<absl::flat_hash_map<Key, Value>> large_map_;
};

// Implementation details follow.

namespace small_int_map_internal {

template <typename T>
class DelayedConstructor {
 public:
  // Does not construct the value yet.
  DelayedConstructor() noexcept {}

  DelayedConstructor(const DelayedConstructor&) = delete;
  DelayedConstructor& operator=(const DelayedConstructor&) = delete;

  ~DelayedConstructor() { value_.~T(); }

  // Construct the value. Must be called exactly once.
  template <typename... Args,
            std::enable_if_t<std::is_constructible_v<T, Args&&...>, int> = 0>
  T& emplace(Args&&... args) ABSL_ATTRIBUTE_LIFETIME_BOUND {
    new (&value_) T(std::forward<Args>(args)...);
    return value_;
  }

 private:
  union {
    T value_;
  };
};

}  // namespace small_int_map_internal

template <typename Key, typename Value, Key expected_min_key,
          size_t array_capacity>
SmallIntMap<Key, Value, expected_min_key, array_capacity>::SmallIntMap(
    SmallIntMap&& that) noexcept
    : num_small_keys_(
          std::exchange(that.num_small_keys_, kPoisonedNumSmallKeys)),
      small_map_(std::move(that.small_map_)),
      small_values_(std::move(that.small_values_)),
      large_map_(std::move(that.large_map_)) {}

template <typename Key, typename Value, Key expected_min_key,
          size_t array_capacity>
SmallIntMap<Key, Value, expected_min_key, array_capacity>&
SmallIntMap<Key, Value, expected_min_key, array_capacity>::operator=(
    SmallIntMap&& that) noexcept {
  num_small_keys_ = std::exchange(that.num_small_keys_, kPoisonedNumSmallKeys);
  small_map_ = std::move(that.small_map_);
  small_values_ = std::move(that.small_values_);
  large_map_ = std::move(that.large_map_);
  return *this;
}

template <typename Key, typename Value, Key expected_min_key,
          size_t array_capacity>
void SmallIntMap<Key, Value, expected_min_key, array_capacity>::Reset() {
  num_small_keys_ = 0;
  small_map_.reset();
  small_values_.reset();
  large_map_.reset();
}

template <typename Key, typename Value, Key expected_min_key,
          size_t array_capacity>
template <typename Iter>
void SmallIntMap<Key, Value, expected_min_key, array_capacity>::Optimize(
    Iter first, Iter last, size_t size) {
  RIEGELI_ASSERT_GE(size, 0u)
      << "Failed precondition of SmallIntMap::Optimize(): "
         "an empty map must have been handled before";
  RawKey max_raw_key = 0;
  for (auto iter = first; iter != last; ++iter) {
    max_raw_key = UnsignedMax(max_raw_key, ToRawKey(iter->first));
  }
  const size_t max_num_small_keys = UnsignedMax(array_capacity, size * 4);
  size_t num_small_values;
  size_t small_values_index;
  if (max_raw_key < max_num_small_keys) {
    // All keys are suitable for `small_map_`. `large_map_` is not used.
    //
    // There is no need for `small_map_` to cover raw keys larger than
    // `max_raw_key` because their lookup is fast if `large_map_` is `nullptr`.
    num_small_keys_ = IntCast<size_t>(max_raw_key) + 1;
    RIEGELI_ASSUME_EQ(small_map_, nullptr) << "Initialization";
    small_map_ =
        std::make_unique<const Value* absl_nullable[]>(num_small_keys_);
    num_small_values = size;
    RIEGELI_ASSUME_EQ(small_values_, nullptr) << "Initialization";
    small_values_ =
        std::make_unique<small_int_map_internal::DelayedConstructor<Value>[]>(
            num_small_values);
    small_values_index = 0;
    for (auto iter = first; iter != last; ++iter) {
      // `(*iter).second` rather than `iter->second` allows moving from a move
      // iterator.
      RIEGELI_ASSERT_EQ(small_map_[ToRawKey(iter->first)], nullptr)
          << "Failed precondition of SmallIntMap initialization: "
             "duplicate key: "
          << iter->first;
      small_map_[ToRawKey(iter->first)] =
          &small_values_[small_values_index++].emplace((*iter).second);
    }
  } else {
    // Some keys are too large for `small_map_`. `large_map_` is used.
    //
    // `small_map_` covers all raw keys below `max_num_small_keys` rather than
    // only up to `max_raw_key`, to reduce lookups in `large_map_`.
    num_small_keys_ = max_num_small_keys;
    RIEGELI_ASSUME_EQ(small_map_, nullptr) << "Initialization";
    small_map_ =
        std::make_unique<const Value* absl_nullable[]>(max_num_small_keys);
    num_small_values = 0;
    for (auto iter = first; iter != last; ++iter) {
      num_small_values += ToRawKey(iter->first) < max_num_small_keys ? 1 : 0;
    }
    RIEGELI_ASSERT_LT(num_small_values, size)
        << "Some keys should have been too large for small_map_";
    RIEGELI_ASSUME_EQ(small_values_, nullptr) << "Initialization";
    if (num_small_values > 0) {
      small_values_ =
          std::make_unique<small_int_map_internal::DelayedConstructor<Value>[]>(
              num_small_values);
    }
    RIEGELI_ASSUME_EQ(large_map_, nullptr) << "Initialization";
    large_map_ = std::make_unique<absl::flat_hash_map<Key, Value>>();
    large_map_->reserve(size - num_small_values);
    small_values_index = 0;
    for (auto iter = first; iter != last; ++iter) {
      // `(*iter).second` rather than `iter->second` allows moving from a move
      // iterator.
      if (ToRawKey(iter->first) < max_num_small_keys) {
        RIEGELI_ASSERT_EQ(small_map_[ToRawKey(iter->first)], nullptr)
            << "Failed precondition of SmallIntMap initialization: "
               "duplicate key: "
            << iter->first;
        small_map_[ToRawKey(iter->first)] =
            &small_values_[small_values_index++].emplace((*iter).second);
      } else {
        const auto inserted =
            large_map_->try_emplace(iter->first, (*iter).second);
        RIEGELI_ASSERT(inserted.second)
            << "Failed precondition of SmallIntMap initialization: "
               "duplicate key: "
            << iter->first;
      }
    }
  }
  RIEGELI_ASSERT_EQ(small_values_index, num_small_values)
      << "The whole small_values_ array should have been filled";
}

template <typename Key, typename Value, Key expected_min_key,
          size_t array_capacity>
ABSL_ATTRIBUTE_ALWAYS_INLINE const Value* absl_nullable
SmallIntMap<Key, Value, expected_min_key, array_capacity>::Find(Key key) const {
  RIEGELI_ASSERT(num_small_keys_ != kPoisonedNumSmallKeys ||
                 small_map_ != nullptr)
      << "Moved-from SmallIntMap";
  if (ToRawKey(key) < num_small_keys_) return small_map_[ToRawKey(key)];
  if (ABSL_PREDICT_TRUE(large_map_ == nullptr)) return nullptr;
  return FindSlow(key);
}

template <typename Key, typename Value, Key expected_min_key,
          size_t array_capacity>
const Value* absl_nullable
SmallIntMap<Key, Value, expected_min_key, array_capacity>::FindSlow(
    Key key) const {
  const auto iter = large_map_->find(key);
  if (iter == large_map_->end()) return nullptr;
  return &iter->second;
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_SMALL_INT_MAP_H_
