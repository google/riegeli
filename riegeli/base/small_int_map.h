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
#include "riegeli/base/iterable.h"
#include "riegeli/base/type_traits.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

namespace small_int_map_internal {

template <typename T>
class DelayedConstructor;
template <typename T>
class SizedDeleter;
template <typename T>
using SizedArray = std::unique_ptr<T[], SizedDeleter<T>>;
template <typename T>
SizedArray<T> MakeSizedArray(size_t size);

template <typename Key, typename Value, Key expected_min_key,
          size_t array_capacity>
class SmallIntMapImpl {
 public:
  static size_t max_size();

  ABSL_ATTRIBUTE_REINITIALIZES void Reset();

  const Value* absl_nullable Find(Key key) const;

 protected:
  SmallIntMapImpl() = default;

  SmallIntMapImpl(const SmallIntMapImpl& that) noexcept;
  SmallIntMapImpl& operator=(const SmallIntMapImpl& that) noexcept;

  SmallIntMapImpl(SmallIntMapImpl&& that) = default;
  SmallIntMapImpl& operator=(SmallIntMapImpl&& that) = default;

  template <typename Src>
  void Initialize(Src&& src);

 private:
  // The raw key corresponding to a key is the key minus `expected_min_key`,
  // represented in an unsigned type with wrap-around.
  using RawKey = std::common_type_t<std::make_unsigned_t<Key>, size_t>;

  using SmallValues = SizedArray<DelayedConstructor<Value>>;
  using SmallMap = SizedArray<const Value* absl_nullable>;
  using LargeMap = absl::flat_hash_map<Key, Value>;

  static constexpr int kInverseMinLoadFactor = 4;  // 25%.

  static RawKey ToRawKey(Key key) {
    // Wrap-around is not an error.
    return static_cast<RawKey>(key) - static_cast<RawKey>(expected_min_key);
  }

  template <typename Src, typename Iterator>
  void Optimize(Iterator first, Iterator last, size_t size);

  absl_nullable SmallValues CopySmallValues() const;
  absl_nullable SmallMap CopySmallMap(
      const DelayedConstructor<Value>* absl_nullable dest_values) const;
  absl_nullable std::unique_ptr<LargeMap> CopyLargeMap() const;

  const Value* absl_nullable FindSlow(Key key) const;

  // Stores values for `small_map_`, in no particular order.
  absl_nullable SmallValues small_values_;
  // Indexed by raw key below `small_map_.get_deleter().size()`. Elements
  // corresponding to present values point to elements of `small_values_`.
  // The remaining elements are `nullptr`.
  absl_nullable SmallMap small_map_;
  // If not `nullptr`, stores the mapping for keys too large for `small_map_`.
  // Uses `std::unique_ptr` rather than `std::optional` to reduce memory usage
  // in the common case when `large_map_` is not used.
  absl_nullable std::unique_ptr<LargeMap> large_map_;
};

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
class SmallIntMap
    : public small_int_map_internal::SmallIntMapImpl<
          Key, Value, expected_min_key, array_capacity>,
      private ConditionallyConstructible<std::is_copy_constructible_v<Value>,
                                         true>,
      private ConditionallyAssignable<std::is_copy_constructible_v<Value>,
                                      true> {
 public:
  // Constructs an empty `SmallIntMap`.
  SmallIntMap() = default;

  // Builds `SmallIntMap` from an iterable `src`. Moves values if `src` is an
  // rvalue which owns its elements.
  template <typename Src,
            std::enable_if_t<
                std::conjunction_v<
                    NotSameRef<SmallIntMap, Src>, IsForwardIterable<Src>,
                    IsIterableOfPairs<Src, Key, Value>,
                    std::conditional_t<HasMovableElements<Src>::value,
                                       std::is_move_constructible<Value>,
                                       std::is_copy_constructible<Value>>>,
                int> = 0>
  explicit SmallIntMap(Src&& src) {
    this->Initialize(std::forward<Src>(src));
  }

  // Builds `SmallIntMap` from an initializer list.
  /*implicit*/ SmallIntMap(std::initializer_list<std::pair<Key, Value>> src) {
    this->Initialize(src);
  }

  SmallIntMap(const SmallIntMap& that) = default;
  SmallIntMap& operator=(const SmallIntMap& that) = default;

  SmallIntMap(SmallIntMap&& that) = default;
  SmallIntMap& operator=(SmallIntMap&& that) = default;

  // Makes `*this` equivalent to a newly constructed `SmallIntMap`.
  using SmallIntMap::SmallIntMapImpl::Reset;
  template <typename Src,
            std::enable_if_t<
                std::conjunction_v<
                    NotSameRef<SmallIntMap, Src>, IsForwardIterable<Src>,
                    IsIterableOfPairs<Src, Key, Value>,
                    std::conditional_t<HasMovableElements<Src>::value,
                                       std::is_move_constructible<Value>,
                                       std::is_copy_constructible<Value>>>,
                int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Src&& src) {
    this->Reset();
    this->Initialize(std::forward<Src>(src));
  }
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      std::initializer_list<std::pair<Key, Value>> src) {
    this->Reset();
    this->Initialize(src);
  }
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

  T& operator*() ABSL_ATTRIBUTE_LIFETIME_BOUND { return value_; }
  const T& operator*() const ABSL_ATTRIBUTE_LIFETIME_BOUND { return value_; }

 private:
  union {
    T value_;
  };
};

template <typename T>
class SizedDeleter {
 public:
  static size_t max_size() {
    return std::allocator_traits<std::allocator<T>>::max_size(
        std::allocator<T>());
  }

  SizedDeleter() = default;

  explicit SizedDeleter(size_t size) : size_(size) {}

  SizedDeleter(SizedDeleter&& that) noexcept
      : size_(std::exchange(that.size_, kPoisonedSize)) {}

  SizedDeleter& operator=(SizedDeleter&& that) noexcept {
    size_ = std::exchange(that.size_, kPoisonedSize);
    return *this;
  }

  void operator()(T* ptr) const {
    for (T* iter = ptr + size_; iter != ptr;) {
      --iter;
      iter->~T();
    }
    std::allocator<T>().deallocate(ptr, size_);
  }

  size_t size() const { return size_; }

  // If the pointer associated with this deleter is `nullptr`, returns `true`
  // when this deleter is moved-from. Otherwise the result is meaningless.
  bool IsMovedFromIfNull() const { return size_ == kPoisonedSize; }

 private:
  // A moved-from `SizedDeleter` has `size_ == kPoisonedSize`. In debug mode
  // this asserts against using a moved-from object. In non-debug mode, if the
  // key is not too large, then this triggers a null pointer dereference with an
  // offset up to 1MB, which is assumed to reliably crash.
  static constexpr size_t kPoisonedSize = (size_t{1} << 20) / sizeof(T);

  size_t size_ = 0;
};

template <typename T>
inline SizedArray<T> MakeSizedArray(size_t size) {
  T* const ptr = std::allocator<T>().allocate(size);
  T* const end = ptr + size;
  for (T* iter = ptr; iter != end; ++iter) {
    new (iter) T();
  }
  return SizedArray<T>(ptr, SizedDeleter<T>(size));
}

template <typename Key, typename Value, Key expected_min_key,
          size_t array_capacity>
inline size_t
SmallIntMapImpl<Key, Value, expected_min_key, array_capacity>::max_size() {
  return UnsignedMin(SizedDeleter<const Value* absl_nullable>::max_size(),
                     SizedDeleter<DelayedConstructor<Value>>::max_size()) /
         kInverseMinLoadFactor;
}

template <typename Key, typename Value, Key expected_min_key,
          size_t array_capacity>
void SmallIntMapImpl<Key, Value, expected_min_key, array_capacity>::Reset() {
  small_values_ = SmallValues();
  small_map_ = SmallMap();
  large_map_.reset();
}

template <typename Key, typename Value, Key expected_min_key,
          size_t array_capacity>
template <typename Src>
void SmallIntMapImpl<Key, Value, expected_min_key, array_capacity>::Initialize(
    Src&& src) {
  using std::begin;
  using std::end;
  if constexpr (IterableHasSize<Src>::value) {
    using std::size;
    const size_t src_size = size(src);
    RIEGELI_ASSERT_EQ(src_size,
                      IntCast<size_t>(std::distance(begin(src), end(src))))
        << "Failed precondition of SmallIntMap initialization: "
           "size does not match the distance between iterators";
    if (src_size > 0) Optimize<Src>(begin(src), end(src), src_size);
  } else {
    auto first = begin(src);
    auto last = end(src);
    const size_t src_size = IntCast<size_t>(std::distance(first, last));
    if (src_size > 0) Optimize<Src>(first, last, src_size);
  }
#if RIEGELI_DEBUG
  // Detect building `SmallIntMap` from a moved-from `src` if possible.
  if constexpr (std::conjunction_v<std::negation<std::is_reference<Src>>,
                                   std::is_move_constructible<Src>>) {
    ABSL_ATTRIBUTE_UNUSED Src moved = std::forward<Src>(src);
  }
#endif
}

template <typename Key, typename Value, Key expected_min_key,
          size_t array_capacity>
template <typename Src, typename Iterator>
void SmallIntMapImpl<Key, Value, expected_min_key, array_capacity>::Optimize(
    Iterator first, Iterator last, size_t size) {
  RIEGELI_ASSERT_GE(size, 0u)
      << "Failed precondition of SmallIntMapImpl::Optimize(): "
         "an empty map must have been handled before";
  RIEGELI_CHECK_LE(size, max_size())
      << "Failed precondition of SmallIntMap initialization: "
         "size overflow";
  RawKey max_raw_key = 0;
  for (auto iter = first; iter != last; ++iter) {
    max_raw_key = UnsignedMax(max_raw_key, ToRawKey(iter->first));
  }
  const size_t max_num_small_keys =
      UnsignedMax(array_capacity, size * kInverseMinLoadFactor);
  size_t small_values_index;
  if (max_raw_key < max_num_small_keys) {
    // All keys are suitable for `small_map_`. `large_map_` is not used.
    //
    // There is no need for `small_map_` to cover raw keys larger than
    // `max_raw_key` because their lookup is fast if `large_map_` is `nullptr`.
    RIEGELI_ASSUME_EQ(small_values_, nullptr) << "Initialization";
    small_values_ = MakeSizedArray<DelayedConstructor<Value>>(size);
    RIEGELI_ASSUME_EQ(small_map_, nullptr) << "Initialization";
    small_map_ = MakeSizedArray<const Value* absl_nullable>(
        IntCast<size_t>(max_raw_key) + 1);
    small_values_index = 0;
    for (auto iter = first; iter != last; ++iter) {
      // `(*iter).second` rather than `iter->second` allows moving from a move
      // iterator.
      RIEGELI_ASSERT_EQ(small_map_[ToRawKey(iter->first)], nullptr)
          << "Failed precondition of SmallIntMap initialization: "
             "duplicate key: "
          << iter->first;
      small_map_[ToRawKey(iter->first)] =
          &small_values_[small_values_index++].emplace(
              (*MaybeMakeMoveIterator<Src>(iter)).second);
    }
  } else {
    // Some keys are too large for `small_map_`. `large_map_` is used.
    //
    // `small_map_` covers all raw keys below `max_num_small_keys` rather than
    // only up to `max_raw_key`, to reduce lookups in `large_map_`.
    size_t num_small_values = 0;
    for (auto iter = first; iter != last; ++iter) {
      num_small_values += ToRawKey(iter->first) < max_num_small_keys ? 1 : 0;
    }
    RIEGELI_ASSERT_LT(num_small_values, size)
        << "Some keys should have been too large for small_map_";
    RIEGELI_ASSUME_EQ(small_values_, nullptr) << "Initialization";
    if (num_small_values > 0) {
      small_values_ =
          MakeSizedArray<DelayedConstructor<Value>>(num_small_values);
    }
    RIEGELI_ASSUME_EQ(small_map_, nullptr) << "Initialization";
    small_map_ = MakeSizedArray<const Value* absl_nullable>(max_num_small_keys);
    RIEGELI_ASSUME_EQ(large_map_, nullptr) << "Initialization";
    large_map_ = std::make_unique<LargeMap>();
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
            &small_values_[small_values_index++].emplace(
                (*MaybeMakeMoveIterator<Src>(iter)).second);
      } else {
        const auto inserted = large_map_->try_emplace(
            iter->first, (*MaybeMakeMoveIterator<Src>(iter)).second);
        RIEGELI_ASSERT(inserted.second)
            << "Failed precondition of SmallIntMap initialization: "
               "duplicate key: "
            << iter->first;
      }
    }
  }
  RIEGELI_ASSERT_EQ(small_values_index, small_values_.get_deleter().size())
      << "The whole small_values_ array should have been filled";
}

template <typename Key, typename Value, Key expected_min_key,
          size_t array_capacity>
SmallIntMapImpl<Key, Value, expected_min_key, array_capacity>::SmallIntMapImpl(
    const SmallIntMapImpl& that) noexcept
    : small_values_(that.CopySmallValues()),
      small_map_(that.CopySmallMap(small_values_.get())),
      large_map_(that.CopyLargeMap()) {}

template <typename Key, typename Value, Key expected_min_key,
          size_t array_capacity>
SmallIntMapImpl<Key, Value, expected_min_key, array_capacity>&
SmallIntMapImpl<Key, Value, expected_min_key, array_capacity>::operator=(
    const SmallIntMapImpl& that) noexcept {
  absl_nullable SmallValues new_small_values = that.CopySmallValues();
  small_map_ = that.CopySmallMap(new_small_values.get());
  small_values_ = std::move(new_small_values);
  large_map_ = that.CopyLargeMap();
  return *this;
}

template <typename Key, typename Value, Key expected_min_key,
          size_t array_capacity>
auto SmallIntMapImpl<Key, Value, expected_min_key,
                     array_capacity>::CopySmallValues() const ->
    absl_nullable SmallValues {
  if (small_values_ == nullptr) return nullptr;
  SmallValues dest_ptr = MakeSizedArray<DelayedConstructor<Value>>(
      small_values_.get_deleter().size());
  DelayedConstructor<Value>* src_iter = small_values_.get();
  DelayedConstructor<Value>* const end =
      dest_ptr.get() + dest_ptr.get_deleter().size();
  for (DelayedConstructor<Value>* dest_iter = dest_ptr.get(); dest_iter != end;
       ++dest_iter) {
    dest_iter->emplace(**src_iter);
    ++src_iter;
  }
  return dest_ptr;
}

template <typename Key, typename Value, Key expected_min_key,
          size_t array_capacity>
auto SmallIntMapImpl<Key, Value, expected_min_key, array_capacity>::
    CopySmallMap(const DelayedConstructor<Value>* absl_nullable dest_values)
        const -> absl_nullable SmallMap {
  if (small_map_ == nullptr) return nullptr;
  const DelayedConstructor<Value>* const absl_nullable src_values =
      small_values_.get();
  SmallMap dest_ptr = MakeSizedArray<const Value* absl_nullable>(
      small_map_.get_deleter().size());
  const Value* absl_nullable* src_iter = small_map_.get();
  const Value* absl_nullable* const end =
      dest_ptr.get() + dest_ptr.get_deleter().size();
  for (const Value* absl_nullable* dest_iter = dest_ptr.get(); dest_iter != end;
       ++dest_iter) {
    if (*src_iter != nullptr) {
      *dest_iter = reinterpret_cast<const Value*>(
          reinterpret_cast<const char*>(dest_values) +
          ((reinterpret_cast<const char*>(*src_iter) -
            reinterpret_cast<const char*>(src_values))));
    }
    ++src_iter;
  }
  return dest_ptr;
}

template <typename Key, typename Value, Key expected_min_key,
          size_t array_capacity>
auto SmallIntMapImpl<Key, Value, expected_min_key,
                     array_capacity>::CopyLargeMap() const ->
    absl_nullable std::unique_ptr<LargeMap> {
  if (large_map_ == nullptr) return nullptr;
  return std::make_unique<LargeMap>(*large_map_);
}

template <typename Key, typename Value, Key expected_min_key,
          size_t array_capacity>
ABSL_ATTRIBUTE_ALWAYS_INLINE const Value* absl_nullable
SmallIntMapImpl<Key, Value, expected_min_key, array_capacity>::Find(
    Key key) const {
  RIEGELI_ASSERT(!small_map_.get_deleter().IsMovedFromIfNull() ||
                 small_map_ != nullptr)
      << "Moved-from SmallIntMap";
  if (ToRawKey(key) < small_map_.get_deleter().size()) {
    return small_map_[ToRawKey(key)];
  }
  if (ABSL_PREDICT_TRUE(large_map_ == nullptr)) return nullptr;
  return FindSlow(key);
}

template <typename Key, typename Value, Key expected_min_key,
          size_t array_capacity>
const Value* absl_nullable
SmallIntMapImpl<Key, Value, expected_min_key, array_capacity>::FindSlow(
    Key key) const {
  const auto iter = large_map_->find(key);
  if (iter == large_map_->end()) return nullptr;
  return &iter->second;
}

}  // namespace small_int_map_internal

}  // namespace riegeli

#endif  // RIEGELI_BASE_SMALL_INT_MAP_H_
