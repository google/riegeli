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

#ifndef RIEGELI_BASE_HYBRID_DIRECT_SET_H_
#define RIEGELI_BASE_HYBRID_DIRECT_SET_H_

#include <stddef.h>

#include <cstring>
#include <functional>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <optional>
#include <type_traits>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/container/flat_hash_set.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/hybrid_direct_common.h"  // IWYU pragma: export
#include "riegeli/base/hybrid_direct_internal.h"
#include "riegeli/base/iterable.h"
#include "riegeli/base/type_traits.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

// `HybridDirectSet` is a set optimized for keys being mostly small integers,
// especially dense near zero. It supports only lookups and iteration, but no
// incremental modification.
//
// It stores a part of the set covering some range of small keys in an array.
// The remaining keys are stored in an `absl::flat_hash_set`.
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
// In the case of duplicate keys, one is retained.
template <typename Key, typename Traits = HybridDirectTraits<Key>>
class HybridDirectSet : public WithEqual<HybridDirectSet<Key, Traits>> {
 private:
  template <typename Src, typename Enable = void>
  struct HasCompatibleKeys : std::false_type {};
  template <typename Src>
  struct HasCompatibleKeys<
      Src,
      std::enable_if_t<std::is_convertible_v<ElementTypeT<const Src&>, Key>>>
      : std::true_type {};

  template <typename Src, typename KeyProjection, typename Enable = void>
  struct HasProjectibleKeys : std::false_type {};
  template <typename Src, typename KeyProjection>
  struct HasProjectibleKeys<
      Src, KeyProjection,
      std::enable_if_t<std::is_convertible_v<
          std::invoke_result_t<const KeyProjection&, ElementTypeT<const Src&>>,
          Key>>> : std::true_type {};

  template <typename Src>
  struct DefaultKeyProjection {
    Key operator()(ElementTypeT<const Src&> key) const { return key; }
  };

 public:
  using value_type = Key;
  using reference = Key;
  using const_reference = Key;
  using pointer = void;
  using const_pointer = void;
  class iterator;
  using const_iterator = iterator;
  using size_type = size_t;
  using difference_type = ptrdiff_t;

  static size_t max_size();

  // Constructs an empty `HybridDirectSet`.
  HybridDirectSet() = default;

  // Builds `HybridDirectSet` from an iterable `src`.
  template <
      typename Src,
      std::enable_if_t<
          std::conjunction_v<NotSameRef<HybridDirectSet, Src>,
                             IsForwardIterable<Src>, HasCompatibleKeys<Src>>,
          int> = 0>
  explicit HybridDirectSet(const Src& src) {
    Initialize(src, DefaultKeyProjection<Src>(),
               kHybridDirectDefaultDirectCapacity);
  }
  template <typename Src,
            std::enable_if_t<std::conjunction_v<IsForwardIterable<Src>,
                                                HasCompatibleKeys<Src>>,
                             int> = 0>
  explicit HybridDirectSet(const Src& src, size_t direct_capacity) {
    Initialize(src, DefaultKeyProjection<Src>(), direct_capacity);
  }

  // Builds `HybridDirectSet` from an initializer list.
  /*implicit*/ HybridDirectSet(
      std::initializer_list<Key> src,
      size_t direct_capacity = kHybridDirectDefaultDirectCapacity) {
    Initialize(src, DefaultKeyProjection<decltype(src)>(), direct_capacity);
  }

  // Builds `HybridDirectSet` from an iterable `src`.
  //
  // Keys are extracted using `key_projection()`. `key_projection()` may be
  // called multiple times for each key so it should be efficient.
  template <
      typename Src, typename KeyProjection = DefaultKeyProjection<Src>,
      std::enable_if_t<
          std::conjunction_v<
              std::negation<std::is_convertible<KeyProjection, size_t>>,
              IsForwardIterable<Src>, HasProjectibleKeys<Src, KeyProjection>>,
          int> = 0>
  explicit HybridDirectSet(
      const Src& src, const KeyProjection& key_projection,
      size_t direct_capacity = kHybridDirectDefaultDirectCapacity) {
    Initialize(src, key_projection, direct_capacity);
  }

  HybridDirectSet(const HybridDirectSet& that) noexcept;
  HybridDirectSet& operator=(const HybridDirectSet& that) noexcept;

  HybridDirectSet(HybridDirectSet&& that) = default;
  HybridDirectSet& operator=(HybridDirectSet&& that) = default;

  // Makes `*this` equivalent to a newly constructed `HybridDirectSet`.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset();
  template <typename Src,
            std::enable_if_t<std::conjunction_v<IsForwardIterable<Src>,
                                                HasCompatibleKeys<Src>>,
                             int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      const Src& src,
      size_t direct_capacity = kHybridDirectDefaultDirectCapacity) {
    Reset();
    Initialize(src, DefaultKeyProjection<Src>(), direct_capacity);
  }
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      std::initializer_list<Key> src,
      size_t direct_capacity = kHybridDirectDefaultDirectCapacity) {
    Reset();
    Initialize(src, DefaultKeyProjection<decltype(src)>(), direct_capacity);
  }
  template <
      typename Src, typename KeyProjection = DefaultKeyProjection<Src>,
      std::enable_if_t<
          std::conjunction_v<
              std::negation<std::is_convertible<KeyProjection, size_t>>,
              IsForwardIterable<Src>, HasProjectibleKeys<Src, KeyProjection>>,
          int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      const Src& src, const KeyProjection& key_projection,
      size_t direct_capacity = kHybridDirectDefaultDirectCapacity) {
    Reset();
    Initialize(src, key_projection, direct_capacity);
  }

  bool contains(Key key) const;

  bool empty() const { return size_ == 0; }
  size_t size() const { return size_; }

  iterator begin() ABSL_ATTRIBUTE_LIFETIME_BOUND;
  const_iterator begin() const ABSL_ATTRIBUTE_LIFETIME_BOUND;
  const_iterator cbegin() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return begin();
  }
  iterator end() ABSL_ATTRIBUTE_LIFETIME_BOUND;
  const_iterator end() const ABSL_ATTRIBUTE_LIFETIME_BOUND;
  const_iterator cend() const ABSL_ATTRIBUTE_LIFETIME_BOUND { return end(); }

  friend bool operator==(const HybridDirectSet& a, const HybridDirectSet& b) {
    return Equal(a, b);
  }

 private:
  using RawKey = std::decay_t<decltype(Traits::ToRawKey(std::declval<Key>()))>;
  static_assert(std::is_unsigned_v<RawKey>);

  using DirectSet = hybrid_direct_internal::SizedArray<bool>;
  using SlowSet = absl::flat_hash_set<RawKey>;

  static constexpr int kInverseMinLoadFactor = 4;  // 25%.

  template <typename Src, typename KeyProjection>
  void Initialize(const Src& src, const KeyProjection& key_projection,
                  size_t direct_capacity);

  template <typename Iterator, typename KeyProjection>
  void Optimize(Iterator first, Iterator last, size_t size,
                const KeyProjection& key_projection, size_t direct_capacity);

  absl_nullable DirectSet CopyDirectSet() const;
  absl_nullable std::unique_ptr<SlowSet> CopySlowSet() const;

  size_t FirstRawKey() const;

  size_t capacity() const {
    return direct_set_.get_deleter().size() +
           (slow_set_ == nullptr ? 0 : slow_set_->capacity());
  }

  static bool Equal(const HybridDirectSet& a, const HybridDirectSet& b);

  // Indexed by raw key below `direct_set_.get_deleter().size()`.
  absl_nullable DirectSet direct_set_;
  // If not `nullptr`, stores the set of keys too large for `direct_set_`.
  // Uses `std::unique_ptr` rather than `std::optional` to reduce memory usage
  // in the common case when `slow_set_` is not used.
  //
  // Invariant: if `slow_set_ != nullptr` then `!slow_set_->empty()`.
  absl_nullable std::unique_ptr<SlowSet> slow_set_;
  size_t size_ = 0;
};

template <typename Key, typename Traits>
class HybridDirectSet<Key, Traits>::iterator : public WithEqual<iterator> {
 public:
  using iterator_concept = std::forward_iterator_tag;
  // `iterator_category` is only `std::input_iterator_tag` because the
  // `LegacyForwardIterator` requirement and above require `reference` to be
  // a true reference type.
  using iterator_category = std::input_iterator_tag;
  using value_type = Key;
  using reference = Key;
  using pointer = void;
  using difference_type = ptrdiff_t;

  iterator() = default;

  iterator(const iterator& that) = default;
  iterator& operator=(const iterator& that) = default;

  reference operator*() const {
    if (ABSL_PREDICT_TRUE(raw_key_complement_ > 0)) {
      return Traits::FromRawKey(
          IntCast<RawKey>(direct_set_size_ - raw_key_complement_));
    }
    return Traits::FromRawKey(**slow_set_iter_);
  }
  iterator& operator++() {
    if (ABSL_PREDICT_TRUE(raw_key_complement_ > 0)) {
      do {
        --raw_key_complement_;
        if (ABSL_PREDICT_FALSE(raw_key_complement_ == 0)) break;
      } while (!*(direct_set_end_ - raw_key_complement_));
    } else {
      ++*slow_set_iter_;
    }
    return *this;
  }
  iterator operator++(int) {
    iterator result = *this;
    ++*this;
    return result;
  }

  friend bool operator==(iterator a, iterator b) {
    RIEGELI_ASSERT_EQ(a.direct_set_end_, b.direct_set_end_)
        << "Failed precondition of operator==(HybridDirectSet::iterator): "
           "incomparable iterators";
    RIEGELI_ASSERT_EQ(a.direct_set_size_, b.direct_set_size_)
        << "Failed precondition of operator==(HybridDirectSet::iterator): "
           "incomparable iterators";
    RIEGELI_ASSERT_EQ(a.slow_set_iter_ != std::nullopt,
                      b.slow_set_iter_ != std::nullopt)
        << "Failed precondition of operator==(HybridDirectSet::iterator): "
           "incomparable iterators";
    if (a.raw_key_complement_ != b.raw_key_complement_) return false;
    if (ABSL_PREDICT_TRUE(a.slow_set_iter_ == std::nullopt)) return true;
    return *a.slow_set_iter_ == *b.slow_set_iter_;
  }

 private:
  friend class HybridDirectSet;

  explicit iterator(const HybridDirectSet* set ABSL_ATTRIBUTE_LIFETIME_BOUND,
                    size_t raw_key_complement)
      : direct_set_end_(set->direct_set_.get() +
                        set->direct_set_.get_deleter().size()),
        direct_set_size_(set->direct_set_.get_deleter().size()),
        raw_key_complement_(raw_key_complement) {}

  explicit iterator(const HybridDirectSet* set ABSL_ATTRIBUTE_LIFETIME_BOUND,
                    size_t raw_key_complement,
                    typename SlowSet::const_iterator slow_set_iter)
      : direct_set_end_(set->direct_set_.get() +
                        set->direct_set_.get_deleter().size()),
        direct_set_size_(set->direct_set_.get_deleter().size()),
        raw_key_complement_(raw_key_complement),
        slow_set_iter_(slow_set_iter) {}

  // The end of the `direct_set_` array.
  //
  // Counting backwards simplifies checking for iteration over `direct_set_`.
  const bool* absl_nullable direct_set_end_ = nullptr;
  // `direct_set_.get_deleter().size()`.
  size_t direct_set_size_ = 0;
  // `direct_set_size_ - raw_key` when iterating over `direct_set_`, otherwise
  // 0.
  //
  // Invariant: if `raw_key_complement > 0` then
  // `*(direct_set_end_ - raw_key_complement_) != nullptr`.
  //
  // Counting backwards simplifies checking for iteration over `direct_set_`.
  size_t raw_key_complement_ = 0;
  // Iterator over `*slow_set_` when `slow_set_ != nullptr`, otherwise
  // `std::nullopt`.
  //
  // Invariant: if `raw_key_complement_ > 0` then
  // `slow_set_iter_ == std::nullopt` or
  // `slow_set_iter_ == slow_set_->begin()`.
  //
  // Distinguishing `std::nullopt` instead of using the default-constructed
  // `SlowSet::iterator` makes the common case of `operator==` faster by
  // reducing usage of `SlowSet` iterators.
  std::optional<typename SlowSet::const_iterator> slow_set_iter_;
};

// Implementation details follow.

template <typename Key, typename Traits>
inline size_t HybridDirectSet<Key, Traits>::max_size() {
  return hybrid_direct_internal::SizedDeleter<bool>::max_size() /
         kInverseMinLoadFactor;
}

template <typename Key, typename Traits>
HybridDirectSet<Key, Traits>::HybridDirectSet(
    const HybridDirectSet& that) noexcept
    : direct_set_(that.CopyDirectSet()),
      slow_set_(that.CopySlowSet()),
      size_(that.size_) {}

template <typename Key, typename Traits>
HybridDirectSet<Key, Traits>& HybridDirectSet<Key, Traits>::operator=(
    const HybridDirectSet& that) noexcept {
  direct_set_ = that.CopyDirectSet();
  slow_set_ = that.CopySlowSet();
  size_ = that.size_;
  return *this;
}

template <typename Key, typename Traits>
void HybridDirectSet<Key, Traits>::Reset() {
  direct_set_ = DirectSet();
  slow_set_.reset();
  size_ = 0;
}

template <typename Key, typename Traits>
template <typename Src, typename KeyProjection>
void HybridDirectSet<Key, Traits>::Initialize(
    const Src& src, const KeyProjection& key_projection,
    size_t direct_capacity) {
  using std::begin;
  using std::end;
  if constexpr (IterableHasSize<Src>::value) {
    using std::size;
    const size_t src_size = size(src);
    RIEGELI_ASSERT_EQ(src_size,
                      IntCast<size_t>(std::distance(begin(src), end(src))))
        << "Failed precondition of HybridDirectSet initialization: "
           "size does not match the distance between iterators";
    if (src_size > 0) {
      Optimize(begin(src), end(src), src_size, key_projection, direct_capacity);
    }
  } else {
    auto first = begin(src);
    auto last = end(src);
    const size_t src_size = IntCast<size_t>(std::distance(first, last));
    if (src_size > 0)
      Optimize(first, last, src_size, key_projection, direct_capacity);
  }
}

template <typename Key, typename Traits>
template <typename Iterator, typename KeyProjection>
void HybridDirectSet<Key, Traits>::Optimize(Iterator first, Iterator last,
                                            size_t size,
                                            const KeyProjection& key_projection,
                                            size_t direct_capacity) {
  RIEGELI_ASSERT_GE(size, 0u)
      << "Failed precondition of HybridDirectSet::Optimize(): "
         "an empty map must have been handled before";
  RIEGELI_CHECK_LE(size, max_size())
      << "Failed precondition of HybridDirectSet initialization: "
         "size overflow";
  RawKey max_raw_key = 0;
  for (auto iter = first; iter != last; ++iter) {
    max_raw_key = UnsignedMax(
        max_raw_key, Traits::ToRawKey(std::invoke(key_projection, *iter)));
  }
  const size_t max_num_direct_keys =
      UnsignedMax(direct_capacity, size * kInverseMinLoadFactor);
  size_ = size;
  if (max_raw_key < max_num_direct_keys) {
    // All keys are suitable for `direct_set_`. `slow_set_` is not used.
    //
    // There is no need for `direct_set_` to cover raw keys larger than
    // `max_raw_key` because their lookup is fast if `slow_set_` is `nullptr`.
    hybrid_direct_internal::AssignToAssumedNull(
        direct_set_, hybrid_direct_internal::MakeSizedArray<bool>(
                         IntCast<size_t>(max_raw_key) + 1));
    for (auto iter = first; iter != last; ++iter) {
      const RawKey raw_key =
          Traits::ToRawKey(std::invoke(key_projection, *iter));
      if (ABSL_PREDICT_FALSE(direct_set_[raw_key])) --size_;
      direct_set_[raw_key] = true;
    }
  } else {
    // Some keys are too large for `direct_set_`. `slow_set_` is used.
    //
    // `direct_set_` covers all raw keys below `max_num_direct_keys` rather than
    // only up to `max_raw_key`, to reduce lookups in `slow_set_`.
    hybrid_direct_internal::AssignToAssumedNull(
        direct_set_,
        hybrid_direct_internal::MakeSizedArray<bool>(max_num_direct_keys));
    size_t num_slow_elements = size;
    for (auto iter = first; iter != last; ++iter) {
      num_slow_elements -= Traits::ToRawKey(std::invoke(
                               key_projection, *iter)) < max_num_direct_keys
                               ? 1
                               : 0;
    }
    RIEGELI_ASSERT_GT(num_slow_elements, 0u)
        << "Some keys should have been too large for direct_set_";
    hybrid_direct_internal::AssignToAssumedNull(slow_set_,
                                                std::make_unique<SlowSet>());
    slow_set_->reserve(num_slow_elements);
    for (auto iter = first; iter != last; ++iter) {
      const RawKey raw_key =
          Traits::ToRawKey(std::invoke(key_projection, *iter));
      if (raw_key < max_num_direct_keys) {
        if (ABSL_PREDICT_FALSE(direct_set_[raw_key])) --size_;
        direct_set_[raw_key] = true;
      } else {
        const auto inserted = slow_set_->insert(raw_key);
        if (ABSL_PREDICT_FALSE(!inserted.second)) --size_;
      }
    }
  }
}

template <typename Key, typename Traits>
auto HybridDirectSet<Key, Traits>::CopyDirectSet() const ->
    absl_nullable DirectSet {
  if (direct_set_ == nullptr) return nullptr;
  DirectSet dest_ptr = hybrid_direct_internal::MakeSizedArrayForOverwrite<bool>(
      direct_set_.get_deleter().size());
  std::memcpy(dest_ptr.get(), direct_set_.get(),
              dest_ptr.get_deleter().size() * sizeof(bool));
  return dest_ptr;
}

template <typename Key, typename Traits>
auto HybridDirectSet<Key, Traits>::CopySlowSet() const ->
    absl_nullable std::unique_ptr<SlowSet> {
  if (slow_set_ == nullptr) return nullptr;
  return std::make_unique<SlowSet>(*slow_set_);
}

template <typename Key, typename Traits>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool HybridDirectSet<Key, Traits>::contains(
    Key key) const {
  RIEGELI_ASSERT(!direct_set_.get_deleter().IsMovedFromIfNull() ||
                 direct_set_ != nullptr)
      << "Moved-from HybridDirectSet";
  const RawKey raw_key = Traits::ToRawKey(key);
  if (raw_key < direct_set_.get_deleter().size()) return direct_set_[raw_key];
  if (ABSL_PREDICT_TRUE(slow_set_ == nullptr)) return false;
  return slow_set_->contains(raw_key);
}

template <typename Key, typename Traits>
inline size_t HybridDirectSet<Key, Traits>::FirstRawKey() const {
  const size_t direct_set_size = direct_set_.get_deleter().size();
  for (size_t raw_key = 0; raw_key < direct_set_size; ++raw_key) {
    if (direct_set_[raw_key]) return raw_key;
  }
  return direct_set_size;
}

template <typename Key, typename Traits>
inline auto HybridDirectSet<Key, Traits>::begin() ABSL_ATTRIBUTE_LIFETIME_BOUND
    -> iterator {
  const size_t raw_key_complement =
      direct_set_.get_deleter().size() - FirstRawKey();
  if (ABSL_PREDICT_TRUE(slow_set_ == nullptr)) {
    return iterator(this, raw_key_complement);
  }
  return iterator(this, raw_key_complement, slow_set_->begin());
}

template <typename Key, typename Traits>
inline auto HybridDirectSet<Key, Traits>::begin() const
    ABSL_ATTRIBUTE_LIFETIME_BOUND -> const_iterator {
  const size_t raw_key_complement =
      direct_set_.get_deleter().size() - FirstRawKey();
  if (ABSL_PREDICT_TRUE(slow_set_ == nullptr)) {
    return const_iterator(this, raw_key_complement);
  }
  return const_iterator(this, raw_key_complement, slow_set_->cbegin());
}

template <typename Key, typename Traits>
inline auto HybridDirectSet<Key, Traits>::end() ABSL_ATTRIBUTE_LIFETIME_BOUND
    -> iterator {
  if (ABSL_PREDICT_TRUE(slow_set_ == nullptr)) return iterator(this, 0);
  return iterator(this, 0, slow_set_->end());
}

template <typename Key, typename Traits>
inline auto HybridDirectSet<Key, Traits>::end() const
    ABSL_ATTRIBUTE_LIFETIME_BOUND -> const_iterator {
  if (ABSL_PREDICT_TRUE(slow_set_ == nullptr)) return const_iterator(this, 0);
  return const_iterator(this, 0, slow_set_->cend());
}

template <typename Key, typename Traits>
bool HybridDirectSet<Key, Traits>::Equal(const HybridDirectSet& a,
                                         const HybridDirectSet& b) {
  if (a.size() != b.size()) return false;
  const HybridDirectSet* outer;
  const HybridDirectSet* inner;
  if (a.capacity() <= b.capacity()) {
    outer = &a;
    inner = &b;
  } else {
    outer = &b;
    inner = &a;
  }
  for (Key key : *outer) {
    if (!inner->contains(key)) return false;
  }
  return true;
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_HYBRID_DIRECT_SET_H_
