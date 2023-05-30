// Copyright 2021 Google LLC
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

#ifndef RIEGELI_CONTAINERS_CHUNKED_SORTED_STRING_SET_H_
#define RIEGELI_CONTAINERS_CHUNKED_SORTED_STRING_SET_H_

#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <initializer_list>
#include <iterator>
#include <limits>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/type_traits.h"
#include "riegeli/containers/linear_sorted_string_set.h"

namespace riegeli {

// A sorted set of strings, split into chunks, compressed by recognizing shared
// prefixes within each chunk.
//
// `ChunkedSortedStringSet` is optimized for memory usage.
class ChunkedSortedStringSet {
 public:
  class Builder;
  class Iterator;

  using value_type = absl::string_view;
  using reference = value_type;
  using const_reference = reference;
  using iterator = Iterator;
  using const_iterator = iterator;
  using size_type = size_t;
  using difference_type = ptrdiff_t;

  // Creates a set consisting of the given elements. They must be sorted and
  // unique.
  //
  // The type of `src` must support iteration yielding `absl::string_view`:
  // `for (const absl::string_view element : src)`,
  // e.g. `std::vector<std::string>`.
  //
  // `chunk_size` tunes the number of elements encoded together. A larger
  // `chunk_size` reduces memory usage, but the time complexity of lookups is
  // roughly proportional to `chunk_size`.
  //
  // `size_hint` is the expected number of elements. If it turns out to not
  // match reality, nothing breaks. If `Src` supports random access iteration,
  // `std::distance(begin(src), end(src))` is automatically used as `size_hint`.
  template <
      typename Src,
      std::enable_if_t<IsIterableOf<Src, absl::string_view>::value, int> = 0>
  static ChunkedSortedStringSet FromSorted(Src&& src, size_t chunk_size,
                                           size_t size_hint = 0);
  static ChunkedSortedStringSet FromSorted(
      std::initializer_list<absl::string_view> src, size_t chunk_size,
      size_t size_hint = 0);

  // Creates a set consisting of the given elements. They do not need to be
  // sorted or unique.
  //
  // The type of `src` must support iteration yielding `absl::string_view`:
  // `for (const absl::string_view element : src)`,
  // e.g. `std::vector<std::string>`.
  //
  // `chunk_size` tunes the number of elements encoded together. A larger
  // `chunk_size` reduces memory usage, but the time complexity of lookups is
  // roughly proportional to `chunk_size`.
  template <
      typename Src,
      std::enable_if_t<IsIterableOf<Src, absl::string_view>::value, int> = 0>
  static ChunkedSortedStringSet FromUnsorted(Src&& src, size_t chunk_size);
  static ChunkedSortedStringSet FromUnsorted(
      std::initializer_list<absl::string_view> src, size_t chunk_size);

  // An empty set.
  ChunkedSortedStringSet() = default;

  ChunkedSortedStringSet(const ChunkedSortedStringSet& that);
  ChunkedSortedStringSet& operator=(const ChunkedSortedStringSet& that);

  ChunkedSortedStringSet(ChunkedSortedStringSet&& that) noexcept;
  ChunkedSortedStringSet& operator=(ChunkedSortedStringSet&& that) noexcept;

  ~ChunkedSortedStringSet();

  // Iteration over the set.
  Iterator begin() const;
  Iterator cbegin() const;
  Iterator end() const;
  Iterator cend() const;

  // Returns `true` if the set is empty.
  bool empty() const { return repr_ == kEmptyRepr; }

  // Returns the number of elements.
  size_t size() const {
    return repr_is_inline() ? inline_repr() : allocated_repr()->size;
  }

  // Returns `true` if `element` is present in the set.
  //
  // Time complexity: `O(log(size / chunk_size) + chunk_size)`.
  bool contains(absl::string_view element) const;

  // Estimates the amount of memory used by this `ChunkedSortedStringSet`,
  // including `sizeof(ChunkedSortedStringSet)`.
  size_t EstimateMemory() const;

  // Support `EstimateMemory()`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const ChunkedSortedStringSet& self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(self.first_chunk_);
    if (self.repr_is_allocated()) {
      memory_estimator.RegisterDynamicObject(*self.allocated_repr());
    }
  }

 private:
  using ChunkIterator = std::vector<LinearSortedStringSet>::const_iterator;

  struct Repr {
    // Support `EstimateMemory()`.
    template <typename MemoryEstimator>
    friend void RiegeliRegisterSubobjects(const Repr& self,
                                          MemoryEstimator& memory_estimator) {
      memory_estimator.RegisterSubobjects(self.chunks);
    }

    // Invariants:
    //   `!chunks.empty()`
    //   none of `chunks` is `empty()`
    std::vector<LinearSortedStringSet> chunks;
    size_t size = 0;
  };

  explicit ChunkedSortedStringSet(LinearSortedStringSet&& first_chunk,
                                  std::vector<LinearSortedStringSet>&& chunks,
                                  size_t size);

  static constexpr uintptr_t kEmptyRepr = 1;
  static bool repr_is_inline(uintptr_t repr) { return (repr & 1) == 1; }
  bool repr_is_inline() const { return repr_is_inline(repr_); }
  static bool repr_is_allocated(uintptr_t repr) { return (repr & 1) == 0; }
  bool repr_is_allocated() const { return repr_is_allocated(repr_); }
  size_t inline_repr() const {
    RIEGELI_ASSERT(repr_is_inline())
        << "Failed precondition of ChunkedSortedStringSet::inline_repr(): "
           "representation is not inline";
    return static_cast<size_t>(repr_ >> 1);
  }
  static const Repr* allocated_repr(uintptr_t repr) {
    RIEGELI_ASSERT(repr_is_allocated(repr))
        << "Failed precondition of ChunkedSortedStringSet::allocated_repr(): "
           "representation is not allocated";
    return reinterpret_cast<const Repr*>(repr);
  }
  const Repr* allocated_repr() const { return allocated_repr(repr_); }
  static uintptr_t make_inline_repr(size_t size) {
    RIEGELI_ASSERT_LE(size, std::numeric_limits<uintptr_t>::max() / 1)
        << "Failed precondition of ChunkedSortedStringSet::make_inline_repr(): "
           "size overflow";
    return (static_cast<uintptr_t>(size) << 1) + 1;
  }
  static uintptr_t make_allocated_repr(const Repr* repr) {
    RIEGELI_ASSERT_EQ(reinterpret_cast<uintptr_t>(repr) & 1, 0u)
        << "Failed precondition of "
           "ChunkedSortedStringSet::make_allocated_repr(): "
           "pointer not aligned";
    return reinterpret_cast<uintptr_t>(repr);
  }

  static void DeleteRepr(uintptr_t repr) {
    if (repr_is_allocated(repr)) DeleteAllocatedRepr(repr);
  }

  static void DeleteAllocatedRepr(uintptr_t repr);

  uintptr_t CopyRepr() const {
    return repr_is_inline() ? repr_ : CopyAllocatedRepr();
  }

  uintptr_t CopyAllocatedRepr() const;

  // The first `LinearSortedStringSet` is stored inline to reduce object size
  // when the set is small.
  LinearSortedStringSet first_chunk_;
  // If `repr_is_inline()`: `chunks` are empty, `size` is `inline_repr()`.
  // If `repr_is_allocated()`: `*allocated_repr()` stores `chunks` and `size`.
  //
  // Invariant: if `first_chunk_.empty()` then `repr_is_inline()`.
  uintptr_t repr_ = kEmptyRepr;
};

// Builds a `ChunkedSortedStringSet` from a sorted sequence of unique strings.
class ChunkedSortedStringSet::Builder {
 public:
  // Begins with an empty set.
  //
  // `chunk_size` tunes the number of elements encoded together. A larger
  // `chunk_size` reduces memory usage, but the time complexity of lookups is
  // roughly proportional to `chunk_size`.
  //
  // `size_hint` is the expected number of elements. If it turns out to not
  // match reality, nothing breaks.
  explicit Builder(size_t chunk_size, size_t size_hint = 0);

  Builder(Builder&& that) noexcept;
  Builder& operator=(Builder&& that) noexcept;

  ~Builder();

  // Inserts an element.
  //
  // Precondition: `element` is greater than all previously inserted elements.
  //
  // `std::string&&` is accepted with a template to avoid implicit conversions
  // to `std::string` which can be ambiguous against `absl::string_view`
  // (e.g. `const char*`).
  void InsertNext(absl::string_view element);
  template <
      typename Element,
      std::enable_if_t<std::is_same<Element, std::string>::value, int> = 0>
  void InsertNext(Element&& element);

  // Inserts an element.
  //
  // If it is not greater than all previously inserted element, then nothing
  // is inserted and an `absl::FailedPreconditionError()` is returned.
  //
  // If `std::string&&` is passed, it is moved only if the result is `true`.
  absl::Status TryInsertNext(absl::string_view element);
  template <
      typename Element,
      std::enable_if_t<std::is_same<Element, std::string>::value, int> = 0>
  absl::Status TryInsertNext(Element&& element);

  // Returns `true` if the set is empty.
  bool empty() const {
    return first_chunk_ == absl::nullopt && current_builder_.empty();
  }

  // Returns the last element. The set must not be empty.
  absl::string_view last() const {
    RIEGELI_ASSERT(!empty())
        << "Failed precondition of ChunkedSortedStringSet::Builder::last(): "
           "empty set";
    return current_builder_.last();
  }

  // Builds the `ChunkedSortedStringSet`. No more elements can be inserted.
  ChunkedSortedStringSet Build() &&;

 private:
  // This template is defined and used only in chunked_sorted_string_set.cc.
  template <typename Element>
  absl::Status InsertNextImpl(Element&& element);

  absl::Status OutOfOrder(absl::string_view element) const;

  size_t size_;
  size_t chunk_size_;
  size_t remaining_current_chunk_size_;

  // Invariant: if `first_chunk_ == absl::nullopt` then `chunks_.empty()`
  absl::optional<LinearSortedStringSet> first_chunk_;
  std::vector<LinearSortedStringSet> chunks_;
  LinearSortedStringSet::Builder current_builder_;
};

// Iterates over a `LinearSortedStringSet` in the sorted order.
class ChunkedSortedStringSet::Iterator {
 public:
  // `iterator_concept` is only `std::input_iterator_tag` because the
  // `std::forward_iterator` requirement and above require references to remain
  // valid while the range exists.
  using iterator_concept = std::input_iterator_tag;
  // `iterator_category` is only `std::input_iterator_tag` also because the
  // `LegacyForwardIterator` requirement and above require `reference` to be
  // a true reference type.
  using iterator_category = std::input_iterator_tag;
  using value_type = absl::string_view;
  using reference = value_type;
  using difference_type = ptrdiff_t;

  class pointer {
   public:
    reference* operator->() { return &ref_; }
    const reference* operator->() const { return &ref_; }

   private:
    friend class Iterator;
    explicit pointer(reference ref) : ref_(ref) {}
    reference ref_;
  };

  // A sentinel value, equal to `end()`.
  Iterator() = default;

  Iterator(const Iterator& that) = default;
  Iterator& operator=(const Iterator& that) = default;

  Iterator(Iterator&& that) noexcept = default;
  Iterator& operator=(Iterator&& that) noexcept = default;

  // Returns the current element.
  //
  // The `absl::string_view` is valid until the next non-const operation on this
  // `Iterator` (the string it points to is owned by `Iterator`).
  reference operator*() const {
    RIEGELI_ASSERT(current_iterator_ != LinearSortedStringSet::Iterator())
        << "Failed precondition of "
           "LinearSortedStringSet::Iterator::operator*(): "
           "iterator is end()";
    return *current_iterator_;
  }

  pointer operator->() const {
    RIEGELI_ASSERT(current_iterator_ != LinearSortedStringSet::Iterator())
        << "Failed precondition of "
           "LinearSortedStringSet::Iterator::operator->(): "
           "iterator is end()";
    return pointer(**this);
  }

  Iterator& operator++() {
    RIEGELI_ASSERT(current_iterator_ != LinearSortedStringSet::Iterator())
        << "Failed precondition of "
           "LinearSortedStringSet::Iterator::operator++(): "
           "iterator is end()";
    Next();
    return *this;
  }
  Iterator operator++(int) {
    const Iterator tmp = *this;
    ++*this;
    return tmp;
  }

  // Iterators can be compared even if they are associated with different
  // `ChunkedSortedStringSet` objects. All `end()` values are equal, while all
  // other values are not equal.
  friend bool operator==(const Iterator& a, const Iterator& b) {
    return a.current_iterator_ == b.current_iterator_;
  }
  friend bool operator!=(const Iterator& a, const Iterator& b) {
    return a.current_iterator_ != b.current_iterator_;
  }

 private:
  friend class ChunkedSortedStringSet;  // For `Iterator::Iterator`.

  using ChunkIterator = ChunkedSortedStringSet::ChunkIterator;
  using Repr = ChunkedSortedStringSet::Repr;

  explicit Iterator(const ChunkedSortedStringSet* set)
      : current_iterator_(set->first_chunk_.cbegin()),
        next_chunk_iterator_(set->repr_is_inline()
                                 ? ChunkIterator()
                                 : set->allocated_repr()->chunks.cbegin()),
        set_(set) {}

  void Next();

  LinearSortedStringSet::Iterator current_iterator_;
  ChunkIterator next_chunk_iterator_ = ChunkIterator();
  const ChunkedSortedStringSet* set_ = nullptr;
};

// Implementation details follow.

template <typename Src,
          std::enable_if_t<IsIterableOf<Src, absl::string_view>::value, int>>
ChunkedSortedStringSet ChunkedSortedStringSet::FromSorted(Src&& src,
                                                          size_t chunk_size,
                                                          size_t size_hint) {
  using std::begin;
  auto iter = begin(src);
  using std::end;
  auto end_iter = end(src);
  using SrcIterator = decltype(iter);
  if (std::is_convertible<
          typename std::iterator_traits<SrcIterator>::iterator_category,
          std::random_access_iterator_tag>::value) {
    size_hint = std::distance(iter, end_iter);
  }
  ChunkedSortedStringSet::Builder builder(chunk_size, size_hint);
  for (; iter != end_iter; ++iter) {
    builder.InsertNext(MaybeMoveElement<Src>(*iter));
  }
  return std::move(builder).Build();
}

template <typename Src,
          std::enable_if_t<IsIterableOf<Src, absl::string_view>::value, int>>
inline ChunkedSortedStringSet ChunkedSortedStringSet::FromUnsorted(
    Src&& src, size_t chunk_size) {
  using std::begin;
  auto iter = begin(src);
  using std::end;
  auto end_iter = end(src);
  using SrcIterator = decltype(iter);
  std::vector<SrcIterator> iterators;
  if (std::is_convertible<
          typename std::iterator_traits<SrcIterator>::iterator_category,
          std::random_access_iterator_tag>::value) {
    iterators.reserve(std::distance(iter, end_iter));
  }
  for (; iter != end_iter; ++iter) {
    iterators.push_back(iter);
  }
  std::sort(iterators.begin(), iterators.end(),
            [](const SrcIterator& a, const SrcIterator& b) {
              return absl::string_view(*a) < absl::string_view(*b);
            });
  iterators.erase(std::unique(iterators.begin(), iterators.end(),
                              [](const SrcIterator& a, const SrcIterator& b) {
                                return absl::string_view(*a) ==
                                       absl::string_view(*b);
                              }),
                  iterators.end());

  ChunkedSortedStringSet::Builder builder(chunk_size, iterators.size());
  for (const SrcIterator& iter : iterators) {
    builder.InsertNext(MaybeMoveElement<Src>(*iter));
  }
  return std::move(builder).Build();
}

inline ChunkedSortedStringSet::ChunkedSortedStringSet(
    const ChunkedSortedStringSet& that)
    : first_chunk_(that.first_chunk_), repr_(that.CopyRepr()) {}

inline ChunkedSortedStringSet& ChunkedSortedStringSet::operator=(
    const ChunkedSortedStringSet& that) {
  first_chunk_ = that.first_chunk_;
  DeleteRepr(std::exchange(repr_, that.CopyRepr()));
  return *this;
}

inline ChunkedSortedStringSet::ChunkedSortedStringSet(
    ChunkedSortedStringSet&& that) noexcept
    : first_chunk_(std::move(that.first_chunk_)),
      repr_(std::exchange(that.repr_, kEmptyRepr)) {}

inline ChunkedSortedStringSet& ChunkedSortedStringSet::operator=(
    ChunkedSortedStringSet&& that) noexcept {
  first_chunk_ = std::move(that.first_chunk_);
  DeleteRepr(std::exchange(repr_, std::exchange(that.repr_, kEmptyRepr)));
  return *this;
}

inline ChunkedSortedStringSet::~ChunkedSortedStringSet() { DeleteRepr(repr_); }

inline ChunkedSortedStringSet::Iterator ChunkedSortedStringSet::begin() const {
  return Iterator(this);
}

inline ChunkedSortedStringSet::Iterator ChunkedSortedStringSet::cbegin() const {
  return begin();
}

inline ChunkedSortedStringSet::Iterator ChunkedSortedStringSet::end() const {
  return Iterator();
}

inline ChunkedSortedStringSet::Iterator ChunkedSortedStringSet::cend() const {
  return end();
}

extern template void ChunkedSortedStringSet::Builder::InsertNext(
    std::string&& element);

extern template absl::Status ChunkedSortedStringSet::Builder::TryInsertNext(
    std::string&& element);

}  // namespace riegeli

#endif  // RIEGELI_CONTAINERS_CHUNKED_SORTED_STRING_SET_H_
