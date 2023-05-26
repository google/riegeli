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

#include <limits>
#include <utility>
#include <vector>

#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/assert.h"
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

  // An empty set.
  ChunkedSortedStringSet() = default;

  ChunkedSortedStringSet(const ChunkedSortedStringSet& that);
  ChunkedSortedStringSet& operator=(const ChunkedSortedStringSet& that);

  ChunkedSortedStringSet(ChunkedSortedStringSet&& that) noexcept;
  ChunkedSortedStringSet& operator=(ChunkedSortedStringSet&& that) noexcept;

  ~ChunkedSortedStringSet();

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
  bool repr_is_inline() const { return (repr_ & 1) == 1; }
  bool repr_is_allocated() const { return (repr_ & 1) == 0; }
  size_t inline_repr() const {
    RIEGELI_ASSERT(repr_is_inline())
        << "Failed precondition of ChunkedSortedStringSet::inline_repr(): "
           "representation is not inline";
    return static_cast<size_t>(repr_ >> 1);
  }
  const Repr* allocated_repr() const {
    RIEGELI_ASSERT(repr_is_allocated())
        << "Failed precondition of ChunkedSortedStringSet::allocated_repr(): "
           "representation is not allocated";
    return reinterpret_cast<const Repr*>(repr_);
  }
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

  void DeleteAllocatedRepr();
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

  Builder(Builder&& that) noexcept = default;
  Builder& operator=(Builder&& that) noexcept = default;

  ~Builder();

  // Inserts an element. It must be greater than all previously inserted
  // elements, otherwise it is not inserted and `false` is returned.
  bool InsertNext(absl::string_view element);

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
  // Iterates over `*set` which must not be changed until the
  // `ChunkedSortedStringSetIterator` is no longer used.
  explicit Iterator(const ChunkedSortedStringSet* set);

  Iterator(Iterator&& that) noexcept = default;
  Iterator& operator=(Iterator&& that) noexcept = default;

  // Returns the next element in the sorted order, or `absl::nullopt` if
  // iteration is exhausted.
  absl::optional<absl::string_view> Next();

 private:
  using ChunkIterator = ChunkedSortedStringSet::ChunkIterator;
  using Repr = ChunkedSortedStringSet::Repr;

  const ChunkedSortedStringSet* set_;
  LinearSortedStringSet::Iterator current_iterator_;
  ChunkIterator next_chunk_iterator_;
};

// Implementation details follow.

inline ChunkedSortedStringSet::ChunkedSortedStringSet(
    const ChunkedSortedStringSet& that)
    : first_chunk_(that.first_chunk_),
      repr_(that.repr_is_inline() ? that.repr_ : that.CopyAllocatedRepr()) {}

inline ChunkedSortedStringSet& ChunkedSortedStringSet::operator=(
    const ChunkedSortedStringSet& that) {
  first_chunk_ = that.first_chunk_;
  const uintptr_t repr =
      that.repr_is_inline() ? that.repr_ : that.CopyAllocatedRepr();
  if (repr_is_allocated()) DeleteAllocatedRepr();
  repr_ = repr;
  return *this;
}

inline ChunkedSortedStringSet::ChunkedSortedStringSet(
    ChunkedSortedStringSet&& that) noexcept
    : first_chunk_(std::move(that.first_chunk_)),
      repr_(std::exchange(that.repr_, kEmptyRepr)) {}

inline ChunkedSortedStringSet& ChunkedSortedStringSet::operator=(
    ChunkedSortedStringSet&& that) noexcept {
  first_chunk_ = std::move(that.first_chunk_);
  // Exchange `that.repr_` early to support self-assignment.
  const uintptr_t repr = std::exchange(that.repr_, kEmptyRepr);
  if (repr_is_allocated()) DeleteAllocatedRepr();
  repr_ = repr;
  return *this;
}

inline ChunkedSortedStringSet::~ChunkedSortedStringSet() {
  if (repr_is_allocated()) DeleteAllocatedRepr();
}

}  // namespace riegeli

#endif  // RIEGELI_CONTAINERS_CHUNKED_SORTED_STRING_SET_H_
