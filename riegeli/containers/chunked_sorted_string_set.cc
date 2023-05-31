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

#include "riegeli/containers/chunked_sorted_string_set.h"

#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <initializer_list>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/compare.h"
#include "absl/types/optional.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/binary_search.h"
#include "riegeli/base/memory_estimator.h"
#include "riegeli/containers/linear_sorted_string_set.h"

namespace riegeli {

ChunkedSortedStringSet ChunkedSortedStringSet::FromSorted(
    std::initializer_list<absl::string_view> src, size_t chunk_size,
    size_t size_hint) {
  return FromSorted<>(src, chunk_size, size_hint);
}

ChunkedSortedStringSet ChunkedSortedStringSet::FromUnsorted(
    std::initializer_list<absl::string_view> src, size_t chunk_size) {
  return FromUnsorted<>(src, chunk_size);
}

inline ChunkedSortedStringSet::ChunkedSortedStringSet(
    LinearSortedStringSet&& first_chunk,
    std::vector<LinearSortedStringSet>&& chunks, size_t size)
    : first_chunk_(std::move(first_chunk)),
      repr_(chunks.empty()
                ? make_inline_repr(size)
                : make_allocated_repr(new Repr{std::move(chunks), size})) {}

void ChunkedSortedStringSet::DeleteAllocatedRepr(uintptr_t repr) {
  delete allocated_repr(repr);
}

uintptr_t ChunkedSortedStringSet::CopyAllocatedRepr() const {
  return make_allocated_repr(new Repr(*allocated_repr()));
}

bool ChunkedSortedStringSet::contains(absl::string_view element) const {
  const LinearSortedStringSet* found_linear_set = &first_chunk_;
  if (repr_is_allocated()) {
    // The target chunk is the last chunk whose first element is less than or
    // equal to the element being searched.
    //
    // `BinarySearch()` is biased so that it is easier to search for the chunk
    // after it, i.e. the first chunk whose first element is greater than the
    // element being searched (or possibly past the end iterator), and then go
    // back by one chunk (possibly to `first_chunk_`, even if its first element
    // is still too large, in which case its `contains()` will return `false`).
    //
    // Do not bother with returning `equal` to `BinarySearch()` if the first
    // element matches because this is rare, and that would require more
    // conditions.
    const SearchResult<ChunkIterator> chunk = BinarySearch(
        allocated_repr()->chunks.cbegin(), allocated_repr()->chunks.cend(),
        [&](ChunkIterator current) {
          if (current->first() <= element) return absl::strong_ordering::less;
          return absl::strong_ordering::greater;
        });
    if (chunk.found != allocated_repr()->chunks.cbegin()) {
      found_linear_set = &chunk.found[-1];
    }
  }
  return found_linear_set->contains(element);
}

bool ChunkedSortedStringSet::EqualImpl(const ChunkedSortedStringSet& a,
                                       const ChunkedSortedStringSet& b) {
  return a.size() == b.size() &&
         std::equal(a.cbegin(), a.cend(), b.cbegin(), b.cend());
}

bool ChunkedSortedStringSet::LessImpl(const ChunkedSortedStringSet& a,
                                      const ChunkedSortedStringSet& b) {
  return std::lexicographical_compare(a.cbegin(), a.cend(), b.cbegin(),
                                      b.cend());
}

size_t ChunkedSortedStringSet::EstimateMemory() const {
  MemoryEstimator memory_estimator;
  memory_estimator.RegisterMemory(sizeof(ChunkedSortedStringSet));
  memory_estimator.RegisterSubobjects(*this);
  return memory_estimator.TotalMemory();
}

size_t ChunkedSortedStringSet::Iterator::Next() {
  RIEGELI_ASSERT(current_iterator_ != LinearSortedStringSet::Iterator())
      << "Failed precondition of ChunkedSortedStringSet::Iterator::Next(): "
         "iterator is end()";
  const size_t shared_length = current_iterator_.Next();
  if (ABSL_PREDICT_TRUE(current_iterator_ !=
                        LinearSortedStringSet::Iterator())) {
    // Staying in the same chunk.
    return shared_length;
  }
  if (ABSL_PREDICT_FALSE(set_->repr_is_inline() ||
                         next_chunk_iterator_ ==
                             set_->allocated_repr()->chunks.cend())) {
    // Reached the end.
    return 0;
  }
  // Moving to the next chunk.
  current_iterator_ = next_chunk_iterator_->cbegin();
  ++next_chunk_iterator_;
  return 0;
}

ChunkedSortedStringSet::Builder::Builder(size_t chunk_size, size_t size_hint)
    : size_(0),
      chunk_size_(chunk_size),
      remaining_current_chunk_size_(chunk_size) {
  if (size_hint > 0) chunks_.reserve((size_hint - 1) / chunk_size);
}

ChunkedSortedStringSet::Builder::Builder(Builder&& that) noexcept = default;

ChunkedSortedStringSet::Builder& ChunkedSortedStringSet::Builder::operator=(
    Builder&& that) noexcept = default;

ChunkedSortedStringSet::Builder::~Builder() = default;

void ChunkedSortedStringSet::Builder::InsertNext(absl::string_view element) {
  const absl::Status status = TryInsertNext(element);
  RIEGELI_CHECK(status.ok())
      << "Failed precondition of "
         "ChunkedSortedStringSet::Builder::InsertNext(): "
      << status.message();
}

template <typename Element,
          std::enable_if_t<std::is_same<Element, std::string>::value, int>>
void ChunkedSortedStringSet::Builder::InsertNext(Element&& element) {
  // `std::move(element)` is correct and `std::forward<Element>(element)` is not
  // necessary: `Element` is always `std::string`, never an lvalue reference.
  const absl::Status status = TryInsertNext(std::move(element));
  RIEGELI_CHECK(status.ok())
      << "Failed precondition of "
         "ChunkedSortedStringSet::Builder::InsertNext(): "
      << status.message();
}

template void ChunkedSortedStringSet::Builder::InsertNext(
    std::string&& element);

absl::Status ChunkedSortedStringSet::Builder::TryInsertNext(
    absl::string_view element) {
  return InsertNextImpl(element);
}

template <typename Element,
          std::enable_if_t<std::is_same<Element, std::string>::value, int>>
absl::Status ChunkedSortedStringSet::Builder::TryInsertNext(Element&& element) {
  // `std::move(element)` is correct and `std::forward<Element>(element)` is not
  // necessary: `Element` is always `std::string`, never an lvalue reference.
  return InsertNextImpl(std::move(element));
}

template absl::Status ChunkedSortedStringSet::Builder::TryInsertNext(
    std::string&& element);

template <typename Element>
absl::Status ChunkedSortedStringSet::Builder::InsertNextImpl(
    Element&& element) {
  if (ABSL_PREDICT_FALSE(remaining_current_chunk_size_ == 0)) {
    if (ABSL_PREDICT_FALSE(element <= current_builder_.last())) {
      return OutOfOrder(element);
    }
    LinearSortedStringSet linear_set = std::move(current_builder_).Build();
    if (first_chunk_ == absl::nullopt) {
      first_chunk_ = std::move(linear_set);
    } else {
      chunks_.push_back(std::move(linear_set));
    }
    current_builder_.Reset();
    remaining_current_chunk_size_ = chunk_size_;
  }
  {
    const absl::Status status =
        current_builder_.TryInsertNext(std::forward<Element>(element));
    if (ABSL_PREDICT_FALSE(!status.ok())) {
      return status;
    }
  }
  ++size_;
  --remaining_current_chunk_size_;
  return absl::OkStatus();
}

absl::Status ChunkedSortedStringSet::Builder::OutOfOrder(
    absl::string_view element) const {
  return absl::FailedPreconditionError(
      element == last()
          ? absl::StrCat("Elements are not unique: new \"",
                         absl::CHexEscape(element), "\" == last")
          : absl::StrCat("Elements are not sorted: new \"",
                         absl::CHexEscape(element), "\" < last \"",
                         absl::CHexEscape(last()), "\""));
}

ChunkedSortedStringSet ChunkedSortedStringSet::Builder::Build() && {
  LinearSortedStringSet linear_set = std::move(current_builder_).Build();
  if (first_chunk_ == absl::nullopt) {
    first_chunk_ = std::move(linear_set);
  } else {
    chunks_.push_back(std::move(linear_set));
  }
  return ChunkedSortedStringSet(*std::move(first_chunk_), std::move(chunks_),
                                size_);
}

}  // namespace riegeli
