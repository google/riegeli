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

#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "absl/types/compare.h"
#include "absl/types/optional.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/binary_search.h"
#include "riegeli/base/memory_estimator.h"
#include "riegeli/containers/linear_sorted_string_set.h"

namespace riegeli {

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

size_t ChunkedSortedStringSet::EstimateMemory() const {
  MemoryEstimator memory_estimator;
  memory_estimator.RegisterMemory(sizeof(ChunkedSortedStringSet));
  memory_estimator.RegisterSubobjects(*this);
  return memory_estimator.TotalMemory();
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

bool ChunkedSortedStringSet::Builder::InsertNext(absl::string_view element) {
  return InsertNextImpl(element);
}

template <typename Element,
          std::enable_if_t<std::is_same<Element, std::string>::value, int>>
bool ChunkedSortedStringSet::Builder::InsertNext(Element&& element) {
  // `std::move(element)` is correct and `std::forward<Element>(element)` is not
  // necessary: `Element` is always `std::string`, never an lvalue reference.
  return InsertNextImpl(std::move(element));
}

template bool ChunkedSortedStringSet::Builder::InsertNext(
    std::string&& element);

template <typename Element>
bool ChunkedSortedStringSet::Builder::InsertNextImpl(Element&& element) {
  if (ABSL_PREDICT_FALSE(remaining_current_chunk_size_ == 0)) {
    if (ABSL_PREDICT_FALSE(element <= current_builder_.last())) {
      return false;  // Out of order (across chunks).
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
  if (ABSL_PREDICT_FALSE(
          !current_builder_.InsertNext(std::forward<Element>(element)))) {
    return false;  // Out of order (within a chunk).
  }
  ++size_;
  --remaining_current_chunk_size_;
  return true;
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

void ChunkedSortedStringSet::Iterator::Next() {
  RIEGELI_ASSERT(current_iterator_ != LinearSortedStringSet::Iterator())
      << "Failed precondition of "
         "ChunkedSortedStringSet::Iterator::operator->(): "
         "iterator is end()";
  ++current_iterator_;
  if (ABSL_PREDICT_TRUE(current_iterator_ !=
                        LinearSortedStringSet::Iterator())) {
    // Staying in the same chunk.
    return;
  }
  if (ABSL_PREDICT_FALSE(set_->repr_is_inline() ||
                         next_chunk_iterator_ ==
                             set_->allocated_repr()->chunks.cend())) {
    // Reached the end.
    return;
  }
  // Moving to the next chunk.
  current_iterator_ = next_chunk_iterator_->cbegin();
  ++next_chunk_iterator_;
}

}  // namespace riegeli
