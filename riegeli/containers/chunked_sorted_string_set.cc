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
#include "absl/status/statusor.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/compare.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/binary_search.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/memory_estimator.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/containers/linear_sorted_string_set.h"
#include "riegeli/varint/varint_reading.h"
#include "riegeli/varint/varint_writing.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if __cplusplus < 201703
constexpr size_t ChunkedSortedStringSet::Options::kDefaultChunkSize;
#endif

ChunkedSortedStringSet ChunkedSortedStringSet::FromSorted(
    std::initializer_list<absl::string_view> src, Options options) {
  return FromSorted<>(src, std::move(options));
}

ChunkedSortedStringSet ChunkedSortedStringSet::FromUnsorted(
    std::initializer_list<absl::string_view> src, Options options) {
  return FromUnsorted<>(src, std::move(options));
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

StrongOrdering ChunkedSortedStringSet::CompareImpl(
    const ChunkedSortedStringSet& a, const ChunkedSortedStringSet& b) {
  Iterator a_iter = a.cbegin();
  Iterator b_iter = b.cbegin();
  while (a_iter != a.cend()) {
    if (b_iter == b.cend()) return StrongOrdering::greater;
    {
      const int ordering = a_iter->compare(*b_iter);
      if (ordering != 0) {
        return AsStrongOrdering(ordering);
      }
    }
    ++a_iter;
    ++b_iter;
  }
  return b_iter == b.cend() ? StrongOrdering::equal : StrongOrdering::less;
}

size_t ChunkedSortedStringSet::EstimateMemory() const {
  MemoryEstimator memory_estimator;
  memory_estimator.RegisterMemory(sizeof(ChunkedSortedStringSet));
  memory_estimator.RegisterSubobjects(*this);
  return memory_estimator.TotalMemory();
}

size_t ChunkedSortedStringSet::EncodedSize() const {
  if (repr_is_inline()) {
    if (first_chunk_.empty()) return 1;
    return 1 + first_chunk_.EncodedSize();
  }

  size_t encoded_size = LengthVarint64(allocated_repr()->chunks.size() + 1) +
                        first_chunk_.EncodedSize();
  for (const LinearSortedStringSet& chunk : allocated_repr()->chunks) {
    encoded_size += chunk.EncodedSize();
  }
  return encoded_size;
}

absl::Status ChunkedSortedStringSet::EncodeImpl(Writer& dest) const {
  if (repr_is_inline()) {
    if (first_chunk_.empty()) {
      if (ABSL_PREDICT_FALSE(!WriteVarint64(0, dest))) return dest.status();
      return absl::OkStatus();
    }
    if (ABSL_PREDICT_FALSE(!WriteVarint64(1, dest))) return dest.status();
    return first_chunk_.Encode(dest);
  }

  if (ABSL_PREDICT_FALSE(
          !WriteVarint64(allocated_repr()->chunks.size() + 1, dest))) {
    return dest.status();
  }
  {
    const absl::Status status = first_chunk_.Encode(dest);
    if (ABSL_PREDICT_FALSE(!status.ok())) {
      return status;
    }
  }
  for (const LinearSortedStringSet& chunk : allocated_repr()->chunks) {
    {
      const absl::Status status = chunk.Encode(dest);
      if (ABSL_PREDICT_FALSE(!status.ok())) {
        return status;
      }
    }
  }
  return absl::OkStatus();
}

absl::Status ChunkedSortedStringSet::DecodeImpl(Reader& src,
                                                DecodeOptions options) {
  uint64_t num_chunks;
  if (ABSL_PREDICT_FALSE(!ReadVarint64(src, num_chunks))) {
    return src.StatusOrAnnotate(
        absl::InvalidArgumentError("Malformed ChunkedSortedStringSet encoding "
                                   "(num_chunks)"));
  }
  if (num_chunks == 0) {
    first_chunk_ = LinearSortedStringSet();
    DeleteRepr(std::exchange(repr_, kEmptyRepr));
    return absl::OkStatus();
  }
  if (ABSL_PREDICT_FALSE(num_chunks > options.max_num_chunks())) {
    return src.AnnotateStatus(absl::ResourceExhaustedError(absl::StrCat(
        "Maximum ChunkedSortedStringSet number of chunks exceeded: ",
        num_chunks, " > ", options.max_num_chunks())));
  }

  LinearSortedStringSet::DecodeState decode_state;
  const LinearSortedStringSet::DecodeOptions linear_options =
      LinearSortedStringSet::DecodeOptions()
          .set_validate(options.validate())
          .set_max_encoded_size(options.max_encoded_chunk_size())
          .set_decode_state(&decode_state);
  LinearSortedStringSet first_chunk;
  {
    const absl::Status status = first_chunk.Decode(src, linear_options);
    if (ABSL_PREDICT_FALSE(!status.ok())) {
      return status;
    }
  }
  if (ABSL_PREDICT_FALSE(first_chunk.empty())) {
    return src.AnnotateStatus(
        absl::InvalidArgumentError("Malformed ChunkedSortedStringSet encoding "
                                   "(empty first chunk)"));
  }
  if (num_chunks == 1) {
    first_chunk_ = std::move(first_chunk);
    DeleteRepr(std::exchange(repr_, make_inline_repr(decode_state.size)));
    return absl::OkStatus();
  }

  std::vector<LinearSortedStringSet> chunks(IntCast<size_t>(num_chunks - 1));
  for (LinearSortedStringSet& chunk : chunks) {
    {
      const absl::Status status = chunk.Decode(src, linear_options);
      if (ABSL_PREDICT_FALSE(!status.ok())) {
        return status;
      }
    }
    if (ABSL_PREDICT_FALSE(chunk.empty())) {
      return src.AnnotateStatus(absl::InvalidArgumentError(
          "Malformed ChunkedSortedStringSet encoding "
          "(empty chunk)"));
    }
  }
  first_chunk_ = std::move(first_chunk);
  DeleteRepr(std::exchange(repr_, make_allocated_repr(new Repr{
                                      std::move(chunks), decode_state.size})));
  return absl::OkStatus();
}

ChunkedSortedStringSet::Iterator&
ChunkedSortedStringSet::Iterator::operator++() {
  RIEGELI_ASSERT(current_iterator_ != LinearSortedStringSet::Iterator())
      << "Failed precondition of ChunkedSortedStringSet::Iterator::operator++: "
         "iterator is end()";
  ++current_iterator_;
  if (ABSL_PREDICT_TRUE(current_iterator_ !=
                        LinearSortedStringSet::Iterator())) {
    // Staying in the same chunk.
    return *this;
  }
  if (ABSL_PREDICT_FALSE(set_->repr_is_inline() ||
                         next_chunk_iterator_ ==
                             set_->allocated_repr()->chunks.cend())) {
    // Reached the end.
    return *this;
  }
  // Moving to the next chunk.
  current_iterator_ = next_chunk_iterator_->cbegin();
  ++next_chunk_iterator_;
  return *this;
}

ChunkedSortedStringSet::Builder::Builder(Options options)
    : size_(0),
      chunk_size_(options.chunk_size()),
      remaining_current_chunk_size_(chunk_size_) {
  if (options.size_hint() > 0) {
    chunks_.reserve((options.size_hint() - 1) / chunk_size_);
  }
}

ChunkedSortedStringSet::Builder::Builder(Builder&& that) noexcept = default;

ChunkedSortedStringSet::Builder& ChunkedSortedStringSet::Builder::operator=(
    Builder&& that) noexcept = default;

ChunkedSortedStringSet::Builder::~Builder() = default;

bool ChunkedSortedStringSet::Builder::InsertNext(absl::string_view element) {
  const absl::StatusOr<bool> inserted = TryInsertNext(element);
  RIEGELI_CHECK(inserted.ok())
      << "Failed precondition of "
         "ChunkedSortedStringSet::Builder::InsertNext(): "
      << inserted.status().message();
  return *inserted;
}

template <typename Element,
          std::enable_if_t<std::is_same<Element, std::string>::value, int>>
bool ChunkedSortedStringSet::Builder::InsertNext(Element&& element) {
  // `std::move(element)` is correct and `std::forward<Element>(element)` is not
  // necessary: `Element` is always `std::string`, never an lvalue reference.
  const absl::StatusOr<bool> inserted = TryInsertNext(std::move(element));
  RIEGELI_CHECK(inserted.ok())
      << "Failed precondition of "
         "ChunkedSortedStringSet::Builder::InsertNext(): "
      << inserted.status().message();
  return *inserted;
}

template bool ChunkedSortedStringSet::Builder::InsertNext(
    std::string&& element);

absl::StatusOr<bool> ChunkedSortedStringSet::Builder::TryInsertNext(
    absl::string_view element) {
  return InsertNextImpl(element);
}

template <typename Element,
          std::enable_if_t<std::is_same<Element, std::string>::value, int>>
absl::StatusOr<bool> ChunkedSortedStringSet::Builder::TryInsertNext(
    Element&& element) {
  // `std::move(element)` is correct and `std::forward<Element>(element)` is not
  // necessary: `Element` is always `std::string`, never an lvalue reference.
  return InsertNextImpl(std::move(element));
}

template absl::StatusOr<bool> ChunkedSortedStringSet::Builder::TryInsertNext(
    std::string&& element);

template <typename Element>
absl::StatusOr<bool> ChunkedSortedStringSet::Builder::InsertNextImpl(
    Element&& element) {
  if (ABSL_PREDICT_FALSE(remaining_current_chunk_size_ == 0)) {
    if (ABSL_PREDICT_FALSE(element <= current_builder_.last())) {
      if (ABSL_PREDICT_TRUE(element == current_builder_.last())) return false;
      return absl::FailedPreconditionError(absl::StrCat(
          "Elements are not sorted: new \"", absl::CHexEscape(element),
          "\" < last \"", absl::CHexEscape(last()), "\""));
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
    const absl::StatusOr<bool> inserted =
        current_builder_.TryInsertNext(std::forward<Element>(element));
    if (ABSL_PREDICT_FALSE(!inserted.ok() || !*inserted)) {
      return inserted;
    }
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

}  // namespace riegeli
