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

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/binary_search.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/debug.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/containers/linear_sorted_string_set.h"
#include "riegeli/varint/varint_reading.h"
#include "riegeli/varint/varint_writing.h"

namespace riegeli {

ChunkedSortedStringSet ChunkedSortedStringSet::FromSorted(
    std::initializer_list<absl::string_view> src, Options options) {
  return FromSorted<>(src, std::move(options));
}

ChunkedSortedStringSet ChunkedSortedStringSet::FromUnsorted(
    std::initializer_list<absl::string_view> src, Options options) {
  return FromUnsorted<>(src, std::move(options));
}

inline ChunkedSortedStringSet::ChunkedSortedStringSet(Chunks&& chunks)
    : chunks_(std::move(chunks)) {}

bool ChunkedSortedStringSet::ContainsImpl(absl::string_view element) const {
  if (chunks_.empty()) return false;
  SearchResult<Chunks::const_iterator> chunk =
      BinarySearch(chunks_.cbegin() + 1, chunks_.cend(), CompareFirst{element});
  --chunk.found;

  if (chunk.ordering == 0) return true;
  if (chunk.found == chunks_.cbegin()) {
    return chunk.found->set.contains(element);
  }
  return chunk.found->set.contains_skip_first(element);
}

bool ChunkedSortedStringSet::ContainsWithIndexImpl(absl::string_view element,
                                                   size_t& index) const {
  if (chunks_.empty()) {
    index = 0;
    return false;
  }
  SearchResult<Chunks::const_iterator> chunk =
      BinarySearch(chunks_.cbegin() + 1, chunks_.cend(), CompareFirst{element});
  --chunk.found;

  if (chunk.ordering == 0) {
    index = chunk.found->cumulative_end_index;
    return true;
  }
  if (chunk.found == chunks_.cbegin()) {
    return chunk.found->set.contains(element, &index);
  }
  const bool result = chunk.found->set.contains_skip_first(element, &index);
  index += chunk.found[-1].cumulative_end_index;
  return result;
}

bool ChunkedSortedStringSet::Equal(const ChunkedSortedStringSet& a,
                                   const ChunkedSortedStringSet& b) {
  return a.size() == b.size() &&
         std::equal(a.cbegin(), a.cend(), b.cbegin(), b.cend());
}

StrongOrdering ChunkedSortedStringSet::Compare(
    const ChunkedSortedStringSet& a, const ChunkedSortedStringSet& b) {
  Iterator a_iter = a.cbegin();
  Iterator b_iter = b.cbegin();
  while (a_iter != a.cend()) {
    if (b_iter == b.cend()) return StrongOrdering::greater;
    if (const StrongOrdering ordering = riegeli::Compare(*a_iter, *b_iter);
        ordering != 0) {
      return ordering;
    }
    ++a_iter;
    ++b_iter;
  }
  return b_iter == b.cend() ? StrongOrdering::equal : StrongOrdering::less;
}

size_t ChunkedSortedStringSet::EncodedSize() const {
  size_t encoded_size = LengthVarint64(chunks_.size());
  for (const Chunk& chunk : chunks_) encoded_size += chunk.set.EncodedSize();
  return encoded_size;
}

absl::Status ChunkedSortedStringSet::EncodeImpl(Writer& dest) const {
  if (ABSL_PREDICT_FALSE(!WriteVarint64(chunks_.size(), dest))) {
    return dest.status();
  }
  for (const Chunk& chunk : chunks_) {
    if (absl::Status status = chunk.set.Encode(dest);
        ABSL_PREDICT_FALSE(!status.ok())) {
      return status;
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
  Chunks chunks(IntCast<size_t>(num_chunks));
  for (Chunk& chunk : chunks) {
    if (absl::Status status = chunk.set.Decode(src, linear_options);
        ABSL_PREDICT_FALSE(!status.ok())) {
      return status;
    }
    if (ABSL_PREDICT_FALSE(chunk.set.empty())) {
      return src.AnnotateStatus(absl::InvalidArgumentError(
          "Malformed ChunkedSortedStringSet encoding "
          "(empty chunk)"));
    }
    chunk.cumulative_end_index = decode_state.cumulative_size;
  }
  chunks_ = std::move(chunks);
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
  RIEGELI_ASSERT(current_chunk_iterator_ != set_->chunks_.cend())
      << "Failed invariant of ChunkedSortedStringSet::Iterator: "
         "current_chunk_iterator_ is end() but current_iterator_ was not";
  ++current_chunk_iterator_;
  if (ABSL_PREDICT_FALSE(current_chunk_iterator_ == set_->chunks_.cend())) {
    // Reached the end.
    return *this;
  }
  // Moving to the next chunk.
  current_iterator_ = current_chunk_iterator_->set.cbegin();
  return *this;
}

ChunkedSortedStringSet::Builder::Builder(Options options)
    : chunk_size_(options.chunk_size()) {
  ApplySizeHint(options.size_hint());
}

ChunkedSortedStringSet::Builder::Builder(Builder&& that) noexcept
    : chunk_size_(that.chunk_size_),
      chunks_(std::exchange(that.chunks_, Chunks())),
      current_builder_(std::move(that.current_builder_)) {}

ChunkedSortedStringSet::Builder& ChunkedSortedStringSet::Builder::operator=(
    Builder&& that) noexcept {
  chunk_size_ = that.chunk_size_;
  chunks_ = std::exchange(that.chunks_, Chunks());
  current_builder_ = std::move(that.current_builder_);
  return *this;
}

ChunkedSortedStringSet::Builder::~Builder() = default;

void ChunkedSortedStringSet::Builder::Reset(Options options) {
  chunk_size_ = options.chunk_size();
  chunks_.clear();
  current_builder_.Reset();
  ApplySizeHint(options.size_hint());
}

inline void ChunkedSortedStringSet::Builder::ApplySizeHint(size_t size_hint) {
  if (size_hint > 0) chunks_.reserve((size_hint - 1) / chunk_size_ + 1);
}

bool ChunkedSortedStringSet::Builder::InsertNext(absl::string_view element) {
  const absl::StatusOr<bool> inserted = TryInsertNext(element);
  RIEGELI_CHECK_OK(inserted)
      << "Failed precondition of ChunkedSortedStringSet::Builder::InsertNext()";
  return *inserted;
}

template <typename Element,
          std::enable_if_t<std::is_same_v<Element, std::string>, int>>
bool ChunkedSortedStringSet::Builder::InsertNext(Element&& element) {
  // `std::move(element)` is correct and `std::forward<Element>(element)` is not
  // necessary: `Element` is always `std::string`, never an lvalue reference.
  const absl::StatusOr<bool> inserted = TryInsertNext(std::move(element));
  RIEGELI_CHECK_OK(inserted)
      << "Failed precondition of ChunkedSortedStringSet::Builder::InsertNext()";
  return *inserted;
}

template bool ChunkedSortedStringSet::Builder::InsertNext(
    std::string&& element);

absl::StatusOr<bool> ChunkedSortedStringSet::Builder::TryInsertNext(
    absl::string_view element) {
  return InsertNextImpl(element);
}

template <typename Element,
          std::enable_if_t<std::is_same_v<Element, std::string>, int>>
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
  if (ABSL_PREDICT_FALSE(current_builder_.size() == chunk_size_)) {
    if (ABSL_PREDICT_FALSE(element <= current_builder_.last())) {
      if (ABSL_PREDICT_TRUE(element == current_builder_.last())) return false;
      return absl::FailedPreconditionError(
          absl::StrCat("Elements are not sorted: new ", riegeli::Debug(element),
                       " < last ", riegeli::Debug(last())));
    }
    AddChunk();
  }
  if (absl::StatusOr<bool> inserted =
          current_builder_.TryInsertNext(std::forward<Element>(element));
      ABSL_PREDICT_FALSE(!inserted.ok() || !*inserted)) {
    return inserted;
  }
  return true;
}

ChunkedSortedStringSet ChunkedSortedStringSet::Builder::Build() {
  if (!current_builder_.empty()) AddChunk();
  ChunkedSortedStringSet set(std::exchange(chunks_, Chunks()));
  RIEGELI_ASSERT(empty())
      << "Failed postcondition of ChunkedSortedStringSet::Builder::Build(): "
         "builder should be empty";
  return set;
}

inline void ChunkedSortedStringSet::Builder::AddChunk() {
  const size_t cumulative_end_index =
      (chunks_.empty() ? 0 : chunks_.back().cumulative_end_index) +
      current_builder_.size();
  chunks_.push_back(Chunk{current_builder_.Build(), cumulative_end_index});
}

}  // namespace riegeli
