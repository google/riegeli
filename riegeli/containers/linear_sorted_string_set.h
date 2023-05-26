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

#ifndef RIEGELI_CONTAINERS_LINEAR_SORTED_STRING_SET_H_
#define RIEGELI_CONTAINERS_LINEAR_SORTED_STRING_SET_H_

#include <stddef.h>

#include <string>

#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/compact_string.h"
#include "riegeli/bytes/compact_string_writer.h"
#include "riegeli/bytes/string_reader.h"

namespace riegeli {

// A sorted set of strings, compressed by recognizing shared prefixes.
//
// `LinearSortedStringSet` is optimized for memory usage. It should be used
// only with very small sets (up to tens of elements), otherwise consider
// `ChunkedSortedStringSet`.
class LinearSortedStringSet {
 public:
  class Builder;
  class Iterator;

  // An empty set.
  LinearSortedStringSet() = default;

  LinearSortedStringSet(const LinearSortedStringSet& that) = default;
  LinearSortedStringSet& operator=(const LinearSortedStringSet& that) = default;

  LinearSortedStringSet(LinearSortedStringSet&& that) noexcept = default;
  LinearSortedStringSet& operator=(LinearSortedStringSet&& that) noexcept =
      default;

  // Returns `true` if the set is empty.
  bool empty() const { return encoded_.empty(); }

  // Returns the first element. The set must not be empty.
  absl::string_view first() const;

  // Returns `true` if `element` is present in the set.
  //
  // Time complexity: `O(size)`.
  bool contains(absl::string_view element) const;

  // Estimates the amount of memory used by this `LinearSortedStringSet`,
  // including `sizeof(LinearSortedStringSet)`.
  size_t EstimateMemory() const;

  // Support `EstimateMemory()`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const LinearSortedStringSet& self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(self.encoded_);
  }

 private:
  explicit LinearSortedStringSet(CompactString&& encoded);

  // Representation of each other element, which consists of the prefix of the
  // previous element with length shared_length, concatenated with unshared,
  // where tagged_length = (unshared_length << 1) | (shared_length > 0 ? 1 : 0):
  //
  //  * tagged_length : varint64
  //  * shared_length : varint64, if shared_length > 0
  //  * unshared      : char[unshared_length]
  CompactString encoded_;
};

// Builds a `LinearSortedStringSet` from a sorted sequence of unique strings.
class LinearSortedStringSet::Builder {
 public:
  // Begins with an empty set.
  Builder();

  Builder(Builder&& that) noexcept;
  Builder& operator=(Builder&& that) noexcept;

  ~Builder();

  // Makes `*this` equivalent to a newly constructed `Builder`.
  void Reset();

  // Inserts an element. It must be greater than all previously inserted
  // elements, otherwise it is not inserted and `false` is returned.
  bool InsertNext(absl::string_view element);

  // Returns `true` if the set is empty.
  bool empty() const { return writer_.pos() == 0; }

  // Returns the last element. The set must not be empty.
  absl::string_view last() const {
    RIEGELI_ASSERT(!empty())
        << "Failed precondition of LinearSortedStringSet::Builder::last(): "
           "empty set";
    return last_;
  }

  // Builds the `LinearSortedStringSet`. No more elements can be inserted.
  LinearSortedStringSet Build() &&;

 private:
  CompactStringWriter<CompactString> writer_;
  std::string last_;
};

// Iterates over a `LinearSortedStringSet` in the sorted order.
class LinearSortedStringSet::Iterator {
 public:
  // Iterates over `*set` which must not be changed until the `Iterator` is no
  // longer used.
  explicit Iterator(const LinearSortedStringSet* set);

  Iterator(Iterator&& that) noexcept = default;
  Iterator& operator=(Iterator&& that) noexcept = default;

  // Makes `*this` equivalent to a newly constructed `Iterator`.
  void Reset(const LinearSortedStringSet* set);

  // Returns the next element in the sorted order, or `absl::nullopt` if
  // iteration is exhausted.
  absl::optional<absl::string_view> Next();

 private:
  StringReader<> reader_;
  std::string current_;
};

}  // namespace riegeli

#endif  // RIEGELI_CONTAINERS_LINEAR_SORTED_STRING_SET_H_
