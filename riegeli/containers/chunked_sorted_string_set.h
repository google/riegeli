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

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/compact_string.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/type_traits.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/containers/linear_sorted_string_set.h"

namespace riegeli {

// A sorted set of strings, split into chunks, compressed by recognizing shared
// prefixes within each chunk.
//
// `ChunkedSortedStringSet` is optimized for memory usage.
class ChunkedSortedStringSet : public WithCompare<ChunkedSortedStringSet> {
 public:
  class Options {
   public:
    Options() noexcept {}

    // Tunes the number of elements encoded together. A larger `chunk_size`
    // reduces memory usage, but the time complexity of lookups is roughly
    // proportional to `chunk_size`.
    //
    // Default: `kDefaultChunkSize` (16).
    static constexpr size_t kDefaultChunkSize = 16;
    Options& set_chunk_size(size_t chunk_size) & {
      chunk_size_ = chunk_size;
      return *this;
    }
    Options&& set_chunk_size(size_t chunk_size) && {
      return std::move(set_chunk_size(chunk_size));
    }
    size_t chunk_size() const { return chunk_size_; }

    // Expected final size, or 0 if unknown. This may improve performance and
    // memory usage.
    //
    // If the size hint turns out to not match reality, nothing breaks.
    //
    // Default: 0.
    Options& set_size_hint(size_t size_hint) & {
      size_hint_ = size_hint;
      return *this;
    }
    Options&& set_size_hint(size_t size_hint) && {
      return std::move(set_size_hint(size_hint));
    }
    size_t size_hint() const { return size_hint_; }

   private:
    size_t chunk_size_ = kDefaultChunkSize;
    size_t size_hint_ = 0;
  };

  class Iterator;
  class Builder;
  class NextInsertIterator;

  // Options for `Decode()`.
  class DecodeOptions {
   public:
    DecodeOptions() noexcept {}

    // If `false`, performs partial validation of the structure of data, which
    // is sufficient to prevent undefined behavior when the set is used. The
    // only aspect not validated is that elements are sorted and unique. This is
    // faster. If elements are not sorted and unique, then iteration yields
    // elements in the stored order, and `contains()` may fail to find an
    // element which can be seen during iteration.
    //
    // If `true`, performs full validation of encoded data, including checking
    // that elements are sorted and unique. This is slower. This can be used for
    // parsing untrusted data.
    //
    // Default: `false`.
    DecodeOptions& set_validate(bool validate) & {
      validate_ = validate;
      return *this;
    }
    DecodeOptions&& set_validate(bool validate) && {
      return std::move(set_validate(validate));
    }
    bool validate() const { return validate_; }

    // `Decode()` fails if more than `set_max_num_chunks()` chunks would need to
    // be created. This can be used for parsing untrusted data.
    //
    // Default: `std::vector<LinearSortedStringSet>().max_size()`.
    DecodeOptions& set_max_num_chunks(size_t max_num_chunks) & {
      max_num_chunks_ = max_num_chunks;
      return *this;
    }
    DecodeOptions&& set_max_num_chunks(size_t max_num_chunks) && {
      return std::move(set_max_num_chunks(max_num_chunks));
    }
    size_t max_num_chunks() const { return max_num_chunks_; }

    // `Decode()` fails if more than `max_encoded_chunk_size()` bytes would need
    // to be allocated for any chunk. This can be used for parsing untrusted
    // data.
    //
    // Default: `CompactString::max_size()`.
    DecodeOptions& set_max_encoded_chunk_size(size_t max_encoded_chunk_size) & {
      max_encoded_chunk_size_ = max_encoded_chunk_size;
      return *this;
    }
    DecodeOptions&& set_max_encoded_chunk_size(
        size_t max_encoded_chunk_size) && {
      return std::move(set_max_encoded_chunk_size(max_encoded_chunk_size));
    }
    size_t max_encoded_chunk_size() const { return max_encoded_chunk_size_; }

   private:
    bool validate_ = false;
    size_t max_num_chunks_ = std::vector<LinearSortedStringSet>().max_size();
    size_t max_encoded_chunk_size_ = CompactString::max_size();
  };

  using value_type = absl::string_view;
  using reference = value_type;
  using const_reference = reference;
  using iterator = Iterator;
  using const_iterator = iterator;
  using size_type = size_t;
  using difference_type = ptrdiff_t;

  // Creates a set consisting of the given elements. They must be sorted.
  // Consecutive duplicates are inserted only once.
  //
  // The type of `src` must support iteration yielding `absl::string_view`:
  // `for (const absl::string_view element : src)`,
  // e.g. `std::vector<std::string>`.
  //
  // If `Src` supports random access iteration,
  // `std::distance(begin(src), end(src))` is automatically used as
  // `Options::size_hint()`.
  template <
      typename Src,
      std::enable_if_t<IsIterableOf<Src, absl::string_view>::value, int> = 0>
  static ChunkedSortedStringSet FromSorted(Src&& src,
                                           Options options = Options());
  static ChunkedSortedStringSet FromSorted(
      std::initializer_list<absl::string_view> src,
      Options options = Options());

  // Creates a set consisting of the given elements. They do not need to be
  // sorted. Duplicates are inserted only once.
  //
  // The type of `src` must support iteration yielding `absl::string_view`:
  // `for (const absl::string_view element : src)`,
  // e.g. `std::vector<std::string>`.
  //
  // If duplicates are expected, `Options::size_hint()` should apply before
  // removing duplicates.
  //
  // If `Src` supports random access iteration,
  // `std::distance(begin(src), end(src))` is automatically used as
  // `Options::size_hint()`.
  template <
      typename Src,
      std::enable_if_t<IsIterableOf<Src, absl::string_view>::value, int> = 0>
  static ChunkedSortedStringSet FromUnsorted(Src&& src,
                                             Options options = Options());
  static ChunkedSortedStringSet FromUnsorted(
      std::initializer_list<absl::string_view> src,
      Options options = Options());

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

  friend bool operator==(const ChunkedSortedStringSet& a,
                         const ChunkedSortedStringSet& b) {
    return EqualImpl(a, b);
  }
  friend StrongOrdering RIEGELI_COMPARE(const ChunkedSortedStringSet& a,
                                        const ChunkedSortedStringSet& b) {
    return CompareImpl(a, b);
  }

  template <typename HashState>
  friend HashState AbslHashValue(HashState hash_state,
                                 const ChunkedSortedStringSet& self) {
    return self.AbslHashValueImpl(std::move(hash_state));
  }

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

  // Returns the size of data that would be written by `Encode()`.
  size_t EncodedSize() const;

  // Encodes the set to a sequence of bytes.
  //
  // As for now the encoding is not guaranteed to not change in future.
  // Please ask qrczak@google.com if you need stability.
  template <
      typename Dest,
      std::enable_if_t<IsValidDependency<Writer*, Dest&&>::value, int> = 0>
  absl::Status Encode(Dest&& dest) const;

  // Decodes the set from the encoded form.
  template <typename Src,
            std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int> = 0>
  absl::Status Decode(Src&& src, DecodeOptions options = DecodeOptions());

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

  static bool EqualImpl(const ChunkedSortedStringSet& a,
                        const ChunkedSortedStringSet& b);
  static StrongOrdering CompareImpl(const ChunkedSortedStringSet& a,
                                    const ChunkedSortedStringSet& b);
  template <typename HashState>
  HashState AbslHashValueImpl(HashState hash_state);

  absl::Status EncodeImpl(Writer& dest) const;
  absl::Status DecodeImpl(Reader& src, DecodeOptions options);

  // The first `LinearSortedStringSet` is stored inline to reduce object size
  // when the set is small.
  LinearSortedStringSet first_chunk_;
  // If `repr_is_inline()`: `chunks` are empty, `size` is `inline_repr()`.
  // If `repr_is_allocated()`: `*allocated_repr()` stores `chunks` and `size`.
  //
  // Invariant: if `first_chunk_.empty()` then `repr_is_inline()`.
  uintptr_t repr_ = kEmptyRepr;
};

// Iterates over a `LinearSortedStringSet` in the sorted order.
class ChunkedSortedStringSet::Iterator : public WithEqual<Iterator> {
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
  // `Iterator` (the string it points to is conditionally owned by `Iterator`).
  reference operator*() const {
    RIEGELI_ASSERT(current_iterator_ != LinearSortedStringSet::Iterator())
        << "Failed precondition of "
           "ChunkedSortedStringSet::Iterator::operator*: "
           "iterator is end()";
    return *current_iterator_;
  }

  pointer operator->() const {
    RIEGELI_ASSERT(current_iterator_ != LinearSortedStringSet::Iterator())
        << "Failed precondition of "
           "ChunkedSortedStringSet::Iterator::operator->: "
           "iterator is end()";
    return pointer(**this);
  }

  Iterator& operator++();
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

 private:
  friend class ChunkedSortedStringSet;  // For `Iterator()`.

  using ChunkIterator = ChunkedSortedStringSet::ChunkIterator;
  using Repr = ChunkedSortedStringSet::Repr;

  explicit Iterator(const ChunkedSortedStringSet* set)
      : current_iterator_(set->first_chunk_.cbegin()),
        next_chunk_iterator_(set->repr_is_inline()
                                 ? ChunkIterator()
                                 : set->allocated_repr()->chunks.cbegin()),
        set_(set) {}

  LinearSortedStringSet::Iterator current_iterator_;
  ChunkIterator next_chunk_iterator_ = ChunkIterator();
  const ChunkedSortedStringSet* set_ = nullptr;
};

// Builds a `ChunkedSortedStringSet` from a sorted sequence of strings.
class ChunkedSortedStringSet::Builder {
 public:
  // Begins with an empty set.
  explicit Builder(Options options = Options());

  Builder(Builder&& that) noexcept;
  Builder& operator=(Builder&& that) noexcept;

  ~Builder();

  // Returns an output iterator which inserts elements to this `Builder`.
  // Consecutive duplicates are inserted only once.
  //
  // Each inserted element must be greater than or equal to the last inserted
  // element.
  //
  // Inserting with a `NextInsertIterator` is equivalent to calling
  // `InsertNext()`. In particular if multiple iterators and explicit
  // `InsertNext()` calls are used together, then their combined element
  // sequence must be ordered.
  NextInsertIterator NextInserter();

  // Inserts an element. Consecutive duplicates are inserted only once.
  //
  // Precondition: `element` is greater than or equal to the last inserted
  // element.
  //
  // Returns `true` if `element` was inserted, or `false` if it is equal to the
  // last inserted element.
  //
  // If `std::string&&` is passed, it is moved only if the result is `true`.
  //
  // `std::string&&` is accepted with a template to avoid implicit conversions
  // to `std::string` which can be ambiguous against `absl::string_view`
  // (e.g. `const char*`).
  bool InsertNext(absl::string_view element);
  template <
      typename Element,
      std::enable_if_t<std::is_same<Element, std::string>::value, int> = 0>
  bool InsertNext(Element&& element);

  // Inserts an element. Elements out of order are skipped.
  //
  // Returns `true` if `element` was inserted, `false` if it is equal to the
  // last inserted element, or `absl::FailedPreconditionError()` if it is less
  // than the last inserted element.
  //
  // If `std::string&&` is passed, it is moved only if the result is `true`.
  //
  // `std::string&&` is accepted with a template to avoid implicit conversions
  // to `std::string` which can be ambiguous against `absl::string_view`
  // (e.g. `const char*`).
  absl::StatusOr<bool> TryInsertNext(absl::string_view element);
  template <
      typename Element,
      std::enable_if_t<std::is_same<Element, std::string>::value, int> = 0>
  absl::StatusOr<bool> TryInsertNext(Element&& element);

  // Returns `true` if the set is empty.
  bool empty() const {
    return first_chunk_ == absl::nullopt && current_builder_.empty();
  }

  // Returns the last inserted element. The set must not be empty.
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
  absl::StatusOr<bool> InsertNextImpl(Element&& element);

  size_t size_;
  size_t chunk_size_;
  size_t remaining_current_chunk_size_;

  // Invariant: if `first_chunk_ == absl::nullopt` then `chunks_.empty()`
  absl::optional<LinearSortedStringSet> first_chunk_;
  std::vector<LinearSortedStringSet> chunks_;
  LinearSortedStringSet::Builder current_builder_;
};

// Inserts elements to a `ChunkedSortedStringSet::Builder`. Consecutive
// duplicates are inserted only once.
//
// Each inserted element must be greater than or equal to the last inserted
// element.
class ChunkedSortedStringSet::NextInsertIterator {
 public:
  using iterator_concept = std::output_iterator_tag;
  using iterator_category = std::output_iterator_tag;
  using value_type = absl::string_view;
  using difference_type = ptrdiff_t;
  using pointer = void;

  class reference {
   public:
    // Inserts the next element.
    //
    // `std::string&&` is accepted with a template to avoid implicit conversions
    // to `std::string` which can be ambiguous against `absl::string_view`
    // (e.g. `const char*`).
    reference& operator=(absl::string_view element) {
      builder_->InsertNext(element);
      return *this;
    }
    template <
        typename Element,
        std::enable_if_t<std::is_same<Element, std::string>::value, int> = 0>
    reference& operator=(Element&& element) {
      // `std::move(element)` is correct and `std::forward<Element>(element)` is
      // not necessary: `Element` is always `std::string`, never an lvalue
      // reference.
      builder_->InsertNext(std::move(element));
      return *this;
    }

   private:
    friend class NextInsertIterator;
    explicit reference(Builder* builder) : builder_(builder) {}
    Builder* builder_;
  };

  // A sentinel value.
  NextInsertIterator() = default;

  NextInsertIterator(const NextInsertIterator& that) = default;
  NextInsertIterator& operator=(const NextInsertIterator& that) = default;

  reference operator*() const {
    RIEGELI_ASSERT(builder_ != nullptr)
        << "Failed precondition of NextInsertIterator::operator*: "
           "iterator is sentinel";
    return reference(builder_);
  }

  NextInsertIterator& operator++() { return *this; }
  NextInsertIterator operator++(int) { return ++*this; }

  Builder* builder() const { return builder_; }

 private:
  friend class Builder;  // For `NextInsertIterator()`.

  explicit NextInsertIterator(Builder* builder) : builder_(builder) {}

  Builder* builder_ = nullptr;
};

// Implementation details follow.

template <typename Src,
          std::enable_if_t<IsIterableOf<Src, absl::string_view>::value, int>>
ChunkedSortedStringSet ChunkedSortedStringSet::FromSorted(Src&& src,
                                                          Options options) {
  using std::begin;
  auto iter = begin(src);
  using std::end;
  auto end_iter = end(src);
  using SrcIterator = decltype(iter);
  if (std::is_convertible<
          typename std::iterator_traits<SrcIterator>::iterator_category,
          std::random_access_iterator_tag>::value) {
    options.set_size_hint(std::distance(iter, end_iter));
  }
  ChunkedSortedStringSet::Builder builder(std::move(options));
  for (; iter != end_iter; ++iter) {
    builder.InsertNext(MaybeMoveElement<Src>(*iter));
  }
  return std::move(builder).Build();
}

template <typename Src,
          std::enable_if_t<IsIterableOf<Src, absl::string_view>::value, int>>
inline ChunkedSortedStringSet ChunkedSortedStringSet::FromUnsorted(
    Src&& src, Options options) {
  using std::begin;
  auto iter = begin(src);
  using std::end;
  auto end_iter = end(src);
  using SrcIterator = decltype(iter);
  if (std::is_convertible<
          typename std::iterator_traits<SrcIterator>::iterator_category,
          std::random_access_iterator_tag>::value) {
    options.set_size_hint(std::distance(iter, end_iter));
  }
  std::vector<SrcIterator> iterators;
  iterators.reserve(options.size_hint());
  for (; iter != end_iter; ++iter) {
    iterators.push_back(iter);
  }
  std::sort(iterators.begin(), iterators.end(),
            [](const SrcIterator& a, const SrcIterator& b) {
              return absl::string_view(*a) < absl::string_view(*b);
            });

  options.set_size_hint(iterators.size());
  ChunkedSortedStringSet::Builder builder(std::move(options));
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

template <typename HashState>
HashState ChunkedSortedStringSet::AbslHashValueImpl(HashState hash_state) {
  for (const absl::string_view element : *this) {
    hash_state = HashState::combine(std::move(hash_state), element);
  }
  return HashState::combine(std::move(hash_state), size());
}

template <typename Dest,
          std::enable_if_t<IsValidDependency<Writer*, Dest&&>::value, int>>
inline absl::Status ChunkedSortedStringSet::Encode(Dest&& dest) const {
  Dependency<Writer*, Dest&&> dest_dep(std::forward<Dest>(dest));
  if (dest_dep.IsOwning()) dest_dep->SetWriteSizeHint(EncodedSize());
  absl::Status status = EncodeImpl(*dest_dep);
  if (dest_dep.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_dep->Close())) {
      status.Update(dest_dep->status());
    }
  }
  return status;
}

template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int>>
inline absl::Status ChunkedSortedStringSet::Decode(Src&& src,
                                                   DecodeOptions options) {
  Dependency<Reader*, Src&&> src_dep(std::forward<Src>(src));
  if (src_dep.IsOwning()) src_dep->SetReadAllHint(true);
  absl::Status status = DecodeImpl(*src_dep, options);
  if (src_dep.IsOwning()) {
    if (ABSL_PREDICT_TRUE(status.ok())) src_dep->VerifyEnd();
    if (ABSL_PREDICT_FALSE(!src_dep->Close())) status.Update(src_dep->status());
  }
  return status;
}

inline ChunkedSortedStringSet::NextInsertIterator
ChunkedSortedStringSet::Builder::NextInserter() {
  return NextInsertIterator(this);
}

extern template bool ChunkedSortedStringSet::Builder::InsertNext(
    std::string&& element);

extern template absl::StatusOr<bool>
ChunkedSortedStringSet::Builder::TryInsertNext(std::string&& element);

}  // namespace riegeli

#endif  // RIEGELI_CONTAINERS_CHUNKED_SORTED_STRING_SET_H_
