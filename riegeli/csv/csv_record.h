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

#ifndef RIEGELI_CSV_CSV_RECORD_H_
#define RIEGELI_CSV_CSV_RECORD_H_

#include <stddef.h>

#include <algorithm>
#include <functional>
#include <initializer_list>
#include <iosfwd>
#include <iterator>
#include <new>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/base/call_once.h"
#include "absl/base/optimization.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/global.h"
#include "riegeli/base/iterable.h"
#include "riegeli/base/reset.h"
#include "riegeli/base/shared_ptr.h"
#include "riegeli/base/string_ref.h"
#include "riegeli/base/type_traits.h"
#include "riegeli/bytes/stringify_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli::csv_internal {

// A pair-like type which supports C++20 `std::common_reference` with similar
// enough pairs. This is needed for `CsvRecord::{,const_}iterator` to satisfy
// C++20 input iterator requirements.
//
// Since C++23, `std::pair<T1, T2>` can be used directly instead, because it has
// the necessary conversions and `std::basic_common_reference` specializations.
template <typename T1, typename T2>
class ReferencePair : public std::pair<T1, T2> {
 public:
  using ReferencePair::pair::pair;

  template <
      class U1, class U2,
      std::enable_if_t<
          std::conjunction_v<
              std::is_constructible<T1, U1&>, std::is_constructible<T2, U2&>,
              std::negation<std::conjunction<std::is_convertible<U1&, T1>,
                                             std::is_convertible<U2&, T2>>>>,
          int> = 0>
  explicit constexpr ReferencePair(std::pair<U1, U2>& p)
      : ReferencePair::pair(p.first, p.second) {}

  template <class U1, class U2,
            std::enable_if_t<std::conjunction_v<std::is_convertible<U1&, T1>,
                                                std::is_convertible<U2&, T2>>,
                             int> = 0>
  /*implicit*/ constexpr ReferencePair(std::pair<U1, U2>& p)
      : ReferencePair::pair(p.first, p.second) {}
};

// `ToStringVector()` converts an iterable of elements convertible to
// `absl::string_view` to a `std::vector<std::string>`.

template <
    typename Values,
    std::enable_if_t<IsIterableOf<Values, absl::string_view>::value, int> = 0>
inline std::vector<std::string> ToStringVector(Values&& values) {
  using std::begin;
  using std::end;
  return std::vector<std::string>(MaybeMakeMoveIterator<Values>(begin(values)),
                                  MaybeMakeMoveIterator<Values>(end(values)));
}

}  // namespace riegeli::csv_internal

#if __cplusplus >= 202002L

template <typename T1, typename T2, template <typename> class TQual,
          template <typename> class UQual>
struct std::basic_common_reference<riegeli::csv_internal::ReferencePair<T1, T2>,
                                   std::pair<std::string, std::string>, TQual,
                                   UQual> {
  using type = riegeli::csv_internal::ReferencePair<
      std::common_reference_t<TQual<T1>, UQual<std::string>>,
      std::common_reference_t<TQual<T2>, UQual<std::string>>>;
};

template <typename T1, typename T2, template <typename> class TQual,
          template <typename> class UQual>
struct std::basic_common_reference<std::pair<std::string, std::string>,
                                   riegeli::csv_internal::ReferencePair<T1, T2>,
                                   TQual, UQual> {
  using type = riegeli::csv_internal::ReferencePair<
      std::common_reference_t<TQual<std::string>, UQual<T1>>,
      std::common_reference_t<TQual<std::string>, UQual<T2>>>;
};

template <typename T1, typename T2, typename U1, typename U2,
          template <typename> class TQual, template <typename> class UQual>
struct std::basic_common_reference<riegeli::csv_internal::ReferencePair<T1, T2>,
                                   riegeli::csv_internal::ReferencePair<U1, U2>,
                                   TQual, UQual> {
  using type = riegeli::csv_internal::ReferencePair<
      std::common_reference_t<TQual<T1>, UQual<U1>>,
      std::common_reference_t<TQual<T2>, UQual<U2>>>;
};

#endif

namespace riegeli {

// A normalizer for `CsvHeader` and `CsvReaderBase::Options::set_normalizer()`,
// providing case insensitive matching.
//
// You may pass a pointer to this function, without wrapping it in a lambda
// (it will not be overloaded).
std::string AsciiCaseInsensitive(absl::string_view name);

// A set of field names. This is commonly specified in a CSV file header.
//
// This is conceptually a set of strings, which remembers the order in which
// they have been added.
//
// Copying a `CsvHeader` object is cheap, sharing the actual set.
class CsvHeader : public WithEqual<CsvHeader> {
 public:
  class iterator : public WithCompare<iterator> {
   public:
    using iterator_category = std::random_access_iterator_tag;
    using value_type = std::string;
    using reference = const std::string&;
    using pointer = const std::string*;
    using difference_type = ptrdiff_t;

    iterator() = default;

    iterator(const iterator& that) = default;
    iterator& operator=(const iterator& that) = default;

    reference operator*() const;
    pointer operator->() const;
    iterator& operator++();
    iterator operator++(int);
    iterator& operator--();
    iterator operator--(int);
    iterator& operator+=(difference_type n);
    iterator operator+(difference_type n) const;
    iterator& operator-=(difference_type n);
    iterator operator-(difference_type n) const;
    reference operator[](difference_type n) const;

    friend bool operator==(iterator a, iterator b) {
      return a.iter_ == b.iter_;
    }
    friend StrongOrdering RIEGELI_COMPARE(iterator a, iterator b) {
      if (a.iter_ < b.iter_) return StrongOrdering::less;
      if (a.iter_ > b.iter_) return StrongOrdering::greater;
      return StrongOrdering::equal;
    }
    friend difference_type operator-(iterator a, iterator b) {
      return a.iter_ - b.iter_;
    }
    friend iterator operator+(difference_type n, iterator a) { return a + n; }

   private:
    friend class CsvHeader;

    explicit iterator(std::vector<std::string>::const_iterator iter)
        : iter_(iter) {}

    std::vector<std::string>::const_iterator iter_{};
  };

  using key_type = std::string;
  using value_type = std::string;
  using reference = const std::string&;
  using const_reference = reference;
  using pointer = const std::string*;
  using const_pointer = pointer;
  using const_iterator = iterator;
  using reverse_iterator = std::reverse_iterator<iterator>;
  using const_reverse_iterator = reverse_iterator;
  using size_type = size_t;
  using difference_type = ptrdiff_t;

  // Creates a `CsvHeader` with no field names.
  CsvHeader() = default;

  // Creates a `CsvHeader` with no field names.
  //
  // Field names are matched by passing them through `normalizer` first.
  // `nullptr` is the same as the identity function.
  //
  // `riegeli::AsciiCaseInsensitive` is a normalizer providing case insensitive
  // matching.
  explicit CsvHeader(std::function<std::string(absl::string_view)> normalizer);

  // Creates a set consisting of the given sequence of field names.
  //
  // The type of `names` must support iteration yielding `absl::string_view`:
  // `for (const absl::string_view name : names)`,
  // e.g. `std::vector<std::string>`.
  //
  // Precondition: `names` have no duplicates
  template <typename Names,
            std::enable_if_t<
                std::conjunction_v<NotSameRef<CsvHeader, Names>,
                                   IsIterableOf<Names, absl::string_view>>,
                int> = 0>
  explicit CsvHeader(Names&& names);
  /*implicit*/ CsvHeader(std::initializer_list<absl::string_view> names);
  CsvHeader& operator=(std::initializer_list<absl::string_view> names);

  // Creates a set consisting of the given sequence of field names.
  //
  // The type of `names` must support iteration yielding `absl::string_view`:
  // `for (const absl::string_view name : names)`,
  // e.g. `std::vector<std::string>`.
  //
  // Field names are matched by passing them through `normalizer` first.
  // `nullptr` is the same as the identity function.
  //
  // `riegeli::AsciiCaseInsensitive` is a normalizer providing case insensitive
  // matching.
  //
  // Precondition: normalized `names` have no duplicates
  template <
      typename Names,
      std::enable_if_t<IsIterableOf<Names, absl::string_view>::value, int> = 0>
  explicit CsvHeader(std::function<std::string(absl::string_view)> normalizer,
                     Names&& names);
  explicit CsvHeader(std::function<std::string(absl::string_view)> normalizer,
                     std::initializer_list<absl::string_view> names);

  CsvHeader(const CsvHeader& that) = default;
  CsvHeader& operator=(const CsvHeader& that) = default;

  // The source `CsvHeader` is left empty.
  CsvHeader(CsvHeader&& that) = default;
  CsvHeader& operator=(CsvHeader&& that) = default;

  // Makes `*this` equivalent to a newly constructed `CsvHeader`.
  //
  // Precondition: like for the corresponding constructor
  ABSL_ATTRIBUTE_REINITIALIZES void Reset();
  template <typename Names,
            std::enable_if_t<
                std::conjunction_v<NotSameRef<CsvHeader, Names>,
                                   IsIterableOf<Names, absl::string_view>>,
                int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Names&& names);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      std::initializer_list<absl::string_view> names);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      std::function<std::string(absl::string_view)> normalizer);
  template <
      typename Names,
      std::enable_if_t<IsIterableOf<Names, absl::string_view>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      std::function<std::string(absl::string_view)> normalizer, Names&& names);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      std::function<std::string(absl::string_view)> normalizer,
      std::initializer_list<absl::string_view> names);

  // Makes `*this` equivalent to a newly constructed `CsvHeader`, reporting
  // whether construction was successful.
  //
  // Return values:
  //  * `absl::OkStatus()`                 - `CsvHeader` is set to `names`
  //  * `absl::FailedPreconditionError(_)` - `names` had duplicates,
  //                                         `CsvHeader` is empty
  template <typename Names,
            std::enable_if_t<
                std::conjunction_v<NotSameRef<CsvHeader, Names>,
                                   IsIterableOf<Names, absl::string_view>>,
                int> = 0>
  absl::Status TryReset(Names&& names);
  absl::Status TryReset(std::initializer_list<absl::string_view> names);
  template <
      typename Names,
      std::enable_if_t<IsIterableOf<Names, absl::string_view>::value, int> = 0>
  absl::Status TryReset(
      std::function<std::string(absl::string_view)> normalizer, Names&& names);
  absl::Status TryReset(
      std::function<std::string(absl::string_view)> normalizer,
      std::initializer_list<absl::string_view> names);

  // Reserve space for future calls to `Add()` or `TryAdd()` if the expected
  // final number of fields is known. This improves performance.
  void Reserve(size_t size);

  // Adds the given field `name`, ordered at the end.
  //
  // Precondition: `name` was not already present
  void Add(StringInitializer name);

  // Equivalent to calling `Add()` for each name in order.
  //
  // Precondition: like for `Add()`
  template <typename... Names,
            std::enable_if_t<(sizeof...(Names) > 0), int> = 0>
  void Add(StringInitializer name, Names&&... names);

  // Adds the given field `name`, ordered at the end, reporting whether this was
  // successful.
  //
  // Return values:
  //  * `absl::OkStatus()`                 - `name` has been added
  //  * `absl::FailedPreconditionError(_)` - `name` was already present,
  //                                         `CsvHeader` is unchanged
  absl::Status TryAdd(StringInitializer name);

  // Equivalent to calling `TryAdd()` for each name in order.
  //
  // Returns early in case of a failure.
  template <typename... Names,
            std::enable_if_t<(sizeof...(Names) > 0), int> = 0>
  absl::Status TryAdd(StringInitializer name, Names&&... names);

  // Returns the sequence of field names, in the order in which they have been
  // added.
  absl::Span<const std::string> names() const ABSL_ATTRIBUTE_LIFETIME_BOUND;

  // Returns the normalizer used to match field names, or `nullptr` which is the
  // same as the identity function.
  const std::function<std::string(absl::string_view)>& normalizer() const
      ABSL_ATTRIBUTE_LIFETIME_BOUND;

  // Iterates over field names, in the order in which they have been added.
  iterator begin() const ABSL_ATTRIBUTE_LIFETIME_BOUND;
  iterator cbegin() const ABSL_ATTRIBUTE_LIFETIME_BOUND { return begin(); }
  iterator end() const ABSL_ATTRIBUTE_LIFETIME_BOUND;
  iterator cend() const ABSL_ATTRIBUTE_LIFETIME_BOUND { return end(); }

  // Iterates over field names, backwards.
  reverse_iterator rbegin() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return reverse_iterator(end());
  }
  reverse_iterator crbegin() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return rbegin();
  }
  reverse_iterator rend() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return reverse_iterator(begin());
  }
  reverse_iterator crend() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return rend();
  }

  // Returns `true` if there are no field names.
  bool empty() const;

  // Returns the number of field names.
  size_t size() const;

  // Returns an iterator positioned at `name`, or `end()` if `name` is not
  // present.
  iterator find(absl::string_view name) const ABSL_ATTRIBUTE_LIFETIME_BOUND;

  // Returns `true` if `name` is present.
  bool contains(absl::string_view name) const;

  // Returns the position of `name` in the sequence of field names, or
  // `std::nullopt` if `name` is not present.
  //
  // This can be used together with `CsvRecord::fields()` to look up the same
  // field in multiple `CsvRecord`s sharing a `CsvHeader`.
  std::optional<size_t> IndexOf(absl::string_view name) const;

  // Compares the sequence of field names. Does not compare the normalizer.
  friend bool operator==(const CsvHeader& a, const CsvHeader& b) {
    return Equal(a, b);
  }

  // Renders contents in a human-readable way.
  std::string DebugString() const;

  // Default stringification by `absl::StrCat()` etc.
  //
  // Writes `src.DebugString()` to `dest`.
  template <typename Sink>
  friend void AbslStringify(Sink& dest, const CsvHeader& src) {
    StringifyWriter<Sink*> writer(&dest);
    src.WriteDebugStringTo(writer);
    writer.Close();
  }

  // Writes `src.DebugString()` to `dest`.
  friend std::ostream& operator<<(std::ostream& dest, const CsvHeader& src) {
    src.Output(dest);
    return dest;
  }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const CsvHeader* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&self->payload_);
  }

 private:
  struct Payload {
    Payload() = default;
    Payload(std::function<std::string(absl::string_view)>&& normalizer)
        : normalizer(std::move(normalizer)) {}
    Payload(const Payload& that);

    // Supports `MemoryEstimator`.
    template <typename MemoryEstimator>
    friend void RiegeliRegisterSubobjects(const Payload* self,
                                          MemoryEstimator& memory_estimator) {
      // Ignore `normalizer`. Even if not `nullptr`, usually it is stateless.
      memory_estimator.RegisterSubobjects(&self->index_to_name);
      memory_estimator.RegisterSubobjects(&self->name_to_index);
    }

    std::function<std::string(absl::string_view)> normalizer;
    std::vector<std::string> index_to_name;
    absl::flat_hash_map<std::string, size_t> name_to_index;

    // Invariants:
    //  * `!index_to_name.empty()`
    //  * `name_to_index.size() == index_to_name.size()`
    //  * for each `i` below `index_to_name.size()`:
    //        `name_to_index[normalizer == nullptr
    //                           ? index_to_name[i]
    //                           : normalizer(index_to_name[i])] == i`
  };

  template <typename Names,
            std::enable_if_t<IsRandomAccessIterable<Names>::value, int> = 0>
  absl::Status TryResetInternal(
      std::function<std::string(absl::string_view)>&& normalizer,
      Names&& names);
  template <typename Names,
            std::enable_if_t<!IsRandomAccessIterable<Names>::value, int> = 0>
  absl::Status TryResetInternal(
      std::function<std::string(absl::string_view)>&& normalizer,
      Names&& names);
  absl::Status TryResetInternal(
      std::function<std::string(absl::string_view)>&& normalizer,
      std::vector<std::string>&& names);

  // Handles `TryReset()` for `names` which are empty or which match the cached
  // payload. Returns `true` if done.
  template <typename Names>
  bool MaybeResetToCachedPayload(Names&& names);

  // Handles `TryReset()` for `names` which are not empty and which do not match
  // the cached payload.
  absl::Status TryResetUncached(
      std::function<std::string(absl::string_view)>&& normalizer,
      std::vector<std::string>&& names);

  void EnsureUnique();

  static bool Equal(const CsvHeader& a, const CsvHeader& b);

  void WriteDebugStringTo(Writer& dest) const;
  void Output(std::ostream& dest) const;

  // A one-element cache of a recently constructed `Payload`, to reuse the
  // `Payload` when multiple `CsvHeader` objects are created from the same
  // iterable of field names. Its `normalizer` is always `nullptr` and its
  // `index_to_name` is never empty.
  //
  // Reusing `CsvHeader` directly is more efficient but not always feasible.
  ABSL_CONST_INIT static absl::Mutex payload_cache_mutex_;
  ABSL_CONST_INIT static SharedPtr<Payload> payload_cache_
      ABSL_GUARDED_BY(payload_cache_mutex_);

  SharedPtr<Payload> payload_;
};

// `CsvHeaderConstant<n>` lazily constructs and stores a `CsvHeader` with `n`
// fields, and never calls its destructor.
//
// It should be used as the type of a variable with static storage duration.
//
// By relying on CTAD the template argument can be deduced from constructor
// arguments.
template <size_t num_fields>
class CsvHeaderConstant {
 public:
  // Will create a `CsvHeader` consisting of the given sequence of field names.
  //
  // The number of `fields` must be `num_fields`, and all `fields` must have
  // static storage duration.
  template <
      typename... Fields,
      std::enable_if_t<std::conjunction_v<
                           std::bool_constant<sizeof...(Fields) == num_fields>,
                           std::is_convertible<Fields&&, absl::string_view>...>,
                       int> = 0>
  /*implicit*/ constexpr CsvHeaderConstant(
      Fields&&... fields ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : fields_{std::forward<Fields>(fields)...} {}

  // Will create a `CsvHeader` consisting of the given sequence of field names.
  //
  // The number of `fields` must be `num_fields`, and all `fields` must have
  // static storage duration.
  //
  // Field names are matched by passing them through `normalizer` first.
  // `nullptr` is the same as the identity function.
  template <
      typename... Fields,
      std::enable_if_t<std::conjunction_v<
                           std::bool_constant<sizeof...(Fields) == num_fields>,
                           std::is_convertible<Fields&&, absl::string_view>...>,
                       int> = 0>
  explicit constexpr CsvHeaderConstant(
      std::string (*normalizer)(absl::string_view),
      Fields&&... fields ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : normalizer_(normalizer), fields_{std::forward<Fields>(fields)...} {}

  // Will create a `CsvHeader` consisting of field names from `base_header`
  // followed by the given sequence of field names.
  //
  // The number of fields in `base_header` plus the number of `fields` must be
  // `num_fields`, and `base_header` and all `fields` must have static storage
  // duration.
  //
  // The normalizer is the same as in `base_header`.
  template <
      size_t base_num_fields, typename... Fields,
      std::enable_if_t<
          std::conjunction_v<
              std::integral_constant<
                  bool, base_num_fields + sizeof...(Fields) == num_fields>,
              std::is_convertible<Fields&&, absl::string_view>...>,
          int> = 0>
  explicit constexpr CsvHeaderConstant(
      const CsvHeaderConstant<base_num_fields>& base_header
          ABSL_ATTRIBUTE_LIFETIME_BOUND,
      Fields&&... fields ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : CsvHeaderConstant(base_header,
                          std::make_index_sequence<base_num_fields>(),
                          std::forward<Fields>(fields)...) {}

  CsvHeaderConstant(const CsvHeaderConstant&) = delete;
  CsvHeaderConstant& operator=(const CsvHeaderConstant&) = delete;

  const CsvHeader* get() const ABSL_ATTRIBUTE_LIFETIME_BOUND;
  const CsvHeader& operator*() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return *get();
  }
  const CsvHeader* operator->() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return get();
  }

 private:
  // For `normalizer_` and `fields_`.
  template <size_t other_num_fields>
  friend class CsvHeaderConstant;

  template <size_t base_num_fields, size_t... base_indices, typename... Fields>
  explicit constexpr CsvHeaderConstant(
      const CsvHeaderConstant<base_num_fields>& base_header,
      std::index_sequence<base_indices...>, Fields&&... fields)
      : normalizer_(base_header.normalizer_),
        fields_{base_header.fields_[base_indices]...,
                std::forward<Fields>(fields)...} {}

  std::string (*const normalizer_)(absl::string_view) = nullptr;
  const absl::string_view fields_[num_fields];
  mutable absl::once_flag once_;
  alignas(CsvHeader) mutable char header_[sizeof(CsvHeader)] = {};
};

template <
    typename... Fields,
    std::enable_if_t<
        std::conjunction_v<std::is_convertible<Fields&&, absl::string_view>...>,
        int> = 0>
/*implicit*/ CsvHeaderConstant(Fields&&... fields)
    -> CsvHeaderConstant<sizeof...(Fields)>;
template <
    typename... Fields,
    std::enable_if_t<
        std::conjunction_v<std::is_convertible<Fields&&, absl::string_view>...>,
        int> = 0>
explicit CsvHeaderConstant(std::string (*normalizer)(absl::string_view),
                           Fields&&... fields)
    -> CsvHeaderConstant<sizeof...(Fields)>;
template <
    size_t base_num_fields, typename... Fields,
    std::enable_if_t<
        std::conjunction_v<std::is_convertible<Fields&&, absl::string_view>...>,
        int> = 0>
explicit CsvHeaderConstant(
    const CsvHeaderConstant<base_num_fields>& base_header, Fields&&... fields)
    -> CsvHeaderConstant<base_num_fields + sizeof...(Fields)>;

// A row of a CSV file, with fields accessed by name.
//
// This is conceptually a mapping from field names to field values, with a fixed
// set of field names. The set of field names is expressed as `CsvHeader`.
class CsvRecord : public WithEqual<CsvRecord> {
 private:
  // Implementation shared between `iterator` and `const_iterator`.
  template <typename FieldIterator>
  class IteratorImpl : public WithCompare<IteratorImpl<FieldIterator>> {
   public:
    using iterator_concept = std::random_access_iterator_tag;
    // `iterator_category` is only `std::input_iterator_tag` because the
    // `LegacyForwardIterator` requirement and above require `reference` to be
    // a true reference type.
    using iterator_category = std::input_iterator_tag;
    using value_type = std::pair<std::string, std::string>;
    using reference = csv_internal::ReferencePair<
        const std::string&,
        typename std::iterator_traits<FieldIterator>::reference>;
    using difference_type = ptrdiff_t;

    class pointer {
     public:
      const reference* operator->() const { return &ref_; }

     private:
      friend class IteratorImpl<FieldIterator>;
      explicit pointer(reference ref) : ref_(ref) {}
      reference ref_;
    };

    IteratorImpl() = default;

    // Conversion from `iterator` to `const_iterator`.
    template <
        typename ThatFieldIterator,
        std::enable_if_t<
            std::conjunction_v<
                std::negation<std::is_same<ThatFieldIterator, FieldIterator>>,
                std::is_convertible<ThatFieldIterator, FieldIterator>>,
            int> = 0>
    /*implicit*/ IteratorImpl(IteratorImpl<ThatFieldIterator> that) noexcept;

    IteratorImpl(const IteratorImpl& that) = default;
    IteratorImpl& operator=(const IteratorImpl& that) = default;

    reference operator*() const;
    pointer operator->() const;
    IteratorImpl& operator++();
    IteratorImpl operator++(int);
    IteratorImpl& operator--();
    IteratorImpl operator--(int);
    IteratorImpl& operator+=(difference_type n);
    IteratorImpl operator+(difference_type n) const;
    IteratorImpl& operator-=(difference_type n);
    IteratorImpl operator-(difference_type n) const;
    reference operator[](difference_type n) const;

    friend bool operator==(IteratorImpl a, IteratorImpl b) {
      return a.field_iter_ == b.field_iter_;
    }
    friend StrongOrdering RIEGELI_COMPARE(IteratorImpl a, IteratorImpl b) {
      if (a.field_iter_ < b.field_iter_) return StrongOrdering::less;
      if (a.field_iter_ > b.field_iter_) return StrongOrdering::greater;
      return StrongOrdering::equal;
    }
    friend difference_type operator-(IteratorImpl a, IteratorImpl b) {
      return a.field_iter_ - b.field_iter_;
    }
    friend IteratorImpl operator+(difference_type n, IteratorImpl a) {
      return a + n;
    }

   private:
    friend class CsvRecord;

    explicit IteratorImpl(CsvHeader::iterator name_iter,
                          FieldIterator field_iter);

    // Invariant:
    //   `name_iter_ - header_.begin() == field_iter_ - fields_.begin()`
    CsvHeader::iterator name_iter_;
    FieldIterator field_iter_;
  };

 public:
  using key_type = std::string;
  using mapped_type = std::string;
  using value_type = std::pair<std::string, std::string>;
  using reference =
      csv_internal::ReferencePair<const std::string&, std::string&>;
  using const_reference =
      csv_internal::ReferencePair<const std::string&, const std::string&>;
  using iterator = IteratorImpl<std::vector<std::string>::iterator>;
  using const_iterator = IteratorImpl<std::vector<std::string>::const_iterator>;
  using reverse_iterator = std::reverse_iterator<iterator>;
  using const_reverse_iterator = std::reverse_iterator<const_iterator>;
  using size_type = size_t;
  using difference_type = ptrdiff_t;

  // Creates a `CsvRecord` with no fields.
  CsvRecord() = default;

  // Creates a `CsvRecord` with the given field names, and with all field values
  // empty.
  explicit CsvRecord(CsvHeader header);

  // Creates a `CsvRecord` with the given field names and field values in the
  // corresponding order.
  //
  // Precondition: `header.size() == fields.size()`
  template <
      typename Fields,
      std::enable_if_t<IsIterableOf<Fields, absl::string_view>::value, int> = 0>
  explicit CsvRecord(CsvHeader header, Fields&& fields);
  explicit CsvRecord(CsvHeader header,
                     std::initializer_list<absl::string_view> fields);

  CsvRecord(const CsvRecord& that) = default;
  CsvRecord& operator=(const CsvRecord& that) = default;

  // The source `CsvRecord` is left empty.
  CsvRecord(CsvRecord&& that) = default;
  CsvRecord& operator=(CsvRecord&& that) = default;

  // Returns the set of field names.
  const CsvHeader& header() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return header_;
  }

  // Makes `*this` equivalent to a newly constructed `CsvRecord`.
  //
  // Precondition: like for the corresponding constructor
  ABSL_ATTRIBUTE_REINITIALIZES void Reset();
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(CsvHeader header);
  template <
      typename Fields,
      std::enable_if_t<IsIterableOf<Fields, absl::string_view>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(CsvHeader header, Fields&& fields);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      CsvHeader header, std::initializer_list<absl::string_view> fields);

  // Makes `*this` equivalent to a newly constructed `CsvRecord`, reporting
  // whether construction was successful.
  //
  // Return values:
  //  * `absl::OkStatus()`                 - `CsvRecord` is set to `header`
  //                                         and `fields`
  //  * `absl::FailedPreconditionError(_)` - lengths of `header` and `fields`
  //                                         do not match, `CsvRecord` is empty
  template <
      typename Fields,
      std::enable_if_t<IsIterableOf<Fields, absl::string_view>::value, int> = 0>
  absl::Status TryReset(CsvHeader header, Fields&& fields);
  absl::Status TryReset(CsvHeader header,
                        std::initializer_list<absl::string_view> fields);

  // Makes all field values empty. The number of fields is unchanged.
  void Clear();

  // Returns the sequence of field values, in the order corresponding to the
  // order of field names in the header.
  absl::Span<std::string> fields() ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return absl::MakeSpan(fields_);
  }
  absl::Span<const std::string> fields() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return fields_;
  }

  // Iterates over pairs of field names and field values, in the order
  // corresponding to the order of field names in the header.
  iterator begin() ABSL_ATTRIBUTE_LIFETIME_BOUND;
  const_iterator begin() const ABSL_ATTRIBUTE_LIFETIME_BOUND;
  const_iterator cbegin() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return begin();
  }
  iterator end() ABSL_ATTRIBUTE_LIFETIME_BOUND;
  const_iterator end() const ABSL_ATTRIBUTE_LIFETIME_BOUND;
  const_iterator cend() const ABSL_ATTRIBUTE_LIFETIME_BOUND { return end(); }

  // Iterates over pairs of field names and field values, backwards.
  reverse_iterator rbegin() ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return reverse_iterator(end());
  }
  const_reverse_iterator rbegin() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return const_reverse_iterator(end());
  }
  const_reverse_iterator crbegin() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return rbegin();
  }
  reverse_iterator rend() ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return reverse_iterator(begin());
  }
  const_reverse_iterator rend() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return const_reverse_iterator(begin());
  }
  const_reverse_iterator crend() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return rend();
  }

  // Returns `true` if there are no fields.
  bool empty() const { return fields_.empty(); }

  // Returns the number of field names, which is the same as the number of field
  // values.
  size_t size() const { return fields_.size(); }

  // Returns a reference to the field value corresponding to the given field
  // `name`.
  //
  // Precondition: `name` is present
  std::string& operator[](absl::string_view name) ABSL_ATTRIBUTE_LIFETIME_BOUND;
  const std::string& operator[](absl::string_view name) const
      ABSL_ATTRIBUTE_LIFETIME_BOUND;

  // Returns an iterator positioned at the pair of the given field `name` and
  // the corresponding field value, or `end()` if `name` is not present.
  iterator find(absl::string_view name) ABSL_ATTRIBUTE_LIFETIME_BOUND;
  const_iterator find(absl::string_view name) const
      ABSL_ATTRIBUTE_LIFETIME_BOUND;

  // Returns `true` if `name` is present.
  bool contains(absl::string_view name) const;

  // Sets all fields resulting from iteration over another iterable of pairs of
  // field names and field values, which can be an associative container or
  // another `CsvRecord`.
  //
  // This can be used to convert a `CsvRecord` to a superset of fields, as long
  // as fields to be preserved have the same names.
  //
  // Preconditions:
  //  * all fields from `src` are present in `*this`
  template <
      typename Src,
      std::enable_if_t<IsIterableOf<Src, std::pair<absl::string_view,
                                                   absl::string_view>>::value,
                       int> = 0>
  void Merge(Src&& src);
  void Merge(
      std::initializer_list<std::pair<absl::string_view, absl::string_view>>
          src);

  // Sets all fields resulting from iteration over another iterable of pairs of
  // field names and field values, which can be an associative container or
  // another `CsvRecord`. Reports whether that was successful.
  //
  // This can be used to convert a `CsvRecord` to a different set of fields, as
  // long as fields to be preserved have the same names.
  //
  // Return values:
  //  * `absl::OkStatus()`                 - all fields from `src` have been set
  //  * `absl::FailedPreconditionError(_)` - some fields were absent in `*this`,
  //                                         only the intersection of fields
  //                                         has been set
  template <
      typename Src,
      std::enable_if_t<IsIterableOf<Src, std::pair<absl::string_view,
                                                   absl::string_view>>::value,
                       int> = 0>
  absl::Status TryMerge(Src&& src);
  absl::Status TryMerge(
      std::initializer_list<std::pair<absl::string_view, absl::string_view>>
          src);

  // Assigns corresponding field values to all field values resulting from
  // iteration over another iterable of pairs of field names and field values,
  // which can be an associative container or another `CsvRecord`.
  //
  // This can be used to project a `CsvRecord` to a subset of fields, as long
  // as fields to be preserved have the same names.
  //
  // Preconditions:
  //  * all fields from `dest` are present in `*this`
  template <typename Dest,
            std::enable_if_t<IsIterableOfPairsWithAssignableValues<
                                 Dest, absl::string_view, std::string>::value,
                             int> = 0>
  void Split(Dest& dest) const;

  // Assigns corresponding field values to all field values resulting from
  // iteration over another iterable of pairs of field names and field values,
  // which can be an associative container or another `CsvRecord`. Reports
  // whether that was successful.
  //
  // This can be used to project a `CsvRecord` to a subset of fields, as long
  // as fields to be preserved have the same names.
  //
  // Return values:
  //  * `absl::OkStatus()`                 - all fields in `dest` have been set
  //  * `absl::FailedPreconditionError(_)` - some fields were absent in `*this`,
  //                                         only the intersection of fields
  //                                         has been set
  template <typename Dest,
            std::enable_if_t<IsIterableOfPairsWithAssignableValues<
                                 Dest, absl::string_view, std::string>::value,
                             int> = 0>
  absl::Status TrySplit(Dest& dest) const;

  friend bool operator==(const CsvRecord& a, const CsvRecord& b) {
    return Equal(a, b);
  }

  // Renders contents in a human-readable way.
  std::string DebugString() const;

  // Default stringification by `absl::StrCat()` etc.
  //
  // Writes `src.DebugString()` to `dest`.
  template <typename Sink>
  friend void AbslStringify(Sink& dest, const CsvRecord& src) {
    StringifyWriter<Sink*> writer(&dest);
    src.WriteDebugStringTo(writer);
    writer.Close();
  }

  // Writes `src.DebugString()` to `dest`.
  friend std::ostream& operator<<(std::ostream& dest, const CsvRecord& src) {
    src.Output(dest);
    return dest;
  }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const CsvRecord* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&self->header_);
    memory_estimator.RegisterSubobjects(&self->fields_);
  }

 private:
  friend class CsvReaderBase;

  template <typename Fields>
  absl::Status TryResetInternal(CsvHeader&& header, Fields&& fields);
  absl::Status TryResetInternal(CsvHeader&& header,
                                std::vector<std::string>&& fields);

  absl::Status FailMissingNames(
      absl::Span<const std::string> missing_names) const;

  static bool Equal(const CsvRecord& a, const CsvRecord& b);

  void WriteDebugStringTo(Writer& dest) const;
  void Output(std::ostream& dest) const;

  // Invariant: `header_.size() == fields_.size()`
  CsvHeader header_;
  std::vector<std::string> fields_;
};

namespace csv_internal {
void WriteDebugQuotedIfNeeded(absl::string_view src, Writer& dest);
}  // namespace csv_internal

// Implementation details follow.

inline typename CsvHeader::iterator::reference CsvHeader::iterator::operator*()
    const {
  return *iter_;
}

inline typename CsvHeader::iterator::pointer CsvHeader::iterator::operator->()
    const {
  return &*iter_;
}

inline CsvHeader::iterator& CsvHeader::iterator::operator++() {
  ++iter_;
  return *this;
}

inline CsvHeader::iterator CsvHeader::iterator::operator++(int) {
  const iterator tmp = *this;
  ++*this;
  return tmp;
}

inline CsvHeader::iterator& CsvHeader::iterator::operator--() {
  --iter_;
  return *this;
}

inline CsvHeader::iterator CsvHeader::iterator::operator--(int) {
  const iterator tmp = *this;
  --*this;
  return tmp;
}

inline CsvHeader::iterator& CsvHeader::iterator::operator+=(difference_type n) {
  iter_ += n;
  return *this;
}

inline CsvHeader::iterator CsvHeader::iterator::operator+(
    difference_type n) const {
  return iterator(*this) += n;
}

inline CsvHeader::iterator& CsvHeader::iterator::operator-=(difference_type n) {
  iter_ -= n;
  return *this;
}

inline CsvHeader::iterator CsvHeader::iterator::operator-(
    difference_type n) const {
  return iterator(*this) -= n;
}

inline typename CsvHeader::iterator::reference CsvHeader::iterator::operator[](
    difference_type n) const {
  return *(*this + n);
}

template <
    typename Names,
    std::enable_if_t<std::conjunction_v<NotSameRef<CsvHeader, Names>,
                                        IsIterableOf<Names, absl::string_view>>,
                     int>>
CsvHeader::CsvHeader(Names&& names) {
  const absl::Status status =
      TryResetInternal(nullptr, std::forward<Names>(names));
  RIEGELI_CHECK_OK(status) << "Failed precondition of CsvHeader::CsvHeader()";
}

template <typename Names,
          std::enable_if_t<IsIterableOf<Names, absl::string_view>::value, int>>
CsvHeader::CsvHeader(std::function<std::string(absl::string_view)> normalizer,
                     Names&& names) {
  const absl::Status status =
      TryResetInternal(std::move(normalizer), std::forward<Names>(names));
  RIEGELI_CHECK_OK(status) << "Failed precondition of CsvHeader::CsvHeader()";
}

template <
    typename Names,
    std::enable_if_t<std::conjunction_v<NotSameRef<CsvHeader, Names>,
                                        IsIterableOf<Names, absl::string_view>>,
                     int>>
void CsvHeader::Reset(Names&& names) {
  const absl::Status status =
      TryResetInternal(nullptr, std::forward<Names>(names));
  RIEGELI_CHECK_OK(status) << "Failed precondition of CsvHeader::Reset()";
}

template <typename Names,
          std::enable_if_t<IsIterableOf<Names, absl::string_view>::value, int>>
void CsvHeader::Reset(std::function<std::string(absl::string_view)> normalizer,
                      Names&& names) {
  const absl::Status status =
      TryResetInternal(std::move(normalizer), std::forward<Names>(names));
  RIEGELI_CHECK_OK(status) << "Failed precondition of CsvHeader::Reset()";
}

template <
    typename Names,
    std::enable_if_t<std::conjunction_v<NotSameRef<CsvHeader, Names>,
                                        IsIterableOf<Names, absl::string_view>>,
                     int>>
absl::Status CsvHeader::TryReset(Names&& names) {
  return TryResetInternal(nullptr, std::forward<Names>(names));
}

template <typename Names,
          std::enable_if_t<IsIterableOf<Names, absl::string_view>::value, int>>
absl::Status CsvHeader::TryReset(
    std::function<std::string(absl::string_view)> normalizer, Names&& names) {
  return TryResetInternal(std::move(normalizer), std::forward<Names>(names));
}

template <typename Names,
          std::enable_if_t<IsRandomAccessIterable<Names>::value, int>>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline absl::Status CsvHeader::TryResetInternal(
    std::function<std::string(absl::string_view)>&& normalizer, Names&& names) {
  // Iterable supports random access, which allows `std::equal()` in
  // `MaybeResetToCachedPayload()` to compare the size first, which makes it
  // efficient to call `MaybeResetToCachedPayload()` before converting `names`
  // to `std::vector<std::string>`.
  if (normalizer == nullptr && MaybeResetToCachedPayload(names)) {
    return absl::OkStatus();
  }
  return TryResetUncached(
      std::move(normalizer),
      csv_internal::ToStringVector(std::forward<Names>(names)));
}

template <typename Names,
          std::enable_if_t<!IsRandomAccessIterable<Names>::value, int>>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline absl::Status CsvHeader::TryResetInternal(
    std::function<std::string(absl::string_view)>&& normalizer, Names&& names) {
  return TryResetInternal(
      std::move(normalizer),
      csv_internal::ToStringVector(std::forward<Names>(names)));
}

ABSL_ATTRIBUTE_ALWAYS_INLINE
inline absl::Status CsvHeader::TryResetInternal(
    std::function<std::string(absl::string_view)>&& normalizer,
    std::vector<std::string>&& names) {
  if (normalizer == nullptr && MaybeResetToCachedPayload(names)) {
    return absl::OkStatus();
  }
  return TryResetUncached(std::move(normalizer), std::move(names));
}

template <typename Names>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool CsvHeader::MaybeResetToCachedPayload(
    Names&& names) {
  using std::begin;
  auto names_iter = begin(names);
  using std::end;
  auto names_end_iter = end(names);
  if (names_iter == names_end_iter) {
    payload_.Reset();
    return true;
  }
  SharedPtr<Payload> payload;
  {
    absl::MutexLock lock(&payload_cache_mutex_);
    payload = payload_cache_;
  }
  if (payload != nullptr &&
      std::equal(payload->index_to_name.begin(), payload->index_to_name.end(),
                 names_iter, names_end_iter)) {
    payload_ = std::move(payload);
    return true;
  }
  return false;
}

template <typename... Names, std::enable_if_t<(sizeof...(Names) > 0), int>>
inline void CsvHeader::Add(StringInitializer name, Names&&... names) {
  Add(std::move(name));
  Add(std::forward<Names>(names)...);
}

template <typename... Names, std::enable_if_t<(sizeof...(Names) > 0), int>>
inline absl::Status CsvHeader::TryAdd(StringInitializer name,
                                      Names&&... names) {
  if (absl::Status status = TryAdd(std::move(name)); !status.ok()) {
    return status;
  }
  return TryAdd(std::forward<Names>(names)...);
}

inline absl::Span<const std::string> CsvHeader::names() const
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  if (ABSL_PREDICT_FALSE(payload_ == nullptr)) return {};
  return payload_->index_to_name;
}

inline const std::function<std::string(absl::string_view)>&
CsvHeader::normalizer() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
  if (ABSL_PREDICT_FALSE(payload_ == nullptr)) {
    return Global<std::function<std::string(absl::string_view)>>();
  }
  return payload_->normalizer;
}

inline CsvHeader::iterator CsvHeader::begin() const
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  if (ABSL_PREDICT_FALSE(payload_ == nullptr)) return iterator();
  return iterator(payload_->index_to_name.cbegin());
}

inline CsvHeader::iterator CsvHeader::end() const
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  if (ABSL_PREDICT_FALSE(payload_ == nullptr)) return iterator();
  return iterator(payload_->index_to_name.cend());
}

inline bool CsvHeader::empty() const {
  return payload_ == nullptr || payload_->index_to_name.empty();
}

inline size_t CsvHeader::size() const {
  if (ABSL_PREDICT_FALSE(payload_ == nullptr)) return 0;
  return payload_->index_to_name.size();
}

template <size_t num_fields>
inline const CsvHeader* CsvHeaderConstant<num_fields>::get() const
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  absl::call_once(once_,
                  [&] { new (header_) CsvHeader(normalizer_, fields_); });
  return std::launder(reinterpret_cast<const CsvHeader*>(header_));
}

template <typename FieldIterator>
inline CsvRecord::IteratorImpl<FieldIterator>::IteratorImpl(
    CsvHeader::iterator name_iter, FieldIterator field_iter)
    : name_iter_(name_iter), field_iter_(field_iter) {}

template <typename FieldIterator>
template <typename ThatFieldIterator,
          std::enable_if_t<
              std::conjunction_v<
                  std::negation<std::is_same<ThatFieldIterator, FieldIterator>>,
                  std::is_convertible<ThatFieldIterator, FieldIterator>>,
              int>>
inline CsvRecord::IteratorImpl<FieldIterator>::IteratorImpl(
    IteratorImpl<ThatFieldIterator> that) noexcept
    : name_iter_(that.name_iter_), field_iter_(that.field_iter_) {}

template <typename FieldIterator>
inline typename CsvRecord::IteratorImpl<FieldIterator>::reference
CsvRecord::IteratorImpl<FieldIterator>::operator*() const {
  return reference(*name_iter_, *field_iter_);
}

template <typename FieldIterator>
inline typename CsvRecord::IteratorImpl<FieldIterator>::pointer
CsvRecord::IteratorImpl<FieldIterator>::operator->() const {
  return pointer(**this);
}

template <typename FieldIterator>
inline CsvRecord::IteratorImpl<FieldIterator>&
CsvRecord::IteratorImpl<FieldIterator>::operator++() {
  ++name_iter_;
  ++field_iter_;
  return *this;
}

template <typename FieldIterator>
inline CsvRecord::IteratorImpl<FieldIterator>
CsvRecord::IteratorImpl<FieldIterator>::operator++(int) {
  const IteratorImpl<FieldIterator> tmp = *this;
  ++*this;
  return tmp;
}

template <typename FieldIterator>
inline CsvRecord::IteratorImpl<FieldIterator>&
CsvRecord::IteratorImpl<FieldIterator>::operator--() {
  --name_iter_;
  --field_iter_;
  return *this;
}

template <typename FieldIterator>
inline CsvRecord::IteratorImpl<FieldIterator>
CsvRecord::IteratorImpl<FieldIterator>::operator--(int) {
  const IteratorImpl<FieldIterator> tmp = *this;
  --*this;
  return tmp;
}

template <typename FieldIterator>
inline CsvRecord::IteratorImpl<FieldIterator>&
CsvRecord::IteratorImpl<FieldIterator>::operator+=(difference_type n) {
  name_iter_ += n;
  field_iter_ += n;
  return *this;
}

template <typename FieldIterator>
inline CsvRecord::IteratorImpl<FieldIterator>
CsvRecord::IteratorImpl<FieldIterator>::operator+(difference_type n) const {
  return IteratorImpl<FieldIterator>(*this) += n;
}

template <typename FieldIterator>
inline CsvRecord::IteratorImpl<FieldIterator>&
CsvRecord::IteratorImpl<FieldIterator>::operator-=(difference_type n) {
  name_iter_ -= n;
  field_iter_ -= n;
  return *this;
}

template <typename FieldIterator>
inline CsvRecord::IteratorImpl<FieldIterator>
CsvRecord::IteratorImpl<FieldIterator>::operator-(difference_type n) const {
  return IteratorImpl<FieldIterator>(*this) -= n;
}

template <typename FieldIterator>
inline typename CsvRecord::IteratorImpl<FieldIterator>::reference
CsvRecord::IteratorImpl<FieldIterator>::operator[](difference_type n) const {
  return *(*this + n);
}

inline CsvRecord::CsvRecord(CsvHeader header)
    : header_(std::move(header)), fields_(header_.size()) {}

template <typename Fields,
          std::enable_if_t<IsIterableOf<Fields, absl::string_view>::value, int>>
CsvRecord::CsvRecord(CsvHeader header, Fields&& fields) {
  const absl::Status status =
      TryResetInternal(std::move(header), std::forward<Fields>(fields));
  RIEGELI_CHECK_OK(status) << "Failed precondition of CsvRecord::CsvRecord()";
}

template <typename Fields,
          std::enable_if_t<IsIterableOf<Fields, absl::string_view>::value, int>>
void CsvRecord::Reset(CsvHeader header, Fields&& fields) {
  const absl::Status status =
      TryResetInternal(std::move(header), std::forward<Fields>(fields));
  RIEGELI_CHECK_OK(status) << "Failed precondition of CsvRecord::Reset()";
}

template <typename Fields,
          std::enable_if_t<IsIterableOf<Fields, absl::string_view>::value, int>>
absl::Status CsvRecord::TryReset(CsvHeader header, Fields&& fields) {
  return TryResetInternal(std::move(header), std::forward<Fields>(fields));
}

template <typename Fields>
inline absl::Status CsvRecord::TryResetInternal(CsvHeader&& header,
                                                Fields&& fields) {
  return TryResetInternal(std::move(header), csv_internal::ToStringVector(
                                                 std::forward<Fields>(fields)));
}

inline CsvRecord::iterator CsvRecord::begin() ABSL_ATTRIBUTE_LIFETIME_BOUND {
  return iterator(header_.begin(), fields_.begin());
}

inline CsvRecord::const_iterator CsvRecord::begin() const
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  return const_iterator(header_.begin(), fields_.begin());
}

inline CsvRecord::iterator CsvRecord::end() ABSL_ATTRIBUTE_LIFETIME_BOUND {
  return iterator(header_.end(), fields_.end());
}

inline CsvRecord::const_iterator CsvRecord::end() const
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  return const_iterator(header_.end(), fields_.end());
}

template <
    typename Src,
    std::enable_if_t<IsIterableOf<Src, std::pair<absl::string_view,
                                                 absl::string_view>>::value,
                     int>>
void CsvRecord::Merge(Src&& src) {
  const absl::Status status = TryMerge(std::forward<Src>(src));
  RIEGELI_CHECK_OK(status) << "Failed precondition of CsvHeader::Merge()";
}

template <
    typename Src,
    std::enable_if_t<IsIterableOf<Src, std::pair<absl::string_view,
                                                 absl::string_view>>::value,
                     int>>
absl::Status CsvRecord::TryMerge(Src&& src) {
  using std::begin;
  auto src_iter = begin(src);
  using std::end;
  auto src_end_iter = end(src);
  iterator this_iter = this->begin();
  // If fields of `src` match a prefix of fields of `*this` (like when extending
  // a `CsvRecord` with fields added at the end), avoid string lookups and just
  // verify the assumption.
  for (;;) {
    if (src_iter == src_end_iter) return absl::OkStatus();
    if (this_iter == this->end() || this_iter->first != src_iter->first) break;
    riegeli::Reset(
        this_iter->second,
        StringInitializer((*MaybeMakeMoveIterator<Src>(src_iter)).second));
    ++this_iter;
    ++src_iter;
  }
  RIEGELI_ASSERT(src_iter != src_end_iter)
      << "The code below assumes that the code above "
         "did not leave the source iterator at the end";
  // The assumption about matching fields no longer holds. Switch to string
  // lookups for the remaining fields.
  std::vector<std::string> missing_names;
  do {
    this_iter = find(src_iter->first);
    if (ABSL_PREDICT_FALSE(this_iter == this->end())) {
      missing_names.emplace_back(src_iter->first);
    } else {
      riegeli::Reset(
          this_iter->second,
          StringInitializer((*MaybeMakeMoveIterator<Src>(src_iter)).second));
    }
    ++src_iter;
  } while (src_iter != src_end_iter);
  if (ABSL_PREDICT_FALSE(!missing_names.empty())) {
    return FailMissingNames(missing_names);
  }
  return absl::OkStatus();
}

template <typename Dest,
          std::enable_if_t<IsIterableOfPairsWithAssignableValues<
                               Dest, absl::string_view, std::string>::value,
                           int>>
void CsvRecord::Split(Dest& dest) const {
  const absl::Status status = TrySplit(dest);
  RIEGELI_CHECK_OK(status) << "Failed precondition of CsvHeader::Split()";
}

template <typename Dest,
          std::enable_if_t<IsIterableOfPairsWithAssignableValues<
                               Dest, absl::string_view, std::string>::value,
                           int>>
absl::Status CsvRecord::TrySplit(Dest& dest) const {
  using std::begin;
  auto dest_iter = begin(dest);
  using std::end;
  auto dest_end_iter = end(dest);
  const_iterator this_iter = this->begin();
  // If fields of `dest` match a prefix of fields of `*this` (like when
  // projecting a `CsvRecord` to its prefix), avoid string lookups and just
  // verify the assumption.
  for (;;) {
    if (dest_iter == dest_end_iter) return absl::OkStatus();
    if (this_iter == this->end() || this_iter->first != dest_iter->first) break;
    dest_iter->second = this_iter->second;
    ++this_iter;
    ++dest_iter;
  }
  RIEGELI_ASSERT(dest_iter != dest_end_iter)
      << "The code below assumes that the code above "
         "did not leave the destination iterator at the end";
  // The assumption about matching fields no longer holds. Switch to string
  // lookups for the remaining fields.
  std::vector<std::string> missing_names;
  do {
    this_iter = find(dest_iter->first);
    if (ABSL_PREDICT_FALSE(this_iter == this->end())) {
      missing_names.emplace_back(dest_iter->first);
    } else {
      dest_iter->second = this_iter->second;
    }
    ++dest_iter;
  } while (dest_iter != dest_end_iter);
  if (ABSL_PREDICT_FALSE(!missing_names.empty())) {
    return FailMissingNames(missing_names);
  }
  return absl::OkStatus();
}

}  // namespace riegeli

#endif  // RIEGELI_CSV_CSV_RECORD_H_
