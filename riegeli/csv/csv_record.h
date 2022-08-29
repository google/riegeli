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
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/base/call_once.h"
#include "absl/base/optimization.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/meta/type_traits.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "riegeli/base/base.h"
#include "riegeli/base/intrusive_ref_count.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/csv/containers.h"

namespace riegeli {
namespace csv_internal {

// A pair-like type which supports C++20 `std::common_reference` with similar
// enough pairs. This is needed for `CsvRecord::{,const_}iterator` to satisfy
// C++20 input iterator requirements.
//
// Since C++23, `std::pair<T1, T2>` can be used directly instead, because it has
// the necessary conversions and `std::basic_common_reference` specializations.
template <typename T1, typename T2>
class ReferencePair : public std::pair<T1, T2> {
 public:
  using std::pair<T1, T2>::pair;

  template <class U1, class U2,
            std::enable_if_t<std::is_constructible<T1, U1&>::value &&
                                 std::is_constructible<T2, U2&>::value &&
                                 !(std::is_convertible<U1&, T1>::value &&
                                   std::is_convertible<U2&, T2>::value),
                             int> = 0>
  explicit constexpr ReferencePair(std::pair<U1, U2>& p)
      : std::pair<T1, T2>(p.first, p.second) {}

  template <class U1, class U2,
            std::enable_if_t<std::is_convertible<U1&, T1>::value &&
                                 std::is_convertible<U2&, T2>::value,
                             int> = 0>
  /*implicit*/ constexpr ReferencePair(std::pair<U1, U2>& p)
      : std::pair<T1, T2>(p.first, p.second) {}
};

}  // namespace csv_internal
}  // namespace riegeli

#if __cplusplus >= 202002L

namespace std {

template <typename T1, typename T2, template <typename> class TQual,
          template <typename> class UQual>
struct basic_common_reference<riegeli::csv_internal::ReferencePair<T1, T2>,
                              std::pair<std::string, std::string>, TQual,
                              UQual> {
  using type = riegeli::csv_internal::ReferencePair<
      std::common_reference_t<TQual<T1>, UQual<std::string>>,
      std::common_reference_t<TQual<T2>, UQual<std::string>>>;
};

template <typename T1, typename T2, template <typename> class TQual,
          template <typename> class UQual>
struct basic_common_reference<std::pair<std::string, std::string>,
                              riegeli::csv_internal::ReferencePair<T1, T2>,
                              TQual, UQual> {
  using type = riegeli::csv_internal::ReferencePair<
      std::common_reference_t<TQual<std::string>, UQual<T1>>,
      std::common_reference_t<TQual<std::string>, UQual<T2>>>;
};

template <typename T1, typename T2, typename U1, typename U2,
          template <typename> class TQual, template <typename> class UQual>
struct basic_common_reference<riegeli::csv_internal::ReferencePair<T1, T2>,
                              riegeli::csv_internal::ReferencePair<U1, U2>,
                              TQual, UQual> {
  using type = riegeli::csv_internal::ReferencePair<
      std::common_reference_t<TQual<T1>, UQual<U1>>,
      std::common_reference_t<TQual<T2>, UQual<U2>>>;
};

}  // namespace std

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
class CsvHeader {
 public:
  class iterator {
   public:
    using iterator_category = std::random_access_iterator_tag;
    using value_type = std::string;
    using reference = const std::string&;
    using pointer = const std::string*;
    using difference_type = ptrdiff_t;

    iterator() noexcept {}

    iterator(const iterator& that) noexcept = default;
    iterator& operator=(const iterator& that) noexcept = default;

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
    friend bool operator!=(iterator a, iterator b) {
      return a.iter_ != b.iter_;
    }
    friend bool operator<(iterator a, iterator b) { return a.iter_ < b.iter_; }
    friend bool operator>(iterator a, iterator b) { return a.iter_ > b.iter_; }
    friend bool operator<=(iterator a, iterator b) {
      return a.iter_ <= b.iter_;
    }
    friend bool operator>=(iterator a, iterator b) {
      return a.iter_ >= b.iter_;
    }
    friend difference_type operator-(iterator a, iterator b) {
      return a.iter_ - b.iter_;
    }
    friend iterator operator+(difference_type n, iterator a) { return a + n; }

   private:
    friend class CsvHeader;

    explicit iterator(const std::string* iter) : iter_(iter) {}

    // This is `const std::string*` and not
    // `std::vector<std::string>::const_iterator` to make it easier to return
    // iterators over an empty range when `payload_ == nullptr`.
    const std::string* iter_ = nullptr;
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
  CsvHeader() noexcept {}

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
                csv_internal::IsIterableOf<Names, absl::string_view>::value &&
                    !std::is_same<std::decay_t<Names>, CsvHeader>::value,
                int> = 0>
  explicit CsvHeader(Names&& names);
  /*implicit*/ CsvHeader(std::initializer_list<absl::string_view> names);

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
      std::enable_if_t<
          csv_internal::IsIterableOf<Names, absl::string_view>::value, int> = 0>
  explicit CsvHeader(std::function<std::string(absl::string_view)> normalizer,
                     Names&& names);
  explicit CsvHeader(std::function<std::string(absl::string_view)> normalizer,
                     std::initializer_list<absl::string_view> names);

  CsvHeader(const CsvHeader& that) noexcept = default;
  CsvHeader& operator=(const CsvHeader& that) noexcept = default;

  // The source `CsvHeader` is left empty.
  CsvHeader(CsvHeader&& that) noexcept = default;
  CsvHeader& operator=(CsvHeader&& that) noexcept = default;

  // Makes `*this` equivalent to a newly constructed `CsvHeader`.
  //
  // Precondition: like for the corresponding constructor
  void Reset();
  template <typename Names,
            std::enable_if_t<
                csv_internal::IsIterableOf<Names, absl::string_view>::value &&
                    !std::is_same<std::decay_t<Names>, CsvHeader>::value,
                int> = 0>
  void Reset(Names&& names);
  void Reset(std::initializer_list<absl::string_view> names);
  void Reset(std::function<std::string(absl::string_view)> normalizer);
  template <
      typename Names,
      std::enable_if_t<
          csv_internal::IsIterableOf<Names, absl::string_view>::value, int> = 0>
  void Reset(std::function<std::string(absl::string_view)> normalizer,
             Names&& names);
  void Reset(std::function<std::string(absl::string_view)> normalizer,
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
                csv_internal::IsIterableOf<Names, absl::string_view>::value &&
                    !std::is_same<std::decay_t<Names>, CsvHeader>::value,
                int> = 0>
  absl::Status TryReset(Names&& names);
  absl::Status TryReset(std::initializer_list<absl::string_view> names);
  template <
      typename Names,
      std::enable_if_t<
          csv_internal::IsIterableOf<Names, absl::string_view>::value, int> = 0>
  absl::Status TryReset(
      std::function<std::string(absl::string_view)> normalizer, Names&& names);
  absl::Status TryReset(
      std::function<std::string(absl::string_view)> normalizer,
      std::initializer_list<absl::string_view> names);

  // Adds the given field `name`, ordered at the end.
  //
  // Precondition: `name` was not already present
  //
  // `std::string&&` is accepted with a template to avoid implicit conversions
  // to `std::string` which can be ambiguous against `absl::string_view`
  // (e.g. `const char*`).
  void Add(absl::string_view name);
  template <typename Name,
            std::enable_if_t<std::is_same<Name, std::string>::value, int> = 0>
  void Add(Name&& name);

  // Equivalent to calling `Add()` for each name in order.
  //
  // Precondition: like for `Add()`
  template <typename... Names,
            std::enable_if_t<(sizeof...(Names) > 0), int> = 0>
  void Add(absl::string_view name, Names&&... names);
  template <typename Name, typename... Names,
            std::enable_if_t<std::is_same<Name, std::string>::value &&
                                 (sizeof...(Names) > 0),
                             int> = 0>
  void Add(Name&& name, Names&&... names);

  // Adds the given field `name`, ordered at the end, reporting whether this was
  // successful.
  //
  // Return values:
  //  * `absl::OkStatus()`                 - `name` has been added
  //  * `absl::FailedPreconditionError(_)` - `name` was already present,
  //                                         `CsvHeader` is unchanged
  //
  // `std::string&&` is accepted with a template to avoid implicit conversions
  // to `std::string` which can be ambiguous against `absl::string_view`
  // (e.g. `const char*`).
  absl::Status TryAdd(absl::string_view name);
  template <typename Name,
            std::enable_if_t<std::is_same<Name, std::string>::value, int> = 0>
  absl::Status TryAdd(Name&& name);

  // Equivalent to calling `TryAdd()` for each name in order.
  //
  // Returns early in case of a failure.
  template <typename... Names,
            std::enable_if_t<(sizeof...(Names) > 0), int> = 0>
  absl::Status TryAdd(absl::string_view name, Names&&... names);
  template <typename Name, typename... Names,
            std::enable_if_t<std::is_same<Name, std::string>::value &&
                                 (sizeof...(Names) > 0),
                             int> = 0>
  absl::Status TryAdd(Name&& name, Names&&... names);

  // Returns the sequence of field names, in the order in which they have been
  // added.
  absl::Span<const std::string> names() const;

  // Returns the normalizer used to match field names, or `nullptr` which is the
  // same as the identity function.
  std::function<std::string(absl::string_view)> normalizer() const;

  // Iterates over field names, in the order in which they have been added.
  iterator begin() const;
  iterator cbegin() const { return begin(); }
  iterator end() const;
  iterator cend() const { return end(); }

  // Iterates over field names, backwards.
  reverse_iterator rbegin() const { return reverse_iterator(end()); }
  reverse_iterator crbegin() const { return rbegin(); }
  reverse_iterator rend() const { return reverse_iterator(begin()); }
  reverse_iterator crend() const { return rend(); }

  // Returns `true` if there are no field names.
  bool empty() const;

  // Returns the number of field names.
  size_t size() const;

  // Returns an iterator positioned at `name`, or `end()` if `name` is not
  // present.
  iterator find(absl::string_view name) const;

  // Returns `true` if `name` is present.
  bool contains(absl::string_view name) const;

  // Returns the position of `name` in the sequence of field names, or
  // `absl::nullopt` if `name` is not present.
  //
  // This can be used together with `CsvRecord::fields()` to look up the same
  // field in multiple `CsvRecord`s sharing a `CsvHeader`.
  absl::optional<size_t> IndexOf(absl::string_view name) const;

  // Compares the sequence of field names. Does not compare the normalizer.
  friend bool operator==(const CsvHeader& a, const CsvHeader& b);
  friend bool operator!=(const CsvHeader& a, const CsvHeader& b);

  // Renders contents in a human-readable way.
  std::string DebugString() const;

  // Same as: `out << header.DebugString()`
  friend std::ostream& operator<<(std::ostream& out, const CsvHeader& header);

 private:
  struct Payload : RefCountedBase<Payload> {
    Payload() noexcept {}
    Payload(std::function<std::string(absl::string_view)>&& normalizer)
        : normalizer(std::move(normalizer)) {}
    Payload(const Payload& that);

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
            std::enable_if_t<csv_internal::IsRandomAccessIterable<Names>::value,
                             int> = 0>
  absl::Status TryResetInternal(
      std::function<std::string(absl::string_view)>&& normalizer,
      Names&& names);
  template <typename Names,
            std::enable_if_t<
                !csv_internal::IsRandomAccessIterable<Names>::value, int> = 0>
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

  void EnsureUniqueOwner();

  // A one-element cache of a recently constructed `Payload`, to reuse the
  // `Payload` when multiple `CsvHeader` objects are created from the same
  // iterable of field names. Its `normalizer` is always `nullptr` and its
  // `index_to_name` is never empty.
  //
  // Reusing `CsvHeader` directly is more efficient but not always feasible.
  ABSL_CONST_INIT static absl::Mutex payload_cache_mutex_;
  ABSL_CONST_INIT static RefCountedPtr<Payload> payload_cache_
      ABSL_GUARDED_BY(payload_cache_mutex_);

  RefCountedPtr<Payload> payload_;
};

// `CsvHeaderConstant<n>` lazily constructs and stores a `CsvHeader` with `n`
// fields, and never calls its destructor.
//
// It should be used as the type of a variable with static storage duration.
//
// By relying on CTAD the template argument can be deduced as the number of
// constructor arguments. This requires C++17.
template <size_t num_fields>
class CsvHeaderConstant {
 public:
  // Will create a `CsvHeader` consisting of the given sequence of field names.
  //
  // The number of `fields` must be `num_fields`, and all `fields` must have
  // static storage duration.
  template <typename... Fields,
            std::enable_if_t<sizeof...(Fields) == num_fields &&
                                 absl::conjunction<std::is_convertible<
                                     Fields, absl::string_view>...>::value,
                             int> = 0>
  /*implicit*/ constexpr CsvHeaderConstant(Fields&&... fields)
      : fields_{std::forward<Fields>(fields)...} {}

  // Will create a `CsvHeader` consisting of the given sequence of field names.
  //
  // The number of `fields` must be `num_fields`, and all `fields` must have
  // static storage duration.
  //
  // Field names are matched by passing them through `normalizer` first.
  // `nullptr` is the same as the identity function.
  template <typename... Fields,
            std::enable_if_t<sizeof...(Fields) == num_fields &&
                                 absl::conjunction<std::is_convertible<
                                     Fields, absl::string_view>...>::value,
                             int> = 0>
  explicit constexpr CsvHeaderConstant(
      std::string (*normalizer)(absl::string_view), Fields&&... fields)
      : normalizer_(normalizer), fields_{std::forward<Fields>(fields)...} {}

  CsvHeaderConstant(const CsvHeaderConstant&) = delete;
  CsvHeaderConstant& operator=(const CsvHeaderConstant&) = delete;

  const CsvHeader* get() const;
  const CsvHeader& operator*() const { return *get(); }
  const CsvHeader* operator->() const { return get(); }

 private:
  std::string (*const normalizer_)(absl::string_view) = nullptr;
  const absl::string_view fields_[num_fields];
  mutable absl::once_flag once_;
  alignas(CsvHeader) mutable char header_[sizeof(CsvHeader)] = {};
};

// Support CTAD.
#if __cpp_deduction_guides
template <typename... Fields,
          std::enable_if_t<absl::conjunction<std::is_convertible<
                               Fields, absl::string_view>...>::value,
                           int> = 0>
/*implicit*/ CsvHeaderConstant(Fields&&... fields)
    ->CsvHeaderConstant<sizeof...(Fields)>;
template <typename... Fields,
          std::enable_if_t<absl::conjunction<std::is_convertible<
                               Fields, absl::string_view>...>::value,
                           int> = 0>
explicit CsvHeaderConstant(std::string (*normalizer)(absl::string_view),
                           Fields&&... fields)
    -> CsvHeaderConstant<sizeof...(Fields)>;
#endif

// A row of a CSV file, with fields accessed by name.
//
// This is conceptually a mapping from field names to field values, with a fixed
// set of field names. The set of field names is expressed as `CsvHeader`.
class CsvRecord {
 private:
  // Implementation shared between `iterator` and `const_iterator`.
  template <typename FieldIterator>
  class IteratorImpl {
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
      template <typename>
      friend class IteratorImpl;
      explicit pointer(reference ref) : ref_(ref) {}
      reference ref_;
    };

    IteratorImpl() noexcept {}

    // Conversion from `iterator` to `const_iterator`.
    template <
        typename ThatFieldIterator,
        std::enable_if_t<
            std::is_convertible<ThatFieldIterator, FieldIterator>::value &&
                !std::is_same<ThatFieldIterator, FieldIterator>::value,
            int> = 0>
    /*implicit*/ IteratorImpl(IteratorImpl<ThatFieldIterator> that) noexcept;

    IteratorImpl(const IteratorImpl& that) noexcept = default;
    IteratorImpl& operator=(const IteratorImpl& that) noexcept = default;

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
    friend bool operator!=(IteratorImpl a, IteratorImpl b) {
      return a.field_iter_ != b.field_iter_;
    }
    friend bool operator<(IteratorImpl a, IteratorImpl b) {
      return a.field_iter_ < b.field_iter_;
    }
    friend bool operator>(IteratorImpl a, IteratorImpl b) {
      return a.field_iter_ > b.field_iter_;
    }
    friend bool operator<=(IteratorImpl a, IteratorImpl b) {
      return a.field_iter_ <= b.field_iter_;
    }
    friend bool operator>=(IteratorImpl a, IteratorImpl b) {
      return a.field_iter_ >= b.field_iter_;
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
  CsvRecord() noexcept {}

  // Creates a `CsvRecord` with the given field names, and with all field values
  // empty.
  explicit CsvRecord(CsvHeader header);

  // Creates a `CsvRecord` with the given field names and field values in the
  // corresponding order.
  //
  // Precondition: `header.size() == fields.size()`
  template <typename Fields,
            std::enable_if_t<
                csv_internal::IsIterableOf<Fields, absl::string_view>::value,
                int> = 0>
  explicit CsvRecord(CsvHeader header, Fields&& fields);
  explicit CsvRecord(CsvHeader header,
                     std::initializer_list<absl::string_view> fields);

  CsvRecord(const CsvRecord& that);
  CsvRecord& operator=(const CsvRecord& that);

  // The source `CsvRecord` is left empty.
  CsvRecord(CsvRecord&& that) noexcept;
  CsvRecord& operator=(CsvRecord&& that) noexcept;

  // Returns the set of field names.
  const CsvHeader& header() const { return header_; }

  // Makes `*this` equivalent to a newly constructed `CsvRecord`.
  //
  // Precondition: like for the corresponding constructor
  void Reset();
  void Reset(CsvHeader header);
  template <typename Fields,
            std::enable_if_t<
                csv_internal::IsIterableOf<Fields, absl::string_view>::value,
                int> = 0>
  void Reset(CsvHeader header, Fields&& fields);
  void Reset(CsvHeader header, std::initializer_list<absl::string_view> fields);

  // Makes `*this` equivalent to a newly constructed `CsvRecord`, reporting
  // whether construction was successful.
  //
  // Return values:
  //  * `absl::OkStatus()`                 - `CsvRecord` is set to `header`
  //                                         and `fields`
  //  * `absl::FailedPreconditionError(_)` - lengths of `header` and `fields`
  //                                         do not match, `CsvRecord` is empty
  template <typename Fields,
            std::enable_if_t<
                csv_internal::IsIterableOf<Fields, absl::string_view>::value,
                int> = 0>
  absl::Status TryReset(CsvHeader header, Fields&& fields);
  absl::Status TryReset(CsvHeader header,
                        std::initializer_list<absl::string_view> fields);

  // Makes all field values empty. The number of fields is unchanged.
  void Clear();

  // Returns the sequence of field values, in the order corresponding to the
  // order of field names in the header.
  absl::Span<std::string> fields() { return absl::MakeSpan(fields_); }
  absl::Span<const std::string> fields() const { return fields_; }

  // Iterates over pairs of field names and field values, in the order
  // corresponding to the order of field names in the header.
  iterator begin();
  const_iterator begin() const;
  const_iterator cbegin() const { return begin(); }
  iterator end();
  const_iterator end() const;
  const_iterator cend() const { return end(); }

  // Iterates over pairs of field names and field values, backwards.
  reverse_iterator rbegin() { return reverse_iterator(end()); }
  const_reverse_iterator rbegin() const {
    return const_reverse_iterator(end());
  }
  const_reverse_iterator crbegin() const { return rbegin(); }
  reverse_iterator rend() { return reverse_iterator(begin()); }
  const_reverse_iterator rend() const {
    return const_reverse_iterator(begin());
  }
  const_reverse_iterator crend() const { return rend(); }

  // Returns `true` if there are no fields.
  bool empty() const { return fields_.empty(); }

  // Returns the number of field names, which is the same as the number of field
  // values.
  size_t size() const { return fields_.size(); }

  // Returns a reference to the field value corresponding to the given field
  // `name`.
  //
  // Precondition: `name` is present
  std::string& operator[](absl::string_view name);
  const std::string& operator[](absl::string_view name) const;

  // Returns an iterator positioned at the pair of the given field `name` and
  // the corresponding field value, or `end()` if `name` is not present.
  iterator find(absl::string_view name);
  const_iterator find(absl::string_view name) const;

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
      std::enable_if_t<
          csv_internal::IsIterableOf<
              Src, std::pair<absl::string_view, absl::string_view>>::value,
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
      std::enable_if_t<
          csv_internal::IsIterableOf<
              Src, std::pair<absl::string_view, absl::string_view>>::value,
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
  template <
      typename Dest,
      std::enable_if_t<csv_internal::IsIterableOfPairsWithAssignableValues<
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
  template <
      typename Dest,
      std::enable_if_t<csv_internal::IsIterableOfPairsWithAssignableValues<
                           Dest, absl::string_view, std::string>::value,
                       int> = 0>
  absl::Status TrySplit(Dest& dest) const;

  friend bool operator==(const CsvRecord& a, const CsvRecord& b);
  friend bool operator!=(const CsvRecord& a, const CsvRecord& b);

  // Renders contents in a human-readable way.
  std::string DebugString() const;

  // Same as: `out << header.DebugString()`
  friend std::ostream& operator<<(std::ostream& out, const CsvRecord& record);

 private:
  friend class CsvReaderBase;

  template <typename Fields>
  absl::Status TryResetInternal(CsvHeader&& header, Fields&& fields);
  absl::Status TryResetInternal(CsvHeader&& header,
                                std::vector<std::string>&& fields);

  absl::Status FailMissingNames(
      absl::Span<const std::string> missing_names) const;

  // Invariant: `header_.size() == fields_.size()`
  CsvHeader header_;
  std::vector<std::string> fields_;
};

namespace csv_internal {
void WriteDebugQuotedIfNeeded(absl::string_view src, Writer& writer);
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

template <typename Names,
          std::enable_if_t<
              csv_internal::IsIterableOf<Names, absl::string_view>::value &&
                  !std::is_same<std::decay_t<Names>, CsvHeader>::value,
              int>>
CsvHeader::CsvHeader(Names&& names) {
  const absl::Status status =
      TryResetInternal(nullptr, std::forward<Names>(names));
  RIEGELI_CHECK(status.ok())
      << "Failed precondition of CsvHeader::CsvHeader(): " << status.message();
}

template <typename Names,
          std::enable_if_t<
              csv_internal::IsIterableOf<Names, absl::string_view>::value, int>>
CsvHeader::CsvHeader(std::function<std::string(absl::string_view)> normalizer,
                     Names&& names) {
  const absl::Status status =
      TryResetInternal(std::move(normalizer), std::forward<Names>(names));
  RIEGELI_CHECK(status.ok())
      << "Failed precondition of CsvHeader::CsvHeader(): " << status.message();
}

template <typename Names,
          std::enable_if_t<
              csv_internal::IsIterableOf<Names, absl::string_view>::value &&
                  !std::is_same<std::decay_t<Names>, CsvHeader>::value,
              int>>
void CsvHeader::Reset(Names&& names) {
  const absl::Status status =
      TryResetInternal(nullptr, std::forward<Names>(names));
  RIEGELI_CHECK(status.ok())
      << "Failed precondition of CsvHeader::Reset(): " << status.message();
}

template <typename Names,
          std::enable_if_t<
              csv_internal::IsIterableOf<Names, absl::string_view>::value, int>>
void CsvHeader::Reset(std::function<std::string(absl::string_view)> normalizer,
                      Names&& names) {
  const absl::Status status =
      TryResetInternal(std::move(normalizer), std::forward<Names>(names));
  RIEGELI_CHECK(status.ok())
      << "Failed precondition of CsvHeader::Reset(): " << status.message();
}

template <typename Names,
          std::enable_if_t<
              csv_internal::IsIterableOf<Names, absl::string_view>::value &&
                  !std::is_same<std::decay_t<Names>, CsvHeader>::value,
              int>>
absl::Status CsvHeader::TryReset(Names&& names) {
  return TryResetInternal(nullptr, std::forward<Names>(names));
}

template <typename Names,
          std::enable_if_t<
              csv_internal::IsIterableOf<Names, absl::string_view>::value, int>>
absl::Status CsvHeader::TryReset(
    std::function<std::string(absl::string_view)> normalizer, Names&& names) {
  return TryResetInternal(std::move(normalizer), std::forward<Names>(names));
}

template <
    typename Names,
    std::enable_if_t<csv_internal::IsRandomAccessIterable<Names>::value, int>>
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
      csv_internal::ToVectorOfStrings(std::forward<Names>(names)));
}

template <
    typename Names,
    std::enable_if_t<!csv_internal::IsRandomAccessIterable<Names>::value, int>>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline absl::Status CsvHeader::TryResetInternal(
    std::function<std::string(absl::string_view)>&& normalizer, Names&& names) {
  return TryResetInternal(
      std::move(normalizer),
      csv_internal::ToVectorOfStrings(std::forward<Names>(names)));
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
ABSL_ATTRIBUTE_ALWAYS_INLINE bool CsvHeader::MaybeResetToCachedPayload(
    Names&& names) {
  using std::begin;
  auto names_iter = begin(names);
  using std::end;
  auto names_end_iter = end(names);
  if (names_iter == names_end_iter) {
    payload_.reset();
    return true;
  }
  RefCountedPtr<Payload> payload;
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

extern template void CsvHeader::Add(std::string&& name);

template <typename... Names, std::enable_if_t<(sizeof...(Names) > 0), int>>
inline void CsvHeader::Add(absl::string_view name, Names&&... names) {
  Add(name);
  Add(std::forward<Names>(names)...);
}

template <
    typename Name, typename... Names,
    std::enable_if_t<
        std::is_same<Name, std::string>::value && (sizeof...(Names) > 0), int>>
inline void CsvHeader::Add(Name&& name, Names&&... names) {
  // `std::move(name)` is correct and `std::forward<Name>(name)` is not
  // necessary: `Name` is always `std::string`, never an lvalue reference.
  Add(std::move(name));
  Add(std::forward<Names>(names)...);
}

extern template absl::Status CsvHeader::TryAdd(std::string&& name);

template <typename... Names, std::enable_if_t<(sizeof...(Names) > 0), int>>
inline absl::Status CsvHeader::TryAdd(absl::string_view name,
                                      Names&&... names) {
  {
    absl::Status status = TryAdd(name);
    if (!status.ok()) return status;
  }
  return TryAdd(std::forward<Names>(names)...);
}

template <
    typename Name, typename... Names,
    std::enable_if_t<
        std::is_same<Name, std::string>::value && (sizeof...(Names) > 0), int>>
inline absl::Status CsvHeader::TryAdd(Name&& name, Names&&... names) {
  // `std::move(name)` is correct and `std::forward<Name>(name)` is not
  // necessary: `Name` is always `std::string`, never an lvalue reference.
  {
    absl::Status status = TryAdd(std::move(name));
    if (!status.ok()) {
      return status;
    }
  }
  return TryAdd(std::forward<Names>(names)...);
}

inline absl::Span<const std::string> CsvHeader::names() const {
  if (ABSL_PREDICT_FALSE(payload_ == nullptr)) return {};
  return payload_->index_to_name;
}

inline std::function<std::string(absl::string_view)> CsvHeader::normalizer()
    const {
  if (ABSL_PREDICT_FALSE(payload_ == nullptr)) return nullptr;
  return payload_->normalizer;
}

inline CsvHeader::iterator CsvHeader::begin() const {
  if (ABSL_PREDICT_FALSE(payload_ == nullptr)) return iterator();
  return iterator(payload_->index_to_name.data());
}

inline CsvHeader::iterator CsvHeader::end() const {
  if (ABSL_PREDICT_FALSE(payload_ == nullptr)) return iterator();
  return iterator(payload_->index_to_name.data() +
                  payload_->index_to_name.size());
}

inline bool CsvHeader::empty() const { return payload_ == nullptr; }

inline size_t CsvHeader::size() const {
  if (ABSL_PREDICT_FALSE(payload_ == nullptr)) return 0;
  return payload_->index_to_name.size();
}

inline bool operator==(const CsvHeader& a, const CsvHeader& b) {
  if (ABSL_PREDICT_TRUE(a.payload_ == b.payload_)) return true;
  if (a.payload_ == nullptr || b.payload_ == nullptr) return false;
  return a.payload_->index_to_name == b.payload_->index_to_name;
}

inline bool operator!=(const CsvHeader& a, const CsvHeader& b) {
  return !(a == b);
}

template <size_t num_fields>
inline const CsvHeader* CsvHeaderConstant<num_fields>::get() const {
  absl::call_once(once_,
                  [&] { new (header_) CsvHeader(normalizer_, fields_); });
  return
#if __cpp_lib_launder >= 201606
      std::launder
#endif
      (reinterpret_cast<const CsvHeader*>(header_));
}

template <typename FieldIterator>
inline CsvRecord::IteratorImpl<FieldIterator>::IteratorImpl(
    CsvHeader::iterator name_iter, FieldIterator field_iter)
    : name_iter_(name_iter), field_iter_(field_iter) {}

template <typename FieldIterator>
template <typename ThatFieldIterator,
          std::enable_if_t<
              std::is_convertible<ThatFieldIterator, FieldIterator>::value &&
                  !std::is_same<ThatFieldIterator, FieldIterator>::value,
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

template <
    typename Fields,
    std::enable_if_t<
        csv_internal::IsIterableOf<Fields, absl::string_view>::value, int>>
CsvRecord::CsvRecord(CsvHeader header, Fields&& fields) {
  const absl::Status status =
      TryResetInternal(std::move(header), std::forward<Fields>(fields));
  RIEGELI_CHECK(status.ok())
      << "Failed precondition of CsvRecord::CsvRecord(): " << status.message();
}

inline CsvRecord::CsvRecord(const CsvRecord& that)
    : header_(that.header_), fields_(that.fields_) {}

inline CsvRecord& CsvRecord::operator=(const CsvRecord& that) {
  header_ = that.header_;
  fields_ = that.fields_;
  return *this;
}

inline CsvRecord::CsvRecord(CsvRecord&& that) noexcept
    : header_(std::move(that.header_)),  // Leaves `that.header_` empty.
      fields_(std::move(that.fields_))   // Leaves `that.fields_` empty.
{}

inline CsvRecord& CsvRecord::operator=(CsvRecord&& that) noexcept {
  header_ = std::move(that.header_);  // Leaves `that.header_` empty.
  fields_ = std::move(that.fields_);  // Leaves `that.fields_` empty.
  return *this;
}

template <
    typename Fields,
    std::enable_if_t<
        csv_internal::IsIterableOf<Fields, absl::string_view>::value, int>>
void CsvRecord::Reset(CsvHeader header, Fields&& fields) {
  const absl::Status status =
      TryResetInternal(std::move(header), std::forward<Fields>(fields));
  RIEGELI_CHECK(status.ok())
      << "Failed precondition of CsvRecord::Reset(): " << status.message();
}

template <
    typename Fields,
    std::enable_if_t<
        csv_internal::IsIterableOf<Fields, absl::string_view>::value, int>>
absl::Status CsvRecord::TryReset(CsvHeader header, Fields&& fields) {
  return TryResetInternal(std::move(header), std::forward<Fields>(fields));
}

template <typename Fields>
inline absl::Status CsvRecord::TryResetInternal(CsvHeader&& header,
                                                Fields&& fields) {
  return TryResetInternal(std::move(header), csv_internal::ToVectorOfStrings(
                                                 std::forward<Fields>(fields)));
}

inline CsvRecord::iterator CsvRecord::begin() {
  return iterator(header_.begin(), fields_.begin());
}

inline CsvRecord::const_iterator CsvRecord::begin() const {
  return const_iterator(header_.begin(), fields_.begin());
}

inline CsvRecord::iterator CsvRecord::end() {
  return iterator(header_.end(), fields_.end());
}

inline CsvRecord::const_iterator CsvRecord::end() const {
  return const_iterator(header_.end(), fields_.end());
}

template <typename Src,
          std::enable_if_t<
              csv_internal::IsIterableOf<
                  Src, std::pair<absl::string_view, absl::string_view>>::value,
              int>>
void CsvRecord::Merge(Src&& src) {
  const absl::Status status = TryMerge(std::forward<Src>(src));
  RIEGELI_CHECK(status.ok())
      << "Failed precondition of CsvHeader::Merge(): " << status.message();
}

template <typename Src,
          std::enable_if_t<
              csv_internal::IsIterableOf<
                  Src, std::pair<absl::string_view, absl::string_view>>::value,
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
    csv_internal::AssignToString(
        csv_internal::MaybeMoveElement<Src>(src_iter->second),
        this_iter->second);
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
      csv_internal::AssignToString(
          csv_internal::MaybeMoveElement<Src>(src_iter->second),
          this_iter->second);
    }
    ++src_iter;
  } while (src_iter != src_end_iter);
  if (ABSL_PREDICT_FALSE(!missing_names.empty())) {
    return FailMissingNames(missing_names);
  }
  return absl::OkStatus();
}

template <typename Dest,
          std::enable_if_t<csv_internal::IsIterableOfPairsWithAssignableValues<
                               Dest, absl::string_view, std::string>::value,
                           int>>
void CsvRecord::Split(Dest& dest) const {
  const absl::Status status = TrySplit(dest);
  RIEGELI_CHECK(status.ok())
      << "Failed precondition of CsvHeader::Split(): " << status.message();
}

template <typename Dest,
          std::enable_if_t<csv_internal::IsIterableOfPairsWithAssignableValues<
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

inline bool operator==(const CsvRecord& a, const CsvRecord& b) {
  return a.header() == b.header() && a.fields() == b.fields();
}

inline bool operator!=(const CsvRecord& a, const CsvRecord& b) {
  return !(a == b);
}

}  // namespace riegeli

#endif  // RIEGELI_CSV_CSV_RECORD_H_
