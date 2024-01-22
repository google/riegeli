// Copyright 2020 Google LLC
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

#ifndef RIEGELI_BASE_BINARY_SEARCH_H_
#define RIEGELI_BASE_BINARY_SEARCH_H_

#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/meta/type_traits.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/optional.h"
#include "riegeli/base/compare.h"

namespace riegeli {

// Explains the result of a binary search.
//
// Assumptions:
//  * All `less` positions precede all `equivalent` positions.
//  * All `equivalent` positions precede all `greater` positions.
//  * All `less` positions precede all `greater` positions,
//    even if there are no `equivalent` positions.
//
// Interpretation of the result of a binary search, depending on `ordering`:
//  * `equivalent` - There is some `equivalent` position,
//                   and `found` is some such position.
//  * `greater`    - There are no `equivalent` positions
//                   but there is some `greater` position,
//                   and `found` is the earliest such position.
//  * `less`       - There are no `equivalent` nor `greater` positions
//                   but there is some `less` position,
//                   and `found` is the end of the range to search.
//  * `unordered`  - All positions are `unordered`,
//                   and `found` is the end of the range to search.
template <typename Pos>
struct SearchResult {
  PartialOrdering ordering;
  Pos found;
};

// The `test()` parameter of `BinarySearch()` is a function which returns
// an ordering (a value comparable with literal 0, such as
// `{Partial,Strong}Ordering`, `{std,absl}::{partial,weak,strong}_ordering`,
// or `int`) or `SearchGuide<Traits::Pos>`.
//
// If the earliest interesting position after `current` can be found
// independently from `test(current)`, `test(current)` can return an ordering.
// The next position will be `traits.Next(current)`.
//
// If the earliest interesting position after `current` can be more easily found
// as a side effect of `test(current)`, `test(current)` can return
// `SearchGuide<Pos>`. If `ordering >= 0` (i.e. `ordering` is `equivalent` or
// `greater`), the associated `next` should be `current` (or another position
// to replace `current` with). Otherwise (i.e. `ordering` is `less` or
// `unordered`), the associated `next` should be the earliest interesting
// position after `current`.
template <typename Pos>
struct SearchGuide {
  template <typename Ordering,
            std::enable_if_t<IsOrdering<Ordering>::value, int> = 0>
  explicit SearchGuide(Ordering ordering, const Pos& next)
      : ordering(AsPartialOrdering(ordering)), next(next) {}
  template <typename Ordering,
            std::enable_if_t<IsOrdering<Ordering>::value, int> = 0>
  explicit SearchGuide(Ordering ordering, Pos&& next)
      : ordering(AsPartialOrdering(ordering)), next(std::move(next)) {}

  SearchGuide(const SearchGuide& other) = default;
  SearchGuide& operator=(const SearchGuide& other) = default;

  SearchGuide(SearchGuide&& other) = default;
  SearchGuide& operator=(SearchGuide&& other) = default;

  template <
      typename OtherPos,
      std::enable_if_t<std::is_convertible<OtherPos, Pos>::value, int> = 0>
  /*implicit*/ SearchGuide(const SearchGuide<OtherPos>& other)
      : ordering(other.ordering), next(other.next) {}
  template <
      typename OtherPos,
      std::enable_if_t<std::is_convertible<OtherPos, Pos>::value, int> = 0>
  SearchGuide& operator=(const SearchGuide<OtherPos>& other) {
    ordering = other.ordering;
    next = other.next;
    return *this;
  }

  template <
      typename OtherPos,
      std::enable_if_t<std::is_convertible<OtherPos, Pos>::value, int> = 0>
  /*implicit*/ SearchGuide(SearchGuide<OtherPos>&& other)
      : ordering(other.ordering), next(std::move(other.next)) {}
  template <
      typename OtherPos,
      std::enable_if_t<std::is_convertible<OtherPos, Pos>::value, int> = 0>
  SearchGuide& operator=(SearchGuide<OtherPos>&& other) {
    ordering = other.ordering;
    next = std::move(other.next);
    return *this;
  }

  PartialOrdering ordering;
  Pos next;
};

// Support CTAD.
#if __cpp_deduction_guides
template <typename Pos, typename Ordering>
explicit SearchGuide(Ordering ordering,
                     Pos next) -> SearchGuide<std::decay_t<Pos>>;
#endif

namespace binary_search_internal {

template <typename T, typename Pos, typename Enable = void>
struct IsSearchGuide : std::false_type {};

template <typename Pos, typename OtherPos>
struct IsSearchGuide<SearchGuide<OtherPos>, Pos>
    : std::is_convertible<OtherPos, Pos> {};

template <typename T, typename Pos>
struct IsOrderingOrSearchGuide
    : absl::disjunction<IsOrdering<T>, IsSearchGuide<T, Pos>> {};

template <typename T, typename Pos>
struct IsOptionalOrderingOrSearchGuide : std::false_type {};

template <typename T, typename Pos>
struct IsOptionalOrderingOrSearchGuide<absl::optional<T>, Pos>
    : IsOrderingOrSearchGuide<T, Pos> {};

template <typename T, typename Pos>
struct IsStatusOrOrderingOrSearchGuide : std::false_type {};

template <typename T, typename Pos>
struct IsStatusOrOrderingOrSearchGuide<absl::StatusOr<T>, Pos>
    : IsOrderingOrSearchGuide<T, Pos> {};

template <typename Test, typename Pos, typename Enable = void>
struct TestReturnsOrderingOrSearchGuide : std::false_type {};

template <typename Test, typename Pos>
struct TestReturnsOrderingOrSearchGuide<
    Test, Pos,
    std::enable_if_t<IsOrderingOrSearchGuide<
        decltype(std::declval<Test>()(std::declval<Pos>())), Pos>::value>>
    : std::true_type {};

template <typename Test, typename Pos, typename Enable = void>
struct TestReturnsOptionalOrderingOrSearchGuide : std::false_type {};

template <typename Test, typename Pos>
struct TestReturnsOptionalOrderingOrSearchGuide<
    Test, Pos,
    std::enable_if_t<IsOptionalOrderingOrSearchGuide<
        decltype(std::declval<Test>()(std::declval<Pos>())), Pos>::value>>
    : std::true_type {};

template <typename Test, typename Pos, typename Enable = void>
struct TestReturnsStatusOrOrderingOrSearchGuide : std::false_type {};

template <typename Test, typename Pos>
struct TestReturnsStatusOrOrderingOrSearchGuide<
    Test, Pos,
    std::enable_if_t<IsStatusOrOrderingOrSearchGuide<
        decltype(std::declval<Test>()(std::declval<Pos>())), Pos>::value>>
    : std::true_type {};

}  // namespace binary_search_internal

// Searches a sequence of elements for a desired element, or for a desired
// position between elements, given that it is possible to determine whether a
// given position is before or after the desired position.
//
// The `traits` parameter specifies the space of possible positions.
// See `DefaultSearchTraits` documentation for details. The default `traits` are
// `DefaultSearchTraits<Pos>()`.
//
// The `low` (inclusive) and `high` (exclusive) parameters specify the range to
// search.
//
// The `test()` function takes `current` of type `Traits::Pos` as a parameter
// and returns an ordering:
//  * `less`       - `current` is before the desired position.
//  * `equivalent` - `current` is desired, searching can stop.
//  * `greater`    - `current` is after the desired position.
//  * `unordered`  - It could not be determined which is the case. `current`
//                   will be skipped.
//
// Alternatively, `test()` can return `SearchGuide<Traits::Pos>`. See
// `SearchGuide` documentation for details.
//
// Preconditions:
//  * All `less` positions precede all `equivalent` positions.
//  * All `equivalent` positions precede all `greater` positions.
//  * All `less` positions precede all `greater` positions,
//    even if there are no `equivalent` positions.
//
// For interpretation of the result, see `SearchResult` documentation.
//
// To find the earliest `equivalent` position instead of an arbitrary one,
// `test()` can be changed to return `greater` in place of `equivalent`.
//
// Further guarantees:
//  * Each `traits.Next(current)` immediately follows a `test(current)` which
//    returned `less` or `unordered`.
//  * Each `test(current)` immediately follows a `traits.Next()` which returned
//    `current`, or a `test()` which returned a `SearchGuide` containing `less`
//    or `unordered` together with `current`, or a `traits.Middle()` which
//    returned `current`.
//  * If `test(current)` returns `equivalent`, `BinarySearch()` immediately
//    returns `current`.
//  * If `test(current)` returns `less`, `test()` will not be called again
//    with arguments before `current`.
//  * If `test(current)` returns `greater`, `test()` will not be called again
//    with arguments after `current`.
//  * `test()` will not be called again with the same argument.
//
// It follows that if a `test()` returns `equivalent` or `greater`,
// `BinarySearch()` returns the argument of the last `test()` call with one of
// these results. This allows to communicate additional context of an
// `equivalent` or `greater` result by a side effect of `test()`.
template <
    typename Pos, typename Test,
    std::enable_if_t<binary_search_internal::TestReturnsOrderingOrSearchGuide<
                         Test, Pos>::value,
                     int> = 0>
SearchResult<Pos> BinarySearch(Pos low, Pos high, Test&& test);
template <
    typename Traits, typename Test,
    std::enable_if_t<binary_search_internal::TestReturnsOrderingOrSearchGuide<
                         Test, typename Traits::Pos>::value,
                     int> = 0>
SearchResult<typename Traits::Pos> BinarySearch(typename Traits::Pos low,
                                                typename Traits::Pos high,
                                                Test&& test,
                                                const Traits& traits);

// A variant of `BinarySearch()` which supports cancellation.
//
// If a `test()` returns `absl::nullopt`, `BinarySearch()` returns
// `absl::nullopt`.
template <typename Pos, typename Test,
          std::enable_if_t<
              binary_search_internal::TestReturnsOptionalOrderingOrSearchGuide<
                  Test, Pos>::value,
              int> = 0>
absl::optional<SearchResult<Pos>> BinarySearch(Pos low, Pos high, Test&& test);
template <typename Traits, typename Test,
          std::enable_if_t<
              binary_search_internal::TestReturnsOptionalOrderingOrSearchGuide<
                  Test, typename Traits::Pos>::value,
              int> = 0>
absl::optional<SearchResult<typename Traits::Pos>> BinarySearch(
    typename Traits::Pos low, typename Traits::Pos high, Test&& test,
    const Traits& traits);

// A variant of `BinarySearch()` which supports cancellation with a `Status`.
//
// If a `test()` returns a failed `absl::StatusOr`, `BinarySearch()` returns
// the corresponding failed `absl::StatusOr`.
template <typename Pos, typename Test,
          std::enable_if_t<
              binary_search_internal::TestReturnsStatusOrOrderingOrSearchGuide<
                  Test, Pos>::value,
              int> = 0>
absl::StatusOr<SearchResult<Pos>> BinarySearch(Pos low, Pos high, Test&& test);
template <typename Traits, typename Test,
          std::enable_if_t<
              binary_search_internal::TestReturnsStatusOrOrderingOrSearchGuide<
                  Test, typename Traits::Pos>::value,
              int> = 0>
absl::StatusOr<SearchResult<typename Traits::Pos>> BinarySearch(
    typename Traits::Pos low, typename Traits::Pos high, Test&& test,
    const Traits& traits);

// The `traits` parameter of `BinarySearch()` specifies the space of positions
// to search.
//
// Some positions might be determined to be uninteresting, which means that for
// the purposes of the search they are equivalent to a nearby interesting
// position. They are skipped during the search.
//
// `DefaultSearchTraits<Pos>` might be appropriate for positions of an
// arithmetic type. If custom traits are needed instead, these comments specify
// generalized requirements of the traits.
template <typename T>
class DefaultSearchTraits {
 public:
  // Identifies a position between elements being searched. This type must be
  // copyable.
  using Pos = T;

  // Returns the earliest interesting position after `current`.
  //
  // `Next()` is used only if the `test()` parameter of `BinarySearch()` returns
  // an ordering. If `test()` returns `SearchGuide<Pos>`, the result of `test()`
  // provides the next position instead.
  //
  // Precondition: `test(current)` returned `less` or `unordered`.
  T Next(T current) const { return current + 1; }

  // Returns `true` if the range between `low` and `high` contains no positions.
  bool Empty(T low, T high) const { return low >= high; }

  // Returns a position in the range from `low` (inclusive) to `high`
  // (exclusive) which is approximately halfway between `low` and `high`.
  // Returns `absl::nullopt` if the range contains no interesting positions.
  absl::optional<T> Middle(T low, T high) const {
    if (low >= high) return absl::nullopt;
    return low + (high - low) / 2;
  }
};

// Implementation details follow.

namespace binary_search_internal {

template <typename Traits, typename Ordering,
          std::enable_if_t<IsOrdering<Ordering>::value, int> = 0>
inline SearchGuide<typename Traits::Pos> GetSearchGuide(
    Ordering ordering, typename Traits::Pos&& pos, const Traits& traits) {
  return SearchGuide<typename Traits::Pos>(
      AsPartialOrdering(ordering),
      ordering >= 0 ? std::move(pos) : traits.Next(std::move(pos)));
}

template <typename Traits, typename OtherPos>
inline SearchGuide<typename Traits::Pos> GetSearchGuide(
    SearchGuide<OtherPos>&& guide,
    ABSL_ATTRIBUTE_UNUSED typename Traits::Pos&& pos,
    ABSL_ATTRIBUTE_UNUSED const Traits& traits) {
  return std::move(guide);
}

template <typename Pos, typename TestResult, typename Enable = void>
struct CancelSearch;

template <typename Pos, typename Ordering>
struct CancelSearch<Pos, Ordering,
                    std::enable_if_t<IsOrdering<Ordering>::value>> {
  static PartialOrdering DoCancel(ABSL_ATTRIBUTE_UNUSED const Pos& pos) {
    return PartialOrdering::equivalent;
  }
  static PartialOrdering DoNotCancel(Ordering ordering) {
    return AsPartialOrdering(ordering);
  }
};

template <typename Pos, typename OtherPos>
struct CancelSearch<Pos, SearchGuide<OtherPos>> {
  static SearchGuide<Pos> DoCancel(const Pos& pos) {
    return SearchGuide<Pos>(PartialOrdering::equivalent, pos);
  }
  static SearchGuide<Pos> DoNotCancel(SearchGuide<OtherPos>&& guide) {
    return std::move(guide);
  }
};

}  // namespace binary_search_internal

template <
    typename Pos, typename Test,
    std::enable_if_t<binary_search_internal::TestReturnsOrderingOrSearchGuide<
                         Test, Pos>::value,
                     int>>
inline SearchResult<Pos> BinarySearch(Pos low, Pos high, Test&& test) {
  return BinarySearch(std::move(low), std::move(high), std::forward<Test>(test),
                      DefaultSearchTraits<Pos>());
}

template <
    typename Traits, typename Test,
    std::enable_if_t<binary_search_internal::TestReturnsOrderingOrSearchGuide<
                         Test, typename Traits::Pos>::value,
                     int>>
inline SearchResult<typename Traits::Pos> BinarySearch(
    typename Traits::Pos low, typename Traits::Pos high, Test&& test,
    const Traits& traits) {
  // Invariants:
  //  * All positions between the original `low` and the current `low` are
  //    `less` or `unordered`.
  //  * All positions between the current `high` and the original `high` are
  //    `greater` or `unordered`.
  //
  // Invariants depending on `greater_result.ordering`:
  //  * `greater`   - `greater_result.found` is the first `greater` position
  //                  between the current `high` and the original `high`.
  //  * `less`      - There are no such positions but there are `less` positions
  //                  between the original `low` and the current `low`,
  //                  and `greater_result.found` is `high`.
  //  * `unordered` - There are no such positions either,
  //                  and `greater_result.found` is `high`.
  using Pos = typename Traits::Pos;
  SearchResult<Pos> greater_result = {PartialOrdering::unordered, high};

again:
  absl::optional<Pos> middle_before_unordered = traits.Middle(low, high);
  if (middle_before_unordered == absl::nullopt) return greater_result;
  Pos middle = *middle_before_unordered;
  // Invariant: all positions between `*middle_before_unordered` and `middle`
  // are `unordered`.
  bool unordered_found = false;
  for (;;) {
    auto test_result = test(middle);
    SearchGuide<Pos> guide = binary_search_internal::GetSearchGuide(
        std::move(test_result), std::move(middle), traits);
    if (guide.ordering < 0) {
      if (!(greater_result.ordering >= 0)) {
        greater_result.ordering = PartialOrdering::less;
      }
      low = std::move(guide.next);
      goto again;
    }
    if (guide.ordering == 0) {
      // Assign instead of returning for NRVO.
      greater_result.ordering = PartialOrdering::equivalent;
      greater_result.found = std::move(guide.next);
      return greater_result;
    }
    if (guide.ordering > 0) {
      greater_result.ordering = PartialOrdering::greater;
      greater_result.found = std::move(guide.next);
      if (unordered_found) break;
      // Use the position from `guide` instead of `*middle_before_unordered`
      // in case the guide provides an earlier upper bound.
      high = greater_result.found;
      goto again;
    }
    unordered_found = true;
    if (traits.Empty(guide.next, high)) break;
    middle = std::move(guide.next);
  }
  // Either a `greater` position was found after some `unordered` positions,
  // or all positions between `*middle_before_unordered` and `high` are
  // `unordered`.
  high = *std::move(middle_before_unordered);
  goto again;
}

template <typename Pos, typename Test,
          std::enable_if_t<
              binary_search_internal::TestReturnsOptionalOrderingOrSearchGuide<
                  Test, Pos>::value,
              int>>
inline absl::optional<SearchResult<Pos>> BinarySearch(Pos low, Pos high,
                                                      Test&& test) {
  return BinarySearch(std::move(low), std::move(high), std::forward<Test>(test),
                      DefaultSearchTraits<Pos>());
}

template <typename Traits, typename Test,
          std::enable_if_t<
              binary_search_internal::TestReturnsOptionalOrderingOrSearchGuide<
                  Test, typename Traits::Pos>::value,
              int>>
inline absl::optional<SearchResult<typename Traits::Pos>> BinarySearch(
    typename Traits::Pos low, typename Traits::Pos high, Test&& test,
    const Traits& traits) {
  bool cancelled = false;
  SearchResult<typename Traits::Pos> result = BinarySearch(
      std::move(low), std::move(high),
      [&](const typename Traits::Pos& pos) {
        auto test_result = test(pos);
        using Cancel = binary_search_internal::CancelSearch<
            typename Traits::Pos, std::decay_t<decltype(*test_result)>>;
        if (ABSL_PREDICT_FALSE(test_result == absl::nullopt)) {
          cancelled = true;
          return Cancel::DoCancel(pos);
        }
        return Cancel::DoNotCancel(*std::move(test_result));
      },
      traits);
  if (ABSL_PREDICT_FALSE(cancelled)) return absl::nullopt;
  return result;
}

template <typename Pos, typename Test,
          std::enable_if_t<
              binary_search_internal::TestReturnsStatusOrOrderingOrSearchGuide<
                  Test, Pos>::value,
              int>>
inline absl::StatusOr<SearchResult<Pos>> BinarySearch(Pos low, Pos high,
                                                      Test&& test) {
  return BinarySearch(std::move(low), std::move(high), std::forward<Test>(test),
                      DefaultSearchTraits<Pos>());
}

template <typename Traits, typename Test,
          std::enable_if_t<
              binary_search_internal::TestReturnsStatusOrOrderingOrSearchGuide<
                  Test, typename Traits::Pos>::value,
              int>>
inline absl::StatusOr<SearchResult<typename Traits::Pos>> BinarySearch(
    typename Traits::Pos low, typename Traits::Pos high, Test&& test,
    const Traits& traits) {
  absl::Status status;
  SearchResult<typename Traits::Pos> result = BinarySearch(
      std::move(low), std::move(high),
      [&](const typename Traits::Pos& pos) {
        auto test_result = test(pos);
        using Cancel = binary_search_internal::CancelSearch<
            typename Traits::Pos, std::decay_t<decltype(*test_result)>>;
        if (ABSL_PREDICT_FALSE(!test_result.ok())) {
          status = test_result.status();
          return Cancel::DoCancel(pos);
        }
        return Cancel::DoNotCancel(*std::move(test_result));
      },
      traits);
  if (ABSL_PREDICT_FALSE(!status.ok())) return status;
  return result;
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_BINARY_SEARCH_H_
