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

#include <utility>

#include "absl/types/compare.h"
#include "absl/types/optional.h"

namespace riegeli {

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
// and returns `absl::partial_ordering`:
//  * `less`       - `current` is before the desired position
//  * `equivalent` - `current` is desired, searching can stop
//  * `greater`    - `current` is after the desired position
//  * `unordered`  - it could not be determined which is the case; `current`
//                   will be skipped
//
// Alternatively, `test()` can return `SearchGuide<Traits::Pos>`. See
// `SearchGuide` documentation for details.
//
// Preconditions:
//  * all `less` positions precede all `equivalent` positions
//  * all `less` positions precede all `greater` positions
//  * all `equivalent` positions precede all `greater` positions
//
// If there is some `equivalent` position, `BinarySearch()` returns some
// `equivalent` position. Otherwise, if there is some `greater` position,
// `BinarySearch()` returns the earliest `greater` position. Otherwise
// `BinarySearch()` returns `high`.
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
template <typename Pos, typename Test>
Pos BinarySearch(Pos low, Pos high, Test test);
template <typename Traits, typename Test>
typename Traits::Pos BinarySearch(typename Traits::Pos low,
                                  typename Traits::Pos high, Test test,
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

  // Returns the smallest interesting position after `current`.
  //
  // `Next()` is used only if the `test()` parameter of `BinarySearch()` returns
  // `absl::partial_ordering`. If `test()` returns `SearchGuide<Pos>`, the
  // result of `test()` provides the next position instead.
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

// The `test()` parameter of `BinarySearch()` is a function which returns either
// `absl::partial_ordering` or `SearchGuide<Traits::Pos>`.
//
// If the smallest interesting position after `current` can be found
// independently from `test(current)`, `test(current)` can return
// `absl::partial_ordering`. The next position will be `traits.Next(current)`.
//
// If the smallest interesting position after `current` can be more easily found
// as a side effect of `test(current)`, `test(current)` can return
// `SearchGuide<Pos>`. If `ordering >= 0` (i.e. `ordering` is `equivalent` or
// `greater`), the associated `pos` should be be `current` (or another position
// to replace `current` with). Otherwise (i.e. `ordering` is `less` or
// `unordered`), the associated `pos` should be the smallest interesting
// position after `current`.
template <typename T>
struct SearchGuide {
  absl::partial_ordering ordering;
  T pos;
};

// Implementation details follow.

namespace internal {

template <typename Traits>
inline SearchGuide<typename Traits::Pos> GetSearchGuide(
    absl::partial_ordering ordering, typename Traits::Pos pos,
    const Traits& traits) {
  return SearchGuide<typename Traits::Pos>{
      ordering, ordering >= 0 ? std::move(pos) : traits.Next(std::move(pos))};
}

template <typename Traits>
inline SearchGuide<typename Traits::Pos> GetSearchGuide(
    SearchGuide<typename Traits::Pos> guide, const typename Traits::Pos& pos,
    const Traits& traits) {
  return guide;
}

}  // namespace internal

template <typename Pos, typename Test>
inline Pos BinarySearch(Pos low, Pos high, Test test) {
  return BinarySearch(std::move(low), std::move(high), std::move(test),
                      DefaultSearchTraits<Pos>());
}

template <typename Traits, typename Test>
typename Traits::Pos BinarySearch(typename Traits::Pos low,
                                  typename Traits::Pos high, Test test,
                                  const Traits& traits) {
  // Invariants:
  //  * All positions between the original `low` and the current `low` are
  //    `less` or `unordered`.
  //  * All positions between the current `high` and the original `high` are
  //    `greater` or `unordered`.
  //  * `greater_found` is the first `greater` position between the current
  //    `high` and the original `high` if it exists, or `high` otherwise.
  using Pos = typename Traits::Pos;
  Pos greater_found = high;

again:
  absl::optional<Pos> middle_before_unordered = traits.Middle(low, high);
  if (middle_before_unordered == absl::nullopt) return std::move(greater_found);
  // Invariant: all positions between `middle_before_unordered` and `middle` are
  // `unordered`.
  Pos middle = *middle_before_unordered;
  bool unordered_found = false;
  for (;;) {
    SearchGuide<Pos> guide =
        internal::GetSearchGuide(test(middle), middle, traits);
    if (guide.ordering < 0) {
      low = std::move(guide.pos);
      goto again;
    }
    if (guide.ordering == 0) return std::move(guide.pos);
    if (guide.ordering > 0) {
      greater_found = std::move(guide.pos);
      if (unordered_found) break;
      // Use the position from `guide` instead of `*middle_before_unordered`
      // in case the guide provides a smaller upper bound.
      high = greater_found;
      goto again;
    }
    unordered_found = true;
    if (traits.Empty(guide.pos, high)) break;
    middle = std::move(guide.pos);
  }
  // Either a `greater` position was found after some `unordered` positions,
  // or all positions between `*middle_before_unordered` and `high` are
  // `unordered`.
  high = *std::move(middle_before_unordered);
  goto again;
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_BINARY_SEARCH_H_
