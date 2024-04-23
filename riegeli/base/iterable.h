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

#ifndef RIEGELI_BASE_ITERABLE_H_
#define RIEGELI_BASE_ITERABLE_H_

#include <iterator>
#include <type_traits>
#include <utility>

#include "absl/meta/type_traits.h"
#include "riegeli/base/dependency.h"

namespace riegeli {

// Let unqualified `begin()` below refer either to a function named `begin()`
// found via ADL or to `std::begin()`, as appropriate for the given iterable.
// This is done in a separate namespace to avoid introducing a name
// `riegeli::begin()`.

namespace adl_begin_sandbox {

using std::begin;

template <typename Iterable>
using IteratorT = decltype(begin(std::declval<Iterable&>()));

}  // namespace adl_begin_sandbox

// `IteratorT<Iterable>` is the type of the iterator over `Iterable`.
using adl_begin_sandbox::IteratorT;

// `IsIterableOf<Iterable, Element>::value` is `true` if iterating over
// `Iterable` yields elements convertible to `Element`.

template <typename Iterable, typename Element, typename Enable = void>
struct IsIterableOf : std::false_type {};

template <typename Iterable, typename Element>
struct IsIterableOf<
    Iterable, Element,
    std::enable_if_t<std::is_convertible<
        decltype(*std::declval<IteratorT<Iterable>>()), Element>::value>>
    : std::true_type {};

// `IsIterableOfPairsWithAssignableValues<Iterable, Key, Value>::value` is
// `true` if iterating over `Iterable` yields pair proxies with keys convertible
// to `Key` and values assignable from `Value`.

template <typename Iterable, typename Key, typename Value,
          typename Enable = void>
struct IsIterableOfPairsWithAssignableValues : std::false_type {};

template <typename Iterable, typename Key, typename Value>
struct IsIterableOfPairsWithAssignableValues<
    Iterable, Key, Value,
    std::enable_if_t<absl::conjunction<
        std::is_convertible<
            decltype(std::declval<IteratorT<Iterable>>()->first), Key>,
        std::is_assignable<
            decltype(std::declval<IteratorT<Iterable>>()->second),
            Value>>::value>> : std::true_type {};

// TODO: Use `typename std::iterator_traits<Iterator>::iterator_concept`
// instead when C++20 is unconditionally available.

namespace iterable_internal {

template <typename Iterator, typename Enable = void>
struct IteratorConcept {
  using type = typename std::iterator_traits<Iterator>::iterator_category;
};

template <typename Iterator>
struct IteratorConcept<
    Iterator,
    absl::void_t<typename std::iterator_traits<Iterator>::iterator_concept>> {
  using type = typename std::iterator_traits<Iterator>::iterator_concept;
};

template <typename Iterator>
using IteratorConceptT = typename IteratorConcept<Iterator>::type;

}  // namespace iterable_internal

// `IsRandomAccessIterable<Iterable>::value` is `true` if the iterator over
// `Iterable` is a random access iterator.

template <typename Iterable, typename Enable = void>
struct IsRandomAccessIterable : std::false_type {};

template <typename Iterable>
struct IsRandomAccessIterable<
    Iterable, std::enable_if_t<std::is_convertible<
                  iterable_internal::IteratorConceptT<IteratorT<Iterable>>,
                  std::random_access_iterator_tag>::value>> : std::true_type {};

// `HasMovableElements<Src>::value` is `true` if moving (rather than copying)
// out of elements of the iterable provided by `Src` is safe.
//
// `Src` is the type of an object providing and possibly owning an iterable,
// supporting `Dependency<const void*, Src>`.
//
// Moving out of elements of the iterable provided by `Src` is safe if `Src`
// owns the iterable and the iterable owns its elements, i.e. it is not a view
// container like `absl::Span<T>`.
//
// By default an iterable is detected as owning its elements when iterating over
// `Iterable&` and `const Iterable&` yields elements of different types. This
// also catches cases where `Iterable` always yields const elements or is const
// itself. In these cases moving would be equivalent to copying, and trying to
// move would just yield unnecessarily separate template instantiations.
//
// To customize that for a class `Iterable`, define a free function
// `friend constexpr bool RiegeliHasMovableElements(Iterable*)` as a friend of
// `Iterable` inside class definition or in the same namespace as `Iterable`,
// so that it can be found via ADL.
//
// The argument of `RiegeliHasMovableElements(Iterable*)` is always a null
// pointer, used to choose the right overload based on the type.

namespace iterable_internal {

template <typename Iterable, typename Enable = void>
struct IterableHasMovableElements
    : absl::negation<
          std::is_same<decltype(*std::declval<IteratorT<Iterable>>()),
                       decltype(*std::declval<IteratorT<const Iterable>>())>> {
};

template <typename Iterable>
struct IterableHasMovableElements<
    Iterable,
    absl::enable_if_t<std::is_convertible<decltype(RiegeliHasMovableElements(
                                              static_cast<Iterable*>(nullptr))),
                                          bool>::value>>
    : std::integral_constant<bool, RiegeliHasMovableElements(
                                       static_cast<Iterable*>(nullptr))> {};

}  // namespace iterable_internal

template <typename Src, typename Enable = void>
struct HasMovableElements : std::false_type {};

template <typename Src>
struct HasMovableElements<
    Src, std::enable_if_t<Dependency<const void*, Src>::kIsOwning>>
    : iterable_internal::IterableHasMovableElements<std::remove_pointer_t<
          typename Dependency<const void*, Src>::Subhandle>> {};

// `MaybeMakeMoveIterator<Src>(iterator)` is `std::make_move_iterator(iterator)`
// or `iterator`, depending on whether moving out of elements of the iterable
// provided by `Src` is safe.

template <typename Src, typename Iterator,
          std::enable_if_t<!HasMovableElements<Src>::value, int> = 0>
inline Iterator MaybeMakeMoveIterator(Iterator iterator) {
  return iterator;
}

template <typename Src, typename Iterator,
          std::enable_if_t<HasMovableElements<Src>::value, int> = 0>
inline std::move_iterator<Iterator> MaybeMakeMoveIterator(Iterator iterator) {
  return std::move_iterator<Iterator>(iterator);
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_ITERABLE_H_
