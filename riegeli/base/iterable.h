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

#include <stddef.h>

#include <iterator>
#include <type_traits>
#include <utility>

#include "absl/base/nullability.h"
#include "riegeli/base/type_traits.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

namespace iterable_internal {

// Let unqualified `begin()` below refer either to a function named `begin()`
// found via ADL or to `std::begin()`, as appropriate for the given iterable.
// This is done in a separate namespace to avoid defining `riegeli::begin`.
// Same for `end()` and `size()`.

using std::begin;
using std::end;
using std::size;

template <typename T, typename Enable = void>
struct IsIterable : std::false_type {};

template <typename T>
struct IsIterable<T, std::void_t<decltype(*begin(std::declval<T&>()))>>
    : std::true_type {};

template <typename Iterable, typename Enable = void>
struct IteratorType {};

template <typename Iterable>
struct IteratorType<Iterable, std::enable_if_t<IsIterable<Iterable>::value>>
    : type_identity<decltype(begin(std::declval<Iterable&>()))> {};

template <typename Iterable, bool move = false, typename Enable = void>
struct ElementTypeInternal {};

template <typename Iterable>
struct ElementTypeInternal<Iterable, false,
                           std::enable_if_t<IsIterable<Iterable>::value>>
    : type_identity<decltype(*begin(std::declval<Iterable&>()))> {};

template <typename Iterable>
struct ElementTypeInternal<Iterable, true,
                           std::enable_if_t<IsIterable<Iterable>::value>>
    : type_identity<decltype(*std::make_move_iterator(
          begin(std::declval<Iterable&>())))> {};

template <typename Iterable, typename Enable = void>
struct IterableHasSize : std::false_type {};

template <typename Iterable>
struct IterableHasSize<
    Iterable, std::enable_if_t<std::is_convertible_v<
                  decltype(size(std::declval<const Iterable&>())), size_t>>>
    : std::true_type {};

}  // namespace iterable_internal

// `IsIterable<T>::value` is `true` when `T` is iterable, supporting
// `begin(iterable)` after `using std::begin;` (not all details are verified).
using iterable_internal::IsIterable;

// `HasMovableElements<Iterable>::value` is `true` when moving (rather than
// copying) out of elements of `Iterable` is safe. This is the case when
// `Iterable` owns its elements, i.e. it is not a view container like
// `absl::Span<T>`, and it is not an lvalue reference.
//
// By default an iterable is detected as owning its elements when iterating over
// `Iterable` and `const Iterable` yields elements of different types. This
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

template <typename Iterable, typename Enable = void>
struct HasMovableElements
    : std::negation<std::is_same<
          typename iterable_internal::ElementTypeInternal<Iterable>::type,
          typename iterable_internal::ElementTypeInternal<
              const Iterable>::type>> {};

template <typename Iterable>
struct HasMovableElements<
    Iterable,
    std::enable_if_t<std::conjunction_v<
        std::negation<std::is_reference<Iterable>>,
        std::is_convertible<decltype(RiegeliHasMovableElements(
                                static_cast<Iterable* absl_nullable>(nullptr))),
                            bool>>>>
    : std::bool_constant<RiegeliHasMovableElements(
          static_cast<Iterable* absl_nullable>(nullptr))> {};

template <typename Iterable>
struct HasMovableElements<Iterable&> : std::false_type {};

template <typename Iterable>
struct HasMovableElements<Iterable&&> : HasMovableElements<Iterable> {};

// `MaybeMakeMoveIterator<Iterable>(iterator)` is
// `std::make_move_iterator(iterator)` or `iterator`, depending on whether
// moving out of elements of `Iterable` is safe.
template <typename Iterable, typename Iterator>
inline auto MaybeMakeMoveIterator(Iterator iterator) {
  if constexpr (HasMovableElements<Iterable>::value) {
    return std::move_iterator<Iterator>(std::move(iterator));
  } else {
    return iterator;
  }
}

// `ElementType<Iterable>::type` and `ElementTypeT<Iterable>` is the type of
// elements yielded by iterating over `Iterable`.
//
// The result is a reference, except when iteration yields temporary objects.
// If moving out of elements of `Iterable` is safe, this is an rvalue reference.

template <typename Iterable, typename Enable = void>
struct ElementType {};

template <typename Iterable>
struct ElementType<Iterable, std::enable_if_t<IsIterable<Iterable>::value>>
    : iterable_internal::ElementTypeInternal<
          Iterable, HasMovableElements<Iterable>::value> {};

template <typename Iterable>
using ElementTypeT = typename ElementType<Iterable>::type;

// `IsIterableOf<Iterable, Element>::value` is `true` when iterating over
// `Iterable` yields elements convertible to `Element`.

template <typename Iterable, typename Element, typename Enable = void>
struct IsIterableOf : std::false_type {};

template <typename Iterable, typename Element>
struct IsIterableOf<Iterable, Element,
                    std::enable_if_t<IsIterable<Iterable>::value>>
    : std::is_convertible<ElementTypeT<Iterable>, Element> {};

// `IsIterableOfPairs<Iterable, Key, Value>::value` is `true` when iterating
// over `Iterable` yields pairs or pair proxies with keys convertible to `Key`
// and values convertible to `Value`.

template <typename Iterable, typename Key, typename Value,
          typename Enable = void>
struct IsIterableOfPairs : std::false_type {};

template <typename Iterable, typename Key, typename Value>
struct IsIterableOfPairs<
    Iterable, Key, Value,
    std::enable_if_t<std::conjunction_v<
        IsIterable<Iterable>,
        std::is_convertible<
            decltype(std::declval<ElementTypeT<Iterable>>().first), Key>,
        std::is_convertible<
            decltype(std::declval<ElementTypeT<Iterable>>().second), Value>>>>
    : std::true_type {};

// `IsIterableOfPairsWithAssignableValues<Iterable, Key, Value>::value`
// is `true` when iterating over `Iterable` yields pair proxies with keys
// convertible to `Key` and values assignable from `Value`.

template <typename Iterable, typename Key, typename Value,
          typename Enable = void>
struct IsIterableOfPairsWithAssignableValues : std::false_type {};

template <typename Iterable, typename Key, typename Value>
struct IsIterableOfPairsWithAssignableValues<
    Iterable, Key, Value,
    std::enable_if_t<std::conjunction_v<
        IsIterable<Iterable>,
        std::is_convertible<
            decltype(std::declval<ElementTypeT<Iterable>>().first), Key>,
        std::is_assignable<
            decltype(std::declval<ElementTypeT<Iterable>>().second), Value>>>>
    : std::true_type {};

// TODO: Use `typename std::iterator_traits<Iterator>::iterator_concept`
// instead when C++20 is unconditionally available.

namespace iterable_internal {

template <typename Iterator, typename Enable = void>
struct IteratorConcept
    : type_identity<
          typename std::iterator_traits<Iterator>::iterator_category> {};

template <typename Iterator>
struct IteratorConcept<
    Iterator,
    std::void_t<typename std::iterator_traits<Iterator>::iterator_concept>>
    : type_identity<typename std::iterator_traits<Iterator>::iterator_concept> {
};

}  // namespace iterable_internal

// `IsForwardIterable<Iterable>::value` is `true` when the iterator over
// `Iterable` is a forward iterator, in particular when it can be iterated
// over multiple times.

template <typename Iterable, typename Enable = void>
struct IsForwardIterable : std::false_type {};

template <typename Iterable>
struct IsForwardIterable<
    Iterable,
    std::enable_if_t<std::conjunction_v<
        IsIterable<Iterable>,
        std::is_convertible<
            typename iterable_internal::IteratorConcept<
                typename iterable_internal::IteratorType<Iterable>::type>::type,
            std::forward_iterator_tag>>>> : std::true_type {};

// `IsRandomAccessIterable<Iterable>::value` is `true` when the iterator over
// `Iterable` is a random access iterator.

template <typename Iterable, typename Enable = void>
struct IsRandomAccessIterable : std::false_type {};

template <typename Iterable>
struct IsRandomAccessIterable<
    Iterable,
    std::enable_if_t<std::conjunction_v<
        IsIterable<Iterable>,
        std::is_convertible<
            typename iterable_internal::IteratorConcept<
                typename iterable_internal::IteratorType<Iterable>::type>::type,
            std::random_access_iterator_tag>>>> : std::true_type {};

// `IterableHasSize<Iterable>::value` is `true` when `Iterable` supports
// `size(iterable)` after `using std::size;`.
using iterable_internal::IterableHasSize;

}  // namespace riegeli

#endif  // RIEGELI_BASE_ITERABLE_H_
