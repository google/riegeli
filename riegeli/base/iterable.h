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

#include "absl/base/attributes.h"
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

// Represents the result of `operator->` if `operator*` returns a proxy
// object rather than a true reference. In particular this can be used as
// `iterator::pointer` if `iterator::reference` is not a true reference.
template <typename Reference>
class ArrowProxy {
 public:
  explicit ArrowProxy(Reference ref) : ref_(std::move(ref)) {}

  ArrowProxy(const ArrowProxy& that) = default;
  ArrowProxy& operator=(const ArrowProxy& that) = default;

  ArrowProxy(ArrowProxy&& that) noexcept = default;
  ArrowProxy& operator=(ArrowProxy&& that) noexcept = default;

  const Reference* operator->() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return &ref_;
  }

 private:
  Reference ref_;
};

// A pair-like type to be used as `iterator::reference` for iterators over a map
// with a separate storage for keys and values. In C++20 this lets the iterator
// satisfy `std::indirectly_readable`.
//
// It extends `std::pair<T1, T2>` with conversions from `std::pair<U1, U2>&`
// and with `std::basic_common_reference` specializations.
//
// Since C++23, `std::pair<T1, T2>` can be used directly instead.
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

}  // namespace riegeli

#if __cplusplus >= 202002L

template <typename T1, typename T2, typename U1, typename U2,
          template <typename> class TQual, template <typename> class UQual>
struct std::basic_common_reference<riegeli::ReferencePair<T1, T2>,
                                   std::pair<U1, U2>, TQual, UQual> {
  using type =
      riegeli::ReferencePair<std::common_reference_t<TQual<T1>, UQual<U1>>,
                             std::common_reference_t<TQual<T2>, UQual<U2>>>;
};

template <typename T1, typename T2, typename U1, typename U2,
          template <typename> class TQual, template <typename> class UQual>
struct std::basic_common_reference<
    std::pair<T1, T2>, riegeli::ReferencePair<U1, U2>, TQual, UQual> {
  using type =
      riegeli::ReferencePair<std::common_reference_t<TQual<T1>, UQual<U1>>,
                             std::common_reference_t<TQual<T2>, UQual<U2>>>;
};

template <typename T1, typename T2, typename U1, typename U2,
          template <typename> class TQual, template <typename> class UQual>
struct std::basic_common_reference<riegeli::ReferencePair<T1, T2>,
                                   riegeli::ReferencePair<U1, U2>, TQual,
                                   UQual> {
  using type =
      riegeli::ReferencePair<std::common_reference_t<TQual<T1>, UQual<U1>>,
                             std::common_reference_t<TQual<T2>, UQual<U2>>>;
};

#endif

#endif  // RIEGELI_BASE_ITERABLE_H_
