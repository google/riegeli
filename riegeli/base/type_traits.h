// Copyright 2017 Google LLC
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

#ifndef RIEGELI_BASE_TYPE_TRAITS_H_
#define RIEGELI_BASE_TYPE_TRAITS_H_

#include <stddef.h>

#include <iterator>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/meta/type_traits.h"
#include "absl/strings/string_view.h"

namespace riegeli {

// `type_identity<T>::type` and `type_identity_t<T>` are `T`, but do not deduce
// the `T` in templates.

template <typename T>
struct type_identity {
  using type = T;
};

template <typename T>
using type_identity_t = typename type_identity<T>::type;

namespace type_traits_internal {

// Transforms a `std::tuple` type to another `std::tuple` type by selecting
// element types corresponding to the given `std::index_sequence`.
template <typename Tuple, typename Indices>
struct SelectTypesFromTuple;

template <typename Tuple, size_t... indices>
struct SelectTypesFromTuple<Tuple, std::index_sequence<indices...>> {
  using type = std::tuple<std::tuple_element_t<indices, Tuple>...>;
};

// Selects element types from a `std::tuple` type corresponding to the given
// `std::index_sequence`.
template <typename Tuple, size_t... indices>
std::tuple<std::tuple_element_t<indices, Tuple>...> SelectFromTuple(
    ABSL_ATTRIBUTE_UNUSED Tuple&& tuple, std::index_sequence<indices...>) {
  return {std::forward<std::tuple_element_t<indices, Tuple>>(
      std::get<indices>(tuple))...};
}

// SFINAE-friendly helper for `GetTypeFromEnd`.
template <size_t reverse_index, typename Enable, typename... T>
struct GetTypeFromEndImpl {};
template <size_t reverse_index, typename... T>
struct GetTypeFromEndImpl<
    reverse_index,
    std::enable_if_t<(reverse_index > 0 && reverse_index <= sizeof...(T))>,
    T...> : std::tuple_element<sizeof...(T) - reverse_index, std::tuple<T...>> {
};

// SFINAE-friendly helper for `RemoveTypesFromEnd`.
template <size_t num_from_end, typename Enable, typename... T>
struct RemoveTypesFromEndImpl {};
template <size_t num_from_end, typename... T>
struct RemoveTypesFromEndImpl<
    num_from_end, std::enable_if_t<(num_from_end <= sizeof...(T))>, T...>
    : SelectTypesFromTuple<std::tuple<T...>, std::make_index_sequence<
                                                 sizeof...(T) - num_from_end>> {
};

// Concatenates `std::index_sequence` types.
template <typename... index_sequences>
struct ConcatIndexSequences;

template <>
struct ConcatIndexSequences<> {
  using type = std::index_sequence<>;
};
template <typename index_sequence>
struct ConcatIndexSequences<index_sequence> {
  using type = index_sequence;
};
template <size_t... indices1, size_t... indices2, typename... index_sequences>
struct ConcatIndexSequences<std::index_sequence<indices1...>,
                            std::index_sequence<indices2...>,
                            index_sequences...> {
  using type = typename ConcatIndexSequences<
      std::index_sequence<indices1..., indices2...>, index_sequences...>::type;
};

// Transforms a tuple type to a `std::index_sequence` type of indices of
// elements satisfying a predicate.
template <template <typename...> class Predicate, typename Tuple,
          typename Indices>
struct FilterTypeImpl;

template <template <typename...> class Predicate, typename Tuple,
          size_t... indices>
struct FilterTypeImpl<Predicate, Tuple, std::index_sequence<indices...>>
    : type_traits_internal::ConcatIndexSequences<std::conditional_t<
          Predicate<std::tuple_element_t<indices, Tuple>>::value,
          std::index_sequence<indices>, std::index_sequence<>>...> {};

}  // namespace type_traits_internal

// `GetTypeFromEnd<reverse_index, T...>::type` and
// `GetTypeFromEndT<reverse_index, T...>` extract a type from a parameter pack
// by its index from the end (1 = last).
template <size_t reverse_index, typename... T>
struct GetTypeFromEnd
    : type_traits_internal::GetTypeFromEndImpl<reverse_index, void, T...> {};
template <size_t reverse_index, typename... T>
using GetTypeFromEndT = typename GetTypeFromEnd<reverse_index, T...>::type;

// `GetFromEnd<reverse_index>(args...)` extracts an argument from a sequence of
// arguments by its index from the end (1 = last).
template <size_t reverse_index, typename... Args,
          std::enable_if_t<
              (reverse_index > 0 && reverse_index <= sizeof...(Args)), int> = 0>
inline GetTypeFromEndT<reverse_index, Args&&...> GetFromEnd(Args&&... args) {
  return std::get<sizeof...(Args) - reverse_index>(
      std::tuple<Args&&...>(std::forward<Args>(args)...));
}

// `RemoveTypesFromEnd<num_from_end, T...>::type` and
// `RemoveTypesFromEndT<num_from_end, T...>` transform a parameter pack to a
// `std::tuple` type by removing the given number of elements from the end.
template <size_t num_from_end, typename... T>
struct RemoveTypesFromEnd
    : type_traits_internal::RemoveTypesFromEndImpl<num_from_end, void, T...> {};
template <size_t num_from_end, typename... T>
using RemoveTypesFromEndT =
    typename RemoveTypesFromEnd<num_from_end, T...>::type;

// `RemoveFromEnd<num_from_end>(args...)` transforms a sequence of arguments to
// a `std::tuple` by removing the given number of arguments from the end.
template <size_t num_from_end, typename... Args,
          std::enable_if_t<(num_from_end <= sizeof...(Args)), int> = 0>
inline RemoveTypesFromEndT<num_from_end, Args&&...> RemoveFromEnd(
    Args&&... args) {
  return type_traits_internal::SelectFromTuple(
      std::tuple<Args&&...>(std::forward<Args>(args)...),
      std::make_index_sequence<sizeof...(Args) - num_from_end>());
}

// `ApplyToTupleElements<F, std::tuple<T...>>::type` and
// `ApplyToTupleElementsT<F, std::tuple<T...>>` is `F<T...>`.
template <template <typename... Args> class F, typename Tuple>
struct ApplyToTupleElements;
template <template <typename... Args> class F, typename... T>
struct ApplyToTupleElements<F, std::tuple<T...>> {
  using type = F<T...>;
};
template <template <typename... Args> class F, typename Tuple>
using ApplyToTupleElementsT = typename ApplyToTupleElements<F, Tuple>::type;

// `TupleElementsSatisfy<Tuple, Predicate>::value` checks if all element types
// of a `std::tuple` type satisfy a predicate.
template <typename Tuple, template <typename...> class Predicate>
struct TupleElementsSatisfy;

template <typename... T, template <typename...> class Predicate>
struct TupleElementsSatisfy<std::tuple<T...>, Predicate>
    : absl::conjunction<Predicate<T>...> {};

// `FilterType<Predicate, T...>::type` and
// `FilterTypeT<Predicate, T...>` transform a parameter pack to a `std::tuple`
// type by selecting types satisfying a predicate.
template <template <typename...> class Predicate, typename... T>
struct FilterType
    : type_traits_internal::SelectTypesFromTuple<
          std::tuple<T...>, typename type_traits_internal::FilterTypeImpl<
                                Predicate, std::tuple<T...>,
                                std::index_sequence_for<T...>>::type> {};
template <template <typename...> class Predicate, typename... T>
using FilterTypeT = typename FilterType<Predicate, T...>::type;

// `Filter<Predicate>(args...)` transforms a sequence of arguments to a
// `std::tuple` by selecting types satisfying a predicate.
template <template <typename...> class Predicate, typename... Args>
inline FilterTypeT<Predicate, Args&&...> Filter(Args&&... args) {
  return type_traits_internal::SelectFromTuple(
      std::tuple<Args&&...>(std::forward<Args>(args)...),
      typename type_traits_internal::FilterTypeImpl<
          Predicate, std::tuple<Args&&...>,
          std::index_sequence_for<Args&&...>>::type());
}

// `DecayTupleType<Tuple>::type` and `DecayTupleTypeT<Tuple>` transform a
// `std::tuple` type by decaying all elements from references to values.
template <typename Tuple>
struct DecayTupleType;

template <typename... T>
struct DecayTupleType<std::tuple<T...>> {
  using type = std::tuple<std::decay_t<T>...>;
};
template <typename Tuple>
using DecayTupleTypeT = typename DecayTupleType<Tuple>::type;

// `DecayTuple(tuple)` transforms a `std::tuple` by decaying all elements from
// references to values.
template <typename Tuple>
inline DecayTupleTypeT<Tuple> DecayTuple(Tuple&& tuple) {
  return tuple;
}

#if __cpp_deduction_guides

// `DeduceClassTemplateArguments<Template, Args...>::type` and
// `DeduceClassTemplateArgumentsT<Template, Args...>` deduce class template
// arguments using CTAD from constructor arguments.
//
// Only templates with solely type template parameters are supported.

template <template <typename...> class Template, typename... Args>
struct DeduceClassTemplateArguments {
  using type = decltype(Template(std::declval<Args>()...));
};

template <template <typename...> class Template, typename... Args>
using DeduceClassTemplateArgumentsT =
    typename DeduceClassTemplateArguments<Template, Args...>::type;

#endif

// `IntersectionType<Ts...>::type` and `IntersectionTypeT<Ts...>` compute the
// smallest of unsigned integer types.

namespace type_traits_internal {

template <typename A, typename B, typename Common>
struct IntersectionTypeImpl;

template <typename A, typename B>
struct IntersectionTypeImpl<A, B, A> {
  using type = B;
};

template <typename A, typename B>
struct IntersectionTypeImpl<A, B, B> {
  using type = A;
};

template <typename A>
struct IntersectionTypeImpl<A, A, A> {
  using type = A;
};

}  // namespace type_traits_internal

template <typename... T>
struct IntersectionType;

template <typename... T>
using IntersectionTypeT = typename IntersectionType<T...>::type;

template <typename A>
struct IntersectionType<A> {
  using type = A;
};

template <typename A, typename B>
struct IntersectionType<A, B>
    : type_traits_internal::IntersectionTypeImpl<A, B,
                                                 std::common_type_t<A, B>> {};

template <typename A, typename B, typename... Rest>
struct IntersectionType<A, B, Rest...>
    : IntersectionType<IntersectionTypeT<A, B>, Rest...> {};

// Let unqualified `begin()` below refer either to a function named `begin()`
// found via ADL or to `std::begin()`, as appropriate for the given iterable.
// This is done in a separate namespace to avoid introducing a name
// `riegeli::begin()`.
namespace adl_begin_sandbox {

using std::begin;

template <typename T>
using IteratorT = decltype(begin(std::declval<T&>()));

template <typename T>
using DereferenceIterableT = decltype(*begin(std::declval<T&>()));

template <typename T>
using DereferenceIterableFirstT = decltype(begin(std::declval<T&>())->first);

template <typename T>
using DereferenceIterableSecondT = decltype(begin(std::declval<T&>())->second);

}  // namespace adl_begin_sandbox

// `IsIterableOf<Iterable, Element>::value` is `true` if iterating over
// `Iterable` yields elements convertible to `Element`.

template <typename Iterable, typename Element, typename Enable = void>
struct IsIterableOf : std::false_type {};

template <typename Iterable, typename Element>
struct IsIterableOf<
    Iterable, Element,
    std::enable_if_t<std::is_convertible<
        adl_begin_sandbox::DereferenceIterableT<Iterable>, Element>::value>>
    : std::true_type {};

// `IsForwardIterable<Iterable>::value` is `true` if the iterator over
// `Iterable` is a forward iterator (supporting multiple passes).

template <typename Iterable, typename Enable = void>
struct IsForwardIterable : std::false_type {};

template <typename Iterable>
struct IsForwardIterable<
    Iterable, std::enable_if_t<std::is_convertible<
                  typename std::iterator_traits<adl_begin_sandbox::IteratorT<
                      Iterable>>::iterator_category,
                  std::forward_iterator_tag>::value>> : std::true_type {};

// `IsRandomAccessIterable<Iterable>::value` is `true` if the iterator over
// `Iterable` is a random access iterator.

template <typename Iterable, typename Enable = void>
struct IsRandomAccessIterable : std::false_type {};

template <typename Iterable>
struct IsRandomAccessIterable<
    Iterable, std::enable_if_t<std::is_convertible<
                  typename std::iterator_traits<adl_begin_sandbox::IteratorT<
                      Iterable>>::iterator_category,
                  std::random_access_iterator_tag>::value>> : std::true_type {};

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
            adl_begin_sandbox::DereferenceIterableFirstT<Iterable>, Key>,
        std::is_assignable<
            adl_begin_sandbox::DereferenceIterableSecondT<Iterable>,
            Value>>::value>> : std::true_type {};

// `HasMovableElements<Iterable>::value` is `true` if moving (rather than
// copying) out of elements of `Iterable` is safe.
template <typename Iterable, typename Enable = void>
struct HasMovableElements : std::false_type {};

// Moving out of elements of `Iterable` is unsafe if it is an lvalue, or a view
// container like `absl::Span<T>`. View containers are detected by checking
// whether iterating over `Iterable&` and `const Iterable&` yields elements of
// the same type.
//
// This also catches cases where `Iterable` always yields const elements, where
// moving would be equivalent to copying, and trying to move would just yield
// unnecessarily separate template instantiations.
template <typename Iterable>
struct HasMovableElements<
    Iterable,
    std::enable_if_t<absl::conjunction<
        absl::negation<std::is_lvalue_reference<Iterable>>,
        absl::negation<std::is_same<
            adl_begin_sandbox::DereferenceIterableT<std::decay_t<Iterable>>,
            adl_begin_sandbox::DereferenceIterableT<
                const std::decay_t<Iterable>>>>>::value>> : std::true_type {};

// `MaybeMoveElement<Src>(element)` is `std::move(element)` or `element`,
// depending on whether moving out of elements of `Src` is safe.

template <typename Src, typename Element,
          std::enable_if_t<!HasMovableElements<Src>::value, int> = 0>
inline Element&& MaybeMoveElement(Element&& element) {
  return std::forward<Element>(element);
}

template <typename Src, typename Element,
          std::enable_if_t<HasMovableElements<Src>::value, int> = 0>
inline std::remove_reference_t<Element>&& MaybeMoveElement(Element&& element) {
  return static_cast<std::remove_reference_t<Element>&&>(element);
}

// `MaybeMakeMoveIterator<Src>(iterator)` is `std::make_move_iterator(iterator)`
// or `iterator`, depending on whether moving out of elements of `Src` is safe.

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

namespace type_traits_internal {

class UnimplementedSink {
 public:
  void Append(size_t length, char src);
  void Append(absl::string_view src);
  friend void AbslFormatFlush(UnimplementedSink* dest, absl::string_view src);
};

}  // namespace type_traits_internal

// Checks if the type supports stringification using `AbslStringify()`.
template <typename T, typename Enable = void>
struct HasAbslStringify : std::false_type {};
template <typename T>
struct HasAbslStringify<
    T, absl::void_t<decltype(AbslStringify(
           std::declval<type_traits_internal::UnimplementedSink&>(),
           std::declval<const T&>()))>> : std::true_type {};

// Deriving a class from `ConditionallyCopyable<is_copyable>` disables default
// copy and move constructor and assignment if `!is_copyable`.
//
// A derived class should either make desired copy and move constructor and
// assignment explicitly defaulted, so that they get effectively defaulted or
// deleted depending on `is_copyable`, or leave them out to make them implicitly
// defined, with the same effect.
//
// An explicit definition of copy and move constructor and assignment, which
// does not refer to the corresponding operation of the base class, would
// circumvent the intent of `!is_copyable`.

template <bool is_copyable>
class ConditionallyCopyable {};

template <>
class ConditionallyCopyable<false> {
 public:
  ConditionallyCopyable() = default;

  ConditionallyCopyable(const ConditionallyCopyable&) = delete;
  ConditionallyCopyable& operator=(const ConditionallyCopyable&) = delete;
};

}  // namespace riegeli

#endif  // RIEGELI_BASE_TYPE_TRAITS_H_
