// Copyright 2023 Google LLC
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

#ifndef RIEGELI_GCS_GCS_INTERNAL_H_
#define RIEGELI_GCS_GCS_INTERNAL_H_

#include <stddef.h>

#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/status/status.h"
#include "google/cloud/status.h"

namespace riegeli::gcs_internal {

inline absl::Status FromCloudStatus(const google::cloud::Status& status) {
  return absl::Status(static_cast<absl::StatusCode>(status.code()),
                      status.message());
}

template <typename T>
T GetOption() {
  return T();
}
template <typename T>
const T& GetOption(const T& option) {
  return option;
}
template <typename T, typename... Options>
const T& GetOption(const T& /*previous_option*/, const T& option,
                   const Options&... options) {
  return GetOption<T>(option, options...);
}
template <typename T, typename Other, typename... Options>
const T& GetOption(const T& option, const Other& /*other_option*/,
                   const Options&... options) {
  return GetOption<T>(option, options...);
}
template <typename T, typename Other, typename... Options>
auto GetOption(const Other& /*other_option*/, const Options&... options) {
  return GetOption<T>(options...);
}

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
    [[maybe_unused]] Tuple&& tuple, std::index_sequence<indices...>) {
  return {std::forward<std::tuple_element_t<indices, Tuple>>(
      std::get<indices>(tuple))...};
}

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
    : ConcatIndexSequences<std::conditional_t<
          Predicate<std::tuple_element_t<indices, Tuple>>::value,
          std::index_sequence<indices>, std::index_sequence<>>...> {};

// `FilterType<Predicate, T...>::type` and `FilterTypeT<Predicate, T...>`
// transform a parameter pack to a `std::tuple` type by selecting types
// satisfying a predicate.
template <template <typename...> class Predicate, typename... T>
struct FilterType
    : SelectTypesFromTuple<
          std::tuple<T...>,
          typename FilterTypeImpl<Predicate, std::tuple<T...>,
                                  std::index_sequence_for<T...>>::type> {};
template <template <typename...> class Predicate, typename... T>
using FilterTypeT = typename FilterType<Predicate, T...>::type;

// `Filter<Predicate>(args...)` transforms a sequence of arguments to a
// `std::tuple` by selecting types satisfying a predicate.
template <template <typename...> class Predicate, typename... Args>
inline FilterTypeT<Predicate, Args&&...> Filter(Args&&... args) {
  return SelectFromTuple(
      std::tuple<Args&&...>(std::forward<Args>(args)...),
      typename FilterTypeImpl<Predicate, std::tuple<Args&&...>,
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

}  // namespace riegeli::gcs_internal

#endif  // RIEGELI_GCS_GCS_INTERNAL_H_
