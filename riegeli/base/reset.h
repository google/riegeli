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

#ifndef RIEGELI_BASE_RESET_H_
#define RIEGELI_BASE_RESET_H_

#include <stddef.h>

#include <type_traits>
#include <utility>

#include "absl/base/nullability.h"
#include "absl/strings/cord.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

// To make an existing `dest` object of some class `T` equivalent to a newly
// constructed `T`, certain classes use a convention of providing member
// functions `Reset()`, which mirror constructors of these classes (except for
// copy and move constructors which are mirrored by the assignment operators).
// This avoids constructing a temporary `T` and moving from it.
//
// Other conventions exist as well. Generic `riegeli::Reset(dest, args...)`
// employs them by calling the first defined form among the following:
//  * `RiegeliReset(dest, args...)`
//  * `dest.Reset(args...)`
//  * `dest.emplace(args...)` - if `args...` are preceded by `std::in_place`
//  * `dest.emplace<Type>(args...)` - if `args...` are preceded by
//     `std::in_place_type<Type>`
//  * `dest.emplace<index>(args...)` - if `args...` are preceded by
//     `std::in_place_index<index>`
//  * `dest.clear()` - if `args...` are empty; `begin()` and `end()` must be
//     defined because this pattern would be risky for non-containers
//  * `dest.assign(args...)` - `begin()` and `end()` must be defined
//  * `dest = args...` - if `args...` has a single element
//  * `dest = T(args...)`
//
// Hence to customize `riegeli::Reset()` for a class `T`, define overloads
// of either a member function `void T::Reset(...)`, or a free function
// `friend void RiegeliReset(T& dest, ...)` as a friend of `T` inside class
// definition or in the same namespace as `T`, so that it can be found via ADL.
//
// Resetting is not necessarily fully equivalent to assigning from a newly
// constructed object. They can differ in:
//  * Assigning from objects previously owned by `dest` (unsafe for resetting).
//  * Exception safety (resetting is not transactional).
//  * Reusing existing memory allocations (common for resetting).
//  * Non-salient attributes like `capacity()`.

inline void RiegeliReset(absl::Cord& dest) { dest.Clear(); }

namespace reset_internal {

template <typename T, typename Enable, typename... Args>
struct HasRiegeliResetImpl : std::false_type {};

template <typename T, typename... Args>
struct HasRiegeliResetImpl<T,
                           std::void_t<decltype(RiegeliReset(
                               std::declval<T&>(), std::declval<Args>()...))>,
                           Args...> : std::true_type {};

template <typename T, typename... Args>
struct HasRiegeliReset : HasRiegeliResetImpl<T, void, Args...> {};

template <typename T, typename Enable, typename... Args>
struct HasResetImpl : std::false_type {};

template <typename T, typename... Args>
struct HasResetImpl<
    T, std::void_t<decltype(std::declval<T&>().Reset(std::declval<Args>()...))>,
    Args...> : std::true_type {};

template <typename T, typename... Args>
struct HasReset : HasResetImpl<T, void, Args...> {};

template <typename T, typename Enable, typename... Args>
struct HasEmplaceImpl : std::false_type {};

template <typename T, typename... Args>
struct HasEmplaceImpl<
    T,
    std::void_t<decltype(std::declval<T&>().emplace(std::declval<Args>()...))>,
    std::in_place_t, Args...> : std::true_type {};

template <typename T, typename... Args>
struct HasEmplaceImpl<
    T,
    std::void_t<decltype(std::declval<T&>().emplace(std::declval<Args>()...))>,
    const std::in_place_t&, Args...> : std::true_type {};

template <typename T, typename... Args>
struct HasEmplace : HasEmplaceImpl<T, void, Args...> {};

template <typename T, typename Enable, typename... Args>
struct HasEmplaceWithTypeImpl : std::false_type {};

template <typename T, typename Type, typename... Args>
struct HasEmplaceWithTypeImpl<
    T,
    std::void_t<decltype(std::declval<T&>().template emplace<Type>(
        std::declval<Args>()...))>,
    std::in_place_type_t<Type>, Args...> : std::true_type {};

template <typename T, typename Type, typename... Args>
struct HasEmplaceWithTypeImpl<
    T,
    std::void_t<decltype(std::declval<T&>().template emplace<Type>(
        std::declval<Args>()...))>,
    const std::in_place_type_t<Type>&, Args...> : std::true_type {};

template <typename T, typename... Args>
struct HasEmplaceWithType : HasEmplaceWithTypeImpl<T, void, Args...> {};

template <typename T, typename Enable, typename... Args>
struct HasEmplaceWithIndexImpl : std::false_type {};

template <typename T, size_t index, typename... Args>
struct HasEmplaceWithIndexImpl<
    T,
    std::void_t<decltype(std::declval<T&>().template emplace<index>(
        std::declval<Args>()...))>,
    std::in_place_index_t<index>, Args...> : std::true_type {};

template <typename T, size_t index, typename... Args>
struct HasEmplaceWithIndexImpl<
    T,
    std::void_t<decltype(std::declval<T&>().template emplace<index>(
        std::declval<Args>()...))>,
    const std::in_place_index_t<index>&, Args...> : std::true_type {};

template <typename T, typename... Args>
struct HasEmplaceWithIndex : HasEmplaceWithIndexImpl<T, void, Args...> {};

template <typename T, typename Enable, typename... Args>
struct HasClearImpl : std::false_type {};

template <typename T>
struct HasClearImpl<T, std::void_t<decltype(std::declval<T&>().clear()),
                                   decltype(std::declval<T&>().begin()),
                                   decltype(std::declval<T&>().end())>>
    : std::true_type {};

template <typename T, typename... Args>
struct HasClear : HasClearImpl<T, void, Args...> {};

template <typename T, typename Enable, typename... Args>
struct HasAssignImpl : std::false_type {};

template <typename T, typename... Args>
struct HasAssignImpl<
    T,
    std::void_t<decltype(std::declval<T&>().assign(std::declval<Args>()...)),
                decltype(std::declval<T&>().begin()),
                decltype(std::declval<T&>().end())>,
    Args...> : std::true_type {};

template <typename T, typename... Args>
struct HasAssign : HasAssignImpl<T, void, Args...> {};

template <typename T, typename Enable, typename... Args>
struct HasAssignmentImpl : std::false_type {};

template <typename T, typename Arg>
struct HasAssignmentImpl<
    T, std::void_t<decltype(std::declval<T&>() = std::declval<Arg>())>, Arg>
    : std::true_type {};

template <typename T, typename... Args>
struct HasAssignment : HasAssignmentImpl<T, void, Args...> {};

template <typename T, typename... Args>
inline void CallEmplace(T& dest, std::in_place_t, Args&&... args) {
  dest.emplace(std::forward<Args>(args)...);
}

template <typename T, typename Type, typename... Args>
inline void CallEmplace(T& dest, std::in_place_type_t<Type>, Args&&... args) {
  dest.template emplace<Type>(std::forward<Args>(args)...);
}

template <typename T, size_t index, typename... Args>
inline void CallEmplace(T& dest, std::in_place_index_t<index>, Args&&... args) {
  dest.template emplace<index>(std::forward<Args>(args)...);
}

}  // namespace reset_internal

// `SupportsReset<T, Args...>::value` is true if `riegeli::Reset(T&, Args...)`
// is supported.
template <typename T, typename... Args>
struct SupportsReset
    : std::disjunction<reset_internal::HasRiegeliReset<T, Args...>,
                       reset_internal::HasReset<T, Args...>,
                       reset_internal::HasEmplace<T, Args...>,
                       reset_internal::HasEmplaceWithType<T, Args...>,
                       reset_internal::HasEmplaceWithIndex<T, Args...>,
                       reset_internal::HasClear<T, Args...>,
                       reset_internal::HasAssign<T, Args...>,
                       reset_internal::HasAssignment<T, Args...>,
                       std::conjunction<std::is_constructible<T, Args...>,
                                        std::is_move_assignable<T>>> {};

template <typename T, typename... Args,
          std::enable_if_t<SupportsReset<T, Args...>::value, int> = 0>
inline void Reset(T& dest, Args&&... args) {
  if constexpr (reset_internal::HasRiegeliReset<T, Args...>::value) {
    RiegeliReset(dest, std::forward<Args>(args)...);
  } else if constexpr (reset_internal::HasReset<T, Args...>::value) {
    dest.Reset(std::forward<Args>(args)...);
  } else if constexpr (std::disjunction_v<
                           reset_internal::HasEmplace<T, Args...>,
                           reset_internal::HasEmplaceWithType<T, Args...>,
                           reset_internal::HasEmplaceWithIndex<T, Args...>>) {
    reset_internal::CallEmplace(dest, std::forward<Args>(args)...);
  } else if constexpr (reset_internal::HasClear<T, Args...>::value) {
    static_assert(sizeof...(Args) == 0, "Implied by HasClear");
    dest.clear();
  } else if constexpr (reset_internal::HasAssign<T, Args...>::value) {
    dest.assign(std::forward<Args>(args)...);
  } else if constexpr (reset_internal::HasAssignment<T, Args...>::value) {
    static_assert(sizeof...(Args) == 1, "Implied by HasAssignment");
    dest = std::forward<Args...>(args...);
  } else if constexpr (std::conjunction_v<std::is_constructible<T, Args...>,
                                          std::is_move_assignable<T>>) {
    dest = T(std::forward<Args>(args)...);
  } else {
    static_assert(std::is_void_v<std::void_t<T>>, "Implied by SupportsReset");
  }
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_RESET_H_
