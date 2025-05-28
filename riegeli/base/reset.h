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

#include <string>
#include <type_traits>
#include <utility>

#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"

namespace riegeli {

// To make an existing `dest` object of some class `T` equivalent to a newly
// constructed `T`, certain classes use a convention of providing member
// functions `Reset()`, which mirror constructors of these classes (except for
// copy and move constructors which are mirrored by the assignment operators).
// This avoids constructing a temporary `T` and moving from it.
//
// If it is not known whether the given class provides member functions
// `Reset()`, generic code can use `riegeli::Reset(dest, args...)`.
// This calls the first defined form among the following:
//  * `RiegeliReset(dest, args...)`
//  * `dest.Reset(args...)`
//  * `dest = args...` (if `args...` has a single element)
//  * `dest = T(args...)`
//
// Hence to customize `riegeli::Reset()` for a class `T`, define overloads of
// either a member function `void T::Reset(...)`, or a free function
// `friend void RiegeliReset(T& dest, ...)` as a friend of `T` inside class
// definition or in the same namespace as `T`, so that it can be found via ADL.
//
// `RiegeliReset()` is predefined for `std::string` and `absl::Cord`.

inline void RiegeliReset(std::string& dest) { dest.clear(); }

inline void RiegeliReset(std::string& dest, size_t size, char fill) {
  dest.assign(size, fill);
}

inline void RiegeliReset(std::string& dest, absl::string_view src) {
  // TODO: When `absl::string_view` becomes C++17 `std::string_view`:
  // `dest.assign(src)`
  dest.assign(src.data(), src.size());
}

inline void RiegeliReset(std::string& dest, const char* src) {
  dest.assign(src);
}

inline void RiegeliReset(std::string& dest, const char* src, size_t length) {
  dest.assign(src, length);
}

inline void RiegeliReset(absl::Cord& dest) { dest.Clear(); }

inline void RiegeliReset(absl::Cord& dest, absl::string_view src) {
  dest = src;
}

namespace reset_internal {

template <typename Enable, typename T, typename... Args>
struct HasRiegeliResetImpl : std::false_type {};

template <typename T, typename... Args>
struct HasRiegeliResetImpl<std::void_t<decltype(RiegeliReset(
                               std::declval<T&>(), std::declval<Args>()...))>,
                           T, Args...> : std::true_type {};

template <typename T, typename... Args>
struct HasRiegeliReset : HasRiegeliResetImpl<void, T, Args...> {};

template <typename Enable, typename T, typename... Args>
struct HasResetImpl : std::false_type {};

template <typename T, typename... Args>
struct HasResetImpl<
    std::void_t<decltype(std::declval<T&>().Reset(std::declval<Args>()...))>, T,
    Args...> : std::true_type {};

template <typename T, typename... Args>
struct HasReset : HasResetImpl<void, T, Args...> {};

template <typename Enable, typename T, typename... Args>
struct HasAssignmentImpl : std::false_type {};

template <typename T, typename Arg>
struct HasAssignmentImpl<
    std::void_t<decltype(std::declval<T&>() = std::declval<Arg>())>, T, Arg>
    : std::true_type {};

template <typename T, typename... Args>
struct HasAssignment : HasAssignmentImpl<void, T, Args...> {};

}  // namespace reset_internal

// `SupportsReset<T, Args...>::value` is true if `riegeli::Reset(T&, Args...)`
// is supported.
template <typename T, typename... Args>
struct SupportsReset
    : std::disjunction<reset_internal::HasRiegeliReset<T, Args...>,
                       reset_internal::HasReset<T, Args...>,
                       reset_internal::HasAssignment<T, Args...>,
                       std::conjunction<std::is_constructible<T, Args...>,
                                        std::is_move_assignable<T>>> {};

template <typename T, typename... Args,
          std::enable_if_t<reset_internal::HasRiegeliReset<T, Args...>::value,
                           int> = 0>
inline void Reset(T& dest, Args&&... args) {
  RiegeliReset(dest, std::forward<Args>(args)...);
}

template <typename T, typename... Args,
          std::enable_if_t<
              std::conjunction_v<
                  std::negation<reset_internal::HasRiegeliReset<T, Args...>>,
                  reset_internal::HasReset<T, Args...>>,
              int> = 0>
inline void Reset(T& dest, Args&&... args) {
  dest.Reset(std::forward<Args>(args)...);
}

template <
    typename T, typename Arg,
    std::enable_if_t<std::conjunction_v<
                         std::negation<reset_internal::HasRiegeliReset<T, Arg>>,
                         std::negation<reset_internal::HasReset<T, Arg>>,
                         reset_internal::HasAssignment<T, Arg>>,
                     int> = 0>
inline void Reset(T& dest, Arg&& arg) {
  dest = std::forward<Arg>(arg);
}

template <
    typename T, typename... Args,
    std::enable_if_t<
        std::conjunction_v<
            std::negation<reset_internal::HasRiegeliReset<T, Args...>>,
            std::negation<reset_internal::HasReset<T, Args...>>,
            std::negation<reset_internal::HasAssignment<T, Args...>>,
            std::is_constructible<T, Args...>, std::is_move_assignable<T>>,
        int> = 0>
inline void Reset(T& dest, Args&&... args) {
  dest = T(std::forward<Args>(args)...);
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_RESET_H_
