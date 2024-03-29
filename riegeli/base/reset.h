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

#include "absl/meta/type_traits.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"

namespace riegeli {

// To make an existing `object` of some class `T` equivalent to a newly
// constructed `T`, certain classes use a convention of providing member
// functions `Reset()`, which mirror constructors of these classes (except for
// copy and move constructors which are mirrored by the assignment operators).
// This avoids constructing a temporary `T` and moving from it.
//
// If it is not known whether the given class provides member functions
// `Reset()`, generic code can use `riegeli::Reset(object, args...)`.
// This calls the first defined form among the following:
//  * `object = args...` (if `args...` has a single element)
//  * `RiegeliReset(object, args...)`
//  * `object.Reset(args...)`
//  * `object = T(args...)`
//
// Hence to customize `riegeli::Reset()` for a class `T`, define overloads of
// either a member function `void T::Reset(...)`, or a free function
// `friend void RiegeliReset(T& self, ...)` as a friend of `T` inside class
// definition or in the same namespace as `T`, so that it can be found via ADL.
//
// `RiegeliReset()` is predefined for `std::string` and `absl::Cord`.

inline void RiegeliReset(std::string& self) { self.clear(); }

inline void RiegeliReset(std::string& self, size_t length, char ch) {
  self.assign(length, ch);
}

inline void RiegeliReset(std::string& self, absl::string_view src) {
  // TODO: When `absl::string_view` becomes C++17 `std::string_view`:
  // `self.assign(src)`
  self.assign(src.data(), src.size());
}

inline void RiegeliReset(std::string& self, const char* src) {
  self.assign(src);
}

inline void RiegeliReset(std::string& self, const char* src, size_t length) {
  self.assign(src, length);
}

inline void RiegeliReset(absl::Cord& self) { self.Clear(); }

inline void RiegeliReset(absl::Cord& self, absl::string_view src) {
  self = src;
}

namespace reset_internal {

template <typename T, typename Enable, typename... Args>
struct HasAssignment : std::false_type {};

template <typename T, typename Arg>
struct HasAssignment<
    T, absl::void_t<decltype(std::declval<T&>() = std::declval<Arg>())>, Arg>
    : std::true_type {};

template <typename T, typename Enable, typename... Args>
struct HasRiegeliReset : std::false_type {};

template <typename T, typename... Args>
struct HasRiegeliReset<T,
                       absl::void_t<decltype(RiegeliReset(
                           std::declval<T&>(), std::declval<Args>()...))>,
                       Args...> : std::true_type {};

template <typename T, typename Enable, typename... Args>
struct HasReset : std::false_type {};

template <typename T, typename... Args>
struct HasReset<
    T,
    absl::void_t<decltype(std::declval<T&>().Reset(std::declval<Args>()...))>,
    Args...> : std::true_type {};

}  // namespace reset_internal

template <typename T, typename Arg,
          std::enable_if_t<reset_internal::HasAssignment<T, void, Arg>::value,
                           int> = 0>
inline void Reset(T& object, Arg&& arg) {
  object = std::forward<Arg>(arg);
}

template <
    typename T, typename... Args,
    std::enable_if_t<
        absl::conjunction<
            absl::negation<reset_internal::HasAssignment<T, void, Args...>>,
            reset_internal::HasRiegeliReset<T, void, Args...>>::value,
        int> = 0>
inline void Reset(T& object, Args&&... args) {
  RiegeliReset(object, std::forward<Args>(args)...);
}

template <
    typename T, typename... Args,
    std::enable_if_t<
        absl::conjunction<
            absl::negation<reset_internal::HasAssignment<T, void, Args...>>,
            absl::negation<reset_internal::HasRiegeliReset<T, void, Args...>>,
            reset_internal::HasReset<T, void, Args...>>::value,
        int> = 0>
inline void Reset(T& object, Args&&... args) {
  object.Reset(std::forward<Args>(args)...);
}

template <
    typename T, typename... Args,
    std::enable_if_t<
        absl::conjunction<
            absl::negation<reset_internal::HasAssignment<T, void, Args...>>,
            absl::negation<reset_internal::HasRiegeliReset<T, void, Args...>>,
            absl::negation<reset_internal::HasReset<T, void, Args...>>>::value,
        int> = 0>
inline void Reset(T& object, Args&&... args) {
  object = T(std::forward<Args>(args)...);
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_RESET_H_
