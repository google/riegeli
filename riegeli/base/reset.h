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
// `Reset()`, generic code can use `riegeli::Reset(object, args...)`. This calls
// the first defined form among the following:
//  * `ResetInternal(object, args...)`
//  * `object.Reset(args...)`
//  * `object = T(args...)`
//
// As special cases, `riegeli::Reset(object, src)` for `src` of type `const T&`
// or `T&&` simply calls `object = src` or `object = std::move(src)`.
//
// Hence to customize `riegeli::Reset()` for a class `T`, either define member
// functions `T::Reset()`, or define free functions `ResetInternal()` in the
// same namespace as `T`, so that they are found via ADL.
//
// `ResetInternal()` is predefined for `std::string` and `absl::Cord`.

template <typename T>
void Reset(T& object, const T& src);

template <typename T>
void Reset(T& object, T&& src);

template <typename T, typename... Args>
void Reset(T& object, Args&&... args);

// Implementation details follow.

inline void ResetInternal(std::string& object) { object.clear(); }

inline void ResetInternal(std::string& object, size_t length, char ch) {
  object.assign(length, ch);
}

inline void ResetInternal(std::string& object, absl::string_view src) {
  // TODO: When `absl::string_view` becomes C++17 `std::string_view`:
  // `object.assign(src)`
  object.assign(src.data(), src.size());
}

inline void ResetInternal(std::string& object, const char* src) {
  object.assign(src);
}

inline void ResetInternal(std::string& object, const char* src, size_t length) {
  object.assign(src, length);
}

inline void ResetInternal(absl::Cord& object) { object.Clear(); }

inline void ResetInternal(absl::Cord& object, absl::string_view src) {
  object = src;
}

namespace reset_internal {

template <typename T, typename Enable, typename... Args>
struct HasResetInternal : std::false_type {};

template <typename T, typename... Args>
struct HasResetInternal<T,
                        absl::void_t<decltype(ResetInternal(
                            std::declval<T&>(), std::declval<Args>()...))>,
                        Args...> : std::true_type {};

template <typename T, typename Enable, typename... Args>
struct HasReset : std::false_type {};

template <typename T, typename... Args>
struct HasReset<
    T,
    absl::void_t<decltype(std::declval<T&>().Reset(std::declval<Args>()...))>,
    Args...> : std::true_type {};

template <typename T, typename... Args,
          std::enable_if_t<HasResetInternal<T, void, Args...>::value, int> = 0>
inline void ResetImpl(T& object, Args&&... args) {
  ResetInternal(object, std::forward<Args>(args)...);
}

template <typename T, typename... Args,
          std::enable_if_t<!HasResetInternal<T, void, Args...>::value &&
                               HasReset<T, void, Args...>::value,
                           int> = 0>
inline void ResetImpl(T& object, Args&&... args) {
  object.Reset(std::forward<Args>(args)...);
}

template <typename T, typename... Args,
          std::enable_if_t<!HasResetInternal<T, void, Args...>::value &&
                               !HasReset<T, void, Args...>::value,
                           int> = 0>
inline void ResetImpl(T& object, Args&&... args) {
  object = T(std::forward<Args>(args)...);
}

}  // namespace reset_internal

template <typename T>
inline void Reset(T& object, const T& src) {
  object = src;
}

template <typename T>
inline void Reset(T& object, T&& src) {
  object = std::move(src);
}

template <typename T, typename... Args>
inline void Reset(T& object, Args&&... args) {
  reset_internal::ResetImpl(object, std::forward<Args>(args)...);
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_RESET_H_
