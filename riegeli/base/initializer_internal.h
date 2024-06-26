// Copyright 2024 Google LLC
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

#ifndef RIEGELI_BASE_INITIALIZER_INTERNAL_H_
#define RIEGELI_BASE_INITIALIZER_INTERNAL_H_

#include <type_traits>

#include "absl/meta/type_traits.h"

namespace riegeli {
namespace initializer_internal {

// In `Maker()`, pass arguments by reference unless they are cheap to pass by
// value.

template <typename T, typename Enable = void>
struct ReferenceOrCheapValue {
  using type = T&&;
};

template <typename T>
struct ReferenceOrCheapValue<
    T, std::enable_if_t<absl::conjunction<
           std::is_trivially_copyable<T>, std::is_trivially_destructible<T>,
           std::integral_constant<bool,
                                  (sizeof(T) <= 2 * sizeof(void*))>>::value>> {
  using type = T;
};

template <typename T>
using ReferenceOrCheapValueT = typename ReferenceOrCheapValue<T>::type;

// `CanBindTo<T&&, Args&&...>::value` is `true` if constructing `T(args...)`
// with `args...` of type `Args&&...` can be elided, with `T&&` binding directly
// to the only element of `args...` instead.
//
// Due to not all compilers implementing http://wg21.link/cwg2352 (converting
// `T*&` to `const T* const&` could have bound the result to a temporary),
// binding a const reference should be done by converting between the
// corresponding pointer types and then dereferencing.

template <typename T, typename... Args>
struct CanBindTo : std::false_type {};

template <typename T, typename Arg>
struct CanBindTo<T&, Arg&> : std::is_convertible<Arg*, T*> {};

template <typename T, typename Arg>
struct CanBindTo<T&, Arg&&> : std::false_type {};

template <typename T, typename Arg>
struct CanBindTo<const T&, Arg&&> : std::is_convertible<Arg*, const T*> {};

template <typename T, typename Arg>
struct CanBindTo<T&&, Arg&> : std::false_type {};

template <typename T, typename Arg>
struct CanBindTo<T&&, Arg&&> : std::is_convertible<Arg*, T*> {};

}  // namespace initializer_internal
}  // namespace riegeli

#endif  // RIEGELI_BASE_INITIALIZER_INTERNAL_H_
