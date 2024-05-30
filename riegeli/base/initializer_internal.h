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

namespace riegeli {
namespace initializer_internal {

// `CanBindTo<T&&, Args&&...>::value` is `true` if constructing `T(args...)`
// with `args...` of type `Args&&...` can be elided, with `T&&` binding directly
// to the only element of `args...` instead.

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
