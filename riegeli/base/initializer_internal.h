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

#include <stddef.h>

#include <type_traits>
#include <utility>

#include "absl/base/casts.h"
#include "absl/base/nullability.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli::initializer_internal {

// `CanBindReference<T&&, Arg&&>::value` is `true` if `Arg&&` can be implicitly
// converted to `T&&` without creating a temporary.
//
// Due to not all compilers implementing http://wg21.link/cwg2352 (converting
// `T*&` to `const T* const&` could have bound the result to a temporary),
// this covers also the case when the corresponding pointers can be converted.
// `BindReference()` should be used for the actual conversion.

template <typename T, typename Arg>
struct CanBindReference : std::false_type {};

template <typename T, typename Arg>
struct CanBindReference<T&, Arg&> : std::is_convertible<Arg*, T*> {};

template <typename T, typename Arg>
struct CanBindReference<T&, Arg&&> : std::false_type {};

template <typename T, typename Arg>
struct CanBindReference<const T&, Arg&&> : std::is_convertible<Arg*, const T*> {
};

template <typename T, typename Arg>
struct CanBindReference<T&&, Arg&> : std::false_type {};

template <typename T, typename Arg>
struct CanBindReference<T&&, Arg&&> : std::is_convertible<Arg*, T*> {};

// `BindReference<T&&>(arg)` returns `arg` implicitly converted to `T&&`.
//
// Due to not all compilers implementing http://wg21.link/cwg2352 (converting
// `T*&` to `const T* const&` could have bound the result to a temporary),
// this is not implemented as a simple implicit conversion, but by converting
// the reference to a pointer, implicitly converting the pointer, and
// dereferencing back.
template <typename T, typename Arg,
          std::enable_if_t<CanBindReference<T&&, Arg&&>::value, int> = 0>
inline T&& BindReference(Arg&& arg) {
  return std::forward<T>(
      *absl::implicit_cast<std::remove_reference_t<T>*>(&arg));
}

}  // namespace riegeli::initializer_internal

#endif  // RIEGELI_BASE_INITIALIZER_INTERNAL_H_
