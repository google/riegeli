// Copyright 2018 Google LLC
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

#ifndef RIEGELI_BASE_CONSTEXPR_H_
#define RIEGELI_BASE_CONSTEXPR_H_

#include "riegeli/base/port.h"
#include "riegeli/base/type_traits.h"  // IWYU pragma: keep

namespace riegeli {

// `RIEGELI_INLINE_CONSTEXPR(type, name, init)` emulates namespace-scope
// `inline constexpr type name = init;` from C++17, but is available since
// C++14.

#if __cpp_inline_variables

#define RIEGELI_INLINE_CONSTEXPR(type, name, init) \
  inline constexpr ::riegeli::type_identity_t<type> name = init

#else

#define RIEGELI_INLINE_CONSTEXPR(type, name, init)                \
  template <typename RiegeliInternalDummy>                        \
  constexpr ::riegeli::type_identity_t<type>                      \
      kRiegeliInternalInlineConstexpr_##name = init;              \
  static constexpr const ::riegeli::type_identity_t<type>& name = \
      kRiegeliInternalInlineConstexpr_##name<void>;               \
  static_assert(sizeof(name) != 0, "Silence unused variable warnings.")

#endif

// Returns `true` if the value of the expression is known at compile time.
#if RIEGELI_INTERNAL_HAS_BUILTIN(__builtin_constant_p) || \
    RIEGELI_INTERNAL_IS_GCC_VERSION(3, 1)
#define RIEGELI_IS_CONSTANT(expr) __builtin_constant_p(expr)
#else
#define RIEGELI_IS_CONSTANT(expr) false
#endif

}  // namespace riegeli

#endif  // RIEGELI_BASE_CONSTEXPR_H_
