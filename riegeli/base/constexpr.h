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

#include "absl/base/nullability.h"
#include "riegeli/base/port.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

// Returns `true` if the value of the expression is known at compile time.
#if RIEGELI_INTERNAL_HAS_BUILTIN(__builtin_constant_p) || \
    RIEGELI_INTERNAL_IS_GCC_VERSION(3, 1)
#define RIEGELI_IS_CONSTANT(expr) __builtin_constant_p(expr)
#else
#define RIEGELI_IS_CONSTANT(expr) false
#endif

}  // namespace riegeli

#endif  // RIEGELI_BASE_CONSTEXPR_H_
