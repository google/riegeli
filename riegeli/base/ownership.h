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

#ifndef RIEGELI_BASE_OWNERSHIP_H_
#define RIEGELI_BASE_OWNERSHIP_H_

#include <type_traits>

#include "riegeli/base/constexpr.h"

namespace riegeli {

// `PassOwnership` and `ShareOwnership` type tags specify how ownership of a
// potentially shared object is transferred, for cases when this is not implied
// by parameter types.
//
//  * `PassOwnership`: the original owner drops its reference. The reference
//    count is decreased unless the new owner gets a reference instead.
//
//  * `ShareOwnership`: The original owner keeps its reference. The reference
//    count is increased if the new owner also gets a reference.

struct PassOwnership {};
RIEGELI_INLINE_CONSTEXPR(PassOwnership, kPassOwnership, PassOwnership());

struct ShareOwnership {};
RIEGELI_INLINE_CONSTEXPR(ShareOwnership, kShareOwnership, ShareOwnership());

// `IsOwnership<T>::value` is `true` if `T` is `PassOwnership` or
// `ShareOwnership`.

template <typename T>
struct IsOwnership : std::false_type {};

template <>
struct IsOwnership<PassOwnership> : std::true_type {};

template <>
struct IsOwnership<ShareOwnership> : std::true_type {};

}  // namespace riegeli

#endif  // RIEGELI_BASE_OWNERSHIP_H_
