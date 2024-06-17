// Copyright 2022 Google LLC
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

#ifndef RIEGELI_BASE_ANY_DEPENDENCY_H_
#define RIEGELI_BASE_ANY_DEPENDENCY_H_

#include <stddef.h>

#include "absl/base/macros.h"
#include "riegeli/base/any.h"

namespace riegeli {

template <typename Handle, size_t inline_size = 0, size_t inline_align = 0>
using AnyDependency ABSL_DEPRECATE_AND_INLINE() =
    Any<Handle, inline_size, inline_align>;

template <typename Handle>
using AnyDependencyRef ABSL_DEPRECATE_AND_INLINE() = AnyRef<Handle>;

}  // namespace riegeli

#endif  // RIEGELI_BASE_ANY_DEPENDENCY_H_
