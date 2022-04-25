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

#include "riegeli/bytes/buffer_options.h"

#include <stddef.h>

#include "absl/flags/flag.h"

ABSL_FLAG(
    bool, riegeli_use_adaptive_buffer_sizes, false,
    "If true, the buffer length in Riegeli classes depending on buffer_options "
    "will vary between min_buffer_size and max_buffer_size, depending on the "
    "access pattern, with {min,max}_buffer_size depending on the concrete "
    "class. If false, the default min_buffer_size will be forced to be the "
    "same as the default max_buffer_size, which effectively disables varying "
    "the buffer size when options are left as default. This flag allows to "
    "separately compile in the new logic and then turn on the behavior, and "
    "then it will serve as an emergency brake: it will later change to true by "
    "default, and then it will be removed. Details: cl/437768440.");

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if __cplusplus < 201703
constexpr size_t BufferOptions::kDefaultMinBufferSize;
constexpr size_t BufferOptions::kDefaultMaxBufferSize;
#endif

}  // namespace riegeli
