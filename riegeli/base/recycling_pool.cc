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

#include "riegeli/base/recycling_pool.h"

#include <stdint.h>

#include <thread>

#include "absl/time/time.h"  // IWYU pragma: keep
#include "riegeli/base/arithmetic.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if !__cpp_inline_variables
constexpr absl::Duration RecyclingPoolOptions::kDefaultMaxAge;
#endif

uint32_t RecyclingPoolOptions::DefaultMaxSizeSlow() {
  return UnsignedMax(uint32_t{16},
                     IntCast<uint32_t>(std::thread::hardware_concurrency()));
}

}  // namespace riegeli
