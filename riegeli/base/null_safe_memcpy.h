// Copyright 2017 Google LLC
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

#ifndef RIEGELI_BASE_NULL_SAFE_MEMCPY_H_
#define RIEGELI_BASE_NULL_SAFE_MEMCPY_H_

#include <stddef.h>

#include <cstring>

#include "absl/base/optimization.h"  // IWYU pragma: keep

namespace riegeli {

// `riegeli::null_safe_memcpy()` is like `std::memcpy()` but accepts null
// pointers, as long as the size is zero.
//
// Everything here applies also to `memmove()`, `memset()`, and some other
// functions.
//
// Background:
//
// `std::memcpy(nullptr, _, 0)` and `std::memcpy(_, nullptr, 0)` have undefined
// behavior, both in C and in C++. This is consistent with C rules that pointer
// arithmetic on `NULL` is undefined, even with zero offsets.
//
// In C++ pointer arithmetic on `nullptr` is defined, and an empty range
// starting from `nullptr` is valid, such as a default-constructed
// `absl::string_view`. Unfortunately `memcpy()` works like in C.
//
// https://www.open-std.org/jtc1/sc22/wg14/www/docs/n3322.pdf is going to change
// `memcpy()` in C2y. There is no proposal so far for C++ though.
//
// Some compilers make use of this undefined behavior. In particular GCC
// optimizes explicit comparisons against `nullptr` after a pointer has been
// passed to `memcpy()`. In GCC and Clang, ubsan checks that parameters of
// `memcpy()` are not null, based on annotations in headers.
//
// A portable implementation of null-safe `memcpy()` uses an explicit length
// check against zero. Unfortunately compilers do not optimize out that
// conditional, even if the runtime library works with null pointers.
//
// According to
// https://github.com/llvm/llvm-project/issues/49459#issuecomment-2579439921 and
// https://github.com/llvm/llvm-project/issues/146484#issuecomment-3022856421,
// `memcpy()` with null pointers has always worked in Clang. Hence for Clang
// `riegeli::null_safe_memcpy()` just calls `std::memcpy()`, while disabling
// the ubsan check which would otherwise complain.

#ifdef __clang__
__attribute__((no_sanitize("nonnull-attribute")))
#endif
inline void null_safe_memcpy(void* dest, const void* src, size_t length) {
#ifndef __clang__
  if (ABSL_PREDICT_FALSE(length == 0)) return;
#endif
  std::memcpy(dest, src, length);
}

#ifdef __clang__
__attribute__((no_sanitize("nonnull-attribute")))
#endif
inline void null_safe_memmove(void* dest, const void* src, size_t length) {
#ifndef __clang__
  if (ABSL_PREDICT_FALSE(length == 0)) return;
#endif
  std::memmove(dest, src, length);
}

#ifdef __clang__
__attribute__((no_sanitize("nonnull-attribute")))
#endif
inline void null_safe_memset(void* dest, int c, size_t length) {
#ifndef __clang__
  if (ABSL_PREDICT_FALSE(length == 0)) return;
#endif
  std::memset(dest, c, length);
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_NULL_SAFE_MEMCPY_H_
