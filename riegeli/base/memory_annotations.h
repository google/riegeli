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

#ifndef RIEGELI_BASE_MEMORY_ANNOTATIONS_H_
#define RIEGELI_BASE_MEMORY_ANNOTATIONS_H_

#ifdef MEMORY_SANITIZER
#include <sanitizer/msan_interface.h>
#endif
#include <stddef.h>

#include "absl/base/nullability.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

// Asserts that a region of memory is initialized, which is checked when running
// under memory sanitizer.
inline void AssertInitialized([[maybe_unused]] const char* data,
                              [[maybe_unused]] size_t size) {
#ifdef MEMORY_SANITIZER
  __msan_check_mem_is_initialized(data, size);
#endif
}

// Marks that a region of memory should be treated as uninitialized, which is
// checked when running under memory sanitizer.
inline void MarkPoisoned([[maybe_unused]] const char* data,
                         [[maybe_unused]] size_t size) {
#ifdef MEMORY_SANITIZER
  __msan_poison(data, size);
#endif
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_MEMORY_ANNOTATIONS_H_
