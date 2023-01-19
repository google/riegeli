// Copyright 2019 Google LLC
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

#ifndef RIEGELI_BASE_ESTIMATED_ALLOCATED_SIZE_H_
#define RIEGELI_BASE_ESTIMATED_ALLOCATED_SIZE_H_

#include <stddef.h>

#include "absl/base/attributes.h"
#include "absl/numeric/bits.h"
#include "riegeli/base/arithmetic.h"

namespace riegeli {

// Returns the estimated size which will be allocated when requesting to
// allocate `requested_size`.
inline size_t EstimatedAllocatedSize(size_t requested_size) {
  // Placeholder for asking the memory manager, which might be possible on some
  // platforms.
  return RoundUp<2 * sizeof(void*)>(
      UnsignedMax(requested_size, 4 * sizeof(void*)));
}

// Returns the estimated size which was allocated at `ptr` when requested to
// allocate `requested_size`.
inline size_t EstimatedAllocatedSize(ABSL_ATTRIBUTE_UNUSED const void* ptr,
                                     size_t requested_size) {
  // Placeholder for using `ptr`, which might be possible on some platforms.
  return EstimatedAllocatedSize(requested_size);
}

// A deterministic variant of `EstimatedAllocatedSize()`, useful for testing.
inline size_t EstimatedAllocatedSizeForTesting(size_t requested_size) {
  return absl::bit_ceil(requested_size);
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_ESTIMATED_ALLOCATED_SIZE_H_
