// Copyright 2026 Google LLC
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

#ifndef RIEGELI_INTERNED_STRING_ARENA_INTERNAL_H_
#define RIEGELI_INTERNED_STRING_ARENA_INTERNAL_H_

#include <stddef.h>

#include "absl/base/nullability.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/new_aligned.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli::interned_internal {

// Manages a contiguous array of bytes.
class StringArenaBlock {
 public:
  static constexpr size_t kMinAlignment = __STDCPP_DEFAULT_NEW_ALIGNMENT__;

  StringArenaBlock() = default;

  // Used for regular blocks.
  explicit StringArenaBlock(size_t min_size, size_t max_size) {
    size_t size;
    data_ = static_cast<char*>(
        SizeReturningNewAligned<void, kMinAlignment>(min_size, &size));
    limit_ = data_ + UnsignedMin(size, max_size);
  }

  // Used for dedicated blocks.
  explicit StringArenaBlock(size_t size) {
    data_ = static_cast<char*>(NewAligned<void, kMinAlignment>(size));
    limit_ = data_ + size;
  }

  StringArenaBlock(const StringArenaBlock& that) = default;
  StringArenaBlock& operator=(const StringArenaBlock&) = default;

  void Delete() {
    DeleteAligned<void, kMinAlignment>(data_, PtrDistance(data_, limit_));
  }

  char* absl_nullable data() const { return data_; }
  char* absl_nullable limit() const { return limit_; }
  size_t size() const { return PtrDistance(data_, limit_); }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const StringArenaBlock* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterDynamicMemory(self->data_, self->size());
  }

 private:
  char* absl_nullable data_ = nullptr;
  char* absl_nullable limit_ = nullptr;
};

}  // namespace riegeli::interned_internal

#endif  // RIEGELI_INTERNED_STRING_ARENA_INTERNAL_H_
