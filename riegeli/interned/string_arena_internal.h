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

#include <utility>

#include "absl/base/nullability.h"
#include "riegeli/base/new_aligned.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli::interned_internal {

// Manages a contiguous array of bytes.
class StringArenaBlock {
 public:
  static constexpr size_t kMinAlignment = __STDCPP_DEFAULT_NEW_ALIGNMENT__;

  explicit StringArenaBlock(size_t min_size)
      : data_(static_cast<char*>(
            SizeReturningNewAligned<void, kMinAlignment>(min_size, &size_))) {}

  StringArenaBlock(StringArenaBlock&& that) noexcept
      : data_(std::exchange(that.data_, nullptr)),
        size_(std::exchange(that.size_, 0)) {}
  StringArenaBlock& operator=(StringArenaBlock&&) = delete;

  ~StringArenaBlock() { DeleteAligned<void, kMinAlignment>(data_, size_); }

  char* data() const { return data_; }
  size_t size() const { return size_; }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const StringArenaBlock* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterDynamicMemory(self->data_, self->size_);
  }

 private:
  char* data_;
  size_t size_;
};

}  // namespace riegeli::interned_internal

#endif  // RIEGELI_INTERNED_STRING_ARENA_INTERNAL_H_
