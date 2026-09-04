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

#ifndef RIEGELI_INTERNED_OBJECT_ARENA_INTERNAL_H_
#define RIEGELI_INTERNED_OBJECT_ARENA_INTERNAL_H_

#include <stddef.h>

#include <new>  // IWYU pragma: keep

#include "absl/base/nullability.h"
#include "riegeli/base/new_aligned.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli::interned_internal {

// Manages a contiguous array of elements of type `T`.
template <typename T>
class ObjectArenaBlock {
 public:
  ObjectArenaBlock() = default;

  explicit ObjectArenaBlock(size_t min_size) {
    size_t size_bytes;
    T* const data = static_cast<T*>(SizeReturningNewAligned<void, alignof(T)>(
        min_size * sizeof(T), &size_bytes));
    size_ = size_bytes / sizeof(T);
    limit_ = data + size_;
  }

  ObjectArenaBlock(ObjectArenaBlock&& that) = default;
  ObjectArenaBlock& operator=(ObjectArenaBlock&& that) = default;

  void DeleteFull() { DeletePartial(limit_); }

  void DeletePartial(T* absl_nullable cursor) {
    Clear(cursor);
    DeleteAligned<void, alignof(T)>(data(), size_ * sizeof(T));
  }

  void Clear(T* absl_nullable cursor) {
    T* const data = this->data();
    while (cursor != data) {
      --cursor;
      cursor->~T();
    }
  }

  bool is_allocated() const { return limit_ != nullptr; }

  T* absl_nullable data() const { return limit_ - size_; }
  T* absl_nullable limit() const { return limit_; }
  size_t size() const { return size_; }

  template <typename MemoryEstimator>
  void RegisterSubobjectsFull(MemoryEstimator& memory_estimator) const {
    RegisterSubobjectsPartial(limit_, memory_estimator);
  }

  template <typename MemoryEstimator>
  void RegisterSubobjectsPartial(const T* absl_nullable cursor,
                                 MemoryEstimator& memory_estimator) const {
    const T* const data = this->data();
    memory_estimator.RegisterDynamicMemory(data, size_ * sizeof(T));
    memory_estimator.RegisterSubobjects(static_cast<const T*>(data), cursor);
  }

 private:
  T* absl_nullable limit_ = nullptr;
  size_t size_ = 0;
};

}  // namespace riegeli::interned_internal

#endif  // RIEGELI_INTERNED_OBJECT_ARENA_INTERNAL_H_
