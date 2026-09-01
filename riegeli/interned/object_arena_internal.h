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
#include <utility>

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
    data_ = static_cast<T*>(SizeReturningNewAligned<void, alignof(T)>(
        min_size * sizeof(T), &size_bytes));
    size_ = size_bytes / sizeof(T);
  }

  ObjectArenaBlock(ObjectArenaBlock&& that) = default;
  ObjectArenaBlock& operator=(ObjectArenaBlock&& that) = default;

  void DeleteFull() { DeletePartial(size_); }

  void DeletePartial(size_t used) {
    Clear(used);
    DeleteAligned<void, alignof(T)>(data_, size_ * sizeof(T));
  }

  void Clear(size_t used) {
    for (size_t i = used; i > 0;) {
      --i;
      data_[i].~T();
    }
  }

  bool is_allocated() const { return data_ != nullptr; }

  size_t size() const { return size_; }

  template <typename... Args>
  T& emplace_back(size_t size, Args&&... args) {
    new (data_ + size) T(std::forward<Args>(args)...);
    return data_[size];
  }

  const T& operator[](size_t index) const { return data_[index]; }

  void pop_back(size_t index) { data_[index].~T(); }

  template <typename MemoryEstimator>
  void RegisterSubobjectsFull(MemoryEstimator& memory_estimator) const {
    RegisterSubobjectsPartial(size_, memory_estimator);
  }

  template <typename MemoryEstimator>
  void RegisterSubobjectsPartial(size_t used,
                                 MemoryEstimator& memory_estimator) const {
    memory_estimator.RegisterDynamicMemory(data_, size_ * sizeof(T));
    memory_estimator.RegisterSubobjects(data_, data_ + used);
  }

 private:
  T* absl_nullable data_ = nullptr;
  size_t size_ = 0;
};

}  // namespace riegeli::interned_internal

#endif  // RIEGELI_INTERNED_OBJECT_ARENA_INTERNAL_H_
