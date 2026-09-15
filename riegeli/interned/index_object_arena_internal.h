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

#ifndef RIEGELI_INTERNED_INDEX_OBJECT_ARENA_INTERNAL_H_
#define RIEGELI_INTERNED_INDEX_OBJECT_ARENA_INTERNAL_H_

#include <stddef.h>

#include <new>  // IWYU pragma: keep
#include <utility>

#include "absl/base/nullability.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/new_aligned.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli::interned_internal {

// Manages a contiguous array of a fixed number of elements of type `T`.
template <typename T, size_t size>
class IndexObjectArenaBlock {
 public:
  IndexObjectArenaBlock() = default;

  explicit IndexObjectArenaBlock(std::in_place_t)
      : data_(static_cast<T*>(NewAligned<void, alignof(T)>(size * sizeof(T)))) {
  }

  IndexObjectArenaBlock(const IndexObjectArenaBlock& that) = default;
  IndexObjectArenaBlock& operator=(const IndexObjectArenaBlock&) = default;

  void DeleteFull() { DeletePartial(limit()); }

  void DeletePartial(T* absl_nullable cursor) {
    ClearPartial(cursor);
    DeleteAligned<void, alignof(T)>(data(), size * sizeof(T));
  }

  void ClearFull() { ClearPartial(limit()); }

  void ClearPartial(T* absl_nullable cursor) {
    T* const data = this->data();
    while (cursor != data) {
      --cursor;
      cursor->~T();
    }
  }

  bool is_allocated() const { return data_ != nullptr; }

  T* data() const {
    RIEGELI_ASSERT_NE(data_, nullptr)
        << "Failed precondition of IndexObjectArenaBlock::data(): "
           "block not allocated";
    return data_;
  }
  T* limit() const {
    RIEGELI_ASSERT_NE(data_, nullptr)
        << "Failed precondition of IndexObjectArenaBlock::limit(): "
           "block not allocated";
    return data_ + size;
  }

  const T& operator[](size_t index) const { return data()[index]; }
  T& operator[](size_t index) { return data()[index]; }

  template <typename MemoryEstimator>
  void RegisterSubobjectsFull(MemoryEstimator& memory_estimator) const {
    RegisterSubobjectsPartial(limit(), memory_estimator);
  }

  template <typename MemoryEstimator>
  void RegisterSubobjectsPartial(const T* absl_nullable cursor,
                                 MemoryEstimator& memory_estimator) const {
    const T* const data = this->data();
    memory_estimator.RegisterDynamicMemory(data, size * sizeof(T));
    memory_estimator.RegisterSubobjects(static_cast<const T*>(data), cursor);
  }

 private:
  T* absl_nullable data_ = nullptr;
};

}  // namespace riegeli::interned_internal

#endif  // RIEGELI_INTERNED_INDEX_OBJECT_ARENA_INTERNAL_H_
