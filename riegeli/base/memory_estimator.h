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

#ifndef RIEGELI_BASE_MEMORY_ESTIMATOR_H_
#define RIEGELI_BASE_MEMORY_ESTIMATOR_H_

#include <stddef.h>

#include <utility>

#include "absl/container/flat_hash_set.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/estimated_allocated_size.h"

namespace riegeli {

// Estimates the amount of memory used by multiple objects which may share their
// subobjects (e.g. by reference counting).
//
// This is done by traversing the objects and their subobjects, skipping objects
// which were already seen.
//
// By convention an object registers itself with a member function with the name
// and meaning depending on whether the object is typically held by value or by
// pointer, and whether object ownership can be shared:
//
// ```
//   // Registers subobjects of this object with MemoryEstimator, but does not
//   // include this object (sizeof(*this)).
//   //
//   // This is applicable to objects held by value and thus with their exact
//   // type known statically.
//   void RegisterSubobjects(riegeli::MemoryEstimator& memory_estimator) const;
//
//   // Registers this object (sizeof(*this)) and its subobjects with
//   // MemoryEstimator.
//   //
//   // This is applicable to objects held by pointer, possibly with only their
//   // base class known statically (it makes sense for RegisterUnique() to be
//   // virtual).
//   void RegisterUnique(riegeli::MemoryEstimator& memory_estimator) const {
//     memory_estimator.RegisterDynamicMemory(sizeof(*this));
//     RegisterSubobjects(memory_estimator);
//   }
//
//   // If this object was not seen yet, registers this object (sizeof(*this))
//   // and its subobjects with MemoryEstimator.
//   //
//   // This is applicable to objects held by pointer if object ownership can be
//   // shared between multiple pointers.
//   void RegisterShared(riegeli::MemoryEstimator& memory_estimator) const {
//     if (memory_estimator.RegisterNode(this)) {
//       RegisterUnique(memory_estimator);
//     }
//   }
// ```
//
// For objects which do not support these conventions, their owner estimates
// their memory usage and possible sharing with whatever means have been found.
// The estimation can thus be inexact.
class MemoryEstimator {
 public:
  MemoryEstimator() {}

  MemoryEstimator(const MemoryEstimator& that);
  MemoryEstimator& operator=(const MemoryEstimator& that);

  MemoryEstimator(MemoryEstimator&& that) noexcept;
  MemoryEstimator& operator=(MemoryEstimator&& that) noexcept;

  // Registers the given amount of memory as used.
  void RegisterMemory(size_t memory);

  // Registers the given length of a block of dynamically allocated memory as
  // used. The length should correspond to a single allocation. The actual
  // registered amount includes estimated overhead of the memory allocator.
  void RegisterDynamicMemory(size_t memory);

  // Registers an object which might be shared by other objects.
  //
  // The argument should be a pointer which uniquely identifies the object.
  // This is usually the pointer to the object itself, but that might not be
  // possible if the object is not public and is registered by code external
  // to it, in which case some proxy is needed.
  //
  // Returns true if this object was not seen yet; only in this case the caller
  // should register its memory and subobjects.
  bool RegisterNode(const void* ptr);

  // Returns the total amount of memory added.
  size_t TotalMemory() const { return total_memory_; }

 private:
  size_t total_memory_ = 0;
  absl::flat_hash_set<const void*> objects_seen_;
};

// Implementation details follow.

inline void MemoryEstimator::RegisterMemory(size_t memory) {
  total_memory_ = SaturatingAdd(total_memory_, memory);
}

inline void MemoryEstimator::RegisterDynamicMemory(size_t memory) {
  RegisterMemory(EstimatedAllocatedSize(memory));
}

inline bool MemoryEstimator::RegisterNode(const void* ptr) {
  return objects_seen_.insert(ptr).second;
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_MEMORY_ESTIMATOR_H_
