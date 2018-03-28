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

#ifndef RIEGELI_BASE_MEMORY_STATS_H_
#define RIEGELI_BASE_MEMORY_STATS_H_

#include <stddef.h>
#include <limits>
#include <unordered_set>

#include "riegeli/base/base.h"

namespace riegeli {

// Estimates the amount of memory used by multiple objects which may share their
// subobjects (e.g. by reference counting).
//
// This is done by traversing the objects and their subobjects, skipping objects
// which were already seen.
//
// By convention an object unconditionally registers itself, adding its memory
// (sizeof(*this)) and its subobjects to a MemoryEstimator, by a member
// function:
//
//   void AddUniqueTo(MemoryEstimator* memory_estimator) const;
//
// By convention an objects registers itself if it was not seen yet by a member
// function:
//
//   void AddSharedTo(MemoryEstimator* memory_estimator) const {
//     if (memory_estimator->AddObject(this)) AddUniqueTo(memory_estimator);
//   }
//
// Context which estimates its memory and stores a pointer to an object should
// call AddSharedTo() on that object if there is a possibility of sharing it,
// and AddUniqueTo() if it stores a unique pointer. If the object does not
// support this convention, these calls should be approximated externally.
class MemoryEstimator {
 public:
  MemoryEstimator() {}

  MemoryEstimator(const MemoryEstimator& src);
  MemoryEstimator& operator=(const MemoryEstimator& src);

  MemoryEstimator(MemoryEstimator&& src) noexcept;
  MemoryEstimator& operator=(MemoryEstimator&& src) noexcept;

  // Registers the given amount of memory as used.
  void AddMemory(size_t memory);

  // Registers an object which might be shared by other objects.
  //
  // The argument should be a pointer which uniquely identifies the object.
  // This is usually the pointer to the object itself, but that might not be
  // possible if the object is not public and is registered by code external
  // to it, in which case some proxy is needed.
  //
  // Returns true if this object was not seen yet; only in this case the caller
  // should register its memory and subobjects.
  bool AddObject(const void* object);

  // Returns the total amount of memory added.
  size_t TotalMemory() const { return total_memory_; }

 private:
  size_t total_memory_ = 0;
  std::unordered_set<const void*> objects_seen_;
};

// Implementation details follow.

inline void MemoryEstimator::AddMemory(size_t memory) {
  total_memory_ +=
      UnsignedMin(memory, std::numeric_limits<size_t>::max() - total_memory_);
}

inline bool MemoryEstimator::AddObject(const void* object) {
  return objects_seen_.insert(object).second;
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_MEMORY_STATS_H_
