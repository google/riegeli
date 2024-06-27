// Copyright 2021 Google LLC
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

#ifndef RIEGELI_BASE_REF_COUNT_H_
#define RIEGELI_BASE_REF_COUNT_H_

#include <stddef.h>

#include <atomic>
#include <type_traits>

#include "riegeli/base/ownership.h"

namespace riegeli {

// `RefCount` provides operations on an atomic reference count.
class RefCount {
 public:
  RefCount() = default;

  RefCount(const RefCount&) = delete;
  RefCount& operator=(const RefCount&) = delete;

  // Increments the reference count.
  //
  // Does nothing if `Ownership` is `PassOwnership`.
  template <typename Ownership = ShareOwnership,
            std::enable_if_t<IsOwnership<Ownership>::value, int> = 0>
  void Ref() const {
    if (std::is_same<Ownership, ShareOwnership>::value) {
      ref_count_.fetch_add(1, std::memory_order_relaxed);
    }
  }

  // Decrements the reference count. Returns `true` when this was the last
  // reference.
  //
  // Does nothing and returns `false` if `Ownership` is `ShareOwnership`.
  //
  // When `Unref()` returns `true`, the decrement can be skipped and the actual
  // value of the reference count is unspecified. This avoids an expensive
  // atomic read-modify-write operation, making the last `Unref()` much faster,
  // at the cost of making a non-last `Unref()` a bit slower.
  template <typename Ownership = PassOwnership,
            std::enable_if_t<IsOwnership<Ownership>::value, int> = 0>
  bool Unref() const {
    return std::is_same<Ownership, PassOwnership>::value &&
           (HasUniqueOwner() ||
            ref_count_.fetch_sub(1, std::memory_order_acq_rel) == 1);
  }

  // Returns `true` if there is only one owner of the object.
  //
  // This can be used to check if the object may be modified.
  bool HasUniqueOwner() const {
    return ref_count_.load(std::memory_order_acquire) == 1;
  }

  // Returns the current count.
  //
  // If the `RefCount` is accessed by multiple threads, this is a snapshot of
  // the count which may change asynchronously, hence usage of `get_count()`
  // should be limited to cases not important for correctness, like producing
  // debugging output.
  size_t GetCount() const { return ref_count_.load(std::memory_order_relaxed); }

 private:
  mutable std::atomic<size_t> ref_count_ = 1;
};

}  // namespace riegeli

#endif  // RIEGELI_BASE_REF_COUNT_H_
