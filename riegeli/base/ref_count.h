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

#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "riegeli/base/ownership.h"

ABSL_POINTERS_DEFAULT_NONNULL

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
    if constexpr (std::is_same_v<Ownership, ShareOwnership>) {
      ref_count_.fetch_add(1, std::memory_order_relaxed);
    }
  }

  // Decrements the reference count. Returns `true` when this was the last
  // reference.
  //
  // Does nothing and returns `false` if `Ownership` is `ShareOwnership`.
  //
  // When `Unref()` returns `true`, the decrement might be skipped, leaving the
  // actual value of the reference count unspecified. This avoids an expensive
  // atomic read-modify-write operation, making the last `Unref()` much faster,
  // at the cost of making a non-last `Unref()` a bit slower. This is in
  // contrast to `std::shared_ptr` in libc++ and libstdc++.
  template <typename Ownership = PassOwnership,
            std::enable_if_t<IsOwnership<Ownership>::value, int> = 0>
  bool Unref() const {
    if constexpr (std::is_same_v<Ownership, PassOwnership>) {
      if (HasUniqueOwner()) return true;
      if (ABSL_PREDICT_FALSE(
              ref_count_.fetch_sub(1, std::memory_order_release) == 1)) {
        // Even though `HasUniqueOwner()` was `false` before, another thread
        // has just decremented the reference count. This is the last reference
        // after all.
#ifdef THREAD_SANITIZER
        // TSAN does not support `std::atomic_thread_fence()`. Using `load()`
        // instead is less efficient but also correct.
        (void)ref_count_.load(std::memory_order_acquire);
#else
        std::atomic_thread_fence(std::memory_order_acquire);
#endif
        return true;
      }
    }
    return false;
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
  // the count which may change asynchronously, hence usage of `GetCount()`
  // should be limited to cases not important for correctness, like producing
  // debugging output.
  //
  // The count can be reliably compared against 1 with `HasUniqueOwner()`.
  size_t GetCount() const { return ref_count_.load(std::memory_order_relaxed); }

 private:
  mutable std::atomic<size_t> ref_count_ = 1;
};

}  // namespace riegeli

#endif  // RIEGELI_BASE_REF_COUNT_H_
