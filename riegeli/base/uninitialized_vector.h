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

#ifndef RIEGELI_BASE_UNINITIALIZED_VECTOR_H_
#define RIEGELI_BASE_UNINITIALIZED_VECTOR_H_

#include <stddef.h>

#include <memory>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/container/inlined_vector.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

// An allocator which default-initializes a newly constructed element instead of
// value-initializing it.
//
// When used with a container of a trivially constructible type,
// newly allocated elements are left uninitialized. The container must use
// `allocator::construct()` to construct the elements.
//
// This makes it faster to resize the container. This is meant for the cases
// when its contents will be filled right after resizing. This also allows
// detecting usages of unfilled elements by msan.
//
// Based on https://howardhinnant.github.io/allocator_boilerplate.html.
template <typename T>
class UninitializedAllocator : public std::allocator<T> {
 public:
  using UninitializedAllocator::allocator::allocator;

  template <typename U, typename... Args>
  void construct(U* ptr, Args&&... args) {
    if constexpr (sizeof...(args) == 0) {
      ::new (ptr) U;  // Default initialization, not value initialization.
    } else {
      ::new (ptr) U(std::forward<Args>(args)...);
    }
  }
};

// Like `std::vector`, but newly allocated elements of trivially-constructible
// types are left uninitialized.
//
// This makes it faster to resize the vector. This is meant for the cases when
// its contents will be filled right after resizing. This also allows detecting
// usages of unfilled elements by msan.
template <typename T>
using UninitializedVector = std::vector<T, UninitializedAllocator<T>>;

// Like `absl::InlinedVector`, but newly allocated elements of
// trivially-constructible types are left uninitialized.
//
// This makes it faster to resize the vector. This is meant for the cases when
// its contents will be filled right after resizing. This also allows detecting
// usages of unfilled elements by msan.
template <typename T, size_t inlined_size>
using UninitializedInlinedVector =
    absl::InlinedVector<T, inlined_size, UninitializedAllocator<T>>;

}  // namespace riegeli

#endif  // RIEGELI_BASE_UNINITIALIZED_VECTOR_H_
