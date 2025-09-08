// Copyright 2022 Google LLC
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

#ifndef RIEGELI_BASE_CLOSING_PTR_H_
#define RIEGELI_BASE_CLOSING_PTR_H_

#include <memory>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

// A deleter for `std::unique_ptr` which does nothing.
struct NullDeleter {
  template <typename T>
  void operator()(ABSL_ATTRIBUTE_UNUSED T* ptr) const {}
};

// Marks the pointer with the intent to transfer the responsibility to close the
// object when done with the pointer, even though the object is not moved nor
// destroyed.
//
// In the context of `Dependency` and `Any`, passing `ClosingPtr(&m)`
// instead of `std::move(m)` avoids moving `m`, but the caller must ensure that
// the dependent object is valid while the host object needs it.

template <typename T>
using ClosingPtrType = std::unique_ptr<T, NullDeleter>;

template <typename T>
inline ClosingPtrType<T> ClosingPtr(T* ptr) {
  return ClosingPtrType<T>(ptr);
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_CLOSING_PTR_H_
