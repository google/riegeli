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

#ifndef RIEGELI_BASE_STRING_UTILS_H_
#define RIEGELI_BASE_STRING_UTILS_H_

#include <stddef.h>

#include <string>
#include <utility>

#include "absl/base/nullability.h"
#include "absl/strings/resize_and_overwrite.h"
#include "riegeli/base/assert.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

namespace string_utils_internal {

void ReserveAmortized(std::string& dest, size_t new_size);

}  // namespace string_utils_internal

// Like `std::string::resize()`, ensuring that repeated growth has the cost
// proportional to the final size.
inline void StringResizeAmortized(std::string& dest, size_t new_size) {
  if (new_size > dest.capacity()) {
    string_utils_internal::ReserveAmortized(dest, new_size);
  }
  RIEGELI_ASSUME_GE(dest.capacity(), new_size);
  dest.resize(new_size);
}

// Like `absl::StringResizeAndOverwrite()`, ensuring that repeated growth has
// the cost proportional to the final size.
template <typename Op>
inline void StringResizeAndOverwriteAmortized(std::string& dest,
                                              size_t new_size, Op op) {
  if (new_size > dest.capacity()) {
    string_utils_internal::ReserveAmortized(dest, new_size);
  }
  RIEGELI_ASSUME_GE(dest.capacity(), new_size);
  return absl::StringResizeAndOverwrite(dest, new_size, std::move(op));
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_STRING_UTILS_H_
