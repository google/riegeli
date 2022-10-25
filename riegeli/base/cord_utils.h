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

#ifndef RIEGELI_BASE_CORD_UTILS_H_
#define RIEGELI_BASE_CORD_UTILS_H_

#include <stddef.h>

#include <string>

#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"

namespace riegeli {

// When deciding whether to copy an array of bytes or share memory to an
// `absl::Cord`, prefer copying up to this length.
//
// `absl::Cord::Append(absl::Cord)` chooses to copy bytes from a source up to
// this length, so it is better to avoid constructing the source as `absl::Cord`
// if it will not be shared anyway.
inline size_t MaxBytesToCopyToCord(absl::Cord& dest) {
  // `absl::cord_internal::kMaxInline`.
  static constexpr size_t kMaxInline = 15;
  // `absl::cord_internal::kMaxBytesToCopy`.
  static constexpr size_t kCordMaxBytesToCopy = 511;
  return dest.empty() ? kMaxInline : kCordMaxBytesToCopy;
}

// Copies `src` to `dest[]`.
void CopyCordToArray(const absl::Cord& src, char* dest);

// Appends `src` to `dest`.
void AppendCordToString(const absl::Cord& src, std::string& dest);

// Variants of `absl::Cord` operations with different block sizing tradeoffs:
//  * `MakeBlockyCord(src)` is like `absl::Cord(src)`.
//  * `AppendToBlockyCord(src, dest)` is like `dest.Append(src)`.
//  * `PrependToBlockyCord(src, dest)` is like `dest.Prepend(src)`.
//
// They assume that the `absl::Cord` is constructed from fragments of reasonable
// sizes, with adjacent sizes being not too small.
//
// They avoid splitting `src` into 4083-byte fragments and avoid overallocation.
absl::Cord MakeBlockyCord(absl::string_view src);
void AppendToBlockyCord(absl::string_view src, absl::Cord& dest);
void PrependToBlockyCord(absl::string_view src, absl::Cord& dest);

}  // namespace riegeli

#endif  // RIEGELI_BASE_CORD_UTILS_H_
