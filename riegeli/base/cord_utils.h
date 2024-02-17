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
#include <stdint.h>

#include <string>

#include "absl/numeric/bits.h"
#include "absl/strings/cord.h"
#include "absl/strings/cord_buffer.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/constexpr.h"

namespace riegeli {
namespace cord_internal {

// `absl::cord_internal::kMaxInline`. Does not have to be accurate.
RIEGELI_INLINE_CONSTEXPR(size_t, kMaxInline, 15);

// `absl::cord_internal::kFlatOverhead`. Does not have to be accurate.
RIEGELI_INLINE_CONSTEXPR(size_t, kFlatOverhead,
                         sizeof(size_t) + sizeof(uint32_t) + sizeof(uint8_t));

// `sizeof(absl::cord_internal::CordRepExternal)`. Does not have to be
// accurate.
RIEGELI_INLINE_CONSTEXPR(size_t, kSizeOfCordRepExternal, 4 * sizeof(void*));

// The `block_size` parameter for `absl::CordBuffer::CreateWithCustomLimit()`.
RIEGELI_INLINE_CONSTEXPR(size_t, kCordBufferBlockSize,
                         UnsignedMin(kDefaultMaxBlockSize,
                                     absl::CordBuffer::kCustomLimit));

// Maximum usable size supported by `absl::CordBuffer`.
RIEGELI_INLINE_CONSTEXPR(
    size_t, kCordBufferMaxSize,
    absl::CordBuffer::MaximumPayload(kCordBufferBlockSize));

// When deciding whether to copy an array of bytes or share memory to an
// `absl::Cord`, prefer copying up to this length.
//
// `absl::Cord::Append(absl::Cord)` chooses to copy bytes from a source up to
// this length, so it is better to avoid constructing the source as `absl::Cord`
// if it will not be shared anyway.
inline size_t MaxBytesToCopyToCord(absl::Cord& dest) {
  // `absl::cord_internal::kMaxBytesToCopy`. Does not have to be accurate.
  static constexpr size_t kMaxBytesToCopy = 511;
  return dest.empty() ? kMaxInline : kMaxBytesToCopy;
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

// The `capacity` parameter for `absl::CordBuffer::CreateWithCustomLimit()`
// sufficient to let it return a block with at least `min_length` of space.
// Does not have to be accurate.
inline size_t CordBufferCapacityForMinLength(size_t min_length) {
  RIEGELI_ASSERT_LE(min_length, kCordBufferMaxSize)
      << "Failed precondition of CordBufferCapacityForMinLength(): "
         "min_length larger than what CordBuffer supports";
  if (min_length <= absl::CordBuffer::kDefaultLimit) return min_length;
  const size_t capacity = min_length + kFlatOverhead;
  const size_t rounded_up = size_t{1} << absl::bit_width(capacity - 1);
  return rounded_up - kFlatOverhead;
}

}  // namespace cord_internal
}  // namespace riegeli

#endif  // RIEGELI_BASE_CORD_UTILS_H_
