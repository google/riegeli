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
#include "riegeli/base/buffering.h"

namespace riegeli::cord_internal {

// `absl::cord_internal::kFlatOverhead`. Does not have to be accurate.
inline constexpr size_t kFlatOverhead =
    sizeof(size_t) + sizeof(uint32_t) + sizeof(uint8_t);

// The `block_size` parameter for `absl::CordBuffer::CreateWithCustomLimit()`.
inline constexpr size_t kCordBufferBlockSize =
    UnsignedMin(kDefaultMaxBlockSize, absl::CordBuffer::kCustomLimit);

// Maximum usable size supported by `absl::CordBuffer`.
inline constexpr size_t kCordBufferMaxSize =
    absl::CordBuffer::MaximumPayload(kCordBufferBlockSize);

// When deciding whether to copy an array of bytes or share memory to an
// `absl::Cord`, prefer copying up to this length when creating a new
// `absl::Cord`.
//
// This is `absl::cord_internal::kMaxInline`. Does not have to be accurate.
inline constexpr size_t kMaxBytesToCopyToEmptyCord = 15;

// When deciding whether to copy an array of bytes or share memory to an
// `absl::Cord`, prefer copying up to this length when appending to a non-empty
// `absl::Cord`.
//
// This is `absl::cord_internal::kMaxBytesToCopy`. Does not have to be accurate.
inline constexpr size_t kMaxBytesToCopyToNonEmptyCord = 511;

// When deciding whether to copy an array of bytes or share memory to an
// `absl::Cord`, prefer copying up to this length when appending to `dest`.
//
// `absl::Cord::Append(absl::Cord)` chooses to copy bytes from a source up to
// this length, so it is better to avoid constructing the source as `absl::Cord`
// if it will not be shared anyway.
inline size_t MaxBytesToCopyToCord(absl::Cord& dest) {
  if (dest.empty()) return kMaxBytesToCopyToEmptyCord;
  return kMaxBytesToCopyToNonEmptyCord;
}

// Copies `src` to `dest[]`.
void CopyCordToArray(const absl::Cord& src, char* dest);

// Appends `src` to `dest`.
void AppendCordToString(const absl::Cord& src, std::string& dest);

// Variants of `absl::Cord` operations with different block sizing tradeoffs:
//  * `MakeBlockyCord(src)` is like `absl::Cord(src)`.
//  * `AssignToBlockyCord(src, dest)` is like `dest = src`.
//  * `AppendToBlockyCord(src, dest)` is like `dest.Append(src)`.
//  * `PrependToBlockyCord(src, dest)` is like `dest.Prepend(src)`.
//
// They avoid splitting `src` into 4083-byte fragments and avoid overallocation,
// without guarantees.
absl::Cord MakeBlockyCord(absl::string_view src);
void AssignToBlockyCord(absl::string_view src, absl::Cord& dest);
void AppendToBlockyCord(absl::string_view src, absl::Cord& dest);
void PrependToBlockyCord(absl::string_view src, absl::Cord& dest);

// Returns usable size provided by `absl::CordBuffer::CreateWithCustomLimit()`
// called with `kCordBufferBlockSize` and `capacity`. Does not have to be
// accurate.
inline size_t CordBufferSizeForCapacity(size_t capacity) {
  if (capacity >= kCordBufferMaxSize) return kCordBufferMaxSize;
  if (capacity <= absl::CordBuffer::kDefaultLimit) return capacity;
  if (!absl::has_single_bit(capacity)) {
    static constexpr size_t kMaxPageSlop = 128;
    const size_t rounded_up = size_t{1} << absl::bit_width(capacity - 1);
    const size_t slop = rounded_up - capacity;
    if (slop >= kFlatOverhead && slop <= kMaxPageSlop + kFlatOverhead) {
      capacity = rounded_up;
    } else {
      const size_t rounded_down = size_t{1} << (absl::bit_width(capacity) - 1);
      capacity = rounded_down;
    }
  }
  return capacity - kFlatOverhead;
}

}  // namespace riegeli::cord_internal

#endif  // RIEGELI_BASE_CORD_UTILS_H_
