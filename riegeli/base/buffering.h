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

#ifndef RIEGELI_BASE_BUFFERING_H_
#define RIEGELI_BASE_BUFFERING_H_

#include <stddef.h>

#include "absl/base/optimization.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/types.h"

namespace riegeli {

// Typical bounds of sizes of memory blocks holding pieces of data in objects.
inline constexpr size_t kDefaultMinBlockSize = 256;
inline constexpr size_t kDefaultMaxBlockSize = size_t{64} << 10;

// When deciding whether to copy an array of bytes or share memory, prefer
// copying up to this length.
//
// Copying can often be done in an inlined fast path. Sharing has more overhead,
// especially in a virtual slow path, so copying sufficiently short lengths
// performs better.
inline constexpr size_t kMaxBytesToCopy = 255;

// Recommends the length of a buffer by modifying the base recommendation.
//
// If `pos` did not pass `size_hint` yet, returns the remaining length instead
// of `base_length`.
inline Position ApplySizeHint(Position base_length,
                              absl::optional<Position> size_hint,
                              Position pos) {
  if (size_hint != absl::nullopt && pos <= *size_hint) return *size_hint - pos;
  return base_length;
}

// Recommends the length of a buffer by modifying the base recommendation.
//
// The following constraints are applied, in the order of weakest to strongest:
//  * At least `recommended_length`.
//  * At most `max_length`.
//  * At least `min_length`.
inline size_t ApplyBufferConstraints(Position base_length, size_t min_length,
                                     size_t recommended_length,
                                     size_t max_length) {
  return UnsignedClamp(UnsignedMax(base_length, recommended_length), min_length,
                       max_length);
}

// Heuristics for whether a data structure with `allocated` bytes utilizing
// `used` bytes for actual data is considered wasteful: `allocated` is larger
// than `2 * used + kDefaultMinBlockSize` (256).
inline bool Wasteful(size_t allocated, size_t used) {
  if (ABSL_PREDICT_FALSE(used > allocated)) return false;
  const size_t unused = allocated - used;
  if (ABSL_PREDICT_TRUE(unused <= kDefaultMinBlockSize)) return false;
  return unused - riegeli::kDefaultMinBlockSize > used;
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_BUFFERING_H_
