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

#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/constexpr.h"
#include "riegeli/base/types.h"

namespace riegeli {

// Typical bounds of sizes of memory blocks holding pieces of data in objects.
RIEGELI_INLINE_CONSTEXPR(size_t, kDefaultMinBlockSize, 256);
RIEGELI_INLINE_CONSTEXPR(size_t, kDefaultMaxBlockSize, size_t{64} << 10);

// When deciding whether to copy an array of bytes or share memory, prefer
// copying up to this length.
//
// Copying can often be done in an inlined fast path. Sharing has more overhead,
// especially in a virtual slow path, so copying sufficiently short lengths
// performs better.
RIEGELI_INLINE_CONSTEXPR(size_t, kMaxBytesToCopy, 255);

// Recommends the length of a read buffer by modifying the base recommendation
// if a size hint is known.
//
// The base recommendation is `length`. If `pos` did not pass `size_hint` yet,
// returns the remaining length instead, increased by 1 which can be helpful to
// verify that there are indeed no more data.
//
// If `multiple_runs` are predicted, it is assumed that reading might not reach
// the size hint, and thus a size hint may decrease but not increase the
// returned length.
inline Position ApplyReadSizeHint(Position length,
                                  absl::optional<Position> size_hint,
                                  Position pos, bool multiple_runs = false) {
  if (size_hint != absl::nullopt && pos <= *size_hint) {
    const Position remaining_plus_1 =
        SaturatingAdd(*size_hint - pos, Position{1});
    if (multiple_runs) {
      return UnsignedMin(length, remaining_plus_1);
    } else {
      return remaining_plus_1;
    }
  } else {
    return length;
  }
}

// Recommends the length of a write buffer by modifying the base recommendation
// if a size hint is known.
//
// The base recommendation is `length`. If `pos` did not reach `size_hint` yet,
// returns the remaining length instead.
//
// If `multiple_runs` are predicted, it is assumed that writing might not reach
// the size hint, and thus a size hint may decrease but not increase the
// returned length.
inline Position ApplyWriteSizeHint(Position length,
                                   absl::optional<Position> size_hint,
                                   Position pos, bool multiple_runs = false) {
  if (size_hint != absl::nullopt && pos < *size_hint) {
    const Position remaining = *size_hint - pos;
    if (multiple_runs) {
      return UnsignedMin(length, remaining);
    } else {
      return remaining;
    }
  } else {
    return length;
  }
}

// Heuristics for whether a partially filled buffer is wasteful.
//
// Precondition: `used <= total`
inline bool Wasteful(size_t total, size_t used) {
  RIEGELI_ASSERT_LE(used, total) << "Failed precondition of Wasteful(): "
                                    "used size larger than total size";
  return total - used > UnsignedMax(used, kDefaultMinBlockSize);
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_BUFFERING_H_
