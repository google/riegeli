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

#ifndef RIEGELI_ORDERED_VARINT_ORDERED_VARINT_READING_H_
#define RIEGELI_ORDERED_VARINT_ORDERED_VARINT_READING_H_

#include <stdint.h>

#include "absl/base/optimization.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/ordered_varint/ordered_varint_internal.h"  // IWYU pragma: export

namespace riegeli {

// An ordered varint represents an unsigned integer in a variable number of
// bytes, such that smaller values are represented by lexicographically smaller
// strings, and also smaller values tend to be represented by shorter strings.
//
// Decoding a 64-bit value X:
//
// Let F = the first byte of the encoding.
//
// Let N = the number of the highest order one bits in F, plus 1.
// N is in the range [1..9]. X will be decoded from N bytes.
//
// Bits of X, from highest to lowest, consist of:
//  * 8 - N lower order bits of F, if N < 8
//  * the remaining N - 1 bytes of the encoding in big endian
//
// Only the canonical representation is accepted, i.e. the shortest: if N > 1
// then X must be at least 1 << ((N - 1) * 7).

// Reads an ordered varint.
//
// Return values:
//  * `true`                     - success (`dest` is set)
//  * `false` (when `src.ok()`)  - source ends
//                                 (`src` position is unchanged,
//                                 `dest` is undefined)
//  * `false` (when `!src.ok()`) - failure
//                                 (`src` position is unchanged,
//                                 `dest` is undefined)
bool ReadOrderedVarint32(Reader& src, uint32_t& dest);
bool ReadOrderedVarint64(Reader& src, uint64_t& dest);

// Implementation details follow.

namespace ordered_varint_internal {

bool ReadOrderedVarint32Slow(Reader& src, uint32_t& dest);
bool ReadOrderedVarint64Slow(Reader& src, uint64_t& dest);

}  // namespace ordered_varint_internal

inline bool ReadOrderedVarint32(Reader& src, uint32_t& dest) {
  if (ABSL_PREDICT_FALSE(!src.Pull(1, kMaxLengthOrderedVarint32))) return false;
  const uint8_t first_byte = static_cast<uint8_t>(*src.cursor());
  if (ABSL_PREDICT_TRUE(first_byte < 0x80)) {
    dest = first_byte;
    src.move_cursor(1);
    return true;
  }
  return ordered_varint_internal::ReadOrderedVarint32Slow(src, dest);
}

inline bool ReadOrderedVarint64(Reader& src, uint64_t& dest) {
  if (ABSL_PREDICT_FALSE(!src.Pull(1, kMaxLengthOrderedVarint64))) return false;
  const uint8_t first_byte = static_cast<uint8_t>(*src.cursor());
  if (ABSL_PREDICT_TRUE(first_byte < 0x80)) {
    dest = first_byte;
    src.move_cursor(1);
    return true;
  }
  return ordered_varint_internal::ReadOrderedVarint64Slow(src, dest);
}

}  // namespace riegeli

#endif  // RIEGELI_ORDERED_VARINT_ORDERED_VARINT_READING_H_
