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
#include "absl/types/optional.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/ordered_varint/ordered_varint.h"

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

// Reads an ordered varint.
//
// Returns `absl::nullopt` on failure, with the current position unchanged.
absl::optional<uint32_t> ReadOrderedVarint32(Reader& src);
absl::optional<uint64_t> ReadOrderedVarint64(Reader& src);

// Reads a varint.
//
// Accepts only the canonical representation, i.e. the shortest: if length > 1
// then the decoded value must be at least 1 << ((length - 1) * 7).
//
// Returns `absl::nullopt` on failure, with the current position unchanged.
absl::optional<uint32_t> ReadCanonicalOrderedVarint32(Reader& src);
absl::optional<uint64_t> ReadCanonicalOrderedVarint64(Reader& src);

// Implementation details follow.

namespace internal {

absl::optional<uint32_t> ReadOrderedVarint32Slow(Reader& src);
absl::optional<uint64_t> ReadOrderedVarint64Slow(Reader& src);

absl::optional<uint32_t> ReadCanonicalOrderedVarint32Slow(Reader& src);
absl::optional<uint64_t> ReadCanonicalOrderedVarint64Slow(Reader& src);

}  // namespace internal

inline absl::optional<uint32_t> ReadOrderedVarint32(Reader& src) {
  if (ABSL_PREDICT_FALSE(!src.Pull(1, kMaxLengthOrderedVarint32))) {
    return absl::nullopt;
  }
  const uint8_t first_byte = static_cast<uint8_t>(*src.cursor());
  if (ABSL_PREDICT_TRUE(first_byte < 0x80)) {
    src.move_cursor(1);
    return first_byte;
  }
  return internal::ReadOrderedVarint32Slow(src);
}

inline absl::optional<uint64_t> ReadOrderedVarint64(Reader& src) {
  if (ABSL_PREDICT_FALSE(!src.Pull(1, kMaxLengthOrderedVarint64))) {
    return absl::nullopt;
  }
  const uint8_t first_byte = static_cast<uint8_t>(*src.cursor());
  if (ABSL_PREDICT_TRUE(first_byte < 0x80)) {
    src.move_cursor(1);
    return first_byte;
  }
  return internal::ReadOrderedVarint64Slow(src);
}

inline absl::optional<uint32_t> ReadCanonicalOrderedVarint32(Reader& src) {
  if (ABSL_PREDICT_FALSE(!src.Pull(1, kMaxLengthOrderedVarint32))) {
    return absl::nullopt;
  }
  const uint8_t first_byte = static_cast<uint8_t>(*src.cursor());
  if (ABSL_PREDICT_TRUE(first_byte < 0x80)) {
    src.move_cursor(1);
    return first_byte;
  }
  return internal::ReadCanonicalOrderedVarint32Slow(src);
}

inline absl::optional<uint64_t> ReadCanonicalOrderedVarint64(Reader& src) {
  if (ABSL_PREDICT_FALSE(!src.Pull(1, kMaxLengthOrderedVarint64))) {
    return absl::nullopt;
  }
  const uint8_t first_byte = static_cast<uint8_t>(*src.cursor());
  if (ABSL_PREDICT_TRUE(first_byte < 0x80)) {
    src.move_cursor(1);
    return first_byte;
  }
  return internal::ReadCanonicalOrderedVarint64Slow(src);
}

}  // namespace riegeli

#endif  // RIEGELI_ORDERED_VARINT_ORDERED_VARINT_READING_H_
