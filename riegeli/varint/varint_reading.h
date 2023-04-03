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

#ifndef RIEGELI_VARINT_VARINT_READING_H_
#define RIEGELI_VARINT_VARINT_READING_H_

#include <stddef.h>
#include <stdint.h>

#include "absl/base/optimization.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/constexpr.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/varint/varint_internal.h"  // IWYU pragma: export

namespace riegeli {

// Unless stated otherwise, reading a varint tolerates representations
// which are not the shortest, but rejects representations longer than
// `kMaxLengthVarint{32,64}` bytes or with bits set outside the range of
// possible values.

// Reads a varint. This corresponds to protobuf types `{int,uint}{32,64}`
// (with a cast needed in the case of `int{32,64}`).
//
// Warning: protobuf writes values of type `int32` by casting them to `uint64`,
// not `uint32` (negative values take 10 bytes, not 5), hence they must be read
// with `ReadVarint64()`, not `ReadVarint32()`, if negative values are possible.
//
// Return values:
//  * `true`                     - success (`dest` is set)
//  * `false` (when `src.ok()`)  - source ends too early
//                                 (`src` position is unchanged,
//                                 `dest` is undefined)
//  * `false` (when `!src.ok()`) - failure
//                                 (`src` position is unchanged,
//                                 `dest` is undefined)
bool ReadVarint32(Reader& src, uint32_t& dest);
bool ReadVarint64(Reader& src, uint64_t& dest);

// Reads a signed varint (zigzag-encoded). This corresponds to protobuf types
// `sint{32,64}`.
//
// Return values:
//  * `true`                     - success (`dest` is set)
//  * `false` (when `src.ok()`)  - source ends too early
//                                 (`src` position is unchanged,
//                                 `dest` is undefined)
//  * `false` (when `!src.ok()`) - failure
//                                 (`src` position is unchanged,
//                                 `dest` is undefined)
bool ReadVarintSigned32(Reader& src, uint32_t& dest);
bool ReadVarintSigned64(Reader& src, uint64_t& dest);

// Reads a varint. This corresponds to protobuf types `{int,uint}{32,64}`
// (with a cast needed in the case of `int{32,64}`).
//
// Accepts only the canonical representation, i.e. the shortest: rejecting a
// trailing zero byte, except for 0 itself.
//
// Warning: protobuf writes values of type `int32` by casting them to `uint64`,
// not `uint32` (negative values take 10 bytes, not 5), hence they must be read
// with `ReadCanonicalVarint64()`, not `ReadCanonicalVarint32()`, if negative
// values are possible.
//
// Return values:
//  * `true`                     - success (`dest` is set)
//  * `false` (when `src.ok()`)  - source ends too early
//                                 (`src` position is unchanged,
//                                 `dest` is undefined)
//  * `false` (when `!src.ok()`) - failure
//                                 (`src` position is unchanged,
//                                 `dest` is undefined)
bool ReadCanonicalVarint32(Reader& src, uint32_t& dest);
bool ReadCanonicalVarint64(Reader& src, uint64_t& dest);

// Reads a varint from an array. This corresponds to protobuf types
// `{int,uint}{32,64}` (with a cast needed in the case of `int{32,64}`).
//
// Warning: protobuf writes values of type `int32` by casting them to `uint64`,
// not `uint32` (negative values take 10 bytes, not 5), hence they must be read
// with `ReadVarint64()`, not `ReadVarint32()`, if negative values are possible.
//
// Return values:
//  * updated `src`   - success (`dest` is set)
//  * `absl::nullopt` - source ends (dest` is undefined)
absl::optional<const char*> ReadVarint32(const char* src, const char* limit,
                                         uint32_t& dest);
absl::optional<const char*> ReadVarint64(const char* src, const char* limit,
                                         uint64_t& dest);

// Reads a signed varint (zigzag-encoded) from an array. This corresponds to
// protobuf types `sint{32,64}`.
//
// Return values:
//  * updated `src`   - success (`dest` is set)
//  * `absl::nullopt` - source ends (dest` is undefined)
absl::optional<const char*> ReadVarintSigned32(const char* src,
                                               const char* limit,
                                               int32_t& dest);
absl::optional<const char*> ReadVarintSigned64(const char* src,
                                               const char* limit,
                                               int64_t& dest);

// Copies a varint to an array.
//
// Writes up to `kMaxLengthVarint{32,64}` bytes to `dest[]`.
//
// Return values:
//  * varint length                      - success (`dest[]` is filled)
//  * `absl::nullopt` (when `src.ok()`)  - source ends too early
//                                         (`src` position is unchanged,
//                                        `dest[]` is undefined)
//  * `absl::nullopt` (when `!src.ok()`) - failure
//                                         (`src` position is unchanged,
//                                         `dest[]` is undefined)
absl::optional<size_t> CopyVarint32(Reader& src, char* dest);
absl::optional<size_t> CopyVarint64(Reader& src, char* dest);

// Copies a varint from an array to an array.
//
// Writes up to `kMaxLengthVarint{32,64}` bytes to `dest[]`.
//
// Return values:
//  * varint length   - success (`dest[]` is filled)
//  * `absl::nullopt` - source ends (`dest[]` is undefined)
absl::optional<size_t> CopyVarint32(const char* src, const char* limit,
                                    char* dest);
absl::optional<size_t> CopyVarint64(const char* src, const char* limit,
                                    char* dest);

// Implementation details follow.

namespace varint_internal {

inline int32_t DecodeSint32(uint32_t repr) {
  return static_cast<int32_t>((repr >> 1) ^ (~(repr & 1) + 1));
}

inline int64_t DecodeSint64(uint64_t repr) {
  return static_cast<int64_t>((repr >> 1) ^ (~(repr & 1) + 1));
}

template <bool canonical>
bool ReadVarint32Slow(Reader& src, uint32_t& dest);
template <bool canonical>
bool ReadVarint64Slow(Reader& src, uint64_t& dest);

extern template bool ReadVarint32Slow<false>(Reader& src, uint32_t& dest);
extern template bool ReadVarint32Slow<true>(Reader& src, uint32_t& dest);
extern template bool ReadVarint64Slow<false>(Reader& src, uint64_t& dest);
extern template bool ReadVarint64Slow<true>(Reader& src, uint64_t& dest);

}  // namespace varint_internal

inline bool ReadVarint32(Reader& src, uint32_t& dest) {
  if (ABSL_PREDICT_FALSE(src.available() < kMaxLengthVarint32)) {
    return varint_internal::ReadVarint32Slow<false>(src, dest);
  }
  const absl::optional<const char*> cursor =
      ReadVarint32(src.cursor(), src.limit(), dest);
  if (ABSL_PREDICT_FALSE(cursor == absl::nullopt)) return false;
  src.set_cursor(*cursor);
  return true;
}

inline bool ReadVarint64(Reader& src, uint64_t& dest) {
  if (ABSL_PREDICT_FALSE(src.available() < kMaxLengthVarint64)) {
    return varint_internal::ReadVarint64Slow<false>(src, dest);
  }
  const absl::optional<const char*> cursor =
      ReadVarint64(src.cursor(), src.limit(), dest);
  if (ABSL_PREDICT_FALSE(cursor == absl::nullopt)) return false;
  src.set_cursor(*cursor);
  return true;
}

inline bool ReadVarintSigned32(Reader& src, int32_t& dest) {
  uint32_t unsigned_dest;
  if (ABSL_PREDICT_FALSE(!ReadVarint32(src, unsigned_dest))) return false;
  dest = varint_internal::DecodeSint32(unsigned_dest);
  return true;
}

inline bool ReadVarintSigned64(Reader& src, int64_t& dest) {
  uint64_t unsigned_dest;
  if (ABSL_PREDICT_FALSE(!ReadVarint64(src, unsigned_dest))) return false;
  dest = varint_internal::DecodeSint64(unsigned_dest);
  return true;
}

inline bool ReadCanonicalVarint32(Reader& src, uint32_t& dest) {
  if (ABSL_PREDICT_FALSE(src.available() < kMaxLengthVarint32)) {
    return varint_internal::ReadVarint32Slow<true>(src, dest);
  }
  const uint8_t first_byte = static_cast<uint8_t>(*src.cursor());
  if (first_byte < 0x80) {
    // Any byte with the highest bit clear is accepted as the only byte,
    // including 0 itself.
    dest = first_byte;
    src.move_cursor(1);
    return true;
  }
  const absl::optional<const char*> cursor =
      ReadVarint32(src.cursor(), src.limit(), dest);
  if (ABSL_PREDICT_FALSE(cursor == absl::nullopt)) return false;
  if (ABSL_PREDICT_FALSE((*cursor)[-1] == 0)) return false;
  src.set_cursor(*cursor);
  return true;
}

inline bool ReadCanonicalVarint64(Reader& src, uint64_t& dest) {
  if (ABSL_PREDICT_FALSE(src.available() < kMaxLengthVarint64)) {
    return varint_internal::ReadVarint64Slow<true>(src, dest);
  }
  const uint8_t first_byte = static_cast<uint8_t>(*src.cursor());
  if (first_byte < 0x80) {
    // Any byte with the highest bit clear is accepted as the only byte,
    // including 0 itself.
    dest = first_byte;
    src.move_cursor(1);
    return true;
  }
  const absl::optional<const char*> cursor =
      ReadVarint64(src.cursor(), src.limit(), dest);
  if (ABSL_PREDICT_FALSE(cursor == absl::nullopt)) return false;
  if (ABSL_PREDICT_FALSE((*cursor)[-1] == 0)) return false;
  src.set_cursor(*cursor);
  return true;
}

namespace varint_internal {

RIEGELI_INLINE_CONSTEXPR(size_t, kReadVarintSlowThreshold, 3 * 7);

absl::optional<const char*> ReadVarint32Slow(const char* src, const char* limit,
                                             uint32_t acc, uint32_t& dest);
absl::optional<const char*> ReadVarint64Slow(const char* src, const char* limit,
                                             uint64_t acc, uint64_t& dest);

}  // namespace varint_internal

inline absl::optional<const char*> ReadVarint32(const char* src,
                                                const char* limit,
                                                uint32_t& dest) {
  if (ABSL_PREDICT_FALSE(src == limit)) return absl::nullopt;
  uint8_t byte = static_cast<uint8_t>(*src++);
  uint32_t acc{byte};
  size_t shift = 7;
  while (byte >= 0x80) {
    if (ABSL_PREDICT_FALSE(shift ==
                           varint_internal::kReadVarintSlowThreshold)) {
      return varint_internal::ReadVarint32Slow(src, limit, acc, dest);
    }
    if (ABSL_PREDICT_FALSE(src == limit)) return absl::nullopt;
    byte = static_cast<uint8_t>(*src++);
    acc += (uint32_t{byte} - 1) << shift;
    shift += 7;
  }
  dest = acc;
  return src;
}

inline absl::optional<const char*> ReadVarint64(const char* src,
                                                const char* limit,
                                                uint64_t& dest) {
  if (ABSL_PREDICT_FALSE(src == limit)) return absl::nullopt;
  uint8_t byte = static_cast<uint8_t>(*src++);
  uint64_t acc{byte};
  size_t shift = 7;
  while (byte >= 0x80) {
    if (ABSL_PREDICT_FALSE(shift ==
                           varint_internal::kReadVarintSlowThreshold)) {
      return varint_internal::ReadVarint64Slow(src, limit, acc, dest);
    }
    if (ABSL_PREDICT_FALSE(src == limit)) return absl::nullopt;
    byte = static_cast<uint8_t>(*src++);
    acc += (uint64_t{byte} - 1) << shift;
    shift += 7;
  }
  dest = acc;
  return src;
}

inline absl::optional<const char*> ReadVarintSigned32(const char* src,
                                                      const char* limit,
                                                      int32_t& dest) {
  uint32_t unsigned_dest;
  const absl::optional<const char*> cursor =
      ReadVarint32(src, limit, unsigned_dest);
  if (ABSL_PREDICT_FALSE(cursor == absl::nullopt)) return absl::nullopt;
  dest = varint_internal::DecodeSint32(unsigned_dest);
  return *cursor;
}

inline absl::optional<const char*> ReadVarintSigned64(const char* src,
                                                      const char* limit,
                                                      int64_t& dest) {
  uint64_t unsigned_dest;
  const absl::optional<const char*> cursor =
      ReadVarint64(src, limit, unsigned_dest);
  if (ABSL_PREDICT_FALSE(cursor == absl::nullopt)) return absl::nullopt;
  dest = varint_internal::DecodeSint64(unsigned_dest);
  return *cursor;
}

inline absl::optional<size_t> CopyVarint32(Reader& src, char* dest) {
  if (ABSL_PREDICT_FALSE(src.available() < kMaxLengthVarint32)) {
    if (src.available() > 0 && static_cast<uint8_t>(src.limit()[-1]) < 0x80) {
      // The buffer contains a potential varint terminator. Avoid pulling the
      // maximum varint length which can be expensive.
    } else {
      src.Pull(kMaxLengthVarint32);
    }
  }
  const absl::optional<size_t> length =
      CopyVarint32(src.cursor(), src.limit(), dest);
  if (ABSL_PREDICT_FALSE(length == absl::nullopt)) return absl::nullopt;
  src.move_cursor(*length);
  return *length;
}

inline absl::optional<size_t> CopyVarint64(Reader& src, char* dest) {
  if (ABSL_PREDICT_FALSE(src.available() < kMaxLengthVarint64)) {
    if (src.available() > 0 && static_cast<uint8_t>(src.limit()[-1]) < 0x80) {
      // The buffer contains a potential varint terminator. Avoid pulling the
      // maximum varint length which can be expensive.
    } else {
      src.Pull(kMaxLengthVarint64);
    }
  }
  const absl::optional<size_t> length =
      CopyVarint64(src.cursor(), src.limit(), dest);
  if (ABSL_PREDICT_FALSE(length == absl::nullopt)) return absl::nullopt;
  src.move_cursor(*length);
  return *length;
}

inline absl::optional<size_t> CopyVarint32(const char* src, const char* limit,
                                           char* dest) {
  if (ABSL_PREDICT_FALSE(src == limit)) return absl::nullopt;
  const char* start = src;
  uint8_t byte = static_cast<uint8_t>(*src++);
  *dest++ = static_cast<char>(byte);
  size_t remaining = kMaxLengthVarint32 - 1;
  while (byte >= 0x80) {
    if (ABSL_PREDICT_FALSE(src == limit)) return absl::nullopt;
    byte = static_cast<uint8_t>(*src++);
    *dest++ = static_cast<char>(byte);
    if (ABSL_PREDICT_FALSE(--remaining == 0)) {
      // Last possible byte.
      if (ABSL_PREDICT_FALSE(
              byte >= uint8_t{1} << (32 - (kMaxLengthVarint32 - 1) * 7))) {
        // The representation is longer than `kMaxLengthVarint32`
        // or the represented value does not fit in `uint32_t`.
        return absl::nullopt;
      }
      break;
    }
  }
  return PtrDistance(start, src);
}

inline absl::optional<size_t> CopyVarint64(const char* src, const char* limit,
                                           char* dest) {
  if (ABSL_PREDICT_FALSE(src == limit)) return absl::nullopt;
  const char* start = src;
  uint8_t byte = static_cast<uint8_t>(*src++);
  *dest++ = static_cast<char>(byte);
  size_t remaining = kMaxLengthVarint64 - 1;
  while (byte >= 0x80) {
    if (ABSL_PREDICT_FALSE(src == limit)) return absl::nullopt;
    byte = static_cast<uint8_t>(*src++);
    *dest++ = static_cast<char>(byte);
    if (ABSL_PREDICT_FALSE(--remaining == 0)) {
      // Last possible byte.
      if (ABSL_PREDICT_FALSE(
              byte >= uint8_t{1} << (64 - (kMaxLengthVarint64 - 1) * 7))) {
        // The representation is longer than `kMaxLengthVarint64`
        // or the represented value does not fit in `uint64_t`.
        return absl::nullopt;
      }
      break;
    }
  }
  return PtrDistance(start, src);
}

}  // namespace riegeli

#endif  // RIEGELI_VARINT_VARINT_READING_H_
