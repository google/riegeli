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

#include <optional>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "riegeli/base/arithmetic.h"
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
//  * `false` (when `src.ok()`)  - source ends too early or varint is invalid
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
//  * `false` (when `src.ok()`)  - source ends too early or varint is invalid
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
//  * `false` (when `src.ok()`)  - source ends too early or varint is invalid
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
//  * positive `length` - success, `length` bytes read (`dest` is set)
//  * 0                 - source ends too early or varint is invalid
//                        (`dest` is undefined)
size_t ReadVarint32(const char* src, size_t available, uint32_t& dest);
size_t ReadVarint64(const char* src, size_t available, uint64_t& dest);

// Reads a varint from an array. This corresponds to protobuf types
// `{int,uint}{32,64}` (with a cast needed in the case of `int{32,64}`).
//
// Warning: protobuf writes values of type `int32` by casting them to `uint64`,
// not `uint32` (negative values take 10 bytes, not 5), hence they must be read
// with `ReadVarint64()`, not `ReadVarint32()`, if negative values are possible.
//
// Return values:
//  * updated `src`  - success (`dest` is set)
//  * `std::nullopt` - source ends too early or varint is invalid
//                     (`dest` is undefined)
ABSL_DEPRECATED(
    "Use ReadVarint32() overload with size_t parameter and size_t result")
std::optional<const char*> ReadVarint32(const char* src, const char* limit,
                                        uint32_t& dest);
ABSL_DEPRECATED(
    "Use ReadVarint64() overload with size_t parameter and size_t result")
std::optional<const char*> ReadVarint64(const char* src, const char* limit,
                                        uint64_t& dest);

// Reads a signed varint (zigzag-encoded) from an array. This corresponds to
// protobuf types `sint{32,64}`.
//
// Return values:
//  * positive `length` - success, `length` bytes read (`dest` is set)
//  * 0                 - source ends too early or varint is invalid
//                        (`dest` is undefined)
size_t ReadVarintSigned32(const char* src, size_t available, int32_t& dest);
size_t ReadVarintSigned64(const char* src, size_t available, int64_t& dest);

// Reads a varint from an array. This corresponds to protobuf types
// `{int,uint}{32,64}` (with a cast needed in the case of `int{32,64}`).
//
// Accepts only the canonical representation, i.e. the shortest: rejecting a
// trailing zero byte, except for 0 itself.
//
// Warning: protobuf writes values of type `int32` by casting them to `uint64`,
// not `uint32` (negative values take 10 bytes, not 5), hence they must be read
// with `ReadVarint64()`, not `ReadVarint32()`, if negative values are possible.
//
// Return values:
//  * positive `length` - success, `length` bytes read (`dest` is set)
//  * 0                 - source ends too early or varint is invalid
//                        (`dest` is undefined)
size_t ReadCanonicalVarint32(const char* src, size_t available, uint32_t& dest);
size_t ReadCanonicalVarint64(const char* src, size_t available, uint64_t& dest);

// Copies a varint to an array.
//
// Writes up to `kMaxLengthVarint{32,64}` bytes to `dest[]`.
//
// Return values:
//  * varint length                      - success (`dest[]` is filled)
//  * `std::nullopt` (when `src.ok()`)  - source ends too early
//                                         (`src` position is unchanged,
//                                        `dest[]` is undefined)
//  * `std::nullopt` (when `!src.ok()`) - failure
//                                         (`src` position is unchanged,
//                                         `dest[]` is undefined)
std::optional<size_t> CopyVarint32(Reader& src, char* dest);
std::optional<size_t> CopyVarint64(Reader& src, char* dest);

// Copies a varint from an array to an array.
//
// Writes up to `kMaxLengthVarint{32,64}` bytes to `dest[]`.
//
// Return values:
//  * varint length   - success (`dest[]` is filled)
//  * `std::nullopt` - source ends (`dest[]` is undefined)
std::optional<size_t> CopyVarint32(const char* src, const char* limit,
                                   char* dest);
std::optional<size_t> CopyVarint64(const char* src, const char* limit,
                                   char* dest);

// Decodes a signed varint (zigzag-decoding) from an unsigned value read as a
// plain varint. This corresponds to protobuf types `sint{32,64}`.
constexpr int32_t DecodeVarintSigned32(uint32_t repr);
constexpr int64_t DecodeVarintSigned64(uint64_t repr);

// Implementation details follow.

namespace varint_internal {

template <typename T, bool canonical, size_t initial_index>
bool ReadVarintFromReaderBuffer(Reader& src, const char* cursor, T acc,
                                T& dest);

extern template bool ReadVarintFromReaderBuffer<uint32_t, false, 2>(
    Reader& src, const char* cursor, uint32_t acc, uint32_t& dest);
extern template bool ReadVarintFromReaderBuffer<uint64_t, false, 2>(
    Reader& src, const char* cursor, uint64_t acc, uint64_t& dest);
extern template bool ReadVarintFromReaderBuffer<uint32_t, true, 2>(
    Reader& src, const char* cursor, uint32_t acc, uint32_t& dest);
extern template bool ReadVarintFromReaderBuffer<uint64_t, true, 2>(
    Reader& src, const char* cursor, uint64_t acc, uint64_t& dest);

template <typename T, bool canonical, size_t initial_index>
bool ReadVarintFromReader(Reader& src, T acc, T& dest);

extern template bool ReadVarintFromReader<uint32_t, false, 1>(Reader& src,
                                                              uint32_t acc,
                                                              uint32_t& dest);
extern template bool ReadVarintFromReader<uint64_t, false, 1>(Reader& src,
                                                              uint64_t acc,
                                                              uint64_t& dest);
extern template bool ReadVarintFromReader<uint32_t, true, 1>(Reader& src,
                                                             uint32_t acc,
                                                             uint32_t& dest);
extern template bool ReadVarintFromReader<uint64_t, true, 1>(Reader& src,
                                                             uint64_t acc,
                                                             uint64_t& dest);

}  // namespace varint_internal

inline bool ReadVarint32(Reader& src, uint32_t& dest) {
  if (ABSL_PREDICT_FALSE(!src.Pull(1, kMaxLengthVarint32))) return false;
  const uint32_t byte0 = uint32_t{static_cast<uint8_t>(src.cursor()[0])};
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) {
    src.move_cursor(1);
    dest = byte0;
    return true;
  }
  if (ABSL_PREDICT_TRUE(src.available() >= 2)) {
    const uint32_t byte1 = uint32_t{static_cast<uint8_t>(src.cursor()[1])};
    const uint32_t acc = byte0 + ((byte1 - 1) << 7);
    if (ABSL_PREDICT_TRUE(byte1 < 0x80)) {
      src.move_cursor(2);
      dest = acc;
      return true;
    }
    return varint_internal::ReadVarintFromReaderBuffer<uint32_t,
                                                       /*canonical=*/false, 2>(
        src, src.cursor(), acc, dest);
  }
  return varint_internal::ReadVarintFromReader<uint32_t,
                                               /*canonical=*/false, 1>(
      src, byte0, dest);
}

inline bool ReadVarint64(Reader& src, uint64_t& dest) {
  if (ABSL_PREDICT_FALSE(!src.Pull(1, kMaxLengthVarint64))) return false;
  const uint64_t byte0 = uint64_t{static_cast<uint8_t>(src.cursor()[0])};
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) {
    src.move_cursor(1);
    dest = byte0;
    return true;
  }
  if (ABSL_PREDICT_TRUE(src.available() >= 2)) {
    const uint64_t byte1 = uint64_t{static_cast<uint8_t>(src.cursor()[1])};
    const uint64_t acc = byte0 + ((byte1 - 1) << 7);
    if (ABSL_PREDICT_TRUE(byte1 < 0x80)) {
      src.move_cursor(2);
      dest = acc;
      return true;
    }
    return varint_internal::ReadVarintFromReaderBuffer<uint64_t,
                                                       /*canonical=*/false, 2>(
        src, src.cursor(), acc, dest);
  }
  return varint_internal::ReadVarintFromReader<uint64_t,
                                               /*canonical=*/false, 1>(
      src, byte0, dest);
}

inline bool ReadVarintSigned32(Reader& src, int32_t& dest) {
  uint32_t unsigned_dest;
  if (ABSL_PREDICT_FALSE(!ReadVarint32(src, unsigned_dest))) return false;
  dest = DecodeVarintSigned32(unsigned_dest);
  return true;
}

inline bool ReadVarintSigned64(Reader& src, int64_t& dest) {
  uint64_t unsigned_dest;
  if (ABSL_PREDICT_FALSE(!ReadVarint64(src, unsigned_dest))) return false;
  dest = DecodeVarintSigned64(unsigned_dest);
  return true;
}

inline bool ReadCanonicalVarint32(Reader& src, uint32_t& dest) {
  if (ABSL_PREDICT_FALSE(!src.Pull(1, kMaxLengthVarint32))) return false;
  const uint32_t byte0 = uint32_t{static_cast<uint8_t>(src.cursor()[0])};
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) {
    src.move_cursor(1);
    dest = byte0;
    return true;
  }
  if (ABSL_PREDICT_TRUE(src.available() >= 2)) {
    const uint32_t byte1 = uint32_t{static_cast<uint8_t>(src.cursor()[1])};
    const uint32_t acc = byte0 + ((byte1 - 1) << 7);
    if (ABSL_PREDICT_TRUE(byte1 < 0x80)) {
      if (ABSL_PREDICT_FALSE(byte1 == 0)) return false;
      src.move_cursor(2);
      dest = acc;
      return true;
    }
    return varint_internal::ReadVarintFromReaderBuffer<uint32_t,
                                                       /*canonical=*/true, 2>(
        src, src.cursor(), acc, dest);
  }
  return varint_internal::ReadVarintFromReader<uint32_t,
                                               /*canonical=*/true, 1>(
      src, byte0, dest);
}

inline bool ReadCanonicalVarint64(Reader& src, uint64_t& dest) {
  if (ABSL_PREDICT_FALSE(!src.Pull(1, kMaxLengthVarint64))) return false;
  const uint64_t byte0 = uint64_t{static_cast<uint8_t>(src.cursor()[0])};
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) {
    src.move_cursor(1);
    dest = byte0;
    return true;
  }
  if (ABSL_PREDICT_TRUE(src.available() >= 2)) {
    const uint64_t byte1 = uint64_t{static_cast<uint8_t>(src.cursor()[1])};
    const uint64_t acc = byte0 + ((byte1 - 1) << 7);
    if (ABSL_PREDICT_TRUE(byte1 < 0x80)) {
      if (ABSL_PREDICT_FALSE(byte1 == 0)) return false;
      src.move_cursor(2);
      dest = acc;
      return true;
    }
    return varint_internal::ReadVarintFromReaderBuffer<uint64_t,
                                                       /*canonical=*/true, 2>(
        src, src.cursor(), acc, dest);
  }
  return varint_internal::ReadVarintFromReader<uint64_t,
                                               /*canonical=*/true, 1>(
      src, byte0, dest);
}

namespace varint_internal {

template <typename T, bool canonical, size_t initial_index>
size_t ReadVarintFromArray(const char* src, size_t available, T acc, T& dest);

extern template size_t ReadVarintFromArray<uint32_t, false, 2>(const char* src,
                                                               size_t available,
                                                               uint32_t acc,
                                                               uint32_t& dest);
extern template size_t ReadVarintFromArray<uint64_t, false, 2>(const char* src,
                                                               size_t available,
                                                               uint64_t acc,
                                                               uint64_t& dest);
extern template size_t ReadVarintFromArray<uint32_t, true, 2>(const char* src,
                                                              size_t available,
                                                              uint32_t acc,
                                                              uint32_t& dest);
extern template size_t ReadVarintFromArray<uint64_t, true, 2>(const char* src,
                                                              size_t available,
                                                              uint64_t acc,
                                                              uint64_t& dest);

}  // namespace varint_internal

inline size_t ReadVarint32(const char* src, size_t available, uint32_t& dest) {
  if (ABSL_PREDICT_FALSE(available == 0)) return 0;
  const uint32_t byte0 = uint32_t{static_cast<uint8_t>(src[0])};
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) {
    dest = byte0;
    return 1;
  }
  if (ABSL_PREDICT_FALSE(available == 1)) return 0;
  const uint32_t byte1 = uint32_t{static_cast<uint8_t>(src[1])};
  const uint32_t acc = byte0 + ((byte1 - 1) << 7);
  if (ABSL_PREDICT_TRUE(byte1 < 0x80)) {
    dest = acc;
    return 2;
  }
  return varint_internal::ReadVarintFromArray<uint32_t, /*canonical=*/false, 2>(
      src, available, acc, dest);
}

inline size_t ReadVarint64(const char* src, size_t available, uint64_t& dest) {
  if (ABSL_PREDICT_FALSE(available == 0)) return 0;
  const uint64_t byte0 = uint64_t{static_cast<uint8_t>(src[0])};
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) {
    dest = byte0;
    return 1;
  }
  if (ABSL_PREDICT_FALSE(available == 1)) return 0;
  const uint64_t byte1 = uint64_t{static_cast<uint8_t>(src[1])};
  const uint64_t acc = byte0 + ((byte1 - 1) << 7);
  if (ABSL_PREDICT_TRUE(byte1 < 0x80)) {
    dest = acc;
    return 2;
  }
  return varint_internal::ReadVarintFromArray<uint64_t, /*canonical=*/false, 2>(
      src, available, acc, dest);
}

inline size_t ReadVarintSigned32(const char* src, size_t available,
                                 int32_t& dest) {
  uint32_t unsigned_dest;
  const size_t length = ReadVarint32(src, available, unsigned_dest);
  if (ABSL_PREDICT_FALSE(length == 0)) return 0;
  dest = DecodeVarintSigned32(unsigned_dest);
  return length;
}

inline size_t ReadVarintSigned64(const char* src, size_t available,
                                 int64_t& dest) {
  uint64_t unsigned_dest;
  const size_t length = ReadVarint64(src, available, unsigned_dest);
  if (ABSL_PREDICT_FALSE(length == 0)) return 0;
  dest = DecodeVarintSigned64(unsigned_dest);
  return length;
}

inline size_t ReadCanonicalVarint32(const char* src, size_t available,
                                    uint32_t& dest) {
  if (ABSL_PREDICT_FALSE(available == 0)) return 0;
  const uint32_t byte0 = uint32_t{static_cast<uint8_t>(src[0])};
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) {
    dest = byte0;
    return 1;
  }
  if (ABSL_PREDICT_FALSE(available == 1)) return 0;
  const uint32_t byte1 = uint32_t{static_cast<uint8_t>(src[1])};
  const uint32_t acc = byte0 + ((byte1 - 1) << 7);
  if (ABSL_PREDICT_TRUE(byte1 < 0x80)) {
    if (ABSL_PREDICT_FALSE(byte1 == 0)) return 0;
    dest = acc;
    return 2;
  }
  return varint_internal::ReadVarintFromArray<uint32_t, /*canonical=*/true, 2>(
      src, available, acc, dest);
}

inline size_t ReadCanonicalVarint64(const char* src, size_t available,
                                    uint64_t& dest) {
  if (ABSL_PREDICT_FALSE(available == 0)) return 0;
  const uint64_t byte0 = uint64_t{static_cast<uint8_t>(src[0])};
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) {
    dest = byte0;
    return 1;
  }
  if (ABSL_PREDICT_FALSE(available == 1)) return 0;
  const uint64_t byte1 = uint64_t{static_cast<uint8_t>(src[1])};
  const uint64_t acc = byte0 + ((byte1 - 1) << 7);
  if (ABSL_PREDICT_TRUE(byte1 < 0x80)) {
    if (ABSL_PREDICT_FALSE(byte1 == 0)) return 0;
    dest = acc;
    return 2;
  }
  return varint_internal::ReadVarintFromArray<uint64_t, /*canonical=*/true, 2>(
      src, available, acc, dest);
}

inline std::optional<const char*> ReadVarint32(const char* src,
                                               const char* limit,
                                               uint32_t& dest) {
  const size_t length = ReadVarint32(src, PtrDistance(src, limit), dest);
  if (ABSL_PREDICT_FALSE(length == 0)) return std::nullopt;
  return src + length;
}

inline std::optional<const char*> ReadVarint64(const char* src,
                                               const char* limit,
                                               uint64_t& dest) {
  const size_t length = ReadVarint64(src, PtrDistance(src, limit), dest);
  if (ABSL_PREDICT_FALSE(length == 0)) return std::nullopt;
  return src + length;
}

inline std::optional<const char*> ReadVarintSigned32(const char* src,
                                                     const char* limit,
                                                     int32_t& dest) {
  const size_t length = ReadVarintSigned32(src, PtrDistance(src, limit), dest);
  if (ABSL_PREDICT_FALSE(length == 0)) return std::nullopt;
  return src + length;
}

inline std::optional<const char*> ReadVarintSigned64(const char* src,
                                                     const char* limit,
                                                     int64_t& dest) {
  const size_t length = ReadVarintSigned64(src, PtrDistance(src, limit), dest);
  if (ABSL_PREDICT_FALSE(length == 0)) return std::nullopt;
  return src + length;
}

inline std::optional<size_t> CopyVarint32(Reader& src, char* dest) {
  if (ABSL_PREDICT_FALSE(src.available() < kMaxLengthVarint32)) {
    if (src.available() > 0 && static_cast<uint8_t>(src.limit()[-1]) < 0x80) {
      // The buffer contains a potential varint terminator. Avoid pulling the
      // maximum varint length which can be expensive.
    } else {
      src.Pull(kMaxLengthVarint32);
    }
  }
  const std::optional<size_t> length =
      CopyVarint32(src.cursor(), src.limit(), dest);
  if (ABSL_PREDICT_FALSE(length == std::nullopt)) return std::nullopt;
  src.move_cursor(*length);
  return *length;
}

inline std::optional<size_t> CopyVarint64(Reader& src, char* dest) {
  if (ABSL_PREDICT_FALSE(src.available() < kMaxLengthVarint64)) {
    if (src.available() > 0 && static_cast<uint8_t>(src.limit()[-1]) < 0x80) {
      // The buffer contains a potential varint terminator. Avoid pulling the
      // maximum varint length which can be expensive.
    } else {
      src.Pull(kMaxLengthVarint64);
    }
  }
  const std::optional<size_t> length =
      CopyVarint64(src.cursor(), src.limit(), dest);
  if (ABSL_PREDICT_FALSE(length == std::nullopt)) return std::nullopt;
  src.move_cursor(*length);
  return *length;
}

inline std::optional<size_t> CopyVarint32(const char* src, const char* limit,
                                          char* dest) {
  if (ABSL_PREDICT_FALSE(src == limit)) return std::nullopt;
  const char* start = src;
  uint8_t byte = static_cast<uint8_t>(*src++);
  *dest++ = static_cast<char>(byte);
  size_t remaining = kMaxLengthVarint32 - 1;
  while (byte >= 0x80) {
    if (ABSL_PREDICT_FALSE(src == limit)) return std::nullopt;
    byte = static_cast<uint8_t>(*src++);
    *dest++ = static_cast<char>(byte);
    if (ABSL_PREDICT_FALSE(--remaining == 0)) {
      // Last possible byte.
      if (ABSL_PREDICT_FALSE(
              byte >= uint8_t{1} << (32 - (kMaxLengthVarint32 - 1) * 7))) {
        // The representation is longer than `kMaxLengthVarint32`
        // or the represented value does not fit in `uint32_t`.
        return std::nullopt;
      }
      break;
    }
  }
  return PtrDistance(start, src);
}

inline std::optional<size_t> CopyVarint64(const char* src, const char* limit,
                                          char* dest) {
  if (ABSL_PREDICT_FALSE(src == limit)) return std::nullopt;
  const char* start = src;
  uint8_t byte = static_cast<uint8_t>(*src++);
  *dest++ = static_cast<char>(byte);
  size_t remaining = kMaxLengthVarint64 - 1;
  while (byte >= 0x80) {
    if (ABSL_PREDICT_FALSE(src == limit)) return std::nullopt;
    byte = static_cast<uint8_t>(*src++);
    *dest++ = static_cast<char>(byte);
    if (ABSL_PREDICT_FALSE(--remaining == 0)) {
      // Last possible byte.
      if (ABSL_PREDICT_FALSE(
              byte >= uint8_t{1} << (64 - (kMaxLengthVarint64 - 1) * 7))) {
        // The representation is longer than `kMaxLengthVarint64`
        // or the represented value does not fit in `uint64_t`.
        return std::nullopt;
      }
      break;
    }
  }
  return PtrDistance(start, src);
}

constexpr int32_t DecodeVarintSigned32(uint32_t repr) {
  return static_cast<int32_t>((repr >> 1) ^ (~(repr & 1) + 1));
}

constexpr int64_t DecodeVarintSigned64(uint64_t repr) {
  return static_cast<int64_t>((repr >> 1) ^ (~(repr & 1) + 1));
}

}  // namespace riegeli

#endif  // RIEGELI_VARINT_VARINT_READING_H_
