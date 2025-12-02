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
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
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

// Reads a varint, at most `available` bytes long. This corresponds to protobuf
// types `{int,uint}{32,64}` (with a cast needed in the case of `int{32,64}`).
//
// Warning: protobuf writes values of type `int32` by casting them to `uint64`,
// not `uint32` (negative values take 10 bytes, not 5), hence they must be read
// with `ReadVarint64()`, not `ReadVarint32()`, if negative values are possible.
//
// Return values:
//  * `true`  - success (`dest` is set)
//  * `false` - source ends too early or varint is invalid
//              (`src` moved less than `available` unless `available == 0`,
//              `dest` is undefined)
bool ReadVarint32(absl::Cord::CharIterator& src, size_t available,
                  uint32_t& dest);
bool ReadVarint64(absl::Cord::CharIterator& src, size_t available,
                  uint64_t& dest);

// Reads a signed varint (zigzag-encoded), at most `available` bytes long.
// This corresponds to protobuf types `sint{32,64}`.
//
// Return values:
//  * `true`  - success (`dest` is set)
//  * `false` - source ends too early or varint is invalid
//              (`src` moved less than `available` unless `available == 0`,
//              `dest` is undefined)
bool ReadVarintSigned32(absl::Cord::CharIterator& src, size_t available,
                        uint32_t& dest);
bool ReadVarintSigned64(absl::Cord::CharIterator& src, size_t available,
                        uint64_t& dest);

// Reads a varint, at most `available` bytes long. This corresponds to protobuf
// types `{int,uint}{32,64}` (with a cast needed in the case of `int{32,64}`).
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
//  * `true`  - success (`dest` is set)
//  * `false` - source ends too early or varint is invalid
//              (`src` moved less than `available` unless `available == 0`,
//              `dest` is undefined)
bool ReadCanonicalVarint32(absl::Cord::CharIterator& src, size_t available,
                           uint32_t& dest);
bool ReadCanonicalVarint64(absl::Cord::CharIterator& src, size_t available,
                           uint64_t& dest);

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

// Copies a varint to an array, without decoding and encoding but with
// validation.
//
// Writes up to `kMaxLengthVarint{32,64}` bytes to `dest[]`.
//
// Return values:
//  * positive `length`    - success, `length` bytes copied (`dest[]` is filled)
//  * 0 (when `src.ok()`)  - source ends too early or varint is invalid
//                           (`src` position is unchanged,
//                           `dest[]` is undefined)
//  * 0 (when `!src.ok()`) - failure
//                           (`src` position is unchanged,
//                           `dest[]` is undefined)
size_t CopyVarint32(Reader& src, char* dest);
size_t CopyVarint64(Reader& src, char* dest);

// Copies a varint to an array, without decoding and encoding but with
// validation.
//
// Accepts only the canonical representation, i.e. the shortest: rejecting a
// trailing zero byte, except for 0 itself.
//
// Writes up to `kMaxLengthVarint{32,64}` bytes to `dest[]`.
//
// Return values:
//  * positive `length`    - success, `length` bytes copied (`dest[]` is filled)
//  * 0 (when `src.ok()`)  - source ends too early or varint is invalid
//                           (`src` position is unchanged,
//                           `dest[]` is undefined)
//  * 0 (when `!src.ok()`) - failure
//                           (`src` position is unchanged,
//                           `dest[]` is undefined)
size_t CopyCanonicalVarint32(Reader& src, char* dest);
size_t CopyCanonicalVarint64(Reader& src, char* dest);

// Copies a varint to an array, at most `available` bytes long, without decoding
// and encoding but with validation.
//
// Writes up to `kMaxLengthVarint{32,64}` bytes to `dest[]`.
//
// Return values:
//  * positive `length` - success, `length` bytes copied (`dest[]` is filled)
//  * 0                 - source ends too early or varint is invalid
//                        (`src` moved less than `available`
//                        unless `available == 0`,
//                        `dest[]` is undefined)
size_t CopyVarint32(absl::Cord::CharIterator& src, size_t available,
                    char* dest);
size_t CopyVarint64(absl::Cord::CharIterator& src, size_t available,
                    char* dest);

// Copies a varint to an array, at most `available` bytes long, without decoding
// and encoding but with validation.
//
// Accepts only the canonical representation, i.e. the shortest: rejecting a
// trailing zero byte, except for 0 itself.
//
// Writes up to `kMaxLengthVarint{32,64}` bytes to `dest[]`.
//
// Return values:
//  * positive `length` - success, `length` bytes copied (`dest[]` is filled)
//  * 0                 - source ends too early or varint is invalid
//                        (`src` moved less than `available`
//                        unless `available == 0`,
//                        `dest[]` is undefined)
size_t CopyCanonicalVarint32(absl::Cord::CharIterator& src, size_t available,
                             char* dest);
size_t CopyCanonicalVarint64(absl::Cord::CharIterator& src, size_t available,
                             char* dest);

// Copies a varint from an array to an array, without decoding and encoding but
// with validation.
//
// Writes up to `kMaxLengthVarint{32,64}` bytes to `dest[]`.
//
// Return values:
//  * positive `length` - success, `length` bytes copied (`dest[]` is filled)
//  * 0                 - source ends too early or varint is invalid
//                        (`dest[]` is undefined)
size_t CopyVarint32(const char* src, size_t available, char* dest);
size_t CopyVarint64(const char* src, size_t available, char* dest);

// Copies a varint from an array to an array, without decoding and encoding but
// with validation.
//
// Writes up to `kMaxLengthVarint{32,64}` bytes to `dest[]`.
//
// Return values:
//  * varint length  - success (`dest[]` is filled)
//  * `std::nullopt` - source ends too early or varint is invalid
//                     (`dest[]` is undefined)
ABSL_DEPRECATED(
    "Use CopyVarint32() overload with size_t parameter and size_t result")
std::optional<size_t> CopyVarint32(const char* src, const char* limit,
                                   char* dest);
ABSL_DEPRECATED(
    "Use CopyVarint64() overload with size_t parameter and size_t result")
std::optional<size_t> CopyVarint64(const char* src, const char* limit,
                                   char* dest);

// Copies a varint from an array to an array, without decoding and encoding but
// with validation.
//
// Accepts only the canonical representation, i.e. the shortest: rejecting a
// trailing zero byte, except for 0 itself.
//
// Writes up to `kMaxLengthVarint{32,64}` bytes to `dest[]`.
//
// Return values:
//  * positive `length` - success, `length` bytes copied (`dest[]` is filled)
//  * 0                 - source ends too early or varint is invalid
//                        (`dest[]` is undefined)
size_t CopyCanonicalVarint32(const char* src, size_t available, char* dest);
size_t CopyCanonicalVarint64(const char* src, size_t available, char* dest);

// Skips a varint, without decoding but with validation.
//
// Return values:
//  * `true`                     - success
//  * `false` (when `src.ok()`)  - source ends too early or varint is invalid
//                                 (`src` position is unchanged)
//  * `false` (when `!src.ok()`) - failure
//                                 (`src` position is unchanged)
bool SkipVarint32(Reader& src);
bool SkipVarint64(Reader& src);

// Skips a varint, without decoding but with validation.
//
// Accepts only the canonical representation, i.e. the shortest: rejecting a
// trailing zero byte, except for 0 itself.
//
// Return values:
//  * `true`                     - success
//  * `false` (when `src.ok()`)  - source ends too early or varint is invalid
//                                 (`src` position is unchanged)
//  * `false` (when `!src.ok()`) - failure
//                                 (`src` position is unchanged)
bool SkipCanonicalVarint32(Reader& src);
bool SkipCanonicalVarint64(Reader& src);

// Skips a varint, at most `available` bytes long, without decoding but with
// validation.
//
// Return values:
//  * positive `length` - success
//  * 0                 - source ends too early or varint is invalid
//                        (`src` moved less than `available`
//                        unless `available == 0`)
bool SkipVarint32(absl::Cord::CharIterator& src, size_t available);
bool SkipVarint64(absl::Cord::CharIterator& src, size_t available);

// Skips a varint, at most `available` bytes long, without decoding but with
// validation.
//
// Accepts only the canonical representation, i.e. the shortest: rejecting a
// trailing zero byte, except for 0 itself.
//
// Return values:
//  * positive `length` - success
//  * 0                 - source ends too early or varint is invalid
//                        (`src` moved less than `available`
//                        unless `available == 0`)
bool SkipCanonicalVarint32(absl::Cord::CharIterator& src, size_t available);
bool SkipCanonicalVarint64(absl::Cord::CharIterator& src, size_t available);

// Skips a varint from an array, without decoding but with validation.
//
// Return values:
//  * positive `length` - success, `length` bytes can be skipped
//  * 0                 - source ends too early or varint is invalid
size_t SkipVarint32(const char* src, size_t available);
size_t SkipVarint64(const char* src, size_t available);

// Skips a varint from an array, without decoding but with validation.
//
// Accepts only the canonical representation, i.e. the shortest: rejecting a
// trailing zero byte, except for 0 itself.
//
// Return values:
//  * positive `length` - success, `length` bytes can be skipped
//  * 0                 - source ends too early or varint is invalid
size_t SkipCanonicalVarint32(const char* src, size_t available);
size_t SkipCanonicalVarint64(const char* src, size_t available);

// Decodes a signed varint (zigzag-decoding) from an unsigned value read as a
// plain varint. This corresponds to protobuf types `sint{32,64}`.
constexpr int32_t DecodeVarintSigned32(uint32_t repr);
constexpr int64_t DecodeVarintSigned64(uint64_t repr);

// Implementation details follow.

namespace varint_internal {

inline size_t Remaining(const absl::Cord::CharIterator& src) {
  return IntCast<size_t>(absl::Cord::Distance(src, absl::Cord::CharIterator()));
}

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
bool ReadVarintFromCordBuffer(absl::Cord::CharIterator& src, size_t available,
                              T acc, T& dest);

extern template bool ReadVarintFromCordBuffer<uint32_t, false, 2>(
    absl::Cord::CharIterator& src, size_t available, uint32_t acc,
    uint32_t& dest);
extern template bool ReadVarintFromCordBuffer<uint64_t, false, 2>(
    absl::Cord::CharIterator& src, size_t available, uint64_t acc,
    uint64_t& dest);
extern template bool ReadVarintFromCordBuffer<uint32_t, true, 2>(
    absl::Cord::CharIterator& src, size_t available, uint32_t acc,
    uint32_t& dest);
extern template bool ReadVarintFromCordBuffer<uint64_t, true, 2>(
    absl::Cord::CharIterator& src, size_t available, uint64_t acc,
    uint64_t& dest);

template <typename T, bool canonical, size_t initial_index>
bool ReadVarintFromCord(absl::Cord::CharIterator& src, size_t available, T acc,
                        T& dest);

extern template bool ReadVarintFromCord<uint32_t, false, 1>(
    absl::Cord::CharIterator& src, size_t available, uint32_t acc,
    uint32_t& dest);
extern template bool ReadVarintFromCord<uint64_t, false, 1>(
    absl::Cord::CharIterator& src, size_t available, uint64_t acc,
    uint64_t& dest);
extern template bool ReadVarintFromCord<uint32_t, true, 1>(
    absl::Cord::CharIterator& src, size_t available, uint32_t acc,
    uint32_t& dest);
extern template bool ReadVarintFromCord<uint64_t, true, 1>(
    absl::Cord::CharIterator& src, size_t available, uint64_t acc,
    uint64_t& dest);

}  // namespace varint_internal

inline bool ReadVarint32(absl::Cord::CharIterator& src, size_t available,
                         uint32_t& dest) {
  RIEGELI_ASSERT_LE(available, varint_internal::Remaining(src))
      << "Failed precondition of ReadVarint32(): not enough remaining data";
  if (ABSL_PREDICT_FALSE(available == 0)) return false;
  const uint32_t byte0 = uint32_t{static_cast<uint8_t>(*src)};
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) {
    ++src;
    dest = byte0;
    return true;
  }
  if (ABSL_PREDICT_FALSE(available == 1)) return false;
  const absl::string_view chunk = absl::Cord::ChunkRemaining(src);
  if (ABSL_PREDICT_TRUE(chunk.size() >= 2)) {
    const uint32_t byte1 = uint32_t{static_cast<uint8_t>(chunk[1])};
    const uint32_t acc = byte0 + ((byte1 - 1) << 7);
    if (ABSL_PREDICT_TRUE(byte1 < 0x80)) {
      absl::Cord::Advance(&src, 2);
      dest = acc;
      return true;
    }
    return varint_internal::ReadVarintFromCordBuffer<uint32_t,
                                                     /*canonical=*/false, 2>(
        src, available, acc, dest);
  }
  return varint_internal::ReadVarintFromCord<uint32_t,
                                             /*canonical=*/false, 1>(
      src, available, byte0, dest);
}

inline bool ReadVarint64(absl::Cord::CharIterator& src, size_t available,
                         uint64_t& dest) {
  RIEGELI_ASSERT_LE(available, varint_internal::Remaining(src))
      << "Failed precondition of ReadVarint64(): not enough remaining data";
  if (ABSL_PREDICT_FALSE(available == 0)) return false;
  const uint64_t byte0 = uint64_t{static_cast<uint8_t>(*src)};
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) {
    ++src;
    dest = byte0;
    return true;
  }
  if (ABSL_PREDICT_FALSE(available == 1)) return false;
  const absl::string_view chunk = absl::Cord::ChunkRemaining(src);
  if (ABSL_PREDICT_TRUE(chunk.size() >= 2)) {
    const uint64_t byte1 = uint64_t{static_cast<uint8_t>(chunk[1])};
    const uint64_t acc = byte0 + ((byte1 - 1) << 7);
    if (ABSL_PREDICT_TRUE(byte1 < 0x80)) {
      absl::Cord::Advance(&src, 2);
      dest = acc;
      return true;
    }
    return varint_internal::ReadVarintFromCordBuffer<uint64_t,
                                                     /*canonical=*/false, 2>(
        src, available, acc, dest);
  }
  return varint_internal::ReadVarintFromCord<uint64_t,
                                             /*canonical=*/false, 1>(
      src, available, byte0, dest);
}

inline bool ReadVarintSigned32(absl::Cord::CharIterator& src, size_t available,
                               int32_t& dest) {
  uint32_t unsigned_dest;
  if (ABSL_PREDICT_FALSE(!ReadVarint32(src, available, unsigned_dest))) {
    return false;
  }
  dest = DecodeVarintSigned32(unsigned_dest);
  return true;
}

inline bool ReadVarintSigned64(absl::Cord::CharIterator& src, size_t available,
                               int64_t& dest) {
  uint64_t unsigned_dest;
  if (ABSL_PREDICT_FALSE(!ReadVarint64(src, available, unsigned_dest))) {
    return false;
  }
  dest = DecodeVarintSigned64(unsigned_dest);
  return true;
}

inline bool ReadCanonicalVarint32(absl::Cord::CharIterator& src,
                                  size_t available, uint32_t& dest) {
  RIEGELI_ASSERT_LE(available, varint_internal::Remaining(src))
      << "Failed precondition of ReadCanonicalVarint32(): "
         "not enough remaining data";
  if (ABSL_PREDICT_FALSE(available == 0)) return false;
  const uint32_t byte0 = uint32_t{static_cast<uint8_t>(*src)};
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) {
    ++src;
    dest = byte0;
    return true;
  }
  if (ABSL_PREDICT_FALSE(available == 1)) return false;
  const absl::string_view chunk = absl::Cord::ChunkRemaining(src);
  if (ABSL_PREDICT_TRUE(chunk.size() >= 2)) {
    const uint32_t byte1 = uint32_t{static_cast<uint8_t>(chunk[1])};
    const uint32_t acc = byte0 + ((byte1 - 1) << 7);
    if (ABSL_PREDICT_TRUE(byte1 < 0x80)) {
      if (ABSL_PREDICT_FALSE(byte1 == 0)) return false;
      absl::Cord::Advance(&src, 2);
      dest = acc;
      return true;
    }
    return varint_internal::ReadVarintFromCordBuffer<uint32_t,
                                                     /*canonical=*/true, 2>(
        src, available, acc, dest);
  }
  return varint_internal::ReadVarintFromCord<uint32_t,
                                             /*canonical=*/true, 1>(
      src, available, byte0, dest);
}

inline bool ReadCanonicalVarint64(absl::Cord::CharIterator& src,
                                  size_t available, uint64_t& dest) {
  RIEGELI_ASSERT_LE(available, varint_internal::Remaining(src))
      << "Failed precondition of ReadCanonicalVarint64(): "
         "not enough remaining data";
  if (ABSL_PREDICT_FALSE(available == 0)) return false;
  const uint64_t byte0 = uint64_t{static_cast<uint8_t>(*src)};
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) {
    ++src;
    dest = byte0;
    return true;
  }
  if (ABSL_PREDICT_FALSE(available == 1)) return false;
  const absl::string_view chunk = absl::Cord::ChunkRemaining(src);
  if (ABSL_PREDICT_TRUE(chunk.size() >= 2)) {
    const uint64_t byte1 = uint64_t{static_cast<uint8_t>(chunk[1])};
    const uint64_t acc = byte0 + ((byte1 - 1) << 7);
    if (ABSL_PREDICT_TRUE(byte1 < 0x80)) {
      if (ABSL_PREDICT_FALSE(byte1 == 0)) return false;
      absl::Cord::Advance(&src, 2);
      dest = acc;
      return true;
    }
    return varint_internal::ReadVarintFromCordBuffer<uint64_t,
                                                     /*canonical=*/true, 2>(
        src, available, acc, dest);
  }
  return varint_internal::ReadVarintFromCord<uint64_t,
                                             /*canonical=*/true, 1>(
      src, available, byte0, dest);
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

namespace varint_internal {

template <typename T, bool canonical, size_t initial_index>
size_t CopyVarintFromReaderBuffer(Reader& src, const char* cursor, char* dest);

extern template size_t CopyVarintFromReaderBuffer<uint32_t, false, 2>(
    Reader& src, const char* cursor, char* dest);
extern template size_t CopyVarintFromReaderBuffer<uint64_t, false, 2>(
    Reader& src, const char* cursor, char* dest);
extern template size_t CopyVarintFromReaderBuffer<uint32_t, true, 2>(
    Reader& src, const char* cursor, char* dest);
extern template size_t CopyVarintFromReaderBuffer<uint64_t, true, 2>(
    Reader& src, const char* cursor, char* dest);

template <typename T, bool canonical, size_t initial_index>
size_t CopyVarintFromReader(Reader& src, char* dest);

extern template size_t CopyVarintFromReader<uint32_t, false, 1>(Reader& src,
                                                                char* dest);
extern template size_t CopyVarintFromReader<uint64_t, false, 1>(Reader& src,
                                                                char* dest);

extern template size_t CopyVarintFromReader<uint32_t, true, 1>(Reader& src,
                                                               char* dest);
extern template size_t CopyVarintFromReader<uint64_t, true, 1>(Reader& src,
                                                               char* dest);

}  // namespace varint_internal

inline size_t CopyVarint32(Reader& src, char* dest) {
  if (ABSL_PREDICT_FALSE(!src.Pull(1, kMaxLengthVarint32))) return 0;
  const uint8_t byte0 = static_cast<uint8_t>(src.cursor()[0]);
  dest[0] = static_cast<char>(byte0);
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) {
    src.move_cursor(1);
    return 1;
  }
  if (ABSL_PREDICT_TRUE(src.available() >= 2)) {
    const uint8_t byte1 = static_cast<uint8_t>(src.cursor()[1]);
    dest[1] = static_cast<char>(byte1);
    if (ABSL_PREDICT_TRUE(byte1 < 0x80)) {
      src.move_cursor(2);
      return 2;
    }
    return varint_internal::CopyVarintFromReaderBuffer<uint32_t,
                                                       /*canonical=*/false, 2>(
        src, src.cursor(), dest);
  }
  return varint_internal::CopyVarintFromReader<uint32_t, /*canonical=*/false,
                                               1>(src, dest);
}

inline size_t CopyVarint64(Reader& src, char* dest) {
  if (ABSL_PREDICT_FALSE(!src.Pull(1, kMaxLengthVarint64))) return 0;
  const uint8_t byte0 = static_cast<uint8_t>(src.cursor()[0]);
  dest[0] = static_cast<char>(byte0);
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) {
    src.move_cursor(1);
    return 1;
  }
  if (ABSL_PREDICT_TRUE(src.available() >= 2)) {
    const uint8_t byte1 = static_cast<uint8_t>(src.cursor()[1]);
    dest[1] = static_cast<char>(byte1);
    if (ABSL_PREDICT_TRUE(byte1 < 0x80)) {
      src.move_cursor(2);
      return 2;
    }
    return varint_internal::CopyVarintFromReaderBuffer<uint64_t,
                                                       /*canonical=*/false, 2>(
        src, src.cursor(), dest);
  }
  return varint_internal::CopyVarintFromReader<uint64_t, /*canonical=*/false,
                                               1>(src, dest);
}

inline size_t CopyCanonicalVarint32(Reader& src, char* dest) {
  if (ABSL_PREDICT_FALSE(!src.Pull(1, kMaxLengthVarint32))) return 0;
  const uint8_t byte0 = static_cast<uint8_t>(src.cursor()[0]);
  dest[0] = static_cast<char>(byte0);
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) {
    src.move_cursor(1);
    return 1;
  }
  if (ABSL_PREDICT_TRUE(src.available() >= 2)) {
    const uint8_t byte1 = static_cast<uint8_t>(src.cursor()[1]);
    dest[1] = static_cast<char>(byte1);
    if (ABSL_PREDICT_TRUE(byte1 < 0x80)) {
      if (ABSL_PREDICT_FALSE(byte1 == 0)) return 0;
      src.move_cursor(2);
      return 2;
    }
    return varint_internal::CopyVarintFromReaderBuffer<uint32_t,
                                                       /*canonical=*/true, 2>(
        src, src.cursor(), dest);
  }
  return varint_internal::CopyVarintFromReader<uint32_t, /*canonical=*/true, 1>(
      src, dest);
}

inline size_t CopyCanonicalVarint64(Reader& src, char* dest) {
  if (ABSL_PREDICT_FALSE(!src.Pull(1, kMaxLengthVarint64))) return 0;
  const uint8_t byte0 = static_cast<uint8_t>(src.cursor()[0]);
  dest[0] = static_cast<char>(byte0);
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) {
    src.move_cursor(1);
    return 1;
  }
  if (ABSL_PREDICT_TRUE(src.available() >= 2)) {
    const uint8_t byte1 = static_cast<uint8_t>(src.cursor()[1]);
    dest[1] = static_cast<char>(byte1);
    if (ABSL_PREDICT_TRUE(byte1 < 0x80)) {
      if (ABSL_PREDICT_FALSE(byte1 == 0)) return 0;
      src.move_cursor(2);
      return 2;
    }
    return varint_internal::CopyVarintFromReaderBuffer<uint64_t,
                                                       /*canonical=*/true, 2>(
        src, src.cursor(), dest);
  }
  return varint_internal::CopyVarintFromReader<uint64_t, /*canonical=*/true, 1>(
      src, dest);
}

namespace varint_internal {

template <typename T, bool canonical, size_t initial_index>
size_t CopyVarintFromCordBuffer(absl::Cord::CharIterator& src, size_t available,
                                char* dest);

extern template size_t CopyVarintFromCordBuffer<uint32_t, false, 2>(
    absl::Cord::CharIterator& src, size_t available, char* dest);
extern template size_t CopyVarintFromCordBuffer<uint64_t, false, 2>(
    absl::Cord::CharIterator& src, size_t available, char* dest);
extern template size_t CopyVarintFromCordBuffer<uint32_t, true, 2>(
    absl::Cord::CharIterator& src, size_t available, char* dest);
extern template size_t CopyVarintFromCordBuffer<uint64_t, true, 2>(
    absl::Cord::CharIterator& src, size_t available, char* dest);

template <typename T, bool canonical, size_t initial_index>
size_t CopyVarintFromCord(absl::Cord::CharIterator& src, size_t available,
                          char* dest);

extern template size_t CopyVarintFromCord<uint32_t, false, 1>(
    absl::Cord::CharIterator& src, size_t available, char* dest);
extern template size_t CopyVarintFromCord<uint64_t, false, 1>(
    absl::Cord::CharIterator& src, size_t available, char* dest);
extern template size_t CopyVarintFromCord<uint32_t, true, 1>(
    absl::Cord::CharIterator& src, size_t available, char* dest);
extern template size_t CopyVarintFromCord<uint64_t, true, 1>(
    absl::Cord::CharIterator& src, size_t available, char* dest);

}  // namespace varint_internal

inline size_t CopyVarint32(absl::Cord::CharIterator& src, size_t available,
                           char* dest) {
  RIEGELI_ASSERT_LE(available, varint_internal::Remaining(src))
      << "Failed precondition of CopyVarint32(): not enough remaining data";
  if (ABSL_PREDICT_FALSE(available == 0)) return 0;
  const uint8_t byte0 = static_cast<uint8_t>(*src);
  dest[0] = static_cast<char>(byte0);
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) {
    ++src;
    return 1;
  }
  if (ABSL_PREDICT_FALSE(available == 1)) return 0;
  const absl::string_view chunk = absl::Cord::ChunkRemaining(src);
  if (ABSL_PREDICT_TRUE(chunk.size() >= 2)) {
    const uint8_t byte1 = static_cast<uint8_t>(chunk[1]);
    dest[1] = static_cast<char>(byte1);
    if (ABSL_PREDICT_TRUE(byte1 < 0x80)) {
      absl::Cord::Advance(&src, 2);
      return 2;
    }
    return varint_internal::CopyVarintFromCordBuffer<uint32_t,
                                                     /*canonical=*/false, 2>(
        src, available, dest);
  }
  return varint_internal::CopyVarintFromCord<uint32_t,
                                             /*canonical=*/false, 1>(
      src, available, dest);
}

inline size_t CopyVarint64(absl::Cord::CharIterator& src, size_t available,
                           char* dest) {
  RIEGELI_ASSERT_LE(available, varint_internal::Remaining(src))
      << "Failed precondition of CopyVarint64(): not enough remaining data";
  if (ABSL_PREDICT_FALSE(available == 0)) return 0;
  const uint8_t byte0 = static_cast<uint8_t>(*src);
  dest[0] = static_cast<char>(byte0);
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) {
    ++src;
    return 1;
  }
  if (ABSL_PREDICT_FALSE(available == 1)) return 0;
  const absl::string_view chunk = absl::Cord::ChunkRemaining(src);
  if (ABSL_PREDICT_TRUE(chunk.size() >= 2)) {
    const uint8_t byte1 = static_cast<uint8_t>(chunk[1]);
    dest[1] = static_cast<char>(byte1);
    if (ABSL_PREDICT_TRUE(byte1 < 0x80)) {
      absl::Cord::Advance(&src, 2);
      return 2;
    }
    return varint_internal::CopyVarintFromCordBuffer<uint64_t,
                                                     /*canonical=*/false, 2>(
        src, available, dest);
  }
  return varint_internal::CopyVarintFromCord<uint64_t,
                                             /*canonical=*/false, 1>(
      src, available, dest);
}

inline size_t CopyCanonicalVarint32(absl::Cord::CharIterator& src,
                                    size_t available, char* dest) {
  RIEGELI_ASSERT_LE(available, varint_internal::Remaining(src))
      << "Failed precondition of CopyCanonicalVarint32(): "
         "not enough remaining data";
  if (ABSL_PREDICT_FALSE(available == 0)) return 0;
  const uint8_t byte0 = static_cast<uint8_t>(*src);
  dest[0] = static_cast<char>(byte0);
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) {
    ++src;
    return 1;
  }
  if (ABSL_PREDICT_FALSE(available == 1)) return 0;
  const absl::string_view chunk = absl::Cord::ChunkRemaining(src);
  if (ABSL_PREDICT_TRUE(chunk.size() >= 2)) {
    const uint8_t byte1 = static_cast<uint8_t>(chunk[1]);
    dest[1] = static_cast<char>(byte1);
    if (ABSL_PREDICT_TRUE(byte1 < 0x80)) {
      if (ABSL_PREDICT_FALSE(byte1 == 0)) return 0;
      absl::Cord::Advance(&src, 2);
      return 2;
    }
    return varint_internal::CopyVarintFromCordBuffer<uint32_t,
                                                     /*canonical=*/true, 2>(
        src, available, dest);
  }
  return varint_internal::CopyVarintFromCord<uint32_t,
                                             /*canonical=*/true, 1>(
      src, available, dest);
}

inline size_t CopyCanonicalVarint64(absl::Cord::CharIterator& src,
                                    size_t available, char* dest) {
  RIEGELI_ASSERT_LE(available, varint_internal::Remaining(src))
      << "Failed precondition of CopyCanonicalVarint64(): "
         "not enough remaining data";
  if (ABSL_PREDICT_FALSE(available == 0)) return 0;
  const uint8_t byte0 = static_cast<uint8_t>(*src);
  dest[0] = static_cast<char>(byte0);
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) {
    ++src;
    return 1;
  }
  if (ABSL_PREDICT_FALSE(available == 1)) return 0;
  const absl::string_view chunk = absl::Cord::ChunkRemaining(src);
  if (ABSL_PREDICT_TRUE(chunk.size() >= 2)) {
    const uint8_t byte1 = static_cast<uint8_t>(chunk[1]);
    dest[1] = static_cast<char>(byte1);
    if (ABSL_PREDICT_TRUE(byte1 < 0x80)) {
      if (ABSL_PREDICT_FALSE(byte1 == 0)) return 0;
      absl::Cord::Advance(&src, 2);
      return 2;
    }
    return varint_internal::CopyVarintFromCordBuffer<uint64_t,
                                                     /*canonical=*/true, 2>(
        src, available, dest);
  }
  return varint_internal::CopyVarintFromCord<uint64_t,
                                             /*canonical=*/true, 1>(
      src, available, dest);
}

namespace varint_internal {

template <typename T, bool canonical, size_t initial_index>
size_t CopyVarintFromArray(const char* src, size_t available, char* dest);

extern template size_t CopyVarintFromArray<uint32_t, false, 2>(const char* src,
                                                               size_t available,
                                                               char* dest);
extern template size_t CopyVarintFromArray<uint64_t, false, 2>(const char* src,
                                                               size_t available,
                                                               char* dest);
extern template size_t CopyVarintFromArray<uint32_t, true, 2>(const char* src,
                                                              size_t available,
                                                              char* dest);
extern template size_t CopyVarintFromArray<uint64_t, true, 2>(const char* src,
                                                              size_t available,
                                                              char* dest);

}  // namespace varint_internal

inline size_t CopyVarint32(const char* src, size_t available, char* dest) {
  if (ABSL_PREDICT_FALSE(available == 0)) return 0;
  const uint8_t byte0 = static_cast<uint8_t>(src[0]);
  dest[0] = static_cast<char>(byte0);
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) return 1;
  if (ABSL_PREDICT_FALSE(available == 1)) return 0;
  const uint8_t byte1 = static_cast<uint8_t>(src[1]);
  dest[1] = static_cast<char>(byte1);
  if (ABSL_PREDICT_TRUE(byte1 < 0x80)) return 2;
  return varint_internal::CopyVarintFromArray<uint32_t, /*canonical=*/false, 2>(
      src, available, dest);
}

inline size_t CopyVarint64(const char* src, size_t available, char* dest) {
  if (ABSL_PREDICT_FALSE(available == 0)) return 0;
  const uint8_t byte0 = static_cast<uint8_t>(src[0]);
  dest[0] = static_cast<char>(byte0);
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) return 1;
  if (ABSL_PREDICT_FALSE(available == 1)) return 0;
  const uint8_t byte1 = static_cast<uint8_t>(src[1]);
  dest[1] = static_cast<char>(byte1);
  if (ABSL_PREDICT_TRUE(byte1 < 0x80)) return 2;
  return varint_internal::CopyVarintFromArray<uint64_t, /*canonical=*/false, 2>(
      src, available, dest);
}

inline std::optional<size_t> CopyVarint32(const char* src, const char* limit,
                                          char* dest) {
  const size_t length = CopyVarint32(src, PtrDistance(src, limit), dest);
  if (ABSL_PREDICT_FALSE(length == 0)) return std::nullopt;
  return length;
}

inline std::optional<size_t> CopyVarint64(const char* src, const char* limit,
                                          char* dest) {
  const size_t length = CopyVarint64(src, PtrDistance(src, limit), dest);
  if (ABSL_PREDICT_FALSE(length == 0)) return std::nullopt;
  return length;
}

inline size_t CopyCanonicalVarint32(const char* src, size_t available,
                                    char* dest) {
  if (ABSL_PREDICT_FALSE(available == 0)) return 0;
  const uint8_t byte0 = static_cast<uint8_t>(src[0]);
  dest[0] = static_cast<char>(byte0);
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) return 1;
  if (ABSL_PREDICT_FALSE(available == 1)) return 0;
  const uint8_t byte1 = static_cast<uint8_t>(src[1]);
  dest[1] = static_cast<char>(byte1);
  if (ABSL_PREDICT_TRUE(byte1 < 0x80)) {
    if (ABSL_PREDICT_FALSE(byte1 == 0)) return 0;
    return 2;
  }
  return varint_internal::CopyVarintFromArray<uint32_t, /*canonical=*/true, 2>(
      src, available, dest);
}

inline size_t CopyCanonicalVarint64(const char* src, size_t available,
                                    char* dest) {
  if (ABSL_PREDICT_FALSE(available == 0)) return 0;
  const uint8_t byte0 = static_cast<uint8_t>(src[0]);
  dest[0] = static_cast<char>(byte0);
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) return 1;
  if (ABSL_PREDICT_FALSE(available == 1)) return 0;
  const uint8_t byte1 = static_cast<uint8_t>(src[1]);
  dest[1] = static_cast<char>(byte1);
  if (ABSL_PREDICT_TRUE(byte1 < 0x80)) {
    if (ABSL_PREDICT_FALSE(byte1 == 0)) return 0;
    return 2;
  }
  return varint_internal::CopyVarintFromArray<uint64_t, /*canonical=*/true, 2>(
      src, available, dest);
}

namespace varint_internal {

template <typename T, bool canonical, size_t initial_index>
bool SkipVarintFromReaderBuffer(Reader& src, const char* cursor);

extern template bool SkipVarintFromReaderBuffer<uint32_t, false, 2>(
    Reader& src, const char* cursor);
extern template bool SkipVarintFromReaderBuffer<uint64_t, false, 2>(
    Reader& src, const char* cursor);
extern template bool SkipVarintFromReaderBuffer<uint32_t, true, 2>(
    Reader& src, const char* cursor);
extern template bool SkipVarintFromReaderBuffer<uint64_t, true, 2>(
    Reader& src, const char* cursor);

template <typename T, bool canonical, size_t initial_index>
bool SkipVarintFromReader(Reader& src);

extern template bool SkipVarintFromReader<uint32_t, false, 1>(Reader& src);
extern template bool SkipVarintFromReader<uint64_t, false, 1>(Reader& src);
extern template bool SkipVarintFromReader<uint32_t, true, 1>(Reader& src);
extern template bool SkipVarintFromReader<uint64_t, true, 1>(Reader& src);

}  // namespace varint_internal

inline bool SkipVarint32(Reader& src) {
  if (ABSL_PREDICT_FALSE(!src.Pull(1, kMaxLengthVarint32))) return false;
  const uint8_t byte0 = static_cast<uint8_t>(src.cursor()[0]);
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) {
    src.move_cursor(1);
    return true;
  }
  if (ABSL_PREDICT_TRUE(src.available() >= 2)) {
    const uint8_t byte1 = static_cast<uint8_t>(src.cursor()[1]);
    if (ABSL_PREDICT_TRUE(byte1 < 0x80)) {
      src.move_cursor(2);
      return true;
    }
    return varint_internal::SkipVarintFromReaderBuffer<uint32_t,
                                                       /*canonical=*/false, 2>(
        src, src.cursor());
  }
  return varint_internal::SkipVarintFromReader<uint32_t, /*canonical=*/false,
                                               1>(src);
}

inline bool SkipVarint64(Reader& src) {
  if (ABSL_PREDICT_FALSE(!src.Pull(1, kMaxLengthVarint64))) return false;
  const uint8_t byte0 = static_cast<uint8_t>(src.cursor()[0]);
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) {
    src.move_cursor(1);
    return true;
  }
  if (ABSL_PREDICT_TRUE(src.available() >= 2)) {
    const uint8_t byte1 = static_cast<uint8_t>(src.cursor()[1]);
    if (ABSL_PREDICT_TRUE(byte1 < 0x80)) {
      src.move_cursor(2);
      return true;
    }
    return varint_internal::SkipVarintFromReaderBuffer<uint64_t,
                                                       /*canonical=*/false, 2>(
        src, src.cursor());
  }
  return varint_internal::SkipVarintFromReader<uint64_t, /*canonical=*/false,
                                               1>(src);
}

inline bool SkipCanonicalVarint32(Reader& src) {
  if (ABSL_PREDICT_FALSE(!src.Pull(1, kMaxLengthVarint32))) return false;
  const uint8_t byte0 = static_cast<uint8_t>(src.cursor()[0]);
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) {
    src.move_cursor(1);
    return true;
  }
  if (ABSL_PREDICT_TRUE(src.available() >= 2)) {
    const uint8_t byte1 = static_cast<uint8_t>(src.cursor()[1]);
    if (ABSL_PREDICT_TRUE(byte1 < 0x80)) {
      if (ABSL_PREDICT_FALSE(byte1 == 0)) return false;
      src.move_cursor(2);
      return true;
    }
    return varint_internal::SkipVarintFromReaderBuffer<uint32_t,
                                                       /*canonical=*/true, 2>(
        src, src.cursor());
  }
  return varint_internal::SkipVarintFromReader<uint32_t, /*canonical=*/true, 1>(
      src);
}

inline bool SkipCanonicalVarint64(Reader& src) {
  if (ABSL_PREDICT_FALSE(!src.Pull(1, kMaxLengthVarint64))) return false;
  const uint8_t byte0 = static_cast<uint8_t>(src.cursor()[0]);
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) {
    src.move_cursor(1);
    return true;
  }
  if (ABSL_PREDICT_TRUE(src.available() >= 2)) {
    const uint8_t byte1 = static_cast<uint8_t>(src.cursor()[1]);
    if (ABSL_PREDICT_TRUE(byte1 < 0x80)) {
      if (ABSL_PREDICT_FALSE(byte1 == 0)) return false;
      src.move_cursor(2);
      return true;
    }
    return varint_internal::SkipVarintFromReaderBuffer<uint64_t,
                                                       /*canonical=*/true, 2>(
        src, src.cursor());
  }
  return varint_internal::SkipVarintFromReader<uint64_t, /*canonical=*/true, 1>(
      src);
}

namespace varint_internal {

template <typename T, bool canonical, size_t initial_index>
bool SkipVarintFromCordBuffer(absl::Cord::CharIterator& src, size_t available);

extern template bool SkipVarintFromCordBuffer<uint32_t, false, 2>(
    absl::Cord::CharIterator& src, size_t available);
extern template bool SkipVarintFromCordBuffer<uint64_t, false, 2>(
    absl::Cord::CharIterator& src, size_t available);
extern template bool SkipVarintFromCordBuffer<uint32_t, true, 2>(
    absl::Cord::CharIterator& src, size_t available);
extern template bool SkipVarintFromCordBuffer<uint64_t, true, 2>(
    absl::Cord::CharIterator& src, size_t available);

template <typename T, bool canonical, size_t initial_index>
bool SkipVarintFromCord(absl::Cord::CharIterator& src, size_t available);

extern template bool SkipVarintFromCord<uint32_t, false, 1>(
    absl::Cord::CharIterator& src, size_t available);
extern template bool SkipVarintFromCord<uint64_t, false, 1>(
    absl::Cord::CharIterator& src, size_t available);
extern template bool SkipVarintFromCord<uint32_t, true, 1>(
    absl::Cord::CharIterator& src, size_t available);
extern template bool SkipVarintFromCord<uint64_t, true, 1>(
    absl::Cord::CharIterator& src, size_t available);

}  // namespace varint_internal

inline bool SkipVarint32(absl::Cord::CharIterator& src, size_t available) {
  RIEGELI_ASSERT_LE(available, varint_internal::Remaining(src))
      << "Failed precondition of SkipVarint32(): not enough remaining data";
  if (ABSL_PREDICT_FALSE(available == 0)) return false;
  const uint32_t byte0 = uint32_t{static_cast<uint8_t>(*src)};
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) {
    ++src;
    return true;
  }
  if (ABSL_PREDICT_FALSE(available == 1)) return false;
  const absl::string_view chunk = absl::Cord::ChunkRemaining(src);
  if (ABSL_PREDICT_TRUE(chunk.size() >= 2)) {
    const uint32_t byte1 = uint32_t{static_cast<uint8_t>(chunk[1])};
    if (ABSL_PREDICT_TRUE(byte1 < 0x80)) {
      absl::Cord::Advance(&src, 2);
      return true;
    }
    return varint_internal::SkipVarintFromCordBuffer<uint32_t,
                                                     /*canonical=*/false, 2>(
        src, available);
  }
  return varint_internal::SkipVarintFromCord<uint32_t,
                                             /*canonical=*/false, 1>(src,
                                                                     available);
}

inline bool SkipVarint64(absl::Cord::CharIterator& src, size_t available) {
  RIEGELI_ASSERT_LE(available, varint_internal::Remaining(src))
      << "Failed precondition of SkipVarint64(): not enough remaining data";
  if (ABSL_PREDICT_FALSE(available == 0)) return false;
  const uint64_t byte0 = uint64_t{static_cast<uint8_t>(*src)};
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) {
    ++src;
    return true;
  }
  if (ABSL_PREDICT_FALSE(available == 1)) return false;
  const absl::string_view chunk = absl::Cord::ChunkRemaining(src);
  if (ABSL_PREDICT_TRUE(chunk.size() >= 2)) {
    const uint64_t byte1 = uint64_t{static_cast<uint8_t>(chunk[1])};
    if (ABSL_PREDICT_TRUE(byte1 < 0x80)) {
      absl::Cord::Advance(&src, 2);
      return true;
    }
    return varint_internal::SkipVarintFromCordBuffer<uint64_t,
                                                     /*canonical=*/false, 2>(
        src, available);
  }
  return varint_internal::SkipVarintFromCord<uint64_t,
                                             /*canonical=*/false, 1>(src,
                                                                     available);
}

inline bool SkipCanonicalVarint32(absl::Cord::CharIterator& src,
                                  size_t available) {
  RIEGELI_ASSERT_LE(available, varint_internal::Remaining(src))
      << "Failed precondition of SkipCanonicalVarint32(): "
         "not enough remaining data";
  if (ABSL_PREDICT_FALSE(available == 0)) return false;
  const uint32_t byte0 = uint32_t{static_cast<uint8_t>(*src)};
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) {
    ++src;
    return true;
  }
  if (ABSL_PREDICT_FALSE(available == 1)) return false;
  const absl::string_view chunk = absl::Cord::ChunkRemaining(src);
  if (ABSL_PREDICT_TRUE(chunk.size() >= 2)) {
    const uint32_t byte1 = uint32_t{static_cast<uint8_t>(chunk[1])};
    if (ABSL_PREDICT_TRUE(byte1 < 0x80)) {
      if (ABSL_PREDICT_FALSE(byte1 == 0)) return false;
      absl::Cord::Advance(&src, 2);
      return true;
    }
    return varint_internal::SkipVarintFromCordBuffer<uint32_t,
                                                     /*canonical=*/true, 2>(
        src, available);
  }
  return varint_internal::SkipVarintFromCord<uint32_t,
                                             /*canonical=*/true, 1>(src,
                                                                    available);
}

inline bool SkipCanonicalVarint64(absl::Cord::CharIterator& src,
                                  size_t available) {
  RIEGELI_ASSERT_LE(available, varint_internal::Remaining(src))
      << "Failed precondition of SkipCanonicalVarint64(): "
         "not enough remaining data";
  if (ABSL_PREDICT_FALSE(available == 0)) return false;
  const uint64_t byte0 = uint64_t{static_cast<uint8_t>(*src)};
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) {
    ++src;
    return true;
  }
  if (ABSL_PREDICT_FALSE(available == 1)) return false;
  const absl::string_view chunk = absl::Cord::ChunkRemaining(src);
  if (ABSL_PREDICT_TRUE(chunk.size() >= 2)) {
    const uint64_t byte1 = uint64_t{static_cast<uint8_t>(chunk[1])};
    if (ABSL_PREDICT_TRUE(byte1 < 0x80)) {
      if (ABSL_PREDICT_FALSE(byte1 == 0)) return false;
      absl::Cord::Advance(&src, 2);
      return true;
    }
    return varint_internal::SkipVarintFromCordBuffer<uint64_t,
                                                     /*canonical=*/true, 2>(
        src, available);
  }
  return varint_internal::SkipVarintFromCord<uint64_t,
                                             /*canonical=*/true, 1>(src,
                                                                    available);
}

namespace varint_internal {

template <typename T, bool canonical, size_t initial_index>
size_t SkipVarintFromArray(const char* src, size_t available);

extern template size_t SkipVarintFromArray<uint32_t, false, 2>(
    const char* src, size_t available);
extern template size_t SkipVarintFromArray<uint64_t, false, 2>(
    const char* src, size_t available);
extern template size_t SkipVarintFromArray<uint32_t, true, 2>(const char* src,
                                                              size_t available);
extern template size_t SkipVarintFromArray<uint64_t, true, 2>(const char* src,
                                                              size_t available);

}  // namespace varint_internal

inline size_t SkipVarint32(const char* src, size_t available) {
  if (ABSL_PREDICT_FALSE(available == 0)) return 0;
  const uint8_t byte0 = static_cast<uint8_t>(src[0]);
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) return 1;
  if (ABSL_PREDICT_FALSE(available == 1)) return 0;
  const uint8_t byte1 = static_cast<uint8_t>(src[1]);
  if (ABSL_PREDICT_TRUE(byte1 < 0x80)) return 2;
  return varint_internal::SkipVarintFromArray<uint32_t, /*canonical=*/false, 2>(
      src, available);
}

inline size_t SkipVarint64(const char* src, size_t available) {
  if (ABSL_PREDICT_FALSE(available == 0)) return 0;
  const uint8_t byte0 = static_cast<uint8_t>(src[0]);
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) return 1;
  if (ABSL_PREDICT_FALSE(available == 1)) return 0;
  const uint8_t byte1 = static_cast<uint8_t>(src[1]);
  if (ABSL_PREDICT_TRUE(byte1 < 0x80)) return 2;
  return varint_internal::SkipVarintFromArray<uint64_t, /*canonical=*/false, 2>(
      src, available);
}

inline size_t SkipCanonicalVarint32(const char* src, size_t available) {
  if (ABSL_PREDICT_FALSE(available == 0)) return 0;
  const uint8_t byte0 = static_cast<uint8_t>(src[0]);
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) return 1;
  if (ABSL_PREDICT_FALSE(available == 1)) return 0;
  const uint8_t byte1 = static_cast<uint8_t>(src[1]);
  if (ABSL_PREDICT_TRUE(byte1 < 0x80)) {
    if (ABSL_PREDICT_FALSE(byte1 == 0)) return 0;
    return 2;
  }
  return varint_internal::SkipVarintFromArray<uint32_t, /*canonical=*/true, 2>(
      src, available);
}

inline size_t SkipCanonicalVarint64(const char* src, size_t available) {
  if (ABSL_PREDICT_FALSE(available == 0)) return 0;
  const uint8_t byte0 = static_cast<uint8_t>(src[0]);
  if (ABSL_PREDICT_TRUE(byte0 < 0x80)) return 1;
  if (ABSL_PREDICT_FALSE(available == 1)) return 0;
  const uint8_t byte1 = static_cast<uint8_t>(src[1]);
  if (ABSL_PREDICT_TRUE(byte1 < 0x80)) {
    if (ABSL_PREDICT_FALSE(byte1 == 0)) return 0;
    return 2;
  }
  return varint_internal::SkipVarintFromArray<uint64_t, /*canonical=*/true, 2>(
      src, available);
}

constexpr int32_t DecodeVarintSigned32(uint32_t repr) {
  return static_cast<int32_t>((repr >> 1) ^ (~(repr & 1) + 1));
}

constexpr int64_t DecodeVarintSigned64(uint64_t repr) {
  return static_cast<int64_t>((repr >> 1) ^ (~(repr & 1) + 1));
}

}  // namespace riegeli

#endif  // RIEGELI_VARINT_VARINT_READING_H_
