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

#ifndef RIEGELI_ENDIAN_ENDIAN_READING_H_
#define RIEGELI_ENDIAN_ENDIAN_READING_H_

#include <stdint.h>

#include <cstring>

#include "absl/base/casts.h"
#include "absl/base/config.h"
#include "absl/base/optimization.h"
#include "absl/numeric/int128.h"
#include "absl/types/span.h"
#include "riegeli/base/null_safe_memcpy.h"
#include "riegeli/base/type_traits.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

// Reads a number in a fixed width Little/Big Endian encoding from an array.
// The width of the encoding is determined by the template argument, which
// must be one of: `{u,}int{8,16,32,64}_t`, `absl::{u,}int128`, `float`, or
// `double`.
//
// Reads `sizeof(T)` bytes from `src[]`.
template <typename T>
T ReadLittleEndian(const char* src);
template <typename T>
T ReadBigEndian(const char* src);

// Reads an array of numbers in a fixed width Little/Big Endian encoding from an
// array. The width of the encoding is determined by the template argument,
// which must be one of: `{u,}int{8,16,32,64}_t`, `absl::{u,}int128`, `float`,
// or `double`.
//
// This is faster than reading them individually if the endianness matches the
// native one.
//
// Reads `dest.size() * sizeof(T)` bytes from `src[]`.
template <typename T>
void ReadLittleEndians(const char* src, absl::Span<type_identity_t<T>> dest);
template <typename T>
void ReadBigEndians(const char* src, absl::Span<type_identity_t<T>> dest);

// Reads a number in a fixed width Little/Big Endian encoding. The width of
// the encoding is determined by the template argument, which must be one of:
// `{u,}int{8,16,32,64}_t`, `absl::{u,}int128`, `float`, or `double`.
//
// Return values:
//  * `true`                     - success (`dest` is set)
//  * `false` (when `src.ok()`)  - source ends
//                                 (`src` position is unchanged,
//                                 `dest` is undefined)
//  * `false` (when `!src.ok()`) - failure
//                                 (`src` position is unchanged,
//                                 `dest` is undefined)
template <typename T>
bool ReadLittleEndian(Reader& src, type_identity_t<T>& dest);
template <typename T>
bool ReadBigEndian(Reader& src, type_identity_t<T>& dest);

// Reads an array of numbers in a fixed width Little/Big Endian encoding.
// The width of the encoding is determined by the template argument, which
// must be one of: `{u,}int{8,16,32,64}_t`, `absl::{u,}int128`, `float`, or
// `double`.
//
// This is faster than reading them individually if the endianness matches the
// native one.
//
// Return values:
//  * `true`                     - success (`dest[]` is filled)
//  * `false` (when `src.ok()`)  - source ends
//                                 (`src` position is undefined,
//                                 `dest[]` is undefined)
//  * `false` (when `!src.ok()`) - failure
//                                 (`src` position is undefined,
//                                 `dest[]` is undefined)
template <typename T>
bool ReadLittleEndians(Reader& src, absl::Span<type_identity_t<T>> dest);
template <typename T>
bool ReadBigEndians(Reader& src, absl::Span<type_identity_t<T>> dest);

// Implementation details follow.

template <>
inline uint8_t ReadLittleEndian<uint8_t>(const char* src) {
  return static_cast<uint8_t>(*src);
}

template <>
inline uint16_t ReadLittleEndian<uint16_t>(const char* src) {
#if ABSL_IS_LITTLE_ENDIAN
  uint16_t dest;
  std::memcpy(&dest, src, sizeof(uint16_t));
  return dest;
#else
  // `static_cast<uint16_t>` avoids triggering `-Wimplicit-int-conversion`:
  // the result of `uint16_t | uint16_t` is `int` (if `uint16_t` is narrower
  // than `int`).
  return static_cast<uint16_t>(uint16_t{static_cast<uint8_t>(src[0])} |
                               (uint16_t{static_cast<uint8_t>(src[1])} << 8));
#endif
}

template <>
inline uint32_t ReadLittleEndian<uint32_t>(const char* src) {
#if ABSL_IS_LITTLE_ENDIAN
  uint32_t dest;
  std::memcpy(&dest, src, sizeof(uint32_t));
  return dest;
#else
  return uint32_t{static_cast<uint8_t>(src[0])} |
         (uint32_t{static_cast<uint8_t>(src[1])} << 8) |
         (uint32_t{static_cast<uint8_t>(src[2])} << (2 * 8)) |
         (uint32_t{static_cast<uint8_t>(src[3])} << (3 * 8));
#endif
}

template <>
inline uint64_t ReadLittleEndian<uint64_t>(const char* src) {
#if ABSL_IS_LITTLE_ENDIAN
  uint64_t dest;
  std::memcpy(&dest, src, sizeof(uint64_t));
  return dest;
#else
  return uint64_t{static_cast<uint8_t>(src[0])} |
         (uint64_t{static_cast<uint8_t>(src[1])} << 8) |
         (uint64_t{static_cast<uint8_t>(src[2])} << (2 * 8)) |
         (uint64_t{static_cast<uint8_t>(src[3])} << (3 * 8)) |
         (uint64_t{static_cast<uint8_t>(src[4])} << (4 * 8)) |
         (uint64_t{static_cast<uint8_t>(src[5])} << (5 * 8)) |
         (uint64_t{static_cast<uint8_t>(src[6])} << (6 * 8)) |
         (uint64_t{static_cast<uint8_t>(src[7])} << (7 * 8));
#endif
}

template <>
inline absl::uint128 ReadLittleEndian<absl::uint128>(const char* src) {
#if ABSL_IS_LITTLE_ENDIAN
  absl::uint128 dest;
  std::memcpy(&dest, src, sizeof(absl::uint128));
  return dest;
#else
  const uint64_t low = ReadLittleEndian<uint64_t>(src);
  const uint64_t high = ReadLittleEndian<uint64_t>(src + sizeof(uint64_t));
  return absl::MakeUint128(high, low);
#endif
}

template <>
inline int8_t ReadLittleEndian<int8_t>(const char* src) {
  return static_cast<int8_t>(*src);
}

template <>
inline int16_t ReadLittleEndian<int16_t>(const char* src) {
  return static_cast<int16_t>(ReadLittleEndian<uint16_t>(src));
}

template <>
inline int32_t ReadLittleEndian<int32_t>(const char* src) {
  return static_cast<int32_t>(ReadLittleEndian<uint32_t>(src));
}

template <>
inline int64_t ReadLittleEndian<int64_t>(const char* src) {
  return static_cast<int64_t>(ReadLittleEndian<uint64_t>(src));
}

template <>
inline absl::int128 ReadLittleEndian<absl::int128>(const char* src) {
  return static_cast<absl::int128>(ReadLittleEndian<absl::uint128>(src));
}

template <>
inline float ReadLittleEndian<float>(const char* src) {
  return absl::bit_cast<float>(ReadLittleEndian<uint32_t>(src));
}

template <>
inline double ReadLittleEndian<double>(const char* src) {
  return absl::bit_cast<double>(ReadLittleEndian<uint64_t>(src));
}

template <>
inline uint8_t ReadBigEndian<uint8_t>(const char* src) {
  return static_cast<uint8_t>(*src);
}

template <>
inline uint16_t ReadBigEndian<uint16_t>(const char* src) {
#if ABSL_IS_BIG_ENDIAN
  uint16_t dest;
  std::memcpy(&dest, src, sizeof(uint16_t));
  return dest;
#else
  // `static_cast<uint16_t>` avoids triggering `-Wimplicit-int-conversion`:
  // the result of `uint16_t | uint16_t` is `int` (if `uint16_t` is narrower
  // than `int`).
  return static_cast<uint16_t>((uint16_t{static_cast<uint8_t>(src[0])} << 8) |
                               uint16_t{static_cast<uint8_t>(src[1])});
#endif
}

template <>
inline uint32_t ReadBigEndian<uint32_t>(const char* src) {
#if ABSL_IS_BIG_ENDIAN
  uint32_t dest;
  std::memcpy(&dest, src, sizeof(uint32_t));
  return dest;
#else
  return (uint32_t{static_cast<uint8_t>(src[0])} << (3 * 8)) |
         (uint32_t{static_cast<uint8_t>(src[1])} << (2 * 8)) |
         (uint32_t{static_cast<uint8_t>(src[2])} << 8) |
         uint32_t{static_cast<uint8_t>(src[3])};
#endif
}

template <>
inline uint64_t ReadBigEndian<uint64_t>(const char* src) {
#if ABSL_IS_BIG_ENDIAN
  uint64_t dest;
  std::memcpy(&dest, src, sizeof(uint64_t));
  return dest;
#else
  return (uint64_t{static_cast<uint8_t>(src[0])} << (7 * 8)) |
         (uint64_t{static_cast<uint8_t>(src[1])} << (6 * 8)) |
         (uint64_t{static_cast<uint8_t>(src[2])} << (5 * 8)) |
         (uint64_t{static_cast<uint8_t>(src[3])} << (4 * 8)) |
         (uint64_t{static_cast<uint8_t>(src[4])} << (3 * 8)) |
         (uint64_t{static_cast<uint8_t>(src[5])} << (2 * 8)) |
         (uint64_t{static_cast<uint8_t>(src[6])} << 8) |
         uint64_t{static_cast<uint8_t>(src[7])};
#endif
}

template <>
inline absl::uint128 ReadBigEndian<absl::uint128>(const char* src) {
#if ABSL_IS_BIG_ENDIAN
  absl::uint128 dest;
  std::memcpy(&dest, src, sizeof(absl::uint128));
  return dest;
#else
  const uint64_t high = ReadBigEndian<uint64_t>(src);
  const uint64_t low = ReadBigEndian<uint64_t>(src + sizeof(uint64_t));
  return absl::MakeUint128(high, low);
#endif
}

template <>
inline int8_t ReadBigEndian<int8_t>(const char* src) {
  return static_cast<int8_t>(*src);
}

template <>
inline int16_t ReadBigEndian<int16_t>(const char* src) {
  return static_cast<int16_t>(ReadBigEndian<uint16_t>(src));
}

template <>
inline int32_t ReadBigEndian<int32_t>(const char* src) {
  return static_cast<int32_t>(ReadBigEndian<uint32_t>(src));
}

template <>
inline int64_t ReadBigEndian<int64_t>(const char* src) {
  return static_cast<int64_t>(ReadBigEndian<uint64_t>(src));
}

template <>
inline absl::int128 ReadBigEndian<absl::int128>(const char* src) {
  return static_cast<absl::int128>(ReadBigEndian<absl::uint128>(src));
}

template <>
inline float ReadBigEndian<float>(const char* src) {
  return absl::bit_cast<float>(ReadBigEndian<uint32_t>(src));
}

template <>
inline double ReadBigEndian<double>(const char* src) {
  return absl::bit_cast<double>(ReadBigEndian<uint64_t>(src));
}

template <typename T>
inline void ReadLittleEndians(const char* src,
                              absl::Span<type_identity_t<T>> dest) {
#if ABSL_IS_LITTLE_ENDIAN
  riegeli::null_safe_memcpy(dest.data(), src, dest.size() * sizeof(T));
#else
  for (T& value : dest) {
    value = ReadLittleEndian<T>(src);
    src += sizeof(T);
  }
#endif
}

template <>
inline void ReadLittleEndians<uint8_t>(const char* src,
                                       absl::Span<uint8_t> dest) {
  riegeli::null_safe_memcpy(dest.data(), src, dest.size());
}

template <>
inline void ReadLittleEndians<int8_t>(const char* src,
                                      absl::Span<int8_t> dest) {
  riegeli::null_safe_memcpy(dest.data(), src, dest.size());
}

template <typename T>
inline void ReadBigEndians(const char* src,
                           absl::Span<type_identity_t<T>> dest) {
#if ABSL_IS_BIG_ENDIAN
  riegeli::null_safe_memcpy(dest.data(), src, dest.size() * sizeof(T));
#else
  if constexpr (sizeof(T) == 1) {
    riegeli::null_safe_memcpy(dest.data(), src, dest.size());
  } else {
    for (T& value : dest) {
      value = ReadBigEndian<T>(src);
      src += sizeof(T);
    }
  }
#endif
}

template <>
inline void ReadBigEndians<uint8_t>(const char* src, absl::Span<uint8_t> dest) {
  riegeli::null_safe_memcpy(dest.data(), src, dest.size());
}

template <>
inline void ReadBigEndians<int8_t>(const char* src, absl::Span<int8_t> dest) {
  riegeli::null_safe_memcpy(dest.data(), src, dest.size());
}

template <typename T>
inline bool ReadLittleEndian(Reader& src, type_identity_t<T>& dest) {
  if (ABSL_PREDICT_FALSE(!src.Pull(sizeof(T)))) return false;
  dest = ReadLittleEndian<T>(src.cursor());
  src.move_cursor(sizeof(T));
  return true;
}

template <>
inline bool ReadLittleEndian<uint8_t>(Reader& src, uint8_t& dest) {
  return src.ReadByte(dest);
}

template <>
inline bool ReadLittleEndian<int8_t>(Reader& src, int8_t& dest) {
  uint8_t byte;
  if (ABSL_PREDICT_FALSE(!src.ReadByte(byte))) return false;
  dest = static_cast<int8_t>(byte);
  return true;
}

template <typename T>
inline bool ReadBigEndian(Reader& src, type_identity_t<T>& dest) {
  if (ABSL_PREDICT_FALSE(!src.Pull(sizeof(T)))) return false;
  dest = ReadBigEndian<T>(src.cursor());
  src.move_cursor(sizeof(T));
  return true;
}

template <>
inline bool ReadBigEndian<uint8_t>(Reader& src, uint8_t& dest) {
  return src.ReadByte(dest);
}

template <>
inline bool ReadBigEndian<int8_t>(Reader& src, int8_t& dest) {
  uint8_t byte;
  if (ABSL_PREDICT_FALSE(!src.ReadByte(byte))) return false;
  dest = static_cast<int8_t>(byte);
  return true;
}

template <typename T>
inline bool ReadLittleEndians(Reader& src,
                              absl::Span<type_identity_t<T>> dest) {
#if ABSL_IS_LITTLE_ENDIAN
  return src.Read(dest.size() * sizeof(T),
                  reinterpret_cast<char*>(dest.data()));
#else
  for (T& dest_value : dest) {
    if (ABSL_PREDICT_FALSE(!ReadLittleEndian<T>(src, dest_value))) return false;
  }
  return true;
#endif
}

template <>
inline bool ReadLittleEndians<uint8_t>(Reader& src, absl::Span<uint8_t> dest) {
  return src.Read(dest.size(), reinterpret_cast<char*>(dest.data()));
}

template <>
inline bool ReadLittleEndians<int8_t>(Reader& src, absl::Span<int8_t> dest) {
  return src.Read(dest.size(), reinterpret_cast<char*>(dest.data()));
}

template <typename T>
inline bool ReadBigEndians(Reader& src, absl::Span<type_identity_t<T>> dest) {
#if ABSL_IS_BIG_ENDIAN
  return src.Read(dest.size() * sizeof(T),
                  reinterpret_cast<char*>(dest.data()));
#else
  for (T& dest_value : dest) {
    if (ABSL_PREDICT_FALSE(!ReadBigEndian<T>(src, dest_value))) return false;
  }
  return true;
#endif
}

template <>
inline bool ReadBigEndians<uint8_t>(Reader& src, absl::Span<uint8_t> dest) {
  return src.Read(dest.size(), reinterpret_cast<char*>(dest.data()));
}

template <>
inline bool ReadBigEndians<int8_t>(Reader& src, absl::Span<int8_t> dest) {
  return src.Read(dest.size(), reinterpret_cast<char*>(dest.data()));
}

}  // namespace riegeli

#endif  // RIEGELI_ENDIAN_ENDIAN_READING_H_
