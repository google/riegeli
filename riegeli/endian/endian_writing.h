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

#ifndef RIEGELI_ENDIAN_ENDIAN_WRITING_H_
#define RIEGELI_ENDIAN_ENDIAN_WRITING_H_

#include <stdint.h>

#include <cstring>

#include "absl/base/casts.h"
#include "absl/base/config.h"
#include "absl/base/macros.h"
#include "absl/base/optimization.h"
#include "absl/numeric/int128.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/null_safe_memcpy.h"
#include "riegeli/base/type_traits.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Writes a number in a fixed width Little/Big Endian encoding to an array.
// The width of the encoding is determined by the template argument, which
// must be one of: `{u,}int{8,16,32,64}_t`, `absl::{u,}int128`, `float`, or
// `double`.
//
// Writes `sizeof(T)` bytes to `dest[]`.
template <typename T>
void WriteLittleEndian(type_identity_t<T> data, char* dest);
template <typename T>
void WriteBigEndian(type_identity_t<T> data, char* dest);

// Writes an array of numbers in a fixed width Little/Big Endian encoding.
// The width of the encoding is determined by the template argument, which
// must be one of: `{u,}int{8,16,32,64}_t`, `absl::{u,}int128`, `float`, or
// `double`.
//
// This is faster than writing them individually if the endianness matches the
// native one.
//
// Writes `data.size() * sizeof(T)` bytes to `dest[]`.
template <typename T>
void WriteLittleEndians(absl::Span<const type_identity_t<T>> data, char* dest);
template <typename T>
void WriteBigEndians(absl::Span<const type_identity_t<T>> data, char* dest);

// Writes a number in a fixed width Little/Big Endian encoding. The width of
// the encoding is determined by the template argument, which must be one of:
// `{u,}int{8,16,32,64}_t`, `absl::{u,}int128`, `float`, or `double`.
//
// Return values:
//  * `true`  - success (`dest.ok()`)
//  * `false` - failure (`!dest.ok()`)
template <typename T>
bool WriteLittleEndian(type_identity_t<T> data, Writer& dest);
template <typename T>
bool WriteBigEndian(type_identity_t<T> data, Writer& dest);

// Writes an array of numbers in a fixed width Little/Big Endian encoding.
// The width of the encoding is determined by the template argument, which
// must be one of: `{u,}int{8,16,32,64}_t`, `absl::{u,}int128`, `float`, or
// `double`.
//
// This is faster than writing them individually if the endianness matches the
// native one.
//
// Return values:
//  * `true`  - success (`dest.ok()`)
//  * `false` - failure (`!dest.ok()`)
template <typename T>
bool WriteLittleEndians(absl::Span<const type_identity_t<T>> data,
                        Writer& dest);
template <typename T>
bool WriteBigEndians(absl::Span<const type_identity_t<T>> data, Writer& dest);

// Writes a number in a fixed width Little/Big Endian encoding to a
// `BackwardWriter`. The width of the encoding is determined by the template
// argument, which must be one of: `{u,}int{8,16,32,64}_t`, `absl::{u,}int128`,
// `float`, or `double`.
//
// Return values:
//  * `true`  - success (`dest.ok()`)
//  * `false` - failure (`!dest.ok()`)
template <typename T>
bool WriteLittleEndian(type_identity_t<T> data, BackwardWriter& dest);
template <typename T>
bool WriteBigEndian(type_identity_t<T> data, BackwardWriter& dest);

// Writes an array of numbers in a fixed width Little/Big Endian encoding to a
// `BackwardWriter`. The width of the encoding is determined by the template
// argument, which must be one of: `{u,}int{8,16,32,64}_t`, `absl::{u,}int128`,
// `float`, or `double`.
//
// This is faster than writing them individually if the endianness matches the
// native one.
//
// Return values:
//  * `true`  - success (`dest.ok()`)
//  * `false` - failure (`!dest.ok()`)
template <typename T>
bool WriteLittleEndians(absl::Span<const type_identity_t<T>> data,
                        BackwardWriter& dest);
template <typename T>
bool WriteBigEndians(absl::Span<const type_identity_t<T>> data,
                     BackwardWriter& dest);

// Implementation details follow.

template <>
inline void WriteLittleEndian<uint8_t>(uint8_t data, char* dest) {
  *dest = static_cast<char>(data);
}

template <>
inline void WriteLittleEndian<uint16_t>(uint16_t data, char* dest) {
#if ABSL_IS_LITTLE_ENDIAN
  std::memcpy(dest, &data, sizeof(uint16_t));
#else
  dest[0] = static_cast<char>(data);
  dest[1] = static_cast<char>(data >> 8);
#endif
}

template <>
inline void WriteLittleEndian<uint32_t>(uint32_t data, char* dest) {
#if ABSL_IS_LITTLE_ENDIAN
  std::memcpy(dest, &data, sizeof(uint32_t));
#else
  dest[0] = static_cast<char>(data);
  dest[1] = static_cast<char>(data >> 8);
  dest[2] = static_cast<char>(data >> (2 * 8));
  dest[3] = static_cast<char>(data >> (3 * 8));
#endif
}

template <>
inline void WriteLittleEndian<uint64_t>(uint64_t data, char* dest) {
#if ABSL_IS_LITTLE_ENDIAN
  std::memcpy(dest, &data, sizeof(uint64_t));
#else
  dest[0] = static_cast<char>(data);
  dest[1] = static_cast<char>(data >> 8);
  dest[2] = static_cast<char>(data >> (2 * 8));
  dest[3] = static_cast<char>(data >> (3 * 8));
  dest[4] = static_cast<char>(data >> (4 * 8));
  dest[5] = static_cast<char>(data >> (5 * 8));
  dest[6] = static_cast<char>(data >> (6 * 8));
  dest[7] = static_cast<char>(data >> (7 * 8));
#endif
}

template <>
inline void WriteLittleEndian<absl::uint128>(absl::uint128 data, char* dest) {
#if ABSL_IS_LITTLE_ENDIAN
  std::memcpy(dest, &data, sizeof(absl::uint128));
#else
  WriteLittleEndian<uint64_t>(absl::Uint128Low64(data), dest);
  WriteLittleEndian<uint64_t>(absl::Uint128High64(data),
                              dest + sizeof(uint64_t));
#endif
}

template <>
inline void WriteLittleEndian<int8_t>(int8_t data, char* dest) {
  *dest = static_cast<char>(data);
}

template <>
inline void WriteLittleEndian<int16_t>(int16_t data, char* dest) {
  WriteLittleEndian<uint16_t>(static_cast<uint16_t>(data), dest);
}

template <>
inline void WriteLittleEndian<int32_t>(int32_t data, char* dest) {
  WriteLittleEndian<uint32_t>(static_cast<uint32_t>(data), dest);
}

template <>
inline void WriteLittleEndian<int64_t>(int64_t data, char* dest) {
  WriteLittleEndian<uint64_t>(static_cast<uint64_t>(data), dest);
}

template <>
inline void WriteLittleEndian<absl::int128>(absl::int128 data, char* dest) {
  WriteLittleEndian<absl::uint128>(static_cast<absl::uint128>(data), dest);
}

template <>
inline void WriteLittleEndian<float>(float data, char* dest) {
  WriteLittleEndian<uint32_t>(absl::bit_cast<uint32_t>(data), dest);
}

template <>
inline void WriteLittleEndian<double>(double data, char* dest) {
  WriteLittleEndian<uint64_t>(absl::bit_cast<uint64_t>(data), dest);
}

template <>
inline void WriteBigEndian<uint8_t>(uint8_t data, char* dest) {
  *dest = static_cast<char>(data);
}

template <>
inline void WriteBigEndian<uint16_t>(uint16_t data, char* dest) {
#if ABSL_IS_BIG_ENDIAN
  std::memcpy(dest, &data, sizeof(uint16_t));
#else
  dest[0] = static_cast<char>(data >> 8);
  dest[1] = static_cast<char>(data);
#endif
}

template <>
inline void WriteBigEndian<uint32_t>(uint32_t data, char* dest) {
#if ABSL_IS_BIG_ENDIAN
  std::memcpy(dest, &data, sizeof(uint32_t));
#else
  dest[0] = static_cast<char>(data >> (3 * 8));
  dest[1] = static_cast<char>(data >> (2 * 8));
  dest[2] = static_cast<char>(data >> 8);
  dest[3] = static_cast<char>(data);
#endif
}

template <>
inline void WriteBigEndian<uint64_t>(uint64_t data, char* dest) {
#if ABSL_IS_BIG_ENDIAN
  std::memcpy(dest, &data, sizeof(uint64_t));
#else
  dest[0] = static_cast<char>(data >> (7 * 8));
  dest[1] = static_cast<char>(data >> (6 * 8));
  dest[2] = static_cast<char>(data >> (5 * 8));
  dest[3] = static_cast<char>(data >> (4 * 8));
  dest[4] = static_cast<char>(data >> (3 * 8));
  dest[5] = static_cast<char>(data >> (2 * 8));
  dest[6] = static_cast<char>(data >> 8);
  dest[7] = static_cast<char>(data);
#endif
}

template <>
inline void WriteBigEndian<absl::uint128>(absl::uint128 data, char* dest) {
#if ABSL_IS_BIG_ENDIAN
  std::memcpy(dest, &data, sizeof(absl::uint128));
#else
  WriteBigEndian<uint64_t>(absl::Uint128High64(data), dest);
  WriteBigEndian<uint64_t>(absl::Uint128Low64(data), dest + sizeof(uint64_t));
#endif
}

template <>
inline void WriteBigEndian<int8_t>(int8_t data, char* dest) {
  *dest = static_cast<char>(data);
}

template <>
inline void WriteBigEndian<int16_t>(int16_t data, char* dest) {
  WriteBigEndian<uint16_t>(static_cast<uint16_t>(data), dest);
}

template <>
inline void WriteBigEndian<int32_t>(int32_t data, char* dest) {
  WriteBigEndian<uint32_t>(static_cast<uint32_t>(data), dest);
}

template <>
inline void WriteBigEndian<int64_t>(int64_t data, char* dest) {
  WriteBigEndian<uint64_t>(static_cast<uint64_t>(data), dest);
}

template <>
inline void WriteBigEndian<absl::int128>(absl::int128 data, char* dest) {
  WriteBigEndian<absl::uint128>(static_cast<absl::uint128>(data), dest);
}

template <>
inline void WriteBigEndian<float>(float data, char* dest) {
  WriteBigEndian<uint32_t>(absl::bit_cast<uint32_t>(data), dest);
}

template <>
inline void WriteBigEndian<double>(double data, char* dest) {
  WriteBigEndian<uint64_t>(absl::bit_cast<uint64_t>(data), dest);
}

template <typename T>
inline void WriteLittleEndians(absl::Span<const type_identity_t<T>> data,
                               char* dest) {
#if ABSL_IS_LITTLE_ENDIAN
  riegeli::null_safe_memcpy(dest, data.data(), data.size() * sizeof(T));
#else
  for (const T value : data) {
    WriteLittleEndian<T>(value, dest);
    dest += sizeof(T);
  }
#endif
}

template <>
inline void WriteLittleEndians<uint8_t>(absl::Span<const uint8_t> data,
                                        char* dest) {
  riegeli::null_safe_memcpy(dest, data.data(), data.size());
}

template <>
inline void WriteLittleEndians<int8_t>(absl::Span<const int8_t> data,
                                       char* dest) {
  riegeli::null_safe_memcpy(dest, data.data(), data.size());
}

template <typename T>
inline void WriteBigEndians(absl::Span<const type_identity_t<T>> data,
                            char* dest) {
#if ABSL_IS_BIG_ENDIAN
  riegeli::null_safe_memcpy(dest, data.data(), data.size() * sizeof(T));
#else
  for (const T value : data) {
    WriteBigEndian<T>(value, dest);
    dest += sizeof(T);
  }
#endif
}

template <>
inline void WriteBigEndians<uint8_t>(absl::Span<const uint8_t> data,
                                     char* dest) {
  riegeli::null_safe_memcpy(dest, data.data(), data.size());
}

template <>
inline void WriteBigEndians<int8_t>(absl::Span<const int8_t> data, char* dest) {
  riegeli::null_safe_memcpy(dest, data.data(), data.size());
}

template <typename T>
inline bool WriteLittleEndian(type_identity_t<T> data, Writer& dest) {
  if (ABSL_PREDICT_FALSE(!dest.Push(sizeof(T)))) return false;
  WriteLittleEndian<T>(data, dest.cursor());
  dest.move_cursor(sizeof(T));
  return true;
}

template <>
inline bool WriteLittleEndian<uint8_t>(uint8_t data, Writer& dest) {
  return dest.WriteByte(data);
}

template <>
inline bool WriteLittleEndian<int8_t>(int8_t data, Writer& dest) {
  return dest.WriteByte(static_cast<uint8_t>(data));
}

template <typename T>
inline bool WriteBigEndian(type_identity_t<T> data, Writer& dest) {
  if (ABSL_PREDICT_FALSE(!dest.Push(sizeof(T)))) return false;
  WriteBigEndian<T>(data, dest.cursor());
  dest.move_cursor(sizeof(T));
  return true;
}

template <>
inline bool WriteBigEndian<uint8_t>(uint8_t data, Writer& dest) {
  return dest.WriteByte(data);
}

template <>
inline bool WriteBigEndian<int8_t>(int8_t data, Writer& dest) {
  return dest.WriteByte(static_cast<uint8_t>(data));
}

template <typename T>
inline bool WriteLittleEndians(absl::Span<const type_identity_t<T>> data,
                               Writer& dest) {
#if ABSL_IS_LITTLE_ENDIAN
  return dest.Write(absl::string_view(
      reinterpret_cast<const char*>(data.data()), data.size() * sizeof(T)));
#else
  for (const T value : data) {
    if (ABSL_PREDICT_FALSE(!WriteLittleEndian<T>(value, dest))) {
      return false;
    }
  }
  return true;
#endif
}

template <>
inline bool WriteLittleEndians<uint8_t>(absl::Span<const uint8_t> data,
                                        Writer& dest) {
  return dest.Write(absl::string_view(
      reinterpret_cast<const char*>(data.data()), data.size()));
}

template <>
inline bool WriteLittleEndians<int8_t>(absl::Span<const int8_t> data,
                                       Writer& dest) {
  return dest.Write(absl::string_view(
      reinterpret_cast<const char*>(data.data()), data.size()));
}

template <typename T>
inline bool WriteBigEndians(absl::Span<const type_identity_t<T>> data,
                            Writer& dest) {
#if ABSL_IS_BIG_ENDIAN
  return dest.Write(absl::string_view(
      reinterpret_cast<const char*>(data.data()), data.size() * sizeof(T)));
#else
  for (const T value : data) {
    if (ABSL_PREDICT_FALSE(!WriteBigEndian<T>(value, dest))) {
      return false;
    }
  }
  return true;
#endif
}

template <>
inline bool WriteBigEndians<uint8_t>(absl::Span<const uint8_t> data,
                                     Writer& dest) {
  return dest.Write(absl::string_view(
      reinterpret_cast<const char*>(data.data()), data.size()));
}

template <>
inline bool WriteBigEndians<int8_t>(absl::Span<const int8_t> data,
                                    Writer& dest) {
  return dest.Write(absl::string_view(
      reinterpret_cast<const char*>(data.data()), data.size()));
}

template <typename T>
inline bool WriteLittleEndian(type_identity_t<T> data, BackwardWriter& dest) {
  if (ABSL_PREDICT_FALSE(!dest.Push(sizeof(T)))) return false;
  dest.move_cursor(sizeof(T));
  WriteLittleEndian<T>(data, dest.cursor());
  return true;
}

template <>
inline bool WriteLittleEndian<uint8_t>(uint8_t data, BackwardWriter& dest) {
  return dest.WriteByte(data);
}

template <>
inline bool WriteLittleEndian<int8_t>(int8_t data, BackwardWriter& dest) {
  return dest.WriteByte(static_cast<uint8_t>(data));
}

template <typename T>
inline bool WriteBigEndian(type_identity_t<T> data, BackwardWriter& dest) {
  if (ABSL_PREDICT_FALSE(!dest.Push(sizeof(T)))) return false;
  dest.move_cursor(sizeof(T));
  WriteBigEndian<T>(data, dest.cursor());
  return true;
}

template <>
inline bool WriteBigEndian<uint8_t>(uint8_t data, BackwardWriter& dest) {
  return dest.WriteByte(data);
}

template <>
inline bool WriteBigEndian<int8_t>(int8_t data, BackwardWriter& dest) {
  return dest.WriteByte(static_cast<uint8_t>(data));
}

template <typename T>
inline bool WriteLittleEndians(absl::Span<const type_identity_t<T>> data,
                               BackwardWriter& dest) {
#if ABSL_IS_LITTLE_ENDIAN
  return dest.Write(absl::string_view(
      reinterpret_cast<const char*>(data.data()), data.size() * sizeof(T)));
#else
  for (auto iter = data.crbegin(); iter != data.crend(); ++iter) {
    if (ABSL_PREDICT_FALSE(!WriteLittleEndian<T>(*iter, dest))) {
      return false;
    }
  }
  return true;
#endif
}

template <>
inline bool WriteLittleEndians<uint8_t>(absl::Span<const uint8_t> data,
                                        BackwardWriter& dest) {
  return dest.Write(absl::string_view(
      reinterpret_cast<const char*>(data.data()), data.size()));
}

template <>
inline bool WriteLittleEndians<int8_t>(absl::Span<const int8_t> data,
                                       BackwardWriter& dest) {
  return dest.Write(absl::string_view(
      reinterpret_cast<const char*>(data.data()), data.size()));
}

template <typename T>
inline bool WriteBigEndians(absl::Span<const type_identity_t<T>> data,
                            BackwardWriter& dest) {
#if ABSL_IS_BIG_ENDIAN
  return dest.Write(absl::string_view(
      reinterpret_cast<const char*>(data.data()), data.size() * sizeof(T)));
#else
  for (auto iter = data.crbegin(); iter != data.crend(); ++iter) {
    if (ABSL_PREDICT_FALSE(!WriteBigEndian<T>(*iter, dest))) {
      return false;
    }
  }
  return true;
#endif
}

template <>
inline bool WriteBigEndians<uint8_t>(absl::Span<const uint8_t> data,
                                     BackwardWriter& dest) {
  return dest.Write(absl::string_view(
      reinterpret_cast<const char*>(data.data()), data.size()));
}

template <>
inline bool WriteBigEndians<int8_t>(absl::Span<const int8_t> data,
                                    BackwardWriter& dest) {
  return dest.Write(absl::string_view(
      reinterpret_cast<const char*>(data.data()), data.size()));
}

// Deprecated aliases.

ABSL_DEPRECATE_AND_INLINE()
inline void WriteLittleEndian16(uint16_t data, char* dest) {
  WriteLittleEndian<uint16_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteLittleEndian32(uint32_t data, char* dest) {
  WriteLittleEndian<uint32_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteLittleEndian64(uint64_t data, char* dest) {
  WriteLittleEndian<uint64_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteLittleEndian128(absl::uint128 data, char* dest) {
  WriteLittleEndian<absl::uint128>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteLittleEndianSigned16(int16_t data, char* dest) {
  WriteLittleEndian<int16_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteLittleEndianSigned32(int32_t data, char* dest) {
  WriteLittleEndian<int32_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteLittleEndianSigned64(int64_t data, char* dest) {
  WriteLittleEndian<int64_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteLittleEndianSigned128(absl::int128 data, char* dest) {
  WriteLittleEndian<absl::int128>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteLittleEndianFloat(float data, char* dest) {
  WriteLittleEndian<float>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteLittleEndianDouble(double data, char* dest) {
  WriteLittleEndian<double>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteBigEndian16(uint16_t data, char* dest) {
  WriteBigEndian<uint16_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteBigEndian32(uint32_t data, char* dest) {
  WriteBigEndian<uint32_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteBigEndian64(uint64_t data, char* dest) {
  WriteBigEndian<uint64_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteBigEndian128(absl::uint128 data, char* dest) {
  WriteBigEndian<absl::uint128>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteBigEndianSigned16(int16_t data, char* dest) {
  WriteBigEndian<int16_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteBigEndianSigned32(int32_t data, char* dest) {
  WriteBigEndian<int32_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteBigEndianSigned64(int64_t data, char* dest) {
  WriteBigEndian<int64_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteBigEndianSigned128(absl::int128 data, char* dest) {
  WriteBigEndian<absl::int128>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteBigEndianFloat(float data, char* dest) {
  WriteBigEndian<float>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteBigEndianDouble(double data, char* dest) {
  WriteBigEndian<double>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteLittleEndian16s(absl::Span<const uint16_t> data, char* dest) {
  WriteLittleEndians<uint16_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteLittleEndian32s(absl::Span<const uint32_t> data, char* dest) {
  WriteLittleEndians<uint32_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteLittleEndian64s(absl::Span<const uint64_t> data, char* dest) {
  WriteLittleEndians<uint64_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteLittleEndian128s(absl::Span<const absl::uint128> data,
                                  char* dest) {
  WriteLittleEndians<absl::uint128>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteLittleEndianSigned16s(absl::Span<const int16_t> data,
                                       char* dest) {
  WriteLittleEndians<int16_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteLittleEndianSigned32s(absl::Span<const int32_t> data,
                                       char* dest) {
  WriteLittleEndians<int32_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteLittleEndianSigned64s(absl::Span<const int64_t> data,
                                       char* dest) {
  WriteLittleEndians<int64_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteLittleEndianSigned128s(absl::Span<const absl::int128> data,
                                        char* dest) {
  WriteLittleEndians<absl::int128>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteLittleEndianFloats(absl::Span<const float> data, char* dest) {
  WriteLittleEndians<float>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteLittleEndianDoubles(absl::Span<const double> data,
                                     char* dest) {
  WriteLittleEndians<double>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteBigEndian16s(absl::Span<const uint16_t> data, char* dest) {
  WriteBigEndians<uint16_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteBigEndian32s(absl::Span<const uint32_t> data, char* dest) {
  WriteBigEndians<uint32_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteBigEndian64s(absl::Span<const uint64_t> data, char* dest) {
  WriteBigEndians<uint64_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteBigEndian128s(absl::Span<const absl::uint128> data,
                               char* dest) {
  WriteBigEndians<absl::uint128>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteBigEndianSigned16s(absl::Span<const int16_t> data,
                                    char* dest) {
  WriteBigEndians<int16_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteBigEndianSigned32s(absl::Span<const int32_t> data,
                                    char* dest) {
  WriteBigEndians<int32_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteBigEndianSigned64s(absl::Span<const int64_t> data,
                                    char* dest) {
  WriteBigEndians<int64_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteBigEndianSigned128s(absl::Span<const absl::int128> data,
                                     char* dest) {
  WriteBigEndians<absl::int128>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteBigEndianFloats(absl::Span<const float> data, char* dest) {
  WriteBigEndians<float>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline void WriteBigEndianDoubles(absl::Span<const double> data, char* dest) {
  WriteBigEndians<double>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteLittleEndian16(uint16_t data, Writer& dest) {
  return WriteLittleEndian<uint16_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteLittleEndian32(uint32_t data, Writer& dest) {
  return WriteLittleEndian<uint32_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteLittleEndian64(uint64_t data, Writer& dest) {
  return WriteLittleEndian<uint64_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteLittleEndian128(absl::uint128 data, Writer& dest) {
  return WriteLittleEndian<absl::uint128>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteLittleEndianSigned16(int16_t data, Writer& dest) {
  return WriteLittleEndian<int16_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteLittleEndianSigned32(int32_t data, Writer& dest) {
  return WriteLittleEndian<int32_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteLittleEndianSigned64(int64_t data, Writer& dest) {
  return WriteLittleEndian<int64_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteLittleEndianSigned128(absl::int128 data, Writer& dest) {
  return WriteLittleEndian<absl::int128>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteLittleEndianFloat(float data, Writer& dest) {
  return WriteLittleEndian<float>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteLittleEndianDouble(double data, Writer& dest) {
  return WriteLittleEndian<double>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteBigEndian16(uint16_t data, Writer& dest) {
  return WriteBigEndian<uint16_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteBigEndian32(uint32_t data, Writer& dest) {
  return WriteBigEndian<uint32_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteBigEndian64(uint64_t data, Writer& dest) {
  return WriteBigEndian<uint64_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteBigEndian128(absl::uint128 data, Writer& dest) {
  return WriteBigEndian<absl::uint128>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteBigEndianSigned16(int16_t data, Writer& dest) {
  return WriteBigEndian<int16_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteBigEndianSigned32(int32_t data, Writer& dest) {
  return WriteBigEndian<int32_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteBigEndianSigned64(int64_t data, Writer& dest) {
  return WriteBigEndian<int64_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteBigEndianSigned128(absl::int128 data, Writer& dest) {
  return WriteBigEndian<absl::int128>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteBigEndianFloat(float data, Writer& dest) {
  return WriteBigEndian<float>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteBigEndianDouble(double data, Writer& dest) {
  return WriteBigEndian<double>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteLittleEndian16s(absl::Span<const uint16_t> data,
                                 Writer& dest) {
  return WriteLittleEndians<uint16_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteLittleEndian32s(absl::Span<const uint32_t> data,
                                 Writer& dest) {
  return WriteLittleEndians<uint32_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteLittleEndian64s(absl::Span<const uint64_t> data,
                                 Writer& dest) {
  return WriteLittleEndians<uint64_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteLittleEndian128s(absl::Span<const absl::uint128> data,
                                  Writer& dest) {
  return WriteLittleEndians<absl::uint128>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteLittleEndianSigned16s(absl::Span<const int16_t> data,
                                       Writer& dest) {
  return WriteLittleEndians<int16_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteLittleEndianSigned32s(absl::Span<const int32_t> data,
                                       Writer& dest) {
  return WriteLittleEndians<int32_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteLittleEndianSigned64s(absl::Span<const int64_t> data,
                                       Writer& dest) {
  return WriteLittleEndians<int64_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteLittleEndianSigned128s(absl::Span<const absl::int128> data,
                                        Writer& dest) {
  return WriteLittleEndians<absl::int128>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteLittleEndianFloats(absl::Span<const float> data,
                                    Writer& dest) {
  return WriteLittleEndians<float>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteLittleEndianDoubles(absl::Span<const double> data,
                                     Writer& dest) {
  return WriteLittleEndians<double>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteBigEndian16s(absl::Span<const uint16_t> data, Writer& dest) {
  return WriteBigEndians<uint16_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteBigEndian32s(absl::Span<const uint32_t> data, Writer& dest) {
  return WriteBigEndians<uint32_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteBigEndian64s(absl::Span<const uint64_t> data, Writer& dest) {
  return WriteBigEndians<uint64_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteBigEndian128s(absl::Span<const absl::uint128> data,
                               Writer& dest) {
  return WriteBigEndians<absl::uint128>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteBigEndianSigned16s(absl::Span<const int16_t> data,
                                    Writer& dest) {
  return WriteBigEndians<int16_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteBigEndianSigned32s(absl::Span<const int32_t> data,
                                    Writer& dest) {
  return WriteBigEndians<int32_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteBigEndianSigned64s(absl::Span<const int64_t> data,
                                    Writer& dest) {
  return WriteBigEndians<int64_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteBigEndianSigned128s(absl::Span<const absl::int128> data,
                                     Writer& dest) {
  return WriteBigEndians<absl::int128>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteBigEndianFloats(absl::Span<const float> data, Writer& dest) {
  return WriteBigEndians<float>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteBigEndianDoubles(absl::Span<const double> data, Writer& dest) {
  return WriteBigEndians<double>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteLittleEndian16(uint16_t data, BackwardWriter& dest) {
  return WriteLittleEndian<uint16_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteLittleEndian32(uint32_t data, BackwardWriter& dest) {
  return WriteLittleEndian<uint32_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteLittleEndian64(uint64_t data, BackwardWriter& dest) {
  return WriteLittleEndian<uint64_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteLittleEndian128(absl::uint128 data, BackwardWriter& dest) {
  return WriteLittleEndian<absl::uint128>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteLittleEndianSigned16(int16_t data, BackwardWriter& dest) {
  return WriteLittleEndian<int16_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteLittleEndianSigned32(int32_t data, BackwardWriter& dest) {
  return WriteLittleEndian<int32_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteLittleEndianSigned64(int64_t data, BackwardWriter& dest) {
  return WriteLittleEndian<int64_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteLittleEndianSigned128(absl::int128 data,
                                       BackwardWriter& dest) {
  return WriteLittleEndian<absl::int128>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteLittleEndianFloat(float data, BackwardWriter& dest) {
  return WriteLittleEndian<float>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteLittleEndianDouble(double data, BackwardWriter& dest) {
  return WriteLittleEndian<double>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteBigEndian16(uint16_t data, BackwardWriter& dest) {
  return WriteBigEndian<uint16_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteBigEndian32(uint32_t data, BackwardWriter& dest) {
  return WriteBigEndian<uint32_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteBigEndian64(uint64_t data, BackwardWriter& dest) {
  return WriteBigEndian<uint64_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteBigEndian128(absl::uint128 data, BackwardWriter& dest) {
  return WriteBigEndian<absl::uint128>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteBigEndianSigned16(int16_t data, BackwardWriter& dest) {
  return WriteBigEndian<int16_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteBigEndianSigned32(int32_t data, BackwardWriter& dest) {
  return WriteBigEndian<int32_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteBigEndianSigned64(int64_t data, BackwardWriter& dest) {
  return WriteBigEndian<int64_t>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteBigEndianSigned128(absl::int128 data, BackwardWriter& dest) {
  return WriteBigEndian<absl::int128>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteBigEndianFloat(float data, BackwardWriter& dest) {
  return WriteBigEndian<float>(data, dest);
}

ABSL_DEPRECATE_AND_INLINE()
inline bool WriteBigEndianDouble(double data, BackwardWriter& dest) {
  return WriteBigEndian<double>(data, dest);
}

}  // namespace riegeli

#endif  // RIEGELI_ENDIAN_ENDIAN_WRITING_H_
