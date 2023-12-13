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
#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/type_traits.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Writes a number in a fixed width Little/Big Endian encoding.
//
// Return values:
//  * `true`  - success (`dest.ok()`)
//  * `false` - failure (`!dest.ok()`)
bool WriteLittleEndian16(uint16_t data, Writer& dest);
bool WriteLittleEndian32(uint32_t data, Writer& dest);
bool WriteLittleEndian64(uint64_t data, Writer& dest);
bool WriteLittleEndianSigned16(int16_t data, Writer& dest);
bool WriteLittleEndianSigned32(int32_t data, Writer& dest);
bool WriteLittleEndianSigned64(int64_t data, Writer& dest);
bool WriteLittleEndianFloat(float data, Writer& dest);
bool WriteLittleEndianDouble(double data, Writer& dest);
bool WriteBigEndian16(uint16_t data, Writer& dest);
bool WriteBigEndian32(uint32_t data, Writer& dest);
bool WriteBigEndian64(uint64_t data, Writer& dest);
bool WriteBigEndianSigned16(int16_t data, Writer& dest);
bool WriteBigEndianSigned32(int32_t data, Writer& dest);
bool WriteBigEndianSigned64(int64_t data, Writer& dest);
bool WriteBigEndianFloat(float data, Writer& dest);
bool WriteBigEndianDouble(double data, Writer& dest);
bool WriteLittleEndian16(uint16_t data, BackwardWriter& dest);
bool WriteLittleEndian32(uint32_t data, BackwardWriter& dest);
bool WriteLittleEndian64(uint64_t data, BackwardWriter& dest);
bool WriteLittleEndianSigned16(int16_t data, BackwardWriter& dest);
bool WriteLittleEndianSigned32(int32_t data, BackwardWriter& dest);
bool WriteLittleEndianSigned64(int64_t data, BackwardWriter& dest);
bool WriteLittleEndianFloat(float data, BackwardWriter& dest);
bool WriteLittleEndianDouble(double data, BackwardWriter& dest);
bool WriteBigEndian16(uint16_t data, BackwardWriter& dest);
bool WriteBigEndian32(uint32_t data, BackwardWriter& dest);
bool WriteBigEndian64(uint64_t data, BackwardWriter& dest);
bool WriteBigEndianSigned16(int16_t data, BackwardWriter& dest);
bool WriteBigEndianSigned32(int32_t data, BackwardWriter& dest);
bool WriteBigEndianSigned64(int64_t data, BackwardWriter& dest);
bool WriteBigEndianFloat(float data, BackwardWriter& dest);
bool WriteBigEndianDouble(double data, BackwardWriter& dest);

// Writes a number in a fixed width Little/Big Endian encoding. The width of
// the encoding is determined by the template argument, which must be one of:
// `{u,}int{8,16,32,64}_t`, `float`, or `double`.
//
// Return values:
//  * `true`  - success (`dest.ok()`)
//  * `false` - failure (`!dest.ok()`)
template <typename T>
bool WriteLittleEndian(type_identity_t<T> data, Writer& dest);
template <typename T>
bool WriteBigEndian(type_identity_t<T> data, Writer& dest);

// Writes an array of numbers in a fixed width Little/Big Endian encoding.
//
// This is faster than writing them individually if the endianness matches the
// native one.
//
// Return values:
//  * `true`  - success (`dest.ok()`)
//  * `false` - failure (`!dest.ok()`)
bool WriteLittleEndian16s(absl::Span<const uint16_t> data, Writer& dest);
bool WriteLittleEndian32s(absl::Span<const uint32_t> data, Writer& dest);
bool WriteLittleEndian64s(absl::Span<const uint64_t> data, Writer& dest);
bool WriteLittleEndianSigned16s(absl::Span<const int16_t> data, Writer& dest);
bool WriteLittleEndianSigned32s(absl::Span<const int32_t> data, Writer& dest);
bool WriteLittleEndianSigned64s(absl::Span<const int64_t> data, Writer& dest);
bool WriteLittleEndianFloats(absl::Span<const float> data, Writer& dest);
bool WriteLittleEndianDoubles(absl::Span<const double> data, Writer& dest);
bool WriteBigEndian16s(absl::Span<const uint16_t> data, Writer& dest);
bool WriteBigEndian32s(absl::Span<const uint32_t> data, Writer& dest);
bool WriteBigEndian64s(absl::Span<const uint64_t> data, Writer& dest);
bool WriteBigEndianSigned16s(absl::Span<const int16_t> data, Writer& dest);
bool WriteBigEndianSigned32s(absl::Span<const int32_t> data, Writer& dest);
bool WriteBigEndianSigned64s(absl::Span<const int64_t> data, Writer& dest);
bool WriteBigEndianFloats(absl::Span<const float> data, Writer& dest);
bool WriteBigEndianDoubles(absl::Span<const double> data, Writer& dest);

// Writes an array of numbers in a fixed width Little/Big Endian encoding.
// The width of the encoding is determined by the template argument, which
// must be one of: `{u,}int{8,16,32,64}_t`, `float`, or `double`.
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

// Writes a number in a fixed width Little/Big Endian encoding to an array.
//
// Writes `sizeof({u,}int{16,32,64}_t)` bytes to `dest[]`.
void WriteLittleEndian16(uint16_t data, char* dest);
void WriteLittleEndian32(uint32_t data, char* dest);
void WriteLittleEndian64(uint64_t data, char* dest);
void WriteLittleEndianSigned16(int16_t data, char* dest);
void WriteLittleEndianSigned32(int32_t data, char* dest);
void WriteLittleEndianSigned64(int64_t data, char* dest);
void WriteLittleEndianFloat(float data, char* dest);
void WriteLittleEndianDouble(double data, char* dest);
void WriteBigEndian16(uint16_t data, char* dest);
void WriteBigEndian32(uint32_t data, char* dest);
void WriteBigEndian64(uint64_t data, char* dest);
void WriteBigEndianSigned16(int16_t data, char* dest);
void WriteBigEndianSigned32(int32_t data, char* dest);
void WriteBigEndianSigned64(int64_t data, char* dest);
void WriteBigEndianFloat(float data, char* dest);
void WriteBigEndianDouble(double data, char* dest);

// Writes a number in a fixed width Little/Big Endian encoding to an array.
// The width of the encoding is determined by the template argument, which
// must be one of: `{u,}int{8,16,32,64}_t`, `float`, or `double`.
//
// Writes `sizeof(T)` bytes to `dest[]`.
template <typename T>
void WriteLittleEndian(type_identity_t<T> data, char* dest);
template <typename T>
void WriteBigEndian(type_identity_t<T> data, char* dest);

// Writes an array of numbers in a fixed width Little/Big Endian encoding to an
// array.
//
// This is faster than writing them individually if the endianness matches the
// native one.
//
// Writes `data.size() * sizeof({u,}int{16,32,64}_t)` bytes to `dest[]`.
void WriteLittleEndian16s(absl::Span<const uint16_t> data, char* dest);
void WriteLittleEndian32s(absl::Span<const uint32_t> data, char* dest);
void WriteLittleEndian64s(absl::Span<const uint64_t> data, char* dest);
void WriteLittleEndianSigned16s(absl::Span<const int16_t> data, char* dest);
void WriteLittleEndianSigned32s(absl::Span<const int32_t> data, char* dest);
void WriteLittleEndianSigned64s(absl::Span<const int64_t> data, char* dest);
void WriteLittleEndianFloats(absl::Span<const float> data, char* dest);
void WriteLittleEndianDoubles(absl::Span<const double> data, char* dest);
void WriteBigEndian16s(absl::Span<const uint16_t> data, char* dest);
void WriteBigEndian32s(absl::Span<const uint32_t> data, char* dest);
void WriteBigEndian64s(absl::Span<const uint64_t> data, char* dest);
void WriteBigEndianSigned16s(absl::Span<const int16_t> data, char* dest);
void WriteBigEndianSigned32s(absl::Span<const int32_t> data, char* dest);
void WriteBigEndianSigned64s(absl::Span<const int64_t> data, char* dest);
void WriteBigEndianFloats(absl::Span<const float> data, char* dest);
void WriteBigEndianDoubles(absl::Span<const double> data, char* dest);

// Writes an array of numbers in a fixed width Little/Big Endian encoding.
// The width of the encoding is determined by the template argument, which
// must be one of: `{u,}int{8,16,32,64}_t`, `float`, or `double`.
//
// This is faster than writing them individually if the endianness matches the
// native one.
//
// Return values:
//  * `true`  - success (`dest.ok()`)
//  * `false` - failure (`!dest.ok()`)
template <typename T>
void WriteLittleEndians(absl::Span<const type_identity_t<T>> data, char* dest);
template <typename T>
void WriteBigEndians(absl::Span<const type_identity_t<T>> data, char* dest);

// Implementation details follow.

inline bool WriteLittleEndian16(uint16_t data, Writer& dest) {
  if (ABSL_PREDICT_FALSE(!dest.Push(sizeof(uint16_t)))) return false;
  WriteLittleEndian16(data, dest.cursor());
  dest.move_cursor(sizeof(uint16_t));
  return true;
}

inline bool WriteLittleEndian32(uint32_t data, Writer& dest) {
  if (ABSL_PREDICT_FALSE(!dest.Push(sizeof(uint32_t)))) return false;
  WriteLittleEndian32(data, dest.cursor());
  dest.move_cursor(sizeof(uint32_t));
  return true;
}

inline bool WriteLittleEndian64(uint64_t data, Writer& dest) {
  if (ABSL_PREDICT_FALSE(!dest.Push(sizeof(uint64_t)))) return false;
  WriteLittleEndian64(data, dest.cursor());
  dest.move_cursor(sizeof(uint64_t));
  return true;
}

inline bool WriteLittleEndianSigned16(int16_t data, Writer& dest) {
  return WriteLittleEndian16(static_cast<uint16_t>(data), dest);
}

inline bool WriteLittleEndianSigned32(int32_t data, Writer& dest) {
  return WriteLittleEndian32(static_cast<uint32_t>(data), dest);
}

inline bool WriteLittleEndianSigned64(int64_t data, Writer& dest) {
  return WriteLittleEndian64(static_cast<uint64_t>(data), dest);
}

inline bool WriteLittleEndianFloat(float data, Writer& dest) {
  return WriteLittleEndian32(absl::bit_cast<uint32_t>(data), dest);
}

inline bool WriteLittleEndianDouble(double data, Writer& dest) {
  return WriteLittleEndian64(absl::bit_cast<uint64_t>(data), dest);
}

inline bool WriteBigEndian16(uint16_t data, Writer& dest) {
  if (ABSL_PREDICT_FALSE(!dest.Push(sizeof(uint16_t)))) return false;
  WriteBigEndian16(data, dest.cursor());
  dest.move_cursor(sizeof(uint16_t));
  return true;
}

inline bool WriteBigEndian32(uint32_t data, Writer& dest) {
  if (ABSL_PREDICT_FALSE(!dest.Push(sizeof(uint32_t)))) return false;
  WriteBigEndian32(data, dest.cursor());
  dest.move_cursor(sizeof(uint32_t));
  return true;
}

inline bool WriteBigEndian64(uint64_t data, Writer& dest) {
  if (ABSL_PREDICT_FALSE(!dest.Push(sizeof(uint64_t)))) return false;
  WriteBigEndian64(data, dest.cursor());
  dest.move_cursor(sizeof(uint64_t));
  return true;
}

inline bool WriteBigEndianSigned16(int16_t data, Writer& dest) {
  return WriteBigEndian16(static_cast<uint16_t>(data), dest);
}

inline bool WriteBigEndianSigned32(int32_t data, Writer& dest) {
  return WriteBigEndian32(static_cast<uint32_t>(data), dest);
}

inline bool WriteBigEndianSigned64(int64_t data, Writer& dest) {
  return WriteBigEndian64(static_cast<uint64_t>(data), dest);
}

inline bool WriteBigEndianFloat(float data, Writer& dest) {
  return WriteBigEndian32(absl::bit_cast<uint32_t>(data), dest);
}

inline bool WriteBigEndianDouble(double data, Writer& dest) {
  return WriteBigEndian64(absl::bit_cast<uint64_t>(data), dest);
}

inline bool WriteLittleEndian16(uint16_t data, BackwardWriter& dest) {
  if (ABSL_PREDICT_FALSE(!dest.Push(sizeof(uint16_t)))) return false;
  dest.move_cursor(sizeof(uint16_t));
  WriteLittleEndian16(data, dest.cursor());
  return true;
}
inline bool WriteLittleEndian32(uint32_t data, BackwardWriter& dest) {
  if (ABSL_PREDICT_FALSE(!dest.Push(sizeof(uint32_t)))) return false;
  dest.move_cursor(sizeof(uint32_t));
  WriteLittleEndian32(data, dest.cursor());
  return true;
}

inline bool WriteLittleEndian64(uint64_t data, BackwardWriter& dest) {
  if (ABSL_PREDICT_FALSE(!dest.Push(sizeof(uint64_t)))) return false;
  dest.move_cursor(sizeof(uint64_t));
  WriteLittleEndian64(data, dest.cursor());
  return true;
}

inline bool WriteLittleEndianSigned16(int16_t data, BackwardWriter& dest) {
  return WriteLittleEndian16(static_cast<uint16_t>(data), dest);
}

inline bool WriteLittleEndianSigned32(int32_t data, BackwardWriter& dest) {
  return WriteLittleEndian32(static_cast<uint32_t>(data), dest);
}

inline bool WriteLittleEndianSigned64(int64_t data, BackwardWriter& dest) {
  return WriteLittleEndian64(static_cast<uint64_t>(data), dest);
}

inline bool WriteLittleEndianFloat(float data, BackwardWriter& dest) {
  return WriteLittleEndian32(absl::bit_cast<uint32_t>(data), dest);
}

inline bool WriteLittleEndianDouble(double data, BackwardWriter& dest) {
  return WriteLittleEndian64(absl::bit_cast<uint64_t>(data), dest);
}

inline bool WriteBigEndian16(uint16_t data, BackwardWriter& dest) {
  if (ABSL_PREDICT_FALSE(!dest.Push(sizeof(uint16_t)))) return false;
  dest.move_cursor(sizeof(uint16_t));
  WriteBigEndian16(data, dest.cursor());
  return true;
}

inline bool WriteBigEndian32(uint32_t data, BackwardWriter& dest) {
  if (ABSL_PREDICT_FALSE(!dest.Push(sizeof(uint32_t)))) return false;
  dest.move_cursor(sizeof(uint32_t));
  WriteBigEndian32(data, dest.cursor());
  return true;
}

inline bool WriteBigEndian64(uint64_t data, BackwardWriter& dest) {
  if (ABSL_PREDICT_FALSE(!dest.Push(sizeof(uint64_t)))) return false;
  dest.move_cursor(sizeof(uint64_t));
  WriteBigEndian64(data, dest.cursor());
  return true;
}

inline bool WriteBigEndianSigned16(int16_t data, BackwardWriter& dest) {
  return WriteBigEndian16(static_cast<uint16_t>(data), dest);
}

inline bool WriteBigEndianSigned32(int32_t data, BackwardWriter& dest) {
  return WriteBigEndian32(static_cast<uint32_t>(data), dest);
}

inline bool WriteBigEndianSigned64(int64_t data, BackwardWriter& dest) {
  return WriteBigEndian64(static_cast<uint64_t>(data), dest);
}

inline bool WriteBigEndianFloat(float data, BackwardWriter& dest) {
  return WriteBigEndian32(absl::bit_cast<uint32_t>(data), dest);
}

inline bool WriteBigEndianDouble(double data, BackwardWriter& dest) {
  return WriteBigEndian64(absl::bit_cast<uint64_t>(data), dest);
}

template <>
inline bool WriteLittleEndian<uint8_t>(uint8_t data, Writer& dest) {
  return dest.WriteByte(data);
}
template <>
inline bool WriteLittleEndian<uint16_t>(uint16_t data, Writer& dest) {
  return WriteLittleEndian16(data, dest);
}
template <>
inline bool WriteLittleEndian<uint32_t>(uint32_t data, Writer& dest) {
  return WriteLittleEndian32(data, dest);
}
template <>
inline bool WriteLittleEndian<uint64_t>(uint64_t data, Writer& dest) {
  return WriteLittleEndian64(data, dest);
}
template <>
inline bool WriteLittleEndian<int8_t>(int8_t data, Writer& dest) {
  return dest.WriteByte(static_cast<uint8_t>(data));
}
template <>
inline bool WriteLittleEndian<int16_t>(int16_t data, Writer& dest) {
  return WriteLittleEndianSigned16(data, dest);
}
template <>
inline bool WriteLittleEndian<int32_t>(int32_t data, Writer& dest) {
  return WriteLittleEndianSigned32(data, dest);
}
template <>
inline bool WriteLittleEndian<int64_t>(int64_t data, Writer& dest) {
  return WriteLittleEndianSigned64(data, dest);
}
template <>
inline bool WriteLittleEndian<float>(float data, Writer& dest) {
  return WriteLittleEndianFloat(data, dest);
}
template <>
inline bool WriteLittleEndian<double>(double data, Writer& dest) {
  return WriteLittleEndianDouble(data, dest);
}

template <>
inline bool WriteBigEndian<uint8_t>(uint8_t data, Writer& dest) {
  return dest.WriteByte(data);
}
template <>
inline bool WriteBigEndian<uint16_t>(uint16_t data, Writer& dest) {
  return WriteBigEndian16(data, dest);
}
template <>
inline bool WriteBigEndian<uint32_t>(uint32_t data, Writer& dest) {
  return WriteBigEndian32(data, dest);
}
template <>
inline bool WriteBigEndian<uint64_t>(uint64_t data, Writer& dest) {
  return WriteBigEndian64(data, dest);
}
template <>
inline bool WriteBigEndian<int8_t>(int8_t data, Writer& dest) {
  return dest.WriteByte(static_cast<uint8_t>(data));
}
template <>
inline bool WriteBigEndian<int16_t>(int16_t data, Writer& dest) {
  return WriteBigEndianSigned16(data, dest);
}
template <>
inline bool WriteBigEndian<int32_t>(int32_t data, Writer& dest) {
  return WriteBigEndianSigned32(data, dest);
}
template <>
inline bool WriteBigEndian<int64_t>(int64_t data, Writer& dest) {
  return WriteBigEndianSigned64(data, dest);
}
template <>
inline bool WriteBigEndian<float>(float data, Writer& dest) {
  return WriteBigEndianFloat(data, dest);
}
template <>
inline bool WriteBigEndian<double>(double data, Writer& dest) {
  return WriteBigEndianDouble(data, dest);
}

inline bool WriteLittleEndian16s(absl::Span<const uint16_t> data,
                                 Writer& dest) {
#if ABSL_IS_LITTLE_ENDIAN
  return dest.Write(
      absl::string_view(reinterpret_cast<const char*>(data.data()),
                        data.size() * sizeof(uint16_t)));
#else
  for (const uint16_t value : data) {
    if (ABSL_PREDICT_FALSE(!WriteLittleEndian16(value, dest))) return false;
  }
  return true;
#endif
}

inline bool WriteLittleEndian32s(absl::Span<const uint32_t> data,
                                 Writer& dest) {
#if ABSL_IS_LITTLE_ENDIAN
  return dest.Write(
      absl::string_view(reinterpret_cast<const char*>(data.data()),
                        data.size() * sizeof(uint32_t)));
#else
  for (const uint32_t value : data) {
    if (ABSL_PREDICT_FALSE(!WriteLittleEndian32(value, dest))) return false;
  }
  return true;
#endif
}

inline bool WriteLittleEndian64s(absl::Span<const uint64_t> data,
                                 Writer& dest) {
#if ABSL_IS_LITTLE_ENDIAN
  return dest.Write(
      absl::string_view(reinterpret_cast<const char*>(data.data()),
                        data.size() * sizeof(uint64_t)));
#else
  for (const uint64_t value : data) {
    if (ABSL_PREDICT_FALSE(!WriteLittleEndian64(value, dest))) return false;
  }
  return true;
#endif
}

inline bool WriteLittleEndianSigned16s(absl::Span<const int16_t> data,
                                       Writer& dest) {
#if ABSL_IS_LITTLE_ENDIAN
  return dest.Write(
      absl::string_view(reinterpret_cast<const char*>(data.data()),
                        data.size() * sizeof(int16_t)));
#else
  for (const int16_t value : data) {
    if (ABSL_PREDICT_FALSE(!WriteLittleEndianSigned16(value, dest))) {
      return false;
    }
  }
  return true;
#endif
}

inline bool WriteLittleEndianSigned32s(absl::Span<const int32_t> data,
                                       Writer& dest) {
#if ABSL_IS_LITTLE_ENDIAN
  return dest.Write(
      absl::string_view(reinterpret_cast<const char*>(data.data()),
                        data.size() * sizeof(int32_t)));
#else
  for (const int32_t value : data) {
    if (ABSL_PREDICT_FALSE(!WriteLittleEndianSigned32(value, dest))) {
      return false;
    }
  }
  return true;
#endif
}

inline bool WriteLittleEndianSigned64s(absl::Span<const int64_t> data,
                                       Writer& dest) {
#if ABSL_IS_LITTLE_ENDIAN
  return dest.Write(
      absl::string_view(reinterpret_cast<const char*>(data.data()),
                        data.size() * sizeof(int64_t)));
#else
  for (const int64_t value : data) {
    if (ABSL_PREDICT_FALSE(!WriteLittleEndianSigned64(value, dest))) {
      return false;
    }
  }
  return true;
#endif
}

inline bool WriteLittleEndianFloats(absl::Span<const float> data,
                                    Writer& dest) {
#if ABSL_IS_LITTLE_ENDIAN
  return dest.Write(absl::string_view(
      reinterpret_cast<const char*>(data.data()), data.size() * sizeof(float)));
#else
  for (const float value : data) {
    if (ABSL_PREDICT_FALSE(!WriteLittleEndianFloat(value, dest))) return false;
  }
  return true;
#endif
}

inline bool WriteLittleEndianDoubles(absl::Span<const double> data,
                                     Writer& dest) {
#if ABSL_IS_LITTLE_ENDIAN
  return dest.Write(
      absl::string_view(reinterpret_cast<const char*>(data.data()),
                        data.size() * sizeof(double)));
#else
  for (const double value : data) {
    if (ABSL_PREDICT_FALSE(!WriteLittleEndianDouble(value, dest))) return false;
  }
  return true;
#endif
}

inline bool WriteBigEndian16s(absl::Span<const uint16_t> data, Writer& dest) {
#if ABSL_IS_BIG_ENDIAN
  return dest.Write(
      absl::string_view(reinterpret_cast<const char*>(data.data()),
                        data.size() * sizeof(uint16_t)));
#else
  for (const uint16_t value : data) {
    if (ABSL_PREDICT_FALSE(!WriteBigEndian16(value, dest))) return false;
  }
  return true;
#endif
}

inline bool WriteBigEndian32s(absl::Span<const uint32_t> data, Writer& dest) {
#if ABSL_IS_BIG_ENDIAN
  return dest.Write(
      absl::string_view(reinterpret_cast<const char*>(data.data()),
                        data.size() * sizeof(uint32_t)));
#else
  for (const uint32_t value : data) {
    if (ABSL_PREDICT_FALSE(!WriteBigEndian32(value, dest))) return false;
  }
  return true;
#endif
}

inline bool WriteBigEndian64s(absl::Span<const uint64_t> data, Writer& dest) {
#if ABSL_IS_BIG_ENDIAN
  return dest.Write(
      absl::string_view(reinterpret_cast<const char*>(data.data()),
                        data.size() * sizeof(uint64_t)));
#else
  for (const uint64_t value : data) {
    if (ABSL_PREDICT_FALSE(!WriteBigEndian64(value, dest))) return false;
  }
  return true;
#endif
}

inline bool WriteBigEndianSigned16s(absl::Span<const int16_t> data,
                                    Writer& dest) {
#if ABSL_IS_BIG_ENDIAN
  return dest.Write(
      absl::string_view(reinterpret_cast<const char*>(data.data()),
                        data.size() * sizeof(int16_t)));
#else
  for (const int16_t value : data) {
    if (ABSL_PREDICT_FALSE(!WriteBigEndianSigned16(value, dest))) return false;
  }
  return true;
#endif
}

inline bool WriteBigEndianSigned32s(absl::Span<const int32_t> data,
                                    Writer& dest) {
#if ABSL_IS_BIG_ENDIAN
  return dest.Write(
      absl::string_view(reinterpret_cast<const char*>(data.data()),
                        data.size() * sizeof(int32_t)));
#else
  for (const int32_t value : data) {
    if (ABSL_PREDICT_FALSE(!WriteBigEndianSigned32(value, dest))) return false;
  }
  return true;
#endif
}

inline bool WriteBigEndianSigned64s(absl::Span<const int64_t> data,
                                    Writer& dest) {
#if ABSL_IS_BIG_ENDIAN
  return dest.Write(
      absl::string_view(reinterpret_cast<const char*>(data.data()),
                        data.size() * sizeof(int64_t)));
#else
  for (const int64_t value : data) {
    if (ABSL_PREDICT_FALSE(!WriteBigEndianSigned64(value, dest))) return false;
  }
  return true;
#endif
}

inline bool WriteBigEndianFloats(absl::Span<const float> data, Writer& dest) {
#if ABSL_IS_BIG_ENDIAN
  return dest.Write(absl::string_view(
      reinterpret_cast<const char*>(data.data()), data.size() * sizeof(float)));
#else
  for (const float value : data) {
    if (ABSL_PREDICT_FALSE(!WriteBigEndianFloat(value, dest))) return false;
  }
  return true;
#endif
}

inline bool WriteBigEndianDoubles(absl::Span<const double> data, Writer& dest) {
#if ABSL_IS_BIG_ENDIAN
  return dest.Write(
      absl::string_view(reinterpret_cast<const char*>(data.data()),
                        data.size() * sizeof(double)));
#else
  for (const double value : data) {
    if (ABSL_PREDICT_FALSE(!WriteBigEndianDouble(value, dest))) return false;
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
inline bool WriteLittleEndians<uint16_t>(absl::Span<const uint16_t> data,
                                         Writer& dest) {
  return WriteLittleEndian16s(data, dest);
}
template <>
inline bool WriteLittleEndians<uint32_t>(absl::Span<const uint32_t> data,
                                         Writer& dest) {
  return WriteLittleEndian32s(data, dest);
}
template <>
inline bool WriteLittleEndians<uint64_t>(absl::Span<const uint64_t> data,
                                         Writer& dest) {
  return WriteLittleEndian64s(data, dest);
}
template <>
inline bool WriteLittleEndians<int8_t>(absl::Span<const int8_t> data,
                                       Writer& dest) {
  return dest.Write(absl::string_view(
      reinterpret_cast<const char*>(data.data()), data.size()));
}
template <>
inline bool WriteLittleEndians<int16_t>(absl::Span<const int16_t> data,
                                        Writer& dest) {
  return WriteLittleEndianSigned16s(data, dest);
}
template <>
inline bool WriteLittleEndians<int32_t>(absl::Span<const int32_t> data,
                                        Writer& dest) {
  return WriteLittleEndianSigned32s(data, dest);
}
template <>
inline bool WriteLittleEndians<int64_t>(absl::Span<const int64_t> data,
                                        Writer& dest) {
  return WriteLittleEndianSigned64s(data, dest);
}
template <>
inline bool WriteLittleEndians<float>(absl::Span<const float> data,
                                      Writer& dest) {
  return WriteLittleEndianFloats(data, dest);
}
template <>
inline bool WriteLittleEndians<double>(absl::Span<const double> data,
                                       Writer& dest) {
  return WriteLittleEndianDoubles(data, dest);
}

template <>
inline bool WriteBigEndians<uint8_t>(absl::Span<const uint8_t> data,
                                     Writer& dest) {
  return dest.Write(absl::string_view(
      reinterpret_cast<const char*>(data.data()), data.size()));
}
template <>
inline bool WriteBigEndians<uint16_t>(absl::Span<const uint16_t> data,
                                      Writer& dest) {
  return WriteBigEndian16s(data, dest);
}
template <>
inline bool WriteBigEndians<uint32_t>(absl::Span<const uint32_t> data,
                                      Writer& dest) {
  return WriteBigEndian32s(data, dest);
}
template <>
inline bool WriteBigEndians<uint64_t>(absl::Span<const uint64_t> data,
                                      Writer& dest) {
  return WriteBigEndian64s(data, dest);
}
template <>
inline bool WriteBigEndians<int8_t>(absl::Span<const int8_t> data,
                                    Writer& dest) {
  return dest.Write(absl::string_view(
      reinterpret_cast<const char*>(data.data()), data.size()));
}
template <>
inline bool WriteBigEndians<int16_t>(absl::Span<const int16_t> data,
                                     Writer& dest) {
  return WriteBigEndianSigned16s(data, dest);
}
template <>
inline bool WriteBigEndians<int32_t>(absl::Span<const int32_t> data,
                                     Writer& dest) {
  return WriteBigEndianSigned32s(data, dest);
}
template <>
inline bool WriteBigEndians<int64_t>(absl::Span<const int64_t> data,
                                     Writer& dest) {
  return WriteBigEndianSigned64s(data, dest);
}
template <>
inline bool WriteBigEndians<float>(absl::Span<const float> data, Writer& dest) {
  return WriteBigEndianFloats(data, dest);
}
template <>
inline bool WriteBigEndians<double>(absl::Span<const double> data,
                                    Writer& dest) {
  return WriteBigEndianDoubles(data, dest);
}

namespace endian_internal {

// If these functions are manually inlined into their callers, clang generates
// poor code (with byte shifting even if the endianness matches the native one).

inline uint16_t EncodeLittleEndian16(uint16_t data) {
  uint16_t encoded;
  unsigned char* const ptr = reinterpret_cast<unsigned char*>(&encoded);
  ptr[0] = static_cast<unsigned char>(data);
  ptr[1] = static_cast<unsigned char>(data >> 8);
  return encoded;
}

inline uint32_t EncodeLittleEndian32(uint32_t data) {
  uint32_t encoded;
  unsigned char* const ptr = reinterpret_cast<unsigned char*>(&encoded);
  ptr[0] = static_cast<unsigned char>(data);
  ptr[1] = static_cast<unsigned char>(data >> 8);
  ptr[2] = static_cast<unsigned char>(data >> (2 * 8));
  ptr[3] = static_cast<unsigned char>(data >> (3 * 8));
  return encoded;
}

inline uint64_t EncodeLittleEndian64(uint64_t data) {
  uint64_t encoded;
  unsigned char* const ptr = reinterpret_cast<unsigned char*>(&encoded);
  ptr[0] = static_cast<unsigned char>(data);
  ptr[1] = static_cast<unsigned char>(data >> 8);
  ptr[2] = static_cast<unsigned char>(data >> (2 * 8));
  ptr[3] = static_cast<unsigned char>(data >> (3 * 8));
  ptr[4] = static_cast<unsigned char>(data >> (4 * 8));
  ptr[5] = static_cast<unsigned char>(data >> (5 * 8));
  ptr[6] = static_cast<unsigned char>(data >> (6 * 8));
  ptr[7] = static_cast<unsigned char>(data >> (7 * 8));
  return encoded;
}

inline uint16_t EncodeBigEndian16(uint16_t data) {
  uint16_t encoded;
  unsigned char* const ptr = reinterpret_cast<unsigned char*>(&encoded);
  ptr[0] = static_cast<unsigned char>(data >> 8);
  ptr[1] = static_cast<unsigned char>(data);
  return encoded;
}

inline uint32_t EncodeBigEndian32(uint32_t data) {
  uint32_t encoded;
  unsigned char* const ptr = reinterpret_cast<unsigned char*>(&encoded);
  ptr[0] = static_cast<unsigned char>(data >> (3 * 8));
  ptr[1] = static_cast<unsigned char>(data >> (2 * 8));
  ptr[2] = static_cast<unsigned char>(data >> 8);
  ptr[3] = static_cast<unsigned char>(data);
  return encoded;
}

inline uint64_t EncodeBigEndian64(uint64_t data) {
  uint64_t encoded;
  unsigned char* const ptr = reinterpret_cast<unsigned char*>(&encoded);
  ptr[0] = static_cast<unsigned char>(data >> (7 * 8));
  ptr[1] = static_cast<unsigned char>(data >> (6 * 8));
  ptr[2] = static_cast<unsigned char>(data >> (5 * 8));
  ptr[3] = static_cast<unsigned char>(data >> (4 * 8));
  ptr[4] = static_cast<unsigned char>(data >> (3 * 8));
  ptr[5] = static_cast<unsigned char>(data >> (2 * 8));
  ptr[6] = static_cast<unsigned char>(data >> 8);
  ptr[7] = static_cast<unsigned char>(data);
  return encoded;
}

}  // namespace endian_internal

inline void WriteLittleEndian16(uint16_t data, char* dest) {
  const uint16_t encoded = endian_internal::EncodeLittleEndian16(data);
  std::memcpy(dest, &encoded, sizeof(uint16_t));
}

inline void WriteLittleEndian32(uint32_t data, char* dest) {
  const uint32_t encoded = endian_internal::EncodeLittleEndian32(data);
  std::memcpy(dest, &encoded, sizeof(uint32_t));
}

inline void WriteLittleEndian64(uint64_t data, char* dest) {
  const uint64_t encoded = endian_internal::EncodeLittleEndian64(data);
  std::memcpy(dest, &encoded, sizeof(uint64_t));
}

inline void WriteLittleEndianSigned16(int16_t data, char* dest) {
  return WriteLittleEndian16(static_cast<uint16_t>(data), dest);
}

inline void WriteLittleEndianSigned32(int32_t data, char* dest) {
  return WriteLittleEndian32(static_cast<uint32_t>(data), dest);
}

inline void WriteLittleEndianSigned64(int64_t data, char* dest) {
  return WriteLittleEndian64(static_cast<uint64_t>(data), dest);
}

inline void WriteLittleEndianFloat(float data, char* dest) {
  return WriteLittleEndian32(absl::bit_cast<uint32_t>(data), dest);
}

inline void WriteLittleEndianDouble(double data, char* dest) {
  return WriteLittleEndian64(absl::bit_cast<uint64_t>(data), dest);
}

inline void WriteBigEndian16(uint16_t data, char* dest) {
  const uint16_t encoded = endian_internal::EncodeBigEndian16(data);
  std::memcpy(dest, &encoded, sizeof(uint16_t));
}

inline void WriteBigEndian32(uint32_t data, char* dest) {
  const uint32_t encoded = endian_internal::EncodeBigEndian32(data);
  std::memcpy(dest, &encoded, sizeof(uint32_t));
}

inline void WriteBigEndian64(uint64_t data, char* dest) {
  const uint64_t encoded = endian_internal::EncodeBigEndian64(data);
  std::memcpy(dest, &encoded, sizeof(uint64_t));
}

inline void WriteBigEndianSigned16(int16_t data, char* dest) {
  return WriteBigEndian16(static_cast<uint16_t>(data), dest);
}

inline void WriteBigEndianSigned32(int32_t data, char* dest) {
  return WriteBigEndian32(static_cast<uint32_t>(data), dest);
}

inline void WriteBigEndianSigned64(int64_t data, char* dest) {
  return WriteBigEndian64(static_cast<uint64_t>(data), dest);
}

inline void WriteBigEndianFloat(float data, char* dest) {
  return WriteBigEndian32(absl::bit_cast<uint32_t>(data), dest);
}

inline void WriteBigEndianDouble(double data, char* dest) {
  return WriteBigEndian64(absl::bit_cast<uint64_t>(data), dest);
}

template <>
inline void WriteLittleEndian<uint8_t>(uint8_t data, char* dest) {
  *dest = static_cast<char>(data);
}
template <>
inline void WriteLittleEndian<uint16_t>(uint16_t data, char* dest) {
  WriteLittleEndian16(data, dest);
}
template <>
inline void WriteLittleEndian<uint32_t>(uint32_t data, char* dest) {
  WriteLittleEndian32(data, dest);
}
template <>
inline void WriteLittleEndian<uint64_t>(uint64_t data, char* dest) {
  WriteLittleEndian64(data, dest);
}
template <>
inline void WriteLittleEndian<int8_t>(int8_t data, char* dest) {
  *dest = static_cast<char>(data);
}
template <>
inline void WriteLittleEndian<int16_t>(int16_t data, char* dest) {
  WriteLittleEndianSigned16(data, dest);
}
template <>
inline void WriteLittleEndian<int32_t>(int32_t data, char* dest) {
  WriteLittleEndianSigned32(data, dest);
}
template <>
inline void WriteLittleEndian<int64_t>(int64_t data, char* dest) {
  WriteLittleEndianSigned64(data, dest);
}
template <>
inline void WriteLittleEndian<float>(float data, char* dest) {
  WriteLittleEndianFloat(data, dest);
}
template <>
inline void WriteLittleEndian<double>(double data, char* dest) {
  WriteLittleEndianDouble(data, dest);
}

template <>
inline void WriteBigEndian<uint8_t>(uint8_t data, char* dest) {
  *dest = static_cast<char>(data);
}
template <>
inline void WriteBigEndian<uint16_t>(uint16_t data, char* dest) {
  WriteBigEndian16(data, dest);
}
template <>
inline void WriteBigEndian<uint32_t>(uint32_t data, char* dest) {
  WriteBigEndian32(data, dest);
}
template <>
inline void WriteBigEndian<uint64_t>(uint64_t data, char* dest) {
  WriteBigEndian64(data, dest);
}
template <>
inline void WriteBigEndian<int8_t>(int8_t data, char* dest) {
  *dest = static_cast<char>(data);
}
template <>
inline void WriteBigEndian<int16_t>(int16_t data, char* dest) {
  WriteBigEndianSigned16(data, dest);
}
template <>
inline void WriteBigEndian<int32_t>(int32_t data, char* dest) {
  WriteBigEndianSigned32(data, dest);
}
template <>
inline void WriteBigEndian<int64_t>(int64_t data, char* dest) {
  WriteBigEndianSigned64(data, dest);
}
template <>
inline void WriteBigEndian<float>(float data, char* dest) {
  WriteBigEndianFloat(data, dest);
}
template <>
inline void WriteBigEndian<double>(double data, char* dest) {
  WriteBigEndianDouble(data, dest);
}

inline void WriteLittleEndian16s(absl::Span<const uint16_t> data, char* dest) {
#if ABSL_IS_LITTLE_ENDIAN
  if (ABSL_PREDICT_TRUE(
          // `std::memcpy(nullptr, _, 0)` and `std::memcpy(_, nullptr, 0)` are
          // undefined.
          !data.empty())) {
    std::memcpy(dest, data.data(), data.size() * sizeof(uint16_t));
  }
#else
  for (const uint16_t value : data) {
    WriteLittleEndian16(value, dest);
    dest += sizeof(uint16_t);
  }
#endif
}

inline void WriteLittleEndian32s(absl::Span<const uint32_t> data, char* dest) {
#if ABSL_IS_LITTLE_ENDIAN
  if (ABSL_PREDICT_TRUE(
          // `std::memcpy(nullptr, _, 0)` and `std::memcpy(_, nullptr, 0)` are
          // undefined.
          !data.empty())) {
    std::memcpy(dest, data.data(), data.size() * sizeof(uint32_t));
  }
#else
  for (const uint32_t value : data) {
    WriteLittleEndian32(value, dest);
    dest += sizeof(uint32_t);
  }
#endif
}

inline void WriteLittleEndian64s(absl::Span<const uint64_t> data, char* dest) {
#if ABSL_IS_LITTLE_ENDIAN
  if (ABSL_PREDICT_TRUE(
          // `std::memcpy(nullptr, _, 0)` and `std::memcpy(_, nullptr, 0)` are
          // undefined.
          !data.empty())) {
    std::memcpy(dest, data.data(), data.size() * sizeof(uint64_t));
  }
#else
  for (const uint64_t value : data) {
    WriteLittleEndian64(value, dest);
    dest += sizeof(uint64_t);
  }
#endif
}

inline void WriteLittleEndianSigned16s(absl::Span<const int16_t> data,
                                       char* dest) {
#if ABSL_IS_LITTLE_ENDIAN
  if (ABSL_PREDICT_TRUE(
          // `std::memcpy(nullptr, _, 0)` and `std::memcpy(_, nullptr, 0)` are
          // undefined.
          !data.empty())) {
    std::memcpy(dest, data.data(), data.size() * sizeof(int16_t));
  }
#else
  for (const int16_t value : data) {
    WriteLittleEndianSigned16(value, dest);
    dest += sizeof(int16_t);
  }
#endif
}

inline void WriteLittleEndianSigned32s(absl::Span<const int32_t> data,
                                       char* dest) {
#if ABSL_IS_LITTLE_ENDIAN
  if (ABSL_PREDICT_TRUE(
          // `std::memcpy(nullptr, _, 0)` and `std::memcpy(_, nullptr, 0)` are
          // undefined.
          !data.empty())) {
    std::memcpy(dest, data.data(), data.size() * sizeof(int32_t));
  }
#else
  for (const int32_t value : data) {
    WriteLittleEndianSigned32(value, dest);
    dest += sizeof(int32_t);
  }
#endif
}

inline void WriteLittleEndianSigned64s(absl::Span<const int64_t> data,
                                       char* dest) {
#if ABSL_IS_LITTLE_ENDIAN
  if (ABSL_PREDICT_TRUE(
          // `std::memcpy(nullptr, _, 0)` and `std::memcpy(_, nullptr, 0)` are
          // undefined.
          !data.empty())) {
    std::memcpy(dest, data.data(), data.size() * sizeof(int64_t));
  }
#else
  for (const int64_t value : data) {
    WriteLittleEndianSigned64(value, dest);
    dest += sizeof(int64_t);
  }
#endif
}

inline void WriteLittleEndianFloats(absl::Span<const float> data, char* dest) {
#if ABSL_IS_LITTLE_ENDIAN
  if (ABSL_PREDICT_TRUE(
          // `std::memcpy(nullptr, _, 0)` and `std::memcpy(_, nullptr, 0)` are
          // undefined.
          !data.empty())) {
    std::memcpy(dest, data.data(), data.size() * sizeof(float));
  }
#else
  for (const float value : data) {
    WriteLittleEndianFloat(value, dest);
    dest += sizeof(float);
  }
#endif
}

inline void WriteLittleEndianDoubles(absl::Span<const double> data,
                                     char* dest) {
#if ABSL_IS_LITTLE_ENDIAN
  if (ABSL_PREDICT_TRUE(
          // `std::memcpy(nullptr, _, 0)` and `std::memcpy(_, nullptr, 0)` are
          // undefined.
          !data.empty())) {
    std::memcpy(dest, data.data(), data.size() * sizeof(double));
  }
#else
  for (const double value : data) {
    WriteLittleEndianDouble(value, dest);
    dest += sizeof(double);
  }
#endif
}

inline void WriteBigEndian16s(absl::Span<const uint16_t> data, char* dest) {
#if ABSL_IS_BIG_ENDIAN
  if (ABSL_PREDICT_TRUE(
          // `std::memcpy(nullptr, _, 0)` and `std::memcpy(_, nullptr, 0)` are
          // undefined.
          !data.empty())) {
    std::memcpy(dest, data.data(), data.size() * sizeof(uint16_t));
  }
#else
  for (const uint16_t value : data) {
    WriteBigEndian16(value, dest);
    dest += sizeof(uint16_t);
  }
#endif
}

inline void WriteBigEndian32s(absl::Span<const uint32_t> data, char* dest) {
#if ABSL_IS_BIG_ENDIAN
  if (ABSL_PREDICT_TRUE(
          // `std::memcpy(nullptr, _, 0)` and `std::memcpy(_, nullptr, 0)` are
          // undefined.
          !data.empty())) {
    std::memcpy(dest, data.data(), data.size() * sizeof(uint32_t));
  }
#else
  for (const uint32_t value : data) {
    WriteBigEndian32(value, dest);
    dest += sizeof(uint32_t);
  }
#endif
}

inline void WriteBigEndian64s(absl::Span<const uint64_t> data, char* dest) {
#if ABSL_IS_BIG_ENDIAN
  if (ABSL_PREDICT_TRUE(
          // `std::memcpy(nullptr, _, 0)` and `std::memcpy(_, nullptr, 0)` are
          // undefined.
          !data.empty())) {
    std::memcpy(dest, data.data(), data.size() * sizeof(uint64_t));
  }
#else
  for (const uint64_t value : data) {
    WriteBigEndian64(value, dest);
    dest += sizeof(uint64_t);
  }
#endif
}

inline void WriteBigEndianSigned16s(absl::Span<const int16_t> data,
                                    char* dest) {
#if ABSL_IS_BIG_ENDIAN
  if (ABSL_PREDICT_TRUE(
          // `std::memcpy(nullptr, _, 0)` and `std::memcpy(_, nullptr, 0)` are
          // undefined.
          !data.empty())) {
    std::memcpy(dest, data.data(), data.size() * sizeof(int16_t));
  }
#else
  for (const int16_t value : data) {
    WriteBigEndianSigned16(value, dest);
    dest += sizeof(int16_t);
  }
#endif
}

inline void WriteBigEndianSigned32s(absl::Span<const int32_t> data,
                                    char* dest) {
#if ABSL_IS_BIG_ENDIAN
  if (ABSL_PREDICT_TRUE(
          // `std::memcpy(nullptr, _, 0)` and `std::memcpy(_, nullptr, 0)` are
          // undefined.
          !data.empty())) {
    std::memcpy(dest, data.data(), data.size() * sizeof(int32_t));
  }
#else
  for (const int32_t value : data) {
    WriteBigEndianSigned32(value, dest);
    dest += sizeof(int32_t);
  }
#endif
}

inline void WriteBigEndianSigned64s(absl::Span<const int64_t> data,
                                    char* dest) {
#if ABSL_IS_BIG_ENDIAN
  if (ABSL_PREDICT_TRUE(
          // `std::memcpy(nullptr, _, 0)` and `std::memcpy(_, nullptr, 0)` are
          // undefined.
          !data.empty())) {
    std::memcpy(dest, data.data(), data.size() * sizeof(int64_t));
  }
#else
  for (const int64_t value : data) {
    WriteBigEndianSigned64(value, dest);
    dest += sizeof(int64_t);
  }
#endif
}

inline void WriteBigEndianFloats(absl::Span<const float> data, char* dest) {
#if ABSL_IS_BIG_ENDIAN
  if (ABSL_PREDICT_TRUE(
          // `std::memcpy(nullptr, _, 0)` and `std::memcpy(_, nullptr, 0)` are
          // undefined.
          !data.empty())) {
    std::memcpy(dest, data.data(), data.size() * sizeof(float));
  }
#else
  for (const float value : data) {
    WriteBigEndianFloat(value, dest);
    dest += sizeof(float);
  }
#endif
}

inline void WriteBigEndianDoubles(absl::Span<const double> data, char* dest) {
#if ABSL_IS_BIG_ENDIAN
  if (ABSL_PREDICT_TRUE(
          // `std::memcpy(nullptr, _, 0)` and `std::memcpy(_, nullptr, 0)` are
          // undefined.
          !data.empty())) {
    std::memcpy(dest, data.data(), data.size() * sizeof(double));
  }
#else
  for (const double value : data) {
    WriteBigEndianDouble(value, dest);
    dest += sizeof(double);
  }
#endif
}

template <>
inline void WriteLittleEndians<uint8_t>(absl::Span<const uint8_t> data,
                                        char* dest) {
  if (ABSL_PREDICT_TRUE(
          // `std::memcpy(nullptr, _, 0)` and `std::memcpy(_, nullptr, 0)` are
          // undefined.
          !data.empty())) {
    std::memcpy(dest, data.data(), data.size());
  }
}
template <>
inline void WriteLittleEndians<uint16_t>(absl::Span<const uint16_t> data,
                                         char* dest) {
  WriteLittleEndian16s(data, dest);
}
template <>
inline void WriteLittleEndians<uint32_t>(absl::Span<const uint32_t> data,
                                         char* dest) {
  WriteLittleEndian32s(data, dest);
}
template <>
inline void WriteLittleEndians<uint64_t>(absl::Span<const uint64_t> data,
                                         char* dest) {
  WriteLittleEndian64s(data, dest);
}
template <>
inline void WriteLittleEndians<int8_t>(absl::Span<const int8_t> data,
                                       char* dest) {
  if (ABSL_PREDICT_TRUE(
          // `std::memcpy(nullptr, _, 0)` and `std::memcpy(_, nullptr, 0)` are
          // undefined.
          !data.empty())) {
    std::memcpy(dest, data.data(), data.size());
  }
}
template <>
inline void WriteLittleEndians<int16_t>(absl::Span<const int16_t> data,
                                        char* dest) {
  WriteLittleEndianSigned16s(data, dest);
}
template <>
inline void WriteLittleEndians<int32_t>(absl::Span<const int32_t> data,
                                        char* dest) {
  WriteLittleEndianSigned32s(data, dest);
}
template <>
inline void WriteLittleEndians<int64_t>(absl::Span<const int64_t> data,
                                        char* dest) {
  WriteLittleEndianSigned64s(data, dest);
}
template <>
inline void WriteLittleEndians<float>(absl::Span<const float> data,
                                      char* dest) {
  WriteLittleEndianFloats(data, dest);
}
template <>
inline void WriteLittleEndians<double>(absl::Span<const double> data,
                                       char* dest) {
  WriteLittleEndianDoubles(data, dest);
}

template <>
inline void WriteBigEndians<uint8_t>(absl::Span<const uint8_t> data,
                                     char* dest) {
  if (ABSL_PREDICT_TRUE(
          // `std::memcpy(nullptr, _, 0)` and `std::memcpy(_, nullptr, 0)` are
          // undefined.
          !data.empty())) {
    std::memcpy(dest, data.data(), data.size());
  }
}
template <>
inline void WriteBigEndians<uint16_t>(absl::Span<const uint16_t> data,
                                      char* dest) {
  WriteBigEndian16s(data, dest);
}
template <>
inline void WriteBigEndians<uint32_t>(absl::Span<const uint32_t> data,
                                      char* dest) {
  WriteBigEndian32s(data, dest);
}
template <>
inline void WriteBigEndians<uint64_t>(absl::Span<const uint64_t> data,
                                      char* dest) {
  WriteBigEndian64s(data, dest);
}
template <>
inline void WriteBigEndians<int8_t>(absl::Span<const int8_t> data, char* dest) {
  if (ABSL_PREDICT_TRUE(
          // `std::memcpy(nullptr, _, 0)` and `std::memcpy(_, nullptr, 0)` are
          // undefined.
          !data.empty())) {
    std::memcpy(dest, data.data(), data.size());
  }
}
template <>
inline void WriteBigEndians<int16_t>(absl::Span<const int16_t> data,
                                     char* dest) {
  WriteBigEndianSigned16s(data, dest);
}
template <>
inline void WriteBigEndians<int32_t>(absl::Span<const int32_t> data,
                                     char* dest) {
  WriteBigEndianSigned32s(data, dest);
}
template <>
inline void WriteBigEndians<int64_t>(absl::Span<const int64_t> data,
                                     char* dest) {
  WriteBigEndianSigned64s(data, dest);
}
template <>
inline void WriteBigEndians<float>(absl::Span<const float> data, char* dest) {
  WriteBigEndianFloats(data, dest);
}
template <>
inline void WriteBigEndians<double>(absl::Span<const double> data, char* dest) {
  WriteBigEndianDoubles(data, dest);
}

}  // namespace riegeli

#endif  // RIEGELI_ENDIAN_ENDIAN_WRITING_H_
