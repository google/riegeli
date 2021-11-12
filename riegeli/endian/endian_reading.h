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

#include "absl/base/optimization.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/endian/endian_internal.h"

namespace riegeli {

// Reads a number in a fixed width Little/Big Endian encoding.
//
// Return values:
//  * `true`                          - success (`dest` is set)
//  * `false` (when `src.healthy()`)  - source ends
//                                      (`src` position is unchanged,
//                                      `dest` is undefined)
//  * `false` (when `!src.healthy()`) - failure
//                                      (`src` position is unchanged,
//                                      `dest` is undefined)
bool ReadLittleEndian16(Reader& src, uint16_t& dest);
bool ReadLittleEndian32(Reader& src, uint32_t& dest);
bool ReadLittleEndian64(Reader& src, uint64_t& dest);
bool ReadLittleEndianSigned16(Reader& src, int16_t& dest);
bool ReadLittleEndianSigned32(Reader& src, int32_t& dest);
bool ReadLittleEndianSigned64(Reader& src, int64_t& dest);
bool ReadBigEndian16(Reader& src, uint16_t& dest);
bool ReadBigEndian32(Reader& src, uint32_t& dest);
bool ReadBigEndian64(Reader& src, uint64_t& dest);
bool ReadBigEndianSigned16(Reader& src, int16_t& dest);
bool ReadBigEndianSigned32(Reader& src, int32_t& dest);
bool ReadBigEndianSigned64(Reader& src, int64_t& dest);

// Reads a number in a fixed width Little/Big Endian encoding. The width of
// the encoding is determined by the template argument, which must be one of:
// `{u,}int{8,16,32,64}_t`.
//
// Return values:
//  * `true`                          - success (`dest` is set)
//  * `false` (when `src.healthy()`)  - source ends
//                                      (`src` position is unchanged,
//                                      `dest` is undefined)
//  * `false` (when `!src.healthy()`) - failure
//                                      (`src` position is unchanged,
//                                      `dest` is undefined)
template <typename T>
bool ReadLittleEndian(Reader& src, internal::type_identity_t<T>& dest);
template <typename T>
bool ReadBigEndian(Reader& src, internal::type_identity_t<T>& dest);

// Reads an array of numbers in a fixed width Little/Big Endian encoding.
//
// This is faster than reading them individually for native endianness.
//
// Return values:
//  * `true`                          - success (`dest[]` is filled)
//  * `false` (when `src.healthy()`)  - source ends
//                                      (`src` position is undefined,
//                                      `dest[]` is undefined)
//  * `false` (when `!src.healthy()`) - failure
//                                      (`src` position is undefined,
//                                      `dest[]` is undefined)
bool ReadLittleEndian16s(Reader& src, absl::Span<uint16_t> dest);
bool ReadLittleEndian32s(Reader& src, absl::Span<uint32_t> dest);
bool ReadLittleEndian64s(Reader& src, absl::Span<uint64_t> dest);
bool ReadLittleEndianSigned16s(Reader& src, absl::Span<int16_t> dest);
bool ReadLittleEndianSigned32s(Reader& src, absl::Span<int32_t> dest);
bool ReadLittleEndianSigned64s(Reader& src, absl::Span<int64_t> dest);
bool ReadBigEndian16s(Reader& src, absl::Span<uint16_t> dest);
bool ReadBigEndian32s(Reader& src, absl::Span<uint32_t> dest);
bool ReadBigEndian64s(Reader& src, absl::Span<uint64_t> dest);
bool ReadBigEndianSigned16s(Reader& src, absl::Span<int16_t> dest);
bool ReadBigEndianSigned32s(Reader& src, absl::Span<int32_t> dest);
bool ReadBigEndianSigned64s(Reader& src, absl::Span<int64_t> dest);

// Reads an array of numbers in a fixed width Little/Big Endian encoding.
// The width of the encoding is determined by the template argument, which
// must be one of: `{u,}int{8,16,32,64}_t`.
//
// This is faster than reading them individually for native endianness.
//
// Return values:
//  * `true`                          - success (`dest[]` is filled)
//  * `false` (when `src.healthy()`)  - source ends
//                                      (`src` position is undefined,
//                                      `dest[]` is undefined)
//  * `false` (when `!src.healthy()`) - failure
//                                      (`src` position is undefined,
//                                      `dest[]` is undefined)
template <typename T>
bool ReadLittleEndians(Reader& src,
                       absl::Span<internal::type_identity_t<T>> dest);
template <typename T>
bool ReadBigEndians(Reader& src, absl::Span<internal::type_identity_t<T>> dest);

// Reads a number in a fixed width Little/Big Endian encoding from an array.
//
// Reads `sizeof(uint{16,32,64}_t)` bytes  from `src[]`.
uint16_t ReadLittleEndian16(const char* src);
uint32_t ReadLittleEndian32(const char* src);
uint64_t ReadLittleEndian64(const char* src);
uint16_t ReadBigEndian16(const char* src);
uint32_t ReadBigEndian32(const char* src);
uint64_t ReadBigEndian64(const char* src);

// Reads an array of numbers in a fixed width Little/Big Endian encoding from an
// array.
//
// This is faster than reading them individually for native endianness.
//
// Reads `dest.size() * sizeof(uint{16,32,64}_t)` bytes  from `src[]`.
void ReadLittleEndian16s(const char* src, absl::Span<uint16_t> dest);
void ReadLittleEndian32s(const char* src, absl::Span<uint32_t> dest);
void ReadLittleEndian64s(const char* src, absl::Span<uint64_t> dest);
void ReadBigEndian16s(const char* src, absl::Span<uint16_t> dest);
void ReadBigEndian32s(const char* src, absl::Span<uint32_t> dest);
void ReadBigEndian64s(const char* src, absl::Span<uint64_t> dest);

// Implementation details follow.

inline bool ReadLittleEndian16(Reader& src, uint16_t& dest) {
  if (ABSL_PREDICT_FALSE(!src.Pull(sizeof(uint16_t)))) return false;
  dest = ReadLittleEndian16(src.cursor());
  src.move_cursor(sizeof(uint16_t));
  return true;
}

inline bool ReadLittleEndian32(Reader& src, uint32_t& dest) {
  if (ABSL_PREDICT_FALSE(!src.Pull(sizeof(uint32_t)))) return false;
  dest = ReadLittleEndian32(src.cursor());
  src.move_cursor(sizeof(uint32_t));
  return true;
}

inline bool ReadLittleEndian64(Reader& src, uint64_t& dest) {
  if (ABSL_PREDICT_FALSE(!src.Pull(sizeof(uint64_t)))) return false;
  dest = ReadLittleEndian64(src.cursor());
  src.move_cursor(sizeof(uint64_t));
  return true;
}

inline bool ReadLittleEndianSigned16(Reader& src, int16_t& dest) {
  uint16_t unsigned_dest;
  if (ABSL_PREDICT_FALSE(!ReadLittleEndian16(src, unsigned_dest))) return false;
  dest = static_cast<int16_t>(unsigned_dest);
  return true;
}

inline bool ReadLittleEndianSigned32(Reader& src, int32_t& dest) {
  uint32_t unsigned_dest;
  if (ABSL_PREDICT_FALSE(!ReadLittleEndian32(src, unsigned_dest))) return false;
  dest = static_cast<int32_t>(unsigned_dest);
  return true;
}

inline bool ReadLittleEndianSigned64(Reader& src, int64_t& dest) {
  uint64_t unsigned_dest;
  if (ABSL_PREDICT_FALSE(!ReadLittleEndian64(src, unsigned_dest))) return false;
  dest = static_cast<int64_t>(unsigned_dest);
  return true;
}

inline bool ReadBigEndian16(Reader& src, uint16_t& dest) {
  if (ABSL_PREDICT_FALSE(!src.Pull(sizeof(uint16_t)))) return false;
  dest = ReadBigEndian16(src.cursor());
  src.move_cursor(sizeof(uint16_t));
  return true;
}

inline bool ReadBigEndian32(Reader& src, uint32_t& dest) {
  if (ABSL_PREDICT_FALSE(!src.Pull(sizeof(uint32_t)))) return false;
  dest = ReadBigEndian32(src.cursor());
  src.move_cursor(sizeof(uint32_t));
  return true;
}

inline bool ReadBigEndian64(Reader& src, uint64_t& dest) {
  if (ABSL_PREDICT_FALSE(!src.Pull(sizeof(uint64_t)))) return false;
  dest = ReadBigEndian64(src.cursor());
  src.move_cursor(sizeof(uint64_t));
  return true;
}

inline bool ReadBigEndianSigned16(Reader& src, int16_t& dest) {
  uint16_t unsigned_dest;
  if (ABSL_PREDICT_FALSE(!ReadBigEndian16(src, unsigned_dest))) return false;
  dest = static_cast<int16_t>(unsigned_dest);
  return true;
}

inline bool ReadBigEndianSigned32(Reader& src, int32_t& dest) {
  uint32_t unsigned_dest;
  if (ABSL_PREDICT_FALSE(!ReadBigEndian32(src, unsigned_dest))) return false;
  dest = static_cast<int32_t>(unsigned_dest);
  return true;
}

inline bool ReadBigEndianSigned64(Reader& src, int64_t& dest) {
  uint64_t unsigned_dest;
  if (ABSL_PREDICT_FALSE(!ReadBigEndian64(src, unsigned_dest))) return false;
  dest = static_cast<int64_t>(unsigned_dest);
  return true;
}

template <>
inline bool ReadLittleEndian<uint8_t>(Reader& src, uint8_t& dest) {
  return src.ReadByte(dest);
}
template <>
inline bool ReadLittleEndian<uint16_t>(Reader& src, uint16_t& dest) {
  return ReadLittleEndian16(src, dest);
}
template <>
inline bool ReadLittleEndian<uint32_t>(Reader& src, uint32_t& dest) {
  return ReadLittleEndian32(src, dest);
}
template <>
inline bool ReadLittleEndian<uint64_t>(Reader& src, uint64_t& dest) {
  return ReadLittleEndian64(src, dest);
}
template <>
inline bool ReadLittleEndian<int8_t>(Reader& src, int8_t& dest) {
  uint8_t unsigned_dest;
  if (ABSL_PREDICT_FALSE(!src.ReadByte(unsigned_dest))) return false;
  dest = static_cast<int8_t>(unsigned_dest);
  return true;
}
template <>
inline bool ReadLittleEndian<int16_t>(Reader& src, int16_t& dest) {
  return ReadLittleEndianSigned16(src, dest);
}
template <>
inline bool ReadLittleEndian<int32_t>(Reader& src, int32_t& dest) {
  return ReadLittleEndianSigned32(src, dest);
}
template <>
inline bool ReadLittleEndian<int64_t>(Reader& src, int64_t& dest) {
  return ReadLittleEndianSigned64(src, dest);
}

template <>
inline bool ReadBigEndian<uint8_t>(Reader& src, uint8_t& dest) {
  return src.ReadByte(dest);
}
template <>
inline bool ReadBigEndian<uint16_t>(Reader& src, uint16_t& dest) {
  return ReadBigEndian16(src, dest);
}
template <>
inline bool ReadBigEndian<uint32_t>(Reader& src, uint32_t& dest) {
  return ReadBigEndian32(src, dest);
}
template <>
inline bool ReadBigEndian<uint64_t>(Reader& src, uint64_t& dest) {
  return ReadBigEndian64(src, dest);
}
template <>
inline bool ReadBigEndian<int8_t>(Reader& src, int8_t& dest) {
  uint8_t unsigned_dest;
  if (ABSL_PREDICT_FALSE(!src.ReadByte(unsigned_dest))) return false;
  dest = static_cast<int8_t>(unsigned_dest);
  return true;
}
template <>
inline bool ReadBigEndian<int16_t>(Reader& src, int16_t& dest) {
  return ReadBigEndianSigned16(src, dest);
}
template <>
inline bool ReadBigEndian<int32_t>(Reader& src, int32_t& dest) {
  return ReadBigEndianSigned32(src, dest);
}
template <>
inline bool ReadBigEndian<int64_t>(Reader& src, int64_t& dest) {
  return ReadBigEndianSigned64(src, dest);
}

inline bool ReadLittleEndian16s(Reader& src, absl::Span<uint16_t> dest) {
  if (internal::IsLittleEndian()) {
    return src.Read(dest.size() * sizeof(uint16_t),
                    reinterpret_cast<char*>(dest.data()));
  } else {
    for (uint16_t& dest_value : dest) {
      if (ABSL_PREDICT_FALSE(!ReadLittleEndian16(src, dest_value))) {
        return false;
      }
    }
    return true;
  }
}

inline bool ReadLittleEndian32s(Reader& src, absl::Span<uint32_t> dest) {
  if (internal::IsLittleEndian()) {
    return src.Read(dest.size() * sizeof(uint32_t),
                    reinterpret_cast<char*>(dest.data()));
  } else {
    for (uint32_t& dest_value : dest) {
      if (ABSL_PREDICT_FALSE(!ReadLittleEndian32(src, dest_value))) {
        return false;
      }
    }
    return true;
  }
}

inline bool ReadLittleEndian64s(Reader& src, absl::Span<uint64_t> dest) {
  if (internal::IsLittleEndian()) {
    return src.Read(dest.size() * sizeof(uint64_t),
                    reinterpret_cast<char*>(dest.data()));
  } else {
    for (uint64_t& dest_value : dest) {
      if (ABSL_PREDICT_FALSE(!ReadLittleEndian64(src, dest_value))) {
        return false;
      }
    }
    return true;
  }
}

inline bool ReadLittleEndianSigned16s(Reader& src, absl::Span<int16_t> dest) {
  if (internal::IsLittleEndian()) {
    return src.Read(dest.size() * sizeof(int16_t),
                    reinterpret_cast<char*>(dest.data()));
  } else {
    for (int16_t& dest_value : dest) {
      if (ABSL_PREDICT_FALSE(!ReadLittleEndianSigned16(src, dest_value))) {
        return false;
      }
    }
    return true;
  }
}

inline bool ReadLittleEndianSigned32s(Reader& src, absl::Span<int32_t> dest) {
  if (internal::IsLittleEndian()) {
    return src.Read(dest.size() * sizeof(int32_t),
                    reinterpret_cast<char*>(dest.data()));
  } else {
    for (int32_t& dest_value : dest) {
      if (ABSL_PREDICT_FALSE(!ReadLittleEndianSigned32(src, dest_value))) {
        return false;
      }
    }
    return true;
  }
}

inline bool ReadLittleEndianSigned64s(Reader& src, absl::Span<int64_t> dest) {
  if (internal::IsLittleEndian()) {
    return src.Read(dest.size() * sizeof(int64_t),
                    reinterpret_cast<char*>(dest.data()));
  } else {
    for (int64_t& dest_value : dest) {
      if (ABSL_PREDICT_FALSE(!ReadLittleEndianSigned64(src, dest_value))) {
        return false;
      }
    }
    return true;
  }
}

inline bool ReadBigEndian16s(Reader& src, absl::Span<uint16_t> dest) {
  if (internal::IsBigEndian()) {
    return src.Read(dest.size() * sizeof(uint16_t),
                    reinterpret_cast<char*>(dest.data()));
  } else {
    for (uint16_t& dest_value : dest) {
      if (ABSL_PREDICT_FALSE(!ReadBigEndian16(src, dest_value))) return false;
    }
    return true;
  }
}

inline bool ReadBigEndian32s(Reader& src, absl::Span<uint32_t> dest) {
  if (internal::IsBigEndian()) {
    return src.Read(dest.size() * sizeof(uint32_t),
                    reinterpret_cast<char*>(dest.data()));
  } else {
    for (uint32_t& dest_value : dest) {
      if (ABSL_PREDICT_FALSE(!ReadBigEndian32(src, dest_value))) return false;
    }
    return true;
  }
}

inline bool ReadBigEndian64s(Reader& src, absl::Span<uint64_t> dest) {
  if (internal::IsBigEndian()) {
    return src.Read(dest.size() * sizeof(uint64_t),
                    reinterpret_cast<char*>(dest.data()));
  } else {
    for (uint64_t& dest_value : dest) {
      if (ABSL_PREDICT_FALSE(!ReadBigEndian64(src, dest_value))) return false;
    }
    return true;
  }
}

inline bool ReadBigEndianSigned16s(Reader& src, absl::Span<int16_t> dest) {
  if (internal::IsBigEndian()) {
    return src.Read(dest.size() * sizeof(int16_t),
                    reinterpret_cast<char*>(dest.data()));
  } else {
    for (int16_t& dest_value : dest) {
      if (ABSL_PREDICT_FALSE(!ReadBigEndianSigned16(src, dest_value)))
        return false;
    }
    return true;
  }
}

inline bool ReadBigEndianSigned32s(Reader& src, absl::Span<int32_t> dest) {
  if (internal::IsBigEndian()) {
    return src.Read(dest.size() * sizeof(int32_t),
                    reinterpret_cast<char*>(dest.data()));
  } else {
    for (int32_t& dest_value : dest) {
      if (ABSL_PREDICT_FALSE(!ReadBigEndianSigned32(src, dest_value)))
        return false;
    }
    return true;
  }
}

inline bool ReadBigEndianSigned64s(Reader& src, absl::Span<int64_t> dest) {
  if (internal::IsBigEndian()) {
    return src.Read(dest.size() * sizeof(int64_t),
                    reinterpret_cast<char*>(dest.data()));
  } else {
    for (int64_t& dest_value : dest) {
      if (ABSL_PREDICT_FALSE(!ReadBigEndianSigned64(src, dest_value)))
        return false;
    }
    return true;
  }
}

template <>
inline bool ReadLittleEndians<uint8_t>(Reader& src, absl::Span<uint8_t> dest) {
  return src.Read(dest.size(), reinterpret_cast<char*>(dest.data()));
}
template <>
inline bool ReadLittleEndians<uint16_t>(Reader& src,
                                        absl::Span<uint16_t> dest) {
  return ReadLittleEndian16s(src, dest);
}
template <>
inline bool ReadLittleEndians<uint32_t>(Reader& src,
                                        absl::Span<uint32_t> dest) {
  return ReadLittleEndian32s(src, dest);
}
template <>
inline bool ReadLittleEndians<uint64_t>(Reader& src,
                                        absl::Span<uint64_t> dest) {
  return ReadLittleEndian64s(src, dest);
}
template <>
inline bool ReadLittleEndians<int8_t>(Reader& src, absl::Span<int8_t> dest) {
  return src.Read(dest.size(), reinterpret_cast<char*>(dest.data()));
}
template <>
inline bool ReadLittleEndians<int16_t>(Reader& src, absl::Span<int16_t> dest) {
  return ReadLittleEndianSigned16s(src, dest);
}
template <>
inline bool ReadLittleEndians<int32_t>(Reader& src, absl::Span<int32_t> dest) {
  return ReadLittleEndianSigned32s(src, dest);
}
template <>
inline bool ReadLittleEndians<int64_t>(Reader& src, absl::Span<int64_t> dest) {
  return ReadLittleEndianSigned64s(src, dest);
}

template <>
inline bool ReadBigEndians<uint8_t>(Reader& src, absl::Span<uint8_t> dest) {
  return src.Read(dest.size(), reinterpret_cast<char*>(dest.data()));
}
template <>
inline bool ReadBigEndians<uint16_t>(Reader& src, absl::Span<uint16_t> dest) {
  return ReadBigEndian16s(src, dest);
}
template <>
inline bool ReadBigEndians<uint32_t>(Reader& src, absl::Span<uint32_t> dest) {
  return ReadBigEndian32s(src, dest);
}
template <>
inline bool ReadBigEndians<uint64_t>(Reader& src, absl::Span<uint64_t> dest) {
  return ReadBigEndian64s(src, dest);
}
template <>
inline bool ReadBigEndians<int8_t>(Reader& src, absl::Span<int8_t> dest) {
  return src.Read(dest.size(), reinterpret_cast<char*>(dest.data()));
}
template <>
inline bool ReadBigEndians<int16_t>(Reader& src, absl::Span<int16_t> dest) {
  return ReadBigEndianSigned16s(src, dest);
}
template <>
inline bool ReadBigEndians<int32_t>(Reader& src, absl::Span<int32_t> dest) {
  return ReadBigEndianSigned32s(src, dest);
}
template <>
inline bool ReadBigEndians<int64_t>(Reader& src, absl::Span<int64_t> dest) {
  return ReadBigEndianSigned64s(src, dest);
}

namespace internal {

// If these functions are manually inlined into their callers, clang generates
// poor code (with byte shifting even for native endianness).

inline uint16_t DecodeLittleEndian16(uint16_t encoded) {
  const unsigned char* const ptr =
      reinterpret_cast<const unsigned char*>(&encoded);
  return uint16_t{ptr[0]} | (uint16_t{ptr[1]} << 8);
}

inline uint32_t DecodeLittleEndian32(uint32_t encoded) {
  const unsigned char* const ptr =
      reinterpret_cast<const unsigned char*>(&encoded);
  return uint32_t{ptr[0]} | (uint32_t{ptr[1]} << 8) |
         (uint32_t{ptr[2]} << (2 * 8)) | (uint32_t{ptr[3]} << (3 * 8));
}

inline uint64_t DecodeLittleEndian64(uint64_t encoded) {
  const unsigned char* const ptr =
      reinterpret_cast<const unsigned char*>(&encoded);
  return uint64_t{ptr[0]} | (uint64_t{ptr[1]} << 8) |
         (uint64_t{ptr[2]} << (2 * 8)) | (uint64_t{ptr[3]} << (3 * 8)) |
         (uint64_t{ptr[4]} << (4 * 8)) | (uint64_t{ptr[5]} << (5 * 8)) |
         (uint64_t{ptr[6]} << (6 * 8)) | (uint64_t{ptr[7]} << (7 * 8));
}

inline uint16_t DecodeBigEndian16(uint16_t encoded) {
  const unsigned char* const ptr =
      reinterpret_cast<const unsigned char*>(&encoded);
  return (uint16_t{ptr[0]} << 8) | uint16_t{ptr[1]};
}

inline uint32_t DecodeBigEndian32(uint32_t encoded) {
  const unsigned char* const ptr =
      reinterpret_cast<const unsigned char*>(&encoded);
  return (uint32_t{ptr[0]} << (3 * 8)) | (uint32_t{ptr[1]} << (2 * 8)) |
         (uint32_t{ptr[2]} << 8) | uint32_t{ptr[3]};
}

inline uint64_t DecodeBigEndian64(uint64_t encoded) {
  const unsigned char* const ptr =
      reinterpret_cast<const unsigned char*>(&encoded);
  return (uint64_t{ptr[0]} << (7 * 8)) | (uint64_t{ptr[1]} << (6 * 8)) |
         (uint64_t{ptr[2]} << (5 * 8)) | (uint64_t{ptr[3]} << (4 * 8)) |
         (uint64_t{ptr[4]} << (3 * 8)) | (uint64_t{ptr[5]} << (2 * 8)) |
         (uint64_t{ptr[6]} << 8) | uint64_t{ptr[7]};
}

}  // namespace internal

inline uint16_t ReadLittleEndian16(const char* src) {
  uint16_t encoded;
  std::memcpy(&encoded, src, sizeof(uint16_t));
  return internal::DecodeLittleEndian16(encoded);
}

inline uint32_t ReadLittleEndian32(const char* src) {
  uint32_t encoded;
  std::memcpy(&encoded, src, sizeof(uint32_t));
  return internal::DecodeLittleEndian32(encoded);
}

inline uint64_t ReadLittleEndian64(const char* src) {
  uint64_t encoded;
  std::memcpy(&encoded, src, sizeof(uint64_t));
  return internal::DecodeLittleEndian64(encoded);
}

inline uint16_t ReadBigEndian16(const char* src) {
  uint16_t encoded;
  std::memcpy(&encoded, src, sizeof(uint16_t));
  return internal::DecodeBigEndian16(encoded);
}

inline uint32_t ReadBigEndian32(const char* src) {
  uint32_t encoded;
  std::memcpy(&encoded, src, sizeof(uint32_t));
  return internal::DecodeBigEndian32(encoded);
}

inline uint64_t ReadBigEndian64(const char* src) {
  uint64_t encoded;
  std::memcpy(&encoded, src, sizeof(uint64_t));
  return internal::DecodeBigEndian64(encoded);
}

inline void ReadLittleEndian16s(const char* src, absl::Span<uint16_t> dest) {
  if (internal::IsLittleEndian()) {
    if (ABSL_PREDICT_TRUE(
            // `std::memcpy(nullptr, _, 0)` and `std::memcpy(_, nullptr, 0)` are
            // undefined.
            !dest.empty())) {
      std::memcpy(dest.data(), src, dest.size() * sizeof(uint16_t));
    }
  } else {
    for (uint16_t& value : dest) {
      value = ReadLittleEndian16(src);
      src += sizeof(uint16_t);
    }
  }
}

inline void ReadLittleEndian32s(const char* src, absl::Span<uint32_t> dest) {
  if (internal::IsLittleEndian()) {
    if (ABSL_PREDICT_TRUE(
            // `std::memcpy(nullptr, _, 0)` and `std::memcpy(_, nullptr, 0)` are
            // undefined.
            !dest.empty())) {
      std::memcpy(dest.data(), src, dest.size() * sizeof(uint32_t));
    }
  } else {
    for (uint32_t& value : dest) {
      value = ReadLittleEndian32(src);
      src += sizeof(uint32_t);
    }
  }
}

inline void ReadLittleEndian64s(const char* src, absl::Span<uint64_t> dest) {
  if (internal::IsLittleEndian()) {
    if (ABSL_PREDICT_TRUE(
            // `std::memcpy(nullptr, _, 0)` and `std::memcpy(_, nullptr, 0)` are
            // undefined.
            !dest.empty())) {
      std::memcpy(dest.data(), src, dest.size() * sizeof(uint64_t));
    }
  } else {
    for (uint64_t& value : dest) {
      value = ReadLittleEndian64(src);
      src += sizeof(uint64_t);
    }
  }
}

inline void ReadBigEndian16s(const char* src, absl::Span<uint16_t> dest) {
  if (internal::IsBigEndian()) {
    if (ABSL_PREDICT_TRUE(
            // `std::memcpy(nullptr, _, 0)` and `std::memcpy(_, nullptr, 0)` are
            // undefined.
            !dest.empty())) {
      std::memcpy(dest.data(), src, dest.size() * sizeof(uint16_t));
    }
  } else {
    for (uint16_t& value : dest) {
      value = ReadBigEndian16(src);
      src += sizeof(uint16_t);
    }
  }
}

inline void ReadBigEndian32s(const char* src, absl::Span<uint32_t> dest) {
  if (internal::IsBigEndian()) {
    if (ABSL_PREDICT_TRUE(
            // `std::memcpy(nullptr, _, 0)` and `std::memcpy(_, nullptr, 0)` are
            // undefined.
            !dest.empty())) {
      std::memcpy(dest.data(), src, dest.size() * sizeof(uint32_t));
    }
  } else {
    for (uint32_t& value : dest) {
      value = ReadBigEndian32(src);
      src += sizeof(uint32_t);
    }
  }
}

inline void ReadBigEndian64s(const char* src, absl::Span<uint64_t> dest) {
  if (internal::IsBigEndian()) {
    if (ABSL_PREDICT_TRUE(
            // `std::memcpy(nullptr, _, 0)` and `std::memcpy(_, nullptr, 0)` are
            // undefined.
            !dest.empty())) {
      std::memcpy(dest.data(), src, dest.size() * sizeof(uint64_t));
    }
  } else {
    for (uint64_t& value : dest) {
      value = ReadBigEndian64(src);
      src += sizeof(uint64_t);
    }
  }
}

}  // namespace riegeli

#endif  // RIEGELI_ENDIAN_ENDIAN_READING_H_
