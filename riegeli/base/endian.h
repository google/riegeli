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

#ifndef RIEGELI_BASE_ENDIAN_H_
#define RIEGELI_BASE_ENDIAN_H_

#include <stdint.h>

#include <cstring>

namespace riegeli {

// Writes a number in a fixed width Little/Big Endian encoding to an array.
//
// Writes `sizeof(uint{16,32,64}_t)` bytes to `dest[]`.
void WriteLittleEndian16(uint16_t data, char* dest);
void WriteLittleEndian32(uint32_t data, char* dest);
void WriteLittleEndian64(uint64_t data, char* dest);
void WriteBigEndian16(uint16_t data, char* dest);
void WriteBigEndian32(uint32_t data, char* dest);
void WriteBigEndian64(uint64_t data, char* dest);

// Reads a number in a fixed width Little/Big Endian encoding from an array.
//
// Reads `sizeof(uint{16,32,64}_t)` bytes  from `src[]`.
uint16_t ReadLittleEndian16(const char* src);
uint32_t ReadLittleEndian32(const char* src);
uint64_t ReadLittleEndian64(const char* src);
uint16_t ReadBigEndian16(const char* src);
uint32_t ReadBigEndian32(const char* src);
uint64_t ReadBigEndian64(const char* src);

// Implementation details follow.

namespace internal {

// If these functions are manually inlined into their callers, clang generates
// poor code (with byte shifting even for native endianness).

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

inline void WriteLittleEndian16(uint16_t data, char* dest) {
  const uint16_t encoded = internal::EncodeLittleEndian16(data);
  std::memcpy(dest, &encoded, sizeof(uint16_t));
}

inline void WriteLittleEndian32(uint32_t data, char* dest) {
  const uint32_t encoded = internal::EncodeLittleEndian32(data);
  std::memcpy(dest, &encoded, sizeof(uint32_t));
}

inline void WriteLittleEndian64(uint64_t data, char* dest) {
  const uint64_t encoded = internal::EncodeLittleEndian64(data);
  std::memcpy(dest, &encoded, sizeof(uint64_t));
}

inline void WriteBigEndian16(uint16_t data, char* dest) {
  const uint16_t encoded = internal::EncodeBigEndian16(data);
  std::memcpy(dest, &encoded, sizeof(uint16_t));
}

inline void WriteBigEndian32(uint32_t data, char* dest) {
  const uint32_t encoded = internal::EncodeBigEndian32(data);
  std::memcpy(dest, &encoded, sizeof(uint32_t));
}

inline void WriteBigEndian64(uint64_t data, char* dest) {
  const uint64_t encoded = internal::EncodeBigEndian64(data);
  std::memcpy(dest, &encoded, sizeof(uint64_t));
}

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

}  // namespace riegeli

#endif  // RIEGELI_BASE_ENDIAN_H_
