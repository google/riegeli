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

namespace riegeli {

// Converts a native 16-bit number to a word such that writing the word to a
// byte array using `std::memcpy()` writes the number in 16-bit Little Endian
// encoding.
inline uint16_t WriteLittleEndian16(uint16_t data) {
  uint16_t word;
  unsigned char* const ptr = reinterpret_cast<unsigned char*>(&word);
  ptr[0] = static_cast<unsigned char>(data);
  ptr[1] = static_cast<unsigned char>(data >> 8);
  return word;
}

// Converts a native 32-bit number to a word such that writing the word to a
// byte array using `std::memcpy()` writes the number in 32-bit Little Endian
// encoding.
inline uint32_t WriteLittleEndian32(uint32_t data) {
  uint32_t word;
  unsigned char* const ptr = reinterpret_cast<unsigned char*>(&word);
  ptr[0] = static_cast<unsigned char>(data);
  ptr[1] = static_cast<unsigned char>(data >> 8);
  ptr[2] = static_cast<unsigned char>(data >> (8 * 2));
  ptr[3] = static_cast<unsigned char>(data >> (8 * 3));
  return word;
}

// Converts a native 64-bit number to a word such that writing the word to a
// byte array using `std::memcpy()` writes the number in 64-bit Little Endian
// encoding.
inline uint64_t WriteLittleEndian64(uint64_t data) {
  uint64_t word;
  unsigned char* const ptr = reinterpret_cast<unsigned char*>(&word);
  ptr[0] = static_cast<unsigned char>(data);
  ptr[1] = static_cast<unsigned char>(data >> 8);
  ptr[2] = static_cast<unsigned char>(data >> (8 * 2));
  ptr[3] = static_cast<unsigned char>(data >> (8 * 3));
  ptr[4] = static_cast<unsigned char>(data >> (8 * 4));
  ptr[5] = static_cast<unsigned char>(data >> (8 * 5));
  ptr[6] = static_cast<unsigned char>(data >> (8 * 6));
  ptr[7] = static_cast<unsigned char>(data >> (8 * 7));
  return word;
}

// Converts a native 16-bit number to a word such that writing the word to a
// byte array using `std::memcpy()` writes the number in 16-bit Big Endian
// encoding.
inline uint16_t WriteBigEndian16(uint16_t data) {
  uint16_t word;
  unsigned char* const ptr = reinterpret_cast<unsigned char*>(&word);
  ptr[0] = static_cast<unsigned char>(data >> 8);
  ptr[1] = static_cast<unsigned char>(data);
  return word;
}

// Converts a native 32-bit number to a word such that writing the word to a
// byte array using `std::memcpy()` writes the number in 32-bit Big Endian
// encoding.
inline uint32_t WriteBigEndian32(uint32_t data) {
  uint32_t word;
  unsigned char* const ptr = reinterpret_cast<unsigned char*>(&word);
  ptr[0] = static_cast<unsigned char>(data >> (8 * 3));
  ptr[1] = static_cast<unsigned char>(data >> (8 * 2));
  ptr[2] = static_cast<unsigned char>(data >> 8);
  ptr[3] = static_cast<unsigned char>(data);
  return word;
}

// Converts a native 64-bit number to a word such that writing the word to a
// byte array using `std::memcpy()` writes the number in 64-bit Big Endian
// encoding.
inline uint64_t WriteBigEndian64(uint64_t data) {
  uint64_t word;
  unsigned char* const ptr = reinterpret_cast<unsigned char*>(&word);
  ptr[0] = static_cast<unsigned char>(data >> (8 * 7));
  ptr[1] = static_cast<unsigned char>(data >> (8 * 6));
  ptr[2] = static_cast<unsigned char>(data >> (8 * 5));
  ptr[3] = static_cast<unsigned char>(data >> (8 * 4));
  ptr[4] = static_cast<unsigned char>(data >> (8 * 3));
  ptr[5] = static_cast<unsigned char>(data >> (8 * 2));
  ptr[6] = static_cast<unsigned char>(data >> 8);
  ptr[7] = static_cast<unsigned char>(data);
  return word;
}

// For a byte array containing a number in 16-bit Little Endian encoding, and a
// word read from the byte array using `std::memcpy()`, converts the word to the
// native 16-bit number.
inline uint16_t ReadLittleEndian16(uint16_t word) {
  const unsigned char* const ptr =
      reinterpret_cast<const unsigned char*>(&word);
  return uint16_t{ptr[0]} | (uint16_t{ptr[1]} << 8);
}

// For a byte array containing a number in 32-bit Little Endian encoding, and a
// word read from the byte array using `std::memcpy()`, converts the word to the
// native 32-bit number.
inline uint32_t ReadLittleEndian32(uint32_t word) {
  const unsigned char* const ptr =
      reinterpret_cast<const unsigned char*>(&word);
  return uint32_t{ptr[0]} | (uint32_t{ptr[1]} << 8) |
         (uint32_t{ptr[2]} << (8 * 2)) | (uint32_t{ptr[3]} << (8 * 3));
}

// For a byte array containing a number in 64-bit Little Endian encoding, and a
// word read from the byte array using `std::memcpy()`, converts the word to the
// native 64-bit number.
inline uint64_t ReadLittleEndian64(uint64_t word) {
  const unsigned char* const ptr =
      reinterpret_cast<const unsigned char*>(&word);
  return uint64_t{ptr[0]} | (uint64_t{ptr[1]} << 8) |
         (uint64_t{ptr[2]} << (8 * 2)) | (uint64_t{ptr[3]} << (8 * 3)) |
         (uint64_t{ptr[4]} << (8 * 4)) | (uint64_t{ptr[5]} << (8 * 5)) |
         (uint64_t{ptr[6]} << (8 * 6)) | (uint64_t{ptr[7]} << (8 * 7));
}

// For a byte array containing a number in 16-bit Big Endian encoding, and a
// word read from the byte array using `std::memcpy()`, converts the word to the
// native 16-bit number.
inline uint16_t ReadBigEndian16(uint16_t word) {
  const unsigned char* const ptr =
      reinterpret_cast<const unsigned char*>(&word);
  return (uint16_t{ptr[0]} << 8) | uint16_t{ptr[1]};
}

// For a byte array containing a number in 32-bit Big Endian encoding, and a
// word read from the byte array using `std::memcpy()`, converts the word to the
// native 32-bit number.
inline uint32_t ReadBigEndian32(uint32_t word) {
  const unsigned char* const ptr =
      reinterpret_cast<const unsigned char*>(&word);
  return (uint32_t{ptr[0]} << (8 * 3)) | (uint32_t{ptr[1]} << (8 * 2)) |
         (uint32_t{ptr[2]} << 8) | uint32_t{ptr[3]};
}

// For a byte array containing a number in 64-bit Big Endian encoding, and a
// word read from the byte array using `std::memcpy()`, converts the word to the
// native 64-bit number.
inline uint64_t ReadBigEndian64(uint64_t word) {
  const unsigned char* const ptr =
      reinterpret_cast<const unsigned char*>(&word);
  return (uint64_t{ptr[0]} << (8 * 7)) | (uint64_t{ptr[1]} << (8 * 6)) |
         (uint64_t{ptr[2]} << (8 * 5)) | (uint64_t{ptr[3]} << (8 * 4)) |
         (uint64_t{ptr[4]} << (8 * 3)) | (uint64_t{ptr[5]} << (8 * 2)) |
         (uint64_t{ptr[6]} << 8) | uint64_t{ptr[7]};
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_ENDIAN_H_
