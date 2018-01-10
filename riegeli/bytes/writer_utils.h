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

#ifndef RIEGELI_BYTES_WRITER_UTILS_H_
#define RIEGELI_BYTES_WRITER_UTILS_H_

#include <stddef.h>
#include <stdint.h>
#include <cstring>

#include "riegeli/base/base.h"
#include "riegeli/bytes/varint.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

bool WriteByte(Writer* dest, uint8_t data);

size_t LengthVarint32(uint32_t data);  // At most kMaxLengthVarint32().
size_t LengthVarint64(uint64_t data);  // At most kMaxLengthVarint64().

// At least LengthVarint32(data) bytes of space at dest must be available.
char* WriteVarint32(char* dest, uint32_t data);
// At least LengthVarint64(data) bytes of space at dest must be available.
char* WriteVarint64(char* dest, uint64_t data);

bool WriteVarint32(Writer* dest, uint32_t data);
bool WriteVarint64(Writer* dest, uint64_t data);

bool WriteZeros(Writer* dest, Position length);

// Implementation details follow.

namespace internal {

size_t LengthBigVarint64(uint64_t data);

// General case of compile time recursion.
template <typename Data, size_t min_length = 1>
size_t LengthVarint(Data data) {
  if (data < (Data{1} << (7 * min_length))) return min_length;
  return LengthVarint<Data, min_length + 1>(data);
}

// Base case of compile time recursion for 32 bits.
template <>
inline size_t LengthVarint<uint32_t, kMaxLengthVarint32()>(uint32_t data) {
  return kMaxLengthVarint32();
}

// Base case of compile time recursion for 64 bits: continue with longer numbers
// in a separately compiled function.
template <>
inline size_t LengthVarint<uint64_t, kMaxLengthVarint32() + 1>(uint64_t data) {
  return LengthBigVarint64(data);
}

char* ContinueWritingVarint64(char* dest, uint64_t data);

// General case of compile time recursion.
template <typename Data, size_t min_length = 1>
char* WriteVarint(char* dest, Data data) {
  if (data < 0x80) {
    *dest++ = static_cast<char>(data);
    return dest;
  }
  *dest++ = static_cast<char>(data | 0x80);
  return WriteVarint<Data, min_length + 1>(dest, data >> 7);
}

// Base case of compile time recursion for 32 bits.
template <>
inline char* WriteVarint<uint32_t, kMaxLengthVarint32()>(char* dest,
                                                         uint32_t data) {
  *dest++ = static_cast<char>(data);
  return dest;
}

// Base case of compile time recursion for 64 bits: continue with longer numbers
// in a separately compiled function.
template <>
inline char* WriteVarint<uint64_t, kMaxLengthVarint32()>(char* dest,
                                                         uint64_t data) {
  if (data < 0x80) {
    *dest++ = static_cast<char>(data);
    return dest;
  }
  return ContinueWritingVarint64(dest, data);
}

bool WriteVarint32Slow(Writer* dest, uint32_t data);
bool WriteVarint64Slow(Writer* dest, uint64_t data);

bool WriteZerosSlow(Writer* dest, Position length);

}  // namespace internal

inline bool WriteByte(Writer* dest, uint8_t data) {
  if (RIEGELI_UNLIKELY(!dest->Push())) return false;
  char* cursor = dest->cursor();
  *cursor++ = static_cast<char>(data);
  dest->set_cursor(cursor);
  return true;
}

inline size_t LengthVarint32(uint32_t data) {
  return internal::LengthVarint(data);
}

inline size_t LengthVarint64(uint64_t data) {
  return internal::LengthVarint(data);
}

inline char* WriteVarint32(char* dest, uint32_t data) {
  return internal::WriteVarint(dest, data);
}

inline char* WriteVarint64(char* dest, uint64_t data) {
  return internal::WriteVarint(dest, data);
}

inline bool WriteVarint32(Writer* dest, uint32_t data) {
  if (RIEGELI_LIKELY(dest->available() >= kMaxLengthVarint32())) {
    dest->set_cursor(WriteVarint32(dest->cursor(), data));
    return true;
  }
  return internal::WriteVarint32Slow(dest, data);
}

inline bool WriteVarint64(Writer* dest, uint64_t data) {
  if (RIEGELI_LIKELY(dest->available() >= kMaxLengthVarint64())) {
    dest->set_cursor(WriteVarint64(dest->cursor(), data));
    return true;
  }
  return internal::WriteVarint64Slow(dest, data);
}

inline bool WriteZeros(Writer* dest, Position length) {
  if (RIEGELI_LIKELY(length <= dest->available())) {
    if (length > 0) {  // memset(nullptr, _, 0) is undefined.
      std::memset(dest->cursor(), 0, length);
      dest->set_cursor(dest->cursor() + length);
    }
    return true;
  }
  return internal::WriteZerosSlow(dest, length);
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_WRITER_UTILS_H_
