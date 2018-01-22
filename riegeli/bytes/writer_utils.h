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
#include "riegeli/base/port.h"
#include "riegeli/bytes/varint.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

bool WriteByte(Writer* dest, uint8_t data);

size_t LengthVarint32(uint32_t data);  // At most kMaxLengthVarint32().
size_t LengthVarint64(uint64_t data);  // At most kMaxLengthVarint64().

// At least LengthVarint32(data) bytes of space at *dest must be available.
char* WriteVarint32(char* dest, uint32_t data);
// At least LengthVarint64(data) bytes of space at *dest must be available.
char* WriteVarint64(char* dest, uint64_t data);

bool WriteVarint32(Writer* dest, uint32_t data);
bool WriteVarint64(Writer* dest, uint64_t data);

bool WriteZeros(Writer* dest, Position length);

// Implementation details follow.

namespace internal {

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
#if RIEGELI_INTERNAL_HAS_BUILTIN(__builtin_clz) || \
    RIEGELI_INTERNAL_IS_GCC_VERSION(3, 4)
  const size_t floor_log2 = sizeof(unsigned) >= 4
                                ? __builtin_clz(data | 1) ^ __builtin_clz(1)
                                : __builtin_clzl(data | 1) ^ __builtin_clzl(1);
  // This is the same as floor_log2 / 7 + 1 for floor_log2 in 0..31
  // but divides by a power of 2.
  return (floor_log2 * 9 + 73) / 64;
#else
  size_t length = 1;
  while (data >= 0x80) {
    ++length;
    data >>= 7;
  }
  return length;
#endif
}

inline size_t LengthVarint64(uint64_t data) {
#if RIEGELI_INTERNAL_HAS_BUILTIN(__builtin_clzll) || \
    RIEGELI_INTERNAL_IS_GCC_VERSION(3, 4)
  const size_t floor_log2 = __builtin_clzll(data | 1) ^ __builtin_clzll(1);
  // This is the same as floor_log2 / 7 + 1 for floor_log2 in 0..63
  // but divides by a power of 2.
  return (floor_log2 * 9 + 73) / 64;
#else
  size_t length = 1;
  while (data >= 0x80) {
    ++length;
    data >>= 7;
  }
  return length;
#endif
}

inline char* WriteVarint32(char* dest, uint32_t data) {
  while (data >= 0x80) {
    *dest++ = static_cast<char>(data | 0x80);
    data >>= 7;
  }
  *dest++ = static_cast<char>(data);
  return dest;
}

inline char* WriteVarint64(char* dest, uint64_t data) {
  while (data >= 0x80) {
    *dest++ = static_cast<char>(data | 0x80);
    data >>= 7;
  }
  *dest++ = static_cast<char>(data);
  return dest;
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
