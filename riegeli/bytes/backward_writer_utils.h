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

#ifndef RIEGELI_BYTES_BACKWARD_WRITER_UTILS_H_
#define RIEGELI_BYTES_BACKWARD_WRITER_UTILS_H_

#include <stddef.h>
#include <stdint.h>

#include "riegeli/base/base.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/varint.h"

namespace riegeli {

bool WriteVarint32(BackwardWriter* dest, uint32_t data);
bool WriteVarint64(BackwardWriter* dest, uint64_t data);

// Implementation details follow.

namespace internal {

char* ContinueWritingVarint64Backwards(char* dest, uint64_t data);

// General case of compile time recursion.
template <typename Data, size_t min_length = 1>
char* WriteVarintBackwards(char* dest, Data data) {
  if (data < 0x80) {
    *--dest = static_cast<char>(data);
  } else {
    dest = WriteVarintBackwards<Data, min_length + 1>(dest, data >> 7);
    *--dest = static_cast<char>(data | 0x80);
  }
  return dest;
}

// Base case of compile time recursion for 32 bits.
template <>
inline char* WriteVarintBackwards<uint32_t, kMaxLengthVarint32()>(
    char* dest, uint32_t data) {
  *--dest = static_cast<char>(data);
  return dest;
}

// Base case of compile time recursion for 64 bits: continue with longer numbers
// in a separately compiled function.
template <>
inline char* WriteVarintBackwards<uint64_t, kMaxLengthVarint32()>(
    char* dest, uint64_t data) {
  if (data < 0x80) {
    *--dest = static_cast<char>(data);
  } else {
    dest = ContinueWritingVarint64Backwards(dest, data);
  }
  return dest;
}

bool WriteVarint32Slow(BackwardWriter* dest, uint32_t data);
bool WriteVarint64Slow(BackwardWriter* dest, uint64_t data);

}  // namespace internal

inline bool WriteVarint32(BackwardWriter* dest, uint32_t data) {
  if (RIEGELI_LIKELY(dest->available() >= kMaxLengthVarint32())) {
    dest->set_cursor(internal::WriteVarintBackwards(dest->cursor(), data));
    return true;
  }
  return internal::WriteVarint32Slow(dest, data);
}

inline bool WriteVarint64(BackwardWriter* dest, uint64_t data) {
  if (RIEGELI_LIKELY(dest->available() >= kMaxLengthVarint64())) {
    dest->set_cursor(internal::WriteVarintBackwards(dest->cursor(), data));
    return true;
  }
  return internal::WriteVarint64Slow(dest, data);
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_BACKWARD_WRITER_UTILS_H_
