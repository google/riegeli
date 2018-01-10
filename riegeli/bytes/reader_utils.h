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

#ifndef RIEGELI_BYTES_READER_UTILS_H_
#define RIEGELI_BYTES_READER_UTILS_H_

#include <stddef.h>
#include <stdint.h>

#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/string_view.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/varint.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

bool ReadByte(Reader* src, uint8_t* data);

// At least kMaxLengthVarint32() bytes of data at *src must be available.
bool ReadVarint32(const char** src, uint32_t* data);
// At least kMaxLengthVarint64() bytes of data at *src must be available.
bool ReadVarint64(const char** src, uint64_t* data);

bool ReadVarint32(Reader* src, uint32_t* data);
bool ReadVarint64(Reader* src, uint64_t* data);

bool ReadAll(Reader* src, string_view* dest, std::string* scratch);
bool ReadAll(Reader* src, std::string* dest);
bool ReadAll(Reader* src, Chain* dest);
bool CopyAll(Reader* src, Writer* dest);
bool CopyAll(Reader* src, BackwardWriter* dest);

// Implementation details follow.

namespace internal {

bool ContinueReadingVarint64(const char** src, uint64_t acc, uint64_t* data);

// General case of compile time recursion.
template <typename Data, size_t min_length = 1>
bool ReadVarint(const char** src, Data* acc, Data* data) {
  const Data byte = static_cast<uint8_t>(*(*src)++);
  *acc += (byte - 1) << (7 * (min_length - 1));
  if (byte < 0x80) {
    *data = *acc;
    return true;
  }
  return ReadVarint<Data, min_length + 1>(src, acc, data);
}

// Base case of compile time recursion for 32 bits.
template <>
inline bool ReadVarint<uint32_t, kMaxLengthVarint32()>(const char** src,
                                                       uint32_t* acc,
                                                       uint32_t* data) {
  const uint32_t byte = static_cast<uint8_t>(*(*src)++);
  if (RIEGELI_UNLIKELY(byte >= 0x10)) return false;
  *acc += (byte - 1) << (7 * (kMaxLengthVarint32() - 1));
  *data = *acc;
  return true;
}

// Base case of compile time recursion for 64 bits: continue with longer numbers
// in a separately compiled function.
template <>
inline bool ReadVarint<uint64_t, kMaxLengthVarint32() + 1>(const char** src,
                                                           uint64_t* acc,
                                                           uint64_t* data) {
  return ContinueReadingVarint64(src, *acc, data);
}

// Another base case of compile time recursion for 64 bits, for the case when
// ReadVarint() is called from ContinueReadingVarint64() with a larger template
// argument.
template <>
inline bool ReadVarint<uint64_t, kMaxLengthVarint64()>(const char** src,
                                                       uint64_t* acc,
                                                       uint64_t* data) {
  const uint64_t byte = static_cast<uint8_t>(*(*src)++);
  if (RIEGELI_UNLIKELY(byte >= 0x02)) return false;
  *acc += (byte - 1) << (7 * (kMaxLengthVarint64() - 1));
  *data = *acc;
  return true;
}

bool ReadVarint32Slow(Reader* src, uint32_t* data);
bool ReadVarint64Slow(Reader* src, uint64_t* data);

}  // namespace internal

inline bool ReadByte(Reader* src, uint8_t* data) {
  if (RIEGELI_UNLIKELY(!src->Pull())) return false;
  const char* cursor = src->cursor();
  *data = static_cast<uint8_t>(*cursor++);
  src->set_cursor(cursor);
  return true;
}

inline bool ReadVarint32(const char** src, uint32_t* data) {
  const char* cursor = *src;
  uint32_t acc = 1;
  bool ok = internal::ReadVarint(&cursor, &acc, data);
  *src = cursor;
  return ok;
}

inline bool ReadVarint64(const char** src, uint64_t* data) {
  const char* cursor = *src;
  uint64_t acc = 1;
  bool ok = internal::ReadVarint(&cursor, &acc, data);
  *src = cursor;
  return ok;
}

inline bool ReadVarint32(Reader* src, uint32_t* data) {
  if (RIEGELI_LIKELY(src->available() >= kMaxLengthVarint32())) {
    const char* cursor = src->cursor();
    const bool ok = ReadVarint32(&cursor, data);
    src->set_cursor(cursor);
    return ok;
  }
  return internal::ReadVarint32Slow(src, data);
}

inline bool ReadVarint64(Reader* src, uint64_t* data) {
  if (RIEGELI_LIKELY(src->available() >= kMaxLengthVarint64())) {
    const char* cursor = src->cursor();
    const bool ok = ReadVarint64(&cursor, data);
    src->set_cursor(cursor);
    return ok;
  }
  return internal::ReadVarint64Slow(src, data);
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_READER_UTILS_H_
