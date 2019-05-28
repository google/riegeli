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

#include <stdint.h>

#include "absl/base/optimization.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/varint.h"

namespace riegeli {

bool ReadByte(Reader* src, uint8_t* data);

// {Read,Copy}Varint{32,64}() tolerate representations which are not the
// shortest. They reject representations longer than kMaxLengthVarint{32,64}
// bytes or with bits set outside the range of possible values.
//
// Warning: the proto library writes values of type int32 (not sint32) by
// casting them to uint64, not uint32 (negative values take 10 bytes, not 5),
// hence they must be read with ReadVarint64(), not ReadVarint32(), if negative
// values are possible.

bool ReadVarint32(Reader* src, uint32_t* data);
bool ReadVarint64(Reader* src, uint64_t* data);

// Variants which accept only the canonical representation, i.e. the shortest:
// rejecting a trailing zero byte, except for 0 itself.
bool ReadCanonicalVarint32(Reader* src, uint32_t* data);
bool ReadCanonicalVarint64(Reader* src, uint64_t* data);

// Returns the updated dest after the copied value, or nullptr on failure.
// At least kMaxLengthVarint32 bytes of space at dest[] must be available.
char* CopyVarint32(Reader* src, char* dest);
// Returns the updated dest after the copied value, or nullptr on failure.
// At least kMaxLengthVarint64 bytes of space at dest[] must be available.
char* CopyVarint64(Reader* src, char* dest);

// Low level variants which read from an array.

bool ReadVarint32(const char** src, const char* limit, uint32_t* data);
bool ReadVarint64(const char** src, const char* limit, uint64_t* data);

char* CopyVarint32(const char** src, const char* limit, char* dest);
char* CopyVarint64(const char** src, const char* limit, char* dest);

// Implementation details follow.

inline bool ReadByte(Reader* src, uint8_t* data) {
  if (ABSL_PREDICT_FALSE(!src->Pull())) return false;
  const char* cursor = src->cursor();
  *data = static_cast<uint8_t>(*cursor++);
  src->set_cursor(cursor);
  return true;
}

inline bool ReadVarint32(Reader* src, uint32_t* data) {
  src->Pull(kMaxLengthVarint32);
  const char* cursor = src->cursor();
  const bool ok = ReadVarint32(&cursor, src->limit(), data);
  src->set_cursor(cursor);
  return ok;
}

inline bool ReadVarint64(Reader* src, uint64_t* data) {
  src->Pull(kMaxLengthVarint64);
  const char* cursor = src->cursor();
  const bool ok = ReadVarint64(&cursor, src->limit(), data);
  src->set_cursor(cursor);
  return ok;
}

inline bool ReadCanonicalVarint32(Reader* src, uint32_t* data) {
  if (ABSL_PREDICT_FALSE(!src->Pull())) return false;
  const char* cursor = src->cursor();
  const uint8_t first_byte = static_cast<uint8_t>(*cursor++);
  if ((first_byte & 0x80) == 0) {
    // Any byte with the highest bit clear is accepted as the only byte,
    // including 0 itself.
    src->set_cursor(cursor);
    *data = first_byte;
    return true;
  }
  if (ABSL_PREDICT_FALSE(!ReadVarint32(src, data))) return false;
  RIEGELI_ASSERT_GT(src->read_from_buffer(), 0u)
      << "ReadCanonicalVarint32() relies on ReadVarint32() leaving the last "
         "byte in the buffer";
  if (ABSL_PREDICT_FALSE(src->cursor()[-1] == 0)) return false;
  return true;
}

inline bool ReadCanonicalVarint64(Reader* src, uint64_t* data) {
  if (ABSL_PREDICT_FALSE(!src->Pull())) return false;
  const char* cursor = src->cursor();
  const uint8_t first_byte = static_cast<uint8_t>(*cursor++);
  if ((first_byte & 0x80) == 0) {
    // Any byte with the highest bit clear is accepted as the only byte,
    // including 0 itself.
    src->set_cursor(cursor);
    *data = first_byte;
    return true;
  }
  if (ABSL_PREDICT_FALSE(!ReadVarint64(src, data))) return false;
  RIEGELI_ASSERT_GT(src->read_from_buffer(), 0u)
      << "ReadCanonicalVarint64() relies on ReadVarint64() leaving the last "
         "byte in the buffer";
  if (ABSL_PREDICT_FALSE(src->cursor()[-1] == 0)) return false;
  return true;
}

inline char* CopyVarint32(Reader* src, char* dest) {
  src->Pull(kMaxLengthVarint32);
  const char* cursor = src->cursor();
  dest = CopyVarint32(&cursor, src->limit(), dest);
  src->set_cursor(cursor);
  return dest;
}

inline char* CopyVarint64(Reader* src, char* dest) {
  src->Pull(kMaxLengthVarint64);
  const char* cursor = src->cursor();
  dest = CopyVarint64(&cursor, src->limit(), dest);
  src->set_cursor(cursor);
  return dest;
}

inline bool ReadVarint32(const char** src, const char* limit, uint32_t* data) {
  const char* cursor = *src;
  uint32_t acc = 0;
  int shift = 0;
  uint8_t byte;
  do {
    if (ABSL_PREDICT_FALSE(cursor == limit)) {
      *src = cursor;
      return false;
    }
    byte = static_cast<uint8_t>(*cursor++);
    acc |= (uint32_t{byte} & 0x7f) << shift;
    if (ABSL_PREDICT_FALSE(shift == (kMaxLengthVarint32 - 1) * 7)) {
      // Last possible byte.
      if (ABSL_PREDICT_FALSE(
              byte >= uint8_t{1} << (32 - (kMaxLengthVarint32 - 1) * 7))) {
        // The representation is longer than kMaxLengthVarint32
        // or the represented value does not fit in uint32_t.
        *src = cursor;
        return false;
      }
      break;
    }
    shift += 7;
  } while ((byte & 0x80) != 0);
  *src = cursor;
  *data = acc;
  return true;
}

inline bool ReadVarint64(const char** src, const char* limit, uint64_t* data) {
  const char* cursor = *src;
  uint64_t acc = 0;
  int shift = 0;
  uint8_t byte;
  do {
    if (ABSL_PREDICT_FALSE(cursor == limit)) {
      *src = cursor;
      return false;
    }
    byte = static_cast<uint8_t>(*cursor++);
    acc |= (uint64_t{byte} & 0x7f) << shift;
    if (ABSL_PREDICT_FALSE(shift == (kMaxLengthVarint64 - 1) * 7)) {
      // Last possible byte.
      if (ABSL_PREDICT_FALSE(
              byte >= uint8_t{1} << (64 - (kMaxLengthVarint64 - 1) * 7))) {
        // The representation is longer than kMaxLengthVarint64
        // or the represented value does not fit in uint64_t.
        *src = cursor;
        return false;
      }
      break;
    }
    shift += 7;
  } while ((byte & 0x80) != 0);
  *src = cursor;
  *data = acc;
  return true;
}

inline char* CopyVarint32(const char** src, const char* limit, char* dest) {
  const char* cursor = *src;
  int remaining = kMaxLengthVarint32;
  uint8_t byte;
  do {
    if (ABSL_PREDICT_FALSE(cursor == limit)) {
      *src = cursor;
      return nullptr;
    }
    byte = static_cast<uint8_t>(*cursor++);
    *dest++ = static_cast<char>(byte);
    if (ABSL_PREDICT_FALSE(--remaining == 0)) {
      // Last possible byte.
      if (ABSL_PREDICT_FALSE(
              byte >= uint8_t{1} << (32 - (kMaxLengthVarint32 - 1) * 7))) {
        // The representation is longer than kMaxLengthVarint32
        // or the represented value does not fit in uint32_t.
        *src = cursor;
        return nullptr;
      }
      break;
    }
  } while ((byte & 0x80) != 0);
  *src = cursor;
  return dest;
}

inline char* CopyVarint64(const char** src, const char* limit, char* dest) {
  const char* cursor = *src;
  int remaining = kMaxLengthVarint64;
  uint8_t byte;
  do {
    if (ABSL_PREDICT_FALSE(cursor == limit)) {
      *src = cursor;
      return nullptr;
    }
    byte = static_cast<uint8_t>(*cursor++);
    *dest++ = static_cast<char>(byte);
    if (ABSL_PREDICT_FALSE(--remaining == 0)) {
      // Last possible byte.
      if (ABSL_PREDICT_FALSE(
              byte >= uint8_t{1} << (64 - (kMaxLengthVarint64 - 1) * 7))) {
        // The representation is longer than kMaxLengthVarint64
        // or the represented value does not fit in uint64_t.
        *src = cursor;
        return nullptr;
      }
      break;
    }
  } while ((byte & 0x80) != 0);
  *src = cursor;
  return dest;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_READER_UTILS_H_
