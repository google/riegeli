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
#include <string>

#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/string_view.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/varint.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

bool ReadByte(Reader* src, uint8_t* data);

// Functions reading or copying a varint treat only canonical representations
// (i.e. without a trailing zero byte, except for 0 itself) as valid.
//
// Warning: the proto library writes values of type int32 (not sint32) by
// casting them to uint64, not uint32 (negative values take 10 bytes, not 5),
// hence they must be read with ReadVarint64(), not ReadVarint32(), if negative
// values are possible.

// At least kMaxLengthVarint32() bytes of data at src[] must be available.
bool ReadVarint32(const char** src, uint32_t* data);
// At least kMaxLengthVarint64() bytes of data at src[] must be available.
bool ReadVarint64(const char** src, uint64_t* data);

bool ReadVarint32(Reader* src, uint32_t* data);
bool ReadVarint64(Reader* src, uint64_t* data);

// Returns the updated dest after the copied value, or nullptr on failure.
// At least kMaxLengthVarint32() bytes of space at dest[] must be available.
char* CopyVarint32(Reader* src, char* dest);
// Returns the updated dest after the copied value, or nullptr on failure.
// At least kMaxLengthVarint64() bytes of space at dest[] must be available.
char* CopyVarint64(Reader* src, char* dest);

bool ReadAll(Reader* src, string_view* dest, std::string* scratch);
bool ReadAll(Reader* src, std::string* dest);
bool ReadAll(Reader* src, Chain* dest);
bool CopyAll(Reader* src, Writer* dest);
bool CopyAll(Reader* src, BackwardWriter* dest);

// Implementation details follow.

namespace internal {

bool ReadVarint32Slow(Reader* src, uint32_t* data);
bool ReadVarint64Slow(Reader* src, uint64_t* data);

char* CopyVarint32Slow(Reader* src, char* dest);
char* CopyVarint64Slow(Reader* src, char* dest);

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
  uint32_t acc = static_cast<uint8_t>(*cursor++);
  if (RIEGELI_UNLIKELY(acc >= 0x80)) {
    // More than a single byte.
    uint32_t byte;
    int shift = 0;
    do {
      byte = static_cast<uint8_t>(*cursor++);
      shift += 7;
      acc += (byte - 1) << shift;
      if (RIEGELI_UNLIKELY(shift == (kMaxLengthVarint32() - 1) * 7)) {
        // Last possible byte.
        if (RIEGELI_UNLIKELY(
                byte >= uint32_t{1} << (32 - (kMaxLengthVarint32() - 1) * 7))) {
          // Some bits are set outside of the range of possible values.
          *src = cursor;
          return false;
        }
        break;
      }
    } while (byte >= 0x80);
    if (RIEGELI_UNLIKELY(byte == 0)) {
      // Overlong representation.
      *src = cursor;
      return false;
    }
  }
  *src = cursor;
  *data = acc;
  return true;
}

inline bool ReadVarint64(const char** src, uint64_t* data) {
  const char* cursor = *src;
  uint64_t acc = static_cast<uint8_t>(*cursor++);
  if (RIEGELI_UNLIKELY(acc >= 0x80)) {
    // More than a single byte.
    uint64_t byte;
    int shift = 0;
    do {
      byte = static_cast<uint8_t>(*cursor++);
      shift += 7;
      acc += (byte - 1) << shift;
      if (RIEGELI_UNLIKELY(shift == (kMaxLengthVarint64() - 1) * 7)) {
        // Last possible byte.
        if (RIEGELI_UNLIKELY(
                byte >= uint64_t{1} << (64 - (kMaxLengthVarint64() - 1) * 7))) {
          // Some bits are set outside of the range of possible values.
          *src = cursor;
          return false;
        }
        break;
      }
    } while (byte >= 0x80);
    if (RIEGELI_UNLIKELY(byte == 0)) {
      // Overlong representation.
      *src = cursor;
      return false;
    }
  }
  *src = cursor;
  *data = acc;
  return true;
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

inline char* CopyVarint32(Reader* src, char* dest) {
  if (RIEGELI_LIKELY(src->available() >= kMaxLengthVarint32())) {
    const char* cursor = src->cursor();
    uint8_t byte = static_cast<uint8_t>(*cursor++);
    *dest++ = static_cast<char>(byte);
    if (RIEGELI_UNLIKELY(byte >= 0x80)) {
      // More than a single byte.
      int remaining = kMaxLengthVarint32() - 1;
      do {
        byte = static_cast<uint8_t>(*cursor++);
        *dest++ = static_cast<char>(byte);
        if (RIEGELI_UNLIKELY(--remaining == 0)) {
          // Last possible byte.
          if (RIEGELI_UNLIKELY(byte >=
                               uint32_t{1}
                                   << (32 - (kMaxLengthVarint32() - 1) * 7))) {
            // Some bits are set outside of the range of possible values.
            src->set_cursor(cursor);
            return nullptr;
          }
          break;
        }
      } while (byte >= 0x80);
      if (RIEGELI_UNLIKELY(byte == 0)) {
        // Overlong representation.
        src->set_cursor(cursor);
        return nullptr;
      }
    }
    src->set_cursor(cursor);
    return dest;
  }
  return internal::CopyVarint32Slow(src, dest);
}

inline char* CopyVarint64(Reader* src, char* dest) {
  if (RIEGELI_LIKELY(src->available() >= kMaxLengthVarint64())) {
    const char* cursor = src->cursor();
    uint8_t byte = static_cast<uint8_t>(*cursor++);
    *dest++ = static_cast<char>(byte);
    if (RIEGELI_UNLIKELY(byte >= 0x80)) {
      // More than a single byte.
      int remaining = kMaxLengthVarint64() - 1;
      do {
        byte = static_cast<uint8_t>(*cursor++);
        *dest++ = static_cast<char>(byte);
        if (RIEGELI_UNLIKELY(--remaining == 0)) {
          // Last possible byte.
          if (RIEGELI_UNLIKELY(byte >=
                               uint8_t{1}
                                   << (64 - (kMaxLengthVarint64() - 1) * 7))) {
            // Some bits are set outside of the range of possible values.
            src->set_cursor(cursor);
            return nullptr;
          }
          break;
        }
      } while (byte >= 0x80);
      if (RIEGELI_UNLIKELY(byte == 0)) {
        // Overlong representation.
        src->set_cursor(cursor);
        return nullptr;
      }
    }
    src->set_cursor(cursor);
    return dest;
  }
  return internal::CopyVarint64Slow(src, dest);
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_READER_UTILS_H_
