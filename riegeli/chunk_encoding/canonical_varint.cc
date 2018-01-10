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

#include "riegeli/chunk_encoding/canonical_varint.h"

#include <stddef.h>
#include <stdint.h>

#include "riegeli/base/base.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/reader_utils.h"

namespace riegeli {
namespace internal {

// General case of compile time recursion.
template <size_t min_length>
bool ReadCanonicalVarint32(Reader* src, uint32_t* acc, uint32_t* data) {
  if (RIEGELI_UNLIKELY(!src->Pull())) return false;
  const char* cursor = src->cursor();
  const uint32_t byte = static_cast<uint8_t>(*cursor++);
  src->set_cursor(cursor);
  *acc += (byte - 1) << (7 * (min_length - 1));
  if (byte < 0x80) {
    if (min_length > 1 && RIEGELI_UNLIKELY(byte == 0)) return false;
    *data = *acc;
    return true;
  }
  return ReadCanonicalVarint32<min_length + 1>(src, acc, data);
}

// Base case of compile time recursion.
template <>
bool ReadCanonicalVarint32<kMaxLengthVarint32()>(Reader* src, uint32_t* acc,
                                                 uint32_t* data) {
  if (RIEGELI_UNLIKELY(!src->Pull())) return false;
  const char* cursor = src->cursor();
  const uint32_t byte = static_cast<uint8_t>(*cursor++);
  src->set_cursor(cursor);
  if (RIEGELI_UNLIKELY(byte >= 0x10)) return false;
  if (RIEGELI_UNLIKELY(byte == 0)) return false;
  *acc += (byte - 1) << (7 * (kMaxLengthVarint32() - 1));
  *data = *acc;
  return true;
}

bool ReadCanonicalVarint32Slow(Reader* src, uint32_t* data) {
  uint32_t acc = 1;
  return ReadCanonicalVarint32<1>(src, &acc, data);
}

bool ContinueVerifyingCanonicalVarint64(const char** src) {
  const char* cursor = *src;
  const uint8_t byte = static_cast<uint8_t>(*cursor++);
  if (byte < 0x80) {
    *src = cursor;
    if (RIEGELI_UNLIKELY(byte == 0)) return false;
    return true;
  }
  const bool ok = VerifyCanonicalVarint64<kMaxLengthVarint32() + 2>(&cursor);
  *src = cursor;
  return ok;
}

// General case of compile time recursion.
template <size_t min_length>
bool VerifyCanonicalVarint64(Reader* src) {
  if (RIEGELI_UNLIKELY(!src->Pull())) return false;
  const char* cursor = src->cursor();
  const uint8_t byte = static_cast<uint8_t>(*cursor++);
  src->set_cursor(cursor);
  if (byte < 0x80) {
    if (min_length > 1 && RIEGELI_UNLIKELY(byte == 0)) return false;
    return true;
  }
  return VerifyCanonicalVarint64<min_length + 1>(src);
}

// Base case of compile time recursion.
template <>
bool VerifyCanonicalVarint64<kMaxLengthVarint64()>(Reader* src) {
  if (RIEGELI_UNLIKELY(!src->Pull())) return false;
  const char* cursor = src->cursor();
  const uint8_t byte = static_cast<uint8_t>(*cursor++);
  src->set_cursor(cursor);
  if (RIEGELI_UNLIKELY(byte >= 0x02)) return false;
  if (RIEGELI_UNLIKELY(byte == 0)) return false;
  return true;
}

bool VerifyCanonicalVarint64Slow(Reader* src) {
  return VerifyCanonicalVarint64<1>(src);
}

char* ContinueCopyingCanonicalVarint64(const char** src, char* dest) {
  const char* cursor = *src;
  const uint8_t byte = static_cast<uint8_t>(*cursor++);
  *dest++ = byte;
  if (byte < 0x80) {
    *src = cursor;
    if (RIEGELI_UNLIKELY(byte == 0)) return nullptr;
    return dest;
  }
  dest = CopyCanonicalVarint64<kMaxLengthVarint32() + 2>(&cursor, dest);
  *src = cursor;
  return dest;
}

// General case of compile time recursion.
template <size_t min_length>
char* CopyCanonicalVarint64(Reader* src, char* dest) {
  if (RIEGELI_UNLIKELY(!src->Pull())) return nullptr;
  const char* cursor = src->cursor();
  const uint8_t byte = static_cast<uint8_t>(*cursor++);
  src->set_cursor(cursor);
  *dest++ = byte;
  if (byte < 0x80) {
    if (min_length > 1 && RIEGELI_UNLIKELY(byte == 0)) return nullptr;
    return dest;
  }
  return CopyCanonicalVarint64<min_length + 1>(src, dest);
}

// Base case of compile time recursion.
template <>
char* CopyCanonicalVarint64<kMaxLengthVarint64()>(Reader* src, char* dest) {
  if (RIEGELI_UNLIKELY(!src->Pull())) return nullptr;
  const char* cursor = src->cursor();
  const uint8_t byte = static_cast<uint8_t>(*cursor++);
  src->set_cursor(cursor);
  if (RIEGELI_UNLIKELY(byte >= 0x02)) return nullptr;
  if (RIEGELI_UNLIKELY(byte == 0)) return nullptr;
  *dest++ = byte;
  return dest;
}

char* CopyCanonicalVarint64Slow(Reader* src, char* dest) {
  return CopyCanonicalVarint64<1>(src, dest);
}

}  // namespace internal
}  // namespace riegeli
