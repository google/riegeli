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

#ifndef RIEGELI_CHUNK_ENCODING_CANONICAL_VARINT_H_
#define RIEGELI_CHUNK_ENCODING_CANONICAL_VARINT_H_

#include <stddef.h>
#include <stdint.h>

#include "riegeli/base/base.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/reader_utils.h"

namespace riegeli {

bool ReadCanonicalVarint32(Reader* src, uint32_t* data);

bool VerifyCanonicalVarint64(Reader* src);

// Returns the updated dest after the copied value, or nullptr on failure.
char* CopyCanonicalVarint64(Reader* src, char* dest);

// Implementation details follow.

namespace internal {

bool ReadCanonicalVarint32Slow(Reader* src, uint32_t* data);

// General case of compile time recursion.
template <size_t min_length>
bool ReadCanonicalVarint32(const char** src, uint32_t* acc, uint32_t* data) {
  const uint32_t byte = static_cast<uint8_t>(*(*src)++);
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
inline bool ReadCanonicalVarint32<kMaxLengthVarint32()>(const char** src,
                                                        uint32_t* acc,
                                                        uint32_t* data) {
  const uint32_t byte = static_cast<uint8_t>(*(*src)++);
  if (RIEGELI_UNLIKELY(byte >= 0x10)) return false;
  if (RIEGELI_UNLIKELY(byte == 0)) return false;
  *acc += (byte - 1) << (7 * (kMaxLengthVarint32() - 1));
  *data = *acc;
  return true;
}

bool ContinueVerifyingCanonicalVarint64(const char** src);

// General case of compile time recursion.
template <size_t min_length>
bool VerifyCanonicalVarint64(const char** src) {
  const uint8_t byte = static_cast<uint8_t>(*(*src)++);
  if (byte < 0x80) {
    if (min_length > 1 && RIEGELI_UNLIKELY(byte == 0)) return false;
    return true;
  }
  return VerifyCanonicalVarint64<min_length + 1>(src);
}

// Base case of compile time recursion: continue with longer numbers in a
// separately compiled function.
template <>
inline bool VerifyCanonicalVarint64<kMaxLengthVarint32() + 1>(
    const char** src) {
  return ContinueVerifyingCanonicalVarint64(src);
}

// Another base case of compile time recursion, for the case when
// VerifyCanonicalVarint64() is called from ContinueVerifyingCanonicalVarint64()
// with a larger template argument.
template <>
inline bool VerifyCanonicalVarint64<kMaxLengthVarint64()>(const char** src) {
  const uint8_t byte = static_cast<uint8_t>(*(*src)++);
  if (RIEGELI_UNLIKELY(byte >= 0x02)) return false;
  if (RIEGELI_UNLIKELY(byte == 0)) return false;
  return true;
}

bool VerifyCanonicalVarint64Slow(Reader* src);

char* ContinueCopyingCanonicalVarint64(const char** src, char* dest);

// General case of compile time recursion.
template <size_t min_length>
char* CopyCanonicalVarint64(const char** src, char* dest) {
  const uint8_t byte = static_cast<uint8_t>(*(*src)++);
  *dest++ = byte;
  if (byte < 0x80) {
    if (min_length > 1 && RIEGELI_UNLIKELY(byte == 0)) return nullptr;
    return dest;
  }
  return CopyCanonicalVarint64<min_length + 1>(src, dest);
}

// Base case of compile time recursion: continue with longer numbers in a
// separately compiled function.
template <>
inline char* CopyCanonicalVarint64<kMaxLengthVarint32() + 1>(const char** src,
                                                             char* dest) {
  return ContinueCopyingCanonicalVarint64(src, dest);
}

// Another base case of compile time recursion, for the case when
// CopyCanonicalVarint64() is called from ContinueCopyingCanonicalVarint64()
// with a larger template argument.
template <>
inline char* CopyCanonicalVarint64<kMaxLengthVarint64()>(const char** src,
                                                         char* dest) {
  const uint8_t byte = static_cast<uint8_t>(*(*src)++);
  if (RIEGELI_UNLIKELY(byte >= 0x02)) return nullptr;
  if (RIEGELI_UNLIKELY(byte == 0)) return nullptr;
  *dest++ = byte;
  return dest;
}

char* CopyCanonicalVarint64Slow(Reader* src, char* dest);

}  // namespace internal

inline bool ReadCanonicalVarint32(Reader* src, uint32_t* data) {
  if (RIEGELI_LIKELY(src->available() >= kMaxLengthVarint32())) {
    const char* cursor = src->cursor();
    uint32_t acc = 1;
    bool ok = internal::ReadCanonicalVarint32<1>(&cursor, &acc, data);
    src->set_cursor(cursor);
    return ok;
  }
  return internal::ReadCanonicalVarint32Slow(src, data);
}

inline bool VerifyCanonicalVarint64(Reader* src) {
  if (RIEGELI_LIKELY(src->available() >= kMaxLengthVarint64())) {
    const char* cursor = src->cursor();
    const bool ok = internal::VerifyCanonicalVarint64<1>(&cursor);
    src->set_cursor(cursor);
    return ok;
  }
  return internal::VerifyCanonicalVarint64Slow(src);
}

inline char* CopyCanonicalVarint64(Reader* src, char* dest) {
  if (RIEGELI_LIKELY(src->available() >= kMaxLengthVarint64())) {
    const char* cursor = src->cursor();
    dest = internal::CopyCanonicalVarint64<1>(&cursor, dest);
    src->set_cursor(cursor);
    return dest;
  }
  return internal::CopyCanonicalVarint64Slow(src, dest);
}

}  // namespace riegeli

#endif  // RIEGELI_CHUNK_ENCODING_CANONICAL_VARINT_H_
