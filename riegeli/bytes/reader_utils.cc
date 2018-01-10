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

#include "riegeli/bytes/reader_utils.h"

#include <stddef.h>
#include <stdint.h>

#include "riegeli/base/assert.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

namespace internal {

bool ContinueReadingVarint64(const char** src, uint64_t acc, uint64_t* data) {
  const char* cursor = *src;
  const uint64_t byte = static_cast<uint8_t>(*cursor++);
  acc += (byte - 1) << (7 * kMaxLengthVarint32());
  if (byte < 0x80) {
    *data = acc;
    *src = cursor;
    return true;
  }
  const bool ok =
      ReadVarint<uint64_t, kMaxLengthVarint32() + 2>(&cursor, &acc, data);
  *src = cursor;
  return ok;
}

// General case of compile time recursion.
template <typename Data, size_t min_length = 1>
bool ReadVarint(Reader* src, Data* acc, Data* data) {
  if (RIEGELI_UNLIKELY(!src->Pull())) return false;
  const char* cursor = src->cursor();
  const Data byte = static_cast<uint8_t>(*cursor++);
  src->set_cursor(cursor);
  *acc += (byte - 1) << (7 * (min_length - 1));
  if (byte < 0x80) {
    *data = *acc;
    return true;
  }
  return ReadVarint<Data, min_length + 1>(src, acc, data);
}

// Base case of compile time recursion for 32 bits.
template <>
inline bool ReadVarint<uint32_t, kMaxLengthVarint32()>(Reader* src,
                                                       uint32_t* acc,
                                                       uint32_t* data) {
  if (RIEGELI_UNLIKELY(!src->Pull())) return false;
  const char* cursor = src->cursor();
  const uint32_t byte = static_cast<uint8_t>(*cursor++);
  src->set_cursor(cursor);
  if (RIEGELI_UNLIKELY(byte >= 0x10)) return false;
  *acc += (byte - 1) << (7 * (kMaxLengthVarint32() - 1));
  *data = *acc;
  return true;
}

// Base case of compile time recursion for 64 bits.
template <>
inline bool ReadVarint<uint64_t, kMaxLengthVarint64()>(Reader* src,
                                                       uint64_t* acc,
                                                       uint64_t* data) {
  if (RIEGELI_UNLIKELY(!src->Pull())) return false;
  const char* cursor = src->cursor();
  const uint64_t byte = static_cast<uint8_t>(*cursor++);
  src->set_cursor(cursor);
  if (RIEGELI_UNLIKELY(byte >= 0x02)) return false;
  *acc += (byte - 1) << (7 * (kMaxLengthVarint64() - 1));
  *data = *acc;
  return true;
}

bool ReadVarint32Slow(Reader* src, uint32_t* data) {
  uint32_t acc = 1;
  return ReadVarint(src, &acc, data);
}

bool ReadVarint64Slow(Reader* src, uint64_t* data) {
  uint64_t acc = 1;
  return ReadVarint(src, &acc, data);
}

}  // namespace internal

bool ReadAll(Reader* src, string_view* dest, std::string* scratch) {
  Position size;
  if (src->Size(&size)) {
    RIEGELI_ASSERT_GE(size, src->pos());
    return src->Read(dest, scratch, size - src->pos());
  }
  scratch->clear();
  const bool ok = ReadAll(src, scratch);
  *dest = *scratch;
  return ok;
}

bool ReadAll(Reader* src, std::string* dest) {
  Position size;
  if (src->Size(&size)) {
    RIEGELI_ASSERT_GE(size, src->pos());
    return src->Read(dest, size - src->pos());
  }
  do {
    const size_t available_length = src->available();
    dest->append(src->cursor(), available_length);
    src->set_cursor(src->cursor() + available_length);
  } while (src->Pull());
  return src->healthy();
}

bool ReadAll(Reader* src, Chain* dest) {
  Position size;
  if (src->Size(&size)) {
    RIEGELI_ASSERT_GE(size, src->pos());
    return src->Read(dest, size - src->pos());
  }
  do {
    if (RIEGELI_UNLIKELY(!src->Read(dest, src->available()))) return false;
  } while (src->Pull());
  return src->healthy();
}

bool CopyAll(Reader* src, Writer* dest) {
  Position size;
  if (src->Size(&size)) {
    RIEGELI_ASSERT_GE(size, src->pos());
    return src->CopyTo(dest, size - src->pos());
  }
  do {
    if (RIEGELI_UNLIKELY(!src->CopyTo(dest, src->available()))) return false;
  } while (src->Pull());
  return src->healthy();
}

bool CopyAll(Reader* src, BackwardWriter* dest) {
  Position size;
  if (src->Size(&size)) {
    RIEGELI_ASSERT_GE(size, src->pos());
    return src->CopyTo(dest, size - src->pos());
  }
  Chain data;
  if (RIEGELI_UNLIKELY(!ReadAll(src, &data))) return false;
  return dest->Write(std::move(data));
}

}  // namespace riegeli
