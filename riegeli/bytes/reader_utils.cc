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
#include <string>
#include <utility>

#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

namespace internal {

bool ReadVarint32Slow(Reader* src, uint32_t* data) {
  if (RIEGELI_UNLIKELY(!src->Pull())) return false;
  const char* cursor = src->cursor();
  uint32_t acc = static_cast<uint8_t>(*cursor++);
  src->set_cursor(cursor);
  if (RIEGELI_UNLIKELY(acc >= 0x80)) {
    // More than a single byte.
    uint32_t byte;
    int shift = 0;
    do {
      if (RIEGELI_UNLIKELY(!src->Pull())) return false;
      const char* cursor = src->cursor();
      byte = static_cast<uint8_t>(*cursor++);
      src->set_cursor(cursor);
      shift += 7;
      acc += (byte - 1) << shift;
      if (RIEGELI_UNLIKELY(shift == (kMaxLengthVarint32() - 1) * 7)) {
        // Last possible byte.
        if (RIEGELI_UNLIKELY(
                byte >= uint32_t{1} << (32 - (kMaxLengthVarint32() - 1) * 7))) {
          // Some bits are set outside of the range of possible values.
          return false;
        }
        break;
      }
    } while (byte >= 0x80);
    if (RIEGELI_UNLIKELY(byte == 0)) {
      // Overlong representation.
      return false;
    }
  }
  *data = acc;
  return true;
}

bool ReadVarint64Slow(Reader* src, uint64_t* data) {
  if (RIEGELI_UNLIKELY(!src->Pull())) return false;
  const char* cursor = src->cursor();
  uint64_t acc = static_cast<uint8_t>(*cursor++);
  src->set_cursor(cursor);
  if (RIEGELI_UNLIKELY(acc >= 0x80)) {
    // More than a single byte.
    uint64_t byte;
    int shift = 0;
    do {
      if (RIEGELI_UNLIKELY(!src->Pull())) return false;
      const char* cursor = src->cursor();
      byte = static_cast<uint8_t>(*cursor++);
      src->set_cursor(cursor);
      shift += 7;
      acc += (byte - 1) << shift;
      if (RIEGELI_UNLIKELY(shift == (kMaxLengthVarint64() - 1) * 7)) {
        // Last possible byte.
        if (RIEGELI_UNLIKELY(
                byte >= uint64_t{1} << (64 - (kMaxLengthVarint64() - 1) * 7))) {
          // Some bits are set outside of the range of possible values.
          return false;
        }
        break;
      }
    } while (byte >= 0x80);
    if (RIEGELI_UNLIKELY(byte == 0)) {
      // Overlong representation.
      return false;
    }
  }
  *data = acc;
  return true;
}

char* CopyVarint32Slow(Reader* src, char* dest) {
  if (RIEGELI_UNLIKELY(!src->Pull())) return nullptr;
  const char* cursor = src->cursor();
  uint8_t byte = static_cast<uint8_t>(*cursor++);
  src->set_cursor(cursor);
  *dest++ = static_cast<char>(byte);
  if (RIEGELI_UNLIKELY(byte >= 0x80)) {
    // More than a single byte.
    int remaining = kMaxLengthVarint32() - 1;
    do {
      if (RIEGELI_UNLIKELY(!src->Pull())) return nullptr;
      const char* cursor = src->cursor();
      byte = static_cast<uint8_t>(*cursor++);
      src->set_cursor(cursor);
      *dest++ = static_cast<char>(byte);
      if (RIEGELI_UNLIKELY(--remaining == 0)) {
        // Last possible byte.
        if (RIEGELI_UNLIKELY(
                byte >= uint8_t{1} << (32 - (kMaxLengthVarint32() - 1) * 7))) {
          // Some bits are set outside of the range of possible values.
          return nullptr;
        }
        break;
      }
    } while (byte >= 0x80);
    if (RIEGELI_UNLIKELY(byte == 0)) {
      // Overlong representation.
      return nullptr;
    }
  }
  return dest;
}

char* CopyVarint64Slow(Reader* src, char* dest) {
  if (RIEGELI_UNLIKELY(!src->Pull())) return nullptr;
  const char* cursor = src->cursor();
  uint8_t byte = static_cast<uint8_t>(*cursor++);
  src->set_cursor(cursor);
  *dest++ = static_cast<char>(byte);
  if (RIEGELI_UNLIKELY(byte >= 0x80)) {
    // More than a single byte.
    int remaining = kMaxLengthVarint64() - 1;
    do {
      if (RIEGELI_UNLIKELY(!src->Pull())) return nullptr;
      const char* cursor = src->cursor();
      byte = static_cast<uint8_t>(*cursor++);
      src->set_cursor(cursor);
      *dest++ = static_cast<char>(byte);
      if (RIEGELI_UNLIKELY(--remaining == 0)) {
        // Last possible byte.
        if (RIEGELI_UNLIKELY(
                byte >= uint8_t{1} << (64 - (kMaxLengthVarint64() - 1) * 7))) {
          // Some bits are set outside of the range of possible values.
          return nullptr;
        }
        break;
      }
    } while (byte >= 0x80);
    if (RIEGELI_UNLIKELY(byte == 0)) {
      // Overlong representation.
      return nullptr;
    }
  }
  return dest;
}

}  // namespace internal

bool ReadAll(Reader* src, absl::string_view* dest, std::string* scratch) {
  Position size;
  if (src->Size(&size)) {
    RIEGELI_ASSERT_LE(src->pos(), size)
        << "Current position is greater than the source size";
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
    RIEGELI_ASSERT_LE(src->pos(), size)
        << "Current position is greater than the source size";
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
    RIEGELI_ASSERT_LE(src->pos(), size)
        << "Current position is greater than the source size";
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
    RIEGELI_ASSERT_LE(src->pos(), size)
        << "Current position is greater than the source size";
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
    RIEGELI_ASSERT_LE(src->pos(), size)
        << "Current position is greater than the source size";
    return src->CopyTo(dest, size - src->pos());
  }
  Chain data;
  if (RIEGELI_UNLIKELY(!ReadAll(src, &data))) return false;
  return dest->Write(std::move(data));
}

}  // namespace riegeli
