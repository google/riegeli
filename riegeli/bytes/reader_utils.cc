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

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

namespace internal {

bool ReadVarint32Slow(Reader* src, uint32_t* data) {
  uint32_t acc = 0;
  int shift = 0;
  uint8_t byte;
  do {
    if (ABSL_PREDICT_FALSE(!ReadByte(src, &byte))) return false;
    acc |= (uint32_t{byte} & 0x7f) << shift;
    if (ABSL_PREDICT_FALSE(shift == (kMaxLengthVarint32() - 1) * 7)) {
      // Last possible byte.
      if (ABSL_PREDICT_FALSE(
              byte >= uint8_t{1} << (32 - (kMaxLengthVarint32() - 1) * 7))) {
        // The representation is longer than kMaxLengthVarint32()
        // or the represented value does not fit in uint32_t.
        return false;
      }
      break;
    }
    shift += 7;
  } while ((byte & 0x80) != 0);
  *data = acc;
  return true;
}

bool ReadVarint64Slow(Reader* src, uint64_t* data) {
  uint64_t acc = 0;
  int shift = 0;
  uint8_t byte;
  do {
    if (ABSL_PREDICT_FALSE(!ReadByte(src, &byte))) return false;
    acc |= (uint64_t{byte} & 0x7f) << shift;
    if (ABSL_PREDICT_FALSE(shift == (kMaxLengthVarint64() - 1) * 7)) {
      // Last possible byte.
      if (ABSL_PREDICT_FALSE(
              byte >= uint8_t{1} << (64 - (kMaxLengthVarint64() - 1) * 7))) {
        // The representation is longer than kMaxLengthVarint64()
        // or the represented value does not fit in uint64_t.
        return false;
      }
      break;
    }
    shift += 7;
  } while ((byte & 0x80) != 0);
  *data = acc;
  return true;
}

char* CopyVarint32Slow(Reader* src, char* dest) {
  int remaining = kMaxLengthVarint32();
  uint8_t byte;
  do {
    if (ABSL_PREDICT_FALSE(!ReadByte(src, &byte))) return nullptr;
    *dest++ = static_cast<char>(byte);
    if (ABSL_PREDICT_FALSE(--remaining == 0)) {
      // Last possible byte.
      if (ABSL_PREDICT_FALSE(
              byte >= uint8_t{1} << (32 - (kMaxLengthVarint32() - 1) * 7))) {
        // The representation is longer than kMaxLengthVarint32()
        // or the represented value does not fit in uint32_t.
        return nullptr;
      }
      break;
    }
  } while ((byte & 0x80) != 0);
  return dest;
}

char* CopyVarint64Slow(Reader* src, char* dest) {
  int remaining = kMaxLengthVarint64();
  uint8_t byte;
  do {
    if (ABSL_PREDICT_FALSE(!ReadByte(src, &byte))) return nullptr;
    *dest++ = static_cast<char>(byte);
    if (ABSL_PREDICT_FALSE(--remaining == 0)) {
      // Last possible byte.
      if (ABSL_PREDICT_FALSE(
              byte >= uint8_t{1} << (64 - (kMaxLengthVarint64() - 1) * 7))) {
        // The representation is longer than kMaxLengthVarint64()
        // or the represented value does not fit in uint64_t.
        return nullptr;
      }
      break;
    }
  } while ((byte & 0x80) != 0);
  return dest;
}

}  // namespace internal

bool ReadAll(Reader* src, absl::string_view* dest, std::string* scratch) {
  if (src->SupportsRandomAccess()) {
    Position size;
    if (ABSL_PREDICT_FALSE(!src->Size(&size))) return false;
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
  if (src->SupportsRandomAccess()) {
    Position size;
    if (ABSL_PREDICT_FALSE(!src->Size(&size))) return false;
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
  if (src->SupportsRandomAccess()) {
    Position size;
    if (ABSL_PREDICT_FALSE(!src->Size(&size))) return false;
    RIEGELI_ASSERT_LE(src->pos(), size)
        << "Current position is greater than the source size";
    return src->Read(dest, size - src->pos());
  }
  do {
    if (ABSL_PREDICT_FALSE(!src->Read(dest, src->available()))) return false;
  } while (src->Pull());
  return src->healthy();
}

bool CopyAll(Reader* src, Writer* dest) {
  if (src->SupportsRandomAccess()) {
    Position size;
    if (ABSL_PREDICT_FALSE(!src->Size(&size))) return false;
    RIEGELI_ASSERT_LE(src->pos(), size)
        << "Current position is greater than the source size";
    return src->CopyTo(dest, size - src->pos());
  }
  do {
    if (ABSL_PREDICT_FALSE(!src->CopyTo(dest, src->available()))) return false;
  } while (src->Pull());
  return src->healthy();
}

bool CopyAll(Reader* src, BackwardWriter* dest) {
  if (src->SupportsRandomAccess()) {
    Position size;
    if (ABSL_PREDICT_FALSE(!src->Size(&size))) return false;
    RIEGELI_ASSERT_LE(src->pos(), size)
        << "Current position is greater than the source size";
    return src->CopyTo(dest, size - src->pos());
  }
  Chain data;
  if (ABSL_PREDICT_FALSE(!ReadAll(src, &data))) return false;
  return dest->Write(std::move(data));
}

}  // namespace riegeli
