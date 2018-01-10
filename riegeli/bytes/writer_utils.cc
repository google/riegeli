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

#include "riegeli/bytes/writer_utils.h"

#include <stddef.h>
#include <stdint.h>
#include <cstring>

#include "riegeli/base/assert.h"
#include "riegeli/base/base.h"
#include "riegeli/base/string_view.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {
namespace internal {

size_t LengthBigVarint64(uint64_t data) {
  RIEGELI_ASSERT_GE(data, uint64_t{1} << (7 * kMaxLengthVarint32()));
  return kMaxLengthVarint32() + LengthVarint(static_cast<uint32_t>(
                                    data >> (7 * kMaxLengthVarint32())));
}

char* ContinueWritingVarint64(char* dest, uint64_t data) {
  RIEGELI_ASSERT_GE(data, uint64_t{0x80});
  RIEGELI_ASSERT_LT(data, uint64_t{1} << (64 - 7 * (kMaxLengthVarint32() - 1)));
  *dest++ = static_cast<char>(data | 0x80);
  return WriteVarint(dest, static_cast<uint32_t>(data >> 7));
}

bool WriteVarint32Slow(Writer* dest, uint32_t data) {
  char buffer[kMaxLengthVarint32()];
  char* const end = WriteVarint32(buffer, data);
  return dest->Write(string_view(buffer, end - buffer));
}

bool WriteVarint64Slow(Writer* dest, uint64_t data) {
  char buffer[kMaxLengthVarint64()];
  char* const end = WriteVarint64(buffer, data);
  return dest->Write(string_view(buffer, end - buffer));
}

bool WriteZerosSlow(Writer* dest, Position length) {
  RIEGELI_ASSERT_GT(length, dest->available());
  if (dest->available() == 0) {  // memset(nullptr, _, 0) is undefined.
    goto skip_copy;
  }
  do {
    {
      const size_t available_length = dest->available();
      std::memset(dest->cursor(), 0, available_length);
      dest->set_cursor(dest->limit());
      length -= available_length;
    }
  skip_copy:
    if (RIEGELI_UNLIKELY(!dest->Push())) return false;
  } while (length > dest->available());
  std::memset(dest->cursor(), 0, length);
  dest->set_cursor(dest->cursor() + length);
  return true;
}

}  // namespace internal
}  // namespace riegeli
