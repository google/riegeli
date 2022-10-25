// Copyright 2021 Google LLC
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

#include "riegeli/ordered_varint/ordered_varint_reading.h"

#include <stdint.h>

#include "absl/base/optimization.h"
#include "riegeli/base/assert.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/endian/endian_reading.h"

namespace riegeli {
namespace ordered_varint_internal {

bool ReadOrderedVarint32Slow(Reader& src, uint32_t& dest) {
  RIEGELI_ASSERT_GT(src.available(), 0u)
      << "Failed precondition of ReadOrderedVarint32Slow(): no data available";
  const uint8_t first_byte = static_cast<uint8_t>(*src.cursor());
  RIEGELI_ASSERT_GE(first_byte, 0x80)
      << "Failed precondition of ReadOrderedVarint32Slow(): length is 1";
  if (first_byte < 0xc0) {
    if (ABSL_PREDICT_FALSE(!src.Pull(2))) return false;
    dest = ReadBigEndian16(src.cursor()) & ~(uint16_t{0x80} << 8);
    if (ABSL_PREDICT_FALSE(dest < uint32_t{1} << 7)) return false;
    src.move_cursor(2);
    return true;
  } else if (first_byte < 0xe0) {
    if (ABSL_PREDICT_FALSE(!src.Pull(3))) return false;
    dest = (static_cast<uint32_t>(static_cast<uint8_t>(src.cursor()[0]) &
                                  ~uint8_t{0xc0})
            << (2 * 8)) |
           ReadBigEndian16(src.cursor() + 1);
    if (ABSL_PREDICT_FALSE(dest < uint32_t{1} << (2 * 7))) return false;
    src.move_cursor(3);
    return true;
  } else if (first_byte < 0xf0) {
    if (ABSL_PREDICT_FALSE(!src.Pull(4))) return false;
    dest = ReadBigEndian32(src.cursor()) & ~(uint32_t{0xe0} << (3 * 8));
    if (ABSL_PREDICT_FALSE(dest < uint32_t{1} << (3 * 7))) return false;
    src.move_cursor(4);
    return true;
  } else {
    if (ABSL_PREDICT_FALSE(first_byte > 0xf0)) return false;
    if (ABSL_PREDICT_FALSE(!src.Pull(5))) return false;
    dest = ReadBigEndian32(src.cursor() + 1);
    if (ABSL_PREDICT_FALSE(dest < uint32_t{1} << (4 * 7))) return false;
    src.move_cursor(5);
    return true;
  }
}

bool ReadOrderedVarint64Slow(Reader& src, uint64_t& dest) {
  RIEGELI_ASSERT_GT(src.available(), 0u)
      << "Failed precondition of ReadOrderedVarint64Slow(): no data available";
  const uint8_t first_byte = static_cast<uint8_t>(*src.cursor());
  RIEGELI_ASSERT_GE(first_byte, 0x80)
      << "Failed precondition of ReadOrderedVarint64Slow(): length is 1";
  if (first_byte < 0xc0) {
    if (ABSL_PREDICT_FALSE(!src.Pull(2))) return false;
    dest = ReadBigEndian16(src.cursor()) & ~(uint16_t{0x80} << 8);
    if (ABSL_PREDICT_FALSE(dest < uint32_t{1} << 7)) return false;
    src.move_cursor(2);
    return true;
  } else if (first_byte < 0xe0) {
    if (ABSL_PREDICT_FALSE(!src.Pull(3))) return false;
    dest = (static_cast<uint32_t>(static_cast<uint8_t>(src.cursor()[0]) &
                                  ~uint8_t{0xc0})
            << (2 * 8)) |
           ReadBigEndian16(src.cursor() + 1);
    if (ABSL_PREDICT_FALSE(dest < uint32_t{1} << (2 * 7))) return false;
    src.move_cursor(3);
    return true;
  } else if (first_byte < 0xf0) {
    if (ABSL_PREDICT_FALSE(!src.Pull(4))) return false;
    dest = ReadBigEndian32(src.cursor()) & ~(uint32_t{0xe0} << (3 * 8));
    if (ABSL_PREDICT_FALSE(dest < uint32_t{1} << (3 * 7))) return false;
    src.move_cursor(4);
    return true;
  } else if (first_byte < 0xf8) {
    if (ABSL_PREDICT_FALSE(!src.Pull(5))) return false;
    dest = (static_cast<uint64_t>(static_cast<uint8_t>(src.cursor()[0]) &
                                  ~uint8_t{0xf0})
            << (4 * 8)) |
           ReadBigEndian32(src.cursor() + 1);
    if (ABSL_PREDICT_FALSE(dest < uint64_t{1} << (4 * 7))) return false;
    src.move_cursor(5);
    return true;
  } else if (first_byte < 0xfc) {
    if (ABSL_PREDICT_FALSE(!src.Pull(6))) return false;
    dest = (static_cast<uint64_t>(ReadBigEndian16(src.cursor()) &
                                  ~(uint16_t{0xf8} << 8))
            << (4 * 8)) |
           ReadBigEndian32(src.cursor() + 2);
    if (ABSL_PREDICT_FALSE(dest < uint64_t{1} << (5 * 7))) return false;
    src.move_cursor(6);
    return true;
  } else if (first_byte < 0xfe) {
    if (ABSL_PREDICT_FALSE(!src.Pull(7))) return false;
    dest = (static_cast<uint64_t>(ReadBigEndian32(src.cursor()) &
                                  ~(uint32_t{0xfc} << (3 * 8)))
            << (3 * 8)) |
           ReadBigEndian32(src.cursor() + 3);
    if (ABSL_PREDICT_FALSE(dest < uint64_t{1} << (6 * 7))) return false;
    src.move_cursor(7);
    return true;
  } else if (first_byte < 0xff) {
    if (ABSL_PREDICT_FALSE(!src.Pull(8))) return false;
    dest = ReadBigEndian64(src.cursor()) & ~(uint64_t{0xfe} << (7 * 8));
    if (ABSL_PREDICT_FALSE(dest < uint64_t{1} << (7 * 7))) return false;
    src.move_cursor(8);
    return true;
  } else {
    if (ABSL_PREDICT_FALSE(!src.Pull(9))) return false;
    dest = ReadBigEndian64(src.cursor() + 1);
    if (ABSL_PREDICT_FALSE(dest < uint64_t{1} << (8 * 7))) return false;
    src.move_cursor(9);
    return true;
  }
}

}  // namespace ordered_varint_internal
}  // namespace riegeli
