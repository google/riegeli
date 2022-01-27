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

#include "riegeli/ordered_varint/ordered_varint_writing.h"

#include <stdint.h>

#include "absl/base/optimization.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/endian/endian_writing.h"

namespace riegeli {
namespace ordered_varint_internal {

bool WriteOrderedVarint32Slow(uint32_t data, Writer& dest) {
  RIEGELI_ASSERT_GE(data, uint32_t{1} << 7)
      << "Failed precondition of WriteOrderedVarint32Slow(): data too small";
  if (data < uint32_t{1} << (2 * 7)) {
    if (ABSL_PREDICT_FALSE(!dest.Push(2))) return false;
    WriteBigEndian16(IntCast<uint16_t>(data) | uint16_t{0x80} << 8,
                     dest.cursor());
    dest.move_cursor(2);
    return true;
  } else if (data < uint32_t{1} << (3 * 7)) {
    if (ABSL_PREDICT_FALSE(!dest.Push(3))) return false;
    dest.cursor()[0] = static_cast<char>(static_cast<uint8_t>(data >> (2 * 8)) |
                                         uint8_t{0xc0});
    WriteBigEndian16(static_cast<uint16_t>(data), dest.cursor() + 1);
    dest.move_cursor(3);
    return true;
  } else if (data < uint32_t{1} << (4 * 7)) {
    if (ABSL_PREDICT_FALSE(!dest.Push(4))) return false;
    WriteBigEndian32(data | uint32_t{0xe0} << (3 * 8), dest.cursor());
    dest.move_cursor(4);
    return true;
  } else {
    if (ABSL_PREDICT_FALSE(!dest.Push(5))) return false;
    dest.cursor()[0] = static_cast<char>(0xf0);
    WriteBigEndian32(data, dest.cursor() + 1);
    dest.move_cursor(5);
    return true;
  }
}

bool WriteOrderedVarint64Slow(uint64_t data, Writer& dest) {
  RIEGELI_ASSERT_GE(data, uint64_t{1} << 7)
      << "Failed precondition of WriteOrderedVarint64Slow(): data too small";
  if (data < uint64_t{1} << (2 * 7)) {
    if (ABSL_PREDICT_FALSE(!dest.Push(2))) return false;
    WriteBigEndian16(IntCast<uint16_t>(data) | uint16_t{0x80} << 8,
                     dest.cursor());
    dest.move_cursor(2);
    return true;
  } else if (data < uint64_t{1} << (3 * 7)) {
    if (ABSL_PREDICT_FALSE(!dest.Push(3))) return false;
    dest.cursor()[0] = static_cast<char>(static_cast<uint8_t>(data >> (2 * 8)) |
                                         uint8_t{0xc0});
    WriteBigEndian16(static_cast<uint16_t>(data), dest.cursor() + 1);
    dest.move_cursor(3);
    return true;
  } else if (data < uint64_t{1} << (4 * 7)) {
    if (ABSL_PREDICT_FALSE(!dest.Push(4))) return false;
    WriteBigEndian32(IntCast<uint32_t>(data) | uint32_t{0xe0} << (3 * 8),
                     dest.cursor());
    dest.move_cursor(4);
    return true;
  } else if (data < uint64_t{1} << (5 * 7)) {
    if (ABSL_PREDICT_FALSE(!dest.Push(5))) return false;
    dest.cursor()[0] = static_cast<char>(static_cast<uint8_t>(data >> (4 * 8)) |
                                         uint8_t{0xf0});
    WriteBigEndian32(static_cast<uint32_t>(data), dest.cursor() + 1);
    dest.move_cursor(5);
    return true;
  } else if (data < uint64_t{1} << (6 * 7)) {
    if (ABSL_PREDICT_FALSE(!dest.Push(6))) return false;
    WriteBigEndian16(
        static_cast<uint16_t>(data >> (4 * 8)) | uint16_t{0xf8} << 8,
        dest.cursor());
    WriteBigEndian32(static_cast<uint32_t>(data), dest.cursor() + 2);
    dest.move_cursor(6);
    return true;
  } else if (data < uint64_t{1} << (7 * 7)) {
    if (ABSL_PREDICT_FALSE(!dest.Push(7))) return false;
    WriteBigEndian32(
        static_cast<uint32_t>(data >> (3 * 8)) | uint32_t{0xfc} << (3 * 8),
        dest.cursor());
    WriteBigEndian32(static_cast<uint32_t>(data), dest.cursor() + 3);
    dest.move_cursor(7);
    return true;
  } else if (data < uint64_t{1} << (8 * 7)) {
    if (ABSL_PREDICT_FALSE(!dest.Push(8))) return false;
    WriteBigEndian64(data | uint64_t{0xfe} << (7 * 8), dest.cursor());
    dest.move_cursor(8);
    return true;
  } else {
    if (ABSL_PREDICT_FALSE(!dest.Push(9))) return false;
    dest.cursor()[0] = static_cast<char>(0xff);
    WriteBigEndian64(data, dest.cursor() + 1);
    dest.move_cursor(9);
    return true;
  }
}

}  // namespace ordered_varint_internal
}  // namespace riegeli
