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

#ifndef RIEGELI_VARINT_VARINT_WRITING_H_
#define RIEGELI_VARINT_VARINT_WRITING_H_

#include <stddef.h>
#include <stdint.h>

#include "absl/base/optimization.h"
#include "absl/numeric/bits.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/constexpr.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/varint/varint_internal.h"  // IWYU pragma: export

namespace riegeli {

// Writes a varint. This corresponds to protobuf types `{int,uint}{32,64}`
// (with a cast needed in the case of `int{32,64}`).
//
// Warning: protobuf writes values of type `int32` by casting them to `uint64`,
// not `uint32` (negative values take 10 bytes, not 5), hence they must be
// written with `WriteVarint64()`, not `WriteVarint32()`, if negative values are
// possible.
//
// Return values:
//  * `true`  - success (`dest.ok()`)
//  * `false` - failure (`!dest.ok()`)
bool WriteVarint32(uint32_t data, Writer& dest);
bool WriteVarint64(uint64_t data, Writer& dest);
bool WriteVarint32(uint32_t data, BackwardWriter& dest);
bool WriteVarint64(uint64_t data, BackwardWriter& dest);

// Writes a signed varint (zigzag-encoded). This corresponds to protobuf types
// `sint{32,64}`.
//
// Return values:
//  * `true`  - success (`dest.ok()`)
//  * `false` - failure (`!dest.ok()`)
bool WriteVarintSigned32(int32_t data, Writer& dest);
bool WriteVarintSigned64(int64_t data, Writer& dest);
bool WriteVarintSigned32(int32_t data, BackwardWriter& dest);
bool WriteVarintSigned64(int64_t data, BackwardWriter& dest);

// Returns the length needed to write a given value as a varint.
// This corresponds to protobuf types `{int,uint}{32,64}` (with a cast needed in
// the case of `int{32,64}`).
//
// Warning: protobuf writes values of type `int32` by casting them to `uint64`,
// not `uint32` (negative values take 10 bytes, not 5), hence they must be
// measured with `LengthVarint64()`, not `LengthVarint32()`, if negative values
// are possible.
//
// The result is at most `kMaxLengthVarint{32,64}`.
size_t LengthVarint32(uint32_t data);
size_t LengthVarint64(uint64_t data);

// Returns the length needed to write a given value as a signed varint
// (zigzag-encoded). This corresponds to protobuf types `sint{32,64}`.
//
// The result is at most `kMaxLengthVarint{32,64}`.
size_t LengthVarintSigned32(int32_t data);
size_t LengthVarintSigned64(int64_t data);

// Writes a varint to an array. This corresponds to protobuf types
// `{int,uint}{32,64}` (with a cast needed in the case of `int{32,64}`).
//
// Warning: protobuf writes values of type `int32` by casting them to `uint64`,
// not `uint32` (negative values take 10 bytes, not 5), hence they must be
// written with `WriteVarint64()`, not `WriteVarint32()`, if negative values are
// possible.
//
// Writes at most `LengthVarint{32,64}(data)` bytes to `dest[]`. Returns the
// updated `dest` after the written value.
char* WriteVarint32(uint32_t data, char* dest);
char* WriteVarint64(uint64_t data, char* dest);

// Writes a signed varint (zigzag-encoded) to an array. This corresponds to
// protobuf types `sint{32,64}`.
//
// Writes at most `LengthVarintSigned{32,64}(data)` bytes to `dest[]`. Returns
// the updated `dest` after the written value.
char* WriteVarintSigned32(int32_t data, char* dest);
char* WriteVarintSigned64(int64_t data, char* dest);

// Implementation details follow.

namespace varint_internal {

inline uint32_t EncodeSint32(int32_t value) {
  return (static_cast<uint32_t>(value) << 1) ^
         static_cast<uint32_t>(value >> 31);
}

inline uint64_t EncodeSint64(int64_t value) {
  return (static_cast<uint64_t>(value) << 1) ^
         static_cast<uint64_t>(value >> 63);
}

}  // namespace varint_internal

inline bool WriteVarint32(uint32_t data, Writer& dest) {
  if (ABSL_PREDICT_FALSE(!dest.Push(RIEGELI_IS_CONSTANT(data) ||
                                            RIEGELI_IS_CONSTANT(data < 0x80)
                                        ? LengthVarint32(data)
                                        : kMaxLengthVarint32))) {
    return false;
  }
  dest.set_cursor(WriteVarint32(data, dest.cursor()));
  return true;
}

inline bool WriteVarint64(uint64_t data, Writer& dest) {
  if (ABSL_PREDICT_FALSE(!dest.Push(RIEGELI_IS_CONSTANT(data) ||
                                            RIEGELI_IS_CONSTANT(data < 0x80)
                                        ? LengthVarint64(data)
                                        : kMaxLengthVarint64))) {
    return false;
  }
  dest.set_cursor(WriteVarint64(data, dest.cursor()));
  return true;
}

inline bool WriteVarint32(uint32_t data, BackwardWriter& dest) {
  const size_t length = LengthVarint32(data);
  if (ABSL_PREDICT_FALSE(!dest.Push(length))) return false;
  dest.move_cursor(length);
  WriteVarint32(data, dest.cursor());
  return true;
}

inline bool WriteVarint64(uint64_t data, BackwardWriter& dest) {
  const size_t length = LengthVarint64(data);
  if (ABSL_PREDICT_FALSE(!dest.Push(length))) return false;
  dest.move_cursor(length);
  WriteVarint64(data, dest.cursor());
  return true;
}

inline bool WriteVarintSigned32(int32_t data, Writer& dest) {
  return WriteVarint32(varint_internal::EncodeSint32(data), dest);
}

inline bool WriteVarintSigned64(int64_t data, Writer& dest) {
  return WriteVarint64(varint_internal::EncodeSint64(data), dest);
}

inline bool WriteVarintSigned32(int32_t data, BackwardWriter& dest) {
  return WriteVarint32(varint_internal::EncodeSint32(data), dest);
}

inline bool WriteVarintSigned64(int64_t data, BackwardWriter& dest) {
  return WriteVarint64(varint_internal::EncodeSint64(data), dest);
}

inline size_t LengthVarint32(uint32_t data) {
  const size_t width = IntCast<size_t>(absl::bit_width(data | 1));
  // This is the same as `(width + 6) / 7` for `width` in [1..32],
  // but performs division by a power of 2.
  return (width * 9 + 64) / 64;
}

inline size_t LengthVarint64(uint64_t data) {
  const size_t width = IntCast<size_t>(absl::bit_width(data | 1));
  // This is the same as `(width + 6) / 7` for `width` in [1..64],
  // but performs division by a power of 2.
  return (width * 9 + 64) / 64;
}

inline size_t LengthVarintSigned32(int32_t data) {
  return LengthVarint32(varint_internal::EncodeSint32(data));
}

inline size_t LengthVarintSigned64(int64_t data) {
  return LengthVarint64(varint_internal::EncodeSint64(data));
}

inline char* WriteVarint32(uint32_t data, char* dest) {
  if (data < 0x80) {
    *dest++ = static_cast<char>(data);
    return dest;
  }
  do {
    *dest++ = static_cast<char>(data | 0x80);
    data >>= 7;
  } while (data >= 0x80);
  *dest++ = static_cast<char>(data);
  return dest;
}

inline char* WriteVarint64(uint64_t data, char* dest) {
  if (data < 0x80) {
    *dest++ = static_cast<char>(data);
    return dest;
  }
  do {
    *dest++ = static_cast<char>(data | 0x80);
    data >>= 7;
  } while (data >= 0x80);
  *dest++ = static_cast<char>(data);
  return dest;
}

inline char* WriteVarintSigned32(int32_t data, char* dest) {
  return WriteVarint32(varint_internal::EncodeSint32(data), dest);
}

inline char* WriteVarintSigned64(int64_t data, char* dest) {
  return WriteVarint64(varint_internal::EncodeSint64(data), dest);
}

}  // namespace riegeli

#endif  // RIEGELI_VARINT_VARINT_WRITING_H_
