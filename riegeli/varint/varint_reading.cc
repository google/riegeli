// Copyright 2019 Google LLC
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

#include "riegeli/varint/varint_reading.h"

#include <stddef.h>
#include <stdint.h>

#include <optional>

#include "absl/base/optimization.h"
#include "riegeli/bytes/reader.h"

namespace riegeli::varint_internal {

namespace {

template <bool canonical>
inline bool ReadVarint32Fast(Reader& src, uint32_t& dest) {
  const std::optional<const char*> cursor =
      ReadVarint32(src.cursor(), src.limit(), dest);
  if (ABSL_PREDICT_FALSE(cursor == std::nullopt)) return false;
  if (canonical && ABSL_PREDICT_FALSE((*cursor)[-1] == 0)) return false;
  src.set_cursor(*cursor);
  return true;
}

template <bool canonical>
inline bool ReadVarint64Fast(Reader& src, uint64_t& dest) {
  const std::optional<const char*> cursor =
      ReadVarint64(src.cursor(), src.limit(), dest);
  if (ABSL_PREDICT_FALSE(cursor == std::nullopt)) return false;
  if (canonical && ABSL_PREDICT_FALSE((*cursor)[-1] == 0)) return false;
  src.set_cursor(*cursor);
  return true;
}

}  // namespace

template <bool canonical>
bool ReadVarint32Slow(Reader& src, uint32_t& dest) {
  if (ABSL_PREDICT_FALSE(!src.Pull(1, kMaxLengthVarint32))) return false;
  const uint8_t first_byte = static_cast<uint8_t>(*src.cursor());
  if (first_byte < 0x80) {
    // Even if `canonical`, any byte with the highest bit clear is accepted as
    // the only byte, including 0 itself.
    dest = first_byte;
    src.move_cursor(1);
    return true;
  }
  if (ABSL_PREDICT_TRUE(src.available() >= kMaxLengthVarint32 ||
                        static_cast<uint8_t>(src.limit()[-1]) < 0x80)) {
    return ReadVarint32Fast<canonical>(src, dest);
  }
  if (ABSL_PREDICT_TRUE(src.ToleratesReadingAhead())) {
    src.Pull(kMaxLengthVarint32);
    return ReadVarint32Fast<canonical>(src, dest);
  }
  uint8_t byte = static_cast<uint8_t>(src.cursor()[0]);
  uint32_t acc{byte};
  size_t length = 1;
  while (byte >= 0x80) {
    if (ABSL_PREDICT_FALSE(!src.Pull(length + 1, kMaxLengthVarint32))) {
      return false;
    }
    byte = static_cast<uint8_t>(src.cursor()[length]);
    acc += (uint32_t{byte} - 1) << (length * 7);
    ++length;
    if (ABSL_PREDICT_FALSE(length == kMaxLengthVarint32)) {
      // Last possible byte.
      if (ABSL_PREDICT_FALSE(
              byte >= uint8_t{1} << (32 - (kMaxLengthVarint32 - 1) * 7))) {
        // The representation is longer than `kMaxLengthVarint32`
        // or the represented value does not fit in `uint32_t`.
        return false;
      }
      break;
    }
  }
  if (canonical && ABSL_PREDICT_FALSE(src.cursor()[length - 1] == 0)) {
    return false;
  }
  dest = acc;
  src.move_cursor(length);
  return true;
}

template <bool canonical>
bool ReadVarint64Slow(Reader& src, uint64_t& dest) {
  if (ABSL_PREDICT_FALSE(!src.Pull(1, kMaxLengthVarint64))) return false;
  const uint8_t first_byte = static_cast<uint8_t>(*src.cursor());
  if (first_byte < 0x80) {
    // Even if `canonical`, any byte with the highest bit clear is accepted as
    // the only byte, including 0 itself.
    dest = first_byte;
    src.move_cursor(1);
    return true;
  }
  if (ABSL_PREDICT_TRUE(src.available() >= kMaxLengthVarint64 ||
                        static_cast<uint8_t>(src.limit()[-1]) < 0x80)) {
    return ReadVarint64Fast<canonical>(src, dest);
  }
  if (ABSL_PREDICT_TRUE(src.ToleratesReadingAhead())) {
    src.Pull(kMaxLengthVarint64);
    return ReadVarint64Fast<canonical>(src, dest);
  }
  uint8_t byte = static_cast<uint8_t>(src.cursor()[0]);
  uint64_t acc{byte};
  size_t length = 1;
  while (byte >= 0x80) {
    if (ABSL_PREDICT_FALSE(!src.Pull(length + 1, kMaxLengthVarint64))) {
      return false;
    }
    byte = static_cast<uint8_t>(src.cursor()[length]);
    acc += (uint64_t{byte} - 1) << (length * 7);
    ++length;
    if (ABSL_PREDICT_FALSE(length == kMaxLengthVarint64)) {
      // Last possible byte.
      if (ABSL_PREDICT_FALSE(
              byte >= uint8_t{1} << (64 - (kMaxLengthVarint64 - 1) * 7))) {
        // The representation is longer than `kMaxLengthVarint64`
        // or the represented value does not fit in `uint64_t`.
        return false;
      }
      break;
    }
  }
  if (canonical && ABSL_PREDICT_FALSE(src.cursor()[length - 1] == 0)) {
    return false;
  }
  dest = acc;
  src.move_cursor(length);
  return true;
}

template bool ReadVarint32Slow<false>(Reader& src, uint32_t& dest);
template bool ReadVarint32Slow<true>(Reader& src, uint32_t& dest);
template bool ReadVarint64Slow<false>(Reader& src, uint64_t& dest);
template bool ReadVarint64Slow<true>(Reader& src, uint64_t& dest);

std::optional<const char*> ReadVarint32Slow(const char* src, const char* limit,
                                            uint32_t acc, uint32_t& dest) {
  uint8_t byte;
  size_t shift = kReadVarintSlowThreshold;
  do {
    if (ABSL_PREDICT_FALSE(src == limit)) return std::nullopt;
    byte = static_cast<uint8_t>(*src++);
    acc += (uint32_t{byte} - 1) << shift;
    shift += 7;
    if (ABSL_PREDICT_FALSE(shift == kMaxLengthVarint32 * 7)) {
      // Last possible byte.
      if (ABSL_PREDICT_FALSE(
              byte >= uint8_t{1} << (32 - (kMaxLengthVarint32 - 1) * 7))) {
        // The representation is longer than `kMaxLengthVarint32`
        // or the represented value does not fit in `uint32_t`.
        return std::nullopt;
      }
      break;
    }
  } while (byte >= 0x80);
  dest = acc;
  return src;
}

std::optional<const char*> ReadVarint64Slow(const char* src, const char* limit,
                                            uint64_t acc, uint64_t& dest) {
  uint8_t byte;
  size_t shift = kReadVarintSlowThreshold;
  do {
    if (ABSL_PREDICT_FALSE(src == limit)) return std::nullopt;
    byte = static_cast<uint8_t>(*src++);
    acc += (uint64_t{byte} - 1) << shift;
    shift += 7;
    if (ABSL_PREDICT_FALSE(shift == kMaxLengthVarint64 * 7)) {
      // Last possible byte.
      if (ABSL_PREDICT_FALSE(
              byte >= uint8_t{1} << (64 - (kMaxLengthVarint64 - 1) * 7))) {
        // The representation is longer than `kMaxLengthVarint64`
        // or the represented value does not fit in `uint64_t`.
        return std::nullopt;
      }
      break;
    }
  } while (byte >= 0x80);
  dest = acc;
  return src;
}

}  // namespace riegeli::varint_internal
