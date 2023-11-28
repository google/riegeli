// Copyright 2022 Google LLC
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

#include "riegeli/bytes/write_int_internal.h"

#include <stddef.h>
#include <stdint.h>

#include <cstring>
#include <limits>

#include "absl/base/attributes.h"
#include "absl/numeric/int128.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"

namespace riegeli {
namespace write_int_internal {

namespace {

// `WriteDec{1,2}Impl()` write a fixed number of digits.

inline char* WriteDec1Impl(uint32_t src, char* dest) {
  RIEGELI_ASSERT_LT(src, 10u)
      << "Failed precondition of WriteDec1Impl(): value too large";
  *dest = '0' + static_cast<char>(src);
  return dest + 1;
}

inline char* WriteDec2Impl(uint32_t src, char* dest) {
  RIEGELI_ASSERT_LT(src, 100u)
      << "Failed precondition of WriteDec2Impl(): value too large";
  static constexpr char kTwoDigits[100][2] = {
      {'0', '0'}, {'0', '1'}, {'0', '2'}, {'0', '3'}, {'0', '4'}, {'0', '5'},
      {'0', '6'}, {'0', '7'}, {'0', '8'}, {'0', '9'}, {'1', '0'}, {'1', '1'},
      {'1', '2'}, {'1', '3'}, {'1', '4'}, {'1', '5'}, {'1', '6'}, {'1', '7'},
      {'1', '8'}, {'1', '9'}, {'2', '0'}, {'2', '1'}, {'2', '2'}, {'2', '3'},
      {'2', '4'}, {'2', '5'}, {'2', '6'}, {'2', '7'}, {'2', '8'}, {'2', '9'},
      {'3', '0'}, {'3', '1'}, {'3', '2'}, {'3', '3'}, {'3', '4'}, {'3', '5'},
      {'3', '6'}, {'3', '7'}, {'3', '8'}, {'3', '9'}, {'4', '0'}, {'4', '1'},
      {'4', '2'}, {'4', '3'}, {'4', '4'}, {'4', '5'}, {'4', '6'}, {'4', '7'},
      {'4', '8'}, {'4', '9'}, {'5', '0'}, {'5', '1'}, {'5', '2'}, {'5', '3'},
      {'5', '4'}, {'5', '5'}, {'5', '6'}, {'5', '7'}, {'5', '8'}, {'5', '9'},
      {'6', '0'}, {'6', '1'}, {'6', '2'}, {'6', '3'}, {'6', '4'}, {'6', '5'},
      {'6', '6'}, {'6', '7'}, {'6', '8'}, {'6', '9'}, {'7', '0'}, {'7', '1'},
      {'7', '2'}, {'7', '3'}, {'7', '4'}, {'7', '5'}, {'7', '6'}, {'7', '7'},
      {'7', '8'}, {'7', '9'}, {'8', '0'}, {'8', '1'}, {'8', '2'}, {'8', '3'},
      {'8', '4'}, {'8', '5'}, {'8', '6'}, {'8', '7'}, {'8', '8'}, {'8', '9'},
      {'9', '0'}, {'9', '1'}, {'9', '2'}, {'9', '3'}, {'9', '4'}, {'9', '5'},
      {'9', '6'}, {'9', '7'}, {'9', '8'}, {'9', '9'}};
  std::memcpy(dest, kTwoDigits[src], 2);
  return dest + 2;
}

// `WriteDecImpl()` writes at least `width` digits.

// Inline to optimize for a constant `width`.
ABSL_ATTRIBUTE_ALWAYS_INLINE
inline char* WriteDecImpl(uint32_t src, char* dest, size_t width) {
  uint32_t digits;

  if (src < 100 && width <= 2) {
    if (src >= 10 || width == 2) goto write_2_digits;
    return WriteDec1Impl(src, dest);
  }
  if (src < 10'000 && width <= 4) {
    if (src >= 1'000 || width == 4) goto write_4_digits;
    digits = src / 100;
    src %= 100;
    dest = WriteDec1Impl(digits, dest);
    goto write_2_digits;
  }
  if (src < 1'000'000 && width <= 6) {
    if (src >= 100'000 || width == 6) goto write_6_digits;
    digits = src / 10'000;
    src %= 10'000;
    dest = WriteDec1Impl(digits, dest);
    goto write_4_digits;
  }
  if (src < 100'000'000 && width <= 8) {
    if (src >= 10'000'000 || width == 8) goto write_8_digits;
    digits = src / 1'000'000;
    src %= 1'000'000;
    dest = WriteDec1Impl(digits, dest);
    goto write_6_digits;
  }
  if (src < 1'000'000'000 && width <= 9) {
    digits = src / 100'000'000;
    src %= 100'000'000;
    dest = WriteDec1Impl(digits, dest);
    goto write_8_digits;
  }

  if (width > 10) {
    // Redundant condition suppresses gcc warning `-Wstringop-overflow`.
    std::memset(dest, '0', width > 10 ? width - 10 : 0);
    dest += width - 10;
  }
  digits = src / 100'000'000;
  src %= 100'000'000;
  dest = WriteDec2Impl(digits, dest);
write_8_digits:
  digits = src / 1'000'000;
  src %= 1'000'000;
  dest = WriteDec2Impl(digits, dest);
write_6_digits:
  digits = src / 10'000;
  src %= 10'000;
  dest = WriteDec2Impl(digits, dest);
write_4_digits:
  digits = src / 100;
  src %= 100;
  dest = WriteDec2Impl(digits, dest);
write_2_digits:
  return WriteDec2Impl(src, dest);
}

// Inline to optimize for a constant `width`.
ABSL_ATTRIBUTE_ALWAYS_INLINE
inline char* WriteDecImpl(uint64_t src, char* dest, size_t width) {
  if (src <= std::numeric_limits<uint32_t>::max()) {
    return WriteDecImpl(IntCast<uint32_t>(src), dest, width);
  }
  // `src` needs at least 10 digits.
  if (src >= 10'000'000'000 || width > 10) {
    // `src` needs more than 10 digits.
    const uint64_t over_10_digits = src / 10'000'000'000;
    src %= 10'000'000'000;
    dest = WriteDecImpl(IntCast<uint32_t>(over_10_digits), dest,
                        SaturatingSub(width, size_t{10}));
  }
  // Now `src < 1e10`. Write `src` using exactly 10 digits. Leading zeros are
  // needed for the case where the original `src` needed more than 10 digits or
  // `width > 10`.
  const uint32_t digits = IntCast<uint32_t>(src / 100'000'000);
  src %= 100'000'000;
  dest = WriteDec2Impl(digits, dest);
  return WriteDecImpl(IntCast<uint32_t>(src), dest, 8);
}

// Inline to optimize for a constant `width`.
ABSL_ATTRIBUTE_ALWAYS_INLINE
inline char* WriteDecImpl(absl::uint128 src, char* dest, size_t width) {
  if (src <= std::numeric_limits<uint64_t>::max()) {
    return WriteDecImpl(IntCast<uint64_t>(src), dest, width);
  }
  // `src` needs at least 20 digits.
  constexpr absl::uint128 k1e20 = absl::MakeUint128(5, 0x6bc75e2d63100000);
  RIEGELI_ASSERT_EQ(
      k1e20, absl::uint128(10'000'000'000) * absl::uint128(10'000'000'000));
  if (src >= k1e20 || width > 20) {
    // `src` needs more than 20 digits.
    const absl::uint128 over_20_digits = src / k1e20;
    src %= k1e20;
    dest = WriteDecImpl(IntCast<uint64_t>(over_20_digits), dest,
                        SaturatingSub(width, size_t{20}));
  }
  // Now `src < 1e20`. Write `src` using exactly 20 digits. Leading zeros are
  // needed for the case where the original `src` needed more than 20 digits or
  // `width > 20`.
  const uint32_t digits = IntCast<uint32_t>(src / 1'000'000'000'000'000'000);
  src %= 1'000'000'000'000'000'000;
  dest = WriteDec2Impl(digits, dest);
  return WriteDecImpl(IntCast<uint64_t>(src), dest, 18);
}

}  // namespace

char* WriteDec(uint32_t src, char* dest) { return WriteDecImpl(src, dest, 0); }

char* WriteDec(uint64_t src, char* dest) { return WriteDecImpl(src, dest, 0); }

char* WriteDec(absl::uint128 src, char* dest) {
  return WriteDecImpl(src, dest, 0);
}

char* WriteDec(int32_t src, char* dest) {
  uint32_t abs_value;
  if (src >= 0) {
    abs_value = UnsignedCast(src);
  } else {
    *dest = '-';
    ++dest;
    abs_value = NegatingUnsignedCast(src);
  }
  return WriteDec(abs_value, dest);
}

char* WriteDec(int64_t src, char* dest) {
  uint64_t abs_value;
  if (src >= 0) {
    abs_value = UnsignedCast(src);
  } else {
    *dest = '-';
    ++dest;
    abs_value = NegatingUnsignedCast(src);
  }
  return WriteDec(abs_value, dest);
}

char* WriteDec(absl::int128 src, char* dest) {
  absl::uint128 abs_value;
  if (src >= 0) {
    abs_value = UnsignedCast(src);
  } else {
    *dest = '-';
    ++dest;
    abs_value = NegatingUnsignedCast(src);
  }
  return WriteDec(abs_value, dest);
}

char* WriteDec(uint32_t src, char* dest, size_t width) {
  return WriteDecImpl(src, dest, width);
}

char* WriteDec(uint64_t src, char* dest, size_t width) {
  return WriteDecImpl(src, dest, width);
}

char* WriteDec(absl::uint128 src, char* dest, size_t width) {
  return WriteDecImpl(src, dest, width);
}

char* WriteDec(int32_t src, char* dest, size_t width) {
  uint32_t abs_value;
  if (src >= 0) {
    abs_value = UnsignedCast(src);
  } else {
    *dest = '-';
    ++dest;
    abs_value = NegatingUnsignedCast(src);
    width = SaturatingSub(width, size_t{1});
  }
  return WriteDec(abs_value, dest, width);
}

char* WriteDec(int64_t src, char* dest, size_t width) {
  uint64_t abs_value;
  if (src >= 0) {
    abs_value = UnsignedCast(src);
  } else {
    *dest = '-';
    ++dest;
    abs_value = NegatingUnsignedCast(src);
    width = SaturatingSub(width, size_t{1});
  }
  return WriteDec(abs_value, dest, width);
}

char* WriteDec(absl::int128 src, char* dest, size_t width) {
  absl::uint128 abs_value;
  if (src >= 0) {
    abs_value = UnsignedCast(src);
  } else {
    *dest = '-';
    ++dest;
    abs_value = NegatingUnsignedCast(src);
    width = SaturatingSub(width, size_t{1});
  }
  return WriteDec(abs_value, dest, width);
}

char* WriteDecBackward(uint32_t src, char* dest) {
  while (src >= 100) {
    const uint32_t digits = src % 100;
    src /= 100;
    dest -= 2;
    WriteDec2Impl(digits, dest);
  }
  if (src >= 10) {
    dest -= 2;
    WriteDec2Impl(src, dest);
  } else {
    --dest;
    WriteDec1Impl(src, dest);
  }
  return dest;
}

char* WriteDecBackward(uint64_t src, char* dest) {
  while (src > std::numeric_limits<uint32_t>::max()) {
    const uint32_t digits = IntCast<uint32_t>(src % 100);
    src /= 100;
    dest -= 2;
    WriteDec2Impl(digits, dest);
  }
  return WriteDecBackward(IntCast<uint32_t>(src), dest);
}

char* WriteDecBackward(absl::uint128 src, char* dest) {
  while (src > std::numeric_limits<uint64_t>::max()) {
    const uint32_t digits = IntCast<uint32_t>(src % 100);
    src /= 100;
    dest -= 2;
    WriteDec2Impl(digits, dest);
  }
  return WriteDecBackward(IntCast<uint64_t>(src), dest);
}

char* WriteDecBackward(int32_t src, char* dest) {
  if (src >= 0) {
    return WriteDecBackward(UnsignedCast(src), dest);
  } else {
    dest = WriteDecBackward(NegatingUnsignedCast(src), dest);
    --dest;
    *dest = '-';
    return dest;
  }
}

char* WriteDecBackward(int64_t src, char* dest) {
  if (src >= 0) {
    return WriteDecBackward(UnsignedCast(src), dest);
  } else {
    dest = WriteDecBackward(NegatingUnsignedCast(src), dest);
    --dest;
    *dest = '-';
    return dest;
  }
}

char* WriteDecBackward(absl::int128 src, char* dest) {
  if (src >= 0) {
    return WriteDecBackward(UnsignedCast(src), dest);
  } else {
    dest = WriteDecBackward(NegatingUnsignedCast(src), dest);
    --dest;
    *dest = '-';
    return dest;
  }
}

}  // namespace write_int_internal
}  // namespace riegeli
