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

#include <array>
#include <cstring>
#include <limits>
#include <type_traits>

#include "absl/log/absl_check.h"
#include "absl/numeric/bits.h"
#include "absl/numeric/int128.h"
#include "riegeli/base/arithmetic.h"

namespace riegeli::write_int_internal {

namespace {

// `WriteDec{1,2,8,10}Impl()` write a fixed number of digits.

inline char* WriteDec1Impl(uint32_t src, char* dest) {
  ABSL_DCHECK_LT(src, 10u)
      << "Failed precondition of WriteDec1Impl(): value too large";
  *dest = '0' + static_cast<char>(src);
  return dest + 1;
}

inline char* WriteDec2Impl(uint32_t src, char* dest) {
  ABSL_DCHECK_LT(src, 100u)
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

inline char* WriteDec8Impl(uint32_t src, char* dest) {
  ABSL_DCHECK_LT(src, 100'000'000u)
      << "Failed precondition of WriteDec8Impl(): value too large";
  uint32_t digits = src / 1'000'000;
  src %= 1'000'000;
  dest = WriteDec2Impl(digits, dest);
  digits = src / 10'000;
  src %= 10'000;
  dest = WriteDec2Impl(digits, dest);
  digits = src / 100;
  src %= 100;
  dest = WriteDec2Impl(digits, dest);
  return WriteDec2Impl(src, dest);
}

inline char* WriteDec10Impl(uint64_t src, char* dest) {
  ABSL_DCHECK_LT(src, 10'000'000'000u)
      << "Failed precondition of WriteDec10Impl(): value too large";
  const uint32_t digits = IntCast<uint32_t>(src / 100'000'000);
  src %= 100'000'000;
  dest = WriteDec2Impl(digits, dest);
  return WriteDec8Impl(IntCast<uint32_t>(src), dest);
}

}  // namespace

char* WriteDec(uint32_t src, char* dest) {
  uint32_t digits;

  if (src < 100) {
    if (src >= 10) goto write_2_digits;
    return WriteDec1Impl(src, dest);
  }
  if (src < 10'000) {
    if (src >= 1'000) goto write_4_digits;
    digits = src / 100;
    src %= 100;
    dest = WriteDec1Impl(digits, dest);
    goto write_2_digits;
  }
  if (src < 1'000'000) {
    if (src >= 100'000) goto write_6_digits;
    digits = src / 10'000;
    src %= 10'000;
    dest = WriteDec1Impl(digits, dest);
    goto write_4_digits;
  }
  if (src < 100'000'000) {
    if (src >= 10'000'000) goto write_8_digits;
    digits = src / 1'000'000;
    src %= 1'000'000;
    dest = WriteDec1Impl(digits, dest);
    goto write_6_digits;
  }
  if (src < 1'000'000'000) {
    digits = src / 100'000'000;
    src %= 100'000'000;
    dest = WriteDec1Impl(digits, dest);
    goto write_8_digits;
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

char* WriteDec(uint64_t src, char* dest) {
  if (src <= std::numeric_limits<uint32_t>::max()) {
    return WriteDec(IntCast<uint32_t>(src), dest);
  }
  // `src` needs at least 10 digits.
  if (src >= 10'000'000'000) {
    // `src` needs more than 10 digits.
    const uint64_t over_10_digits = src / 10'000'000'000;
    src %= 10'000'000'000;
    dest = WriteDec(IntCast<uint32_t>(over_10_digits), dest);
  }
  // Now `src < 1e10`. Write `src` using exactly 10 digits.
  return WriteDec10Impl(src, dest);
}

char* WriteDec(absl::uint128 src, char* dest) {
  if (src <= std::numeric_limits<uint64_t>::max()) {
    return WriteDec(IntCast<uint64_t>(src), dest);
  }
  // `src` needs at least 20 digits.
  constexpr absl::uint128 k1e20 = absl::MakeUint128(5, 0x6bc75e2d63100000);
  ABSL_DCHECK_EQ(k1e20,
                 absl::uint128(10'000'000'000) * absl::uint128(10'000'000'000));
  if (src >= k1e20) {
    // `src` needs more than 20 digits.
    const absl::uint128 over_20_digits = src / k1e20;
    src %= k1e20;
    dest = WriteDec(IntCast<uint64_t>(over_20_digits), dest);
  }
  // Now `src < 1e20`. Write `src` using exactly 20 digits.
  dest = WriteDec10Impl(IntCast<uint64_t>(src / 10'000'000'000), dest);
  return WriteDec10Impl(IntCast<uint64_t>(src % 10'000'000'000), dest);
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

// For `StringifiedSize(value)`, there is an entry for each
// `absl::bit_width(value) / 2`.
template <typename T>
struct StringifiedSizeEntry {
  using Size = std::conditional_t<(sizeof(T) < sizeof(size_t)), T, size_t>;
  Size size;    // Number of digits at the upper bound of this entry.
  T threshold;  // If `value < threshold`, there is one digit less.
};

template <typename T>
constexpr size_t kNumStringifiedSizeEntries =
    std::numeric_limits<T>::digits / 2 + 1;

template <typename T>
constexpr std::array<StringifiedSizeEntry<T>, kNumStringifiedSizeEntries<T>>
MakeStringifiedSizeEntries() {
  // Default construction of `entries` should not be needed, but without that
  // GCC rejects this code in constexpr context for some reason.
  std::array<StringifiedSizeEntry<T>, kNumStringifiedSizeEntries<T>> entries{};
  typename StringifiedSizeEntry<T>::Size size = 1;
  T threshold = 1;
  T upper_bound = 3;
  for (StringifiedSizeEntry<T>& entry : entries) {
    entry.size = size;
    entry.threshold = threshold;
    if (size < std::numeric_limits<T>::digits10 + 1) {
      // Some `absl::uint128` operations are not constexpr,
      // hence multiplication is avoided.
      upper_bound = (upper_bound << 2) + 3;
      const T next_threshold = (threshold << 3) + (threshold << 1);
      if (upper_bound >= next_threshold) {
        ++size;
        threshold = next_threshold;
      }
    }
  }
  entries[0].threshold = T{0};  // 0 has one digit.
  return entries;
}

size_t StringifiedSize(uint32_t src) {
  static constexpr std::array<StringifiedSizeEntry<uint32_t>,
                              kNumStringifiedSizeEntries<uint32_t>>
      kEntries = MakeStringifiedSizeEntries<uint32_t>();
  const size_t width = IntCast<size_t>(absl::bit_width(src));
  const StringifiedSizeEntry<uint32_t>& entry = kEntries[width / 2];
  return size_t{entry.size} - (src < entry.threshold ? 1 : 0);
}

size_t StringifiedSize(uint64_t src) {
  static constexpr std::array<StringifiedSizeEntry<uint64_t>,
                              kNumStringifiedSizeEntries<uint64_t>>
      kEntries = MakeStringifiedSizeEntries<uint64_t>();
  const size_t width = IntCast<size_t>(absl::bit_width(src));
  const StringifiedSizeEntry<uint64_t>& entry = kEntries[width / 2];
  return size_t{entry.size} - (src < entry.threshold ? 1 : 0);
}

size_t StringifiedSize(absl::uint128 src) {
  static constexpr std::array<StringifiedSizeEntry<absl::uint128>,
                              kNumStringifiedSizeEntries<absl::uint128>>
      kEntries = MakeStringifiedSizeEntries<absl::uint128>();
  const size_t width =
      IntCast<size_t>(absl::Uint128High64(src) == 0
                          ? absl::bit_width(absl::Uint128Low64(src))
                          : absl::bit_width(absl::Uint128High64(src)) + 64);
  const StringifiedSizeEntry<absl::uint128>& entry = kEntries[width / 2];
  return size_t{entry.size} - (src < entry.threshold ? 1 : 0);
}

size_t StringifiedSize(int32_t src) {
  if (src >= 0) {
    return StringifiedSize(UnsignedCast(src));
  } else {
    return StringifiedSize(NegatingUnsignedCast(src)) + 1;
  }
}

size_t StringifiedSize(int64_t src) {
  if (src >= 0) {
    return StringifiedSize(UnsignedCast(src));
  } else {
    return StringifiedSize(NegatingUnsignedCast(src)) + 1;
  }
}

size_t StringifiedSize(absl::int128 src) {
  if (src >= 0) {
    return StringifiedSize(UnsignedCast(src));
  } else {
    return StringifiedSize(NegatingUnsignedCast(src)) + 1;
  }
}

}  // namespace riegeli::write_int_internal
