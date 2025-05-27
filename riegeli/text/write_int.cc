// Copyright 2023 Google LLC
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

#include "riegeli/text/write_int.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __SSSE3__
#include <emmintrin.h>
#include <tmmintrin.h>
#endif

#include <cstring>
#include <limits>  // IWYU pragma: keep

#include "absl/base/attributes.h"
#include "absl/numeric/bits.h"  // IWYU pragma: keep
#include "absl/numeric/int128.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/endian/endian_writing.h"

namespace riegeli::write_int_internal {

namespace {

#ifdef __SSSE3__

template <DigitCase digit_case>
__m128i HexDigits();

template <>
inline __m128i HexDigits<DigitCase::kLower>() {
  return _mm_setr_epi8('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a',
                       'b', 'c', 'd', 'e', 'f');
}

template <>
inline __m128i HexDigits<DigitCase::kUpper>() {
  return _mm_setr_epi8('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A',
                       'B', 'C', 'D', 'E', 'F');
}

template <DigitCase digit_case>
inline __m128i WriteHex2Impl(uint8_t src) {
  // Load 8-bit value to 128-bit register.
  const __m128i value = _mm_cvtsi32_si128(src);
  // Shift right by 4 bits.
  const __m128i shifted = _mm_srli_epi16(value, 4);
  // Interleave low and high nibbles into bytes.
  const __m128i interleaved = _mm_unpacklo_epi8(shifted, value);
  // Mask out high nibbles of bytes.
  const __m128i masked = _mm_and_si128(interleaved, _mm_set1_epi8(0xf));
  // Convert to characters.
  return _mm_shuffle_epi8(HexDigits<digit_case>(), masked);
}

template <DigitCase digit_case>
inline __m128i WriteHex4Impl(uint16_t src) {
  // Convert to Big Endian.
  char encoded[2];
  riegeli::WriteBigEndian16(src, encoded);
  // Load 16-bit value to 128-bit register.
  const __m128i value = _mm_loadu_si16(encoded);
  // Shift right by 4 bits.
  const __m128i shifted = _mm_srli_epi16(value, 4);
  // Interleave low and high nibbles into bytes.
  const __m128i interleaved = _mm_unpacklo_epi8(shifted, value);
  // Mask out high nibbles of bytes.
  const __m128i masked = _mm_and_si128(interleaved, _mm_set1_epi8(0xf));
  // Convert to characters.
  return _mm_shuffle_epi8(HexDigits<digit_case>(), masked);
}

template <DigitCase digit_case>
inline __m128i WriteHex8Impl(uint32_t src) {
  // Convert to Big Endian.
  char encoded[4];
  riegeli::WriteBigEndian32(src, encoded);
  // Load 32-bit value to 128-bit register.
  const __m128i value = _mm_loadu_si32(encoded);
  // Shift right by 4 bits.
  const __m128i shifted = _mm_srli_epi32(value, 4);
  // Interleave low and high nibbles into bytes.
  const __m128i interleaved = _mm_unpacklo_epi8(shifted, value);
  // Mask out high nibbles of bytes.
  const __m128i masked = _mm_and_si128(interleaved, _mm_set1_epi8(0xf));
  // Convert to characters.
  return _mm_shuffle_epi8(HexDigits<digit_case>(), masked);
}

template <DigitCase digit_case>
inline __m128i WriteHex16Impl(uint64_t src) {
  // Convert to Big Endian.
  char encoded[8];
  riegeli::WriteBigEndian64(src, encoded);
  // Load 64-bit value to 128-bit register.
  const __m128i value = _mm_loadu_si64(&encoded);
  // Shift right by 4 bits.
  const __m128i shifted = _mm_srli_epi64(value, 4);
  // Interleave low and high nibbles into bytes.
  const __m128i interleaved = _mm_unpacklo_epi8(shifted, value);
  // Mask out high nibbles of bytes.
  const __m128i masked = _mm_and_si128(interleaved, _mm_set1_epi8(0xf));
  // Convert to characters.
  return _mm_shuffle_epi8(HexDigits<digit_case>(), masked);
}

#endif

template <DigitCase digit_case, typename T>
inline T DigitCaseDependent(T for_lower, T for_upper) {
  switch (digit_case) {
    case DigitCase::kLower:
      return for_lower;
    case DigitCase::kUpper:
      return for_upper;
  }
}

// `WriteHex{1,2,4,8,16,32}Impl()` write a fixed number of digits.

template <DigitCase digit_case>
inline char* WriteHex1Impl(uint8_t src, char* dest) {
  RIEGELI_ASSERT_LT(src, 0x10)
      << "Failed precondition of WriteHex1Impl(): value too large";
  *dest = static_cast<char>(src) +
          (src < 10 ? '0' : DigitCaseDependent<digit_case>('a', 'A') - 10);
  return dest + 1;
}

template <DigitCase digit_case>
inline char* WriteHex2Impl(uint8_t src, char* dest) {
#ifdef __SSSE3__
  _mm_storeu_si16(dest, WriteHex2Impl<digit_case>(src));
#else
  uint16_t out = src;
  // Spread out nibbles to bytes (00AB -> 0AXB -> 0A0B).
  out = (out | (out << 4)) & 0x0f0f;
  // Convert each byte [0..9] to [6..15], and [10..15] to [16..21].
  out += 0x0606;
  // Keep bytes [6..15] unchanged. Convert each byte [16..21] to [55..60]
  // for `DigitCase::kLower`, or [23..28] for `DigitCase::kUpper`.
  out += DigitCaseDependent<digit_case>(39, 7) * ((out & 0x1010) >> 4);
  // Convert each byte [6..15] to ['0'..'9'], and [55..60] to ['a'..'f'] for
  // `DigitCase::kLower`, or [23..28] to ['A'..'F'] for `DigitCase::kUpper`.
  out += 0x2a2a;
  // Write the result, swapping the bytes.
  riegeli::WriteBigEndian16(out, dest);
#endif
  return dest + 2;
}

template <DigitCase digit_case>
inline char* WriteHex4Impl(uint16_t src, char* dest) {
#ifdef __SSSE3__
  _mm_storeu_si32(dest, WriteHex4Impl<digit_case>(src));
#else
  uint32_t out = src;
  // Spread out nibbles to bytes, swapping the middle ones
  // (0000ABCD -> 0ABCXBCD -> 0A0C0B0D).
  out = (out | (out << 12)) & 0x0f0f0f0f;
  // Convert each byte [0..9] to [6..15], and [10..15] to [16..21].
  out += 0x06060606;
  // Keep bytes [6..15] unchanged. Convert each byte [16..21] to [55..60]
  // for `DigitCase::kLower`, or [23..28] for `DigitCase::kUpper`.
  out += DigitCaseDependent<digit_case>(39, 7) * ((out & 0x10101010) >> 4);
  // Convert each byte [6..15] to ['0'..'9'], and [55..60] to ['a'..'f'] for
  // `DigitCase::kLower`, or [23..28] to ['A'..'F'] for `DigitCase::kUpper`.
  out += 0x2a2a2a2a;
  // Swap the first and the last byte.
  out = (out << 24) | (out >> 24) | (out & 0x00ffff00);
  // Write the result.
  riegeli::WriteLittleEndian32(out, dest);
#endif
  return dest + 4;
}

template <DigitCase digit_case>
inline char* WriteHex8Impl(uint32_t src, char* dest) {
#ifdef __SSSE3__
  _mm_storeu_si64(dest, WriteHex8Impl<digit_case>(src));
  return dest + 8;
#else
  dest = WriteHex4Impl<digit_case>(IntCast<uint16_t>(src >> 16), dest);
  return WriteHex4Impl<digit_case>(static_cast<uint16_t>(src), dest);
#endif
}

template <DigitCase digit_case>
inline char* WriteHex16Impl(uint64_t src, char* dest) {
#ifdef __SSSE3__
  _mm_storeu_si128(reinterpret_cast<__m128i*>(dest),
                   WriteHex16Impl<digit_case>(src));
  return dest + 16;
#else
  dest = WriteHex8Impl<digit_case>(IntCast<uint32_t>(src >> 32), dest);
  return WriteHex8Impl<digit_case>(static_cast<uint32_t>(src), dest);
#endif
}

template <DigitCase digit_case>
inline char* WriteHex32Impl(absl::uint128 src, char* dest) {
  dest = WriteHex16Impl<digit_case>(absl::Uint128High64(src), dest);
  return WriteHex16Impl<digit_case>(absl::Uint128Low64(src), dest);
}

// `WriteHexImpl()` writes at least `width` digits.

// Inline to optimize for a constant `width`.
template <DigitCase digit_case>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline char* WriteHexImpl(uint8_t src, char* dest,
                                                       size_t width) {
  if (src < uint8_t{1} << 4 && width <= 1) {
    return WriteHex1Impl<digit_case>(src, dest);
  }
  if (width > 2) {
    // Redundant condition suppresses gcc warning `-Wstringop-overflow`.
    std::memset(dest, '0', width > 2 ? width - 2 : 0);
    dest += width - 2;
  }
  return WriteHex2Impl<digit_case>(src, dest);
}

// Inline to optimize for a constant `width`.
template <DigitCase digit_case>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline char* WriteHexImpl(uint16_t src, char* dest,
                                                       size_t width) {
#ifdef __SSSE3__
  const __m128i out = WriteHex4Impl<digit_case>(src);
  if (src >= uint32_t{1} << 12 || width >= 4) {
    if (width > 4) {
      // Redundant condition suppresses gcc warning `-Wstringop-overflow`.
      std::memset(dest, '0', width > 4 ? width - 4 : 0);
      dest += width - 4;
    }
    _mm_storeu_si32(dest, out);
    return dest + 4;
  }
  char str[4];
  _mm_storeu_si32(str, out);
  width = UnsignedMax(
      width,
      (IntCast<size_t>(absl::bit_width(IntCast<uint16_t>(src | 1))) + 3) / 4);
  std::memcpy(dest, str + 4 - width, width);
  return dest + width;
#else
  if (src <= std::numeric_limits<uint8_t>::max()) {
    return WriteHexImpl<digit_case>(IntCast<uint8_t>(src), dest, width);
  }
  dest = WriteHexImpl<digit_case>(IntCast<uint8_t>(src >> 8), dest,
                                  SaturatingSub(width, size_t{2}));
  return WriteHex2Impl<digit_case>(static_cast<uint8_t>(src), dest);
#endif
}

// Inline to optimize for a constant `width`.
template <DigitCase digit_case>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline char* WriteHexImpl(uint32_t src, char* dest,
                                                       size_t width) {
#ifdef __SSSE3__
  const __m128i out = WriteHex8Impl<digit_case>(src);
  if (src >= uint32_t{1} << 28 || width >= 8) {
    if (width > 8) {
      // Redundant condition suppresses gcc warning `-Wstringop-overflow`.
      std::memset(dest, '0', width > 0 ? width - 8 : 0);
      dest += width - 8;
    }
    _mm_storeu_si64(dest, out);
    return dest + 8;
  }
  char str[8];
  _mm_storeu_si64(str, out);
  width =
      UnsignedMax(width, (IntCast<size_t>(absl::bit_width(src | 1)) + 3) / 4);
  std::memcpy(dest, str + 8 - width, width);
  return dest + width;
#else
  if (src <= std::numeric_limits<uint16_t>::max()) {
    return WriteHexImpl<digit_case>(IntCast<uint16_t>(src), dest, width);
  }
  dest = WriteHexImpl<digit_case>(IntCast<uint16_t>(src >> 16), dest,
                                  SaturatingSub(width, size_t{4}));
  return WriteHex4Impl<digit_case>(static_cast<uint16_t>(src), dest);
#endif
}

// Inline to optimize for a constant `width`.
template <DigitCase digit_case>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline char* WriteHexImpl(uint64_t src, char* dest,
                                                       size_t width) {
#ifdef __SSSE3__
  const __m128i out = WriteHex16Impl<digit_case>(src);
  if (src >= uint64_t{1} << 60 || width >= 16) {
    if (width > 16) {
      // Redundant condition suppresses gcc warning `-Wstringop-overflow`.
      std::memset(dest, '0', width > 16 ? width - 16 : 0);
      dest += width - 16;
    }
    _mm_storeu_si128(reinterpret_cast<__m128i*>(dest), out);
    return dest + 16;
  }
  alignas(16) char str[16];
  _mm_store_si128(reinterpret_cast<__m128i*>(str), out);
  width =
      UnsignedMax(width, (IntCast<size_t>(absl::bit_width(src | 1)) + 3) / 4);
  std::memcpy(dest, str + 16 - width, width);
  return dest + width;
#else
  if (src <= std::numeric_limits<uint32_t>::max()) {
    return WriteHexImpl<digit_case>(IntCast<uint32_t>(src), dest, width);
  }
  dest = WriteHexImpl<digit_case>(IntCast<uint32_t>(src >> 32), dest,
                                  SaturatingSub(width, size_t{8}));
  return WriteHex8Impl<digit_case>(static_cast<uint32_t>(src), dest);
#endif
}

// Inline to optimize for a constant `width`.
template <DigitCase digit_case>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline char* WriteHexImpl(absl::uint128 src,
                                                       char* dest,
                                                       size_t width) {
  if (absl::Uint128High64(src) == 0) {
    return WriteHexImpl<digit_case>(absl::Uint128Low64(src), dest, width);
  }
  dest = WriteHexImpl<digit_case>(absl::Uint128High64(src), dest,
                                  SaturatingSub(width, size_t{16}));
  return WriteHex16Impl<digit_case>(absl::Uint128Low64(src), dest);
}

}  // namespace

template <>
char* WriteHex2<DigitCase::kLower>(uint8_t src, char* dest) {
  return WriteHex2Impl<DigitCase::kLower>(src, dest);
}
template <>
char* WriteHex4<DigitCase::kLower>(uint16_t src, char* dest) {
  return WriteHex4Impl<DigitCase::kLower>(src, dest);
}
template <>
char* WriteHex8<DigitCase::kLower>(uint32_t src, char* dest) {
  return WriteHex8Impl<DigitCase::kLower>(src, dest);
}
template <>
char* WriteHex16<DigitCase::kLower>(uint64_t src, char* dest) {
  return WriteHex16Impl<DigitCase::kLower>(src, dest);
}
template <>
char* WriteHex32<DigitCase::kLower>(absl::uint128 src, char* dest) {
  return WriteHex32Impl<DigitCase::kLower>(src, dest);
}

template <>
char* WriteHex2<DigitCase::kUpper>(uint8_t src, char* dest) {
  return WriteHex2Impl<DigitCase::kUpper>(src, dest);
}
template <>
char* WriteHex4<DigitCase::kUpper>(uint16_t src, char* dest) {
  return WriteHex4Impl<DigitCase::kUpper>(src, dest);
}
template <>
char* WriteHex8<DigitCase::kUpper>(uint32_t src, char* dest) {
  return WriteHex8Impl<DigitCase::kUpper>(src, dest);
}
template <>
char* WriteHex16<DigitCase::kUpper>(uint64_t src, char* dest) {
  return WriteHex16Impl<DigitCase::kUpper>(src, dest);
}
template <>
char* WriteHex32<DigitCase::kUpper>(absl::uint128 src, char* dest) {
  return WriteHex32Impl<DigitCase::kUpper>(src, dest);
}

template <>
char* WriteHex<DigitCase::kLower>(uint8_t src, char* dest) {
  return WriteHexImpl<DigitCase::kLower>(src, dest, 0);
}
template <>
char* WriteHex<DigitCase::kLower>(uint16_t src, char* dest) {
  return WriteHexImpl<DigitCase::kLower>(src, dest, 0);
}
template <>
char* WriteHex<DigitCase::kLower>(uint32_t src, char* dest) {
  return WriteHexImpl<DigitCase::kLower>(src, dest, 0);
}
template <>
char* WriteHex<DigitCase::kLower>(uint64_t src, char* dest) {
  return WriteHexImpl<DigitCase::kLower>(src, dest, 0);
}
template <>
char* WriteHex<DigitCase::kLower>(absl::uint128 src, char* dest) {
  return WriteHexImpl<DigitCase::kLower>(src, dest, 0);
}

template <>
char* WriteHex<DigitCase::kUpper>(uint8_t src, char* dest) {
  return WriteHexImpl<DigitCase::kUpper>(src, dest, 0);
}
template <>
char* WriteHex<DigitCase::kUpper>(uint16_t src, char* dest) {
  return WriteHexImpl<DigitCase::kUpper>(src, dest, 0);
}
template <>
char* WriteHex<DigitCase::kUpper>(uint32_t src, char* dest) {
  return WriteHexImpl<DigitCase::kUpper>(src, dest, 0);
}
template <>
char* WriteHex<DigitCase::kUpper>(uint64_t src, char* dest) {
  return WriteHexImpl<DigitCase::kUpper>(src, dest, 0);
}
template <>
char* WriteHex<DigitCase::kUpper>(absl::uint128 src, char* dest) {
  return WriteHexImpl<DigitCase::kUpper>(src, dest, 0);
}

template <>
char* WriteHex<DigitCase::kLower>(uint8_t src, char* dest, size_t width) {
  return WriteHexImpl<DigitCase::kLower>(src, dest, width);
}
template <>
char* WriteHex<DigitCase::kLower>(uint16_t src, char* dest, size_t width) {
  return WriteHexImpl<DigitCase::kLower>(src, dest, width);
}
template <>
char* WriteHex<DigitCase::kLower>(uint32_t src, char* dest, size_t width) {
  return WriteHexImpl<DigitCase::kLower>(src, dest, width);
}
template <>
char* WriteHex<DigitCase::kLower>(uint64_t src, char* dest, size_t width) {
  return WriteHexImpl<DigitCase::kLower>(src, dest, width);
}
template <>
char* WriteHex<DigitCase::kLower>(absl::uint128 src, char* dest, size_t width) {
  return WriteHexImpl<DigitCase::kLower>(src, dest, width);
}

template <>
char* WriteHex<DigitCase::kUpper>(uint8_t src, char* dest, size_t width) {
  return WriteHexImpl<DigitCase::kUpper>(src, dest, width);
}
template <>
char* WriteHex<DigitCase::kUpper>(uint16_t src, char* dest, size_t width) {
  return WriteHexImpl<DigitCase::kUpper>(src, dest, width);
}
template <>
char* WriteHex<DigitCase::kUpper>(uint32_t src, char* dest, size_t width) {
  return WriteHexImpl<DigitCase::kUpper>(src, dest, width);
}
template <>
char* WriteHex<DigitCase::kUpper>(uint64_t src, char* dest, size_t width) {
  return WriteHexImpl<DigitCase::kUpper>(src, dest, width);
}
template <>
char* WriteHex<DigitCase::kUpper>(absl::uint128 src, char* dest, size_t width) {
  return WriteHexImpl<DigitCase::kUpper>(src, dest, width);
}

template <>
void WriteHexBackward2<DigitCase::kLower>(uint8_t src, char* dest) {
  WriteHex2Impl<DigitCase::kLower>(src, dest - 2);
}
template <>
void WriteHexBackward4<DigitCase::kLower>(uint16_t src, char* dest) {
  WriteHex4Impl<DigitCase::kLower>(src, dest - 4);
}
template <>
void WriteHexBackward8<DigitCase::kLower>(uint32_t src, char* dest) {
  WriteHex8Impl<DigitCase::kLower>(src, dest - 8);
}
template <>
void WriteHexBackward16<DigitCase::kLower>(uint64_t src, char* dest) {
  WriteHex16Impl<DigitCase::kLower>(src, dest - 16);
}
template <>
void WriteHexBackward32<DigitCase::kLower>(absl::uint128 src, char* dest) {
  WriteHex32Impl<DigitCase::kLower>(src, dest - 32);
}

template <>
void WriteHexBackward2<DigitCase::kUpper>(uint8_t src, char* dest) {
  WriteHex2Impl<DigitCase::kUpper>(src, dest - 2);
}
template <>
void WriteHexBackward4<DigitCase::kUpper>(uint16_t src, char* dest) {
  WriteHex4Impl<DigitCase::kUpper>(src, dest - 4);
}
template <>
void WriteHexBackward8<DigitCase::kUpper>(uint32_t src, char* dest) {
  WriteHex8Impl<DigitCase::kUpper>(src, dest - 8);
}
template <>
void WriteHexBackward16<DigitCase::kUpper>(uint64_t src, char* dest) {
  WriteHex16Impl<DigitCase::kUpper>(src, dest - 16);
}
template <>
void WriteHexBackward32<DigitCase::kUpper>(absl::uint128 src, char* dest) {
  WriteHex32Impl<DigitCase::kUpper>(src, dest - 32);
}

}  // namespace riegeli::write_int_internal
