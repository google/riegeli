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

#ifndef RIEGELI_TEXT_WRITE_INT_H_
#define RIEGELI_TEXT_WRITE_INT_H_

#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <ostream>
#include <type_traits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/meta/type_traits.h"
#include "absl/numeric/bits.h"
#include "absl/numeric/int128.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/bytes/ostream_writer.h"
#include "riegeli/bytes/write_int_internal.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// The type returned by `Dec()`.
template <typename T>
class DecType {
 public:
  explicit DecType(T value, size_t width)
      : value_(std::move(value)), width_(width) {}

  const T& value() const { return value_; }
  size_t width() const { return width_; }

  template <typename Sink, typename DependentT = T,
            std::enable_if_t<IsInt<DependentT>::value, int> = 0>
  friend void AbslStringify(Sink& dest, const DecType& src) {
    src.Stringify(dest);
  }

  template <typename DependentT = T,
            std::enable_if_t<IsInt<DependentT>::value, int> = 0>
  friend std::ostream& operator<<(std::ostream& dest, const DecType& src) {
    OStreamWriter<> writer(&dest);
    src.WriteTo(writer);
    writer.Close();
    return dest;
  }

 private:
  template <typename Sink, typename DependentT = T,
            std::enable_if_t<IsUnsignedInt<DependentT>::value, int> = 0>
  void Stringify(Sink& dest) const;
  template <typename Sink, typename DependentT = T,
            std::enable_if_t<IsSignedInt<DependentT>::value, int> = 0>
  void Stringify(Sink& dest) const;
  // Faster implementation if `Sink` is `WriterAbslStringifySink`.
  void Stringify(WriterAbslStringifySink& dest) const { WriteTo(*dest.dest()); }

  template <typename DependentT = T,
            std::enable_if_t<IsUnsignedInt<DependentT>::value, int> = 0>
  void WriteTo(Writer& dest) const;
  template <typename DependentT = T,
            std::enable_if_t<IsSignedInt<DependentT>::value, int> = 0>
  void WriteTo(Writer& dest) const;

  T value_;
  size_t width_;
};

// Specialization of `DecType` for `char` which is written as unsigned.
template <>
class DecType<char> {
 public:
  explicit DecType(char value, size_t width) : value_(value), width_(width) {}

  const char& value() const { return value_; }
  size_t width() const { return width_; }

  template <typename Sink>
  friend void AbslStringify(Sink& dest, const DecType& src) {
    AbslStringify(
        dest, DecType<unsigned char>(static_cast<unsigned char>(src.value()),
                                     src.width()));
  }

  friend std::ostream& operator<<(std::ostream& dest, const DecType& src) {
    return dest << DecType<unsigned char>(
               static_cast<unsigned char>(src.value()), src.width());
  }

 private:
  char value_;
  size_t width_;
};

// Wraps an integer such that its stringified representation is padded with
// zeros to at least the given width.
//
// For negative numbers the width includes the minus sign.
//
// `char` is stringified as unsigned.
//
// If no minimum width is needed, integers can be written to `riegeli::Writer`
// and `riegeli::BackwardWriter` directly, without wrapping, except that `bool`,
// `wchar_t`, `char16_t`, nor `char32_t` cannot be written directly, and for
// `char` and `char8_t` written directly the character itself is written, not
// its decimal representation.
template <typename T>
inline DecType<T> Dec(T value, size_t width = 0) {
  return DecType<T>(value, width);
}

enum class DigitCase {
  kLower,  // ['0'..'9'], ['a'..'f']
  kUpper,  // ['0'..'9'], ['A'..'F']
};

// The type returned by `Hex()`.
template <typename T, DigitCase digit_case = DigitCase::kLower>
class HexType {
 public:
  explicit HexType(T value, size_t width)
      : value_(std::move(value)), width_(width) {}

  const T& value() const { return value_; }
  size_t width() const { return width_; }

  template <typename Sink, typename DependentT = T,
            std::enable_if_t<IsInt<DependentT>::value, int> = 0>
  friend void AbslStringify(Sink& dest, const HexType& src) {
    src.Stringify(dest);
  }

  template <typename DependentT = T,
            std::enable_if_t<IsInt<DependentT>::value, int> = 0>
  friend std::ostream& operator<<(std::ostream& dest, const HexType& src) {
    OStreamWriter<> writer(&dest);
    src.WriteTo(writer);
    writer.Close();
    return dest;
  }

 private:
  template <typename Sink, typename DependentT = T,
            std::enable_if_t<IsUnsignedInt<DependentT>::value, int> = 0>
  void Stringify(Sink& dest) const;
  template <typename Sink, typename DependentT = T,
            std::enable_if_t<IsSignedInt<DependentT>::value, int> = 0>
  void Stringify(Sink& dest) const;
  // Faster implementation if `Sink` is `WriterAbslStringifySink`.
  void Stringify(WriterAbslStringifySink& dest) const { WriteTo(*dest.dest()); }

  template <typename DependentT = T,
            std::enable_if_t<IsUnsignedInt<DependentT>::value, int> = 0>
  void WriteTo(Writer& dest) const;
  template <typename DependentT = T,
            std::enable_if_t<IsSignedInt<DependentT>::value, int> = 0>
  void WriteTo(Writer& dest) const;

  T value_;
  size_t width_;
};

// Specialization of `HexType` for `char` which is written as unsigned.
template <DigitCase digit_case>
class HexType<char, digit_case> {
 public:
  explicit HexType(char value, size_t width) : value_(value), width_(width) {}

  const char& value() const { return value_; }
  size_t width() const { return width_; }

  template <typename Sink>
  friend void AbslStringify(Sink& dest, const HexType& src) {
    AbslStringify(dest,
                  HexType<unsigned char, digit_case>(
                      static_cast<unsigned char>(src.value()), src.width()));
  }

  friend std::ostream& operator<<(std::ostream& dest, const HexType& src) {
    return dest << HexType<unsigned char, digit_case>(
               static_cast<unsigned char>(src.value()), src.width());
  }

 private:
  char value_;
  size_t width_;
};

// Wraps an integer such that its stringified representation is hexadecimal,
// with lower case digits, padded with zeros to at least the given width.
//
// For negative numbers the width includes the minus sign.
//
// `char` is written as unsigned.
template <typename T>
inline HexType<T> Hex(T value, size_t width = 0) {
  return HexType<T>(value, width);
}

// Wraps an integer such that its stringified representation is hexadecimal,
// with upper case digits, padded with zeros to at least the given width.
//
// For negative numbers the width includes the minus sign.
//
// `char` is written as unsigned.
template <typename T>
inline HexType<T, DigitCase::kUpper> HexUpperCase(T value, size_t width = 0) {
  return HexType<T, DigitCase::kUpper>(value, width);
}

// Implementation details follow.

namespace write_int_internal {

// `WriteHex{2,4,8,16,32}()` writes a fixed number of digits.
template <DigitCase digit_case>
char* WriteHex2(uint8_t src, char* dest);
template <DigitCase digit_case>
char* WriteHex4(uint16_t src, char* dest);
template <DigitCase digit_case>
char* WriteHex8(uint32_t src, char* dest);
template <DigitCase digit_case>
char* WriteHex16(uint64_t src, char* dest);
template <DigitCase digit_case>
char* WriteHex32(absl::uint128 src, char* dest);

template <>
char* WriteHex2<DigitCase::kLower>(uint8_t src, char* dest);
template <>
char* WriteHex4<DigitCase::kLower>(uint16_t src, char* dest);
template <>
char* WriteHex8<DigitCase::kLower>(uint32_t src, char* dest);
template <>
char* WriteHex16<DigitCase::kLower>(uint64_t src, char* dest);
template <>
char* WriteHex32<DigitCase::kLower>(absl::uint128 src, char* dest);

template <>
char* WriteHex2<DigitCase::kUpper>(uint8_t src, char* dest);
template <>
char* WriteHex4<DigitCase::kUpper>(uint16_t src, char* dest);
template <>
char* WriteHex8<DigitCase::kUpper>(uint32_t src, char* dest);
template <>
char* WriteHex16<DigitCase::kUpper>(uint64_t src, char* dest);
template <>
char* WriteHex32<DigitCase::kUpper>(absl::uint128 src, char* dest);

// `WriteHex()` with no width parameter writes no leading zeros, except for 0
// itself.
template <DigitCase digit_case>
char* WriteHex(uint8_t src, char* dest);
template <DigitCase digit_case>
char* WriteHex(uint16_t src, char* dest);
template <DigitCase digit_case>
char* WriteHex(uint32_t src, char* dest);
template <DigitCase digit_case>
char* WriteHex(uint64_t src, char* dest);
template <DigitCase digit_case>
char* WriteHex(absl::uint128 src, char* dest);

template <>
char* WriteHex<DigitCase::kLower>(uint8_t src, char* dest);
template <>
char* WriteHex<DigitCase::kLower>(uint16_t src, char* dest);
template <>
char* WriteHex<DigitCase::kLower>(uint32_t src, char* dest);
template <>
char* WriteHex<DigitCase::kLower>(uint64_t src, char* dest);
template <>
char* WriteHex<DigitCase::kLower>(absl::uint128 src, char* dest);

template <>
char* WriteHex<DigitCase::kUpper>(uint8_t src, char* dest);
template <>
char* WriteHex<DigitCase::kUpper>(uint16_t src, char* dest);
template <>
char* WriteHex<DigitCase::kUpper>(uint32_t src, char* dest);
template <>
char* WriteHex<DigitCase::kUpper>(uint64_t src, char* dest);
template <>
char* WriteHex<DigitCase::kUpper>(absl::uint128 src, char* dest);

// `WriteHex()` with a width parameter writes at least `width` digits.
template <DigitCase digit_case>
char* WriteHex(uint8_t src, char* dest, size_t width);
template <DigitCase digit_case>
char* WriteHex(uint16_t src, char* dest, size_t width);
template <DigitCase digit_case>
char* WriteHex(uint32_t src, char* dest, size_t width);
template <DigitCase digit_case>
char* WriteHex(uint64_t src, char* dest, size_t width);
template <DigitCase digit_case>
char* WriteHex(absl::uint128 src, char* dest, size_t width);

template <>
char* WriteHex<DigitCase::kLower>(uint8_t src, char* dest, size_t width);
template <>
char* WriteHex<DigitCase::kLower>(uint16_t src, char* dest, size_t width);
template <>
char* WriteHex<DigitCase::kLower>(uint32_t src, char* dest, size_t width);
template <>
char* WriteHex<DigitCase::kLower>(uint64_t src, char* dest, size_t width);
template <>
char* WriteHex<DigitCase::kLower>(absl::uint128 src, char* dest, size_t width);

template <>
char* WriteHex<DigitCase::kUpper>(uint8_t src, char* dest, size_t width);
template <>
char* WriteHex<DigitCase::kUpper>(uint16_t src, char* dest, size_t width);
template <>
char* WriteHex<DigitCase::kUpper>(uint32_t src, char* dest, size_t width);
template <>
char* WriteHex<DigitCase::kUpper>(uint64_t src, char* dest, size_t width);
template <>
char* WriteHex<DigitCase::kUpper>(absl::uint128 src, char* dest, size_t width);

// `WriteHexUnsigned()` writes at least `width` digits.

template <DigitCase digit_case, typename T,
          std::enable_if_t<FitsIn<T, uint8_t>::value, int> = 0>
inline char* WriteHexUnsigned(T src, char* dest, size_t width) {
  return width == 2 ? WriteHex2<digit_case>(IntCast<uint8_t>(src), dest)
         : width <= 1
             ? WriteHex<digit_case>(IntCast<uint8_t>(src), dest)
             : WriteHex<digit_case>(IntCast<uint8_t>(src), dest, width);
}

template <DigitCase digit_case, typename T,
          std::enable_if_t<absl::conjunction<absl::negation<FitsIn<T, uint8_t>>,
                                             FitsIn<T, uint16_t>>::value,
                           int> = 0>
inline char* WriteHexUnsigned(T src, char* dest, size_t width) {
  return width == 4 ? WriteHex4<digit_case>(IntCast<uint16_t>(src), dest)
         : width <= 1
             ? WriteHex<digit_case>(IntCast<uint16_t>(src), dest)
             : WriteHex<digit_case>(IntCast<uint16_t>(src), dest, width);
}

template <
    DigitCase digit_case, typename T,
    std::enable_if_t<absl::conjunction<absl::negation<FitsIn<T, uint16_t>>,
                                       FitsIn<T, uint32_t>>::value,
                     int> = 0>
inline char* WriteHexUnsigned(T src, char* dest, size_t width) {
  return width == 8 ? WriteHex8<digit_case>(IntCast<uint32_t>(src), dest)
         : width <= 1
             ? WriteHex<digit_case>(IntCast<uint32_t>(src), dest)
             : WriteHex<digit_case>(IntCast<uint32_t>(src), dest, width);
}

template <
    DigitCase digit_case, typename T,
    std::enable_if_t<absl::conjunction<absl::negation<FitsIn<T, uint32_t>>,
                                       FitsIn<T, uint64_t>>::value,
                     int> = 0>
inline char* WriteHexUnsigned(T src, char* dest, size_t width) {
  return width == 16 ? WriteHex16<digit_case>(IntCast<uint64_t>(src), dest)
         : width <= 1
             ? WriteHex<digit_case>(IntCast<uint64_t>(src), dest)
             : WriteHex<digit_case>(IntCast<uint64_t>(src), dest, width);
}

template <
    DigitCase digit_case, typename T,
    std::enable_if_t<absl::conjunction<absl::negation<FitsIn<T, uint64_t>>,
                                       FitsIn<T, absl::uint128>>::value,
                     int> = 0>
inline char* WriteHexUnsigned(T src, char* dest, size_t width) {
  return width == 32 ? WriteHex32<digit_case>(IntCast<absl::uint128>(src), dest)
         : width <= 1
             ? WriteHex<digit_case>(IntCast<absl::uint128>(src), dest)
             : WriteHex<digit_case>(IntCast<absl::uint128>(src), dest, width);
}

// `WriteHexBackward{2,4,8,16,32}()` writes a fixed number of digits.
template <DigitCase digit_case>
void WriteHexBackward2(uint8_t src, char* dest);
template <DigitCase digit_case>
void WriteHexBackward4(uint16_t src, char* dest);
template <DigitCase digit_case>
void WriteHexBackward8(uint32_t src, char* dest);
template <DigitCase digit_case>
void WriteHexBackward16(uint64_t src, char* dest);
template <DigitCase digit_case>
void WriteHexBackward32(absl::uint128 src, char* dest);

template <>
void WriteHexBackward2<DigitCase::kLower>(uint8_t src, char* dest);
template <>
void WriteHexBackward4<DigitCase::kLower>(uint16_t src, char* dest);
template <>
void WriteHexBackward8<DigitCase::kLower>(uint32_t src, char* dest);
template <>
void WriteHexBackward16<DigitCase::kLower>(uint64_t src, char* dest);
template <>
void WriteHexBackward32<DigitCase::kLower>(absl::uint128 src, char* dest);

template <>
void WriteHexBackward2<DigitCase::kUpper>(uint8_t src, char* dest);
template <>
void WriteHexBackward4<DigitCase::kUpper>(uint16_t src, char* dest);
template <>
void WriteHexBackward8<DigitCase::kUpper>(uint32_t src, char* dest);
template <>
void WriteHexBackward16<DigitCase::kUpper>(uint64_t src, char* dest);
template <>
void WriteHexBackward32<DigitCase::kUpper>(absl::uint128 src, char* dest);

template <typename T>
constexpr size_t MaxLengthWriteHexUnsignedBackward() {
  return FitsIn<T, uint8_t>::value    ? 2
         : FitsIn<T, uint16_t>::value ? 4
         : FitsIn<T, uint32_t>::value ? 8
         : FitsIn<T, uint64_t>::value ? 16
                                      : 32;
}

// `WriteHexUnsignedBackward<T>()` writes at least `width` digits.
//
// `width` must be at most `MaxLengthWriteHexUnsignedBackward<T>()`, and that
// much space must be available before `dest`.

template <DigitCase digit_case, typename T,
          std::enable_if_t<FitsIn<T, uint8_t>::value, int> = 0>
inline char* WriteHexUnsignedBackward(T src, char* dest, size_t width) {
  RIEGELI_ASSERT_LE(width, 2u)
      << "Failed precondition of WriteHexUnsignedBackward(): width too large";
  WriteHexBackward2<digit_case>(IntCast<uint8_t>(src), dest);
  width =
      UnsignedMax(width, IntCast<uint8_t>(src) < 0x10 ? size_t{1} : size_t{2});
  return dest - width;
}

template <DigitCase digit_case, typename T,
          std::enable_if_t<absl::conjunction<absl::negation<FitsIn<T, uint8_t>>,
                                             FitsIn<T, uint16_t>>::value,
                           int> = 0>
inline char* WriteHexUnsignedBackward(T src, char* dest, size_t width) {
  RIEGELI_ASSERT_LE(width, 4u)
      << "Failed precondition of WriteHexUnsignedBackward(): width too large";
  WriteHexBackward4<digit_case>(IntCast<uint16_t>(src), dest);
  width = UnsignedMax(width, (IntCast<size_t>(absl::bit_width(IntCast<uint16_t>(
                                  IntCast<uint16_t>(src) | 1))) +
                              3) /
                                 4);
  return dest - width;
}

template <
    DigitCase digit_case, typename T,
    std::enable_if_t<absl::conjunction<absl::negation<FitsIn<T, uint16_t>>,
                                       FitsIn<T, uint32_t>>::value,
                     int> = 0>
inline char* WriteHexUnsignedBackward(T src, char* dest, size_t width) {
  RIEGELI_ASSERT_LE(width, 8u)
      << "Failed precondition of WriteHexUnsignedBackward(): width too large";
  WriteHexBackward8<digit_case>(IntCast<uint32_t>(src), dest);
  width = UnsignedMax(
      width,
      (IntCast<size_t>(absl::bit_width(IntCast<uint32_t>(src) | 1)) + 3) / 4);
  return dest - width;
}

template <
    DigitCase digit_case, typename T,
    std::enable_if_t<absl::conjunction<absl::negation<FitsIn<T, uint32_t>>,
                                       FitsIn<T, uint64_t>>::value,
                     int> = 0>
inline char* WriteHexUnsignedBackward(T src, char* dest, size_t width) {
  RIEGELI_ASSERT_LE(width, 16u)
      << "Failed precondition of WriteHexUnsignedBackward(): width too large";
  WriteHexBackward16<digit_case>(IntCast<uint64_t>(src), dest);
  width = UnsignedMax(
      width,
      (IntCast<size_t>(absl::bit_width(IntCast<uint64_t>(src) | 1)) + 3) / 4);
  return dest - width;
}

template <
    DigitCase digit_case, typename T,
    std::enable_if_t<absl::conjunction<absl::negation<FitsIn<T, uint64_t>>,
                                       FitsIn<T, absl::uint128>>::value,
                     int> = 0>
inline char* WriteHexUnsignedBackward(T src, char* dest, size_t width) {
  RIEGELI_ASSERT_LE(width, 32u)
      << "Failed precondition of WriteHexUnsignedBackward(): width too large";
  WriteHexBackward32<digit_case>(IntCast<absl::uint128>(src), dest);
  width = UnsignedMax(
      width,
      (IntCast<size_t>(absl::Uint128High64(src) == 0
                           ? absl::bit_width(absl::Uint128Low64(src) | 1)
                           : absl::bit_width(absl::Uint128High64(src)) + 64) +
       3) /
          4);
  return dest - width;
}

}  // namespace write_int_internal

template <typename T>
template <typename Sink, typename DependentT,
          std::enable_if_t<IsUnsignedInt<DependentT>::value, int>>
inline void DecType<T>::Stringify(Sink& dest) const {
  // `digits10` is rounded down, `kMaxNumDigits` is rounded up, hence `+ 1`.
  constexpr size_t kMaxNumDigits = std::numeric_limits<T>::digits10 + 1;
  char str[kMaxNumDigits];
  char* const begin =
      write_int_internal::WriteDecUnsignedBackward(value_, str + kMaxNumDigits);
  const size_t length = PtrDistance(begin, str + kMaxNumDigits);
  if (width_ > length) dest.Append(width_ - length, '0');
  dest.Append(absl::string_view(begin, length));
}

template <typename T>
template <typename Sink, typename DependentT,
          std::enable_if_t<IsSignedInt<DependentT>::value, int>>
inline void DecType<T>::Stringify(Sink& dest) const {
  // `digits10` is rounded down, `kMaxNumDigits` is rounded up, hence `+ 1`.
  constexpr size_t kMaxNumDigits = std::numeric_limits<T>::digits10 + 1;
  // `+ 1` for the minus sign.
  char str[kMaxNumDigits + 1];
  char* begin;
  size_t length;
  if (value_ >= 0) {
    begin = write_int_internal::WriteDecUnsignedBackward(
        UnsignedCast(value_), str + (kMaxNumDigits + 1));
    length = PtrDistance(begin, str + (kMaxNumDigits + 1));
    if (width_ > length) dest.Append(width_ - length, '0');
  } else {
    // Leave space for the minus sign.
    begin = write_int_internal::WriteDecUnsignedBackward(
                NegatingUnsignedCast(value_), str + (kMaxNumDigits + 1)) -
            1;
    length = PtrDistance(begin, str + (kMaxNumDigits + 1));
    if (width_ > length) {
      dest.Append("-");
      dest.Append(width_ - length, '0');
      ++begin;
      --length;
    } else {
      *begin = '-';
    }
  }
  dest.Append(absl::string_view(begin, length));
}

template <typename T>
template <typename DependentT,
          std::enable_if_t<IsUnsignedInt<DependentT>::value, int>>
inline void DecType<T>::WriteTo(Writer& dest) const {
  // `digits10` is rounded down, `kMaxNumDigits` is rounded up, hence `+ 1`.
  constexpr size_t kMaxNumDigits = std::numeric_limits<T>::digits10 + 1;
  if (ABSL_PREDICT_FALSE(!dest.Push(UnsignedMax(width_, kMaxNumDigits)))) {
    return;
  }
  dest.set_cursor(
      write_int_internal::WriteDecUnsigned(value_, dest.cursor(), width_));
}

template <typename T>
template <typename DependentT,
          std::enable_if_t<IsSignedInt<DependentT>::value, int>>
inline void DecType<T>::WriteTo(Writer& dest) const {
  // `digits10` is rounded down, `kMaxNumDigits` is rounded up, hence `+ 1`.
  constexpr size_t kMaxNumDigits = std::numeric_limits<T>::digits10 + 1;
  // `+ 1` for the minus sign.
  if (ABSL_PREDICT_FALSE(!dest.Push(UnsignedMax(width_, kMaxNumDigits + 1)))) {
    return;
  }
  dest.set_cursor(
      write_int_internal::WriteDecSigned(value_, dest.cursor(), width_));
}

template <typename T, DigitCase digit_case>
template <typename Sink, typename DependentT,
          std::enable_if_t<IsUnsignedInt<DependentT>::value, int>>
inline void HexType<T, digit_case>::Stringify(Sink& dest) const {
  constexpr size_t kMaxNumDigits =
      write_int_internal::MaxLengthWriteHexUnsignedBackward<T>();
  size_t width = width_;
  if (width > kMaxNumDigits) {
    dest.Append(width - kMaxNumDigits, '0');
    width = kMaxNumDigits;
  }
  char str[kMaxNumDigits];
  char* const begin = write_int_internal::WriteHexUnsignedBackward<digit_case>(
      value_, str + kMaxNumDigits, width);
  dest.Append(
      absl::string_view(begin, PtrDistance(begin, str + kMaxNumDigits)));
}

template <typename T, DigitCase digit_case>
template <typename Sink, typename DependentT,
          std::enable_if_t<IsSignedInt<DependentT>::value, int>>
inline void HexType<T, digit_case>::Stringify(Sink& dest) const {
  constexpr size_t kMaxNumDigits =
      write_int_internal::MaxLengthWriteHexUnsignedBackward<MakeUnsignedT<T>>();
  size_t width = width_;
  // `+ 1` for the minus sign.
  char str[kMaxNumDigits + 1];
  char* begin;
  if (value_ >= 0) {
    if (width > kMaxNumDigits) {
      dest.Append(width - kMaxNumDigits, '0');
      width = kMaxNumDigits;
    }
    begin = write_int_internal::WriteHexUnsignedBackward<digit_case>(
        UnsignedCast(value_), str + (kMaxNumDigits + 1), width);
  } else if (width > kMaxNumDigits + 1) {
    dest.Append("-");
    dest.Append(width - (kMaxNumDigits + 1), '0');
    begin = write_int_internal::WriteHexUnsignedBackward<digit_case>(
        NegatingUnsignedCast(value_), str + (kMaxNumDigits + 1), kMaxNumDigits);
  } else {
    begin = write_int_internal::WriteHexUnsignedBackward<digit_case>(
        NegatingUnsignedCast(value_), str + (kMaxNumDigits + 1),
        SaturatingSub(width, size_t{1}));
    --begin;
    *begin = '-';
  }
  dest.Append(
      absl::string_view(begin, PtrDistance(begin, str + (kMaxNumDigits + 1))));
}

template <typename T, DigitCase digit_case>
template <typename DependentT,
          std::enable_if_t<IsUnsignedInt<DependentT>::value, int>>
inline void HexType<T, digit_case>::WriteTo(Writer& dest) const {
  constexpr size_t kMaxNumDigits = (std::numeric_limits<T>::digits + 3) / 4;
  if (ABSL_PREDICT_FALSE(!dest.Push(UnsignedMax(width_, kMaxNumDigits)))) {
    return;
  }
  dest.set_cursor(write_int_internal::WriteHexUnsigned<digit_case>(
      value_, dest.cursor(), width_));
}

template <typename T, DigitCase digit_case>
template <typename DependentT,
          std::enable_if_t<IsSignedInt<DependentT>::value, int>>
inline void HexType<T, digit_case>::WriteTo(Writer& dest) const {
  constexpr size_t kMaxNumDigits = (std::numeric_limits<T>::digits + 3) / 4;
  // `+ 1` for the minus sign.
  if (ABSL_PREDICT_FALSE(!dest.Push(UnsignedMax(width_, kMaxNumDigits + 1)))) {
    return;
  }
  MakeUnsignedT<T> abs_value;
  char* cursor = dest.cursor();
  size_t width = width_;
  if (value_ >= 0) {
    abs_value = UnsignedCast(value_);
  } else {
    *cursor = '-';
    ++cursor;
    abs_value = NegatingUnsignedCast(value_);
    width = SaturatingSub(width, size_t{1});
  }
  dest.set_cursor(write_int_internal::WriteHexUnsigned<digit_case>(
      abs_value, cursor, width));
}

}  // namespace riegeli

#endif  // RIEGELI_TEXT_WRITE_INT_H_
