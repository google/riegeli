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
#include <type_traits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/numeric/bits.h"
#include "absl/numeric/int128.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
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
  friend void AbslStringify(Sink& sink, const DecType& self) {
    self.AbslStringifyImpl(sink);
  }

 private:
  template <typename Sink, typename DependentT = T,
            std::enable_if_t<IsUnsignedInt<DependentT>::value, int> = 0>
  void AbslStringifyImpl(Sink& sink) const;
  template <typename Sink, typename DependentT = T,
            std::enable_if_t<IsSignedInt<DependentT>::value, int> = 0>
  void AbslStringifyImpl(Sink& sink) const;
  // Faster implementations if `Sink` is `WriterAbslStringifySink`.
  template <typename DependentT = T,
            std::enable_if_t<IsUnsignedInt<DependentT>::value, int> = 0>
  void AbslStringifyImpl(WriterAbslStringifySink& sink) const;
  template <typename DependentT = T,
            std::enable_if_t<IsSignedInt<DependentT>::value, int> = 0>
  void AbslStringifyImpl(WriterAbslStringifySink& sink) const;

  T value_;
  size_t width_;
};

// Wraps an integer such that its stringified representation is padded with
// zeros to at least the given width.
//
// For negative numbers the width includes the minus sign.
//
// If no minimum width is needed, integers can be stringified directly, without
// wrapping.
template <typename T>
inline DecType<T> Dec(T value, size_t width = 0) {
  return DecType<T>(value, width);
}

// The type returned by `Hex()`.
template <typename T>
class HexType {
 public:
  explicit HexType(T value, size_t width)
      : value_(std::move(value)), width_(width) {}

  const T& value() const { return value_; }
  size_t width() const { return width_; }

  template <typename Sink, typename DependentT = T,
            std::enable_if_t<IsInt<DependentT>::value, int> = 0>
  friend void AbslStringify(Sink& sink, const HexType& self) {
    self.AbslStringifyImpl(sink);
  }

 private:
  template <typename Sink, typename DependentT = T,
            std::enable_if_t<IsUnsignedInt<DependentT>::value, int> = 0>
  void AbslStringifyImpl(Sink& sink) const;
  template <typename Sink, typename DependentT = T,
            std::enable_if_t<IsSignedInt<DependentT>::value, int> = 0>
  void AbslStringifyImpl(Sink& sink) const;
  // Faster implementations if `Sink` is `WriterAbslStringifySink`.
  template <typename DependentT = T,
            std::enable_if_t<IsUnsignedInt<DependentT>::value, int> = 0>
  void AbslStringifyImpl(WriterAbslStringifySink& sink) const;
  template <typename DependentT = T,
            std::enable_if_t<IsSignedInt<DependentT>::value, int> = 0>
  void AbslStringifyImpl(WriterAbslStringifySink& sink) const;

  T value_;
  size_t width_;
};

// Wraps an integer such that its stringified representation is hexadecimal,
// padded with zeros to at least the given width.
//
// For negative numbers the width includes the minus sign.
template <typename T>
inline HexType<T> Hex(T value, size_t width = 0) {
  return HexType<T>(value, width);
}

// Implementation details follow.

namespace write_int_internal {

// `WriteHex{2,4,8,16,32}()` writes a fixed number of digits.
char* WriteHex2(uint8_t src, char* dest);
char* WriteHex4(uint16_t src, char* dest);
char* WriteHex8(uint32_t src, char* dest);
char* WriteHex16(uint64_t src, char* dest);
char* WriteHex32(absl::uint128 src, char* dest);

// `WriteHex()` with no width parameter writes no leading zeros, except for 0
// itself.
char* WriteHex(uint8_t src, char* dest);
char* WriteHex(uint16_t src, char* dest);
char* WriteHex(uint32_t src, char* dest);
char* WriteHex(uint64_t src, char* dest);
char* WriteHex(absl::uint128 src, char* dest);

// `WriteHex()` with a width parameter writes at least `width` digits.
char* WriteHex(uint8_t src, char* dest, size_t width);
char* WriteHex(uint16_t src, char* dest, size_t width);
char* WriteHex(uint32_t src, char* dest, size_t width);
char* WriteHex(uint64_t src, char* dest, size_t width);
char* WriteHex(absl::uint128 src, char* dest, size_t width);

// `WriteHexUnsigned()` writes at least `width` digits.

template <typename T, std::enable_if_t<(std::numeric_limits<T>::max() <=
                                        std::numeric_limits<uint8_t>::max()),
                                       int> = 0>
inline char* WriteHexUnsigned(T src, char* dest, size_t width) {
  return width == 2   ? WriteHex2(IntCast<uint8_t>(src), dest)
         : width <= 1 ? WriteHex(IntCast<uint8_t>(src), dest)
                      : WriteHex(IntCast<uint8_t>(src), dest, width);
}

template <
    typename T,
    std::enable_if_t<
        (std::numeric_limits<T>::max() > std::numeric_limits<uint8_t>::max() &&
         std::numeric_limits<T>::max() <= std::numeric_limits<uint16_t>::max()),
        int> = 0>
inline char* WriteHexUnsigned(T src, char* dest, size_t width) {
  return width == 4   ? WriteHex4(IntCast<uint16_t>(src), dest)
         : width <= 1 ? WriteHex(IntCast<uint16_t>(src), dest)
                      : WriteHex(IntCast<uint16_t>(src), dest, width);
}

template <
    typename T,
    std::enable_if_t<
        (std::numeric_limits<T>::max() > std::numeric_limits<uint16_t>::max() &&
         std::numeric_limits<T>::max() <= std::numeric_limits<uint32_t>::max()),
        int> = 0>
inline char* WriteHexUnsigned(T src, char* dest, size_t width) {
  return width == 8   ? WriteHex8(IntCast<uint32_t>(src), dest)
         : width <= 1 ? WriteHex(IntCast<uint32_t>(src), dest)
                      : WriteHex(IntCast<uint32_t>(src), dest, width);
}

template <
    typename T,
    std::enable_if_t<
        (std::numeric_limits<T>::max() > std::numeric_limits<uint32_t>::max() &&
         std::numeric_limits<T>::max() <= std::numeric_limits<uint64_t>::max()),
        int> = 0>
inline char* WriteHexUnsigned(T src, char* dest, size_t width) {
  return width == 16  ? WriteHex16(IntCast<uint64_t>(src), dest)
         : width <= 1 ? WriteHex(IntCast<uint64_t>(src), dest)
                      : WriteHex(IntCast<uint64_t>(src), dest, width);
}

template <typename T,
          std::enable_if_t<(std::numeric_limits<T>::max() >
                                std::numeric_limits<uint64_t>::max() &&
                            std::numeric_limits<T>::max() <=
                                std::numeric_limits<absl::uint128>::max()),
                           int> = 0>
inline char* WriteHexUnsigned(T src, char* dest, size_t width) {
  return width == 32  ? WriteHex32(IntCast<absl::uint128>(src), dest)
         : width <= 1 ? WriteHex(IntCast<absl::uint128>(src), dest)
                      : WriteHex(IntCast<absl::uint128>(src), dest, width);
}

// `WriteHexBackward{2,4,8,16,32}()` writes a fixed number of digits.
void WriteHexBackward2(uint8_t src, char* dest);
void WriteHexBackward4(uint16_t src, char* dest);
void WriteHexBackward8(uint32_t src, char* dest);
void WriteHexBackward16(uint64_t src, char* dest);
void WriteHexBackward32(absl::uint128 src, char* dest);

template <typename T>
constexpr size_t MaxLengthWriteHexUnsignedBackward() {
  return std::numeric_limits<T>::max() <= std::numeric_limits<uint8_t>::max()
             ? 2
         : std::numeric_limits<T>::max() <= std::numeric_limits<uint16_t>::max()
             ? 4
         : std::numeric_limits<T>::max() <= std::numeric_limits<uint32_t>::max()
             ? 8
         : std::numeric_limits<T>::max() <= std::numeric_limits<uint64_t>::max()
             ? 16
             : 32;
}

// `WriteHexUnsignedBackward<T>()` writes at least `width` digits.
//
// `width` must be at most `MaxLengthWriteHexUnsignedBackward<T>()`, and that
// much space must be available before `dest`.

template <typename T, std::enable_if_t<(std::numeric_limits<T>::max() <=
                                        std::numeric_limits<uint8_t>::max()),
                                       int> = 0>
inline char* WriteHexUnsignedBackward(T src, char* dest, size_t width) {
  RIEGELI_ASSERT_LE(width, 2u)
      << "Failed precondition of WriteHexUnsignedBackward(): width too large";
  WriteHexBackward2(IntCast<uint8_t>(src), dest);
  width =
      UnsignedMax(width, IntCast<uint8_t>(src) < 0x10 ? size_t{1} : size_t{2});
  return dest - width;
}

template <
    typename T,
    std::enable_if_t<
        (std::numeric_limits<T>::max() > std::numeric_limits<uint8_t>::max() &&
         std::numeric_limits<T>::max() <= std::numeric_limits<uint16_t>::max()),
        int> = 0>
inline char* WriteHexUnsignedBackward(T src, char* dest, size_t width) {
  RIEGELI_ASSERT_LE(width, 4u)
      << "Failed precondition of WriteHexUnsignedBackward(): width too large";
  WriteHexBackward4(IntCast<uint16_t>(src), dest);
  width = UnsignedMax(width, (IntCast<size_t>(absl::bit_width(IntCast<uint16_t>(
                                  IntCast<uint16_t>(src) | 1))) +
                              3) /
                                 4);
  return dest - width;
}

template <
    typename T,
    std::enable_if_t<
        (std::numeric_limits<T>::max() > std::numeric_limits<uint16_t>::max() &&
         std::numeric_limits<T>::max() <= std::numeric_limits<uint32_t>::max()),
        int> = 0>
inline char* WriteHexUnsignedBackward(T src, char* dest, size_t width) {
  RIEGELI_ASSERT_LE(width, 8u)
      << "Failed precondition of WriteHexUnsignedBackward(): width too large";
  WriteHexBackward8(IntCast<uint32_t>(src), dest);
  width = UnsignedMax(
      width,
      (IntCast<size_t>(absl::bit_width(IntCast<uint32_t>(src) | 1)) + 3) / 4);
  return dest - width;
}

template <
    typename T,
    std::enable_if_t<
        (std::numeric_limits<T>::max() > std::numeric_limits<uint32_t>::max() &&
         std::numeric_limits<T>::max() <= std::numeric_limits<uint64_t>::max()),
        int> = 0>
inline char* WriteHexUnsignedBackward(T src, char* dest, size_t width) {
  RIEGELI_ASSERT_LE(width, 16u)
      << "Failed precondition of WriteHexUnsignedBackward(): width too large";
  WriteHexBackward16(IntCast<uint64_t>(src), dest);
  width = UnsignedMax(
      width,
      (IntCast<size_t>(absl::bit_width(IntCast<uint64_t>(src) | 1)) + 3) / 4);
  return dest - width;
}

template <typename T,
          std::enable_if_t<(std::numeric_limits<T>::max() >
                                std::numeric_limits<uint64_t>::max() &&
                            std::numeric_limits<T>::max() <=
                                std::numeric_limits<absl::uint128>::max()),
                           int> = 0>
inline char* WriteHexUnsignedBackward(T src, char* dest, size_t width) {
  RIEGELI_ASSERT_LE(width, 32u)
      << "Failed precondition of WriteHexUnsignedBackward(): width too large";
  WriteHexBackward32(IntCast<absl::uint128>(src), dest);
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
inline void DecType<T>::AbslStringifyImpl(Sink& sink) const {
  // `digits10` is rounded down, `kMaxNumDigits` is rounded up, hence `+ 1`.
  constexpr size_t kMaxNumDigits = std::numeric_limits<T>::digits10 + 1;
  char str[kMaxNumDigits];
  char* const begin =
      write_int_internal::WriteDecUnsignedBackward(value_, str + kMaxNumDigits);
  const size_t length = PtrDistance(begin, str + kMaxNumDigits);
  if (width_ > length) sink.Append(width_ - length, '0');
  sink.Append(absl::string_view(begin, length));
}

template <typename T>
template <typename Sink, typename DependentT,
          std::enable_if_t<IsSignedInt<DependentT>::value, int>>
inline void DecType<T>::AbslStringifyImpl(Sink& sink) const {
  // `digits10` is rounded down, `kMaxNumDigits` is rounded up, hence `+ 1`.
  constexpr size_t kMaxNumDigits = std::numeric_limits<T>::digits10 + 1;
  char str[kMaxNumDigits + 1];
  char* begin;
  size_t length;
  if (value_ >= 0) {
    begin = write_int_internal::WriteDecUnsignedBackward(
        UnsignedCast(value_), str + (kMaxNumDigits + 1));
    length = PtrDistance(begin, str + (kMaxNumDigits + 1));
    if (width_ > length) sink.Append(width_ - length, '0');
  } else {
    // Leave space for the minus sign.
    begin = write_int_internal::WriteDecUnsignedBackward(
                NegatingUnsignedCast(value_), str + (kMaxNumDigits + 1)) -
            1;
    length = PtrDistance(begin, str + (kMaxNumDigits + 1));
    if (width_ > length) {
      sink.Append("-");
      sink.Append(width_ - length, '0');
      ++begin;
      --length;
    } else {
      *begin = '-';
    }
  }
  sink.Append(absl::string_view(begin, length));
}

template <typename T>
template <typename DependentT,
          std::enable_if_t<IsUnsignedInt<DependentT>::value, int>>
inline void DecType<T>::AbslStringifyImpl(WriterAbslStringifySink& sink) const {
  // `digits10` is rounded down, `kMaxNumDigits` is rounded up, hence `+ 1`.
  constexpr size_t kMaxNumDigits = std::numeric_limits<T>::digits10 + 1;
  if (ABSL_PREDICT_FALSE(
          !sink.dest()->Push(UnsignedMax(width_, kMaxNumDigits)))) {
    return;
  }
  sink.dest()->set_cursor(write_int_internal::WriteDecUnsigned(
      value_, sink.dest()->cursor(), width_));
}

template <typename T>
template <typename DependentT,
          std::enable_if_t<IsSignedInt<DependentT>::value, int>>
inline void DecType<T>::AbslStringifyImpl(WriterAbslStringifySink& sink) const {
  // `digits10` is rounded down, `kMaxNumDigits` is rounded up, hence `+ 1`.
  constexpr size_t kMaxNumDigits = std::numeric_limits<T>::digits10 + 1;
  if (ABSL_PREDICT_FALSE(
          !sink.dest()->Push(UnsignedMax(width_, kMaxNumDigits + 1)))) {
    return;
  }
  MakeUnsignedT<T> abs_value;
  char* cursor = sink.dest()->cursor();
  size_t width = width_;
  if (value_ >= 0) {
    abs_value = UnsignedCast(value_);
  } else {
    *cursor = '-';
    ++cursor;
    abs_value = NegatingUnsignedCast(value_);
    width = SaturatingSub(width, size_t{1});
  }
  sink.dest()->set_cursor(
      write_int_internal::WriteDecUnsigned(abs_value, cursor, width));
}

template <typename T>
template <typename Sink, typename DependentT,
          std::enable_if_t<IsUnsignedInt<DependentT>::value, int>>
inline void HexType<T>::AbslStringifyImpl(Sink& sink) const {
  constexpr size_t kMaxNumDigits =
      write_int_internal::MaxLengthWriteHexUnsignedBackward<T>();
  size_t width = width_;
  if (width > kMaxNumDigits) {
    sink.Append(width - kMaxNumDigits, '0');
    width = kMaxNumDigits;
  }
  char str[kMaxNumDigits];
  char* const begin = write_int_internal::WriteHexUnsignedBackward(
      value_, str + kMaxNumDigits, width);
  sink.Append(
      absl::string_view(begin, PtrDistance(begin, str + kMaxNumDigits)));
}

template <typename T>
template <typename Sink, typename DependentT,
          std::enable_if_t<IsSignedInt<DependentT>::value, int>>
inline void HexType<T>::AbslStringifyImpl(Sink& sink) const {
  constexpr size_t kMaxNumDigits =
      write_int_internal::MaxLengthWriteHexUnsignedBackward<T>();
  size_t width = width_;
  char str[kMaxNumDigits + 1];
  char* begin;
  if (value_ >= 0) {
    if (width > kMaxNumDigits) {
      sink.Append(width - kMaxNumDigits, '0');
      width = kMaxNumDigits;
    }
    begin = write_int_internal::WriteHexUnsignedBackward(
        UnsignedCast(value_), str + (kMaxNumDigits + 1), width);
  } else if (width > kMaxNumDigits + 1) {
    sink.Append("-");
    sink.Append(width - (kMaxNumDigits + 1), '0');
    begin = write_int_internal::WriteHexUnsignedBackward(
        NegatingUnsignedCast(value_), str + (kMaxNumDigits + 1), kMaxNumDigits);
  } else {
    begin = write_int_internal::WriteHexUnsignedBackward(
        NegatingUnsignedCast(value_), str + (kMaxNumDigits + 1),
        SaturatingSub(width, size_t{1}));
    --begin;
    *begin = '-';
  }
  sink.Append(
      absl::string_view(begin, PtrDistance(begin, str + (kMaxNumDigits + 1))));
}

template <typename T>
template <typename DependentT,
          std::enable_if_t<IsUnsignedInt<DependentT>::value, int>>
inline void HexType<T>::AbslStringifyImpl(WriterAbslStringifySink& sink) const {
  constexpr size_t kMaxNumDigits = (std::numeric_limits<T>::digits + 3) / 4;
  if (ABSL_PREDICT_FALSE(
          !sink.dest()->Push(UnsignedMax(width_, kMaxNumDigits)))) {
    return;
  }
  sink.dest()->set_cursor(write_int_internal::WriteHexUnsigned(
      value_, sink.dest()->cursor(), width_));
}

template <typename T>
template <typename DependentT,
          std::enable_if_t<IsSignedInt<DependentT>::value, int>>
inline void HexType<T>::AbslStringifyImpl(WriterAbslStringifySink& sink) const {
  constexpr size_t kMaxNumDigits = (std::numeric_limits<T>::digits + 3) / 4;
  if (ABSL_PREDICT_FALSE(
          !sink.dest()->Push(UnsignedMax(width_, kMaxNumDigits + 1)))) {
    return;
  }
  MakeUnsignedT<T> abs_value;
  char* cursor = sink.dest()->cursor();
  size_t width = width_;
  if (value_ >= 0) {
    abs_value = UnsignedCast(value_);
  } else {
    *cursor = '-';
    ++cursor;
    abs_value = NegatingUnsignedCast(value_);
    width = SaturatingSub(width, size_t{1});
  }
  sink.dest()->set_cursor(
      write_int_internal::WriteHexUnsigned(abs_value, cursor, width));
}

}  // namespace riegeli

#endif  // RIEGELI_TEXT_WRITE_INT_H_
