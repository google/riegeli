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

#ifndef RIEGELI_BASE_ARITHMETIC_H_
#define RIEGELI_BASE_ARITHMETIC_H_

#include <stddef.h>

#include <limits>
#include <type_traits>

#include "absl/base/optimization.h"
#include "absl/meta/type_traits.h"
#include "absl/numeric/bits.h"
#include "absl/numeric/int128.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/type_traits.h"

namespace riegeli {

// `IsUnsignedInt<T>::value` is `true` for unsigned integral types, including
// `absl::uint128`.
template <typename T>
struct IsUnsignedInt
    : absl::conjunction<std::is_integral<T>, std::is_unsigned<T>> {};
template <>
struct IsUnsignedInt<absl::uint128> : std::true_type {};

// `IsSignedInt<T>::value` is `true` for signed integral types, including
// `absl::int128`.
template <typename T>
struct IsSignedInt : absl::conjunction<std::is_integral<T>, std::is_signed<T>> {
};
template <>
struct IsSignedInt<absl::int128> : std::true_type {};

// `IsInt<T>::value` is `true` for integral types, including `absl::uint128` and
// `absl::int128`.
template <typename T>
struct IsInt : std::is_integral<T> {};
template <>
struct IsInt<absl::uint128> : std::true_type {};
template <>
struct IsInt<absl::int128> : std::true_type {};

// `MakeUnsigned<T>::type` and `MakeUnsignedT<T>` transform a signed integral
// type to the corresponding unsigned type, including `absl::int128`, and leave
// unsigned integral types unchanged, including `absl::uint128`.
template <typename T>
struct MakeUnsigned : std::make_unsigned<T> {};

template <>
struct MakeUnsigned<absl::int128> {
  using type = absl::uint128;
};

template <>
struct MakeUnsigned<absl::uint128> {
  using type = absl::uint128;
};

template <typename T>
using MakeUnsignedT = typename MakeUnsigned<T>::type;

// `IntCast<A>(value)` converts between integral types, asserting that `value`
// fits in the target type.

template <
    typename A, typename B,
    std::enable_if_t<
        absl::conjunction<IsUnsignedInt<A>, IsUnsignedInt<B>>::value, int> = 0>
inline A IntCast(B value) {
  RIEGELI_ASSERT_LE(value, std::numeric_limits<A>::max())
      << "Failed precondition of IntCast(): value out of range";
  return static_cast<A>(value);
}

template <
    typename A, typename B,
    std::enable_if_t<absl::conjunction<IsUnsignedInt<A>, IsSignedInt<B>>::value,
                     int> = 0>
inline A IntCast(B value) {
  RIEGELI_ASSERT_GE(value, 0)
      << "Failed precondition of IntCast(): value out of range";
  RIEGELI_ASSERT_LE(static_cast<MakeUnsignedT<B>>(value),
                    std::numeric_limits<A>::max())
      << "Failed precondition of IntCast(): value out of range";
  return static_cast<A>(value);
}

template <
    typename A, typename B,
    std::enable_if_t<absl::conjunction<IsSignedInt<A>, IsUnsignedInt<B>>::value,
                     int> = 0>
inline A IntCast(B value) {
  RIEGELI_ASSERT_LE(value, MakeUnsignedT<A>{std::numeric_limits<A>::max()})
      << "Failed precondition of IntCast(): value out of range";
  return static_cast<A>(value);
}

template <
    typename A, typename B,
    std::enable_if_t<absl::conjunction<IsSignedInt<A>, IsSignedInt<B>>::value,
                     int> = 0>
inline A IntCast(B value) {
  RIEGELI_ASSERT_GE(value, std::numeric_limits<A>::min())
      << "Failed precondition of IntCast(): value out of range";
  RIEGELI_ASSERT_LE(value, std::numeric_limits<A>::max())
      << "Failed precondition of IntCast(): value out of range";
  return static_cast<A>(value);
}

// `UnsignedCast(value)` converts `value` to the corresponding unsigned type,
// asserting that `value` was non-negative.
template <
    typename T,
    std::enable_if_t<absl::disjunction<IsSignedInt<T>, IsUnsignedInt<T>>::value,
                     int> = 0>
inline MakeUnsignedT<T> UnsignedCast(T value) {
  return IntCast<MakeUnsignedT<T>>(value);
}

// `NegatingUnsignedCast(value)` converts `-value` to the corresponding unsigned
// type, asserting that `value` was non-positive, and correctly handling
// `std::numeric_limits<T>::min()`.
template <typename T, std::enable_if_t<IsSignedInt<T>::value, int> = 0>
inline MakeUnsignedT<T> NegatingUnsignedCast(T value) {
  RIEGELI_ASSERT_LE(value, 0)
      << "Failed precondition of NegatingUnsignedCast(): positive value";
  // Negate in the unsigned space to correctly handle
  // `std::numeric_limits<T>::min()`.
  return static_cast<MakeUnsignedT<T>>(0 -
                                       static_cast<MakeUnsignedT<T>>(value));
}

// `SignedMin()` returns the minimum of its arguments, which must be signed
// integers, as their widest type.

template <typename A, std::enable_if_t<IsSignedInt<A>::value, int> = 0>
constexpr A SignedMin(A a) {
  return a;
}

template <
    typename A, typename B,
    std::enable_if_t<absl::conjunction<IsSignedInt<A>, IsSignedInt<B>>::value,
                     int> = 0>
constexpr std::common_type_t<A, B> SignedMin(A a, B b) {
  return a <= b ? a : b;
}

template <typename A, typename B, typename... Rest,
          std::enable_if_t<
              absl::conjunction<
                  std::integral_constant<bool, (sizeof...(Rest) > 0)>,
                  IsSignedInt<A>, IsSignedInt<B>, IsSignedInt<Rest>...>::value,
              int> = 0>
constexpr std::common_type_t<A, B, Rest...> SignedMin(A a, B b, Rest... rest) {
  return SignedMin(SignedMin(a, b), rest...);
}

// `SignedMax()` returns the maximum of its arguments, which must be signed
// integers, as their widest type.

template <typename A, std::enable_if_t<IsSignedInt<A>::value, int> = 0>
constexpr A SignedMax(A a) {
  return a;
}

template <
    typename A, typename B,
    std::enable_if_t<absl::conjunction<IsSignedInt<A>, IsSignedInt<B>>::value,
                     int> = 0>
constexpr std::common_type_t<A, B> SignedMax(A a, B b) {
  return a >= b ? a : b;
}

template <typename A, typename B, typename... Rest,
          std::enable_if_t<
              absl::conjunction<
                  std::integral_constant<bool, (sizeof...(Rest) > 0)>,
                  IsSignedInt<A>, IsSignedInt<B>, IsSignedInt<Rest>...>::value,
              int> = 0>
constexpr std::common_type_t<A, B, Rest...> SignedMax(A a, B b, Rest... rest) {
  return SignedMax(SignedMax(a, b), rest...);
}

// `UnsignedMin()` returns the minimum of its arguments, which must be unsigned
// integers, as their narrowest type.

template <typename A, std::enable_if_t<IsUnsignedInt<A>::value, int> = 0>
constexpr A UnsignedMin(A a) {
  return a;
}

template <
    typename A, typename B,
    std::enable_if_t<
        absl::conjunction<IsUnsignedInt<A>, IsUnsignedInt<B>>::value, int> = 0>
constexpr IntersectionTypeT<A, B> UnsignedMin(A a, B b) {
  return static_cast<IntersectionTypeT<A, B>>(a <= b ? a : b);
}

template <
    typename A, typename B, typename... Rest,
    std::enable_if_t<
        absl::conjunction<std::integral_constant<bool, (sizeof...(Rest) > 0)>,
                          IsUnsignedInt<A>, IsUnsignedInt<B>,
                          IsUnsignedInt<Rest>...>::value,
        int> = 0>
constexpr IntersectionTypeT<A, B, Rest...> UnsignedMin(A a, B b, Rest... rest) {
  return UnsignedMin(UnsignedMin(a, b), rest...);
}

// `UnsignedMax()` returns the maximum of its arguments, which must be unsigned
// integers, as their widest type.

template <typename A, std::enable_if_t<IsUnsignedInt<A>::value, int> = 0>
constexpr A UnsignedMax(A a) {
  return a;
}

template <
    typename A, typename B,
    std::enable_if_t<
        absl::conjunction<IsUnsignedInt<A>, IsUnsignedInt<B>>::value, int> = 0>
constexpr std::common_type_t<A, B> UnsignedMax(A a, B b) {
  return a >= b ? a : b;
}

template <
    typename A, typename B, typename... Rest,
    std::enable_if_t<
        absl::conjunction<std::integral_constant<bool, (sizeof...(Rest) > 0)>,
                          IsUnsignedInt<A>, IsUnsignedInt<B>,
                          IsUnsignedInt<Rest>...>::value,
        int> = 0>
constexpr std::common_type_t<A, B, Rest...> UnsignedMax(A a, B b,
                                                        Rest... rest) {
  return UnsignedMax(UnsignedMax(a, b), rest...);
}

// `UnsignedClamp(value, min_value, max_value)` is at least `min_value`,
// at most `max(max_value, min_value)`, preferably `value`.
template <
    typename Value, typename Min, typename Max,
    std::enable_if_t<absl::conjunction<IsUnsignedInt<Value>, IsUnsignedInt<Min>,
                                       IsUnsignedInt<Max>>::value,
                     int> = 0>
inline std::common_type_t<IntersectionTypeT<Value, Max>, Min> UnsignedClamp(
    Value value, Min min, Max max) {
  return UnsignedMax(UnsignedMin(value, max), min);
}

// `SaturatingIntCast()` converts an integer value to another integer type, or
// returns the appropriate bound of the type if conversion would overflow.

template <
    typename A, typename B,
    std::enable_if_t<
        absl::conjunction<IsUnsignedInt<A>, IsUnsignedInt<B>>::value, int> = 0>
inline A SaturatingIntCast(B value) {
  if (ABSL_PREDICT_FALSE(value > std::numeric_limits<A>::max())) {
    return std::numeric_limits<A>::max();
  }
  return static_cast<A>(value);
}

template <
    typename A, typename B,
    std::enable_if_t<absl::conjunction<IsUnsignedInt<A>, IsSignedInt<B>>::value,
                     int> = 0>
inline A SaturatingIntCast(B value) {
  if (ABSL_PREDICT_FALSE(value < 0)) return 0;
  if (ABSL_PREDICT_FALSE(static_cast<MakeUnsignedT<B>>(value) >
                         std::numeric_limits<A>::max())) {
    return std::numeric_limits<A>::max();
  }
  return static_cast<A>(value);
}

template <
    typename A, typename B,
    std::enable_if_t<absl::conjunction<IsSignedInt<A>, IsUnsignedInt<B>>::value,
                     int> = 0>
inline A SaturatingIntCast(B value) {
  if (ABSL_PREDICT_FALSE(value >
                         MakeUnsignedT<A>{std::numeric_limits<A>::max()})) {
    return std::numeric_limits<A>::max();
  }
  return static_cast<A>(value);
}

template <
    typename A, typename B,
    std::enable_if_t<absl::conjunction<IsSignedInt<A>, IsSignedInt<B>>::value,
                     int> = 0>
inline A SaturatingIntCast(B value) {
  if (ABSL_PREDICT_FALSE(value < std::numeric_limits<A>::min())) {
    return std::numeric_limits<A>::min();
  }
  if (ABSL_PREDICT_FALSE(value > std::numeric_limits<A>::max())) {
    return std::numeric_limits<A>::max();
  }
  return static_cast<A>(value);
}

// `SaturatingAdd()` adds unsigned values, or returns max possible value of the
// type if addition would overflow.

template <typename T, std::enable_if_t<IsUnsignedInt<T>::value, int> = 0>
constexpr T SaturatingAdd() {
  return 0;
}

template <typename T, std::enable_if_t<IsUnsignedInt<T>::value, int> = 0>
constexpr T SaturatingAdd(T a) {
  return a;
}

template <typename T, std::enable_if_t<IsUnsignedInt<T>::value, int> = 0>
constexpr T SaturatingAdd(T a, T b) {
  return a + UnsignedMin(b, std::numeric_limits<T>::max() - a);
}

template <
    typename T, typename... Rest,
    std::enable_if_t<
        absl::conjunction<std::integral_constant<bool, (sizeof...(Rest) > 0)>,
                          IsUnsignedInt<T>, IsUnsignedInt<Rest>...>::value,
        int> = 0>
constexpr T SaturatingAdd(T a, T b, Rest... rest) {
  return SaturatingAdd(SaturatingAdd(a, b), rest...);
}

// `SaturatingSub()` subtracts unsigned values, or returns 0 if subtraction
// would underflow.
template <
    typename T, typename U,
    std::enable_if_t<
        absl::conjunction<IsUnsignedInt<T>, IsUnsignedInt<U>>::value, int> = 0>
constexpr T SaturatingSub(T a, U b) {
  return a - UnsignedMin(b, a);
}

// `RoundDown()` rounds an unsigned value downwards to the nearest multiple of
// the given power of 2.
template <
    size_t alignment, typename T,
    std::enable_if_t<
        absl::conjunction<IsUnsignedInt<T>,
                          std::integral_constant<
                              bool, absl::has_single_bit(alignment)>>::value,
        int> = 0>
constexpr T RoundDown(T value) {
  return value & ~T{alignment - 1};
}

// `RoundUp()` rounds an unsigned value upwards to the nearest multiple of the
// given power of 2.
template <
    size_t alignment, typename T,
    std::enable_if_t<
        absl::conjunction<IsUnsignedInt<T>,
                          std::integral_constant<
                              bool, absl::has_single_bit(alignment)>>::value,
        int> = 0>
constexpr T RoundUp(T value) {
  return ((value - 1) | T{alignment - 1}) + 1;
}

// `PtrDistance(first, last)` returns `last - first` as `size_t`, asserting that
// `first <= last`.
template <typename A>
inline size_t PtrDistance(const A* first, const A* last) {
  RIEGELI_ASSERT(first <= last)
      << "Failed invariant of PtrDistance(): pointers in the wrong order";
  return static_cast<size_t>(last - first);
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_ARITHMETIC_H_
