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
#include "riegeli/base/assert.h"
#include "riegeli/base/type_traits.h"

namespace riegeli {

// `IntCast<A>(value)` converts between integral types, asserting that the value
// fits in the target type.

template <
    typename A, typename B,
    std::enable_if_t<std::is_unsigned<A>::value && std::is_unsigned<B>::value,
                     int> = 0>
inline A IntCast(B value) {
  RIEGELI_ASSERT_LE(value, std::numeric_limits<A>::max())
      << "Value out of range";
  return static_cast<A>(value);
}

template <typename A, typename B,
          std::enable_if_t<
              std::is_unsigned<A>::value && std::is_signed<B>::value, int> = 0>
inline A IntCast(B value) {
  RIEGELI_ASSERT_GE(value, 0) << "Value out of range";
  RIEGELI_ASSERT_LE(static_cast<std::make_unsigned_t<B>>(value),
                    std::numeric_limits<A>::max())
      << "Value out of range";
  return static_cast<A>(value);
}

template <typename A, typename B,
          std::enable_if_t<
              std::is_signed<A>::value && std::is_unsigned<B>::value, int> = 0>
inline A IntCast(B value) {
  RIEGELI_ASSERT_LE(value,
                    std::make_unsigned_t<A>{std::numeric_limits<A>::max()})
      << "Value out of range";
  return static_cast<A>(value);
}

template <typename A, typename B,
          std::enable_if_t<std::is_signed<A>::value && std::is_signed<B>::value,
                           int> = 0>
inline A IntCast(B value) {
  RIEGELI_ASSERT_GE(value, std::numeric_limits<A>::min())
      << "Value out of range";
  RIEGELI_ASSERT_LE(value, std::numeric_limits<A>::max())
      << "Value out of range";
  return static_cast<A>(value);
}

// `SignedMin()` returns the minimum of its arguments, which must be signed
// integers, as their widest type.

template <typename A, std::enable_if_t<std::is_signed<A>::value, int> = 0>
constexpr A SignedMin(A a) {
  return a;
}

template <typename A, typename B,
          std::enable_if_t<std::is_signed<A>::value && std::is_signed<B>::value,
                           int> = 0>
constexpr std::common_type_t<A, B> SignedMin(A a, B b) {
  return a <= b ? a : b;
}

template <
    typename A, typename B, typename... Rest,
    std::enable_if_t<(sizeof...(Rest) > 0 &&
                      absl::conjunction<std::is_signed<A>, std::is_signed<B>,
                                        std::is_signed<Rest>...>::value),
                     int> = 0>
constexpr std::common_type_t<A, B, Rest...> SignedMin(A a, B b, Rest... rest) {
  return SignedMin(SignedMin(a, b), rest...);
}

// `SignedMax()` returns the maximum of its arguments, which must be signed
// integers, as their widest type.

template <typename A, std::enable_if_t<std::is_signed<A>::value, int> = 0>
constexpr A SignedMax(A a) {
  return a;
}

template <typename A, typename B,
          std::enable_if_t<std::is_signed<A>::value && std::is_signed<B>::value,
                           int> = 0>
constexpr std::common_type_t<A, B> SignedMax(A a, B b) {
  return a >= b ? a : b;
}

template <
    typename A, typename B, typename... Rest,
    std::enable_if_t<(sizeof...(Rest) > 0 &&
                      absl::conjunction<std::is_signed<A>, std::is_signed<B>,
                                        std::is_signed<Rest>...>::value),
                     int> = 0>
constexpr std::common_type_t<A, B, Rest...> SignedMax(A a, B b, Rest... rest) {
  return SignedMax(SignedMax(a, b), rest...);
}

// `UnsignedMin()` returns the minimum of its arguments, which must be unsigned
// integers, as their narrowest type.

template <typename A, std::enable_if_t<std::is_unsigned<A>::value, int> = 0>
constexpr A UnsignedMin(A a) {
  return a;
}

template <
    typename A, typename B,
    std::enable_if_t<std::is_unsigned<A>::value && std::is_unsigned<B>::value,
                     int> = 0>
constexpr IntersectionTypeT<A, B> UnsignedMin(A a, B b) {
  return static_cast<IntersectionTypeT<A, B>>(a <= b ? a : b);
}

template <typename A, typename B, typename... Rest,
          std::enable_if_t<
              (sizeof...(Rest) > 0 &&
               absl::conjunction<std::is_unsigned<A>, std::is_unsigned<B>,
                                 std::is_unsigned<Rest>...>::value),
              int> = 0>
constexpr IntersectionTypeT<A, B, Rest...> UnsignedMin(A a, B b, Rest... rest) {
  return UnsignedMin(UnsignedMin(a, b), rest...);
}

// `UnsignedMax()` returns the maximum of its arguments, which must be unsigned
// integers, as their widest type.

template <typename A, std::enable_if_t<std::is_unsigned<A>::value, int> = 0>
constexpr A UnsignedMax(A a) {
  return a;
}

template <
    typename A, typename B,
    std::enable_if_t<std::is_unsigned<A>::value && std::is_unsigned<B>::value,
                     int> = 0>
constexpr std::common_type_t<A, B> UnsignedMax(A a, B b) {
  return a >= b ? a : b;
}

template <typename A, typename B, typename... Rest,
          std::enable_if_t<
              (sizeof...(Rest) > 0 &&
               absl::conjunction<std::is_unsigned<A>, std::is_unsigned<B>,
                                 std::is_unsigned<Rest>...>::value),
              int> = 0>
constexpr std::common_type_t<A, B, Rest...> UnsignedMax(A a, B b,
                                                        Rest... rest) {
  return UnsignedMax(UnsignedMax(a, b), rest...);
}

// `UnsignedClamp(value, min_value, max_value)` is at least `min_value`,
// at most `max(max_value, min_value)`, preferably `value`.
template <typename Value, typename Min, typename Max,
          std::enable_if_t<std::is_unsigned<Value>::value &&
                               std::is_unsigned<Min>::value &&
                               std::is_unsigned<Max>::value,
                           int> = 0>
inline std::common_type_t<IntersectionTypeT<Value, Max>, Min> UnsignedClamp(
    Value value, Min min, Max max) {
  return UnsignedMax(UnsignedMin(value, max), min);
}

// `SaturatingIntCast()` converts an integer value to another integer type, or
// returns the appropriate bound of the type if conversion would overflow.

template <
    typename A, typename B,
    std::enable_if_t<std::is_unsigned<A>::value && std::is_unsigned<B>::value,
                     int> = 0>
inline A SaturatingIntCast(B value) {
  if (ABSL_PREDICT_FALSE(value > std::numeric_limits<A>::max())) {
    return std::numeric_limits<A>::max();
  }
  return static_cast<A>(value);
}

template <typename A, typename B,
          std::enable_if_t<
              std::is_unsigned<A>::value && std::is_signed<B>::value, int> = 0>
inline A SaturatingIntCast(B value) {
  if (ABSL_PREDICT_FALSE(value < 0)) return 0;
  if (ABSL_PREDICT_FALSE(static_cast<std::make_unsigned_t<B>>(value) >
                         std::numeric_limits<A>::max())) {
    return std::numeric_limits<A>::max();
  }
  return static_cast<A>(value);
}

template <typename A, typename B,
          std::enable_if_t<
              std::is_signed<A>::value && std::is_unsigned<B>::value, int> = 0>
inline A SaturatingIntCast(B value) {
  if (ABSL_PREDICT_FALSE(
          value > std::make_unsigned_t<A>{std::numeric_limits<A>::max()})) {
    return std::numeric_limits<A>::max();
  }
  return static_cast<A>(value);
}

template <typename A, typename B,
          std::enable_if_t<std::is_signed<A>::value && std::is_signed<B>::value,
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

template <typename T, std::enable_if_t<std::is_unsigned<T>::value, int> = 0>
constexpr T SaturatingAdd(T a) {
  return a;
}

template <typename T, std::enable_if_t<std::is_unsigned<T>::value, int> = 0>
constexpr T SaturatingAdd(T a, T b) {
  return a + UnsignedMin(b, std::numeric_limits<T>::max() - a);
}

template <
    typename T, typename... Rest,
    std::enable_if_t<(sizeof...(Rest) > 0 &&
                      absl::conjunction<std::is_unsigned<T>,
                                        std::is_unsigned<Rest>...>::value),
                     int> = 0>
constexpr T SaturatingAdd(T a, T b, Rest... rest) {
  return SaturatingAdd(SaturatingAdd(a, b), rest...);
}

// `SaturatingSub()` subtracts unsigned values, or returns 0 if subtraction
// would underflow.
template <
    typename T, typename U,
    std::enable_if_t<std::is_unsigned<T>::value && std::is_unsigned<U>::value,
                     int> = 0>
constexpr T SaturatingSub(T a, U b) {
  return a - UnsignedMin(b, a);
}

// `RoundDown()` rounds an unsigned value downwards to the nearest multiple of
// the given power of 2.
template <
    size_t alignment, typename T,
    std::enable_if_t<
        std::is_unsigned<T>::value && absl::has_single_bit(alignment), int> = 0>
constexpr T RoundDown(T value) {
  return value & ~T{alignment - 1};
}

// `RoundUp()` rounds an unsigned value upwards to the nearest multiple of the
// given power of 2.
template <
    size_t alignment, typename T,
    std::enable_if_t<
        std::is_unsigned<T>::value && absl::has_single_bit(alignment), int> = 0>
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
