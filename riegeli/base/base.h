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

#ifndef RIEGELI_BASE_BASE_H_
#define RIEGELI_BASE_BASE_H_

#include <stddef.h>
#include <stdint.h>
#include <ios>
#include <type_traits>
#include <utility>

#include "riegeli/base/port.h"

namespace riegeli {

// Entities defined in namespace riegeli and macros beginning with RIEGELI_ are
// a part of the public API, except for entities defined in namespace
// riegeli::internal and macros beginning with RIEGELI_INTERNAL_.

// RIEGELI_LIKELY() hints the compiler that the condition is likely to be true.
// RIEGELI_UNLIKELY() hints the compiler that the condition is likely to be
// false. They may help the compiler generating better code.
//
// They are primarily used to distinguish normal paths from error paths, and
// fast paths of inline code where data is in the buffer from slow paths where
// the performance is less important.
#if RIEGELI_INTERNAL_HAS_BUILTIN(__builtin_expect) || \
    RIEGELI_INTERNAL_IS_GCC_VERSION(3, 0)
#define RIEGELI_LIKELY(x) __builtin_expect(bool(x), true)
#define RIEGELI_UNLIKELY(x) __builtin_expect(bool(x), false)
#else
#define RIEGELI_LIKELY(x) bool(x)
#define RIEGELI_UNLIKELY(x) bool(x)
#endif

// Marks a function as unlikely to be executed. This may help the compiler
// generating better code.
//
// This is primarily used in implementation of error paths.
#if RIEGELI_INTERNAL_HAS_ATTRIBUTE(cold) || \
    RIEGELI_INTERNAL_IS_GCC_VERSION(4, 3)
#define RIEGELI_ATTRIBUTE_COLD __attribute__((cold))
#else
#define RIEGELI_ATTRIBUTE_COLD
#endif

// Marks a function that never returns. This not only helps the compiler
// generating better code but also silences spurious warnings.
#if RIEGELI_INTERNAL_HAS_CPP_ATTRIBUTE(noreturn)
#define RIEGELI_ATTRIBUTE_NORETURN [[noreturn]]
#elif RIEGELI_INTERNAL_HAS_ATTRIBUTE(noreturn) || \
    RIEGELI_INTERNAL_IS_GCC_VERSION(2, 5)
#define RIEGELI_ATTRIBUTE_NORETURN __attribute__((noreturn))
#elif defined(_MSC_VER)
#define RIEGELI_ATTRIBUTE_NORETURN __declspec(noreturn)
#else
#define RIEGELI_ATTRIBUTE_NORETURN
#endif

// Asks the compiler to not inline a function.
#if RIEGELI_INTERNAL_HAS_ATTRIBUTE(noinline) || \
    RIEGELI_INTERNAL_IS_GCC_VERSION(3, 1)
#define RIEGELI_ATTRIBUTE_NOINLINE __attribute__((noinline))
#elif defined(_MSC_VER)
#define RIEGELI_ATTRIBUTE_NOINLINE __declspec(noinline)
#else
#define RIEGELI_ATTRIBUTE_NOINLINE
#endif

// Annotates implicit fallthrough between case labels which is intended.
#if RIEGELI_INTERNAL_HAS_CPP_ATTRIBUTE(fallthrough)
#define RIEGELI_FALLTHROUGH [[fallthrough]]
#elif RIEGELI_INTERNAL_HAS_CPP_ATTRIBUTE(clang::fallthrough)
#define RIEGELI_FALLTHROUGH [[clang::fallthrough]]
#else
#define RIEGELI_FALLTHROUGH
#endif

// riegeli::exchange() is the same as std::exchange() from C++14, but is
// available since C++11.

#if __cpp_lib_exchange_function

using std::exchange;

#else  // !__cpp_lib_exchange_function

template <typename T, typename U = T>
T exchange(T& obj, U&& new_value) {
  T old_value = std::move(obj);
  obj = std::forward<U>(new_value);
  return old_value;
}

#endif  // !__cpp_lib_exchange_function

// UnsignedMin() returns the minimum of its arguments, which must be unsigned
// integers, as their narrowest type.

namespace internal {

template <typename A, typename B, typename Common>
struct IntersectionType;

template <typename A, typename B>
struct IntersectionType<A, B, A> {
  using type = B;
};

template <typename A, typename B>
struct IntersectionType<A, B, B> {
  using type = A;
};

template <typename A>
struct IntersectionType<A, A, A> {
  using type = A;
};

}  // namespace internal

template <typename A, typename B, typename... Rest>
struct IntersectionType
    : IntersectionType<typename IntersectionType<A, B>::type, Rest...> {};

template <typename A, typename B>
struct IntersectionType<A, B>
    : internal::IntersectionType<A, B, typename std::common_type<A, B>::type> {
};

template <typename A, typename B>
constexpr typename IntersectionType<A, B>::type UnsignedMin(A a, B b) {
  static_assert(std::is_unsigned<A>::value,
                "UnsignedMin() requires unsigned types");
  static_assert(std::is_unsigned<B>::value,
                "UnsignedMin() requires unsigned types");
  return static_cast<typename IntersectionType<A, B>::type>(a < b ? a : b);
}

template <typename A, typename B, typename... Rest>
constexpr typename IntersectionType<A, B, Rest...>::type UnsignedMin(
    A a, B b, Rest... rest) {
  return UnsignedMin(UnsignedMin(a, b), rest...);
}

// UnsignedMax() returns the maximum of its arguments, which must be unsigned
// integers, as their widest type.

template <typename A, typename B>
constexpr typename std::common_type<A, B>::type UnsignedMax(A a, B b) {
  static_assert(std::is_unsigned<A>::value,
                "UnsignedMax() requires unsigned types");
  static_assert(std::is_unsigned<B>::value,
                "UnsignedMax() requires unsigned types");
  return a < b ? b : a;
}

template <typename A, typename B, typename... Rest>
constexpr typename std::common_type<A, B, Rest...>::type UnsignedMax(
    A a, B b, Rest... rest) {
  return UnsignedMax(UnsignedMax(a, b), rest...);
}

// RoundUp() rounds an unsigned value upwards to the nearest multiple of the
// given power of 2.
template <size_t alignment, typename T>
constexpr T RoundUp(T value) {
  static_assert(std::is_unsigned<T>::value,
                "RoundUp() requires an unsigned type");
  static_assert(alignment != 0 && (alignment & (alignment - 1)) == 0,
                "alignment must be a power of 2");
  return ((value - 1) | (alignment - 1)) + 1;
}

// Position in a stream of bytes, used also for stream sizes.
//
// This is an unsigned integer type at least as wide as size_t, streamoff, and
// uint64_t.
using Position =
    std::common_type<size_t, std::make_unsigned<std::streamoff>::type,
                     uint64_t>::type;

// The default size of buffers used to amortize copying data to/from a more
// expensive destination/source.
constexpr size_t kDefaultBufferSize() { return size_t{64} << 10; }

// In the fast path of certain functions, even if enough data is available in a
// buffer, the data is not copied if more than kMaxBytesToCopy() is requested,
// falling back to a virtual slow path instead. The virtual function might take
// advantage of sharing instead of copying.
constexpr size_t kMaxBytesToCopy() { return 511; }

}  // namespace riegeli

#endif  // RIEGELI_BASE_BASE_H_
