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
#include <limits>
#include <ostream>
#include <sstream>
#include <string>
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

// RIEGELI_DEBUG determines whether assertions are verified or just assumed.
// By default it follows NDEBUG.

#ifndef RIEGELI_DEBUG
#define RIEGELI_DEBUG !defined(NDEBUG)
#endif

namespace internal {

#if RIEGELI_INTERNAL_HAS_BUILTIN(__builtin_unreachable) || \
    RIEGELI_INTERNAL_IS_GCC_VERSION(4, 5)
#define RIEGELI_INTERNAL_UNREACHABLE() __builtin_unreachable()
#elif defined(_MSC_VER)
#define RIEGELI_INTERNAL_UNREACHABLE() __assume(false)
#else
#define RIEGELI_INTERNAL_UNREACHABLE() \
  do {                                 \
  } while (false)
#endif

// prints a check failure message and terminates the program.
class CheckFailed {
 public:
  // Begins formatting the message as:
  // "Check failed at file:line in function: message ".
  RIEGELI_ATTRIBUTE_COLD CheckFailed(const char* file, int line,
                                     const char* function, const char* message);

  // Allows to add details to the message by writing to the stream.
  std::ostream& stream() { return stream_; }

  // Prints the formatted message and terminates the program.
  RIEGELI_ATTRIBUTE_NORETURN ~CheckFailed();

 private:
  std::stringstream stream_;
};

// Stores an optional pointer to a message of a check failure.
class CheckResult {
 public:
  // Stores no message pointer.
  CheckResult() noexcept {}

  // Stores a message pointer.
  explicit CheckResult(const char* message)
      : failed_(true), message_(message) {}

  // Returns true if a message pointer is stored.
  operator bool() const { return failed_; }

  // Returns the stored message pointer.
  //
  // Precondition: *this is true.
  const char* message() { return message_; }

 private:
  bool failed_ = false;
  const char* message_ = nullptr;
};

template <typename A, typename B>
RIEGELI_ATTRIBUTE_COLD const char* FormatCheckOpMessage(const char* message,
                                                        const A& a,
                                                        const B& b) {
  std::stringstream stream;
  stream << message << " (" << a << " vs. " << b << ")";
  // Do not bother with freeing this string: the program will soon terminate.
  return (new std::string(stream.str()))->c_str();
}

// These functions allow to use a and b multiple times without reevaluation.
// They are small enough to be inlined, with the slow path delegated to
// FormatCheckOpMessage().
#define RIEGELI_INTERNAL_DEFINE_CHECK_OP(name, op)                       \
  template <typename A, typename B>                                      \
  CheckResult Check##name(const char* message, const A& a, const B& b) { \
    if (RIEGELI_LIKELY(a op b)) {                                        \
      return CheckResult();                                              \
    } else {                                                             \
      return CheckResult(FormatCheckOpMessage(message, a, b));           \
    }                                                                    \
  }                                                                      \
  static_assert(true, "")  // Eat a semicolon.

RIEGELI_INTERNAL_DEFINE_CHECK_OP(Eq, ==);
RIEGELI_INTERNAL_DEFINE_CHECK_OP(Ne, !=);
RIEGELI_INTERNAL_DEFINE_CHECK_OP(Lt, <);
RIEGELI_INTERNAL_DEFINE_CHECK_OP(Gt, >);
RIEGELI_INTERNAL_DEFINE_CHECK_OP(Le, <=);
RIEGELI_INTERNAL_DEFINE_CHECK_OP(Ge, >=);

#undef RIEGELI_INTERNAL_DEFINE_CHECK_OP

template <typename T>
T CheckNotNull(const char* file, int line, const char* function,
               const char* message, T&& value) {
  if (RIEGELI_UNLIKELY(value == nullptr)) {
    CheckFailed(file, line, function, message);
  }
  return std::forward<T>(value);
}

#if !RIEGELI_DEBUG

class UnreachableStream {
 public:
  UnreachableStream() { RIEGELI_INTERNAL_UNREACHABLE(); }

  RIEGELI_ATTRIBUTE_NORETURN ~UnreachableStream() {
    RIEGELI_INTERNAL_UNREACHABLE();
  }

  template <typename T>
  UnreachableStream& operator<<(T&& value) {
    return *this;
  }
};

template <typename T>
T AssertNotNull(T&& value) {
  if (value == nullptr) RIEGELI_INTERNAL_UNREACHABLE();
  return std::forward<T>(value);
}

#endif  // !RIEGELI_DEBUG

}  // namespace internal

// RIEGELI_CHECK(expr) checks that expr is true, terminating the program if not.
//
// RIEGELI_CHECK_{EQ,NE,LT,GT,LE,GE}(a, b) check the relationship between a and
// b, and include values of a and b in the failure message.
//
// RIEGELI_CHECK_NOTNULL(expr) checks that expr is not nullptr.
//
// RIEGELI_CHECK_UNREACHABLE() checks  that this expression is not reached.
//
// RIEGELI_CHECK_NOTNULL(expr) is an expression which evaluates to expr.
// The remaining RIEGELI_CHECK* macros can be followed by streaming <<
// operators in order to append more details to the failure message
// (streamed expressions are evaluated only on assertion failure).
//
// If RIEGELI_DEBUG is true, RIEGELI_ASSERT* macros are equivalent to the
// corresponding RIEGELI_CHECK* macros; if RIEGELI_DEBUG is false, they do
// nothing, but the behavior is undefined if RIEGELI_ASSERT_UNREACHABLE() is
// reached.

#if defined(__clang__) || RIEGELI_INTERNAL_IS_GCC_VERSION(2, 6)
#define RIEGELI_INTERNAL_FUNCTION __PRETTY_FUNCTION__
#elif defined(_MSC_VER)
#define RIEGELI_INTERNAL_FUNCTION __FUNCSIG__
#else
#define RIEGELI_INTERNAL_FUNCTION __func__
#endif

#define RIEGELI_INTERNAL_CHECK_OP(name, op, a, b)                       \
  while (::riegeli::internal::CheckResult riegeli_internal_check =      \
             ::riegeli::internal::Check##name(#a " " #op " " #b, a, b)) \
  ::riegeli::internal::CheckFailed(__FILE__, __LINE__,                  \
                                   RIEGELI_INTERNAL_FUNCTION,           \
                                   riegeli_internal_check.message())    \
      .stream()

#define RIEGELI_CHECK(expr)                                          \
  while (RIEGELI_UNLIKELY(!(expr)))                                  \
  ::riegeli::internal::CheckFailed(__FILE__, __LINE__,               \
                                   RIEGELI_INTERNAL_FUNCTION, #expr) \
      .stream()
#define RIEGELI_CHECK_EQ(a, b) RIEGELI_INTERNAL_CHECK_OP(Eq, ==, a, b)
#define RIEGELI_CHECK_NE(a, b) RIEGELI_INTERNAL_CHECK_OP(Ne, !=, a, b)
#define RIEGELI_CHECK_LT(a, b) RIEGELI_INTERNAL_CHECK_OP(Lt, <, a, b)
#define RIEGELI_CHECK_GT(a, b) RIEGELI_INTERNAL_CHECK_OP(Gt, >, a, b)
#define RIEGELI_CHECK_LE(a, b) RIEGELI_INTERNAL_CHECK_OP(Le, <=, a, b)
#define RIEGELI_CHECK_GE(a, b) RIEGELI_INTERNAL_CHECK_OP(Ge, >=, a, b)
#define RIEGELI_CHECK_NOTNULL(expr)                            \
  ::riegeli::internal::CheckNotNull(__FILE__, __LINE__,        \
                                    RIEGELI_INTERNAL_FUNCTION, \
                                    #expr " != nullptr", expr)
#define RIEGELI_CHECK_UNREACHABLE()                                         \
  ::riegeli::internal::CheckFailed(__FILE__, __LINE__,                      \
                                   RIEGELI_INTERNAL_FUNCTION, "Impossible") \
      .stream()

#if RIEGELI_DEBUG

#define RIEGELI_ASSERT RIEGELI_CHECK
#define RIEGELI_ASSERT_EQ RIEGELI_CHECK_EQ
#define RIEGELI_ASSERT_NE RIEGELI_CHECK_NE
#define RIEGELI_ASSERT_LT RIEGELI_CHECK_LT
#define RIEGELI_ASSERT_GT RIEGELI_CHECK_GT
#define RIEGELI_ASSERT_LE RIEGELI_CHECK_LE
#define RIEGELI_ASSERT_GE RIEGELI_CHECK_GE
#define RIEGELI_ASSERT_NOTNULL RIEGELI_CHECK_NOTNULL
#define RIEGELI_ASSERT_UNREACHABLE RIEGELI_CHECK_UNREACHABLE

#else  // !RIEGELI_DEBUG

#define RIEGELI_ASSERT(expr) \
  while (false && !(expr)) ::riegeli::internal::UnreachableStream()

#define RIEGELI_ASSERT_EQ(a, b) RIEGELI_ASSERT((a) == (b))
#define RIEGELI_ASSERT_NE(a, b) RIEGELI_ASSERT((a) != (b))
#define RIEGELI_ASSERT_LT(a, b) RIEGELI_ASSERT((a) < (b))
#define RIEGELI_ASSERT_GT(a, b) RIEGELI_ASSERT((a) > (b))
#define RIEGELI_ASSERT_LE(a, b) RIEGELI_ASSERT((a) <= (b))
#define RIEGELI_ASSERT_GE(a, b) RIEGELI_ASSERT((a) >= (b))
#define RIEGELI_ASSERT_NOTNULL(expr) ::riegeli::internal::AssertNotNull(expr)
#define RIEGELI_ASSERT_UNREACHABLE() ::riegeli::internal::UnreachableStream()

#endif  // !RIEGELI_DEBUG

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

// IntCast<A>(value) converts between integral types, asserting that the value
// fits in the target type.

namespace internal {

template <typename A, typename B>
typename std::enable_if<
    std::is_unsigned<A>::value && std::is_unsigned<B>::value, A>::type
IntCastImpl(B value) {
  RIEGELI_ASSERT_LE(value, std::numeric_limits<A>::max())
      << "Value out of range";
  return static_cast<A>(value);
}

template <typename A, typename B>
typename std::enable_if<std::is_unsigned<A>::value && std::is_signed<B>::value,
                        A>::type
IntCastImpl(B value) {
  RIEGELI_ASSERT_GE(value, 0) << "Value out of range";
  RIEGELI_ASSERT_LE(static_cast<typename std::make_unsigned<B>::type>(value),
                    std::numeric_limits<A>::max())
      << "Value out of range";
  return static_cast<A>(value);
}

template <typename A, typename B>
typename std::enable_if<std::is_signed<A>::value && std::is_unsigned<B>::value,
                        A>::type
IntCastImpl(B value) {
  RIEGELI_ASSERT_LE(
      value,
      typename std::make_unsigned<A>::type{std::numeric_limits<A>::max()})
      << "Value out of range";
  return static_cast<A>(value);
}

template <typename A, typename B>
typename std::enable_if<std::is_signed<A>::value && std::is_signed<B>::value,
                        A>::type
IntCastImpl(B value) {
  RIEGELI_ASSERT_GE(value, std::numeric_limits<A>::min())
      << "Value out of range";
  RIEGELI_ASSERT_LE(value, std::numeric_limits<A>::max())
      << "Value out of range";
  return static_cast<A>(value);
}

}  // namespace internal

template <typename A, typename B>
A IntCast(B value) {
  static_assert(std::is_integral<A>::value,
                "IntCast() requires integral types");
  static_assert(std::is_integral<B>::value,
                "IntCast() requires integral types");
  return internal::IntCastImpl<A>(value);
}

// PtrDistance(first, last) returns last - first as size_t, asserting that
// first <= last.
template <typename A>
size_t PtrDistance(const A* first, const A* last) {
  RIEGELI_ASSERT(first <= last) << "Pointers are in a wrong order";
  return static_cast<size_t>(last - first);
}

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
  return IntCast<typename IntersectionType<A, B>::type>(a < b ? a : b);
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

// RoundDown() rounds an unsigned value downwards to the nearest multiple of the
// given power of 2.
template <size_t alignment, typename T>
constexpr T RoundDown(T value) {
  static_assert(std::is_unsigned<T>::value,
                "RoundDown() requires an unsigned type");
  static_assert(alignment != 0 && (alignment & (alignment - 1)) == 0,
                "alignment must be a power of 2");
  return value & ~T{alignment - 1};
}

// RoundUp() rounds an unsigned value upwards to the nearest multiple of the
// given power of 2.
template <size_t alignment, typename T>
constexpr T RoundUp(T value) {
  static_assert(std::is_unsigned<T>::value,
                "RoundUp() requires an unsigned type");
  static_assert(alignment != 0 && (alignment & (alignment - 1)) == 0,
                "alignment must be a power of 2");
  return ((value - 1) | T{alignment - 1}) + 1;
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
