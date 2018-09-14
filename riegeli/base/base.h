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

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/meta/type_traits.h"
#include "riegeli/base/port.h"

namespace riegeli {

// Entities defined in namespace riegeli and macros beginning with RIEGELI_ are
// a part of the public API, except for entities defined in namespace
// riegeli::internal and macros beginning with RIEGELI_INTERNAL_.

// RIEGELI_DEBUG determines whether assertions are verified or just assumed.
// By default it follows NDEBUG.

#ifndef RIEGELI_DEBUG
#ifdef NDEBUG
#define RIEGELI_DEBUG 0
#else
#define RIEGELI_DEBUG 1
#endif
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
  ABSL_ATTRIBUTE_COLD explicit CheckFailed(const char* file, int line,
                                           const char* function,
                                           const char* message);

  // Allows to add details to the message by writing to the stream.
  std::ostream& stream() { return stream_; }

  // Prints the formatted message and terminates the program.
  ABSL_ATTRIBUTE_NORETURN ~CheckFailed();

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
ABSL_ATTRIBUTE_COLD const char* FormatCheckOpMessage(const char* message,
                                                     const A& a, const B& b) {
  std::stringstream stream;
  stream << message << " (" << a << " vs. " << b << ")";
  // Do not bother with freeing this string: the program will soon terminate.
  return (new std::string(stream.str()))->c_str();
}

// These functions allow to use a and b multiple times without reevaluation.
// They are small enough to be inlined, with the slow path delegated to
// FormatCheckOpMessage().
#define RIEGELI_INTERNAL_DEFINE_CHECK_OP(name, op)                \
  template <typename A, typename B>                               \
  inline CheckResult Check##name(const char* message, const A& a, \
                                 const B& b) {                    \
    if (ABSL_PREDICT_TRUE(a op b)) {                              \
      return CheckResult();                                       \
    } else {                                                      \
      return CheckResult(FormatCheckOpMessage(message, a, b));    \
    }                                                             \
  }                                                               \
  static_assert(true, "")  // Eat a semicolon.

RIEGELI_INTERNAL_DEFINE_CHECK_OP(Eq, ==);
RIEGELI_INTERNAL_DEFINE_CHECK_OP(Ne, !=);
RIEGELI_INTERNAL_DEFINE_CHECK_OP(Lt, <);
RIEGELI_INTERNAL_DEFINE_CHECK_OP(Gt, >);
RIEGELI_INTERNAL_DEFINE_CHECK_OP(Le, <=);
RIEGELI_INTERNAL_DEFINE_CHECK_OP(Ge, >=);

#undef RIEGELI_INTERNAL_DEFINE_CHECK_OP

template <typename T>
inline T CheckNotNull(const char* file, int line, const char* function,
                      const char* message, T&& value) {
  if (ABSL_PREDICT_FALSE(value == nullptr)) {
    CheckFailed(file, line, function, message);
  }
  return std::forward<T>(value);
}

#if !RIEGELI_DEBUG

class UnreachableStream {
 public:
  UnreachableStream() { RIEGELI_INTERNAL_UNREACHABLE(); }

  ABSL_ATTRIBUTE_NORETURN ~UnreachableStream() {
    RIEGELI_INTERNAL_UNREACHABLE();
  }

  template <typename T>
  UnreachableStream& operator<<(T&& value) {
    return *this;
  }
};

template <typename T>
inline T AssertNotNull(T&& value) {
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
  while (ABSL_PREDICT_FALSE(!(expr)))                                \
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

// riegeli::type_identity_t<T> is T, but does not deduce the T in templates.

template <typename T>
struct type_identity {
  using type = T;
};

template <typename T>
using type_identity_t = typename type_identity<T>::type;

// riegeli::exchange() is the same as std::exchange() from C++14, but is
// available since C++11.

#if __cpp_lib_exchange_function

using std::exchange;

#else  // !__cpp_lib_exchange_function

template <typename T, typename U = T>
inline T exchange(T& obj, U&& new_value) {
  T old_value = std::move(obj);
  obj = std::forward<U>(new_value);
  return old_value;
}

#endif  // !__cpp_lib_exchange_function

// IntCast<A>(value) converts between integral types, asserting that the value
// fits in the target type.

namespace internal {

template <typename A, typename B>
inline absl::enable_if_t<
    std::is_unsigned<A>::value && std::is_unsigned<B>::value, A>
IntCastImpl(B value) {
  RIEGELI_ASSERT_LE(value, std::numeric_limits<A>::max())
      << "Value out of range";
  return static_cast<A>(value);
}

template <typename A, typename B>
inline absl::enable_if_t<std::is_unsigned<A>::value && std::is_signed<B>::value,
                         A>
IntCastImpl(B value) {
  RIEGELI_ASSERT_GE(value, 0) << "Value out of range";
  RIEGELI_ASSERT_LE(static_cast<absl::make_unsigned_t<B>>(value),
                    std::numeric_limits<A>::max())
      << "Value out of range";
  return static_cast<A>(value);
}

template <typename A, typename B>
inline absl::enable_if_t<std::is_signed<A>::value && std::is_unsigned<B>::value,
                         A>
IntCastImpl(B value) {
  RIEGELI_ASSERT_LE(value,
                    absl::make_unsigned_t<A>{std::numeric_limits<A>::max()})
      << "Value out of range";
  return static_cast<A>(value);
}

template <typename A, typename B>
inline absl::enable_if_t<std::is_signed<A>::value && std::is_signed<B>::value,
                         A>
IntCastImpl(B value) {
  RIEGELI_ASSERT_GE(value, std::numeric_limits<A>::min())
      << "Value out of range";
  RIEGELI_ASSERT_LE(value, std::numeric_limits<A>::max())
      << "Value out of range";
  return static_cast<A>(value);
}

}  // namespace internal

template <typename A, typename B>
inline A IntCast(B value) {
  static_assert(std::is_integral<A>::value,
                "IntCast() requires integral types");
  static_assert(std::is_integral<B>::value,
                "IntCast() requires integral types");
  return internal::IntCastImpl<A>(value);
}

// PtrDistance(first, last) returns last - first as size_t, asserting that
// first <= last.
template <typename A>
inline size_t PtrDistance(const A* first, const A* last) {
  RIEGELI_ASSERT(first <= last)
      << "Failed invariant of PtrDistance(): pointers in the wrong order";
  return static_cast<size_t>(last - first);
}

// SignedMin() returns the minimum of its arguments, which must be signed
// integers, as their widest type.

template <typename A, typename B>
constexpr absl::common_type_t<A, B> SignedMin(A a, B b) {
  static_assert(std::is_signed<A>::value, "SignedMin() requires signed types");
  static_assert(std::is_signed<B>::value, "SignedMin() requires signed types");
  return a < b ? b : a;
}

template <typename A, typename B, typename... Rest>
constexpr absl::common_type_t<A, B, Rest...> SignedMin(A a, B b, Rest... rest) {
  return SignedMin(SignedMin(a, b), rest...);
}

// SignedMax() returns the maximum of its arguments, which must be signed
// integers, as their widest type.

template <typename A, typename B>
constexpr absl::common_type_t<A, B> SignedMax(A a, B b) {
  static_assert(std::is_signed<A>::value, "SignedMax() requires signed types");
  static_assert(std::is_signed<B>::value, "SignedMax() requires signed types");
  return a < b ? b : a;
}

template <typename A, typename B, typename... Rest>
constexpr absl::common_type_t<A, B, Rest...> SignedMax(A a, B b, Rest... rest) {
  return SignedMax(SignedMax(a, b), rest...);
}

// UnsignedMin() returns the minimum of its arguments, which must be unsigned
// integers, as their narrowest type.

namespace internal {

template <typename A, typename B, typename Common>
struct IntersectionTypeImpl;

template <typename A, typename B>
struct IntersectionTypeImpl<A, B, A> {
  using type = B;
};

template <typename A, typename B>
struct IntersectionTypeImpl<A, B, B> {
  using type = A;
};

template <typename A>
struct IntersectionTypeImpl<A, A, A> {
  using type = A;
};

}  // namespace internal

template <typename... T>
struct IntersectionType;

template <typename... T>
using IntersectionTypeT = typename IntersectionType<T...>::type;

template <typename A>
struct IntersectionType<A> {
  using type = A;
};

template <typename A, typename B>
struct IntersectionType<A, B>
    : internal::IntersectionTypeImpl<A, B, absl::common_type_t<A, B>> {};

template <typename A, typename B, typename... Rest>
struct IntersectionType<A, B, Rest...>
    : IntersectionType<IntersectionTypeT<A, B>, Rest...> {};

template <typename A, typename B>
constexpr IntersectionTypeT<A, B> UnsignedMin(A a, B b) {
  static_assert(std::is_unsigned<A>::value,
                "UnsignedMin() requires unsigned types");
  static_assert(std::is_unsigned<B>::value,
                "UnsignedMin() requires unsigned types");
  return IntCast<IntersectionTypeT<A, B>>(a < b ? a : b);
}

template <typename A, typename B, typename... Rest>
constexpr IntersectionTypeT<A, B, Rest...> UnsignedMin(A a, B b, Rest... rest) {
  return UnsignedMin(UnsignedMin(a, b), rest...);
}

// UnsignedMax() returns the maximum of its arguments, which must be unsigned
// integers, as their widest type.

template <typename A, typename B>
constexpr absl::common_type_t<A, B> UnsignedMax(A a, B b) {
  static_assert(std::is_unsigned<A>::value,
                "UnsignedMax() requires unsigned types");
  static_assert(std::is_unsigned<B>::value,
                "UnsignedMax() requires unsigned types");
  return a < b ? b : a;
}

template <typename A, typename B, typename... Rest>
constexpr absl::common_type_t<A, B, Rest...> UnsignedMax(A a, B b,
                                                         Rest... rest) {
  return UnsignedMax(UnsignedMax(a, b), rest...);
}

// SaturatingAdd() adds unsigned values, or returns max possible value of the
// type if addition would overflow.
template <typename T>
constexpr T SaturatingAdd(T a, T b) {
  static_assert(std::is_unsigned<T>::value,
                "SaturatingAdd() requires an unsigned type");
  return a + UnsignedMin(b, std::numeric_limits<T>::max() - a);
}

// SaturatingSub() subtracts unsigned values, or returns 0 if subtraction would
// underflow.
template <typename T>
constexpr T SaturatingSub(T a, T b) {
  static_assert(std::is_unsigned<T>::value,
                "SaturatingSub() requires an unsigned type");
  return a - UnsignedMin(b, a);
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
    absl::common_type_t<size_t, absl::make_unsigned_t<std::streamoff>,
                        uint64_t>;

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
