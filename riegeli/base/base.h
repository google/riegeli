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
#include "absl/numeric/bits.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"

namespace riegeli {

// Entities defined in namespace `riegeli` and macros beginning with `RIEGELI_`
// are a part of the public API, except for entities defined in namespace
// `riegeli::internal` and macros beginning with `RIEGELI_INTERNAL_`.

// Clang has `__has_builtin()`. Other compilers need other means to detect
// availability of builtins.
#ifdef __has_builtin
#define RIEGELI_INTERNAL_HAS_BUILTIN(x) __has_builtin(x)
#else
#define RIEGELI_INTERNAL_HAS_BUILTIN(x) 0
#endif

#define RIEGELI_INTERNAL_IS_GCC_VERSION(major, minor) \
  (__GNUC__ > (major) || (__GNUC__ == (major) && __GNUC_MINOR__ >= (minor)))

// `RIEGELI_DEBUG` determines whether assertions are verified or just assumed.
// By default it follows `NDEBUG`.

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

// Prints a check failure message and terminates the program.
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
  std::ostringstream stream_;
};

// Stores an optional pointer to a message of a check failure.
class CheckResult {
 public:
  // Stores no message pointer.
  CheckResult() noexcept {}

  // Stores a message pointer.
  explicit CheckResult(const char* message)
      : failed_(true), message_(message) {}

  // Returns `true` if a message pointer is stored.
  explicit operator bool() const { return failed_; }

  // Returns the stored message pointer.
  //
  // Precondition: `*this` is `true`.
  const char* message() { return message_; }

 private:
  bool failed_ = false;
  const char* message_ = nullptr;
};

template <typename A, typename B>
ABSL_ATTRIBUTE_COLD const char* FormatCheckOpMessage(const char* message,
                                                     const A& a, const B& b) {
  std::ostringstream stream;
  stream << message << " (" << a << " vs. " << b << ")";
  // Do not bother with freeing this string: the program will soon terminate.
  return (new std::string(stream.str()))->c_str();
}

// These functions allow using `a` and `b` multiple times without reevaluation.
// They are small enough to be inlined, with the slow path delegated to
// `FormatCheckOpMessage()`.
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

// `RIEGELI_CHECK(expr)` checks that `expr` is `true`, terminating the program
// if not.
//
// `RIEGELI_CHECK_{EQ,NE,LT,GT,LE,GE}(a, b)` check the relationship between `a`
// and `b`, and include values of `a` and `b` in the failure message.
//
// `RIEGELI_CHECK_NOTNULL(expr)` checks that `expr` is not `nullptr`.
//
// `RIEGELI_CHECK_UNREACHABLE()` checks  that this expression is not reached.
//
// `RIEGELI_CHECK_NOTNULL(expr)` is an expression which evaluates to `expr`.
// The remaining `RIEGELI_CHECK*` macros can be followed by streaming `<<`
// operators in order to append more details to the failure message
// (streamed expressions are evaluated only on assertion failure).
//
// If `RIEGELI_DEBUG` is true, `RIEGELI_ASSERT*` macros are equivalent to the
// corresponding `RIEGELI_CHECK*` macros; if `RIEGELI_DEBUG` is false, they do
// nothing, but the behavior is undefined if `RIEGELI_ASSERT_UNREACHABLE()` is
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

// Returns `true` if the value of the expression is known at compile time.
#if RIEGELI_INTERNAL_HAS_BUILTIN(__builtin_constant_p) || \
    RIEGELI_INTERNAL_IS_GCC_VERSION(3, 1)
#define RIEGELI_IS_CONSTANT(expr) __builtin_constant_p(expr)
#else
#define RIEGELI_IS_CONSTANT(expr) false
#endif

namespace internal {

// `type_identity_t<T>` is `T`, but does not deduce the `T` in templates.

template <typename T>
struct type_identity {
  using type = T;
};

template <typename T>
using type_identity_t = typename type_identity<T>::type;

}  // namespace internal

// `RIEGELI_INTERNAL_INLINE_CONSTEXPR(type, name, init)` emulates
// namespace-scope `inline constexpr type name = init;` from C++17,
// but is available since C++11.

#if __cpp_inline_variables

#define RIEGELI_INTERNAL_INLINE_CONSTEXPR(type, name, init) \
  inline constexpr ::riegeli::internal::type_identity_t<type> name = init

#else

#define RIEGELI_INTERNAL_INLINE_CONSTEXPR(type, name, init)                   \
  template <typename RiegeliInternalDummy>                                    \
  struct RiegeliInternalInlineConstexpr_##name {                              \
    static constexpr ::riegeli::internal::type_identity_t<type> kInstance =   \
        init;                                                                 \
  };                                                                          \
  template <typename RiegeliInternalDummy>                                    \
  constexpr ::riegeli::internal::type_identity_t<type>                        \
      RiegeliInternalInlineConstexpr_##name<RiegeliInternalDummy>::kInstance; \
  static constexpr const ::riegeli::internal::type_identity_t<type>& name =   \
      RiegeliInternalInlineConstexpr_##name<void>::kInstance;                 \
  static_assert(sizeof(name) != 0, "Silence unused variable warnings.")

#endif

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

// `PtrDistance(first, last)` returns `last - first` as `size_t`, asserting that
// `first <= last`.
template <typename A>
inline size_t PtrDistance(const A* first, const A* last) {
  RIEGELI_ASSERT(first <= last)
      << "Failed invariant of PtrDistance(): pointers in the wrong order";
  return static_cast<size_t>(last - first);
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
    : internal::IntersectionTypeImpl<A, B, std::common_type_t<A, B>> {};

template <typename A, typename B, typename... Rest>
struct IntersectionType<A, B, Rest...>
    : IntersectionType<IntersectionTypeT<A, B>, Rest...> {};

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

// Position in a stream of bytes, used also for stream sizes.
//
// This is an unsigned integer type at least as wide as `size_t`,
// `std::streamoff`, and `uint64_t`.
using Position =
    std::common_type_t<size_t, std::make_unsigned_t<std::streamoff>, uint64_t>;

// Specifies the scope of objects to flush and the intended data durability
// (without a guarantee).
enum class FlushType {
  // Makes data written so far visible in other objects, propagating flushing
  // through owned dependencies of the given writer.
  kFromObject = 0,
  // Makes data written so far visible outside the process, propagating flushing
  // through dependencies of the given writer. This is generally the default.
  kFromProcess = 1,
  // Makes data written so far visible outside the process and durable in case
  // of operating system crash, propagating flushing through dependencies of the
  // given writer.
  kFromMachine = 2,
};

// Specifies the scope of objects to synchronize.
enum class SyncType {
  // Propagates synchronization through owned dependencies of the given reader.
  kFromObject = 0,
  // Propagates synchronization through all dependencies of the given reader.
  // This is generally the default.
  kFromProcess = 1,
};

ABSL_DEPRECATED("This is no longer used in Riegeli")
RIEGELI_INTERNAL_INLINE_CONSTEXPR(size_t, kDefaultBufferSize, size_t{64} << 10);

// Typical bounds of sizes of memory blocks holding pieces of data in objects.
RIEGELI_INTERNAL_INLINE_CONSTEXPR(size_t, kDefaultMinBlockSize, 256);
RIEGELI_INTERNAL_INLINE_CONSTEXPR(size_t, kDefaultMaxBlockSize,
                                  size_t{64} << 10);

// When deciding whether to copy an array of bytes or share memory, prefer
// copying up to this length.
//
// Copying can often be done in an inlined fast path. Sharing has more overhead,
// especially in a virtual slow path, so copying sufficiently short lengths
// performs better.
RIEGELI_INTERNAL_INLINE_CONSTEXPR(size_t, kMaxBytesToCopy, 255);

// When deciding whether to copy an array of bytes or share memory to an
// `absl::Cord`, prefer copying up to this length.
//
// `absl::Cord::Append(absl::Cord)` chooses to copy bytes from a source up to
// this length, so it is better to avoid constructing the source as `absl::Cord`
// if it will not be shared anyway.
inline size_t MaxBytesToCopyToCord(absl::Cord& dest) {
  // `absl::cord_internal::kMaxInline`.
  static constexpr size_t kMaxInline = 15;
  // `absl::cord_internal::kMaxBytesToCopy`.
  static constexpr size_t kCordMaxBytesToCopy = 511;
  return dest.empty() ? kMaxInline : kCordMaxBytesToCopy;
}

// Recommends the length of a read buffer by modifying the base recommendation
// if a size hint is known.
//
// The base recommendation is `length`. If `pos` did not pass `size_hint` yet,
// returns the remaining length instead, increased by 1 which can be helpful to
// verify that there are indeed no more data.
//
// If `multiple_runs` are predicted, it is assumed that reading might not reach
// the size hint, and thus a size hint may decrease but not increase the
// returned length.
inline Position ApplyReadSizeHint(Position length,
                                  absl::optional<Position> size_hint,
                                  Position pos, bool multiple_runs = false) {
  if (size_hint != absl::nullopt && pos <= *size_hint) {
    const Position remaining_plus_1 =
        SaturatingAdd(*size_hint - pos, Position{1});
    if (multiple_runs) {
      return UnsignedMin(length, remaining_plus_1);
    } else {
      return remaining_plus_1;
    }
  } else {
    return length;
  }
}

// Recommends the length of a write buffer by modifying the base recommendation
// if a size hint is known.
//
// The base recommendation is `length`. If `pos` did not reach `size_hint` yet,
// returns the remaining length instead.
//
// If `multiple_runs` are predicted, it is assumed that writing might not reach
// the size hint, and thus a size hint may decrease but not increase the
// returned length.
inline Position ApplyWriteSizeHint(Position length,
                                   absl::optional<Position> size_hint,
                                   Position pos, bool multiple_runs = false) {
  if (size_hint != absl::nullopt && pos < *size_hint) {
    const Position remaining = *size_hint - pos;
    if (multiple_runs) {
      return UnsignedMin(length, remaining);
    } else {
      return remaining;
    }
  } else {
    return length;
  }
}

// Heuristics for whether a partially filled buffer is wasteful.
inline bool Wasteful(size_t total, size_t used) {
  return total - used > UnsignedMax(used, kDefaultMinBlockSize);
}

// Resize `dest` to `size`, ensuring that repeated growth has the cost
// proportional to the final size. New contents are unspecified.
void ResizeStringAmortized(std::string& dest, size_t new_size);

// Variants of `absl::Cord` operations with different block sizing tradeoffs:
//  * `MakeBlockyCord(src)` is like `absl::Cord(src)`.
//  * `AppendToBlockyCord(src, dest)` is like `dest.Append(src)`.
//  * `PrependToBlockyCord(src, dest)` is like `dest.Prepend(src)`.
//
// They assume that the `absl::Cord` is constructed from fragments of reasonable
// sizes, with adjacent sizes being not too small.
//
// They avoid splitting `src` into 4083-byte fragments and avoid overallocation.
absl::Cord MakeBlockyCord(absl::string_view src);
void AppendToBlockyCord(absl::string_view src, absl::Cord& dest);
void PrependToBlockyCord(absl::string_view src, absl::Cord& dest);

// Assert that a region of memory is initialized, which is checked when running
// under memory sanitizer.
inline void AssertInitialized(const char* data, size_t size) {
#ifdef MEMORY_SANITIZER
  __msan_check_mem_is_initialized(data, size);
#endif
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_BASE_H_
