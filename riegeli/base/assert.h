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

#ifndef RIEGELI_BASE_ASSERT_H_
#define RIEGELI_BASE_ASSERT_H_

#include <stddef.h>
#include <stdint.h>

#include <ostream>  // IWYU pragma: export
#include <sstream>
#include <string>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "riegeli/base/port.h"

namespace riegeli {

// `RIEGELI_DEBUG` determines whether assertions are verified or just assumed.
// By default it follows `NDEBUG`.

#ifndef RIEGELI_DEBUG
#ifdef NDEBUG
#define RIEGELI_DEBUG 0
#else
#define RIEGELI_DEBUG 1
#endif
#endif

namespace assert_internal {

#if RIEGELI_INTERNAL_HAS_BUILTIN(__builtin_unreachable) || \
    RIEGELI_INTERNAL_IS_GCC_VERSION(4, 5)
#define RIEGELI_INTERNAL_UNREACHABLE() __builtin_unreachable()
#elif defined(_WIN32)
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
  CheckResult() = default;

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

// For showing a value in a failure message involving a comparison, if needed
// then converts the value to a different type with the appropriate behavior of
// `operator<<`: characters are shown as integers.

inline int GetStreamable(char value) { return value; }
inline int GetStreamable(signed char value) { return value; }
inline unsigned GetStreamable(unsigned char value) { return value; }
inline int GetStreamable(wchar_t value) { return value; }
#if __cpp_char8_t
inline unsigned GetStreamable(char8_t value) { return value; }
#endif
inline uint16_t GetStreamable(char16_t value) { return value; }
inline uint32_t GetStreamable(char32_t value) { return value; }

template <typename T>
inline const T& GetStreamable(const T& value) {
  return value;
}

template <typename A, typename B>
ABSL_ATTRIBUTE_COLD const char* FormatCheckOpMessage(const char* message,
                                                     const A& a, const B& b) {
  std::ostringstream stream;
  stream << message << " (" << GetStreamable(a) << " vs. " << GetStreamable(b)
         << ")";
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

#ifdef _MSC_VER
// Silence MSVC warning for destructor that does not return.
#pragma warning(push)
#pragma warning(disable : 4722)
#endif

class UnreachableStream {
 public:
  UnreachableStream() { RIEGELI_INTERNAL_UNREACHABLE(); }

  ABSL_ATTRIBUTE_NORETURN ~UnreachableStream() {
    RIEGELI_INTERNAL_UNREACHABLE();
  }

  template <typename T>
  UnreachableStream& operator<<(ABSL_ATTRIBUTE_UNUSED T&& value) {
    return *this;
  }
};

#ifdef _MSC_VER
#pragma warning(pop)
#endif

template <typename T>
inline T AssertNotNull(T&& value) {
  if (value == nullptr) RIEGELI_INTERNAL_UNREACHABLE();
  return std::forward<T>(value);
}

#endif  // !RIEGELI_DEBUG

}  // namespace assert_internal

// `RIEGELI_CHECK(expr)` checks that `expr` is `true`, terminating the program
// if not.
//
// `RIEGELI_CHECK_{EQ,NE,LT,GT,LE,GE}(a, b)` check the relationship between `a`
// and `b`, and include values of `a` and `b` in the failure message.
//
// `RIEGELI_CHECK_NOTNULL(expr)` checks that `expr` is not `nullptr` and returns
// `expr`.
//
// `RIEGELI_CHECK_UNREACHABLE()` checks that this expression is not reached.
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
#elif defined(_WIN32)
#define RIEGELI_INTERNAL_FUNCTION __FUNCSIG__
#else
#define RIEGELI_INTERNAL_FUNCTION __func__
#endif

#define RIEGELI_INTERNAL_CHECK_OP(name, op, a, b)                              \
  while (::riegeli::assert_internal::CheckResult riegeli_internal_check =      \
             ::riegeli::assert_internal::Check##name(#a " " #op " " #b, a, b)) \
  ::riegeli::assert_internal::CheckFailed(__FILE__, __LINE__,                  \
                                          RIEGELI_INTERNAL_FUNCTION,           \
                                          riegeli_internal_check.message())    \
      .stream()

#define RIEGELI_CHECK(expr)                                                 \
  while (ABSL_PREDICT_FALSE(!(expr)))                                       \
  ::riegeli::assert_internal::CheckFailed(__FILE__, __LINE__,               \
                                          RIEGELI_INTERNAL_FUNCTION, #expr) \
      .stream()
#define RIEGELI_CHECK_EQ(a, b) RIEGELI_INTERNAL_CHECK_OP(Eq, ==, a, b)
#define RIEGELI_CHECK_NE(a, b) RIEGELI_INTERNAL_CHECK_OP(Ne, !=, a, b)
#define RIEGELI_CHECK_LT(a, b) RIEGELI_INTERNAL_CHECK_OP(Lt, <, a, b)
#define RIEGELI_CHECK_GT(a, b) RIEGELI_INTERNAL_CHECK_OP(Gt, >, a, b)
#define RIEGELI_CHECK_LE(a, b) RIEGELI_INTERNAL_CHECK_OP(Le, <=, a, b)
#define RIEGELI_CHECK_GE(a, b) RIEGELI_INTERNAL_CHECK_OP(Ge, >=, a, b)
#define RIEGELI_CHECK_NOTNULL(expr)                                   \
  ::riegeli::assert_internal::CheckNotNull(__FILE__, __LINE__,        \
                                           RIEGELI_INTERNAL_FUNCTION, \
                                           #expr " != nullptr", expr)
#define RIEGELI_CHECK_UNREACHABLE()                                \
  ::riegeli::assert_internal::CheckFailed(                         \
      __FILE__, __LINE__, RIEGELI_INTERNAL_FUNCTION, "Impossible") \
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
  while (false && !(expr)) ::riegeli::assert_internal::UnreachableStream()

#define RIEGELI_ASSERT_EQ(a, b) RIEGELI_ASSERT((a) == (b))
#define RIEGELI_ASSERT_NE(a, b) RIEGELI_ASSERT((a) != (b))
#define RIEGELI_ASSERT_LT(a, b) RIEGELI_ASSERT((a) < (b))
#define RIEGELI_ASSERT_GT(a, b) RIEGELI_ASSERT((a) > (b))
#define RIEGELI_ASSERT_LE(a, b) RIEGELI_ASSERT((a) <= (b))
#define RIEGELI_ASSERT_GE(a, b) RIEGELI_ASSERT((a) >= (b))
#define RIEGELI_ASSERT_NOTNULL(expr) \
  ::riegeli::assert_internal::AssertNotNull(expr)
#define RIEGELI_ASSERT_UNREACHABLE() \
  ::riegeli::assert_internal::UnreachableStream()

#endif  // !RIEGELI_DEBUG

// Asserts that a region of memory is initialized, which is checked when running
// under memory sanitizer.
inline void AssertInitialized(ABSL_ATTRIBUTE_UNUSED const char* data,
                              ABSL_ATTRIBUTE_UNUSED size_t size) {
#ifdef MEMORY_SANITIZER
  __msan_check_mem_is_initialized(data, size);
#endif
}

// Marks that a region of memory should be treated as uninitialized, which is
// checked when running under memory sanitizer.
inline void MarkPoisoned(ABSL_ATTRIBUTE_UNUSED const char* data,
                         ABSL_ATTRIBUTE_UNUSED size_t size) {
#ifdef MEMORY_SANITIZER
  __msan_poison(data, size);
#endif
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_ASSERT_H_
