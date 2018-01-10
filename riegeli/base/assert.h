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

#include <ostream>
#include <utility>

#include "riegeli/base/base.h"
#include "riegeli/base/port.h"

namespace riegeli {

namespace internal {

// RIEGELI_DEBUG determines whether assertions are verified or just assumed.
// By default it follows NDEBUG.

#ifndef RIEGELI_DEBUG
#define RIEGELI_DEBUG !defined(NDEBUG)
#endif

#if RIEGELI_DEBUG

class FailedAssert {
 public:
  FailedAssert() = default;

  // Prints "Assertion failed at file:line in function: message ".
  RIEGELI_ATTRIBUTE_COLD FailedAssert(const char* file, int line,
                                      const char* function,
                                      const char* message);

  // Prints "Impossible happened at file:line in function ".
  RIEGELI_ATTRIBUTE_COLD
  FailedAssert(const char* file, int line, const char* function);

  // Prints a newline and aborts.
  RIEGELI_ATTRIBUTE_NORETURN ~FailedAssert();

  std::ostream& stream();
};

// Prints "Assertion failed at file:line in function: message ".
RIEGELI_ATTRIBUTE_COLD std::ostream& FailedAssertMessage(const char* file,
                                                         int line,
                                                         const char* function,
                                                         const char* message);

#define RIEGELI_INTERNAL_DEFINE_ASSERT_OP(name, op)                   \
  template <typename A, typename B>                                   \
  bool Assert##name(const char* file, int line, const char* function, \
                    const char* assertion, const A& a, const B& b) {  \
    if (RIEGELI_LIKELY(a op b)) {                                     \
      return true;                                                    \
    } else {                                                          \
      FailedAssertMessage(file, line, function, assertion)            \
          << "(" << a << " vs. " << b << ") ";                        \
      return false;                                                   \
    }                                                                 \
  }                                                                   \
  static_assert(true, "")  // Eat a semicolon.

RIEGELI_INTERNAL_DEFINE_ASSERT_OP(Eq, ==);
RIEGELI_INTERNAL_DEFINE_ASSERT_OP(Ne, !=);
RIEGELI_INTERNAL_DEFINE_ASSERT_OP(Lt, <);
RIEGELI_INTERNAL_DEFINE_ASSERT_OP(Gt, >);
RIEGELI_INTERNAL_DEFINE_ASSERT_OP(Le, <=);
RIEGELI_INTERNAL_DEFINE_ASSERT_OP(Ge, >=);

#undef RIEGELI_INTERNAL_DEFINE_ASSERT_OP

template <typename T>
T AssertNotNull(const char* file, int line, const char* function,
                const char* message, T&& value) {
  if (RIEGELI_UNLIKELY(value == nullptr)) {
    FailedAssert(file, line, function, message);
  }
  return std::forward<T>(value);
}

#else  // !RIEGELI_DEBUG

#if RIEGELI_INTERNAL_HAS_BUILTIN(__builtin_unreachable) || \
    RIEGELI_INTERNAL_IS_GCC_VERSION(4, 5)
#define RIEGELI_INTERNAL_UNREACHABLE() __builtin_unreachable()
#elif defined(_MSC_VER)
#define RIEGELI_INTERNAL_UNREACHABLE() __assume(false)
#else
#define RIEGELI_INTERNAL_UNREACHABLE() (void)0
#endif

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

// "Asserts" below means: if RIEGELI_DEBUG is true, verifies that the assertion
// holds and aborts the program if not; if RIEGELI_DEBUG is false, does nothing
// but the behavior is undefined if the assertion does not hold.
//
// RIEGELI_ASSERT(expr) asserts that expr is true.
//
// RIEGELI_ASSERT_{EQ,NE,LT,GT,LE,GE}(a, b) assert the relationship between a
// and b, and include values of a and b in the failure message.
//
// RIEGELI_ASSERT_NOTNULL(expr) asserts that expr is not nullptr.
//
// RIEGELI_UNREACHABLE() asserts that this expression is not reached.
//
// RIEGELI_ASSERT_NOTNULL(expr) is an expression which evaluates to expr.
// The remaining RIEGELI_ASSERT* macros can be followed by streaming <<
// operators in order to append more details to the failure message
// (streamed expressions are evaluated only on assertion failure).

#if RIEGELI_DEBUG

#if defined(__clang__) || RIEGELI_INTERNAL_IS_GCC_VERSION(2, 6)
#define RIEGELI_INTERNAL_FUNCTION __PRETTY_FUNCTION__
#elif defined(_MSC_VER)
#define RIEGELI_INTERNAL_FUNCTION __FUNCSIG__
#else
#define RIEGELI_INTERNAL_FUNCTION __func__
#endif

// "switch (0) default:" prevents warnings about dangling "else" if this is used
// inside "if".
#define RIEGELI_ASSERT(expr)                                              \
  switch (0)                                                              \
  default:                                                                \
    if (RIEGELI_LIKELY(expr)) {                                           \
    } else                                                                \
      ::riegeli::internal::FailedAssert(__FILE__, __LINE__,               \
                                        RIEGELI_INTERNAL_FUNCTION, #expr) \
          .stream()

// "switch (0) default:" prevents warnings about dangling "else" if this is used
// inside "if".
#define RIEGELI_INTERNAL_ASSERT_OP(name, op, a, b)                    \
  switch (0)                                                          \
  default:                                                            \
    if (::riegeli::internal::Assert##name(__FILE__, __LINE__,         \
                                          RIEGELI_INTERNAL_FUNCTION,  \
                                          #a " " #op " " #b, a, b)) { \
    } else                                                            \
      ::riegeli::internal::FailedAssert().stream()

#define RIEGELI_ASSERT_EQ(a, b) RIEGELI_INTERNAL_ASSERT_OP(Eq, ==, a, b)
#define RIEGELI_ASSERT_NE(a, b) RIEGELI_INTERNAL_ASSERT_OP(Ne, !=, a, b)
#define RIEGELI_ASSERT_LT(a, b) RIEGELI_INTERNAL_ASSERT_OP(Lt, <, a, b)
#define RIEGELI_ASSERT_GT(a, b) RIEGELI_INTERNAL_ASSERT_OP(Gt, >, a, b)
#define RIEGELI_ASSERT_LE(a, b) RIEGELI_INTERNAL_ASSERT_OP(Le, <=, a, b)
#define RIEGELI_ASSERT_GE(a, b) RIEGELI_INTERNAL_ASSERT_OP(Ge, >=, a, b)

#define RIEGELI_ASSERT_NOTNULL(expr)                            \
  ::riegeli::internal::AssertNotNull(__FILE__, __LINE__,        \
                                     RIEGELI_INTERNAL_FUNCTION, \
                                     #expr " != nullptr", expr)

#define RIEGELI_UNREACHABLE()                                  \
  ::riegeli::internal::FailedAssert(__FILE__, __LINE__,        \
                                    RIEGELI_INTERNAL_FUNCTION) \
      .stream()

#else  // !RIEGELI_DEBUG

// "switch (0) default:" prevents warnings about dangling "else" if this is used
// inside "if".
#define RIEGELI_ASSERT(expr) \
  switch (0)                 \
  default:                   \
    if (true || (expr)) {    \
    } else                   \
      ::riegeli::internal::UnreachableStream()

// "switch (0) default:" prevents warnings about dangling "else" if this is used
// inside "if".
#define RIEGELI_INTERNAL_ASSERT_OP(name, op, a, b) \
  switch (0)                                       \
  default:                                         \
    if (true || (a)op(b)) {                        \
    } else                                         \
      ::riegeli::internal::UnreachableStream()

#define RIEGELI_ASSERT_EQ(a, b) RIEGELI_INTERNAL_ASSERT_OP(Eq, ==, a, b)
#define RIEGELI_ASSERT_NE(a, b) RIEGELI_INTERNAL_ASSERT_OP(Ne, !=, a, b)
#define RIEGELI_ASSERT_LT(a, b) RIEGELI_INTERNAL_ASSERT_OP(Lt, <, a, b)
#define RIEGELI_ASSERT_GT(a, b) RIEGELI_INTERNAL_ASSERT_OP(Gt, >, a, b)
#define RIEGELI_ASSERT_LE(a, b) RIEGELI_INTERNAL_ASSERT_OP(Le, <=, a, b)
#define RIEGELI_ASSERT_GE(a, b) RIEGELI_INTERNAL_ASSERT_OP(Ge, >=, a, b)
#define RIEGELI_ASSERT_NOTNULL(expr) ::riegeli::internal::AssertNotNull(expr)
#define RIEGELI_UNREACHABLE() ::riegeli::internal::UnreachableStream()

#endif  // !RIEGELI_DEBUG

}  // namespace riegeli

#endif  // RIEGELI_BASE_ASSERT_H_
