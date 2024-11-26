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

#include <ostream>  // IWYU pragma: export
#include <sstream>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/meta/type_traits.h"
#include "riegeli/base/debug.h"
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

#if __cpp_lib_unreachable
#define RIEGELI_INTERNAL_UNREACHABLE() ::std::unreachable()
#elif RIEGELI_INTERNAL_HAS_BUILTIN(__builtin_unreachable) || \
    RIEGELI_INTERNAL_IS_GCC_VERSION(4, 5)
#define RIEGELI_INTERNAL_UNREACHABLE() __builtin_unreachable()
#elif defined(_WIN32)
#define RIEGELI_INTERNAL_UNREACHABLE() __assume(false)
#else
#define RIEGELI_INTERNAL_UNREACHABLE() for (;;)
#endif

// Indicates that a check succeeded or failed.
//
// If it failed, stores a stream for writing the header.
class CheckResult {
 public:
  // A check succeeded.
  CheckResult() = default;

  // A check failed. The header will begin with
  // "Check failed in function: prefix".
  explicit CheckResult(const char* function, const char* prefix);

  CheckResult(const CheckResult& that) = default;
  CheckResult& operator=(const CheckResult& that) = default;

  // Returns `true` if the check succeeded.
  explicit operator bool() const { return header_ == nullptr; }

  // Returns the header stream.
  //
  // Precondition: the check failed, i.e. `*this` is `false`.
  std::ostringstream& header() { return *header_; }

 private:
  std::ostringstream* header_ = nullptr;
};

// Stores a `CheckResult` and a stream for adding details to the message.
// The message is "header; details", or just "header" if details are empty.
// In the destructor, outputs the message and terminates the program.
class CheckFailed {
 public:
  explicit CheckFailed(const char* file, int line, CheckResult check_result);

  // Allows to add details to the message by writing to the stream.
  std::ostream& details() { return *details_; }

  // Prints the check failure message and terminates the program.
  ABSL_ATTRIBUTE_NORETURN ~CheckFailed();

 private:
  const char* file_;
  int line_;
  CheckResult check_result_;
  std::ostringstream* details_ = nullptr;
};

// Indicates that a check failed with the message header
// "Check failed in function: assertion (a vs. b)".
template <typename A, typename B>
ABSL_ATTRIBUTE_COLD CheckResult CheckOpResult(const char* function,
                                              const char* assertion, const A& a,
                                              const B& b) {
  CheckResult check_result(function, assertion);
  check_result.header() << " (" << riegeli::Debug(a) << " vs. "
                        << riegeli::Debug(b) << ")";
  return check_result;
}

// Indicates that a check failed with the message header
// "Check failed in function: expression is OK (status)".

namespace assert_internal {

template <typename T, typename Enable = void>
struct HasStatus : std::false_type {};

template <typename T>
struct HasStatus<T, absl::void_t<decltype(std::declval<T>().status())>>
    : std::true_type {};

}  // namespace assert_internal

template <
    typename StatusType,
    std::enable_if_t<!assert_internal::HasStatus<StatusType>::value, int> = 0>
ABSL_ATTRIBUTE_COLD CheckResult CheckOkResult(const char* function,
                                              const char* expression,
                                              const StatusType& status) {
  CheckResult check_result(function, expression);
  check_result.header() << " is OK (" << status << ")";
  return check_result;
}

template <
    typename StatusType,
    std::enable_if_t<assert_internal::HasStatus<StatusType>::value, int> = 0>
ABSL_ATTRIBUTE_COLD CheckResult CheckOkResult(const char* function,
                                              const char* expression,
                                              const StatusType& status_or) {
  return CheckOkResult(function, expression, status_or.status());
}

// Writes "Check failed in function: expression != nullptr" and terminates
// the program.
ABSL_ATTRIBUTE_NORETURN void CheckNotNullFailed(const char* file, int line,
                                                const char* function,
                                                const char* expression);

// Indicates that a check failed with the message header
// "Check failed in function: Impossible".
ABSL_ATTRIBUTE_COLD CheckResult CheckImpossibleResult(const char* function);

// These functions allow using `a` and `b` multiple times without reevaluation.
// They are small enough to be inlined, with the slow path delegated to
// `CheckOpResult()`.
#define RIEGELI_INTERNAL_DEFINE_CHECK_OP(name, op)                            \
  template <typename A, typename B>                                           \
  inline CheckResult Check##name(const char* function, const char* assertion, \
                                 const A& a, const B& b) {                    \
    if (ABSL_PREDICT_TRUE(a op b)) return CheckResult();                      \
    CheckResult check_result = CheckOpResult(function, assertion, a, b);      \
    if (check_result) RIEGELI_INTERNAL_UNREACHABLE();                         \
    return check_result;                                                      \
  }                                                                           \
  static_assert(true, "")  // Eat a semicolon.

RIEGELI_INTERNAL_DEFINE_CHECK_OP(Eq, ==);
RIEGELI_INTERNAL_DEFINE_CHECK_OP(Ne, !=);
RIEGELI_INTERNAL_DEFINE_CHECK_OP(Lt, <);
RIEGELI_INTERNAL_DEFINE_CHECK_OP(Gt, >);
RIEGELI_INTERNAL_DEFINE_CHECK_OP(Le, <=);
RIEGELI_INTERNAL_DEFINE_CHECK_OP(Ge, >=);

#undef RIEGELI_INTERNAL_DEFINE_CHECK_OP

template <typename StatusType>
inline CheckResult CheckOk(const char* function, const char* expression,
                           const StatusType& status) {
  if (ABSL_PREDICT_TRUE(status.ok())) return CheckResult();
  CheckResult check_result = CheckOkResult(function, expression, status);
  if (check_result) RIEGELI_INTERNAL_UNREACHABLE();
  return check_result;
}

template <typename T>
inline T&& CheckNotNull(const char* file, int line, const char* function,
                        const char* expression, T&& value) {
  if (ABSL_PREDICT_FALSE(value == nullptr)) {
    CheckNotNullFailed(file, line, function, expression);
  }
  return std::forward<T>(value);
}

#if !RIEGELI_DEBUG

// These functions allow using `a` and `b` multiple times without reevaluation.
// They are small enough to be inlined.
#define RIEGELI_INTERNAL_DEFINE_EVAL_ASSERT_OP(name, op)   \
  template <typename A, typename B>                        \
  inline bool EvalAssert##name(const A& a, const B& b) {   \
    return true || a op b; /* Check that this compiles. */ \
  }                                                        \
  static_assert(true, "")  // Eat a semicolon.

RIEGELI_INTERNAL_DEFINE_EVAL_ASSERT_OP(Eq, ==);
RIEGELI_INTERNAL_DEFINE_EVAL_ASSERT_OP(Ne, !=);
RIEGELI_INTERNAL_DEFINE_EVAL_ASSERT_OP(Lt, <);
RIEGELI_INTERNAL_DEFINE_EVAL_ASSERT_OP(Gt, >);
RIEGELI_INTERNAL_DEFINE_EVAL_ASSERT_OP(Le, <=);
RIEGELI_INTERNAL_DEFINE_EVAL_ASSERT_OP(Ge, >=);

#undef RIEGELI_INTERNAL_DEFINE_EVAL_ASSERT_OP

template <typename T>
inline T&& EvalAssertNotNull(T&& value) {
  ABSL_ATTRIBUTE_UNUSED const bool condition =
      true || value == nullptr;  // Check that this compiles.
  return std::forward<T>(value);
}

template <typename StatusType>
inline bool EvalAssertOk(const StatusType& status) {
  return true || status.ok();  // Check that this compiles.
}

template <typename T>
inline T&& AssumeNotNull(T&& value) {
  if (value == nullptr) RIEGELI_INTERNAL_UNREACHABLE();
  return std::forward<T>(value);
}

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
  UnreachableStream& operator<<(ABSL_ATTRIBUTE_UNUSED T&& src) {
    return *this;
  }
};

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif  // !RIEGELI_DEBUG

// Allow `MACRO()` expanding to `if (condition) ...; else ...`, to be usable as
// `if (condition) MACRO();` without a warning about ambiguous `else`.
// The definition of `MACRO()` must begin with `RIEGELI_INTERNAL_BLOCK_ELSE`.
#define RIEGELI_INTERNAL_BLOCK_ELSE \
  switch (0)                        \
  case 0:                           \
  default:

}  // namespace assert_internal

// `RIEGELI_CHECK(expr)` checks that `expr` is `true`, terminating the program
// if not.
//
// `RIEGELI_CHECK_{EQ,NE,LT,GT,LE,GE}(a, b)` check the relationship between `a`
// and `b`, and include values of `a` and `b` in the failure message. The values
// must be printable using `riegeli::Debug()`.
//
// `RIEGELI_CHECK_NOTNULL(expr)` checks that `expr` is not `nullptr` and returns
// `expr`.
//
// `RIEGELI_CHECK_OK(expr)` checks that `expr.ok()`, and includes either
// `expr.status()` or `expr` in the failure message. Supported types include
// `absl::Status`, `absl::StatusOr<T>`, and classes deriving from `Object`.
//
// `RIEGELI_CHECK_NOTNULL(expr)` is an expression which evaluates to `expr`.
// The remaining `RIEGELI_CHECK*` macros can be followed by streaming `<<`
// operators in order to append more details to the failure message
// (streamed expressions are evaluated only on assertion failure).
//
// `RIEGELI_CHECK_UNREACHABLE()` checks that this point is not reached.

#if defined(__clang__) || RIEGELI_INTERNAL_IS_GCC_VERSION(2, 6)
#define RIEGELI_INTERNAL_FUNCTION __PRETTY_FUNCTION__
#elif defined(_WIN32)
#define RIEGELI_INTERNAL_FUNCTION __FUNCSIG__
#else
#define RIEGELI_INTERNAL_FUNCTION __func__
#endif

#define RIEGELI_INTERNAL_CHECK_OP(name, op, a, b)                            \
  RIEGELI_INTERNAL_BLOCK_ELSE                                                \
  if (const ::riegeli::assert_internal::CheckResult riegeli_internal_check = \
          ::riegeli::assert_internal::Check##name(RIEGELI_INTERNAL_FUNCTION, \
                                                  #a " " #op " " #b, a, b))  \
    ;                                                                        \
  else                                                                       \
    ::riegeli::assert_internal::CheckFailed(__FILE__, __LINE__,              \
                                            riegeli_internal_check)          \
        .details()

#define RIEGELI_CHECK(expr)                                                \
  RIEGELI_INTERNAL_BLOCK_ELSE                                              \
  if (ABSL_PREDICT_TRUE(expr))                                             \
    ;                                                                      \
  else                                                                     \
    ::riegeli::assert_internal::CheckFailed(                               \
        __FILE__, __LINE__,                                                \
        ::riegeli::assert_internal::CheckResult(RIEGELI_INTERNAL_FUNCTION, \
                                                #expr))                    \
        .details()
#define RIEGELI_CHECK_EQ(a, b) RIEGELI_INTERNAL_CHECK_OP(Eq, ==, a, b)
#define RIEGELI_CHECK_NE(a, b) RIEGELI_INTERNAL_CHECK_OP(Ne, !=, a, b)
#define RIEGELI_CHECK_LT(a, b) RIEGELI_INTERNAL_CHECK_OP(Lt, <, a, b)
#define RIEGELI_CHECK_GT(a, b) RIEGELI_INTERNAL_CHECK_OP(Gt, >, a, b)
#define RIEGELI_CHECK_LE(a, b) RIEGELI_INTERNAL_CHECK_OP(Le, <=, a, b)
#define RIEGELI_CHECK_GE(a, b) RIEGELI_INTERNAL_CHECK_OP(Ge, >=, a, b)
#define RIEGELI_CHECK_OK(status)                                             \
  RIEGELI_INTERNAL_BLOCK_ELSE                                                \
  if (const ::riegeli::assert_internal::CheckResult riegeli_internal_check = \
          ::riegeli::assert_internal::CheckOk(RIEGELI_INTERNAL_FUNCTION,     \
                                              #status, status))              \
    ;                                                                        \
  else                                                                       \
    ::riegeli::assert_internal::CheckFailed(__FILE__, __LINE__,              \
                                            riegeli_internal_check)          \
        .details()
#define RIEGELI_CHECK_NOTNULL(expr)         \
  ::riegeli::assert_internal::CheckNotNull( \
      __FILE__, __LINE__, RIEGELI_INTERNAL_FUNCTION, #expr, expr)
#define RIEGELI_CHECK_UNREACHABLE()                      \
  ::riegeli::assert_internal::CheckFailed(               \
      __FILE__, __LINE__,                                \
      ::riegeli::assert_internal::CheckImpossibleResult( \
          RIEGELI_INTERNAL_FUNCTION))                    \
      .details()

// If `RIEGELI_DEBUG` is `true`, `RIEGELI_ASSERT*` macros are equivalent to the
// corresponding `RIEGELI_CHECK*` macros.
//
// If `RIEGELI_DEBUG` is `false`, they do nothing except for ensuring that the
// assertion compiles, and that any code appending to the stream compiles.
//
// There is no `RIEGELI_ASSERT_NOTNULL` because the argument is returned, and
// thus it is necessarily always evaluated also if `RIEGELI_DEBUG` is `false`
// (the semantics of `RIEGELI_ASSERT*` of doing nothing if `RIEGELI_DEBUG` is
// `false` cannot be followed). Use `RIEGELI_EVAL_ASSERT_NOTNULL` instead.
//
// There is no `RIEGELI_ASSERT_UNREACHABLE` because no following code is
// expected, and thus this point is necessarily never reached also if
// `RIEGELI_DEBUG` is `false` (the semantics of `RIEGELI_ASSERT*` of doing
// nothing if `RIEGELI_DEBUG` is `false` cannot be followed). Use
// `RIEGELI_ASSUME_UNREACHABLE` instead.

#if RIEGELI_DEBUG

#define RIEGELI_ASSERT RIEGELI_CHECK
#define RIEGELI_ASSERT_EQ RIEGELI_CHECK_EQ
#define RIEGELI_ASSERT_NE RIEGELI_CHECK_NE
#define RIEGELI_ASSERT_LT RIEGELI_CHECK_LT
#define RIEGELI_ASSERT_GT RIEGELI_CHECK_GT
#define RIEGELI_ASSERT_LE RIEGELI_CHECK_LE
#define RIEGELI_ASSERT_GE RIEGELI_CHECK_GE
#define RIEGELI_ASSERT_OK RIEGELI_CHECK_OK

#else  // !RIEGELI_DEBUG

#define RIEGELI_ASSERT(expr)  \
  RIEGELI_INTERNAL_BLOCK_ELSE \
  if (true || (expr))         \
    ;                         \
  else                        \
    ::riegeli::assert_internal::UnreachableStream()

#define RIEGELI_ASSERT_EQ(a, b) RIEGELI_ASSERT((a) == (b))
#define RIEGELI_ASSERT_NE(a, b) RIEGELI_ASSERT((a) != (b))
#define RIEGELI_ASSERT_LT(a, b) RIEGELI_ASSERT((a) < (b))
#define RIEGELI_ASSERT_GT(a, b) RIEGELI_ASSERT((a) > (b))
#define RIEGELI_ASSERT_LE(a, b) RIEGELI_ASSERT((a) <= (b))
#define RIEGELI_ASSERT_GE(a, b) RIEGELI_ASSERT((a) >= (b))
#define RIEGELI_ASSERT_OK(status) RIEGELI_ASSERT((status).ok())

#endif  // !RIEGELI_DEBUG

// If `RIEGELI_DEBUG` is `true`, `RIEGELI_EVAL_ASSERT*` macros are equivalent to
// the corresponding `RIEGELI_CHECK*` macros.
//
// If `RIEGELI_DEBUG` is `false`, they evaluate the arguments, but do not check
// the assertion, although they verify that evaluating the assertion and any
// code appending to the stream compiles.
//
// There is no `RIEGELI_EVAL_ASSERT_UNREACHABLE` because there is no argument
// to evaluate, and because no following code is expected, and thus this point
// is necessarily never reached also if `RIEGELI_DEBUG` is `false` (the
// semantics of `RIEGELI_EVAL_ASSERT*` of doing nothing besides evaluating the
// arguments cannot be followed). Use `RIEGELI_ASSUME_UNREACHABLE` instead.

#if RIEGELI_DEBUG

#define RIEGELI_EVAL_ASSERT RIEGELI_CHECK
#define RIEGELI_EVAL_ASSERT_EQ RIEGELI_CHECK_EQ
#define RIEGELI_EVAL_ASSERT_NE RIEGELI_CHECK_NE
#define RIEGELI_EVAL_ASSERT_LT RIEGELI_CHECK_LT
#define RIEGELI_EVAL_ASSERT_GT RIEGELI_CHECK_GT
#define RIEGELI_EVAL_ASSERT_LE RIEGELI_CHECK_LE
#define RIEGELI_EVAL_ASSERT_GE RIEGELI_CHECK_GE
#define RIEGELI_EVAL_ASSERT_OK RIEGELI_CHECK_OK
#define RIEGELI_EVAL_ASSERT_NOTNULL RIEGELI_CHECK_NOTNULL

#else  // !RIEGELI_DEBUG

#define RIEGELI_INTERNAL_EVAL_ASSERT_OP(name, a, b)       \
  RIEGELI_INTERNAL_BLOCK_ELSE                             \
  if (::riegeli::assert_internal::EvalAssert##name(a, b)) \
    ;                                                     \
  else                                                    \
    ::riegeli::assert_internal::UnreachableStream()

#define RIEGELI_EVAL_ASSERT(expr) \
  RIEGELI_INTERNAL_BLOCK_ELSE     \
  if ((expr) || true)             \
    ;                             \
  else                            \
    ::riegeli::assert_internal::UnreachableStream()
#define RIEGELI_EVAL_ASSERT_EQ(a, b) RIEGELI_INTERNAL_EVAL_ASSERT_OP(Eq, a, b)
#define RIEGELI_EVAL_ASSERT_NE(a, b) RIEGELI_INTERNAL_EVAL_ASSERT_OP(Ne, a, b)
#define RIEGELI_EVAL_ASSERT_LT(a, b) RIEGELI_INTERNAL_EVAL_ASSERT_OP(Lt, a, b)
#define RIEGELI_EVAL_ASSERT_GT(a, b) RIEGELI_INTERNAL_EVAL_ASSERT_OP(Gt, a, b)
#define RIEGELI_EVAL_ASSERT_LE(a, b) RIEGELI_INTERNAL_EVAL_ASSERT_OP(Le, a, b)
#define RIEGELI_EVAL_ASSERT_GE(a, b) RIEGELI_INTERNAL_EVAL_ASSERT_OP(Ge, a, b)
#define RIEGELI_EVAL_ASSERT_NOTNULL(expr) \
  ::riegeli::assert_internal::EvalAssertNotNull(expr)
#define RIEGELI_EVAL_ASSERT_OK(status)                  \
  RIEGELI_INTERNAL_BLOCK_ELSE                           \
  if (::riegeli::assert_internal::EvalAssertOk(status)) \
    ;                                                   \
  else                                                  \
    ::riegeli::assert_internal::UnreachableStream()

#endif

// If `RIEGELI_DEBUG` is `true`, `RIEGELI_ASSUME*` macros are equivalent to the
// corresponding `RIEGELI_CHECK*` macros.
//
// If `RIEGELI_DEBUG` is `false`, the behavior is undefined if the assertion
// fails, which allows the compiler to perform optimizations based on that.
//
// The condition is evaluated unconditionally, but this should not be relied
// upon, as a future implementation might not ensure this. To make it optimized
// out when `RIEGELI_DEBUG` is `false`, it should use only operations which are
// expected to be optimized out when the result of the condition is not needed,
// in particular it should not call non-inline functions.

#if RIEGELI_DEBUG

#define RIEGELI_ASSUME RIEGELI_CHECK
#define RIEGELI_ASSUME_EQ RIEGELI_CHECK_EQ
#define RIEGELI_ASSUME_NE RIEGELI_CHECK_NE
#define RIEGELI_ASSUME_LT RIEGELI_CHECK_LT
#define RIEGELI_ASSUME_GT RIEGELI_CHECK_GT
#define RIEGELI_ASSUME_LE RIEGELI_CHECK_LE
#define RIEGELI_ASSUME_GE RIEGELI_CHECK_GE
#define RIEGELI_ASSUME_OK RIEGELI_CHECK_OK
#define RIEGELI_ASSUME_NOTNULL RIEGELI_CHECK_NOTNULL
#define RIEGELI_ASSUME_UNREACHABLE RIEGELI_CHECK_UNREACHABLE

#else  // !RIEGELI_DEBUG

#define RIEGELI_ASSUME(expr)  \
  RIEGELI_INTERNAL_BLOCK_ELSE \
  if (expr)                   \
    ;                         \
  else                        \
    RIEGELI_ASSUME_UNREACHABLE()

#define RIEGELI_ASSUME_EQ(a, b) RIEGELI_ASSUME((a) == (b))
#define RIEGELI_ASSUME_NE(a, b) RIEGELI_ASSUME((a) != (b))
#define RIEGELI_ASSUME_LT(a, b) RIEGELI_ASSUME((a) < (b))
#define RIEGELI_ASSUME_GT(a, b) RIEGELI_ASSUME((a) > (b))
#define RIEGELI_ASSUME_LE(a, b) RIEGELI_ASSUME((a) <= (b))
#define RIEGELI_ASSUME_GE(a, b) RIEGELI_ASSUME((a) >= (b))
#define RIEGELI_ASSUME_OK(status) RIEGELI_ASSUME((status).ok())
#define RIEGELI_ASSUME_NOTNULL(expr) \
  ::riegeli::assert_internal::AssumeNotNull(expr)
#define RIEGELI_ASSUME_UNREACHABLE() \
  ::riegeli::assert_internal::UnreachableStream()

#endif  // !RIEGELI_DEBUG

// Deprecated aliases:
#define RIEGELI_ASSERT_NOTNULL RIEGELI_EVAL_ASSERT_NOTNULL
#define RIEGELI_ASSERT_UNREACHABLE RIEGELI_ASSUME_UNREACHABLE

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
