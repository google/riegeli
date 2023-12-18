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

#ifndef RIEGELI_BASE_COMPARE_H_
#define RIEGELI_BASE_COMPARE_H_

#include <type_traits>

#include "absl/meta/type_traits.h"
#if !__cpp_impl_three_way_comparison
#include "absl/types/compare.h"
#endif

// Emulate C++20 `operator<=>` machinery for earlier C++ versions.

namespace riegeli {

// `PartialOrdering` is `std::partial_ordering` in C++20 or
// `absl::partial_ordering` in earlier C++ versions.
#if __cpp_impl_three_way_comparison
using PartialOrdering = decltype(0.0 <=> 0.0);
#else
using PartialOrdering = absl::partial_ordering;
#endif

// `WeakOrdering` is not provided because it cannot be implemented without
// conditionally including `<compare>`.

// `StrongOrdering` is `std::strong_ordering` in C++20 or
// `absl::strong_ordering` in earlier C++ versions.
#if __cpp_impl_three_way_comparison
using StrongOrdering = decltype(0 <=> 0);
#else
using StrongOrdering = absl::strong_ordering;
#endif

// Define `friend auto RIEGELI_COMPARE` instead of C++20
// `friend auto operator<=>`.
//
// It should return `PartialOrdering` or `StrongOrdering`.
//
// It is meant to be called by `Compare(a, b)`, not directly as
// `RIEGELI_COMPARE(a, b)`.
#if __cpp_impl_three_way_comparison
#define RIEGELI_COMPARE operator<=>
#else
#define RIEGELI_COMPARE RiegeliCompare
#endif

// `IsOrdering<T>::value` is `true` if values of type `T` can be assumed to
// indicate an ordering: they are comparable with literal 0.
//
// This includes `{std,absl}::{partial,weak,strong}_ordering`, and `int` being
// the result of `std::memcmp()` or `absl::string_view::compare()`.

template <typename T, typename Enable = void>
struct IsOrdering : std::false_type {};

template <typename T>
struct IsOrdering<T, absl::void_t<decltype(std::declval<T>() < 0),
                                  decltype(std::declval<T>() > 0),
                                  decltype(std::declval<T>() == 0)>>
    : std::true_type {};

// `IsTotalOrdering<T>::value` is `true` if values of type `T` can be assumed to
// indicate a total ordering: they are comparable with literal 0, and
// `T::unordered` is not defined.
//
// This includes `{std,absl}::{weak,strong}_ordering`, and `int` being the
// result of `std::memcmp()` or `absl::string_view::compare()`.

template <typename T, typename Enable = void>
struct IsTotalOrdering : IsOrdering<T> {};

template <typename T>
struct IsTotalOrdering<T, absl::void_t<decltype(T::unordered)>>
    : std::false_type {};

namespace compare_internal {

template <typename T, typename Enable = void>
struct IsTotalOrderingWithEqual : std::false_type {};

template <typename T>
struct IsTotalOrderingWithEqual<T, absl::void_t<decltype(T::equal)>>
    : IsTotalOrdering<T> {};

}  // namespace compare_internal

// `IsStrongOrdering<T>::value` is `true` if values of type `T` can be assumed
// to indicate a strong ordering: they are comparable with literal 0,
// `T::unordered` is not defined, and either `T::equivalent` is not defined or
// `T::equal` is defined too.
//
// This includes `{std,absl}::strong_ordering`, and `int` being the result of
// `std::memcmp()` or `absl::string_view::compare()`.

template <typename T, typename Enable = void>
struct IsStrongOrdering : IsTotalOrdering<T> {};

template <typename T>
struct IsStrongOrdering<T, absl::void_t<decltype(T::equivalent)>>
    : compare_internal::IsTotalOrderingWithEqual<T> {};

// Converts a value indicating an ordering to `PartialOrdering`.

template <typename T,
          std::enable_if_t<
              absl::conjunction<IsOrdering<T>,
                                std::is_convertible<T, PartialOrdering>>::value,
              int> = 0>
inline PartialOrdering AsPartialOrdering(T ordering) {
  return ordering;
}

template <typename T,
          std::enable_if_t<absl::conjunction<IsOrdering<T>,
                                             absl::negation<std::is_convertible<
                                                 T, PartialOrdering>>>::value,
                           int> = 0>
inline PartialOrdering AsPartialOrdering(T ordering) {
  return ordering < 0    ? PartialOrdering::less
         : ordering > 0  ? PartialOrdering::greater
         : ordering == 0 ? PartialOrdering::equivalent
                         : PartialOrdering::unordered;
}

// Converts a value indicating a strong ordering to `StrongOrdering`.

template <typename T,
          std::enable_if_t<
              absl::conjunction<IsStrongOrdering<T>,
                                std::is_convertible<T, StrongOrdering>>::value,
              int> = 0>
inline StrongOrdering AsStrongOrdering(T ordering) {
  return ordering;
}

template <typename T,
          std::enable_if_t<absl::conjunction<IsStrongOrdering<T>,
                                             absl::negation<std::is_convertible<
                                                 T, StrongOrdering>>>::value,
                           int> = 0>
inline StrongOrdering AsStrongOrdering(T ordering) {
  return ordering < 0   ? StrongOrdering::less
         : ordering > 0 ? StrongOrdering::greater
                        : StrongOrdering::equal;
}

#if !__cpp_impl_three_way_comparison

// Definitions of `RIEGELI_COMPARE` which in C++20 are provided automatically.

template <typename A, typename B,
          std::enable_if_t<absl::conjunction<std::is_integral<A>,
                                             std::is_integral<B>>::value,
                           int> = 0>
inline StrongOrdering RIEGELI_COMPARE(A a, B b) {
  return a < b   ? StrongOrdering::less
         : a > b ? StrongOrdering::greater
                 : StrongOrdering::equal;
}

template <
    typename A, typename B,
    std::enable_if_t<
        absl::conjunction<absl::negation<absl::conjunction<
                              std::is_integral<A>, std::is_integral<B>>>,
                          std::is_arithmetic<A>, std::is_arithmetic<B>>::value,
        int> = 0>
inline PartialOrdering RIEGELI_COMPARE(A a, B b) {
  static_assert(
      std::is_floating_point<A>::value || std::is_floating_point<B>::value,
      "Arithmetic types which are not integral types "
      "must be floating point types");
  return a < b    ? PartialOrdering::less
         : a > b  ? PartialOrdering::greater
         : a == b ? PartialOrdering::equivalent
                  : PartialOrdering::unordered;
}

template <typename T, std::enable_if_t<std::is_enum<T>::value, int> = 0>
inline StrongOrdering RIEGELI_COMPARE(T a, T b) {
  return a < b   ? StrongOrdering::less
         : a > b ? StrongOrdering::greater
                 : StrongOrdering::equal;
}

template <typename T>
inline StrongOrdering RIEGELI_COMPARE(T* a, T* b) {
  return a < b   ? StrongOrdering::less
         : a > b ? StrongOrdering::greater
                 : StrongOrdering::equal;
}

#endif

namespace compare_internal {

#if !__cpp_impl_three_way_comparison

template <typename A, typename B, typename Enable = void>
struct HasEqual : std::false_type {};

template <typename A, typename B>
struct HasEqual<A, B,
                absl::void_t<decltype(std::declval<const A&>() ==
                                      std::declval<const B&>())>>
    : std::true_type {};

#endif

template <typename A, typename B, typename Enable = void>
struct HasCompare : std::false_type {};

template <typename A, typename B>
struct HasCompare<A, B,
                  absl::void_t<decltype(
#if __cpp_impl_three_way_comparison
                      std::declval<const A&>() <=> std::declval<const B&>()
#else
                      RIEGELI_COMPARE(std::declval<const A&>(),
                                      std::declval<const B&>())
#endif
                          )>> : std::true_type {
};

template <typename T, typename Enable = void>
struct IsDedicatedOrdering : std::false_type {};

template <typename T>
struct IsDedicatedOrdering<
    T, absl::void_t<decltype(T::less), decltype(T::greater)>> : std::true_type {
};

template <typename T, typename Enable = void>
struct HasCompareWithLiteral0 : std::false_type {};

template <typename T>
struct HasCompareWithLiteral0<T, absl::void_t<decltype(
#if __cpp_impl_three_way_comparison
                                     0 <=> std::declval<T>()
#else
                                     RIEGELI_COMPARE(0, std::declval<T>())
#endif
                                         )>> : std::true_type {
};

}  // namespace compare_internal

// Call `Compare(a, b)` instead of C++20 `a <=> b`.
template <typename A, typename B,
          std::enable_if_t<compare_internal::HasCompare<A, B>::value, int> = 0>
inline auto Compare(const A& a, const B& b) {
#if __cpp_impl_three_way_comparison
  return a <=> b;
#else
  return RIEGELI_COMPARE(a, b);
#endif
}

// Call `NegateOrdering(ordering)` instead of C++20 `0 <=> ordering`.
//
// `Compare(0, ordering)` does not work because it does not properly forward to
// `<=>` the property that the argument is a literal 0.

template <typename Ordering,
          std::enable_if_t<
              absl::conjunction<
                  compare_internal::IsDedicatedOrdering<Ordering>,
                  compare_internal::HasCompareWithLiteral0<Ordering>>::value,
              int> = 0>
inline Ordering NegateOrdering(Ordering ordering) {
#if __cpp_impl_three_way_comparison
  return 0 <=> ordering;
#else
  return RIEGELI_COMPARE(0, ordering);
#endif
}

template <typename Ordering,
          std::enable_if_t<
              absl::conjunction<
                  compare_internal::IsDedicatedOrdering<Ordering>,
                  absl::negation<compare_internal::HasCompareWithLiteral0<
                      Ordering>>>::value,
              int> = 0>
inline Ordering NegateOrdering(Ordering ordering) {
  if (0 < ordering) return Ordering::less;
  if (0 > ordering) return Ordering::greater;
  return ordering;
}

// For types which support equality, derive `T` from `WithEqual<T>`, and define
// `friend bool operator==` with the first parameter of type `const T&` or `T`,
// and the second parameter of the same type, or possibly also of other types.
//
// `WithEqual` provides `!=`. For heterogeneous equality it provides `==` and
// `!=` with swapped parameters.
//
// In C++20 this is automatic.
template <typename T>
class WithEqual {
 public:
#if !__cpp_impl_three_way_comparison
  template <
      typename Other,
      std::enable_if_t<compare_internal::HasEqual<T, Other>::value, int> = 0>
  friend bool operator!=(const T& a, const Other& b) {
    return !(a == b);
  }

  template <typename Other,
            std::enable_if_t<
                absl::conjunction<absl::negation<std::is_same<Other, T>>,
                                  compare_internal::HasEqual<T, Other>>::value,
                int> = 0>
  friend bool operator==(const Other& a, const T& b) {
    return b == a;
  }
  template <typename Other,
            std::enable_if_t<
                absl::conjunction<absl::negation<std::is_same<Other, T>>,
                                  compare_internal::HasEqual<T, Other>>::value,
                int> = 0>
  friend bool operator!=(const Other& a, const T& b) {
    return !(b == a);
  }
#endif
};

// For types which support comparison, derive `T` from `WithCompare<T>`. and
// define `friend bool operator==` and `friend auto RIEGELI_COMPARE` with the
// first parameter of type `const T&` or `T`, and the second parameter of the
// same type, or possibly also of other types.
//
// `WithCompare` provides `!=`, `<`, `>`, `<=`, and `>=`. For heterogeneous
// comparison it provides `==`, `!=`, `RIEGELI_COMPARE, `<`, `>`, `<=`, and `>=`
// with swapped parameters.
//
// In C++20 this is automatic.
template <typename T>
class WithCompare : public WithEqual<T> {
 public:
#if !__cpp_impl_three_way_comparison
  template <
      typename Other,
      std::enable_if_t<compare_internal::HasCompare<T, Other>::value, int> = 0>
  friend bool operator<(const T& a, const Other& b) {
    return RIEGELI_COMPARE(a, b) < 0;
  }
  template <
      typename Other,
      std::enable_if_t<compare_internal::HasCompare<T, Other>::value, int> = 0>
  friend bool operator>(const T& a, const Other& b) {
    return RIEGELI_COMPARE(a, b) > 0;
  }
  template <
      typename Other,
      std::enable_if_t<compare_internal::HasCompare<T, Other>::value, int> = 0>
  friend bool operator<=(const T& a, const Other& b) {
    return RIEGELI_COMPARE(a, b) <= 0;
  }
  template <
      typename Other,
      std::enable_if_t<compare_internal::HasCompare<T, Other>::value, int> = 0>
  friend bool operator>=(const T& a, const Other& b) {
    return RIEGELI_COMPARE(a, b) >= 0;
  }

  template <typename Other,
            std::enable_if_t<absl::conjunction<
                                 absl::negation<std::is_same<Other, T>>,
                                 compare_internal::HasCompare<T, Other>>::value,
                             int> = 0>
  friend auto RIEGELI_COMPARE(const Other& a, const T& b) {
    return NegateOrdering(RIEGELI_COMPARE(b, a));
  }
  template <typename Other,
            std::enable_if_t<absl::conjunction<
                                 absl::negation<std::is_same<Other, T>>,
                                 compare_internal::HasCompare<T, Other>>::value,
                             int> = 0>
  friend bool operator<(const Other& a, const T& b) {
    return 0 < RIEGELI_COMPARE(b, a);
  }
  template <typename Other,
            std::enable_if_t<absl::conjunction<
                                 absl::negation<std::is_same<Other, T>>,
                                 compare_internal::HasCompare<T, Other>>::value,
                             int> = 0>
  friend bool operator>(const Other& a, const T& b) {
    return 0 > RIEGELI_COMPARE(b, a);
  }
  template <typename Other,
            std::enable_if_t<absl::conjunction<
                                 absl::negation<std::is_same<Other, T>>,
                                 compare_internal::HasCompare<T, Other>>::value,
                             int> = 0>
  friend bool operator<=(const Other& a, const T& b) {
    return 0 <= RIEGELI_COMPARE(b, a);
  }
  template <typename Other,
            std::enable_if_t<absl::conjunction<
                                 absl::negation<std::is_same<Other, T>>,
                                 compare_internal::HasCompare<T, Other>>::value,
                             int> = 0>
  friend bool operator>=(const Other& a, const T& b) {
    return 0 >= RIEGELI_COMPARE(b, a);
  }
#endif
};

}  // namespace riegeli

#endif  // RIEGELI_BASE_COMPARE_H_
