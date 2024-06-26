// Copyright 2024 Google LLC
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

#ifndef RIEGELI_BASE_INVOKER_H_
#define RIEGELI_BASE_INVOKER_H_

#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/meta/type_traits.h"
#include "absl/utility/utility.h"
#include "riegeli/base/type_traits.h"

namespace riegeli {

namespace invoker_internal {

template <typename Enable, typename Function, typename... Args>
struct IsConstCallableImpl : std::false_type {};

template <typename Function, typename... Args>
struct IsConstCallableImpl<absl::void_t<decltype(absl::apply(
                               std::declval<const Function&>(),
                               std::declval<std::tuple<const Args&...>>()))>,
                           Function, Args...> : std::true_type {};

template <typename Function, typename... Args>
struct IsConstCallable : IsConstCallableImpl<void, Function, Args...> {};

template <typename Enable, typename Function, typename... Args>
struct ConstResultImpl {
  using type = void;
};

template <typename Function, typename... Args>
struct ConstResultImpl<
    std::enable_if_t<IsConstCallable<Function, Args...>::value>, Function,
    Args...> {
  using type =
      decltype(absl::apply(std::declval<const Function&>(),
                           std::declval<const std::tuple<Args...>&>()));
};

template <typename Function, typename... Args>
struct ConstResult : ConstResultImpl<void, Function, Args...> {};

}  // namespace invoker_internal

// `InvokerType<Function, Args...>`, usually made with
// `riegeli::Invoker(function, args...)`, packs a function together with its
// arguments. `InvokerType<Function, Args...>` is convertible to
// `Initializer<T>` when the result of `Function` is convertible to `T`.
//
// This allows the function taking `Initializer<T>` to construct the object
// in-place, avoiding constructing a temporary and moving from it.
//
// `InvokerType` complements `MakerType` by extending constructors with factory
// functions.
//
// The function and arguments are interpreted as by `std::invoke()`: the
// function can also be a member pointer, in which case the first argument is
// the target reference, reference wrapper, or pointer.
template <typename Function, typename... Args>
class InvokerType : public ConditionallyAssignable<absl::conjunction<
                        absl::negation<std::is_reference<Args>>...>::value> {
 public:
  // The result of calling `InvokerType&&` with no arguments.
  using Result = decltype(absl::apply(std::declval<Function&&>(),
                                      std::declval<std::tuple<Args...>&&>()));

  // If `true`, `const InvokerType&` can be called too.
  static constexpr bool kIsConstCallable =
      invoker_internal::IsConstCallable<Function, Args...>::value;

  // The result of calling `const InvokerType&` with no arguments,
  // or `void` placeholder if `!kIsConstCallable`.
  using ConstResult =
      typename invoker_internal::ConstResult<Function, Args...>::type;

  // Constructs `InvokerType` from `function` convertible to `Function` and
  // `args...` convertible to `Args...`.
  template <typename SrcFunction, typename... SrcArgs,
            std::enable_if_t<
                absl::conjunction<
                    absl::disjunction<
                        std::integral_constant<bool, (sizeof...(SrcArgs) > 0)>,
                        absl::negation<std::is_same<std::decay_t<SrcFunction>,
                                                    InvokerType>>>,
                    std::is_convertible<SrcFunction&&, Function>,
                    std::is_convertible<SrcArgs&&, Args>...>::value,
                int> = 0>
  /*implicit*/ InvokerType(SrcFunction&& function, SrcArgs&&... args)
      : function_(std::forward<SrcFunction>(function)),
        args_{std::forward<SrcArgs>(args)...} {}

  InvokerType(InvokerType&& that) = default;
  InvokerType& operator=(InvokerType&& that) = default;

  InvokerType(const InvokerType& that) = default;
  InvokerType& operator=(const InvokerType& that) = default;

  // Calls the function.
  Result operator()() && {
    return absl::apply(std::forward<Function>(function_), std::move(args_));
  }
  template <bool dependent_is_const_callable = kIsConstCallable,
            std::enable_if_t<dependent_is_const_callable, int> = 0>
  ConstResult operator()() const& {
    return absl::apply(function_, args_);
  }

  // Calls the function by an implicit conversion to `Result` or `ConstResult`.
  //
  // It is preferred to explicitly call `operator()` instead. This conversion
  // allows to pass `InvokerType<Function, Args...>` to another function which
  // accepts a value convertible to `Result` or `ConstResult` for construction
  // in-place, including functions like `std::make_unique<T>()`,
  // `std::vector<T>::emplace_back()`, or the constructor of `absl::optional<T>`
  // or `absl::StatusOr<T>`.
  /*implicit*/ operator Result() && { return std::move(*this)(); }
  template <typename DependentConstResult = ConstResult,
            bool dependent_is_const_callable = kIsConstCallable,
            std::enable_if_t<dependent_is_const_callable, int> = 0>
  /*implicit*/ operator DependentConstResult() const& {
    return (*this)();
  }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Function function_;
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS std::tuple<Args...> args_;
};

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if !__cpp_inline_variables
template <typename Function, typename... Args>
constexpr bool InvokerType<Function, Args...>::kIsConstCallable;
#endif

// Support CTAD.
#if __cpp_deduction_guides
template <typename Function, typename... Args>
explicit InvokerType(Function&&, Args&&...)
    -> InvokerType<std::decay_t<Function>, std::decay_t<Args>...>;
#endif

// `riegeli::Invoker(function, args...)` returns
// `InvokerType<Function, Args...>` which packs a function together with its
// arguments. `InvokerType<Function, Args...>` is convertible to
// `Initializer<T>` when the result of `Function` is convertible to `T`.
//
// This allows the function taking `Initializer<T>` to construct the object
// in-place, avoiding constructing a temporary and moving from it.
//
// `riegeli::Invoker()` complements `riegeli::Maker()` by extending constructors
// with factory functions.
//
// The function and arguments are interpreted as by `std::invoke()`: the
// function can also be a member pointer, in which case the first argument is
// the target reference, reference wrapper, or pointer.
//
// `riegeli::Invoker(function, args...)` does not generally own `function` or
// `args`, even if they involve temporaries, hence it should be used only as a
// parameter of a function or constructor, so that the temporaries outlive its
// usage. For storing a `InvokerType` in a variable or returning it from a
// function, use `riegeli::OwningInvoker(function, args...)` or construct
// `InvokerType` directly.
//
// Some arguments can be stored by value instead of by reference as an
// optimization: some of `Function&&` or `Args&&...` in the result type can be
// `Function` or `Args...`.
template <typename Function, typename... Args>
inline InvokerType<ReferenceOrCheapValueT<Function>,
                   ReferenceOrCheapValueT<Args>...>
Invoker(Function&& function ABSL_ATTRIBUTE_LIFETIME_BOUND,
        Args&&... args ABSL_ATTRIBUTE_LIFETIME_BOUND) {
  return {std::forward<Function>(function), std::forward<Args>(args)...};
}

// `riegeli::OwningInvoker()` is like `riegeli::Invoker()`, but the arguments
// are stored by value instead of by reference. This is useful for storing the
// `InvokerType` in a variable or returning it from a function.
template <typename Function, typename... Args>
inline InvokerType<std::decay_t<Function>, std::decay_t<Args>...> OwningInvoker(
    Function&& function, Args&&... args) {
  return {std::forward<Function>(function), std::forward<Args>(args)...};
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_INVOKER_H_
