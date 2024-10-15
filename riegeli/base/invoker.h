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

#include <functional>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/meta/type_traits.h"
#include "absl/utility/utility.h"
#include "riegeli/base/type_traits.h"

namespace riegeli {

template <typename Function, typename... Args>
class InvokerType;

namespace invoker_internal {

#if __cpp_lib_invoke
template <typename Function, typename... Args>
std::invoke_result_t<Function, Args...> Invoke(Function&& function,
                                               Args&&... args) {
  return std::invoke(std::forward<Function>(function),
                     std::forward<Args>(args)...);
}
#else
template <typename Function, typename... Args>
decltype(absl::apply(std::declval<Function&&>(),
                     std::declval<std::tuple<Args...>&&>()))
Invoke(Function&& function, Args&&... args) {
  return absl::apply(std::forward<Function>(function),
                     std::forward_as_tuple(std::forward<Args>(args)...));
}
#endif

template <typename Function>
decltype(std::declval<Function&&>()()) Invoke(Function&& function) {
  return std::forward<Function>(function)();
}

template <typename Function, typename... Args>
class InvokerBase : public ConditionallyAssignable<absl::conjunction<
                        absl::negation<std::is_reference<Args>>...>::value> {
 public:
  // The result of calling `InvokerType&&` with no arguments.
  using Result = decltype(invoker_internal::Invoke(std::declval<Function&&>(),
                                                   std::declval<Args&&>()...));

  // Constructs `InvokerType` from `function` convertible to `Function` and
  // `args...` convertible to `Args...`.
  template <
      typename SrcFunction, typename... SrcArgs,
      std::enable_if_t<
          absl::conjunction<absl::negation<std::is_same<
                                std::tuple<std::decay_t<SrcFunction>,
                                           std::decay_t<SrcArgs>...>,
                                std::tuple<InvokerType<Function, Args...>>>>,
                            std::is_convertible<SrcFunction&&, Function>,
                            std::is_convertible<SrcArgs&&, Args>...>::value,
          int> = 0>
  /*implicit*/ InvokerBase(SrcFunction&& function, SrcArgs&&... args)
      : function_(std::forward<SrcFunction>(function)),
        args_(std::forward<SrcArgs>(args)...) {}

  InvokerBase(InvokerBase&& that) = default;
  InvokerBase& operator=(InvokerBase&& that) = default;

  InvokerBase(const InvokerBase& that) = default;
  InvokerBase& operator=(const InvokerBase& that) = default;

  // Calls the function.
  /*implicit*/ operator Result() && {
    return absl::apply(std::forward<Function>(function_), std::move(args_));
  }

 protected:
  const Function& function() const { return function_; }
  const std::tuple<Args...>& args() const { return args_; }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Function function_;
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS std::tuple<Args...> args_;
};

template <typename Enable, typename Function, typename... Args>
struct IsConstCallableImpl : std::false_type {};

template <typename Function, typename... Args>
struct IsConstCallableImpl<
    absl::void_t<decltype(invoker_internal::Invoke(
        std::declval<const Function&>(), std::declval<const Args&>()...))>,
    Function, Args...> : std::true_type {};

template <typename Function, typename... Args>
struct IsConstCallable : IsConstCallableImpl<void, Function, Args...> {};

template <bool is_const_callable, typename Function, typename... Args>
class InvokerConstBase;

template <typename Function, typename... Args>
class InvokerConstBase</*is_const_callable=*/false, Function, Args...>
    : public InvokerBase<Function, Args...> {
 public:
  using InvokerConstBase::InvokerBase::InvokerBase;

  InvokerConstBase(InvokerConstBase&& that) = default;
  InvokerConstBase& operator=(InvokerConstBase&& that) = default;

  InvokerConstBase(const InvokerConstBase& that) = default;
  InvokerConstBase& operator=(const InvokerConstBase& that) = default;
};

template <typename Function, typename... Args>
class InvokerConstBase</*is_const_callable=*/true, Function, Args...>
    : public InvokerBase<Function, Args...> {
 public:
  using InvokerConstBase::InvokerBase::InvokerBase;

  InvokerConstBase(InvokerConstBase&& that) = default;
  InvokerConstBase& operator=(InvokerConstBase&& that) = default;

  InvokerConstBase(const InvokerConstBase& that) = default;
  InvokerConstBase& operator=(const InvokerConstBase& that) = default;

  // The result of calling `const InvokerType&` with no arguments.
  using ConstResult = decltype(invoker_internal::Invoke(
      std::declval<const Function&>(), std::declval<const Args&>()...));

  // Calls the function.
  using InvokerConstBase::InvokerBase::operator typename InvokerConstBase::
      Result;
  /*implicit*/ operator ConstResult() const& {
    return absl::apply(this->function(), this->args());
  }
};

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
class InvokerType
    : public invoker_internal::InvokerConstBase<
          invoker_internal::IsConstCallable<Function, Args...>::value, Function,
          Args...> {
 public:
  using InvokerType::InvokerConstBase::InvokerConstBase;

  InvokerType(InvokerType&& that) = default;
  InvokerType& operator=(InvokerType&& that) = default;

  InvokerType(const InvokerType& that) = default;
  InvokerType& operator=(const InvokerType& that) = default;
};

// Support CTAD.
#if __cpp_deduction_guides
template <typename Function, typename... Args>
explicit InvokerType(Function&&, Args&&...)
    -> InvokerType<std::decay_t<Function>, std::decay_t<Args>...>;
#endif

// `InvokerResult<T>::type` and `InvokerResultT<T>` deduce the appropriate
// result type of a possibly const-qualified invoker or invoker reference,
// such that `T` is convertible to `InvokerResultT<T>`.
//
// This is undefined when the invoker is not callable in the given const and
// reference context.

namespace invoker_internal {

template <typename T, typename Enable = void>
struct InvokerResultImpl {
  // No `type` member when the invoker is not callable in the given const and
  // reference context.
};

template <typename T>
struct InvokerResultImpl<T&, absl::void_t<typename T::ConstResult>> {
  using type = typename T::ConstResult;
};

template <typename T>
struct InvokerResultImpl<const T&, absl::void_t<typename T::ConstResult>> {
  using type = typename T::ConstResult;
};

template <typename T>
struct InvokerResultImpl<T&&, absl::void_t<typename T::Result>> {
  using type = typename T::Result;
};

template <typename T>
struct InvokerResultImpl<const T&&, absl::void_t<typename T::ConstResult>> {
  using type = typename T::ConstResult;
};

}  // namespace invoker_internal

template <typename T>
struct InvokerResult : invoker_internal::InvokerResultImpl<T&&> {};

template <typename T>
using InvokerResultT = typename InvokerResult<T>::type;

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
// `riegeli::Invoker(function, args...)` does not own `function` or `args`, even
// if they involve temporaries, hence it should be used only as a parameter of a
// function or constructor, so that the temporaries outlive its usage. For
// storing a `InvokerType` in a variable or returning it from a function, use
// `riegeli::OwningInvoker(function, args...)` or construct `InvokerType`
// directly.
template <typename Function, typename... Args>
inline InvokerType<Function&&, Args&&...> Invoker(
    Function&& function ABSL_ATTRIBUTE_LIFETIME_BOUND,
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
