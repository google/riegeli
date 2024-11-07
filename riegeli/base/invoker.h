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

#include <stddef.h>

#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/meta/type_traits.h"
#include "riegeli/base/type_traits.h"

namespace riegeli {

template <typename Function, typename... Args>
class InvokerType;

namespace invoker_internal {

template <typename Function, typename... Args>
class InvokerBase : public ConditionallyAssignable<absl::conjunction<
                        absl::negation<std::is_reference<Args>>...>::value> {
 protected:
  template <typename DependentFunction = Function>
  using Result = invoke_result_t<DependentFunction&&, Args&&...>;
  template <typename DependentFunction = Function>
  using ConstResult = invoke_result_t<const DependentFunction&, const Args&...>;

 public:
  // Constructs `InvokerType` from `function` convertible to `Function` and
  // `args...` convertible to `Args...`.
  template <
      typename SrcFunction, typename... SrcArgs,
      std::enable_if_t<
          absl::conjunction<absl::negation<std::is_same<
                                std::tuple<std::decay_t<SrcFunction>,
                                           std::decay_t<SrcArgs>...>,
                                std::tuple<InvokerType<Function, Args...>>>>,
                            is_invocable<Function&&, Args&&...>,
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

  // Extracts the function.
  Function& function() & { return function_; }
  const Function& function() const& { return function_; }
  Function&& function() && { return std::move(function_); }
  const Function&& function() const&& { return std::move(function_); }

  // Extracts the given argument.
  template <size_t index, std::enable_if_t<(index < sizeof...(Args)), int> = 0>
  std::tuple_element_t<index, std::tuple<Args...>>& arg() & {
    return std::get<index>(args_);
  }
  template <size_t index, std::enable_if_t<(index < sizeof...(Args)), int> = 0>
  const std::tuple_element_t<index, std::tuple<Args...>>& arg() const& {
    return std::get<index>(args_);
  }
  template <size_t index, std::enable_if_t<(index < sizeof...(Args)), int> = 0>
  std::tuple_element_t<index, std::tuple<Args...>>& arg() && {
    return std::get<index>(std::move(args_));
  }
  template <size_t index, std::enable_if_t<(index < sizeof...(Args)), int> = 0>
  const std::tuple_element_t<index, std::tuple<Args...>>& arg() const&& {
    return std::get<index>(std::move(args_));
  }

 protected:
  std::tuple<Args...>&& args() && { return args_; }
  const std::tuple<Args...>& args() const& { return args_; }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Function function_;
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS std::tuple<Args...> args_;
};

template <typename Enable, typename Function, typename... Args>
class InvokerConditionalConversion : public InvokerBase<Function, Args...> {
 private:
  using Result =
      typename InvokerConditionalConversion::InvokerBase::template Result<>;
  using ConstResult = typename InvokerConditionalConversion::InvokerBase::
      template ConstResult<>;

 public:
  using InvokerConditionalConversion::InvokerBase::InvokerBase;

  InvokerConditionalConversion(InvokerConditionalConversion&& that) = default;
  InvokerConditionalConversion& operator=(InvokerConditionalConversion&& that) =
      default;

  InvokerConditionalConversion(const InvokerConditionalConversion& that) =
      default;
  InvokerConditionalConversion& operator=(
      const InvokerConditionalConversion& that) = default;

  // Invokes the function.
  /*implicit*/ operator Result() && {
    return absl::apply(std::forward<Function>(this->function()),
                       std::move(this->args()));
  }
  /*implicit*/ operator ConstResult() const& {
    return absl::apply(this->function(), this->args());
  }
};

// Disable const functionality when the const function is not invocable with the
// const arguments.
template <typename Function, typename... Args>
class InvokerConditionalConversion<
    std::enable_if_t<absl::conjunction<
        is_invocable<Function&&, Args&&...>,
        absl::negation<is_invocable<const Function&, const Args&...>>>::value>,
    Function, Args...> : public InvokerBase<Function, Args...> {
 private:
  using Result =
      typename InvokerConditionalConversion::InvokerBase::template Result<>;

 public:
  using InvokerConditionalConversion::InvokerBase::InvokerBase;

  InvokerConditionalConversion(InvokerConditionalConversion&& that) = default;
  InvokerConditionalConversion& operator=(InvokerConditionalConversion&& that) =
      default;

  InvokerConditionalConversion(const InvokerConditionalConversion& that) =
      default;
  InvokerConditionalConversion& operator=(
      const InvokerConditionalConversion& that) = default;

  // Invokes the function.
  /*implicit*/ operator Result() && {
    return absl::apply(std::forward<Function>(this->function()),
                       std::move(this->args()));
  }
};

// Disable functionality when the function is not invocable with the arguments.
template <typename Function, typename... Args>
class InvokerConditionalConversion<
    std::enable_if_t<!is_invocable<Function&&, Args&&...>::value>, Function,
    Args...> : public InvokerBase<Function, Args...> {
 public:
  using InvokerConditionalConversion::InvokerBase::InvokerBase;

  InvokerConditionalConversion(InvokerConditionalConversion&& that) = default;
  InvokerConditionalConversion& operator=(InvokerConditionalConversion&& that) =
      default;

  InvokerConditionalConversion(const InvokerConditionalConversion& that) =
      default;
  InvokerConditionalConversion& operator=(
      const InvokerConditionalConversion& that) = default;
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
    : public invoker_internal::InvokerConditionalConversion<void, Function,
                                                            Args...> {
 public:
  using InvokerType::InvokerConditionalConversion::InvokerConditionalConversion;

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

// `InvokerTargetRef<T>::type` and `InvokerTargetRefT<T>` deduce the appropriate
// target type of a possibly const-qualified `InvokerType<Function, Args...>`
// or its reference, such that `T` is convertible to `InvokerTargetRefT<T>`.
//
// They are undefined when the invoker is not usable in the given const and
// reference context.

template <typename T>
struct InvokerTargetRef;

template <typename Function, typename... Args>
struct InvokerTargetRef<InvokerType<Function, Args...>>
    : invoke_result<Function&&, Args&&...> {};

template <typename Function, typename... Args>
struct InvokerTargetRef<const InvokerType<Function, Args...>>
    : invoke_result<const Function&, const Args&...> {};

template <typename T>
struct InvokerTargetRef<T&> : InvokerTargetRef<const T> {};

template <typename T>
struct InvokerTargetRef<T&&> : InvokerTargetRef<T> {};

template <typename T>
using InvokerTargetRefT = typename InvokerTargetRef<T>::type;

// `InvokerTarget<T>::type` and `InvokerTargetT<T>` deduce the appropriate
// target type of a possibly const-qualified `InvokerType<Function, Args...>`
// or its reference, decayed to its value type, such that `T` is convertible to
// `InvokerTargetT<T>`.
//
// This makes the result independent from whether the function returns a value
// or a reference, if the result needs to be stored for later.
//
// They are undefined when the invoker is not usable in the given const and
// reference context.

namespace invoker_internal {

template <typename T, typename Enable = void>
struct InvokerTargetImpl {
  // No `type` member when the invoker is not usable in the given const and
  // reference context.
};

template <typename T>
struct InvokerTargetImpl<T, absl::void_t<InvokerTargetRefT<T>>>
    : std::decay<InvokerTargetRefT<T>> {};

}  // namespace invoker_internal

template <typename T>
struct InvokerTarget : invoker_internal::InvokerTargetImpl<T> {};

template <typename T>
using InvokerTargetT = typename InvokerTarget<T>::type;

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
template <typename Function, typename... Args,
          std::enable_if_t<is_invocable<Function&&, Args&&...>::value, int> = 0>
inline InvokerType<Function&&, Args&&...> Invoker(
    Function&& function ABSL_ATTRIBUTE_LIFETIME_BOUND,
    Args&&... args ABSL_ATTRIBUTE_LIFETIME_BOUND) {
  return {std::forward<Function>(function), std::forward<Args>(args)...};
}

// `riegeli::OwningInvoker()` is like `riegeli::Invoker()`, but the arguments
// are stored by value instead of by reference. This is useful for storing the
// `InvokerType` in a variable or returning it from a function.
template <typename Function, typename... Args,
          std::enable_if_t<is_invocable<std::decay_t<Function>,
                                        std::decay_t<Args>...>::value,
                           int> = 0>
inline InvokerType<std::decay_t<Function>, std::decay_t<Args>...> OwningInvoker(
    Function&& function, Args&&... args) {
  return {std::forward<Function>(function), std::forward<Args>(args)...};
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_INVOKER_H_
