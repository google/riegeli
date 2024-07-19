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
          absl::conjunction<std::is_convertible<SrcFunction&&, Function>,
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

// Specializations of `InvokerBase` for 0 to 4 arguments to make it trivially
// copy constructible when possible (for `ReferenceOrCheapValue` optimization),
// and to apply `ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS`.

template <typename Function>
class InvokerBase<Function> {
 public:
  using Result = decltype(std::declval<Function&&>()());

  template <typename SrcFunction,
            std::enable_if_t<
                absl::conjunction<
                    absl::negation<std::is_same<std::decay_t<SrcFunction>,
                                                InvokerType<Function>>>,
                    std::is_convertible<SrcFunction&&, Function>>::value,
                int> = 0>
  /*implicit*/ InvokerBase(SrcFunction&& function)
      : function_(std::forward<SrcFunction>(function)) {}

  InvokerBase(InvokerBase&& that) = default;
  InvokerBase& operator=(InvokerBase&& that) = default;

  InvokerBase(const InvokerBase& that) = default;
  InvokerBase& operator=(const InvokerBase& that) = default;

  /*implicit*/ operator Result() && {
    return std::forward<Function>(function_)();
  }

 protected:
  const Function& function() const { return function_; }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Function function_;
};

template <typename Function, typename Arg0>
class InvokerBase<Function, Arg0> {
 public:
  using Result = decltype(invoker_internal::Invoke(std::declval<Function&&>(),
                                                   std::declval<Arg0&&>()));

  template <typename SrcFunction, typename SrcArg0,
            std::enable_if_t<
                absl::conjunction<std::is_convertible<SrcFunction&&, Function>,
                                  std::is_convertible<SrcArg0&&, Arg0>>::value,
                int> = 0>
  /*implicit*/ InvokerBase(SrcFunction&& function, SrcArg0&& arg0)
      : function_(std::forward<SrcFunction>(function)),
        arg0_(std::forward<SrcArg0>(arg0)) {}

  InvokerBase(InvokerBase&& that) = default;
  InvokerBase& operator=(InvokerBase&& that) = default;

  InvokerBase(const InvokerBase& that) = default;
  InvokerBase& operator=(const InvokerBase& that) = default;

  /*implicit*/ operator Result() && {
    return invoker_internal::Invoke(std::forward<Function>(function_),
                                    std::forward<Arg0>(arg0_));
  }

 protected:
  const Function& function() const { return function_; }
  const Arg0& arg0() const { return arg0_; }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Function function_;
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Arg0 arg0_;
};

template <typename Function, typename Arg0, typename Arg1>
class InvokerBase<Function, Arg0, Arg1> {
 public:
  using Result = decltype(invoker_internal::Invoke(std::declval<Function&&>(),
                                                   std::declval<Arg0&&>(),
                                                   std::declval<Arg1&&>()));

  template <typename SrcFunction, typename SrcArg0, typename SrcArg1,
            std::enable_if_t<
                absl::conjunction<std::is_convertible<SrcFunction&&, Function>,
                                  std::is_convertible<SrcArg0&&, Arg0>,
                                  std::is_convertible<SrcArg1&&, Arg1>>::value,
                int> = 0>
  /*implicit*/ InvokerBase(SrcFunction&& function, SrcArg0&& arg0,
                           SrcArg1&& arg1)
      : function_(std::forward<SrcFunction>(function)),
        arg0_(std::forward<SrcArg0>(arg0)),
        arg1_(std::forward<SrcArg1>(arg1)) {}

  InvokerBase(InvokerBase&& that) = default;
  InvokerBase& operator=(InvokerBase&& that) = default;

  InvokerBase(const InvokerBase& that) = default;
  InvokerBase& operator=(const InvokerBase& that) = default;

  /*implicit*/ operator Result() && {
    return invoker_internal::Invoke(std::forward<Function>(function_),
                                    std::forward<Arg0>(arg0_),
                                    std::forward<Arg1>(arg1_));
  }

 protected:
  const Function& function() const { return function_; }
  const Arg0& arg0() const { return arg0_; }
  const Arg1& arg1() const { return arg1_; }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Function function_;
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Arg0 arg0_;
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Arg1 arg1_;
};

template <typename Function, typename Arg0, typename Arg1, typename Arg2>
class InvokerBase<Function, Arg0, Arg1, Arg2> {
 public:
  using Result = decltype(invoker_internal::Invoke(
      std::declval<Function&&>(), std::declval<Arg0&&>(),
      std::declval<Arg1&&>(), std::declval<Arg2&&>()));

  template <typename SrcFunction, typename SrcArg0, typename SrcArg1,
            typename SrcArg2,
            std::enable_if_t<
                absl::conjunction<std::is_convertible<SrcFunction&&, Function>,
                                  std::is_convertible<SrcArg0&&, Arg0>,
                                  std::is_convertible<SrcArg1&&, Arg1>,
                                  std::is_convertible<SrcArg2&&, Arg2>>::value,
                int> = 0>
  /*implicit*/ InvokerBase(SrcFunction&& function, SrcArg0&& arg0,
                           SrcArg1&& arg1, SrcArg2&& arg2)
      : function_(std::forward<SrcFunction>(function)),
        arg0_(std::forward<SrcArg0>(arg0)),
        arg1_(std::forward<SrcArg1>(arg1)),
        arg2_(std::forward<SrcArg2>(arg2)) {}

  InvokerBase(InvokerBase&& that) = default;
  InvokerBase& operator=(InvokerBase&& that) = default;

  InvokerBase(const InvokerBase& that) = default;
  InvokerBase& operator=(const InvokerBase& that) = default;

  /*implicit*/ operator Result() && {
    return invoker_internal::Invoke(
        std::forward<Function>(function_), std::forward<Arg0>(arg0_),
        std::forward<Arg1>(arg1_), std::forward<Arg2>(arg2_));
  }

 protected:
  const Function& function() const { return function_; }
  const Arg0& arg0() const { return arg0_; }
  const Arg1& arg1() const { return arg1_; }
  const Arg2& arg2() const { return arg2_; }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Function function_;
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Arg0 arg0_;
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Arg1 arg1_;
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Arg2 arg2_;
};

template <typename Function, typename Arg0, typename Arg1, typename Arg2,
          typename Arg3>
class InvokerBase<Function, Arg0, Arg1, Arg2, Arg3> {
 public:
  using Result = decltype(invoker_internal::Invoke(
      std::declval<Function&&>(), std::declval<Arg0&&>(),
      std::declval<Arg1&&>(), std::declval<Arg2&&>(), std::declval<Arg3&&>()));

  template <typename SrcFunction, typename SrcArg0, typename SrcArg1,
            typename SrcArg2, typename SrcArg3,
            std::enable_if_t<
                absl::conjunction<std::is_convertible<SrcFunction&&, Function>,
                                  std::is_convertible<SrcArg0&&, Arg0>,
                                  std::is_convertible<SrcArg1&&, Arg1>,
                                  std::is_convertible<SrcArg2&&, Arg2>,
                                  std::is_convertible<SrcArg3&&, Arg3>>::value,
                int> = 0>
  /*implicit*/ InvokerBase(SrcFunction&& function, SrcArg0&& arg0,
                           SrcArg1&& arg1, SrcArg2&& arg2, SrcArg3&& arg3)
      : function_(std::forward<SrcFunction>(function)),
        arg0_(std::forward<SrcArg0>(arg0)),
        arg1_(std::forward<SrcArg1>(arg1)),
        arg2_(std::forward<SrcArg2>(arg2)),
        arg3_(std::forward<SrcArg3>(arg3)) {}

  InvokerBase(InvokerBase&& that) = default;
  InvokerBase& operator=(InvokerBase&& that) = default;

  InvokerBase(const InvokerBase& that) = default;
  InvokerBase& operator=(const InvokerBase& that) = default;

  /*implicit*/ operator Result() && {
    return invoker_internal::Invoke(
        std::forward<Function>(function_), std::forward<Arg0>(arg0_),
        std::forward<Arg1>(arg1_), std::forward<Arg2>(arg2_),
        std::forward<Arg3>(arg3_));
  }

 protected:
  const Function& function() const { return function_; }
  const Arg0& arg0() const { return arg0_; }
  const Arg1& arg1() const { return arg1_; }
  const Arg2& arg2() const { return arg2_; }
  const Arg3& arg3() const { return arg3_; }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Function function_;
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Arg0 arg0_;
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Arg1 arg1_;
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Arg2 arg2_;
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Arg3 arg3_;
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

// Specializations of `InvokerConstBase<true, ...>` for 0 to 4 arguments to make
// it compatible with specializations of `InvokerBase`.

template <typename Function>
class InvokerConstBase</*is_const_callable=*/true, Function>
    : public InvokerBase<Function> {
 public:
  using InvokerConstBase::InvokerBase::InvokerBase;

  InvokerConstBase(InvokerConstBase&& that) = default;
  InvokerConstBase& operator=(InvokerConstBase&& that) = default;

  InvokerConstBase(const InvokerConstBase& that) = default;
  InvokerConstBase& operator=(const InvokerConstBase& that) = default;

  using ConstResult = decltype(std::declval<const Function&>()());

  using InvokerConstBase::InvokerBase::operator typename InvokerConstBase::
      Result;
  /*implicit*/ operator ConstResult() const& { return this->function()(); }
};

template <typename Function, typename Arg0>
class InvokerConstBase</*is_const_callable=*/true, Function, Arg0>
    : public InvokerBase<Function, Arg0> {
 public:
  using InvokerConstBase::InvokerBase::InvokerBase;

  InvokerConstBase(InvokerConstBase&& that) = default;
  InvokerConstBase& operator=(InvokerConstBase&& that) = default;

  InvokerConstBase(const InvokerConstBase& that) = default;
  InvokerConstBase& operator=(const InvokerConstBase& that) = default;

  using ConstResult = decltype(invoker_internal::Invoke(
      std::declval<const Function&>(), std::declval<const Arg0&>()));

  using InvokerConstBase::InvokerBase::operator typename InvokerConstBase::
      Result;
  /*implicit*/ operator ConstResult() const& {
    return invoker_internal::Invoke(this->function(), this->arg0());
  }
};

template <typename Function, typename Arg0, typename Arg1>
class InvokerConstBase</*is_const_callable=*/true, Function, Arg0, Arg1>
    : public InvokerBase<Function, Arg0, Arg1> {
 public:
  using InvokerConstBase::InvokerBase::InvokerBase;

  InvokerConstBase(InvokerConstBase&& that) = default;
  InvokerConstBase& operator=(InvokerConstBase&& that) = default;

  InvokerConstBase(const InvokerConstBase& that) = default;
  InvokerConstBase& operator=(const InvokerConstBase& that) = default;

  using ConstResult = decltype(invoker_internal::Invoke(
      std::declval<const Function&>(), std::declval<const Arg0&>(),
      std::declval<const Arg1&>()));

  using InvokerConstBase::InvokerBase::operator typename InvokerConstBase::
      Result;
  /*implicit*/ operator ConstResult() const& {
    return invoker_internal::Invoke(this->function(), this->arg0(),
                                    this->arg1());
  }
};

template <typename Function, typename Arg0, typename Arg1, typename Arg2>
class InvokerConstBase</*is_const_callable=*/true, Function, Arg0, Arg1, Arg2>
    : public InvokerBase<Function, Arg0, Arg1, Arg2> {
 public:
  using InvokerConstBase::InvokerBase::InvokerBase;

  InvokerConstBase(InvokerConstBase&& that) = default;
  InvokerConstBase& operator=(InvokerConstBase&& that) = default;

  InvokerConstBase(const InvokerConstBase& that) = default;
  InvokerConstBase& operator=(const InvokerConstBase& that) = default;

  using ConstResult = decltype(invoker_internal::Invoke(
      std::declval<const Function&>(), std::declval<const Arg0&>(),
      std::declval<const Arg1&>, std::declval<const Arg2&>()));

  using InvokerConstBase::InvokerBase::operator typename InvokerConstBase::
      Result;
  /*implicit*/ operator ConstResult() const& {
    return invoker_internal::Invoke(this->function(), this->arg0(),
                                    this->arg1(), this->arg2());
  }
};

template <typename Function, typename Arg0, typename Arg1, typename Arg2,
          typename Arg3>
class InvokerConstBase</*is_const_callable=*/true, Function, Arg0, Arg1, Arg2,
                       Arg3>
    : public InvokerBase<Function, Arg0, Arg1, Arg2, Arg3> {
 public:
  using InvokerConstBase::InvokerBase::InvokerBase;

  InvokerConstBase(InvokerConstBase&& that) = default;
  InvokerConstBase& operator=(InvokerConstBase&& that) = default;

  InvokerConstBase(const InvokerConstBase& that) = default;
  InvokerConstBase& operator=(const InvokerConstBase& that) = default;

  using ConstResult = decltype(invoker_internal::Invoke(
      std::declval<const Function&>(), std::declval<const Arg0&>(),
      std::declval<const Arg1&>, std::declval<const Arg2&>(),
      std::declval<const Arg3&>()));

  using InvokerConstBase::InvokerBase::operator typename InvokerConstBase::
      Result;
  /*implicit*/ operator ConstResult() const& {
    return invoker_internal::Invoke(this->function(), this->arg0(),
                                    this->arg1(), this->arg2(), this->arg3());
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
