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

#ifndef RIEGELI_BASE_MAKER_H_
#define RIEGELI_BASE_MAKER_H_

#include <stddef.h>

#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "riegeli/base/reset.h"
#include "riegeli/base/temporary_storage.h"
#include "riegeli/base/type_traits.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

// `MakerType<Args...>`, usually made with `riegeli::Maker(args...)`, packs
// constructor arguments for a yet unspecified type, which will be specified by
// the caller. `MakerType<Args...>` is convertible to `Initializer<T>` for any
// `T` which can be constructed from `Args...`.
//
// This allows the function taking `Initializer<T>` to construct the object
// in-place, avoiding constructing a temporary and moving from it.
//
// In contrast to `MakerTypeFor<T, Args...>`, `MakerType<Args...>` requires the
// caller to know `T`.
//
// `InvokerType` complements `MakerType` by extending constructors with factory
// functions.
template <typename... Args>
class MakerType
    : public ConditionallyAssignable<
          std::conjunction_v<std::negation<std::is_reference<Args>>...>> {
 public:
  // Constructs `MakerType` from `args...` convertible to `Args...`.
  template <typename... SrcArgs,
            std::enable_if_t<
                std::conjunction_v<NotSameRef<MakerType, SrcArgs...>,
                                   std::is_convertible<SrcArgs&&, Args>...>,
                int> = 0>
  /*implicit*/ MakerType(SrcArgs&&... args)
      : args_(std::forward<SrcArgs>(args)...) {}

  MakerType(MakerType&& that) = default;
  MakerType& operator=(MakerType&& that) = default;

  MakerType(const MakerType& that) = default;
  MakerType& operator=(const MakerType& that) = default;

  // Constructs the `T`.
  template <typename T,
            std::enable_if_t<std::is_constructible_v<T, Args&&...>, int> = 0>
  T Construct() && {
    return std::make_from_tuple<T>(std::move(args_));
  }
  template <
      typename T,
      std::enable_if_t<std::is_constructible_v<T, const Args&...>, int> = 0>
  T Construct() const& {
    return std::make_from_tuple<T>(args_);
  }

  // Constructs the `std::decay_t<T>` on the heap.
  //
  // In contrast to `std::make_unique()`, this supports custom deleters.
  template <typename T, typename Deleter = std::default_delete<std::decay_t<T>>,
            std::enable_if_t<
                std::is_constructible_v<std::decay_t<T>, Args&&...>, int> = 0>
  std::unique_ptr<std::decay_t<T>, Deleter> UniquePtr() && {
    return std::unique_ptr<std::decay_t<T>, Deleter>(
        new std::decay_t<T>(std::move(*this).template Construct<T>()));
  }
  template <typename T, typename Deleter,
            std::enable_if_t<
                std::is_constructible_v<std::decay_t<T>, Args&&...>, int> = 0>
  std::unique_ptr<std::decay_t<T>, Deleter> UniquePtr(Deleter&& deleter) && {
    return std::unique_ptr<std::decay_t<T>, Deleter>(
        new std::decay_t<T>(std::move(*this).template Construct<T>()),
        std::forward<Deleter>(deleter));
  }
  template <
      typename T, typename Deleter = std::default_delete<std::decay_t<T>>,
      std::enable_if_t<std::is_constructible_v<std::decay_t<T>, const Args&...>,
                       int> = 0>
  std::unique_ptr<std::decay_t<T>, Deleter> UniquePtr() const& {
    return std::unique_ptr<std::decay_t<T>, Deleter>(
        new std::decay_t<T>(Construct<T>()));
  }
  template <
      typename T, typename Deleter,
      std::enable_if_t<std::is_constructible_v<std::decay_t<T>, const Args&...>,
                       int> = 0>
  std::unique_ptr<std::decay_t<T>, Deleter> UniquePtr(
      Deleter&& deleter) const& {
    return std::unique_ptr<std::decay_t<T>, Deleter>(
        new std::decay_t<T>(Construct<T>()), std::forward<Deleter>(deleter));
  }

  // Constructs the `T` in `storage` which must outlive the returned reference.
  //
  // `Reference()` instead of `Construct()` supports `Initializer::Reference()`.
  //
  // If the `storage` argument is omitted, the result is returned by value
  // instead of by reference, which is a more efficient way to construct the
  // temporary.
  template <typename T,
            std::enable_if_t<std::is_constructible_v<T, Args&&...>, int> = 0>
  T Reference() && {
    return std::move(*this).template Construct<T>();
  }
  template <typename T,
            std::enable_if_t<std::is_constructible_v<T, Args&&...>, int> = 0>
  T&& Reference(
      TemporaryStorage<T>&& storage ABSL_ATTRIBUTE_LIFETIME_BOUND) && {
    return std::apply(
        [&](Args&&... args) -> T&& {
          return std::move(storage).emplace(std::forward<Args>(args)...);
        },
        std::move(args_));
  }
  template <
      typename T,
      std::enable_if_t<std::is_constructible_v<T, const Args&...>, int> = 0>
  T Reference() const& {
    return Construct<T>();
  }
  template <
      typename T,
      std::enable_if_t<std::is_constructible_v<T, const Args&...>, int> = 0>
  T&& Reference(
      TemporaryStorage<T>&& storage ABSL_ATTRIBUTE_LIFETIME_BOUND) const& {
    return std::apply(
        [&](const Args&... args) -> T&& {
          return std::move(storage).emplace(args...);
        },
        args_);
  }

  // `riegeli::Reset(dest, MakerType)` makes `dest` equivalent to the
  // constructed `T`. This avoids constructing a temporary `T` and moving from
  // it.
  template <
      typename T,
      std::enable_if_t<std::conjunction_v<std::negation<std::is_reference<T>>,
                                          SupportsReset<T, Args&&...>>,
                       int> = 0>
  friend void RiegeliReset(T& dest, MakerType&& src) {
    std::apply(
        [&](Args&&... args) {
          riegeli::Reset(dest, std::forward<Args>(args)...);
        },
        std::move(src.args_));
  }
  template <
      typename T,
      std::enable_if_t<std::conjunction_v<std::negation<std::is_reference<T>>,
                                          SupportsReset<T, const Args&...>>,
                       int> = 0>
  friend void RiegeliReset(T& dest, const MakerType& src) {
    std::apply([&](const Args&... args) { riegeli::Reset(dest, args...); },
               src.args_);
  }

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

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS std::tuple<Args...> args_;
};

// `MakerTypeFor<T, Args...>, usually made with `riegeli::Maker<T>(args...)`,
// packs constructor arguments for `T`. `MakerTypeFor<T, Args...>` is
// convertible to `Initializer<T>`.
//
// This allows the function taking `Initializer<T>` to construct the object
// in-place, avoiding constructing a temporary and moving from it.
//
// In contrast to `MakerType<Args...>`, `MakerTypeFor<T, Args...>` allows the
// caller to deduce `T`, e.g. using `TargetT`.
template <typename T, typename... Args>
class MakerTypeFor
    : public ConditionallyAssignable<
          std::conjunction_v<std::negation<std::is_reference<Args>>...>> {
 public:
  // Constructs `MakerTypeFor` from `args...` convertible to `Args...`.
  template <typename... SrcArgs,
            std::enable_if_t<
                std::conjunction_v<NotSameRef<MakerTypeFor, SrcArgs...>,
                                   std::is_constructible<T, Args&&...>,
                                   std::is_convertible<SrcArgs&&, Args>...>,
                int> = 0>
  /*implicit*/ MakerTypeFor(SrcArgs&&... args)
      : maker_(std::forward<SrcArgs>(args)...) {}

  MakerTypeFor(MakerTypeFor&& that) = default;
  MakerTypeFor& operator=(MakerTypeFor&& that) = default;

  MakerTypeFor(const MakerTypeFor& that) = default;
  MakerTypeFor& operator=(const MakerTypeFor& that) = default;

  // Constructs the `T`.
  template <
      typename DependentT = T,
      std::enable_if_t<std::is_constructible_v<DependentT, Args&&...>, int> = 0>
  /*implicit*/ operator T() && {
    return std::move(*this).Construct();
  }
  template <typename DependentT = T,
            std::enable_if_t<
                std::is_constructible_v<DependentT, const Args&...>, int> = 0>
  /*implicit*/ operator T() const& {
    return Construct();
  }

  // Constructs the `T`.
  //
  // Usually conversion to `T` is preferred because it can avoid creating a
  // temporary if the context accepts an arbitrary type convertible to `T`.
  // An explicit `Construct()` call can force construction right away while
  // avoiding specifying the full target type.
  template <
      typename DependentT = T,
      std::enable_if_t<std::is_constructible_v<DependentT, Args&&...>, int> = 0>
  T Construct() && {
    return std::move(*this).maker().template Construct<T>();
  }
  template <typename DependentT = T,
            std::enable_if_t<
                std::is_constructible_v<DependentT, const Args&...>, int> = 0>
  T Construct() const& {
    return this->maker().template Construct<T>();
  }

  // Constructs the `std::decay_t<T>` on the heap.
  //
  // In contrast to `std::make_unique()`, this supports deducing class template
  // arguments and custom deleters.
  //
  // For a non-default-constructed deleter, use `UniquePtr(deleter)`.
  template <
      typename Target, typename Deleter,
      std::enable_if_t<
          std::conjunction_v<std::is_constructible<std::decay_t<T>, Args&&...>,
                             std::is_convertible<std::decay_t<T>*, Target*>>,
          int> = 0>
  /*implicit*/ operator std::unique_ptr<Target, Deleter>() && {
    return std::move(*this).template UniquePtr<Deleter>();
  }
  template <typename Target, typename Deleter,
            std::enable_if_t<
                std::conjunction_v<
                    std::is_constructible<std::decay_t<T>, const Args&...>,
                    std::is_convertible<std::decay_t<T>*, Target*>>,
                int> = 0>
  /*implicit*/ operator std::unique_ptr<Target, Deleter>() const& {
    return UniquePtr<Deleter>();
  }

  // Constructs the `std::decay_t<T>` on the heap.
  //
  // In contrast to `std::make_unique()`, this supports deducing class template
  // arguments and custom deleters.
  //
  // Usually conversion to `std::unique_ptr` is preferred because it leads to
  // simpler source code. An explicit `UniquePtr()` call can force construction
  // right away while avoiding writing the full target type, and it allows to
  // use a non-default-constructed deleter.
  template <typename Deleter = std::default_delete<std::decay_t<T>>,
            typename DependentT = T,
            std::enable_if_t<
                std::is_constructible_v<std::decay_t<DependentT>, Args&&...>,
                int> = 0>
  std::unique_ptr<std::decay_t<T>, Deleter> UniquePtr() && {
    return std::move(*this).maker().template UniquePtr<T, Deleter>();
  }
  template <typename Deleter, typename DependentT = T,
            std::enable_if_t<
                std::is_constructible_v<std::decay_t<DependentT>, Args&&...>,
                int> = 0>
  std::unique_ptr<std::decay_t<T>, Deleter> UniquePtr(Deleter&& deleter) && {
    return std::move(*this).maker().template UniquePtr<T, Deleter>(
        std::forward<Deleter>(deleter));
  }
  template <typename Deleter = std::default_delete<std::decay_t<T>>,
            typename DependentT = T,
            std::enable_if_t<std::is_constructible_v<std::decay_t<DependentT>,
                                                     const Args&...>,
                             int> = 0>
  std::unique_ptr<std::decay_t<T>, Deleter> UniquePtr() const& {
    return this->maker().template UniquePtr<T, Deleter>();
  }
  template <typename Deleter, typename DependentT = T,
            std::enable_if_t<std::is_constructible_v<std::decay_t<DependentT>,
                                                     const Args&...>,
                             int> = 0>
  std::unique_ptr<std::decay_t<T>, Deleter> UniquePtr(
      Deleter&& deleter) const& {
    return this->maker().template UniquePtr<T, Deleter>(
        std::forward<Deleter>(deleter));
  }

  // Constructs the `T` in `storage` which must outlive the returned reference.
  //
  // `Reference()` instead of conversion to `T` or `Construct()` supports
  // `Initializer::Reference()`.
  //
  // If the `storage` argument is omitted, the result is returned by value
  // instead of by reference, which is a more efficient way to construct the
  // temporary.
  template <
      typename DependentT = T,
      std::enable_if_t<std::is_constructible_v<DependentT, Args&&...>, int> = 0>
  T Reference() && {
    return std::move(*this).maker().template Reference<T>();
  }
  template <
      typename DependentT = T,
      std::enable_if_t<std::is_constructible_v<DependentT, Args&&...>, int> = 0>
  T&& Reference(
      TemporaryStorage<T>&& storage ABSL_ATTRIBUTE_LIFETIME_BOUND) && {
    return std::move(*this).maker().template Reference<T>(std::move(storage));
  }
  template <typename DependentT = T,
            std::enable_if_t<
                std::is_constructible_v<DependentT, const Args&...>, int> = 0>
  T Reference() const& {
    return this->maker().template Reference<T>();
  }
  template <typename DependentT = T,
            std::enable_if_t<
                std::is_constructible_v<DependentT, const Args&...>, int> = 0>
  T&& Reference(
      TemporaryStorage<T>&& storage ABSL_ATTRIBUTE_LIFETIME_BOUND) const& {
    return this->maker().template Reference<T>(std::move(storage));
  }

  // `riegeli::Reset(dest, MakerTypeFor)` makes `dest` equivalent to the
  // constructed `T`. This avoids constructing a temporary `T` and moving from
  // it.
  template <typename DependentT = T,
            std::enable_if_t<
                std::conjunction_v<std::negation<std::is_reference<DependentT>>,
                                   SupportsReset<DependentT, Args&&...>>,
                int> = 0>
  friend void RiegeliReset(T& dest, MakerTypeFor&& src) {
    riegeli::Reset(dest, std::move(src).maker());
  }
  template <typename DependentT = T,
            std::enable_if_t<
                std::conjunction_v<std::negation<std::is_reference<DependentT>>,
                                   SupportsReset<DependentT, const Args&...>>,
                int> = 0>
  friend void RiegeliReset(T& dest, const MakerTypeFor& src) {
    riegeli::Reset(dest, src.maker());
  }

  // Extracts the given argument.
  template <size_t index, std::enable_if_t<(index < sizeof...(Args)), int> = 0>
  std::tuple_element_t<index, std::tuple<Args...>>& arg() & {
    return maker().template arg<index>();
  }
  template <size_t index, std::enable_if_t<(index < sizeof...(Args)), int> = 0>
  const std::tuple_element_t<index, std::tuple<Args...>>& arg() const& {
    return maker().template arg<index>();
  }
  template <size_t index, std::enable_if_t<(index < sizeof...(Args)), int> = 0>
  std::tuple_element_t<index, std::tuple<Args...>>& arg() && {
    return std::move(*this).maker().template arg<index>();
  }
  template <size_t index, std::enable_if_t<(index < sizeof...(Args)), int> = 0>
  const std::tuple_element_t<index, std::tuple<Args...>>& arg() const&& {
    return std::move(*this).maker().template arg<index>();
  }

  // Extracts the corresponding `MakerType` which does not specify `T`.
  //
  // This is useful for handling `MakerType` and `MakerTypeFor` generically.
  MakerType<Args...>& maker() & { return maker_; }
  const MakerType<Args...>& maker() const& { return maker_; }
  MakerType<Args...>&& maker() && { return std::move(maker_); }
  const MakerType<Args...>&& maker() const&& { return std::move(maker_); }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS MakerType<Args...> maker_;
};

// `MakerTarget<T>::type` and `MakerTargetT<T>` deduce the appropriate target
// type of a possibly const-qualified `MakerTypeFor<Target, Args...>` or its
// reference, such that `T` is convertible to `MakerTargetT<T>`, and
// `T::Construct()` returns `MakerTargetT<T>`.
//
// They are undefined when the maker is not usable in the given const and
// reference context.

namespace maker_internal {

template <typename T, typename Enable = void>
struct MakerTargetImpl {
  // No `type` member when the maker is not usable in the given const and
  // reference context.
};

template <typename Target, typename... Args>
struct MakerTargetImpl<
    MakerTypeFor<Target, Args...>,
    std::enable_if_t<std::is_constructible_v<Target, Args&&...>>> {
  using type = Target;
};

template <typename Target, typename... Args>
struct MakerTargetImpl<
    const MakerTypeFor<Target, Args...>,
    std::enable_if_t<std::is_constructible_v<Target, const Args&...>>> {
  using type = Target;
};

}  // namespace maker_internal

template <typename T>
struct MakerTarget : maker_internal::MakerTargetImpl<T> {};

template <typename T>
struct MakerTarget<T&> : maker_internal::MakerTargetImpl<const T> {};

template <typename T>
struct MakerTarget<T&&> : maker_internal::MakerTargetImpl<T> {};

template <typename T>
using MakerTargetT = typename MakerTarget<T>::type;

// `riegeli::Maker(args...)` returns `MakerType<Args&&...>` which packs
// constructor arguments for a yet unspecified type, which will be specified by
// the caller. `riegeli::Maker(args...)` is convertible to `Initializer<T>` for
// any `T` which can be constructed from `Args...`.
//
// This allows the function taking `Initializer<T>` to construct the object
// in-place, avoiding constructing a temporary and moving from it.
//
// In contrast to `riegeli::Maker<T>(args...)`, `riegeli::Maker(args...)`
// requires the caller to know `T`. Prefer
// `Template(riegeli::Maker<T>(args...))` over
// `Template<T>(riegeli::Maker(args...))` if CTAD of `Template` can be used.
//
// `riegeli::Invoker()` complements `riegeli::Maker()` by extending constructors
// with factory functions.
//
// `riegeli::Maker(args...)` does not own `args`, even if they involve
// temporaries, hence it should be used only as a parameter of a function or
// constructor, so that the temporaries outlive its usage. For storing a
// `MakerType` in a variable or returning it from a function, use
// `riegeli::OwningMaker(args...)` or construct `MakerType` directly.
//
// The `generic` template parameter lets `riegeli::Maker<T>()` with an explicit
// template argument unambiguously call another overload of `riegeli::Maker()`.
template <int generic = 0, typename... Args>
MakerType<Args&&...> Maker(Args&&... args ABSL_ATTRIBUTE_LIFETIME_BOUND) {
  return {std::forward<Args>(args)...};
}

// `riegeli::Maker<T>(args...)` returns `MakerTypeFor<T, Args&&...>` which packs
// constructor arguments for `T`. `riegeli::Maker<T>(args...)` is convertible to
// `Initializer<T>`.
//
// This allows the function taking `Initializer<T>` to construct the object
// in-place, avoiding constructing a temporary and moving from it.
//
// `riegeli::Invoker()` complements `riegeli::Maker<T>()` by extending
// constructors with factory functions.
//
// In contrast to `riegeli::Maker(args...)`, `riegeli::Maker<T>(args...)` allows
// the caller to deduce `T`, e.g. using `TargetT`.
//
// `riegeli::Maker<T>(args...)` does not own `args`, even if they involve
// temporaries, hence it should be used only as a parameter of a function or
// constructor, so that the temporaries outlive its usage. For storing a
// `MakerTypeFor` in a variable or returning it from a function, use
// `riegeli::OwningMaker<T>(args...)` or construct `MakerTypeFor` directly.
template <typename T, typename... Args,
          std::enable_if_t<std::is_constructible_v<T, Args&&...>, int> = 0>
MakerTypeFor<T, Args&&...> Maker(Args&&... args ABSL_ATTRIBUTE_LIFETIME_BOUND) {
  return {std::forward<Args>(args)...};
}

// `riegeli::Maker<Template>()` is like `riegeli::Maker<T>()`, but the exact
// target type is deduced using CTAD from the class template and the constructor
// arguments.
//
// Only class templates with solely type template parameters are supported.
template <template <typename...> class Template, typename... Args,
          std::enable_if_t<
              std::is_constructible_v<
                  DeduceClassTemplateArgumentsT<Template, Args...>, Args&&...>,
              int> = 0>
MakerTypeFor<DeduceClassTemplateArgumentsT<Template, Args...>, Args&&...> Maker(
    Args&&... args ABSL_ATTRIBUTE_LIFETIME_BOUND) {
  return {std::forward<Args>(args)...};
}

// `riegeli::OwningMaker()` is like `riegeli::Maker()`, but the arguments are
// stored by value instead of by reference. This is useful for storing the
// `MakerType` in a variable or returning it from a function.
//
// If a particular argument is heavy and its lifetime is sufficient for storing
// it by reference, wrap it in `std::ref()` or `std::cref()`.
template <int generic = 0, typename... Args>
MakerType<unwrap_ref_decay_t<Args>...> OwningMaker(Args&&... args) {
  return {std::forward<Args>(args)...};
}

// `riegeli::OwningMaker<T>()` is like `riegeli::Maker<T>()`, but the arguments
// are stored by value instead of by reference. This is useful for storing the
// `MakerTypeFor` in a variable or returning it from a function.
//
// If a particular argument is heavy and its lifetime is sufficient for storing
// it by reference, wrap it in `std::ref()` or `std::cref()`.
template <
    typename T, typename... Args,
    std::enable_if_t<std::is_constructible_v<T, unwrap_ref_decay_t<Args>&&...>,
                     int> = 0>
MakerTypeFor<T, unwrap_ref_decay_t<Args>...> OwningMaker(Args&&... args) {
  return {std::forward<Args>(args)...};
}

// `riegeli::OwningMaker<Template>()` is like `riegeli::OwningMaker<T>()`, but
// the exact target type is deduced using CTAD from the class template and the
// constructor arguments.
//
// Only class templates with solely type template parameters are supported.
//
// If a particular argument is heavy and its lifetime is sufficient for storing
// it by reference, wrap it in `std::ref()` or `std::cref()`.
template <template <typename...> class Template, typename... Args,
          std::enable_if_t<std::is_constructible_v<
                               DeduceClassTemplateArgumentsT<
                                   Template, unwrap_ref_decay_t<Args>...>,
                               unwrap_ref_decay_t<Args>...>,
                           int> = 0>
MakerTypeFor<
    DeduceClassTemplateArgumentsT<Template, unwrap_ref_decay_t<Args>&&...>,
    unwrap_ref_decay_t<Args>...>
OwningMaker(Args&&... args) {
  return {std::forward<Args>(args)...};
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_MAKER_H_
