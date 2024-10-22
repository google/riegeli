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

#include <new>  // IWYU pragma: keep
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/meta/type_traits.h"
#include "absl/utility/utility.h"
#include "riegeli/base/reset.h"
#include "riegeli/base/temporary_storage.h"
#include "riegeli/base/type_traits.h"

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
class MakerType : public ConditionallyAssignable<absl::conjunction<
                      absl::negation<std::is_reference<Args>>...>::value> {
 public:
  // Constructs `MakerType` from `args...` convertible to `Args...`.
  template <
      typename... SrcArgs,
      std::enable_if_t<
          absl::conjunction<
              absl::negation<std::is_same<std::tuple<std::decay_t<SrcArgs>...>,
                                          std::tuple<MakerType>>>,
              std::is_convertible<SrcArgs&&, Args>...>::value,
          int> = 0>
  /*implicit*/ MakerType(SrcArgs&&... args)
      : args_(std::forward<SrcArgs>(args)...) {}

  MakerType(MakerType&& that) = default;
  MakerType& operator=(MakerType&& that) = default;

  MakerType(const MakerType& that) = default;
  MakerType& operator=(const MakerType& that) = default;

  // Constructs the `T`.
  template <
      typename T,
      std::enable_if_t<std::is_constructible<T, Args&&...>::value, int> = 0>
  T Construct() && {
    return absl::make_from_tuple<T>(std::move(args_));
  }
  template <typename T,
            std::enable_if_t<std::is_constructible<T, const Args&...>::value,
                             int> = 0>
  T Construct() const& {
    return absl::make_from_tuple<T>(args_);
  }

  // Constructs the `T` at `ptr` using placement `new`.
  template <
      typename T,
      std::enable_if_t<std::is_constructible<T, Args&&...>::value, int> = 0>
  void ConstructAt(void* ptr) && {
    std::move(*this).template ConstructAtImpl<T>(
        ptr, std::index_sequence_for<Args...>());
  }
  template <typename T,
            std::enable_if_t<std::is_constructible<T, const Args&...>::value,
                             int> = 0>
  void ConstructAt(void* ptr) const& {
    ConstructAtImpl<T>(ptr, std::index_sequence_for<Args...>());
  }

  // Constructs the `T` in `storage` which must outlive the returned reference.
  //
  // `Reference()` instead of `Construct()` supports `Initializer::Reference()`,
  // and is compatible with immovable types before C++17 which guarantees copy
  // elision.
  //
  // If copy elision is guaranteed and the `storage` argument is omitted, the
  // result is returned by value instead of by reference, which is a more
  // efficient way to construct the temporary.
#if __cpp_guaranteed_copy_elision
  template <
      typename T,
      std::enable_if_t<std::is_constructible<T, Args&&...>::value, int> = 0>
  T Reference() && {
    return std::move(*this).template Construct<T>();
  }
#endif
  template <
      typename T,
      std::enable_if_t<std::is_constructible<T, Args&&...>::value, int> = 0>
  T&& Reference(TemporaryStorage<T>&& storage ABSL_ATTRIBUTE_LIFETIME_BOUND
#if !__cpp_guaranteed_copy_elision
                = TemporaryStorage<T>()
#endif
                    ) && {
    return absl::apply(
        [&](Args&&... args) -> T&& {
          return std::move(storage).emplace(std::forward<Args>(args)...);
        },
        std::move(args_));
  }
#if __cpp_guaranteed_copy_elision
  template <typename T,
            std::enable_if_t<std::is_constructible<T, const Args&...>::value,
                             int> = 0>
  T Reference() const& {
    return Construct<T>();
  }
#endif
  template <typename T,
            std::enable_if_t<std::is_constructible<T, const Args&...>::value,
                             int> = 0>
  T&& Reference(TemporaryStorage<T>&& storage ABSL_ATTRIBUTE_LIFETIME_BOUND
#if !__cpp_guaranteed_copy_elision
                = TemporaryStorage<T>()
#endif
  ) const& {
    return absl::apply(
        [&](const Args&... args) -> T&& {
          return std::move(storage).emplace(args...);
        },
        args_);
  }

  // `riegeli::Reset(dest, MakerType)` makes `dest` equivalent to the
  // constructed `T`. This avoids constructing a temporary `T` and moving from
  // it.
  template <typename T,
            std::enable_if_t<
                absl::conjunction<absl::negation<std::is_reference<T>>,
                                  std::is_move_assignable<T>,
                                  std::is_constructible<T, Args&&...>>::value,
                int> = 0>
  friend void RiegeliReset(T& dest, MakerType&& src) {
    absl::apply(
        [&](Args&&... args) {
          riegeli::Reset(dest, std::forward<Args>(args)...);
        },
        std::move(src.args_));
  }
  template <
      typename T,
      std::enable_if_t<
          absl::conjunction<absl::negation<std::is_reference<T>>,
                            std::is_move_assignable<T>,
                            std::is_constructible<T, const Args&...>>::value,
          int> = 0>
  friend void RiegeliReset(T& dest, const MakerType& src) {
    absl::apply([&](const Args&... args) { riegeli::Reset(dest, args...); },
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
  template <typename T, size_t... indices>
  void ConstructAtImpl(void* ptr, std::index_sequence<indices...>) && {
    new (ptr) T(std::forward<Args>(std::get<indices>(args_))...);
  }
  template <typename T, size_t... indices>
  void ConstructAtImpl(void* ptr, std::index_sequence<indices...>) const& {
    new (ptr) T(std::get<indices>(args_)...);
  }

  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS std::tuple<Args...> args_;
};

// Support CTAD.
#if __cpp_deduction_guides
template <typename... Args>
/*implicit*/ MakerType(Args&&...) -> MakerType<std::decay_t<Args>...>;
#endif

template <typename T, typename... Args>
class MakerTypeFor;

namespace maker_internal {

template <typename T, typename... Args>
class MakerTypeForBase
    : public ConditionallyAssignable<absl::conjunction<
          absl::negation<std::is_reference<Args>>...>::value> {
 public:
  // Constructs `MakerTypeFor` from `args...` convertible to `Args...`.
  template <
      typename... SrcArgs,
      std::enable_if_t<
          absl::conjunction<std::is_constructible<T, Args&&...>,
                            absl::negation<std::is_same<
                                std::tuple<std::decay_t<SrcArgs>...>,
                                std::tuple<MakerTypeFor<T, Args...>>>>,
                            std::is_convertible<SrcArgs&&, Args>...>::value,
          int> = 0>
  /*implicit*/ MakerTypeForBase(SrcArgs&&... args)
      : maker_(std::forward<SrcArgs>(args)...) {}

  MakerTypeForBase(MakerTypeForBase&& that) = default;
  MakerTypeForBase& operator=(MakerTypeForBase&& that) = default;

  MakerTypeForBase(const MakerTypeForBase& that) = default;
  MakerTypeForBase& operator=(const MakerTypeForBase& that) = default;

  // Constructs the `T`.
  /*implicit*/ operator T() && {
    return std::move(*this).maker().template Construct<T>();
  }

  // Constructs the `T` at `ptr` using placement `new`.
  void ConstructAt(void* ptr) && {
    std::move(*this).maker().template ConstructAt<T>(ptr);
  }

  // Constructs the `T` in `storage` which must outlive the returned reference.
  //
  // `Reference()` instead of conversion to `T` supports
  // `Initializer::Reference()`, and is compatible with immovable types before
  // C++17 which guarantees copy elision.
  //
  // If copy elision is guaranteed and the `storage` argument is omitted, the
  // result is returned by value instead of by reference, which is a more
  // efficient way to construct the temporary.
#if __cpp_guaranteed_copy_elision
  T Reference() && { return std::move(*this).maker().template Reference<T>(); }
#endif
  T&& Reference(TemporaryStorage<T>&& storage ABSL_ATTRIBUTE_LIFETIME_BOUND
#if !__cpp_guaranteed_copy_elision
                = TemporaryStorage<T>()
#endif
                    ) && {
    return std::move(*this).maker().template Reference<T>(std::move(storage));
  }

  // `riegeli::Reset(dest, MakerTypeFor)` makes `dest` equivalent to the
  // constructed `T`. This avoids constructing a temporary `T` and moving from
  // it.
  template <typename DependentT = T,
            std::enable_if_t<
                absl::conjunction<absl::negation<std::is_reference<DependentT>>,
                                  std::is_move_assignable<DependentT>>::value,
                int> = 0>
  friend void RiegeliReset(T& dest, MakerTypeForBase&& src) {
    riegeli::Reset(dest, std::move(src).maker());
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

template <bool is_const_makable, typename T, typename... Args>
class MakerTypeForByConstBase;

template <typename T, typename... Args>
class MakerTypeForByConstBase</*is_const_makable=*/false, T, Args...>
    : public MakerTypeForBase<T, Args...> {
 public:
  using MakerTypeForByConstBase::MakerTypeForBase::MakerTypeForBase;

  MakerTypeForByConstBase(MakerTypeForByConstBase&& that) = default;
  MakerTypeForByConstBase& operator=(MakerTypeForByConstBase&& that) = default;

  MakerTypeForByConstBase(const MakerTypeForByConstBase& that) = default;
  MakerTypeForByConstBase& operator=(const MakerTypeForByConstBase& that) =
      default;
};

template <typename T, typename... Args>
class MakerTypeForByConstBase</*is_const_makable=*/true, T, Args...>
    : public MakerTypeForBase<T, Args...> {
 public:
  using MakerTypeForByConstBase::MakerTypeForBase::MakerTypeForBase;

  MakerTypeForByConstBase(MakerTypeForByConstBase&& that) = default;
  MakerTypeForByConstBase& operator=(MakerTypeForByConstBase&& that) = default;

  MakerTypeForByConstBase(const MakerTypeForByConstBase& that) = default;
  MakerTypeForByConstBase& operator=(const MakerTypeForByConstBase& that) =
      default;

  using MakerTypeForByConstBase::MakerTypeForBase::operator T;
  /*implicit*/ operator T() const& {
    return this->maker().template Construct<T>();
  }

  using MakerTypeForByConstBase::MakerTypeForBase::ConstructAt;
  void ConstructAt(void* ptr) const& {
    this->maker().template ConstructAt<T>(ptr);
  }

  // Constructs the `T` in `storage` which must outlive the returned reference.
  //
  // `Reference()` instead of conversion to `T` supports
  // `Initializer::Reference()`, and is compatible with immovable types before
  // C++17 which guarantees copy elision.
  //
  // If copy elision is guaranteed and the `storage` argument is omitted, the
  // result is returned by value instead of by reference, which is a more
  // efficient way to construct the temporary.
  using MakerTypeForByConstBase::MakerTypeForBase::Reference;
#if __cpp_guaranteed_copy_elision
  T Reference() const& { return this->maker().template Reference<T>(); }
#endif
  T&& Reference(TemporaryStorage<T>&& storage ABSL_ATTRIBUTE_LIFETIME_BOUND
#if !__cpp_guaranteed_copy_elision
                = TemporaryStorage<T>()
#endif
  ) const& {
    return this->maker().template Reference<T>(std::move(storage));
  }

  template <typename DependentT = T,
            std::enable_if_t<
                absl::conjunction<absl::negation<std::is_reference<DependentT>>,
                                  std::is_move_assignable<DependentT>>::value,
                int> = 0>
  friend void RiegeliReset(T& dest, const MakerTypeForByConstBase& src) {
    riegeli::Reset(dest, src.maker());
  }
};

}  // namespace maker_internal

// `MakerTypeFor<T, Args...>, usually made with `riegeli::Maker<T>(args...)`,
// packs constructor arguments for `T`. `MakerTypeFor<T, Args...>` is
// convertible to `Initializer<T>`.
//
// This allows the function taking `Initializer<T>` to construct the object
// in-place, avoiding constructing a temporary and moving from it.
//
// In contrast to `MakerType<Args...>`, `MakerTypeFor<T, Args...>` allows the
// caller to deduce `T`, e.g. using `InitializerTargetT`.
template <typename T, typename... Args>
class MakerTypeFor
    : public maker_internal::MakerTypeForByConstBase<
          std::is_constructible<T, const Args&...>::value, T, Args...> {
 public:
  using MakerTypeFor::MakerTypeForByConstBase::MakerTypeForByConstBase;

  MakerTypeFor(const MakerTypeFor& that) = default;
  MakerTypeFor& operator=(const MakerTypeFor& that) = default;

  MakerTypeFor(MakerTypeFor&& that) = default;
  MakerTypeFor& operator=(MakerTypeFor&& that) = default;
};

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
// the caller to deduce `T`, e.g. using `InitializerTargetT`.
//
// `riegeli::Maker<T>(args...)` does not own `args`, even if they involve
// temporaries, hence it should be used only as a parameter of a function or
// constructor, so that the temporaries outlive its usage. For storing a
// `MakerTypeFor` in a variable or returning it from a function, use
// `riegeli::OwningMaker<T>(args...)` or construct `MakerTypeFor` directly.
template <typename T, typename... Args,
          std::enable_if_t<std::is_constructible<T, Args&&...>::value, int> = 0>
MakerTypeFor<T, Args&&...> Maker(Args&&... args ABSL_ATTRIBUTE_LIFETIME_BOUND) {
  return {std::forward<Args>(args)...};
}

#if __cpp_deduction_guides
// `riegeli::Maker<Template>()` is like `riegeli::Maker<T>()`, but the exact
// target type is deduced using CTAD from the class template and the constructor
// arguments.
//
// Only class templates with solely type template parameters are supported.
template <template <typename...> class Template, typename... Args,
          std::enable_if_t<std::is_constructible<
                               DeduceClassTemplateArgumentsT<Template, Args...>,
                               Args&&...>::value,
                           int> = 0>
MakerTypeFor<DeduceClassTemplateArgumentsT<Template, Args...>, Args&&...> Maker(
    Args&&... args ABSL_ATTRIBUTE_LIFETIME_BOUND) {
  return {std::forward<Args>(args)...};
}
#endif

// `riegeli::OwningMaker()` is like `riegeli::Maker()`, but the arguments are
// stored by value instead of by reference. This is useful for storing the
// `MakerType` in a variable or returning it from a function.
template <int generic = 0, typename... Args>
MakerType<std::decay_t<Args>...> OwningMaker(Args&&... args) {
  return {std::forward<Args>(args)...};
}

// `riegeli::OwningMaker<T>()` is like `riegeli::Maker<T>()`, but the arguments
// are stored by value instead of by reference. This is useful for storing the
// `MakerTypeFor` in a variable or returning it from a function.
template <
    typename T, typename... Args,
    std::enable_if_t<std::is_constructible<T, std::decay_t<Args>&&...>::value,
                     int> = 0>
MakerTypeFor<T, std::decay_t<Args>...> OwningMaker(Args&&... args) {
  return {std::forward<Args>(args)...};
}

#if __cpp_deduction_guides
// `riegeli::OwningMaker<Template>()` is like `riegeli::OwningMaker<T>()`, but
// the exact target type is deduced using CTAD from the class template and the
// constructor arguments.
//
// Only class templates with solely type template parameters are supported.
template <
    template <typename...> class Template, typename... Args,
    std::enable_if_t<std::is_constructible<DeduceClassTemplateArgumentsT<
                                               Template, std::decay_t<Args>...>,
                                           std::decay_t<Args>...>::value,
                     int> = 0>
MakerTypeFor<DeduceClassTemplateArgumentsT<Template, std::decay_t<Args>&&...>,
             std::decay_t<Args>...>
OwningMaker(Args&&... args) {
  return {std::forward<Args>(args)...};
}
#endif

}  // namespace riegeli

#endif  // RIEGELI_BASE_MAKER_H_
