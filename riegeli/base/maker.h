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

#include <new>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/meta/type_traits.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/reset.h"
#include "riegeli/base/type_traits.h"

namespace riegeli {

namespace initializer_internal {

// `CanBindTo<T&&, Args&&...>::value` is `true` if constructing `T(args...)`
// with `args...` of type `Args&&...` can be elided, with `T&&` binding directly
// to the only element of `args...` instead.

template <typename T, typename... Args>
struct CanBindTo : std::false_type {};

template <typename T, typename Arg>
struct CanBindTo<T&, Arg&> : std::is_convertible<Arg*, T*> {};

template <typename T, typename Arg>
struct CanBindTo<T&, Arg&&> : std::false_type {};

template <typename T, typename Arg>
struct CanBindTo<const T&, Arg&&> : std::is_convertible<Arg*, const T*> {};

template <typename T, typename Arg>
struct CanBindTo<T&&, Arg&> : std::false_type {};

template <typename T, typename Arg>
struct CanBindTo<T&&, Arg&&> : std::is_convertible<Arg*, T*> {};

// Internal storage which is conditionally needed for storing the object that
// `MakerType<Args...>::Reference<T>()`,
// `MakerTypeFor<T, Args...>::Reference()`, and `Initializer<T>::Reference()`
// refer to.
//
// `ReferenceStorage<T>()` is passed as the default value of a parameter of
// `Initializer<T>::Reference()` with type `ReferenceStorage<T>&&`, so that
// it is allocated as a temporary by the caller.
//
// It can also be passed explicitly if the call to
// `MakerType<Args...>::Reference<T>()`,
// `MakerTypeFor<T, Args...>::Reference()`, or `Initializer<T>::Reference()`
// happens in a context which needs the returned reference to be valid longer
// than the full expression containing the call. This passes the responsibility
// for passing a `ReferenceStorage<T>` with a suitable lifetime to the caller of
// that context.
template <typename T, typename Enable = void>
class ReferenceStorage {
 public:
  ReferenceStorage() noexcept {}

  ReferenceStorage(const ReferenceStorage&) = delete;
  ReferenceStorage& operator=(const ReferenceStorage&) = delete;

  ~ReferenceStorage() {
    if (initialized_) value_.~T();
  }

  template <typename... Args>
  void emplace(Args&&... args) {
    RIEGELI_ASSERT(!initialized_)
        << "Failed precondition of ReferenceStorage::emplace(): "
           "already initialized";
    new (&value_) T(std::forward<Args>(args)...);
    initialized_ = true;
  }

  T&& operator*() && {
    RIEGELI_ASSERT(initialized_)
        << "Failed precondition of ReferenceStorage::operator*: "
           "not initialized";
    return std::move(value_);
  }

 private:
  union {
    std::remove_cv_t<T> value_;
  };
  bool initialized_ = false;
};

// Specialization of `ReferenceStorage<T>` for trivially destructible types.
// There is no need to track whether the object was initialized.
template <typename T>
class ReferenceStorage<
    T, std::enable_if_t<std::is_trivially_destructible<T>::value>> {
 public:
  ReferenceStorage() noexcept {}

  ReferenceStorage(const ReferenceStorage&) = delete;
  ReferenceStorage& operator=(const ReferenceStorage&) = delete;

  template <typename... Args>
  void emplace(Args&&... args) {
    new (&value_) T(std::forward<Args>(args)...);
  }

  T&& operator*() && { return std::move(value_); }

 private:
  union {
    std::remove_cv_t<T> value_;
  };
};

}  // namespace initializer_internal

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
template <typename... Args>
class MakerType : public ConditionallyAssignable<absl::conjunction<
                      absl::negation<std::is_reference<Args>>...>::value> {
 public:
  template <typename T>
  using ReferenceStorage = initializer_internal::ReferenceStorage<T>;

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

  // Constructs the `T`, or returns a reference to an already constructed object
  // if that was passed to the `MakerType`. This can avoid moving it.
  //
  // `reference_storage` must outlive usages of the returned reference.
  template <
      typename T,
      std::enable_if_t<std::is_constructible<T, Args&&...>::value, int> = 0>
  T&& Reference(ReferenceStorage<T>&& reference_storage
                    ABSL_ATTRIBUTE_LIFETIME_BOUND = ReferenceStorage<T>()) && {
    return std::move(*this).template ReferenceImpl<T>(
        std::move(reference_storage));
  }
  template <typename T,
            std::enable_if_t<std::is_constructible<T, const Args&...>::value,
                             int> = 0>
  T&& Reference(ReferenceStorage<T>&& reference_storage
                    ABSL_ATTRIBUTE_LIFETIME_BOUND =
                        ReferenceStorage<T>()) const& {
    return this->template ReferenceImpl<T>(std::move(reference_storage));
  }

  // Makes `object` equivalent to the constructed `T`. This avoids constructing
  // a temporary `T` and moving from it.
  template <typename T,
            std::enable_if_t<
                absl::conjunction<absl::negation<std::is_reference<T>>,
                                  std::is_move_assignable<T>,
                                  std::is_constructible<T, Args&&...>>::value,
                int> = 0>
  void AssignTo(T& object) && {
    absl::apply(
        [&](Args&&... args) {
          riegeli::Reset(object, std::forward<Args>(args)...);
        },
        std::move(args_));
  }
  template <
      typename T,
      std::enable_if_t<
          absl::conjunction<absl::negation<std::is_reference<T>>,
                            std::is_move_assignable<T>,
                            std::is_constructible<T, const Args&...>>::value,
          int> = 0>
  void AssignTo(T& object) const& {
    absl::apply([&](const Args&... args) { riegeli::Reset(object, args...); },
                args_);
  }

 private:
  template <
      typename T,
      std::enable_if_t<initializer_internal::CanBindTo<T&&, Args&&...>::value,
                       int> = 0>
  T&& ReferenceImpl(ABSL_ATTRIBUTE_UNUSED ReferenceStorage<T>&&
                        reference_storage = ReferenceStorage<T>()) && {
    return std::forward<T>(std::get<0>(std::move(args_)));
  }
  template <
      typename T,
      std::enable_if_t<!initializer_internal::CanBindTo<T&&, Args&&...>::value,
                       int> = 0>
  T&& ReferenceImpl(ReferenceStorage<T>&& reference_storage
                        ABSL_ATTRIBUTE_LIFETIME_BOUND =
                            ReferenceStorage<T>()) && {
    absl::apply(
        [&](Args&&... args) {
          reference_storage.emplace(std::forward<Args>(args)...);
        },
        std::move(args_));
    return *std::move(reference_storage);
  }

  template <
      typename T,
      std::enable_if_t<
          initializer_internal::CanBindTo<T&&, const Args&...>::value, int> = 0>
  T&& ReferenceImpl(ABSL_ATTRIBUTE_UNUSED ReferenceStorage<T>&&
                        reference_storage = ReferenceStorage<T>()) const& {
    return std::forward<T>(std::get<0>(args_));
  }
  template <typename T, std::enable_if_t<!initializer_internal::CanBindTo<
                                             T&&, const Args&...>::value,
                                         int> = 0>
  T&& ReferenceImpl(ReferenceStorage<T>&& reference_storage
                        ABSL_ATTRIBUTE_LIFETIME_BOUND =
                            ReferenceStorage<T>()) const& {
    absl::apply(
        [&](const Args&... args) { reference_storage.emplace(args...); },
        args_);
    return *std::move(reference_storage);
  }

  std::tuple<Args...> args_;
};

// Support CTAD.
#if __cpp_deduction_guides
template <typename... Args>
/*implicit*/ MakerType(Args&&...) -> MakerType<std::decay_t<Args>...>;
#endif

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
class MakerTypeFor : public ConditionallyAssignable<absl::conjunction<
                         absl::negation<std::is_reference<Args>>...>::value> {
 public:
  using ReferenceStorage = initializer_internal::ReferenceStorage<T>;

  // Constructs `MakerTypeFor` from `args...` convertible to `Args...`.
  template <
      typename... SrcArgs,
      std::enable_if_t<
          absl::conjunction<
              std::is_constructible<T, Args&&...>,
              absl::negation<std::is_same<std::tuple<std::decay_t<SrcArgs>...>,
                                          std::tuple<MakerTypeFor>>>,
              std::is_convertible<SrcArgs&&, Args>...>::value,
          int> = 0>
  /*implicit*/ MakerTypeFor(SrcArgs&&... args)
      : maker_(std::forward<SrcArgs>(args)...) {}

  MakerTypeFor(MakerTypeFor&& that) = default;
  MakerTypeFor& operator=(MakerTypeFor&& that) = default;

  MakerTypeFor(const MakerTypeFor& that) = default;
  MakerTypeFor& operator=(const MakerTypeFor& that) = default;

  // Constructs the `T`.
  T Construct() && { return std::move(maker_).template Construct<T>(); }
  template <
      typename DependentT = T,
      std::enable_if_t<std::is_constructible<DependentT, const Args&...>::value,
                       int> = 0>
  T Construct() const& {
    return maker_.template Construct<T>();
  }

  // Constructs the `T` by an implicit conversion to `T`.
  //
  // It is preferred to explicitly call `Construct()` instead. This conversion
  // allows to pass `MakerTypeFor<T, Args...>` to another function which accepts
  // a value convertible to `T` for construction in-place, including functions
  // like `std::vector<T>::emplace_back()` or the constructor of
  // `absl::optional<T>` or `absl::StatusOr<T>`.
  /*implicit*/ operator T() && { return std::move(*this).Construct(); }
  /*implicit*/ operator T() const& { return Construct(); }

  // Constructs the `T`, or returns a reference to an already constructed object
  // if that was passed to the `MakerTypeFor`. This can avoid moving it.
  //
  // `reference_storage` must outlive usages of the returned reference.
  T&& Reference(ReferenceStorage&& reference_storage
                    ABSL_ATTRIBUTE_LIFETIME_BOUND = ReferenceStorage()) && {
    return std::move(maker_).template Reference<T>(
        std::move(reference_storage));
  }
  template <
      typename DependentT = T,
      std::enable_if_t<std::is_constructible<DependentT, const Args&...>::value,
                       int> = 0>
  T&& Reference(ReferenceStorage&& reference_storage
                    ABSL_ATTRIBUTE_LIFETIME_BOUND = ReferenceStorage()) const& {
    return maker_.template Reference<T>(std::move(reference_storage));
  }

  // Makes `object` equivalent to the constructed `T`. This avoids constructing
  // a temporary `T` and moving from it.
  template <typename DependentT = T,
            std::enable_if_t<
                absl::conjunction<absl::negation<std::is_reference<DependentT>>,
                                  std::is_move_assignable<DependentT>>::value,
                int> = 0>
  void AssignTo(T& object) && {
    std::move(maker_).template AssignTo<T>(object);
  }
  template <typename DependentT = T,
            std::enable_if_t<
                absl::conjunction<
                    absl::negation<std::is_reference<DependentT>>,
                    std::is_move_assignable<DependentT>,
                    std::is_constructible<DependentT, const Args&...>>::value,
                int> = 0>
  void AssignTo(T& object) const& {
    return maker_.template AssignTo<T>(object);
  }

  // Returns the corresponding `MakerType` which does not specify `T`.
  //
  // This is useful for handling `MakerType` and `MakerTypeFor` generically.
  MakerType<Args...>&& maker() && { return std::move(maker_); }
  const MakerType<Args...>& maker() const& { return maker_; }

 private:
  MakerType<Args...> maker_;
};

namespace initializer_internal {

// In `MakerType()`, pass arguments by reference unless they are cheap to pass
// by value.

template <typename T, typename Enable = void>
struct ArgMode {
  using type = T&&;
};

template <typename T>
struct ArgMode<
    T, std::enable_if_t<absl::conjunction<
           std::is_trivially_copyable<T>, std::is_trivially_destructible<T>,
           std::integral_constant<bool,
                                  (sizeof(T) <= 2 * sizeof(void*))>>::value>> {
  using type = T;
};

template <typename T>
using ArgModeT = typename ArgMode<T>::type;

}  // namespace initializer_internal

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
// `riegeli::Maker(args...)` does not generally own `args`, even if they
// involve temporaries, hence it should be used only as a parameter of a
// function or constructor, so that the temporaries outlive its usage.
// For storing a `MakerType` in a variable or returning it from a function,
// use `riegeli::OwningMaker(args...)` or construct `MakerType` directly.
//
// Some arguments can be stored by value instead of by reference as an
// optimization: some of `Args&&...` in the result type can be `Args...`.
//
// The `generic` template parameter lets `riegeli::Maker<T>()` with an explicit
// template argument unambiguously call another overload of `riegeli::Maker()`.
template <int generic = 0, typename... Args>
MakerType<initializer_internal::ArgModeT<Args>...> Maker(
    Args&&... args ABSL_ATTRIBUTE_LIFETIME_BOUND) {
  return {std::forward<Args>(args)...};
}

// `riegeli::Maker<T>(args...)` returns `MakerTypeFor<T, Args&&...>` which packs
// constructor arguments for `T`. `riegeli::Maker<T>(args...)` is convertible to
// `Initializer<T>`.
//
// This allows the function taking `Initializer<T>` to construct the object
// in-place, avoiding constructing a temporary and moving from it.
//
// In contrast to `riegeli::Maker(args...)`, `riegeli::Maker<T>(args...)` allows
// the caller to deduce `T`, e.g. using `InitializerTargetT`.
//
// `riegeli::Maker<T>(args...)` does not generally own `args`, even if they
// involve temporaries, hence it should be used only as a parameter of a
// function or constructor, so that the temporaries outlive its usage.
// For storing a `MakerTypeFor` in a variable or returning it from a function,
// use `riegeli::OwningMaker<T>(args...)` or construct `MakerTypeFor` directly.
//
// Some arguments can be stored by value instead of by reference as an
// optimization: some of `Args&&...` in the result type can be `Args...`.
template <typename T, typename... Args,
          std::enable_if_t<std::is_constructible<T, Args&&...>::value, int> = 0>
MakerTypeFor<T, initializer_internal::ArgModeT<Args>...> Maker(
    Args&&... args ABSL_ATTRIBUTE_LIFETIME_BOUND) {
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
MakerTypeFor<DeduceClassTemplateArgumentsT<Template, Args...>,
             initializer_internal::ArgModeT<Args>...>
Maker(Args&&... args ABSL_ATTRIBUTE_LIFETIME_BOUND) {
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
