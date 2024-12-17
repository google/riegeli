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
#include <new>  // IWYU pragma: keep
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/meta/type_traits.h"
#include "absl/utility/utility.h"
#include "riegeli/base/initializer_internal.h"
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
          absl::conjunction<NotSelfCopy<MakerType, SrcArgs...>,
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

  // Constructs the `std::decay_t<T>` on the heap.
  //
  // In contrast to `std::make_unique()`, this supports custom deleters.
  template <
      typename T, typename Deleter = std::default_delete<std::decay_t<T>>,
      std::enable_if_t<std::is_constructible<std::decay_t<T>, Args&&...>::value,
                       int> = 0>
  std::unique_ptr<std::decay_t<T>, Deleter> UniquePtr() && {
    void* const ptr = initializer_internal::Allocate<std::decay_t<T>>();
    std::move(*this).template ConstructAt<T>(ptr);
    return std::unique_ptr<std::decay_t<T>, Deleter>(

#if __cpp_lib_launder >= 201606
        std::launder
#endif
        (static_cast<std::decay_t<T>*>(ptr)));
  }
  template <
      typename T, typename Deleter,
      std::enable_if_t<std::is_constructible<std::decay_t<T>, Args&&...>::value,
                       int> = 0>
  std::unique_ptr<std::decay_t<T>, Deleter> UniquePtr(Deleter&& deleter) && {
    void* const ptr = initializer_internal::Allocate<std::decay_t<T>>();
    std::move(*this).template ConstructAt<T>(ptr);
    return std::unique_ptr<std::decay_t<T>, Deleter>(

#if __cpp_lib_launder >= 201606
        std::launder
#endif
        (static_cast<std::decay_t<T>*>(ptr)),
        std::forward<Deleter>(deleter));
  }
  template <typename T, typename Deleter = std::default_delete<std::decay_t<T>>,
            std::enable_if_t<
                std::is_constructible<std::decay_t<T>, const Args&...>::value,
                int> = 0>
  std::unique_ptr<std::decay_t<T>, Deleter> UniquePtr() const& {
    void* const ptr = initializer_internal::Allocate<std::decay_t<T>>();
    ConstructAt<T>(ptr);
    return std::unique_ptr<std::decay_t<T>, Deleter>(

#if __cpp_lib_launder >= 201606
        std::launder
#endif
        (static_cast<std::decay_t<T>*>(ptr)));
  }
  template <typename T, typename Deleter,
            std::enable_if_t<
                std::is_constructible<std::decay_t<T>, const Args&...>::value,
                int> = 0>
  std::unique_ptr<std::decay_t<T>, Deleter> UniquePtr(
      Deleter&& deleter) const& {
    void* const ptr = initializer_internal::Allocate<std::decay_t<T>>();
    ConstructAt<T>(ptr);
    return std::unique_ptr<std::decay_t<T>, Deleter>(

#if __cpp_lib_launder >= 201606
        std::launder
#endif
        (static_cast<std::decay_t<T>*>(ptr)),
        std::forward<Deleter>(deleter));
  }

  // Constructs the `std::decay_t<T>` at `ptr` using placement `new`.
  template <
      typename T,
      std::enable_if_t<std::is_constructible<std::decay_t<T>, Args&&...>::value,
                       int> = 0>
  void ConstructAt(void* ptr) && {
    std::move(*this).template ConstructAtImpl<T>(
        ptr, std::index_sequence_for<Args...>());
  }
  template <typename T,
            std::enable_if_t<
                std::is_constructible<std::decay_t<T>, const Args&...>::value,
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
  template <
      typename T,
      std::enable_if_t<absl::conjunction<absl::negation<std::is_reference<T>>,
                                         SupportsReset<T, Args&&...>>::value,
                       int> = 0>
  friend void RiegeliReset(T& dest, MakerType&& src) {
    absl::apply(
        [&](Args&&... args) {
          riegeli::Reset(dest, std::forward<Args>(args)...);
        },
        std::move(src.args_));
  }
  template <typename T,
            std::enable_if_t<
                absl::conjunction<absl::negation<std::is_reference<T>>,
                                  SupportsReset<T, const Args&...>>::value,
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
    new (ptr) std::decay_t<T>(std::forward<Args>(std::get<indices>(args_))...);
  }
  template <typename T, size_t... indices>
  void ConstructAtImpl(void* ptr, std::index_sequence<indices...>) const& {
    new (ptr) std::decay_t<T>(std::get<indices>(args_)...);
  }

  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS std::tuple<Args...> args_;
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
// caller to deduce `T`, e.g. using `TargetT`.
template <typename T, typename... Args>
class MakerTypeFor : public ConditionallyAssignable<absl::conjunction<
                         absl::negation<std::is_reference<Args>>...>::value> {
 public:
  // Constructs `MakerTypeFor` from `args...` convertible to `Args...`.
  template <
      typename... SrcArgs,
      std::enable_if_t<
          absl::conjunction<NotSelfCopy<MakerTypeFor, SrcArgs...>,
                            std::is_constructible<T, Args&&...>,
                            std::is_convertible<SrcArgs&&, Args>...>::value,
          int> = 0>
  /*implicit*/ MakerTypeFor(SrcArgs&&... args)
      : maker_(std::forward<SrcArgs>(args)...) {}

  MakerTypeFor(MakerTypeFor&& that) = default;
  MakerTypeFor& operator=(MakerTypeFor&& that) = default;

  MakerTypeFor(const MakerTypeFor& that) = default;
  MakerTypeFor& operator=(const MakerTypeFor& that) = default;

  // Constructs the `T`.
  template <typename DependentT = T,
            std::enable_if_t<
                std::is_constructible<DependentT, Args&&...>::value, int> = 0>
  /*implicit*/ operator T() && {
    return std::move(*this).Construct();
  }
  template <
      typename DependentT = T,
      std::enable_if_t<std::is_constructible<DependentT, const Args&...>::value,
                       int> = 0>
  /*implicit*/ operator T() const& {
    return Construct();
  }

  // Constructs the `T`.
  //
  // Usually conversion to `T` is preferred because it can avoid creating a
  // temporary if the context accepts an arbitrary type convertible to `T`.
  // An explicit `Construct()` call can force construction right away while
  // avoiding specifying the full target type.
  template <typename DependentT = T,
            std::enable_if_t<
                std::is_constructible<DependentT, Args&&...>::value, int> = 0>
  T Construct() && {
    return std::move(*this).maker().template Construct<T>();
  }
  template <
      typename DependentT = T,
      std::enable_if_t<std::is_constructible<DependentT, const Args&...>::value,
                       int> = 0>
  T Construct() const& {
    return this->maker().template Construct<T>();
  }

  // Constructs the `std::decay_t<T>` on the heap.
  //
  // In contrast to `std::make_unique()`, this supports deducing class template
  // arguments and custom deleters.
  //
  // For a non-default-constructed deleter, use `UniquePtr(deleter)`.
  template <typename Target, typename Deleter,
            std::enable_if_t<
                absl::conjunction<
                    std::is_constructible<std::decay_t<T>, Args&&...>,
                    std::is_convertible<std::decay_t<T>*, Target*>>::value,
                int> = 0>
  /*implicit*/ operator std::unique_ptr<Target, Deleter>() && {
    return std::move(*this).template UniquePtr<Deleter>();
  }
  template <typename Target, typename Deleter,
            std::enable_if_t<
                absl::conjunction<
                    std::is_constructible<std::decay_t<T>, const Args&...>,
                    std::is_convertible<std::decay_t<T>*, Target*>>::value,
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
            std::enable_if_t<std::is_constructible<std::decay_t<DependentT>,
                                                   Args&&...>::value,
                             int> = 0>
  std::unique_ptr<std::decay_t<T>, Deleter> UniquePtr() && {
    return std::move(*this).maker().template UniquePtr<T, Deleter>();
  }
  template <typename Deleter, typename DependentT = T,
            std::enable_if_t<std::is_constructible<std::decay_t<DependentT>,
                                                   Args&&...>::value,
                             int> = 0>
  std::unique_ptr<std::decay_t<T>, Deleter> UniquePtr(Deleter&& deleter) && {
    return std::move(*this).maker().template UniquePtr<T, Deleter>(
        std::forward<Deleter>(deleter));
  }
  template <typename Deleter = std::default_delete<std::decay_t<T>>,
            typename DependentT = T,
            std::enable_if_t<std::is_constructible<std::decay_t<DependentT>,
                                                   const Args&...>::value,
                             int> = 0>
  std::unique_ptr<std::decay_t<T>, Deleter> UniquePtr() const& {
    return this->maker().template UniquePtr<T, Deleter>();
  }
  template <typename Deleter, typename DependentT = T,
            std::enable_if_t<std::is_constructible<std::decay_t<DependentT>,
                                                   const Args&...>::value,
                             int> = 0>
  std::unique_ptr<std::decay_t<T>, Deleter> UniquePtr(
      Deleter&& deleter) const& {
    return this->maker().template UniquePtr<T, Deleter>(
        std::forward<Deleter>(deleter));
  }

  // Constructs the `std::decay_t<T>` at `ptr` using placement `new`.
  template <typename DependentT = T,
            std::enable_if_t<std::is_constructible<std::decay_t<DependentT>,
                                                   Args&&...>::value,
                             int> = 0>
  void ConstructAt(void* ptr) && {
    std::move(*this).maker().template ConstructAt<T>(ptr);
  }
  template <typename DependentT = T,
            std::enable_if_t<std::is_constructible<std::decay_t<DependentT>,
                                                   const Args&...>::value,
                             int> = 0>
  void ConstructAt(void* ptr) const& {
    this->maker().template ConstructAt<T>(ptr);
  }

  // Constructs the `T` in `storage` which must outlive the returned reference.
  //
  // `Reference()` instead of conversion to `T` or `Construct()` supports
  // `Initializer::Reference()`, and is compatible with immovable types before
  // C++17 which guarantees copy elision.
  //
  // If copy elision is guaranteed and the `storage` argument is omitted, the
  // result is returned by value instead of by reference, which is a more
  // efficient way to construct the temporary.
#if __cpp_guaranteed_copy_elision
  template <typename DependentT = T,
            std::enable_if_t<
                std::is_constructible<DependentT, Args&&...>::value, int> = 0>
  T Reference() && {
    return std::move(*this).maker().template Reference<T>();
  }
#endif
  template <typename DependentT = T,
            std::enable_if_t<
                std::is_constructible<DependentT, Args&&...>::value, int> = 0>
  T&& Reference(TemporaryStorage<T>&& storage ABSL_ATTRIBUTE_LIFETIME_BOUND
#if !__cpp_guaranteed_copy_elision
                = TemporaryStorage<T>()
#endif
                    ) && {
    return std::move(*this).maker().template Reference<T>(std::move(storage));
  }
#if __cpp_guaranteed_copy_elision
  template <
      typename DependentT = T,
      std::enable_if_t<std::is_constructible<DependentT, const Args&...>::value,
                       int> = 0>
  T Reference() const& {
    return this->maker().template Reference<T>();
  }
#endif
  template <
      typename DependentT = T,
      std::enable_if_t<std::is_constructible<DependentT, const Args&...>::value,
                       int> = 0>
  T&& Reference(TemporaryStorage<T>&& storage ABSL_ATTRIBUTE_LIFETIME_BOUND
#if !__cpp_guaranteed_copy_elision
                = TemporaryStorage<T>()
#endif
  ) const& {
    return this->maker().template Reference<T>(std::move(storage));
  }

  // `riegeli::Reset(dest, MakerTypeFor)` makes `dest` equivalent to the
  // constructed `T`. This avoids constructing a temporary `T` and moving from
  // it.
  template <typename DependentT = T,
            std::enable_if_t<
                absl::conjunction<absl::negation<std::is_reference<DependentT>>,
                                  SupportsReset<DependentT, Args&&...>>::value,
                int> = 0>
  friend void RiegeliReset(T& dest, MakerTypeFor&& src) {
    riegeli::Reset(dest, std::move(src).maker());
  }
  template <
      typename DependentT = T,
      std::enable_if_t<
          absl::conjunction<absl::negation<std::is_reference<DependentT>>,
                            SupportsReset<DependentT, const Args&...>>::value,
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
    std::enable_if_t<std::is_constructible<Target, Args&&...>::value>> {
  using type = Target;
};

template <typename Target, typename... Args>
struct MakerTargetImpl<
    const MakerTypeFor<Target, Args...>,
    std::enable_if_t<std::is_constructible<Target, const Args&...>::value>> {
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
