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

#ifndef RIEGELI_BASE_INITIALIZER_H_
#define RIEGELI_BASE_INITIALIZER_H_

#include <stddef.h>

#include <functional>
#include <memory>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "riegeli/base/initializer_internal.h"
#include "riegeli/base/invoker.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/reset.h"
#include "riegeli/base/temporary_storage.h"
#include "riegeli/base/type_erased_ref.h"
#include "riegeli/base/type_traits.h"

namespace riegeli {

template <typename T>
class Initializer;

namespace initializer_internal {

// `IsInitializer` detects `Initializer` types with the given target type.

template <typename T, typename Arg>
struct IsInitializer : std::false_type {};

template <typename T>
struct IsInitializer<T, Initializer<T>> : std::true_type {};

// Part of `Initializer<T>` for `T` being a non-reference type.
template <typename T>
class InitializerBase {
 public:
  // Constructs the `T`.
  /*implicit*/ operator T() && { return std::move(*this).Construct(); }

  // Constructs the `T`.
  //
  // Usually conversion to `T` is preferred because it can avoid creating a
  // temporary if the context accepts an arbitrary type convertible to `T` and
  // it leads to simpler source code. An explicit `Construct()` call can force
  // construction right away while avoiding specifying the full target type.
  T Construct() && { return methods()->construct(context()); }

  // Constructs the `std::decay_t<T>` on the heap.
  //
  // In contrast to `std::make_unique()`, this supports custom deleters.
  //
  // For a non-default-constructed deleter, use `UniquePtr(deleter)`.
  template <typename Target, typename Deleter,
            std::enable_if_t<std::is_convertible_v<std::decay_t<T>*, Target*>,
                             int> = 0>
  /*implicit*/ operator std::unique_ptr<Target, Deleter>() && {
    return std::move(*this).template UniquePtr<Deleter>();
  }
  template <typename Target, typename Deleter,
            std::enable_if_t<std::is_convertible_v<std::decay_t<T>*, Target*>,
                             int> = 0>
  /*implicit*/ operator std::unique_ptr<Target, Deleter>() const& {
    return UniquePtr<Deleter>();
  }

  // Constructs the `std::decay_t<T>` on the heap.
  //
  // In contrast to `std::make_unique()`, this supports custom deleters.
  //
  // Usually conversion to `std::unique_ptr` is preferred because it leads to
  // simpler source code. An explicit `UniquePtr()` call can force construction
  // right away while avoiding writing the full target type, and it allows to
  // use a non-default-constructed deleter.
  template <typename Deleter = std::default_delete<std::decay_t<T>>>
  std::unique_ptr<std::decay_t<T>, Deleter> UniquePtr() && {
    return std::unique_ptr<std::decay_t<T>, Deleter>(
        new std::decay_t<T>(std::move(*this)));
  }
  template <typename Deleter>
  std::unique_ptr<std::decay_t<T>, Deleter> UniquePtr(Deleter&& deleter) && {
    return std::unique_ptr<std::decay_t<T>, Deleter>(
        new std::decay_t<T>(std::move(*this)), std::forward<Deleter>(deleter));
  }

  // Constructs the `T` in `storage` which must outlive the returned reference,
  // or returns a reference to an already constructed object if a compatible
  // object was passed to `Initializer` constructor.
  //
  // `Reference()` instead of conversion to `T` or `Construct()` can avoid
  // moving the object if the caller does not need to store the object, or if it
  // will be moved later because the target location for the object is not ready
  // yet.
  //
  // `storage` must outlive usages of the returned reference.
  T&& Reference(
      TemporaryStorage<T>&& storage ABSL_ATTRIBUTE_LIFETIME_BOUND = {}) &&
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return methods()->reference(context(), std::move(storage));
  }

 private:
  static T ConstructMethodDefault(TypeErasedRef context);

  template <typename Arg>
  static T ConstructMethodFromObject(TypeErasedRef context);

  template <typename... Args>
  static T ConstructMethodFromMaker(TypeErasedRef context);

  template <typename... Args>
  static T ConstructMethodFromConstMaker(TypeErasedRef context);

  template <typename Arg>
  static T ConstructMethodFromConvertedReference(TypeErasedRef context);

  static T&& ReferenceMethodDefault(TypeErasedRef context,
                                    TemporaryStorage<T>&& storage);

  template <typename Arg,
            std::enable_if_t<CanBindReference<T&&, Arg&&>::value, int> = 0>
  static T&& ReferenceMethodFromObject(TypeErasedRef context,
                                       TemporaryStorage<T>&& storage);
  template <typename Arg,
            std::enable_if_t<!CanBindReference<T&&, Arg&&>::value, int> = 0>
  static T&& ReferenceMethodFromObject(TypeErasedRef context,
                                       TemporaryStorage<T>&& storage);

  template <typename... Args>
  static T&& ReferenceMethodFromMaker(TypeErasedRef context,
                                      TemporaryStorage<T>&& storage);

  template <typename... Args>
  static T&& ReferenceMethodFromConstMaker(TypeErasedRef context,
                                           TemporaryStorage<T>&& storage);

  template <typename Arg>
  static T&& ReferenceMethodFromConvertedReference(
      TypeErasedRef context, TemporaryStorage<T>&& storage);

 protected:
  struct Methods {
    T (*construct)(TypeErasedRef context);
    T && (*reference)(TypeErasedRef context, TemporaryStorage<T>&& storage);
  };

  explicit InitializerBase(const Methods* methods);

  template <typename Arg>
  explicit InitializerBase(const Methods* methods, Arg&& arg);

  InitializerBase(InitializerBase&& that) = default;
  InitializerBase& operator=(InitializerBase&&) = delete;

  template <typename Dummy = void>
  static constexpr Methods kMethodsDefault = {ConstructMethodDefault,
                                              ReferenceMethodDefault};

  template <typename Arg>
  static constexpr Methods kMethodsFromObject = {
      ConstructMethodFromObject<Arg>, ReferenceMethodFromObject<Arg>};

  template <typename... Args>
  static constexpr Methods kMethodsFromMaker = {
      ConstructMethodFromMaker<Args...>, ReferenceMethodFromMaker<Args...>};

  template <typename... Args>
  static constexpr Methods kMethodsFromConstMaker = {
      ConstructMethodFromConstMaker<Args...>,
      ReferenceMethodFromConstMaker<Args...>};

  template <typename Arg>
  static constexpr Methods kMethodsFromConvertedReference = {
      ConstructMethodFromConvertedReference<Arg>,
      ReferenceMethodFromConvertedReference<Arg>};

  const Methods* methods() const { return methods_; }
  TypeErasedRef context() const { return context_; }

 private:
  const Methods* methods_;
  TypeErasedRef context_;
};

// Part of `Initializer<T>` for `T` being a move-assignable non-reference type.
template <typename T>
class InitializerAssignableBase : public InitializerBase<T> {
 public:
  // `riegeli::Reset(dest, Initializer)` makes `dest` equivalent to the
  // constructed `T`. This avoids constructing a temporary `T` and moving from
  // it.
  friend void RiegeliReset(T& dest, InitializerAssignableBase&& src) {
    src.methods()->reset(src.context(), dest);
  }

 private:
  static void ResetMethodDefault(TypeErasedRef context, T& dest);

  template <typename Arg>
  static void ResetMethodFromObject(TypeErasedRef context, T& dest);

  template <typename... Args>
  static void ResetMethodFromMaker(TypeErasedRef context, T& dest);

  template <typename... Args>
  static void ResetMethodFromConstMaker(TypeErasedRef context, T& dest);

  template <typename Arg>
  static void ResetMethodFromConvertedReference(TypeErasedRef context, T& dest);

 protected:
  struct Methods : InitializerAssignableBase::InitializerBase::Methods {
    void (*reset)(TypeErasedRef context, T& dest);
  };

  template <typename Dummy = void>
  static constexpr Methods kMethodsDefault = {
      InitializerAssignableBase::InitializerBase::template kMethodsDefault<>,
      ResetMethodDefault};

  template <typename Arg>
  static constexpr Methods kMethodsFromObject = {
      InitializerAssignableBase::InitializerBase::template kMethodsFromObject<
          Arg>,
      ResetMethodFromObject<Arg>};

  template <typename... Args>
  static constexpr Methods kMethodsFromMaker = {
      InitializerAssignableBase::InitializerBase::template kMethodsFromMaker<
          Args...>,
      ResetMethodFromMaker<Args...>};

  template <typename... Args>
  static constexpr Methods kMethodsFromConstMaker = {
      InitializerAssignableBase::InitializerBase::
          template kMethodsFromConstMaker<Args...>,
      ResetMethodFromConstMaker<Args...>};

  template <typename Arg>
  static constexpr Methods kMethodsFromConvertedReference = {
      InitializerAssignableBase::InitializerBase::
          template kMethodsFromConvertedReference<Arg>,
      ResetMethodFromConvertedReference<Arg>};

  explicit InitializerAssignableBase(const Methods* methods)
      : InitializerAssignableBase::InitializerBase(methods) {}

  template <typename Arg>
  explicit InitializerAssignableBase(const Methods* methods, Arg&& arg)
      : InitializerAssignableBase::InitializerBase(methods,
                                                   std::forward<Arg>(arg)) {}

  InitializerAssignableBase(InitializerAssignableBase&& that) = default;
  InitializerAssignableBase& operator=(InitializerAssignableBase&&) = delete;

  const Methods* methods() const {
    return static_cast<const Methods*>(
        InitializerAssignableBase::InitializerBase::methods());
  }
};

// Part of `Initializer<T>` for `T` being a reference type.
template <typename T>
class InitializerReference {
 public:
  // Constructs the `T`.
  /*implicit*/ operator T() && { return std::move(*this).Construct(); }

  // Constructs the `T`.
  //
  // Usually conversion to `T` is preferred because it leads to simpler source
  // code. An explicit `Construct()` call can force construction right away
  // while avoiding specifying the full target type.
  T Construct() && { return methods()->construct(context()); }

  // Constructs the `std::decay_t<T>` on the heap.
  //
  // In contrast to `std::make_unique()`, this supports custom deleters.
  //
  // For a non-default-constructed deleter, use `UniquePtr(deleter)`.
  template <typename Target, typename Deleter,
            std::enable_if_t<std::is_convertible_v<std::decay_t<T>*, Target*>,
                             int> = 0>
  /*implicit*/ operator std::unique_ptr<Target, Deleter>() && {
    return std::move(*this).template UniquePtr<Deleter>();
  }
  template <typename Target, typename Deleter,
            std::enable_if_t<std::is_convertible_v<std::decay_t<T>*, Target*>,
                             int> = 0>
  /*implicit*/ operator std::unique_ptr<Target, Deleter>() const& {
    return UniquePtr<Deleter>();
  }

  // Constructs the `std::decay_t<T>` on the heap.
  //
  // In contrast to `std::make_unique()`, this supports custom deleters.
  //
  // Usually conversion to `std::unique_ptr` is preferred because it leads to
  // simpler source code. An explicit `UniquePtr()` call can force construction
  // right away while avoiding writing the full target type, and it allows to
  // use a non-default-constructed deleter.
  template <typename Deleter = std::default_delete<std::decay_t<T>>,
            typename DependentT = T,
            std::enable_if_t<
                std::is_constructible_v<std::decay_t<DependentT>, DependentT>,
                int> = 0>
  std::unique_ptr<std::decay_t<T>, Deleter> UniquePtr() && {
    return std::unique_ptr<std::decay_t<T>, Deleter>(
        new std::decay_t<T>(std::move(*this)));
  }
  template <typename Deleter, typename DependentT = T,
            std::enable_if_t<
                std::is_constructible_v<std::decay_t<DependentT>, DependentT>,
                int> = 0>
  std::unique_ptr<std::decay_t<T>, Deleter> UniquePtr(Deleter&& deleter) && {
    return std::unique_ptr<std::decay_t<T>, Deleter>(
        new std::decay_t<T>(std::move(*this)), std::forward<Deleter>(deleter));
  }

  // `Reference()` can be defined in terms of conversion to `T` because
  // reference storage is never used for reference types.
  //
  // Unused `storage` parameter makes the signature compatible with the
  // non-reference specialization.
  T&& Reference() && ABSL_ATTRIBUTE_LIFETIME_BOUND {
    // `T` is a reference type here, so `T&&` is the same as `T`.
    return std::move(*this).Construct();
  }
  T&& Reference(ABSL_ATTRIBUTE_UNUSED TemporaryStorage<T>&& storage) &&
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return std::move(*this).Reference();
  }

 private:
  template <typename Arg>
  static T ConstructMethodFromObject(TypeErasedRef context);

  template <typename... Args>
  static T ConstructMethodFromMaker(TypeErasedRef context);

  template <typename... Args>
  static T ConstructMethodFromConstMaker(TypeErasedRef context);

  template <typename Arg>
  static T ConstructMethodFromConvertedReference(TypeErasedRef context);

 protected:
  struct Methods {
    T (*construct)(TypeErasedRef context);
  };

  explicit InitializerReference(const Methods* methods);

  template <typename Arg>
  explicit InitializerReference(const Methods* methods, Arg&& arg);

  InitializerReference(InitializerReference&& that) = default;
  InitializerReference& operator=(InitializerReference&&) = delete;

  template <typename Arg>
  static constexpr Methods kMethodsFromObject = {
      ConstructMethodFromObject<Arg>};

  template <typename... Args>
  static constexpr Methods kMethodsFromMaker = {
      ConstructMethodFromMaker<Args...>};

  template <typename... Args>
  static constexpr Methods kMethodsFromConstMaker = {
      ConstructMethodFromConstMaker<Args...>};

  template <typename Arg>
  static constexpr Methods kMethodsFromConvertedReference = {
      ConstructMethodFromConvertedReference<Arg>};

  const Methods* methods() const { return methods_; }
  TypeErasedRef context() const { return context_; }

 private:
  const Methods* methods_;
  TypeErasedRef context_;
};

template <typename T, typename Enable = void>
struct InitializerImpl;

template <typename T>
struct InitializerImpl<T, std::enable_if_t<!std::is_convertible_v<T&&, T>>> {
  using type = InitializerBase<T>;
};

template <typename T>
struct InitializerImpl<
    T, std::enable_if_t<std::conjunction_v<
           std::negation<std::is_reference<T>>, std::is_convertible<T&&, T>,
           std::negation<std::is_move_assignable<T>>>>> {
  using type = InitializerBase<T>;
};

template <typename T>
struct InitializerImpl<
    T, std::enable_if_t<std::conjunction_v<std::negation<std::is_reference<T>>,
                                           std::is_convertible<T&&, T>,
                                           std::is_move_assignable<T>>>> {
  using type = InitializerAssignableBase<T>;
};

template <typename T>
struct InitializerImpl<T, std::enable_if_t<std::is_reference_v<T>>> {
  using type = InitializerReference<T>;
};

}  // namespace initializer_internal

// A parameter of type `Initializer<T>` allows the caller to specify a `T` by
// passing a value convertible to `T`, or constructor arguments for `T` packed
// in `riegeli::Maker(args...)` or `riegeli::Maker<T>(args...)`.
//
// In contrast to accepting `T` directly, this allows to construct the object
// in-place, avoiding constructing a temporary and moving from it. This also
// avoids separate overloads for `const T&` and `T&&` or a template.
//
// `Initializer<T>(arg)` does not own `arg`, even if it involves temporaries,
// hence it should be used only as a parameter of a function or constructor,
// so that the temporaries outlive its usage. Instead of storing an
// `Initializer<T>` in a variable or returning it from a function, consider
// `riegeli::OwningMaker<T>(args...)`, `MakerTypeFor<T, Args...>`, or `T`.
template <typename T>
class ABSL_NULLABILITY_COMPATIBLE Initializer
    : public initializer_internal::InitializerImpl<T>::type {
 private:
  // For `ABSL_NULLABILITY_COMPATIBLE`.
  using pointer = std::conditional_t<std::is_pointer_v<T>, T, void*>;

  using Base = typename initializer_internal::InitializerImpl<T>::type;

 public:
  // Constructs `Initializer<T>` which specifies `T()`.
  template <
      typename DependentT = T,
      std::enable_if_t<std::is_default_constructible_v<DependentT>, int> = 0>
  Initializer() : Base(&Base::template kMethodsDefault<>) {}

  // Constructs `Initializer<T>` from a value convertible to `T`.
  template <
      typename Arg,
      std::enable_if_t<
          std::conjunction_v<std::negation<initializer_internal::IsInitializer<
                                 T, std::decay_t<Arg>>>,
                             std::is_convertible<Arg&&, T>>,
          int> = 0>
  /*implicit*/ Initializer(Arg&& arg ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Base(&Base::template kMethodsFromObject<Arg>, std::forward<Arg>(arg)) {}

  // Constructs `Initializer<T&>` from `std::reference_wrapper<Arg>` with a
  // compatible `Arg`.
  template <typename Arg,
            std::enable_if_t<
                std::conjunction_v<
                    std::is_reference<T>,
                    std::is_convertible<Arg*, std::remove_reference_t<T>*>>,
                int> = 0>
  /*implicit*/ Initializer(std::reference_wrapper<Arg> arg)
      : Base(&Base::template kMethodsFromObject<Arg&>, arg.get()) {}

  // Constructs `Initializer<T>` from constructor arguments for `T` packed in
  // `riegeli::Maker(args...)`.
  //
  // Prefer `Template(riegeli::Maker<T>(args...))` over
  // `Template<T>(riegeli::Maker(args...))` if CTAD for `Template` can be used.
  template <typename... Args,
            std::enable_if_t<std::is_constructible_v<T, Args&&...>, int> = 0>
  /*implicit*/ Initializer(
      MakerType<Args...>&& args ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Base(&Base::template kMethodsFromMaker<Args...>, std::move(args)) {}
  template <
      typename... Args,
      std::enable_if_t<std::is_constructible_v<T, const Args&...>, int> = 0>
  /*implicit*/ Initializer(
      const MakerType<Args...>& args ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Base(&Base::template kMethodsFromConstMaker<Args...>, args) {}

  // Constructs `Initializer<T>` from constructor arguments for `T` packed in
  // `riegeli::Maker<T>(args...)`.
  template <
      typename... Args,
      std::enable_if_t<std::conjunction_v<std::negation<std::is_convertible<
                                              MakerTypeFor<T, Args...>&&, T>>,
                                          std::is_constructible<T, Args&&...>>,
                       int> = 0>
  /*implicit*/ Initializer(
      MakerTypeFor<T, Args...>&& args ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Base(&Base::template kMethodsFromMaker<Args...>,
             std::move(args).maker()) {}
  template <typename... Args,
            std::enable_if_t<
                std::conjunction_v<std::negation<std::is_convertible<
                                       const MakerTypeFor<T, Args...>&, T>>,
                                   std::is_constructible<T, const Args&...>>,
                int> = 0>
  /*implicit*/ Initializer(
      const MakerTypeFor<T, Args...>& args ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Base(&Base::template kMethodsFromConstMaker<Args...>, args.maker()) {}

  // Constructs `Initializer<T>` from constructor arguments for `T` packed in
  // `riegeli::Maker<Target>(args...)` with a different but compatible `Target`.
  template <typename Target, typename... Args,
            std::enable_if_t<
                std::conjunction_v<std::negation<std::is_same<Target, T>>,
                                   std::negation<std::is_convertible<
                                       MakerTypeFor<Target, Args...>&&, T>>,
                                   std::is_constructible<Target, Args&&...>,
                                   IsConvertibleFromResult<T, Target&&>>,
                int> = 0>
  /*implicit*/ Initializer(
      MakerTypeFor<Target, Args...>&& args ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Base(&Base::template kMethodsFromConvertedReference<
                 MakerTypeFor<Target, Args...>>,
             std::move(args)) {}
  template <typename Target, typename... Args,
            std::enable_if_t<std::conjunction_v<
                                 std::negation<std::is_same<Target, T>>,
                                 std::negation<std::is_convertible<
                                     const MakerTypeFor<Target, Args...>&, T>>,
                                 std::is_constructible<Target, const Args&...>,
                                 IsConvertibleFromResult<T, Target&&>>,
                             int> = 0>
  /*implicit*/ Initializer(
      const MakerTypeFor<Target, Args...>& args ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Base(&Base::template kMethodsFromConvertedReference<
                 const MakerTypeFor<Target, Args...>&>,
             args) {}

  // Constructs `Initializer<T>` from a factory function for `T` packed in
  // `riegeli::Invoker(function, args...)` with a possibly different but
  // compatible function result.
  template <
      typename Function, typename... Args,
      std::enable_if_t<std::conjunction_v<
                           std::negation<std::is_convertible<
                               InvokerType<Function, Args...>&&, T>>,
                           IsConvertibleFromResult<
                               T, std::invoke_result_t<Function&&, Args&&...>>>,
                       int> = 0>
  /*implicit*/ Initializer(
      InvokerType<Function, Args...>&& invoker ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Base(&Base::template kMethodsFromObject<InvokerType<Function, Args...>>,
             std::move(invoker)) {}
  template <
      typename Function, typename... Args,
      std::enable_if_t<
          std::conjunction_v<
              std::negation<std::is_convertible<
                  const InvokerType<Function, Args...>&, T>>,
              IsConvertibleFromResult<
                  T, std::invoke_result_t<const Function&, const Args&...>>>,
          int> = 0>
  /*implicit*/ Initializer(const InvokerType<Function, Args...>& invoker
                               ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Base(&Base::template kMethodsFromObject<
                 const InvokerType<Function, Args...>&>,
             invoker) {}

  // Constructs `Initializer<T>` from `Initializer<Target>` with a different but
  // compatible `Target`.
  template <
      typename Target,
      std::enable_if_t<
          std::conjunction_v<
              std::negation<std::is_same<Target, T>>,
              std::negation<std::is_convertible<Initializer<Target>&&, T>>,
              IsConvertibleFromResult<T, Target&&>>,
          int> = 0>
  /*implicit*/ Initializer(Initializer<Target>&& initializer)
      : Base(
            &Base::template kMethodsFromConvertedReference<Initializer<Target>>,
            std::move(initializer)) {}

  Initializer(Initializer&& that) = default;
  Initializer& operator=(Initializer&&) = delete;
};

// `Target<T>::type` and `TargetT<T>` deduce the appropriate target type such
// that `T` is convertible to `Initializer<TargetT<T>>`.
//
// This allows a single template to uniformly handle a `Target` passed directly,
// as `riegeli::Maker<Target>(args...)`, as
// `riegeli::Invoker(function, args...)`, or as `Initializer<Target>`. This is
// also useful for CTAD guides to deduce a template argument as `TargetT<T>`.
//
// They are undefined in the case of `riegeli::Maker(args...)` which requires
// the target type to be specified by the caller, or when the object is not
// usable in the given const and reference context.

namespace initializer_internal {

template <typename Value, typename Reference>
struct TargetImpl {
  using type = Value;
};

template <typename T, typename Reference>
struct TargetImpl<std::reference_wrapper<T>, Reference> {
  using type = T&;
};

template <typename... Args, typename Reference>
struct TargetImpl<MakerType<Args...>, Reference> {
  // No `type` member when the target type is unspecified.
};

template <typename Target, typename... Args, typename Reference>
struct TargetImpl<MakerTypeFor<Target, Args...>, Reference>
    : MakerTarget<Reference> {};

template <typename Function, typename... Args, typename Reference>
struct TargetImpl<InvokerType<Function, Args...>, Reference>
    : InvokerTarget<Reference> {};

template <typename T, typename Reference>
struct TargetImpl<Initializer<T>, Reference> {
  using type = T;
};

};  // namespace initializer_internal

template <typename T>
struct Target : initializer_internal::TargetImpl<std::decay_t<T>, T&&> {};

template <typename T>
using TargetT = typename Target<T>::type;

// `TargetRef<T>::type` and `TargetRefT<T>` are like `TargetT<T>`, but if the
// object is already constructed, then they are the corresponding reference type
// instead of the value type. It is still true that `T` is convertible to
// `Initializer<TargetRefT<T>>`.
//
// This allows to avoid moving or copying the object if a reference to it is
// sufficient.

namespace initializer_internal {

template <typename Value, typename Reference>
struct TargetRefImpl {
  using type = Reference;
};

template <typename T, typename Reference>
struct TargetRefImpl<std::reference_wrapper<T>, Reference> {
  using type = T&;
};

template <typename... Args, typename Reference>
struct TargetRefImpl<MakerType<Args...>, Reference> {
  // No `type` member when the target type is unspecified.
};

template <typename Target, typename... Args, typename Reference>
struct TargetRefImpl<MakerTypeFor<Target, Args...>, Reference>
    : MakerTarget<Reference> {};

template <typename Function, typename... Args, typename Reference>
struct TargetRefImpl<InvokerType<Function, Args...>, Reference>
    : InvokerTargetRef<Reference> {};

template <typename T, typename Reference>
struct TargetRefImpl<Initializer<T>, Reference> {
  using type = T;
};

};  // namespace initializer_internal

template <typename T>
struct TargetRef : initializer_internal::TargetRefImpl<std::decay_t<T>, T&&> {};

template <typename T>
using TargetRefT = typename TargetRef<T>::type;

// Implementation details follow.

namespace initializer_internal {

template <typename T>
inline InitializerBase<T>::InitializerBase(const Methods* methods)
    : methods_(methods) {}

template <typename T>
template <typename Arg>
inline InitializerBase<T>::InitializerBase(const Methods* methods, Arg&& arg)
    : methods_(methods), context_(std::forward<Arg>(arg)) {}

template <typename T>
T InitializerBase<T>::ConstructMethodDefault(
    ABSL_ATTRIBUTE_UNUSED TypeErasedRef context) {
  return T();
}

template <typename T>
template <typename Arg>
T InitializerBase<T>::ConstructMethodFromObject(TypeErasedRef context) {
  return T(context.Cast<Arg>());
}

template <typename T>
template <typename... Args>
T InitializerBase<T>::ConstructMethodFromMaker(TypeErasedRef context) {
  return context.Cast<MakerType<Args...>>().template Construct<T>();
}

template <typename T>
template <typename... Args>
T InitializerBase<T>::ConstructMethodFromConstMaker(TypeErasedRef context) {
  return context.Cast<const MakerType<Args...>&>().template Construct<T>();
}

template <typename T>
template <typename Arg>
T InitializerBase<T>::ConstructMethodFromConvertedReference(
    TypeErasedRef context) {
  return T(context.Cast<Arg>().Reference());
}

template <typename T>
T&& InitializerBase<T>::ReferenceMethodDefault(
    ABSL_ATTRIBUTE_UNUSED TypeErasedRef context,
    TemporaryStorage<T>&& storage) {
  return std::move(storage).emplace();
}

template <typename T>
template <typename Arg,
          std::enable_if_t<CanBindReference<T&&, Arg&&>::value, int>>
T&& InitializerBase<T>::ReferenceMethodFromObject(
    TypeErasedRef context,
    ABSL_ATTRIBUTE_UNUSED TemporaryStorage<T>&& storage) {
  return BindReference<T&&>(context.Cast<Arg>());
}

template <typename T>
template <typename Arg,
          std::enable_if_t<!CanBindReference<T&&, Arg&&>::value, int>>
T&& InitializerBase<T>::ReferenceMethodFromObject(
    TypeErasedRef context, TemporaryStorage<T>&& storage) {
  return std::move(storage).emplace(context.Cast<Arg>());
}

template <typename T>
template <typename... Args>
T&& InitializerBase<T>::ReferenceMethodFromMaker(
    TypeErasedRef context, TemporaryStorage<T>&& storage) {
  return context.Cast<MakerType<Args...>>().template Reference<T>(
      std::move(storage));
}

template <typename T>
template <typename... Args>
T&& InitializerBase<T>::ReferenceMethodFromConstMaker(
    TypeErasedRef context, TemporaryStorage<T>&& storage) {
  return context.Cast<const MakerType<Args...>&>().template Reference<T>(
      std::move(storage));
}

template <typename T>
template <typename Arg>
T&& InitializerBase<T>::ReferenceMethodFromConvertedReference(
    TypeErasedRef context, TemporaryStorage<T>&& storage) {
  return std::move(storage).emplace(context.Cast<Arg>().Reference());
}

template <typename T>
void InitializerAssignableBase<T>::ResetMethodDefault(
    ABSL_ATTRIBUTE_UNUSED TypeErasedRef context, T& dest) {
  riegeli::Reset(dest);
}

template <typename T>
template <typename Arg>
void InitializerAssignableBase<T>::ResetMethodFromObject(TypeErasedRef context,
                                                         T& dest) {
  riegeli::Reset(dest, context.Cast<Arg>());
}

template <typename T>
template <typename... Args>
void InitializerAssignableBase<T>::ResetMethodFromMaker(TypeErasedRef context,
                                                        T& dest) {
  riegeli::Reset(dest, context.Cast<MakerType<Args...>>());
}

template <typename T>
template <typename... Args>
void InitializerAssignableBase<T>::ResetMethodFromConstMaker(
    TypeErasedRef context, T& dest) {
  riegeli::Reset(dest, context.Cast<const MakerType<Args...>&>());
}

template <typename T>
template <typename Arg>
void InitializerAssignableBase<T>::ResetMethodFromConvertedReference(
    TypeErasedRef context, T& dest) {
  riegeli::Reset(dest, context.Cast<Arg>().Reference());
}

template <typename T>
inline InitializerReference<T>::InitializerReference(const Methods* methods)
    : methods_(methods) {}

template <typename T>
template <typename Arg>
inline InitializerReference<T>::InitializerReference(const Methods* methods,
                                                     Arg&& arg)
    : methods_(methods), context_(std::forward<Arg>(arg)) {}

template <typename T>
template <typename Arg>
T InitializerReference<T>::ConstructMethodFromObject(TypeErasedRef context) {
  return T(context.Cast<Arg>());
}

template <typename T>
template <typename... Args>
T InitializerReference<T>::ConstructMethodFromMaker(TypeErasedRef context) {
  return context.Cast<MakerType<Args...>>().template Construct<T>();
}

template <typename T>
template <typename... Args>
T InitializerReference<T>::ConstructMethodFromConstMaker(
    TypeErasedRef context) {
  return context.Cast<const MakerType<Args...>&>().template Construct<T>();
}

template <typename T>
template <typename Arg>
T InitializerReference<T>::ConstructMethodFromConvertedReference(
    TypeErasedRef context) {
  return T(context.Cast<Arg>().Reference());
}

}  // namespace initializer_internal

}  // namespace riegeli

#endif  // RIEGELI_BASE_INITIALIZER_H_
