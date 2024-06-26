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

#include <type_traits>

#include "absl/base/attributes.h"
#include "absl/base/casts.h"
#include "absl/meta/type_traits.h"
#include "riegeli/base/initializer_internal.h"
#include "riegeli/base/invoker.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/reset.h"
#include "riegeli/base/temporary_storage.h"
#include "riegeli/base/type_traits.h"

namespace riegeli {

template <typename T, bool allow_explicit, typename Enable>
class Initializer;

namespace initializer_internal {

// `IsCompatibleArg<false, T, Arg>::value` is `true` if `T` is implicitly
// constructible from `Arg`, i.e. if `Arg` is convertible to `T`.
//
// `IsCompatibleArg<true, T, Arg>::value` is `true` if `T` is implicitly
// or explicitly constructible from `Arg`.

template <bool allow_explicit, typename T, typename Arg>
struct IsCompatibleArg;

template <typename T, typename Arg>
struct IsCompatibleArg<false, T, Arg> : std::is_convertible<Arg, T> {};

template <typename T, typename Arg>
struct IsCompatibleArg<true, T, Arg> : std::is_constructible<T, Arg> {};

// `IsCompatibleResult<allow_explicit, T, Result>` is like
// `IsCompatibleArg<allow_explicit, T, Result>`, except that `Result`
// represents the result of a function. Since C++17 which guarantees copy
// elision, `T` and `Result` can also be the same immovable type, possibly with
// different qualifiers.

template <bool allow_explicit, typename T, typename Arg>
struct IsCompatibleResult;

template <typename T, typename Result>
struct IsCompatibleResult<false, T, Result>
    : IsConvertibleFromResult<T, Result> {};

template <typename T, typename Result>
struct IsCompatibleResult<true, T, Result>
    : IsConstructibleFromResult<T, Result> {};

// `IsInitializer` detects `Initializer` type with the given type parameter and
// any `allow_explicit`.

template <typename T, typename U>
struct IsInitializer : std::false_type {};

template <typename T, bool allow_explicit>
struct IsInitializer<T, Initializer<T, allow_explicit, void>> : std::true_type {
};

// Part of `Initializer<T>` common to all specializations.
template <typename T>
class InitializerBase {
 public:
  // Constructs the `T`.
  T Construct() && { return methods()->construct(context()); }

  // Constructs the `T` by an implicit conversion to `T`.
  //
  // It is preferred to explicitly call `Construct()` instead. This conversion
  // allows to pass `Initializer<T>` to another function which accepts a value
  // convertible to `T` for construction in-place, including functions like
  // `std::make_unique<T>()`, `std::vector<T>::emplace_back()`, or the
  // constructor of `absl::optional<T>` or `absl::StatusOr<T>`.
  /*implicit*/ operator T() && { return std::move(*this).Construct(); }

 private:
  static T ConstructMethodDefault(void* context);

  template <typename Arg>
  static T ConstructMethodFromObject(void* context);

  template <typename... Args>
  static T ConstructMethodFromMaker(void* context);

  template <typename... Args>
  static T ConstructMethodFromConstMaker(void* context);

  template <typename Function, typename... Args>
  static T ConstructMethodFromInvoker(void* context);

  template <typename Function, typename... Args>
  static T ConstructMethodFromConstInvoker(void* context);

 protected:
  struct Methods {
    T (*construct)(void* context);
  };

  explicit InitializerBase(const Methods* methods);

  template <typename Arg>
  explicit InitializerBase(const Methods* methods, Arg&& arg);

  InitializerBase(InitializerBase&& that) = default;
  InitializerBase& operator=(InitializerBase&&) = delete;

  template <typename Dummy = void>
  static constexpr Methods kMethodsDefault = {ConstructMethodDefault};

  template <typename Arg>
  static constexpr Methods kMethodsFromObject = {
      ConstructMethodFromObject<Arg>};

  template <typename... Args>
  static constexpr Methods kMethodsFromMaker = {
      ConstructMethodFromMaker<Args...>};

  template <typename... Args>
  static constexpr Methods kMethodsFromConstMaker = {
      ConstructMethodFromConstMaker<Args...>};

  template <typename Function, typename... Args>
  static constexpr Methods kMethodsFromInvoker = {
      ConstructMethodFromInvoker<Function, Args...>};

  template <typename Function, typename... Args>
  static constexpr Methods kMethodsFromConstInvoker = {
      ConstructMethodFromConstInvoker<Function, Args...>};

  const Methods* methods() const { return methods_; }
  void* context() const { return context_; }

 private:
  const Methods* methods_;
  void* context_ = nullptr;
};

// Part of `Initializer<T>` for `T` being a non-reference type.
template <typename T>
class InitializerValueBase : public InitializerBase<T> {
 public:
  // Constructs the `T`, or returns a reference to an already constructed object
  // if that was passed to the `Initializer`.
  //
  // `Reference()` instead of `Construct()` can avoid moving the object if the
  // caller does not need to store the object, or if it will be moved later
  // because the target location for the object is not ready yet.
  //
  // `storage` must outlive usages of the returned reference.
  T&& Reference(TemporaryStorage<T>&& storage ABSL_ATTRIBUTE_LIFETIME_BOUND =
                    TemporaryStorage<T>()) && {
    return methods()->reference(this->context(), std::move(storage));
  }

  // Constructs the `T`, or returns a const reference to an already constructed
  // object if that was passed to the `Initializer`.
  //
  // `ConstReference()` can avoid moving the object in more cases than
  // `Reference()` if the caller does not need to store the object.
  //
  // `storage` must outlive usages of the returned reference.
  const T& ConstReference(TemporaryStorage<T>&& storage
                              ABSL_ATTRIBUTE_LIFETIME_BOUND =
                                  TemporaryStorage<T>()) && {
    return methods()->const_reference(this->context(), std::move(storage));
  }

 private:
  static T&& ReferenceMethodDefault(void* context,
                                    TemporaryStorage<T>&& storage);

  template <typename Arg,
            std::enable_if_t<CanBindTo<T&&, Arg&&>::value, int> = 0>
  static T&& ReferenceMethodFromObject(void* context,
                                       TemporaryStorage<T>&& storage);
  template <typename Arg,
            std::enable_if_t<!CanBindTo<T&&, Arg&&>::value, int> = 0>
  static T&& ReferenceMethodFromObject(void* context,
                                       TemporaryStorage<T>&& storage);

  template <typename... Args>
  static T&& ReferenceMethodFromMaker(void* context,
                                      TemporaryStorage<T>&& storage);

  template <typename... Args>
  static T&& ReferenceMethodFromConstMaker(void* context,
                                           TemporaryStorage<T>&& storage);

  template <typename Function, typename... Args>
  static T&& ReferenceMethodFromInvoker(void* context,
                                        TemporaryStorage<T>&& storage);

  template <typename Function, typename... Args>
  static T&& ReferenceMethodFromConstInvoker(void* context,
                                             TemporaryStorage<T>&& storage);

  static const T& ConstReferenceMethodDefault(void* context,
                                              TemporaryStorage<T>&& storage);

  template <typename Arg,
            std::enable_if_t<CanBindTo<const T&, Arg&&>::value, int> = 0>
  static const T& ConstReferenceMethodFromObject(void* context,
                                                 TemporaryStorage<T>&& storage);
  template <typename Arg,
            std::enable_if_t<!CanBindTo<const T&, Arg&&>::value, int> = 0>
  static const T& ConstReferenceMethodFromObject(void* context,
                                                 TemporaryStorage<T>&& storage);

  template <typename... Args>
  static const T& ConstReferenceMethodFromMaker(void* context,
                                                TemporaryStorage<T>&& storage);

  template <typename... Args>
  static const T& ConstReferenceMethodFromConstMaker(
      void* context, TemporaryStorage<T>&& storage);

  template <typename Function, typename... Args>
  static const T& ConstReferenceMethodFromInvoker(
      void* context, TemporaryStorage<T>&& storage);

  template <typename Function, typename... Args>
  static const T& ConstReferenceMethodFromConstInvoker(
      void* context, TemporaryStorage<T>&& storage);

 protected:
  struct Methods : InitializerValueBase::InitializerBase::Methods {
    T && (*reference)(void* context, TemporaryStorage<T>&& storage);
    const T& (*const_reference)(void* context, TemporaryStorage<T>&& storage);
  };

#if __cpp_aggregate_bases
  template <typename Dummy = void>
  static constexpr Methods kMethodsDefault = {
      InitializerValueBase::InitializerBase::template kMethodsDefault<>,
      ReferenceMethodDefault, ConstReferenceMethodDefault};

  template <typename Arg>
  static constexpr Methods kMethodsFromObject = {
      InitializerValueBase::InitializerBase::template kMethodsFromObject<Arg>,
      ReferenceMethodFromObject<Arg>, ConstReferenceMethodFromObject<Arg>};

  template <typename... Args>
  static constexpr Methods kMethodsFromMaker = {
      InitializerValueBase::InitializerBase::template kMethodsFromMaker<
          Args...>,
      ReferenceMethodFromMaker<Args...>,
      ConstReferenceMethodFromMaker<Args...>};

  template <typename... Args>
  static constexpr Methods kMethodsFromConstMaker = {
      InitializerValueBase::InitializerBase::template kMethodsFromConstMaker<
          Args...>,
      ReferenceMethodFromConstMaker<Args...>,
      ConstReferenceMethodFromConstMaker<Args...>};

  template <typename Function, typename... Args>
  static constexpr Methods kMethodsFromInvoker = {
      InitializerValueBase::InitializerBase::template kMethodsFromInvoker<
          Function, Args...>,
      ReferenceMethodFromInvoker<Function, Args...>,
      ConstReferenceMethodFromInvoker<Function, Args...>};

  template <typename Function, typename... Args>
  static constexpr Methods kMethodsFromConstInvoker = {
      InitializerValueBase::InitializerBase::template kMethodsFromConstInvoker<
          Function, Args...>,
      ReferenceMethodFromConstInvoker<Function, Args...>,
      ConstReferenceMethodFromConstInvoker<Function, Args...>};
#else
  static constexpr Methods MakeMethodsDefault() {
    Methods methods;
    static_cast<typename InitializerValueBase::InitializerBase::Methods&>(
        methods) =
        InitializerValueBase::InitializerBase::template kMethodsDefault<>;
    methods.reference = ReferenceMethodDefault;
    methods.const_reference = ConstReferenceMethodDefault;
    return methods;
  }
  template <typename Dummy = void>
  static constexpr Methods kMethodsDefault = MakeMethodsDefault();

  template <typename Arg>
  static constexpr Methods MakeMethodsFromObject() {
    Methods methods;
    static_cast<typename InitializerValueBase::InitializerBase::Methods&>(
        methods) =
        InitializerValueBase::InitializerBase::template kMethodsFromObject<Arg>;
    methods.reference = ReferenceMethodFromObject<Arg>;
    methods.const_reference = ConstReferenceMethodFromObject<Arg>;
    return methods;
  }
  template <typename Arg>
  static constexpr Methods kMethodsFromObject = MakeMethodsFromObject<Arg>();

  template <typename... Args>
  static constexpr Methods MakeMethodsFromMaker() {
    Methods methods;
    static_cast<typename InitializerValueBase::InitializerBase::Methods&>(
        methods) =
        InitializerValueBase::InitializerBase::template kMethodsFromMaker<
            Args...>;
    methods.reference = ReferenceMethodFromMaker<Args...>;
    methods.const_reference = ConstReferenceMethodFromMaker<Args...>;
    return methods;
  }
  template <typename... Args>
  static constexpr Methods kMethodsFromMaker = MakeMethodsFromMaker<Args...>();

  template <typename... Args>
  static constexpr Methods MakeMethodsFromConstMaker() {
    Methods methods;
    static_cast<typename InitializerValueBase::InitializerBase::Methods&>(
        methods) =
        InitializerValueBase::InitializerBase::template kMethodsFromConstMaker<
            Args...>;
    methods.reference = ReferenceMethodFromConstMaker<Args...>;
    methods.const_reference = ConstReferenceMethodFromConstMaker<Args...>;
    return methods;
  }
  template <typename... Args>
  static constexpr Methods kMethodsFromConstMaker =
      MakeMethodsFromConstMaker<Args...>();

  template <typename Function, typename... Args>
  static constexpr Methods MakeMethodsFromInvoker() {
    Methods methods;
    static_cast<typename InitializerValueBase::InitializerBase::Methods&>(
        methods) =
        InitializerValueBase::InitializerBase::template kMethodsFromInvoker<
            Function, Args...>;
    methods.reference = ReferenceMethodFromInvoker<Function, Args...>;
    methods.const_reference =
        ConstReferenceMethodFromInvoker<Function, Args...>;
    return methods;
  }
  template <typename Function, typename... Args>
  static constexpr Methods kMethodsFromInvoker =
      MakeMethodsFromInvoker<Function, Args...>();

  template <typename Function, typename... Args>
  static constexpr Methods MakeMethodsFromConstInvoker() {
    Methods methods;
    static_cast<typename InitializerValueBase::InitializerBase::Methods&>(
        methods) = InitializerValueBase::InitializerBase::
        template kMethodsFromConstInvoker<Function, Args...>;
    methods.reference = ReferenceMethodFromConstInvoker<Function, Args...>;
    methods.const_reference =
        ConstReferenceMethodFromConstInvoker<Function, Args...>;
    return methods;
  }
  template <typename Function, typename... Args>
  static constexpr Methods kMethodsFromConstInvoker =
      MakeMethodsFromConstInvoker<Function, Args...>();
#endif

  explicit InitializerValueBase(const Methods* methods)
      : InitializerValueBase::InitializerBase(methods) {}

  template <typename Arg>
  explicit InitializerValueBase(const Methods* methods, Arg&& arg)
      : InitializerValueBase::InitializerBase(methods, std::forward<Arg>(arg)) {
  }

  InitializerValueBase(InitializerValueBase&& that) = default;
  InitializerValueBase& operator=(InitializerValueBase&&) = delete;

  const Methods* methods() const {
    return static_cast<const Methods*>(
        InitializerValueBase::InitializerBase::methods());
  }
};

// Part of `Initializer<T>` for `T` being a non-reference move-assignable type.
template <typename T>
class InitializerAssignableValueBase : public InitializerValueBase<T> {
 public:
  // Makes `object` equivalent to the constructed `T`. This avoids constructing
  // a temporary `T` and moving from it.
  void AssignTo(T& object) && { methods()->assign_to(this->context(), object); }

 private:
  static void AssignToMethodDefault(void* context, T& object);

  template <typename Arg>
  static void AssignToMethodFromObject(void* context, T& object);

  template <typename... Args>
  static void AssignToMethodFromMaker(void* context, T& object);

  template <typename... Args>
  static void AssignToMethodFromConstMaker(void* context, T& object);

  template <typename Function, typename... Args>
  static void AssignToMethodFromInvoker(void* context, T& object);

  template <typename Function, typename... Args>
  static void AssignToMethodFromConstInvoker(void* context, T& object);

 protected:
  struct Methods
      : InitializerAssignableValueBase::InitializerValueBase::Methods {
    void (*assign_to)(void* context, T& object);
  };

#if __cpp_aggregate_bases
  template <typename Dummy = void>
  static constexpr Methods kMethodsDefault = {
      InitializerAssignableValueBase::InitializerValueBase::
          template kMethodsDefault<>,
      AssignToMethodDefault};

  template <typename Arg>
  static constexpr Methods kMethodsFromObject = {
      InitializerAssignableValueBase::InitializerValueBase::
          template kMethodsFromObject<Arg>,
      AssignToMethodFromObject<Arg>};

  template <typename... Args>
  static constexpr Methods kMethodsFromMaker = {
      InitializerAssignableValueBase::InitializerValueBase::
          template kMethodsFromMaker<Args...>,
      AssignToMethodFromMaker<Args...>};

  template <typename... Args>
  static constexpr Methods kMethodsFromConstMaker = {
      InitializerAssignableValueBase::InitializerValueBase::
          template kMethodsFromConstMaker<Args...>,
      AssignToMethodFromConstMaker<Args...>};

  template <typename Function, typename... Args>
  static constexpr Methods kMethodsFromInvoker = {
      InitializerAssignableValueBase::InitializerValueBase::
          template kMethodsFromInvoker<Function, Args...>,
      AssignToMethodFromInvoker<Function, Args...>};

  template <typename Function, typename... Args>
  static constexpr Methods kMethodsFromConstInvoker = {
      InitializerAssignableValueBase::InitializerValueBase::
          template kMethodsFromConstInvoker<Function, Args...>,
      AssignToMethodFromConstInvoker<Function, Args...>};
#else
  static constexpr Methods MakeMethodsDefault() {
    Methods methods;
    static_cast<typename InitializerAssignableValueBase::InitializerValueBase::
                    Methods&>(methods) = InitializerAssignableValueBase::
        InitializerValueBase::template kMethodsDefault<>;
    methods.assign_to = AssignToMethodDefault;
    return methods;
  }
  template <typename Dummy = void>
  static constexpr Methods kMethodsDefault = MakeMethodsDefault();

  template <typename Arg>
  static constexpr Methods MakeMethodsFromObject() {
    Methods methods;
    static_cast<typename InitializerAssignableValueBase::InitializerValueBase::
                    Methods&>(methods) = InitializerAssignableValueBase::
        InitializerValueBase::template kMethodsFromObject<Arg>;
    methods.assign_to = AssignToMethodFromObject<Arg>;
    return methods;
  }
  template <typename Arg>
  static constexpr Methods kMethodsFromObject = MakeMethodsFromObject<Arg>();

  template <typename... Args>
  static constexpr Methods MakeMethodsFromMaker() {
    Methods methods;
    static_cast<typename InitializerAssignableValueBase::InitializerValueBase::
                    Methods&>(methods) = InitializerAssignableValueBase::
        InitializerValueBase::template kMethodsFromMaker<Args...>;
    methods.assign_to = AssignToMethodFromMaker<Args...>;
    return methods;
  }
  template <typename... Args>
  static constexpr Methods kMethodsFromMaker = MakeMethodsFromMaker<Args...>();

  template <typename... Args>
  static constexpr Methods MakeMethodsFromConstMaker() {
    Methods methods;
    static_cast<typename InitializerAssignableValueBase::InitializerValueBase::
                    Methods&>(methods) = InitializerAssignableValueBase::
        InitializerValueBase::template kMethodsFromConstMaker<Args...>;
    methods.assign_to = AssignToMethodFromConstMaker<Args...>;
    return methods;
  }
  template <typename... Args>
  static constexpr Methods kMethodsFromConstMaker =
      MakeMethodsFromConstMaker<Args...>();

  template <typename Function, typename... Args>
  static constexpr Methods MakeMethodsFromInvoker() {
    Methods methods;
    static_cast<typename InitializerAssignableValueBase::InitializerValueBase::
                    Methods&>(methods) = InitializerAssignableValueBase::
        InitializerValueBase::template kMethodsFromInvoker<Function, Args...>;
    methods.assign_to = AssignToMethodFromInvoker<Function, Args...>;
    return methods;
  }
  template <typename Function, typename... Args>
  static constexpr Methods kMethodsFromInvoker =
      MakeMethodsFromInvoker<Function, Args...>();

  template <typename Function, typename... Args>
  static constexpr Methods MakeMethodsFromConstInvoker() {
    Methods methods;
    static_cast<typename InitializerAssignableValueBase::InitializerValueBase::
                    Methods&>(methods) =
        InitializerAssignableValueBase::InitializerValueBase::
            template kMethodsFromConstInvoker<Function, Args...>;
    methods.assign_to = AssignToMethodFromConstInvoker<Function, Args...>;
    return methods;
  }
  template <typename Function, typename... Args>
  static constexpr Methods kMethodsFromConstInvoker =
      MakeMethodsFromConstInvoker<Function, Args...>();
#endif

  explicit InitializerAssignableValueBase(const Methods* methods)
      : InitializerAssignableValueBase::InitializerValueBase(methods) {}

  template <typename Arg>
  explicit InitializerAssignableValueBase(const Methods* methods, Arg&& arg)
      : InitializerAssignableValueBase::InitializerValueBase(
            methods, std::forward<Arg>(arg)) {}

  InitializerAssignableValueBase(InitializerAssignableValueBase&& that) =
      default;
  InitializerAssignableValueBase& operator=(InitializerAssignableValueBase&&) =
      delete;

  const Methods* methods() const {
    return static_cast<const Methods*>(
        InitializerAssignableValueBase::InitializerValueBase::methods());
  }
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

// Primary definition of `Initializer` for non-reference move-assignable types.
template <typename T, bool allow_explicit = false, typename Enable = void>
class Initializer
    : public initializer_internal::InitializerAssignableValueBase<T> {
 public:
  // `Initializer<T>::AllowingExplicit` is implicitly convertible also from
  // a value explicitly convertible to `T`.
  //
  // This is useful for `Initializer<std::string>::AllowingExplicit` to accept
  // also `absl::string_view`.
  using AllowingExplicit = Initializer<T, true>;

  // Constructs `Initializer<T>` which specifies `T()`.
  template <typename DependentT = T,
            std::enable_if_t<std::is_default_constructible<DependentT>::value,
                             int> = 0>
  Initializer()
      : Initializer::InitializerAssignableValueBase(
            &Initializer::InitializerAssignableValueBase::
                template kMethodsDefault<>) {}

  // Constructs `Initializer<T>` from a value convertible to `T`.
  template <
      typename Arg,
      std::enable_if_t<
          absl::conjunction<absl::negation<initializer_internal::IsInitializer<
                                T, std::decay_t<Arg>>>,
                            initializer_internal::IsCompatibleArg<
                                allow_explicit, T, Arg&&>>::value,
          int> = 0>
  /*implicit*/ Initializer(Arg&& arg ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer::InitializerAssignableValueBase(
            &Initializer::InitializerAssignableValueBase::
                template kMethodsFromObject<Arg>,
            std::forward<Arg>(arg)) {}

  // Constructs `Initializer<T>` from constructor arguments for `T` packed in
  // `riegeli::Maker(args...)`.
  //
  // Prefer `Template(riegeli::Maker<T>(args...))` over
  // `Template<T>(riegeli::Maker(args...))` if CTAD for `Template` can be used.
  template <
      typename... Args,
      std::enable_if_t<std::is_constructible<T, Args&&...>::value, int> = 0>
  /*implicit*/ Initializer(
      MakerType<Args...>&& args ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer::InitializerAssignableValueBase(
            &Initializer::InitializerAssignableValueBase::
                template kMethodsFromMaker<Args...>,
            std::move(args)) {}
  template <typename... Args,
            std::enable_if_t<std::is_constructible<T, const Args&...>::value,
                             int> = 0>
  /*implicit*/ Initializer(
      const MakerType<Args...>& args ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer::InitializerAssignableValueBase(
            &Initializer::InitializerAssignableValueBase::
                template kMethodsFromConstMaker<Args...>,
            args) {}

  // Constructs `Initializer<T>` from constructor arguments for `T` packed in
  // `riegeli::Maker<T>(args...)`.
  template <
      typename... Args,
      std::enable_if_t<std::is_constructible<T, Args&&...>::value, int> = 0>
  /*implicit*/ Initializer(
      MakerTypeFor<T, Args...>&& args ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer::InitializerAssignableValueBase(
            &Initializer::InitializerAssignableValueBase::
                template kMethodsFromMaker<Args...>,
            std::move(args).maker()) {}
  // Constructs `Initializer<T>` from constructor arguments for `T` packed in
  // `riegeli::Maker<T>(args...)`.
  template <typename... Args,
            std::enable_if_t<std::is_constructible<T, const Args&...>::value,
                             int> = 0>
  /*implicit*/ Initializer(
      const MakerTypeFor<T, Args...>& args ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer::InitializerAssignableValueBase(
            &Initializer::InitializerAssignableValueBase::
                template kMethodsFromConstMaker<Args...>,
            args.maker()) {}

  template <typename Function, typename... Args,
            std::enable_if_t<
                initializer_internal::IsCompatibleResult<
                    allow_explicit, T,
                    typename InvokerType<Function, Args...>::Result>::value,
                int> = 0>
  /*implicit*/ Initializer(
      InvokerType<Function, Args...>&& invoker ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer::InitializerAssignableValueBase(
            &Initializer::InitializerAssignableValueBase::
                template kMethodsFromInvoker<Function, Args...>,
            std::move(invoker)) {}
  template <
      typename Function, typename... Args,
      std::enable_if_t<
          absl::conjunction<
              std::integral_constant<
                  bool, InvokerType<Function, Args...>::kIsConstCallable>,
              initializer_internal::IsCompatibleResult<
                  allow_explicit, T,
                  typename InvokerType<Function, Args...>::ConstResult>>::value,
          int> = 0>
  /*implicit*/ Initializer(const InvokerType<Function, Args...>& invoker
                               ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer::InitializerAssignableValueBase(
            &Initializer::InitializerAssignableValueBase::
                template kMethodsFromConstInvoker<Function, Args...>,
            invoker) {}

  // Constructs `Initializer<T>` from `Initializer<T>` with a different
  // `allow_explicit` by adopting its state instead of wrapping.
  template <bool other_allow_explicit>
  /*implicit*/ Initializer(Initializer<T, other_allow_explicit> initializer)
      : Initializer::InitializerAssignableValueBase(std::move(initializer)) {}

  Initializer(Initializer&& that) = default;
  Initializer& operator=(Initializer&&) = delete;
};

// Specialization of `Initializer` for non-reference non-assignable types.
template <typename T, bool allow_explicit>
class Initializer<T, allow_explicit,
                  std::enable_if_t<absl::conjunction<
                      absl::negation<std::is_reference<T>>,
                      absl::negation<std::is_move_assignable<T>>>::value>>
    : public initializer_internal::InitializerValueBase<T> {
 public:
  // `Initializer<T>::AllowingExplicit` is implicitly convertible also from
  // a value explicitly convertible to `T`.
  using AllowingExplicit = Initializer<T, true>;

  // Constructs `Initializer<T>` which specifies `T()`.
  template <typename DependentT = T,
            std::enable_if_t<std::is_default_constructible<DependentT>::value,
                             int> = 0>
  Initializer()
      : Initializer::InitializerValueBase(
            &Initializer::InitializerValueBase::template kMethodsDefault<>) {}

  // Constructs `Initializer<T>` from a value convertible to `T`.
  template <
      typename Arg,
      std::enable_if_t<
          absl::conjunction<absl::negation<initializer_internal::IsInitializer<
                                T, std::decay_t<Arg>>>,
                            initializer_internal::IsCompatibleArg<
                                allow_explicit, T, Arg&&>>::value,
          int> = 0>
  /*implicit*/ Initializer(Arg&& arg ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer::InitializerValueBase(
            &Initializer::InitializerValueBase::template kMethodsFromObject<
                Arg>,
            std::forward<Arg>(arg)) {}

  // Constructs `Initializer<T>` from constructor arguments for `T` packed in
  // `riegeli::Maker(args...)`.
  //
  // Prefer `Template(riegeli::Maker<T>(args...))` over
  // `Template<T>(riegeli::Maker(args...))` if CTAD for `Template` can be used.
  template <
      typename... Args,
      std::enable_if_t<std::is_constructible<T, Args&&...>::value, int> = 0>
  /*implicit*/ Initializer(
      MakerType<Args...>&& args ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer::InitializerValueBase(
            &Initializer::InitializerValueBase::template kMethodsFromMaker<
                Args...>,
            std::move(args)) {}
  template <typename... Args,
            std::enable_if_t<std::is_constructible<T, const Args&...>::value,
                             int> = 0>
  /*implicit*/ Initializer(
      const MakerType<Args...>& args ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer::InitializerValueBase(
            &Initializer::InitializerValueBase::template kMethodsFromConstMaker<
                Args...>,
            args) {}

  // Constructs `Initializer<T>` from constructor arguments for `T` packed in
  // `riegeli::Maker<T>(args...)`.
  template <
      typename... Args,
      std::enable_if_t<std::is_constructible<T, Args&&...>::value, int> = 0>
  /*implicit*/ Initializer(
      MakerTypeFor<T, Args...>&& args ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer::InitializerValueBase(
            &Initializer::InitializerValueBase::template kMethodsFromMaker<
                Args...>,
            std::move(args).maker()) {}
  template <typename... Args,
            std::enable_if_t<std::is_constructible<T, const Args&...>::value,
                             int> = 0>
  /*implicit*/ Initializer(
      const MakerTypeFor<T, Args...>& args ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer::InitializerValueBase(
            &Initializer::InitializerValueBase::template kMethodsFromConstMaker<
                Args...>,
            args.maker()) {}

  template <typename Function, typename... Args,
            std::enable_if_t<
                initializer_internal::IsCompatibleResult<
                    allow_explicit, T,
                    typename InvokerType<Function, Args...>::Result>::value,
                int> = 0>
  /*implicit*/ Initializer(
      InvokerType<Function, Args...>&& invoker ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer::InitializerValueBase(
            &Initializer::InitializerValueBase::template kMethodsFromInvoker<
                Function, Args...>,
            std::move(invoker)) {}
  template <
      typename Function, typename... Args,
      std::enable_if_t<
          absl::conjunction<
              std::integral_constant<
                  bool, InvokerType<Function, Args...>::kIsConstCallable>,
              initializer_internal::IsCompatibleResult<
                  allow_explicit, T,
                  typename InvokerType<Function, Args...>::ConstResult>>::value,
          int> = 0>
  /*implicit*/ Initializer(const InvokerType<Function, Args...>& invoker
                               ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer::InitializerValueBase(
            &Initializer::InitializerValueBase::
                template kMethodsFromConstInvoker<Function, Args...>,
            invoker) {}

  // Constructs `Initializer<T>` from `Initializer<T>` with a different
  // `allow_explicit` by adopting its state instead of wrapping.
  template <bool other_allow_explicit>
  /*implicit*/ Initializer(Initializer<T, other_allow_explicit> initializer)
      : Initializer::InitializerValueBase(std::move(initializer)) {}

  Initializer(Initializer&& that) = default;
  Initializer& operator=(Initializer&&) = delete;
};

// Specialization of `Initializer` for reference types.
template <typename T, bool allow_explicit>
class Initializer<T, allow_explicit,
                  std::enable_if_t<std::is_reference<T>::value>>
    : public initializer_internal::InitializerBase<T> {
 public:
  // `Initializer<T>::AllowingExplicit` is implicitly convertible also from
  // a value explicitly convertible to `T`.
  using AllowingExplicit = Initializer<T, true>;

  // Constructs `Initializer<T>` from a value convertible to `T`.
  template <
      typename Arg,
      std::enable_if_t<
          absl::conjunction<absl::negation<initializer_internal::IsInitializer<
                                T, std::decay_t<Arg>>>,
                            initializer_internal::IsCompatibleArg<
                                allow_explicit, T, Arg&&>>::value,
          int> = 0>
  /*implicit*/ Initializer(Arg&& arg ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer::InitializerBase(
            &Initializer::InitializerBase::template kMethodsFromObject<Arg>,
            std::forward<Arg>(arg)) {}

  // Constructs `Initializer<T>` from constructor arguments for `T` packed in
  // `riegeli::Maker(args...)`.
  //
  // Prefer `Template(riegeli::Maker<T>(args...))` over
  // `Template<T>(riegeli::Maker(args...))` if CTAD for `Template` can be used.
  template <
      typename... Args,
      std::enable_if_t<std::is_constructible<T, Args&&...>::value, int> = 0>
  /*implicit*/ Initializer(
      MakerType<Args...>&& args ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer::InitializerBase(
            &Initializer::InitializerBase::template kMethodsFromMaker<Args...>,
            std::move(args)) {}
  template <typename... Args,
            std::enable_if_t<std::is_constructible<T, const Args&...>::value,
                             int> = 0>
  /*implicit*/ Initializer(
      const MakerType<Args...>& args ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer::InitializerBase(
            &Initializer::InitializerBase::template kMethodsFromConstMaker<
                Args...>,
            args) {}

  // Constructs `Initializer<T>` from constructor arguments for `T` packed in
  // `riegeli::Maker<T>(args...)`.
  template <
      typename... Args,
      std::enable_if_t<std::is_constructible<T, Args&&...>::value, int> = 0>
  /*implicit*/ Initializer(
      MakerTypeFor<T, Args...>&& args ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer::InitializerBase(
            &Initializer::InitializerBase::template kMethodsFromMaker<Args...>,
            std::move(args).maker()) {}
  template <typename... Args,
            std::enable_if_t<std::is_constructible<T, const Args&...>::value,
                             int> = 0>
  /*implicit*/ Initializer(
      const MakerTypeFor<T, Args...>& args ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer::InitializerBase(
            &Initializer::InitializerBase::template kMethodsFromConstMaker<
                Args...>,
            args.maker()) {}

  template <typename Function, typename... Args,
            std::enable_if_t<
                initializer_internal::IsCompatibleResult<
                    allow_explicit, T,
                    typename InvokerType<Function, Args...>::Result>::value,
                int> = 0>
  /*implicit*/ Initializer(
      InvokerType<Function, Args...>&& invoker ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer::InitializerBase(
            &Initializer::InitializerBase::template kMethodsFromInvoker<
                Function, Args...>,
            std::move(invoker)) {}
  template <
      typename Function, typename... Args,
      std::enable_if_t<
          absl::conjunction<
              std::integral_constant<
                  bool, InvokerType<Function, Args...>::kIsConstCallable>,
              initializer_internal::IsCompatibleResult<
                  allow_explicit, T,
                  typename InvokerType<Function, Args...>::ConstResult>>::value,
          int> = 0>
  /*implicit*/ Initializer(const InvokerType<Function, Args...>& invoker
                               ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer::InitializerBase(
            &Initializer::InitializerBase::template kMethodsFromConstInvoker<
                Function, Args...>,
            invoker) {}

  // Constructs `Initializer<T>` from `Initializer<T>` with a different
  // `allow_explicit` by adopting its state instead of wrapping.
  template <bool other_allow_explicit>
  /*implicit*/ Initializer(Initializer<T, other_allow_explicit> initializer)
      : Initializer::InitializerBase(std::move(initializer)) {}

  Initializer(Initializer&& that) = default;
  Initializer& operator=(Initializer&&) = delete;

  // `Reference()` and `ConstReference()` can be defined in terms of
  // `Construct()` because reference storage is never used for reference types.
  //
  // Unused `storage` parameter makes the signature compatible with
  // the non-reference specialization.
  T&& Reference(ABSL_ATTRIBUTE_UNUSED TemporaryStorage<T> storage =
                    TemporaryStorage<T>()) && {
    // `T` is a reference type here, so `T&&` is the same as `T`.
    return std::move(*this).Construct();
  }
  const T& ConstReference(ABSL_ATTRIBUTE_UNUSED TemporaryStorage<T> storage =
                              TemporaryStorage<T>()) && {
    // `T` is a reference type here, but it can be an rvalue reference, in which
    // case `const T&` collapses to an lvalue reference.
    T reference = std::move(*this).Construct();
    return reference;
  }
};

// `InitializerTarget<T>::type` and `InitializerTargetT<T>` deduce the
// appropriate target type such that `T` is convertible to
// `Initializer<InitializerTargetT<T>>`.
//
// This allows a single template to uniformly handle a `Target` passed directly,
// as `riegeli::Maker<Target>(args...)`, or as `Initializer<Target>`. This is
// also useful for CTAD guides to deduce a template argument as
// `InitializerTargetT<T>`.
//
// This is undefined in the case of `riegeli::Maker(args...)` which requires the
// target type to be specified by the caller.

namespace initializer_internal {

template <typename Value, typename Original>
struct InitializerTargetImpl {
  using type = Value;
};

template <typename... Args, typename Original>
struct InitializerTargetImpl<MakerType<Args...>, Original> {
  // No `type` member when the target type is unspecified.
};

template <typename T, typename... Args, typename Original>
struct InitializerTargetImpl<MakerTypeFor<T, Args...>, Original> {
  using type = T;
};

template <typename Function, typename... Args, typename Original>
struct InitializerTargetImpl<InvokerType<Function, Args...>, Original> {
  using type = decltype(std::declval<Original>()());
};

template <typename T, typename Original>
struct InitializerTargetImpl<Initializer<T>, Original> {
  using type = T;
};

};  // namespace initializer_internal

template <typename T>
struct InitializerTarget
    : initializer_internal::InitializerTargetImpl<std::decay_t<T>, T> {};

template <typename T>
using InitializerTargetT = typename InitializerTarget<T>::type;

// Implementation details follow.

namespace initializer_internal {

template <typename T>
inline InitializerBase<T>::InitializerBase(const Methods* methods)
    : methods_(methods) {}

template <typename T>
template <typename Arg>
inline InitializerBase<T>::InitializerBase(const Methods* methods, Arg&& arg)
    : methods_(methods),
      context_(const_cast<absl::remove_cvref_t<Arg>*>(&arg)) {}

template <typename T>
T InitializerBase<T>::ConstructMethodDefault(
    ABSL_ATTRIBUTE_UNUSED void* context) {
  return T();
}

template <typename T>
template <typename Arg>
T InitializerBase<T>::ConstructMethodFromObject(void* context) {
  return T(
      std::forward<Arg>(*static_cast<std::remove_reference_t<Arg>*>(context)));
}

template <typename T>
template <typename... Args>
T InitializerBase<T>::ConstructMethodFromMaker(void* context) {
  return std::move(*static_cast<MakerType<Args...>*>(context))
      .template Construct<T>();
}

template <typename T>
template <typename... Args>
T InitializerBase<T>::ConstructMethodFromConstMaker(void* context) {
  return static_cast<const MakerType<Args...>*>(context)
      ->template Construct<T>();
}

template <typename T>
template <typename Function, typename... Args>
T InitializerBase<T>::ConstructMethodFromInvoker(void* context) {
  return T(std::move(*static_cast<InvokerType<Function, Args...>*>(context))());
}

template <typename T>
template <typename Function, typename... Args>
T InitializerBase<T>::ConstructMethodFromConstInvoker(void* context) {
  return T((*static_cast<const InvokerType<Function, Args...>*>(context))());
}

template <typename T>
T&& InitializerValueBase<T>::ReferenceMethodDefault(
    ABSL_ATTRIBUTE_UNUSED void* context, TemporaryStorage<T>&& storage) {
  return std::move(storage).emplace();
}

template <typename T>
template <typename Arg, std::enable_if_t<CanBindTo<T&&, Arg&&>::value, int>>
T&& InitializerValueBase<T>::ReferenceMethodFromObject(
    void* context, ABSL_ATTRIBUTE_UNUSED TemporaryStorage<T>&& storage) {
  return std::forward<Arg>(
      *static_cast<std::remove_reference_t<Arg>*>(context));
}

template <typename T>
template <typename Arg, std::enable_if_t<!CanBindTo<T&&, Arg&&>::value, int>>
T&& InitializerValueBase<T>::ReferenceMethodFromObject(
    void* context, TemporaryStorage<T>&& storage) {
  return std::move(storage).emplace(
      std::forward<Arg>(*static_cast<std::remove_reference_t<Arg>*>(context)));
}

template <typename T>
template <typename... Args>
T&& InitializerValueBase<T>::ReferenceMethodFromMaker(
    void* context, TemporaryStorage<T>&& storage) {
  return std::move(*static_cast<MakerType<Args...>*>(context))
      .template Reference<T>(std::move(storage));
}

template <typename T>
template <typename... Args>
T&& InitializerValueBase<T>::ReferenceMethodFromConstMaker(
    void* context, TemporaryStorage<T>&& storage) {
  return static_cast<const MakerType<Args...>*>(context)->template Reference<T>(
      std::move(storage));
}

template <typename T>
template <typename Function, typename... Args>
T&& InitializerValueBase<T>::ReferenceMethodFromInvoker(
    void* context, TemporaryStorage<T>&& storage) {
  return std::move(storage).invoke(
      std::move(*static_cast<InvokerType<Function, Args...>*>(context)));
}

template <typename T>
template <typename Function, typename... Args>
T&& InitializerValueBase<T>::ReferenceMethodFromConstInvoker(
    void* context, TemporaryStorage<T>&& storage) {
  return std::move(storage).invoke(
      *static_cast<const InvokerType<Function, Args...>*>(context));
}

template <typename T>
const T& InitializerValueBase<T>::ConstReferenceMethodDefault(
    ABSL_ATTRIBUTE_UNUSED void* context, TemporaryStorage<T>&& storage) {
  return storage.emplace();
}

template <typename T>
template <typename Arg,
          std::enable_if_t<CanBindTo<const T&, Arg&&>::value, int>>
const T& InitializerValueBase<T>::ConstReferenceMethodFromObject(
    void* context, ABSL_ATTRIBUTE_UNUSED TemporaryStorage<T>&& storage) {
  return *absl::implicit_cast<const T*>(
      static_cast<std::remove_reference_t<Arg>*>(context));
}

template <typename T>
template <typename Arg,
          std::enable_if_t<!CanBindTo<const T&, Arg&&>::value, int>>
const T& InitializerValueBase<T>::ConstReferenceMethodFromObject(
    void* context, TemporaryStorage<T>&& storage) {
  return storage.emplace(
      std::forward<Arg>(*static_cast<std::remove_reference_t<Arg>*>(context)));
}

template <typename T>
template <typename... Args>
const T& InitializerValueBase<T>::ConstReferenceMethodFromMaker(
    void* context, TemporaryStorage<T>&& storage) {
  return std::move(*static_cast<MakerType<Args...>*>(context))
      .template ConstReference<T>(std::move(storage));
}

template <typename T>
template <typename... Args>
const T& InitializerValueBase<T>::ConstReferenceMethodFromConstMaker(
    void* context, TemporaryStorage<T>&& storage) {
  return static_cast<const MakerType<Args...>*>(context)
      ->template ConstReference<T>(std::move(storage));
}

template <typename T>
template <typename Function, typename... Args>
const T& InitializerValueBase<T>::ConstReferenceMethodFromInvoker(
    void* context, TemporaryStorage<T>&& storage) {
  return storage.invoke(
      std::move(*static_cast<InvokerType<Function, Args...>*>(context)));
}

template <typename T>
template <typename Function, typename... Args>
const T& InitializerValueBase<T>::ConstReferenceMethodFromConstInvoker(
    void* context, TemporaryStorage<T>&& storage) {
  return storage.invoke(
      *static_cast<const InvokerType<Function, Args...>*>(context));
}

template <typename T>
void InitializerAssignableValueBase<T>::AssignToMethodDefault(
    ABSL_ATTRIBUTE_UNUSED void* context, T& object) {
  riegeli::Reset(object);
}

template <typename T>
template <typename Arg>
void InitializerAssignableValueBase<T>::AssignToMethodFromObject(void* context,
                                                                 T& object) {
  riegeli::Reset(
      object,
      std::forward<Arg>(*static_cast<std::remove_reference_t<Arg>*>(context)));
}

template <typename T>
template <typename... Args>
void InitializerAssignableValueBase<T>::AssignToMethodFromMaker(void* context,
                                                                T& object) {
  std::move(*static_cast<MakerType<Args...>*>(context))
      .template AssignTo<T>(object);
}

template <typename T>
template <typename... Args>
void InitializerAssignableValueBase<T>::AssignToMethodFromConstMaker(
    void* context, T& object) {
  static_cast<const MakerType<Args...>*>(context)->template AssignTo<T>(object);
}

template <typename T>
template <typename Function, typename... Args>
void InitializerAssignableValueBase<T>::AssignToMethodFromInvoker(void* context,
                                                                  T& object) {
  riegeli::Reset(
      object,
      std::move(*static_cast<InvokerType<Function, Args...>*>(context))());
}

template <typename T>
template <typename Function, typename... Args>
void InitializerAssignableValueBase<T>::AssignToMethodFromConstInvoker(
    void* context, T& object) {
  riegeli::Reset(
      object, (*static_cast<const InvokerType<Function, Args...>*>(context))());
}

}  // namespace initializer_internal

}  // namespace riegeli

#endif  // RIEGELI_BASE_INITIALIZER_H_
