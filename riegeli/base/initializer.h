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

#include <tuple>
#include <type_traits>

#include "absl/base/attributes.h"
#include "absl/meta/type_traits.h"
#include "absl/utility/utility.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/reset.h"

namespace riegeli {

namespace initializer_internal {

// `IsConstructible<false, T, Arg>::value` is `true` if `T` is implicitly
// constructible from `Arg`.
//
// `IsConstructible<true, T, Arg>::value` is `true` if `T` is implicitly or
// explicitly constructible from `Arg`.

template <bool allow_explicit, typename T, typename Arg>
struct IsConstructible;

template <typename T, typename Arg>
struct IsConstructible<false, T, Arg> : std::is_convertible<Arg, T> {};

template <typename T, typename Arg>
struct IsConstructible<true, T, Arg> : std::is_constructible<T, Arg> {};

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
  // `std::vector<T>::emplace_back()` or the constructor of `absl::optional<T>`
  // or `absl::StatusOr<T>`.
  /*implicit*/ operator T() && { return std::move(*this).Construct(); }

 private:
  static T ConstructMethodDefault(void* context);

  template <typename Arg>
  static T ConstructMethodFromObject(void* context);

  template <typename... Args>
  static T ConstructMethodFromMaker(void* context);

  template <typename... Args>
  static T ConstructMethodFromConstMaker(void* context);

  template <typename... Args>
  static T ConstructMethodFromTuple(void* context);

 protected:
  struct Methods {
    T (*construct)(void* context);
  };

  explicit InitializerBase(const Methods* methods);

  template <
      typename Arg,
      std::enable_if_t<!std::is_same<std::decay_t<Arg>, InitializerBase>::value,
                       int> = 0>
  explicit InitializerBase(const Methods* methods, Arg&& arg);

  template <typename... Args>
  explicit InitializerBase(const Methods* methods, MakerType<Args...>&& args);

  template <typename... Args>
  explicit InitializerBase(const Methods* methods,
                           const MakerType<Args...>& args);

  template <typename... Args>
  explicit InitializerBase(const Methods* methods, std::tuple<Args...>&& args);

  InitializerBase(InitializerBase&& other) = default;
  InitializerBase& operator=(InitializerBase&&) = delete;

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

  template <typename... Args>
  static constexpr Methods kMethodsFromTuple = {
      ConstructMethodFromTuple<Args...>};

  const Methods* methods() const { return methods_; }
  void* context() const { return context_; }

 private:
  const Methods* methods_;
  void* context_;
};

// Part of `Initializer<T>` for `T` being a non-reference type.
template <typename T>
class InitializerValueBase : public InitializerBase<T> {
 public:
  using ReferenceStorage = initializer_internal::ReferenceStorage<T>;

  // Constructs the `T`, or returns a reference to an already constructed object
  // if that was passed to the `Initializer`. This can avoid moving it.
  //
  // `reference_storage` must outlive usages of the returned reference.
  T&& Reference(ReferenceStorage&& reference_storage
                    ABSL_ATTRIBUTE_LIFETIME_BOUND = ReferenceStorage()) && {
    return methods()->reference(this->context(), std::move(reference_storage));
  }

 private:
  static T&& ReferenceMethodDefault(void* context,
                                    ReferenceStorage&& reference_storage);

  template <typename Arg, std::enable_if_t<CanBindTo<T, Arg>::value, int> = 0>
  static T&& ReferenceMethodFromObject(void* context,
                                       ReferenceStorage&& reference_storage);
  template <typename Arg, std::enable_if_t<!CanBindTo<T, Arg>::value, int> = 0>
  static T&& ReferenceMethodFromObject(void* context,
                                       ReferenceStorage&& reference_storage);

  template <typename... Args>
  static T&& ReferenceMethodFromMaker(void* context,
                                      ReferenceStorage&& reference_storage);

  template <typename... Args>
  static T&& ReferenceMethodFromConstMaker(
      void* context, ReferenceStorage&& reference_storage);

  template <typename... Args,
            std::enable_if_t<CanBindTo<T, Args...>::value, int> = 0>
  static T&& ReferenceMethodFromTuple(void* context,
                                      ReferenceStorage&& reference_storage);
  template <typename... Args,
            std::enable_if_t<!CanBindTo<T, Args...>::value, int> = 0>
  static T&& ReferenceMethodFromTuple(void* context,
                                      ReferenceStorage&& reference_storage);

 protected:
  struct Methods : InitializerValueBase::InitializerBase::Methods {
    T && (*reference)(void* context, ReferenceStorage&& reference_storage);
  };

#if __cpp_aggregate_bases
  static constexpr Methods kMethodsDefault = {
      InitializerValueBase::InitializerBase::kMethodsDefault,
      ReferenceMethodDefault};

  template <typename Arg>
  static constexpr Methods kMethodsFromObject = {
      InitializerValueBase::InitializerBase::template kMethodsFromObject<Arg>,
      ReferenceMethodFromObject<Arg>};

  template <typename... Args>
  static constexpr Methods kMethodsFromMaker = {
      InitializerValueBase::InitializerBase::template kMethodsFromMaker<
          Args...>,
      ReferenceMethodFromMaker<Args...>};

  template <typename... Args>
  static constexpr Methods kMethodsFromConstMaker = {
      InitializerValueBase::InitializerBase::template kMethodsFromConstMaker<
          Args...>,
      ReferenceMethodFromConstMaker<Args...>};

  template <typename... Args>
  static constexpr Methods kMethodsFromTuple = {
      InitializerValueBase::InitializerBase::template kMethodsFromTuple<
          Args...>,
      ReferenceMethodFromTuple<Args...>};
#else
  static constexpr Methods kMethodsDefault = [] {
    Methods methods(InitializerValueBase::InitializerBase::kMethodsDefault);
    methods.reference = ReferenceMethodDefault;
    return methods;
  }();

  template <typename Arg>
  static constexpr Methods kMethodsFromObject = [] {
    Methods methods(
        InitializerValueBase::InitializerBase::template kMethodsFromObject<
            Arg>);
    methods.reference = ReferenceMethodFromObject<Arg>;
    return methods;
  }();

  template <typename... Args>
  static constexpr Methods kMethodsFromMaker = [] {
    Methods methods(
        InitializerValueBase::InitializerBase::template kMethodsFromMaker<
            Args...>);
    methods.reference = ReferenceMethodFromMaker<Args...>;
    return methods;
  }();

  template <typename... Args>
  static constexpr Methods kMethodsFromConstMaker = [] {
    Methods methods(
        InitializerValueBase::InitializerBase::template kMethodsFromConstMaker<
            Args...>);
    methods.reference = ReferenceMethodFromConstMaker<Args...>;
    return methods;
  }();

  template <typename... Args>
  static constexpr Methods kMethodsFromTuple = [] {
    Methods methods(
        InitializerValueBase::InitializerBase::template kMethodsFromTuple<
            Args...>);
    methods.reference = ReferenceMethodFromTuple<Args...>;
    return methods;
  }();
#endif

  explicit InitializerValueBase(const Methods* methods)
      : InitializerValueBase::InitializerBase(methods) {}

  template <typename Arg,
            std::enable_if_t<
                !std::is_same<std::decay_t<Arg>, InitializerValueBase>::value,
                int> = 0>
  explicit InitializerValueBase(const Methods* methods, Arg&& arg)
      : InitializerValueBase::InitializerBase(methods, std::forward<Arg>(arg)) {
  }

  template <typename... Args>
  explicit InitializerValueBase(const Methods* methods,
                                MakerType<Args...>&& args)
      : InitializerValueBase::InitializerBase(methods, std::move(args)) {}

  template <typename... Args>
  explicit InitializerValueBase(const Methods* methods,
                                const MakerType<Args...>& args)
      : InitializerValueBase::InitializerBase(methods, args) {}

  template <typename... Args>
  explicit InitializerValueBase(const Methods* methods,
                                std::tuple<Args...>&& args)
      : InitializerValueBase::InitializerBase(methods, std::move(args)) {}

  InitializerValueBase(InitializerValueBase&& other) = default;
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

  template <typename... Args>
  static void AssignToMethodFromTuple(void* context, T& object);

 protected:
  struct Methods
      : InitializerAssignableValueBase::InitializerValueBase::Methods {
    void (*assign_to)(void* context, T& object);
  };

#if __cpp_aggregate_bases
  static constexpr Methods kMethodsDefault = {
      InitializerAssignableValueBase::InitializerValueBase::kMethodsDefault,
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

  template <typename... Args>
  static constexpr Methods kMethodsFromTuple = {
      InitializerAssignableValueBase::InitializerValueBase::
          template kMethodsFromTuple<Args...>,
      AssignToMethodFromTuple<Args...>};
#else
  static constexpr Methods kMethodsDefault = [] {
    Methods methods(
        InitializerAssignableValueBase::InitializerValueBase::kMethodsDefault);
    methods.assign_to = AssignToMethodDefault;
    return methods;
  }();

  template <typename Arg>
  static constexpr Methods kMethodsFromObject = [] {
    Methods methods(InitializerAssignableValueBase::InitializerValueBase::
                        template kMethodsFromObject<Arg>);
    methods.assign_to = AssignToMethodFromObject<Arg>;
    return methods;
  }();

  template <typename... Args>
  static constexpr Methods kMethodsFromMaker = [] {
    Methods methods(InitializerAssignableValueBase::InitializerValueBase::
                        template kMethodsFromMaker<Args...>);
    methods.assign_to = AssignToMethodFromMaker<Args...>;
    return methods;
  }();

  template <typename... Args>
  static constexpr Methods kMethodsFromConstMaker = [] {
    Methods methods(InitializerAssignableValueBase::InitializerValueBase::
                        template kMethodsFromConstMaker<Args...>);
    methods.assign_to = AssignToMethodFromConstMaker<Args...>;
    return methods;
  }();

  template <typename... Args>
  static constexpr Methods kMethodsFromTuple = [] {
    Methods methods(InitializerAssignableValueBase::InitializerValueBase::
                        template kMethodsFromTuple<Args...>);
    methods.assign_to = AssignToMethodFromTuple<Args...>;
    return methods;
  }();
#endif

  explicit InitializerAssignableValueBase(const Methods* methods)
      : InitializerAssignableValueBase::InitializerValueBase(methods) {}

  template <
      typename Arg,
      std::enable_if_t<!std::is_same<std::decay_t<Arg>,
                                     InitializerAssignableValueBase>::value,
                       int> = 0>
  explicit InitializerAssignableValueBase(const Methods* methods, Arg&& arg)
      : InitializerAssignableValueBase::InitializerValueBase(
            methods, std::forward<Arg>(arg)) {}

  template <typename... Args>
  explicit InitializerAssignableValueBase(const Methods* methods,
                                          MakerType<Args...>&& args)
      : InitializerAssignableValueBase::InitializerValueBase(methods,
                                                             std::move(args)) {}

  template <typename... Args>
  explicit InitializerAssignableValueBase(const Methods* methods,
                                          const MakerType<Args...>& args)
      : InitializerAssignableValueBase::InitializerValueBase(methods, args) {}

  template <typename... Args>
  explicit InitializerAssignableValueBase(const Methods* methods,
                                          std::tuple<Args...>&& args)
      : InitializerAssignableValueBase::InitializerValueBase(methods,
                                                             std::move(args)) {}

  InitializerAssignableValueBase(InitializerAssignableValueBase&& other) =
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
            &Initializer::InitializerAssignableValueBase::kMethodsDefault) {}

  // Constructs `Initializer<T>` from a value convertible to `T`.
  template <
      typename Arg,
      std::enable_if_t<absl::conjunction<absl::negation<std::is_same<
                                             std::decay_t<Arg>, Initializer>>,
                                         initializer_internal::IsConstructible<
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

  // Deprecated: use `riegeli::Maker(args...)` instead of
  // `std::forward_as_tuple(args...)`.
  //
  // Prefer `Template(riegeli::Maker<T>(args...))` over
  // `Template<T>(riegeli::Maker(args...))` if CTAD for `Template` can be used.
  template <
      typename... Args,
      std::enable_if_t<std::is_constructible<T, Args&&...>::value, int> = 0>
  ABSL_DEPRECATED(
      "Use riegeli::Maker(args...) instead of std::forward_as_tuple(args...). "
      "Prefer Template(riegeli::Maker<T>(args...)) over "
      "Template<T>(riegeli::Maker(args...)) if CTAD for Template can be used")
  /*implicit*/ Initializer(
      std::tuple<Args...>&& args ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer::InitializerAssignableValueBase(
            &Initializer::InitializerAssignableValueBase::
                template kMethodsFromTuple<Args...>,
            std::move(args)) {}

  Initializer(Initializer&& other) = default;
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
            &Initializer::InitializerValueBase::kMethodsDefault) {}

  // Constructs `Initializer<T>` from a value convertible to `T`.
  template <
      typename Arg,
      std::enable_if_t<absl::conjunction<absl::negation<std::is_same<
                                             std::decay_t<Arg>, Initializer>>,
                                         initializer_internal::IsConstructible<
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

  // Deprecated: use `riegeli::Maker(args...)` instead of
  // `std::forward_as_tuple(args...)`.
  //
  // Prefer `Template(riegeli::Maker<T>(args...))` over
  // `Template<T>(riegeli::Maker(args...))` if CTAD for `Template` can be used.
  template <
      typename... Args,
      std::enable_if_t<std::is_constructible<T, Args&&...>::value, int> = 0>
  ABSL_DEPRECATED(
      "Use riegeli::Maker(args...) instead of std::forward_as_tuple(args...). "
      "Prefer Template(riegeli::Maker<T>(args...)) over "
      "Template<T>(riegeli::Maker(args...)) if CTAD for Template can be used")
  /*implicit*/ Initializer(
      std::tuple<Args...>&& args ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer::InitializerValueBase(
            &Initializer::InitializerValueBase::template kMethodsFromTuple<
                Args...>,
            std::move(args)) {}

  Initializer(Initializer&& other) = default;
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

  struct ReferenceStorage {};

  // Constructs `Initializer<T>` from a value convertible to `T`.
  template <
      typename Arg,
      std::enable_if_t<absl::conjunction<absl::negation<std::is_same<
                                             std::decay_t<Arg>, Initializer>>,
                                         initializer_internal::IsConstructible<
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

  // Deprecated: use `riegeli::Maker(args...)` instead of
  // `std::forward_as_tuple(args...)`.
  //
  // Prefer `Template(riegeli::Maker<T>(args...))` over
  // `Template<T>(riegeli::Maker(args...))` if CTAD for `Template` can be used.
  template <
      typename... Args,
      std::enable_if_t<std::is_constructible<T, Args&&...>::value, int> = 0>
  ABSL_DEPRECATED(
      "Use riegeli::Maker(args...) instead of std::forward_as_tuple(args...). "
      "Prefer Template(riegeli::Maker<T>(args...)) over "
      "Template<T>(riegeli::Maker(args...)) if CTAD for Template can be used")
  /*implicit*/ Initializer(
      std::tuple<Args...>&& args ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer::InitializerBase(
            &Initializer::InitializerBase::template kMethodsFromTuple<Args...>,
            std::move(args)) {}

  Initializer(Initializer&& other) = default;
  Initializer& operator=(Initializer&&) = delete;

  // `Reference()` can be defined in terms of `Construct()` because reference
  // storage is never used for reference types.
  //
  // Unused `reference_storage` parameter makes the signature compatible with
  // the non-reference specialization.
  T&& Reference(ABSL_ATTRIBUTE_UNUSED ReferenceStorage reference_storage =
                    ReferenceStorage()) && {
    return std::move(*this).Construct();
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
// This is undefined in the case of `riegeli::Maker(args...)` and deprecated
// `std::tuple<Args...>` which require the target type to be specified by the
// caller.

namespace initializer_internal {

template <typename T>
struct InitializerTargetImpl {
  using type = T;
};

template <typename... Args>
struct InitializerTargetImpl<MakerType<Args...>> {
  // No `type` member when the target type is unspecified.
};

template <typename T, typename... Args>
struct InitializerTargetImpl<MakerTypeFor<T, Args...>> {
  using type = T;
};

template <typename... Args>
struct InitializerTargetImpl<std::tuple<Args...>> {
  // No `type` member when the target type is unspecified.
};

template <typename T>
struct InitializerTargetImpl<Initializer<T>> {
  using type = T;
};

};  // namespace initializer_internal

template <typename T>
struct InitializerTarget
    : initializer_internal::InitializerTargetImpl<std::decay_t<T>> {};

template <typename T>
using InitializerTargetT = typename InitializerTarget<T>::type;

// Implementation details follow.

namespace initializer_internal {

template <typename T>
inline InitializerBase<T>::InitializerBase(const Methods* methods)
    : methods_(methods) {}

template <typename T>
template <typename Arg,
          std::enable_if_t<
              !std::is_same<std::decay_t<Arg>, InitializerBase<T>>::value, int>>
inline InitializerBase<T>::InitializerBase(const Methods* methods, Arg&& arg)
    : methods_(methods),
      context_(const_cast<absl::remove_cvref_t<Arg>*>(&arg)) {}

template <typename T>
template <typename... Args>
inline InitializerBase<T>::InitializerBase(const Methods* methods,
                                           MakerType<Args...>&& args)
    : methods_(methods), context_(&args) {}

template <typename T>
template <typename... Args>
inline InitializerBase<T>::InitializerBase(const Methods* methods,
                                           const MakerType<Args...>& args)
    : methods_(methods), context_(const_cast<MakerType<Args...>*>(&args)) {}

template <typename T>
template <typename... Args>
inline InitializerBase<T>::InitializerBase(const Methods* methods,
                                           std::tuple<Args...>&& args)
    : methods_(methods), context_(&args) {}

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
template <typename... Args>
T InitializerBase<T>::ConstructMethodFromTuple(void* context) {
  return absl::make_from_tuple<T>(
      std::move(*static_cast<std::tuple<Args...>*>(context)));
}

template <typename T>
T&& InitializerValueBase<T>::ReferenceMethodDefault(
    ABSL_ATTRIBUTE_UNUSED void* context, ReferenceStorage&& reference_storage) {
  reference_storage.emplace();
  return *std::move(reference_storage);
}

template <typename T>
template <typename Arg, std::enable_if_t<CanBindTo<T, Arg>::value, int>>
T&& InitializerValueBase<T>::ReferenceMethodFromObject(
    void* context, ABSL_ATTRIBUTE_UNUSED ReferenceStorage&& reference_storage) {
  return std::forward<T>(*static_cast<std::remove_reference_t<Arg>*>(context));
}

template <typename T>
template <typename Arg, std::enable_if_t<!CanBindTo<T, Arg>::value, int>>
T&& InitializerValueBase<T>::ReferenceMethodFromObject(
    void* context, ReferenceStorage&& reference_storage) {
  reference_storage.emplace(
      std::forward<Arg>(*static_cast<std::remove_reference_t<Arg>*>(context)));
  return *std::move(reference_storage);
}

template <typename T>
template <typename... Args>
T&& InitializerValueBase<T>::ReferenceMethodFromMaker(
    void* context, ReferenceStorage&& reference_storage) {
  return std::move(*static_cast<MakerType<Args...>*>(context))
      .template Reference<T>(std::move(reference_storage));
}

template <typename T>
template <typename... Args>
T&& InitializerValueBase<T>::ReferenceMethodFromConstMaker(
    void* context, ReferenceStorage&& reference_storage) {
  return static_cast<const MakerType<Args...>*>(context)->template Reference<T>(
      std::move(reference_storage));
}

template <typename T>
template <typename... Args, std::enable_if_t<CanBindTo<T, Args...>::value, int>>
T&& InitializerValueBase<T>::ReferenceMethodFromTuple(
    void* context, ABSL_ATTRIBUTE_UNUSED ReferenceStorage&& reference_storage) {
  return std::forward<T>(
      std::get<0>(*static_cast<std::tuple<Args...>*>(context)));
}

template <typename T>
template <typename... Args,
          std::enable_if_t<!CanBindTo<T, Args...>::value, int>>
T&& InitializerValueBase<T>::ReferenceMethodFromTuple(
    void* context, ReferenceStorage&& reference_storage) {
  absl::apply(
      [&](Args&&... args) {
        reference_storage.emplace(std::forward<Args>(args)...);
      },
      std::move(*static_cast<std::tuple<Args...>*>(context)));
  return *std::move(reference_storage);
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
template <typename... Args>
void InitializerAssignableValueBase<T>::AssignToMethodFromTuple(void* context,
                                                                T& object) {
  absl::apply(
      [&](Args&&... args) {
        riegeli::Reset(object, std::forward<Args>(args)...);
      },
      std::move(*static_cast<std::tuple<Args...>*>(context)));
}

}  // namespace initializer_internal

}  // namespace riegeli

#endif  // RIEGELI_BASE_INITIALIZER_H_
