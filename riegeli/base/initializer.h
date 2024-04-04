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

#include <new>
#include <tuple>
#include <type_traits>

#include "absl/base/attributes.h"
#include "absl/meta/type_traits.h"
#include "absl/utility/utility.h"
#include "riegeli/base/assert.h"
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

// `CanBindTo<T, Args...>::value` is `true` if constructing `T(args...)` with
// `args...` of type `Args...` can be elided, with `T&&` binding directly to
// the only element of `args...` instead.

template <typename T, typename... Args>
struct CanBindTo : std::false_type {};

template <typename T, typename Arg>
struct CanBindTo<T, Arg> : std::is_convertible<Arg*, T*> {};

template <typename T, typename Arg>
struct CanBindTo<T, Arg&> : std::false_type {};

template <typename T, typename Arg>
struct CanBindTo<T, Arg&&> : std::is_convertible<Arg*, T*> {};

// Internal storage which is conditionally needed for storing the object that
// `Initializer<T>::Reference()` refers to.
//
// `ReferenceStorage<T>()` is passed as the default value of a parameter of
// `Initializer<T>::Reference()` with type `ReferenceStorage<T>&&`, so that
// it is allocated as a temporary by the caller.
//
// It can also be passed explicitly if the call to `Initializer<T>::Reference()`
// happens in a context which needs the returned reference to be valid longer
// than the full expression containing the call. This passes the responsibility
// for passing a `ReferenceStorage<T>` with a suitable lifetime to the caller
// of that context.
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

// Part of `Initializer<T>` common to all specializations.
template <typename T>
class InitializerBase {
 public:
  // Constructs the `T`.
  T Construct() && { return methods()->construct(context()); }

  // Constructs the `T` by an implicit conversion to `T`.
  //
  // It is preferred to explicitly call `Construct()` instead, but this
  // conversion allows to pass `Initializer<T>` to another function no matter
  // whether it is expecting an `Initializer<T>` or `T`, including functions
  // like `std::vector<T>::emplace_back()`.
  /*implicit*/ operator T() && { return std::move(*this).Construct(); }

 private:
  template <typename Arg>
  static T ConstructMethod(void* context);

  template <typename... Args>
  static T ConstructMethodFromTuple(void* context);

 protected:
  struct Methods {
    T (*construct)(void* context);
  };

  template <
      typename Arg,
      std::enable_if_t<!std::is_same<std::decay_t<Arg>, InitializerBase>::value,
                       int> = 0>
  explicit InitializerBase(const Methods* methods, Arg&& arg);

  template <typename... Args>
  explicit InitializerBase(const Methods* methods, std::tuple<Args...>&& args);

  InitializerBase(InitializerBase&& other) = default;
  InitializerBase& operator=(InitializerBase&&) = delete;

  template <typename Arg>
  static constexpr Methods kMethods = {ConstructMethod<Arg>};

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
    return methods()->reference(this->context(), reference_storage);
  }

 private:
  template <typename Arg, std::enable_if_t<CanBindTo<T, Arg>::value, int> = 0>
  static T&& ReferenceMethod(void* context,
                             ReferenceStorage& reference_storage);
  template <typename Arg, std::enable_if_t<!CanBindTo<T, Arg>::value, int> = 0>
  static T&& ReferenceMethod(void* context,
                             ReferenceStorage& reference_storage);

  template <typename... Args,
            std::enable_if_t<CanBindTo<T, Args...>::value, int> = 0>
  static T&& ReferenceMethodFromTuple(void* context,
                                      ReferenceStorage& reference_storage);
  template <typename... Args,
            std::enable_if_t<!CanBindTo<T, Args...>::value, int> = 0>
  static T&& ReferenceMethodFromTuple(void* context,
                                      ReferenceStorage& reference_storage);

 protected:
  struct Methods : InitializerValueBase::InitializerBase::Methods {
    T && (*reference)(void* context, ReferenceStorage& reference_storage);
  };

#if __cpp_aggregate_bases
  template <typename Arg>
  static constexpr Methods kMethods = {
      InitializerValueBase::InitializerBase::template kMethods<Arg>,
      ReferenceMethod<Arg>};

  template <typename... Args>
  static constexpr Methods kMethodsFromTuple = {
      InitializerValueBase::InitializerBase::template kMethodsFromTuple<
          Args...>,
      ReferenceMethodFromTuple<Args...>};
#else
  template <typename Arg>
  static constexpr Methods kMethods = [] {
    Methods methods(
        InitializerValueBase::InitializerBase::template kMethods<Arg>);
    methods.reference = ReferenceMethod<Arg>;
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

  template <typename Arg,
            std::enable_if_t<
                !std::is_same<std::decay_t<Arg>, InitializerValueBase>::value,
                int> = 0>
  explicit InitializerValueBase(const Methods* methods, Arg&& arg)
      : InitializerValueBase::InitializerBase(methods, std::forward<Arg>(arg)) {
  }

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
  template <typename Arg>
  static void AssignToMethod(void* context, T& object);

  template <typename... Args>
  static void AssignToMethodFromTuple(void* context, T& object);

 protected:
  struct Methods
      : InitializerAssignableValueBase::InitializerValueBase::Methods {
    void (*assign_to)(void* context, T& object);
  };

#if __cpp_aggregate_bases
  template <typename Arg>
  static constexpr Methods kMethods = {
      InitializerAssignableValueBase::InitializerValueBase::template kMethods<
          Arg>,
      AssignToMethod<Arg>};

  template <typename... Args>
  static constexpr Methods kMethodsFromTuple = {
      InitializerAssignableValueBase::InitializerValueBase::
          template kMethodsFromTuple<Args...>,
      AssignToMethodFromTuple<Args...>};
#else
  template <typename Arg>
  static constexpr Methods kMethods = [] {
    Methods methods(
        InitializerAssignableValueBase::InitializerValueBase::template kMethods<
            Arg>);
    methods.assign_to = AssignToMethod<Arg>;
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
// passing a value convertible to `T` or a tuple of constructor arguments for
// `T` (usually made with `std::forward_as_tuple()`).
//
// In contrast to accepting `T` directly, this allows to avoid constructing a
// temporary `T` and moving from it. This also avoids separate overloads for
// `const T&` and `T&&` or a template.
//
// `Initializer<T>` does not own the object which was passed to it, even if that
// is a temporary, so it should be used only as a parameter of a function or
// constructor, so that the temporaries outlive its usage.
//
// `Initializer<T>` itself is move-constructible.

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
            &Initializer::InitializerAssignableValueBase::template kMethods<
                Arg>,
            std::forward<Arg>(arg)) {}

  // Constructs `Initializer<T>` from a tuple of constructor arguments for `T`
  // (usually made with `std::forward_as_tuple()`).
  template <
      typename... Args,
      std::enable_if_t<std::is_constructible<T, Args&&...>::value, int> = 0>
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
            &Initializer::InitializerValueBase::template kMethods<Arg>,
            std::forward<Arg>(arg)) {}

  // Constructs `Initializer<T>` from a tuple of constructor arguments for `T`
  // (usually made with `std::forward_as_tuple()`).
  template <
      typename... Args,
      std::enable_if_t<std::is_constructible<T, Args&&...>::value, int> = 0>
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
            &Initializer::InitializerBase::template kMethods<Arg>,
            std::forward<Arg>(arg)) {}

  // Constructs `Initializer<T>` from a tuple of constructor arguments for `T`
  // (usually made with `std::forward_as_tuple()`).
  template <
      typename... Args,
      std::enable_if_t<std::is_constructible<T, Args&&...>::value, int> = 0>
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

// Implementation details follow.

namespace initializer_internal {

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
                                           std::tuple<Args...>&& args)
    : methods_(methods), context_(&args) {}

template <typename T>
template <typename Arg>
T InitializerBase<T>::ConstructMethod(void* context) {
  return T(
      std::forward<Arg>(*static_cast<std::remove_reference_t<Arg>*>(context)));
}

template <typename T>
template <typename... Args>
T InitializerBase<T>::ConstructMethodFromTuple(void* context) {
  return absl::make_from_tuple<T>(
      std::move(*static_cast<std::tuple<Args...>*>(context)));
}

template <typename T>
template <typename Arg, std::enable_if_t<CanBindTo<T, Arg>::value, int>>
T&& InitializerValueBase<T>::ReferenceMethod(
    void* context, ABSL_ATTRIBUTE_UNUSED ReferenceStorage& reference_storage) {
  return std::forward<T>(*static_cast<std::remove_reference_t<Arg>*>(context));
}

template <typename T>
template <typename Arg, std::enable_if_t<!CanBindTo<T, Arg>::value, int>>
T&& InitializerValueBase<T>::ReferenceMethod(
    void* context, ReferenceStorage& reference_storage) {
  reference_storage.emplace(
      std::forward<Arg>(*static_cast<std::remove_reference_t<Arg>*>(context)));
  return *std::move(reference_storage);
}

template <typename T>
template <typename... Args, std::enable_if_t<CanBindTo<T, Args...>::value, int>>
T&& InitializerValueBase<T>::ReferenceMethodFromTuple(
    void* context, ABSL_ATTRIBUTE_UNUSED ReferenceStorage& reference_storage) {
  return std::forward<T>(
      std::get<0>(*static_cast<std::tuple<Args...>*>(context)));
}

template <typename T>
template <typename... Args,
          std::enable_if_t<!CanBindTo<T, Args...>::value, int>>
T&& InitializerValueBase<T>::ReferenceMethodFromTuple(
    void* context, ReferenceStorage& reference_storage) {
  absl::apply(
      [&](Args&&... args) {
        reference_storage.emplace(std::forward<Args>(args)...);
      },
      std::move(*static_cast<std::tuple<Args...>*>(context)));
  return *std::move(reference_storage);
}

template <typename T>
template <typename Arg>
void InitializerAssignableValueBase<T>::AssignToMethod(void* context,
                                                       T& object) {
  riegeli::Reset(
      object,
      std::forward<Arg>(*static_cast<std::remove_reference_t<Arg>*>(context)));
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
