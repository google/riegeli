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
  T Construct() && { return construct_(context()); }

  // Constructs the `T` by an implicit conversion to `T`.
  //
  // It is preferred to explicitly call `Construct()` instead, but this
  // conversion allows to pass `Initializer<T>` to another function no matter
  // whether it is expecting an `Initializer<T>` or `T`.
  /*implicit*/ operator T() && { return std::move(*this).Construct(); }

 protected:
  template <
      typename Arg,
      std::enable_if_t<!std::is_same<std::decay_t<Arg>, InitializerBase>::value,
                       int> = 0>
  explicit InitializerBase(Arg&& arg);

  template <typename... Args>
  explicit InitializerBase(std::tuple<Args...>&& args);

  InitializerBase(InitializerBase&& other) = default;
  InitializerBase& operator=(InitializerBase&&) = delete;

  void* context() const { return context_; }

 private:
  void* context_;
  T (*construct_)(void* context);
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
    return reference_(this->context(), reference_storage);
  }

 protected:
  template <typename Arg,
            std::enable_if_t<
                !std::is_same<std::decay_t<Arg>, InitializerValueBase>::value,
                int> = 0>
  explicit InitializerValueBase(Arg&& arg);

  template <typename... Args>
  explicit InitializerValueBase(std::tuple<Args...>&& args);

  InitializerValueBase(InitializerValueBase&& other) = default;
  InitializerValueBase& operator=(InitializerValueBase&&) = delete;

 private:
  template <typename Arg, std::enable_if_t<CanBindTo<T, Arg>::value, int> = 0>
  static T&& ReferenceMethod(Arg&& arg, ReferenceStorage& reference_storage);
  template <typename Arg, std::enable_if_t<!CanBindTo<T, Arg>::value, int> = 0>
  static T&& ReferenceMethod(Arg&& arg, ReferenceStorage& reference_storage);

  template <typename... Args,
            std::enable_if_t<CanBindTo<T, Args...>::value, int> = 0>
  static T&& ReferenceMethod(std::tuple<Args...>& args,
                             ReferenceStorage& reference_storage);
  template <typename... Args,
            std::enable_if_t<!CanBindTo<T, Args...>::value, int> = 0>
  static T&& ReferenceMethod(std::tuple<Args...>& args,
                             ReferenceStorage& reference_storage);

  T && (*reference_)(void* context, ReferenceStorage& reference_storage);
};

// Part of `Initializer<T>` for `T` being a non-reference move-assignable type.
template <typename T>
class InitializerAssignableValueBase : public InitializerValueBase<T> {
 public:
  // Makes `object` equivalent to the constructed `T`. This avoids constructing
  // a temporary `T` and moving from it.
  void AssignTo(T& object) && { assign_to_(this->context(), object); }

 protected:
  template <
      typename Arg,
      std::enable_if_t<!std::is_same<std::decay_t<Arg>,
                                     InitializerAssignableValueBase>::value,
                       int> = 0>
  explicit InitializerAssignableValueBase(Arg&& arg);

  template <typename... Args>
  explicit InitializerAssignableValueBase(std::tuple<Args...>&& args);

  InitializerAssignableValueBase(InitializerAssignableValueBase&& other) =
      default;
  InitializerAssignableValueBase& operator=(InitializerAssignableValueBase&&) =
      delete;

 private:
  void (*assign_to_)(void* context, T& object);
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
template <typename T, typename Enable = void>
class Initializer
    : public initializer_internal::InitializerAssignableValueBase<T> {
 public:
  // Constructs `Initializer<T>` from a value convertible to `T`.
  template <
      typename Arg,
      std::enable_if_t<absl::conjunction<absl::negation<std::is_same<
                                             std::decay_t<Arg>, Initializer>>,
                                         std::is_convertible<Arg&&, T>>::value,
                       int> = 0>
  /*implicit*/ Initializer(Arg&& arg ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer::InitializerAssignableValueBase(std::forward<Arg>(arg)) {}

  // Constructs `Initializer<T>` from a tuple of constructor arguments for `T`
  // (usually made with `std::forward_as_tuple()`).
  template <
      typename... Args,
      std::enable_if_t<std::is_constructible<T, Args&&...>::value, int> = 0>
  /*implicit*/ Initializer(
      std::tuple<Args...>&& args ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer::InitializerAssignableValueBase(std::move(args)) {}

  Initializer(Initializer&& other) = default;
  Initializer& operator=(Initializer&&) = delete;
};

// Specialization of `Initializer` for non-reference non-assignable types.
template <typename T>
class Initializer<T, std::enable_if_t<absl::conjunction<
                         absl::negation<std::is_reference<T>>,
                         absl::negation<std::is_move_assignable<T>>>::value>>
    : public initializer_internal::InitializerValueBase<T> {
 public:
  // Constructs `Initializer<T>` from a value convertible to `T`.
  template <
      typename Arg,
      std::enable_if_t<absl::conjunction<absl::negation<std::is_same<
                                             std::decay_t<Arg>, Initializer>>,
                                         std::is_convertible<Arg&&, T>>::value,
                       int> = 0>
  /*implicit*/ Initializer(Arg&& arg ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer::InitializerValueBase(std::forward<Arg>(arg)) {}

  // Constructs `Initializer<T>` from a tuple of constructor arguments for `T`
  // (usually made with `std::forward_as_tuple()`).
  template <
      typename... Args,
      std::enable_if_t<std::is_constructible<T, Args&&...>::value, int> = 0>
  /*implicit*/ Initializer(
      std::tuple<Args...>&& args ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer::InitializerValueBase(std::move(args)) {}

  Initializer(Initializer&& other) = default;
  Initializer& operator=(Initializer&&) = delete;
};

// Specialization of `Initializer` for reference types.
template <typename T>
class Initializer<T, std::enable_if_t<std::is_reference<T>::value>>
    : public initializer_internal::InitializerBase<T> {
 public:
  struct ReferenceStorage {};

  // Constructs `Initializer<T>` from a value convertible to `T`.
  template <
      typename Arg,
      std::enable_if_t<absl::conjunction<absl::negation<std::is_same<
                                             std::decay_t<Arg>, Initializer>>,
                                         std::is_convertible<Arg&&, T>>::value,
                       int> = 0>
  /*implicit*/ Initializer(Arg&& arg ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer::InitializerBase(std::forward<Arg>(arg)) {}

  // Constructs `Initializer<T>` from a tuple of constructor arguments for `T`
  // (usually made with `std::forward_as_tuple()`).
  template <
      typename... Args,
      std::enable_if_t<std::is_constructible<T, Args&&...>::value, int> = 0>
  /*implicit*/ Initializer(
      std::tuple<Args...>&& args ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer::InitializerBase(std::move(args)) {}

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
inline InitializerBase<T>::InitializerBase(Arg&& arg)
    : context_(const_cast<absl::remove_cvref_t<Arg>*>(&arg)),
      construct_([](void* context) -> T {
        return std::forward<Arg>(
            *static_cast<std::remove_reference_t<Arg>*>(context));
      }) {}

template <typename T>
template <typename... Args>
inline InitializerBase<T>::InitializerBase(std::tuple<Args...>&& args)
    : context_(&args), construct_([](void* context) -> T {
        return absl::make_from_tuple<T>(
            std::move(*static_cast<std::tuple<Args...>*>(context)));
      }) {}

template <typename T>
template <
    typename Arg,
    std::enable_if_t<
        !std::is_same<std::decay_t<Arg>, InitializerValueBase<T>>::value, int>>
inline InitializerValueBase<T>::InitializerValueBase(Arg&& arg)
    : InitializerValueBase::InitializerBase(std::forward<Arg>(arg)),
      reference_([](void* context, ReferenceStorage& reference_storage) -> T&& {
        return ReferenceMethod(
            std::forward<Arg>(
                *static_cast<std::remove_reference_t<Arg>*>(context)),
            reference_storage);
      }) {}

template <typename T>
template <typename... Args>
inline InitializerValueBase<T>::InitializerValueBase(std::tuple<Args...>&& args)
    : InitializerValueBase::InitializerBase(std::move(args)),
      reference_([](void* context, ReferenceStorage& reference_storage) -> T&& {
        return ReferenceMethod(*static_cast<std::tuple<Args...>*>(context),
                               reference_storage);
      }) {}

template <typename T>
template <typename Arg, std::enable_if_t<CanBindTo<T, Arg>::value, int>>
inline T&& InitializerValueBase<T>::ReferenceMethod(
    Arg&& arg, ABSL_ATTRIBUTE_UNUSED ReferenceStorage& reference_storage) {
  return std::forward<T>(arg);
}

template <typename T>
template <typename Arg, std::enable_if_t<!CanBindTo<T, Arg>::value, int>>
inline T&& InitializerValueBase<T>::ReferenceMethod(
    Arg&& arg, ReferenceStorage& reference_storage) {
  reference_storage.emplace(std::forward<Arg>(arg));
  return *std::move(reference_storage);
}

template <typename T>
template <typename... Args, std::enable_if_t<CanBindTo<T, Args...>::value, int>>
inline T&& InitializerValueBase<T>::ReferenceMethod(
    std::tuple<Args...>& args,
    ABSL_ATTRIBUTE_UNUSED ReferenceStorage& reference_storage) {
  return std::forward<T>(std::get<0>(args));
}

template <typename T>
template <typename... Args,
          std::enable_if_t<!CanBindTo<T, Args...>::value, int>>
inline T&& InitializerValueBase<T>::ReferenceMethod(
    std::tuple<Args...>& args, ReferenceStorage& reference_storage) {
  absl::apply(
      [&](Args&&... args) {
        reference_storage.emplace(std::forward<Args>(args)...);
      },
      std::move(args));
  return *std::move(reference_storage);
}

template <typename T>
template <
    typename Arg,
    std::enable_if_t<!std::is_same<std::decay_t<Arg>,
                                   InitializerAssignableValueBase<T>>::value,
                     int>>
inline InitializerAssignableValueBase<T>::InitializerAssignableValueBase(
    Arg&& arg)
    : InitializerAssignableValueBase::InitializerValueBase(
          std::forward<Arg>(arg)),
      assign_to_([](void* context, T& object) {
        riegeli::Reset(
            object, std::forward<Arg>(
                        *static_cast<std::remove_reference_t<Arg>*>(context)));
      }) {}

template <typename T>
template <typename... Args>
inline InitializerAssignableValueBase<T>::InitializerAssignableValueBase(
    std::tuple<Args...>&& args)
    : InitializerAssignableValueBase::InitializerValueBase(std::move(args)),
      assign_to_([](void* context, T& object) {
        absl::apply(
            [&](Args&&... args) {
              riegeli::Reset(object, std::forward<Args>(args)...);
            },
            std::move(*static_cast<std::tuple<Args...>*>(context)));
      }) {}

}  // namespace initializer_internal

}  // namespace riegeli

#endif  // RIEGELI_BASE_INITIALIZER_H_
