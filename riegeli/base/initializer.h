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

#include <memory>
#include <new>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/meta/type_traits.h"
#include "riegeli/base/initializer_internal.h"
#include "riegeli/base/invoker.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/reset.h"
#include "riegeli/base/temporary_storage.h"
#include "riegeli/base/type_erased_ref.h"
#include "riegeli/base/type_traits.h"

namespace riegeli {

template <typename T, bool allow_explicit>
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
struct IsInitializer<T, Initializer<T, allow_explicit>> : std::true_type {};

template <typename T, typename Enable = void>
struct HasClassSpecificOperatorNew : std::false_type {};

template <typename T>
struct HasClassSpecificOperatorNew<
    T, std::enable_if_t<std::is_convertible<
           decltype(T::operator new(std::declval<size_t>())), void*>::value>>
    : std::true_type {};

#if __cpp_aligned_new

template <typename T, typename Enable = void>
struct HasClassSpecificAlignedOperatorNew : std::false_type {};

template <typename T>
struct HasClassSpecificAlignedOperatorNew<
    T, std::enable_if_t<std::is_convertible<
           decltype(T::operator new(std::declval<size_t>(),
                                    std::declval<std::align_val_t>())),
           void*>::value>> : std::true_type {};

template <typename T,
          std::enable_if_t<
              absl::conjunction<
                  std::integral_constant<
                      bool, (alignof(T) > __STDCPP_DEFAULT_NEW_ALIGNMENT__)>,
                  HasClassSpecificAlignedOperatorNew<T>>::value,
              int> = 0>
inline void* Allocate() {
  return T::operator new(sizeof(T), std::align_val_t{alignof(T)});
}

#endif  // __cpp_aligned_new

template <
    typename T,
    std::enable_if_t<
        absl::conjunction<
#if __cpp_aligned_new
            absl::disjunction<
                std::integral_constant<
                    bool, (alignof(T) <= __STDCPP_DEFAULT_NEW_ALIGNMENT__)>,
                absl::negation<HasClassSpecificAlignedOperatorNew<T>>>,
#endif  // __cpp_aligned_new
            HasClassSpecificOperatorNew<T>>::value,
        int> = 0>
inline void* Allocate() {
  return T::operator new(sizeof(T));
}

#if __cpp_aligned_new
template <typename T,
          std::enable_if_t<
              absl::conjunction<
                  std::integral_constant<
                      bool, (alignof(T) > __STDCPP_DEFAULT_NEW_ALIGNMENT__)>,
                  absl::negation<HasClassSpecificAlignedOperatorNew<T>>,
                  absl::negation<HasClassSpecificOperatorNew<T>>>::value,
              int> = 0>
inline void* Allocate() {
  return operator new(sizeof(T), std::align_val_t{alignof(T)});
}
#endif  // __cpp_aligned_new

template <typename T,
          std::enable_if_t<
              absl::conjunction<
#if __cpp_aligned_new
                  std::integral_constant<
                      bool, (alignof(T) <= __STDCPP_DEFAULT_NEW_ALIGNMENT__)>,
                  absl::negation<HasClassSpecificAlignedOperatorNew<T>>,
#else   // !__cpp_aligned_new
                  std::integral_constant<bool,
                                         (alignof(T) <= alignof(max_align_t))>,
#endif  // !__cpp_aligned_new
                  absl::negation<HasClassSpecificOperatorNew<T>>>::value,
              int> = 0>
inline void* Allocate() {
  return operator new(sizeof(T));
}

// Part of `Initializer<T>` for `T` being a non-reference type.
//
// Since C++17 which guarantees copy elision, this includes conversion to `T`.
// Otherwise that conversion requires `T` to be move-constructible and is
// included in a separate class `InitializerMovableBase`.
template <typename T>
class InitializerBase {
 public:
#if __cpp_guaranteed_copy_elision
  // Constructs the `T`.
  /*implicit*/ operator T() && { return methods()->construct(context()); }
#endif

  // Constructs the `T` on the heap.
  std::unique_ptr<T> MakeUnique() && {
    void* const ptr = Allocate<T>();
    std::move(*this).ConstructAt(ptr);
    return std::unique_ptr<T>(
#if __cpp_lib_launder >= 201606
        std::launder
#endif
        (static_cast<T*>(ptr)));
  }

  // Constructs the `T` at `ptr` using placement `new`.
  void ConstructAt(void* ptr) && {
#if __cpp_guaranteed_copy_elision
    new (ptr) T(methods()->construct(context()));
#else
    methods()->construct_at(context(), ptr);
#endif
  }

  // Constructs the `T` in `storage` which must outlive the returned reference,
  // or returns a reference to an already constructed object if a compatible
  // object was passed to `Initializer` constructor.
  //
  // `Reference()` instead of conversion to `T` can avoid moving the object if
  // the caller does not need to store the object, or if it will be moved later
  // because the target location for the object is not ready yet.
  //
  // `storage` must outlive usages of the returned reference.
  T&& Reference(TemporaryStorage<T>&& storage ABSL_ATTRIBUTE_LIFETIME_BOUND =
                    TemporaryStorage<T>()) &&
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return methods()->reference(context(), std::move(storage));
  }

 private:
#if __cpp_guaranteed_copy_elision
  static T ConstructMethodDefault(TypeErasedRef context);

  template <typename Arg>
  static T ConstructMethodFromObject(TypeErasedRef context);

  template <typename... Args>
  static T ConstructMethodFromMaker(TypeErasedRef context);

  template <typename... Args>
  static T ConstructMethodFromConstMaker(TypeErasedRef context);
#else   // !__cpp_guaranteed_copy_elision
  static void ConstructAtMethodDefault(TypeErasedRef context, void* ptr);

  template <typename Arg>
  static void ConstructAtMethodFromObject(TypeErasedRef context, void* ptr);

  template <typename... Args>
  static void ConstructAtMethodFromMaker(TypeErasedRef context, void* ptr);

  template <typename... Args>
  static void ConstructAtMethodFromConstMaker(TypeErasedRef context, void* ptr);
#endif  // !__cpp_guaranteed_copy_elision

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

 protected:
  struct Methods {
#if __cpp_guaranteed_copy_elision
    T (*construct)(TypeErasedRef context);
#else
    void (*construct_at)(TypeErasedRef context, void* ptr);
#endif
    T && (*reference)(TypeErasedRef context, TemporaryStorage<T>&& storage);
  };

  explicit InitializerBase(const Methods* methods);

  template <typename Arg>
  explicit InitializerBase(const Methods* methods, Arg&& arg);

  InitializerBase(InitializerBase&& that) = default;
  InitializerBase& operator=(InitializerBase&&) = delete;

  template <typename Dummy = void>
  static constexpr Methods kMethodsDefault = {
#if __cpp_guaranteed_copy_elision
      ConstructMethodDefault,
#else
      ConstructAtMethodDefault,
#endif
      ReferenceMethodDefault};

  template <typename Arg>
  static constexpr Methods kMethodsFromObject = {
#if __cpp_guaranteed_copy_elision
      ConstructMethodFromObject<Arg>,
#else
      ConstructAtMethodFromObject<Arg>,
#endif
      ReferenceMethodFromObject<Arg>};

  template <typename... Args>
  static constexpr Methods kMethodsFromMaker = {
#if __cpp_guaranteed_copy_elision
      ConstructMethodFromMaker<Args...>,
#else
      ConstructAtMethodFromMaker<Args...>,
#endif
      ReferenceMethodFromMaker<Args...>};

  template <typename... Args>
  static constexpr Methods kMethodsFromConstMaker = {
#if __cpp_guaranteed_copy_elision
      ConstructMethodFromConstMaker<Args...>,
#else
      ConstructAtMethodFromConstMaker<Args...>,
#endif
      ReferenceMethodFromConstMaker<Args...>};

  const Methods* methods() const { return methods_; }
  TypeErasedRef context() const { return context_; }

 private:
  const Methods* methods_;
  TypeErasedRef context_;
};

// Part of `Initializer<T>` for `T` being a move-constructible non-reference
// type.
//
// Since C++17 which guarantees copy elision, this functionality does not
// require `T` to be move-constructible and is included in `InitializerBase`.
template <typename T>
class InitializerMovableBase : public InitializerBase<T> {
#if __cpp_guaranteed_copy_elision
 protected:
  using typename InitializerMovableBase::InitializerBase::Methods;
#else  // !__cpp_guaranteed_copy_elision
 public:
  // Constructs the `T`.
  /*implicit*/ operator T() && { return methods()->construct(this->context()); }

 private:
  static T ConstructMethodDefault(TypeErasedRef context);

  template <typename Arg>
  static T ConstructMethodFromObject(TypeErasedRef context);

  template <typename... Args>
  static T ConstructMethodFromMaker(TypeErasedRef context);

  template <typename... Args>
  static T ConstructMethodFromConstMaker(TypeErasedRef context);

 protected:
  struct Methods : InitializerMovableBase::InitializerBase::Methods {
    T (*construct)(TypeErasedRef context);
  };

#if __cpp_aggregate_bases
  template <typename Dummy = void>
  static constexpr Methods kMethodsDefault = {
      InitializerMovableBase::InitializerBase::template kMethodsDefault<>,
      ConstructMethodDefault};

  template <typename Arg>
  static constexpr Methods kMethodsFromObject = {
      InitializerMovableBase::InitializerBase::template kMethodsFromObject<Arg>,
      ConstructMethodFromObject<Arg>};

  template <typename... Args>
  static constexpr Methods kMethodsFromMaker = {
      InitializerMovableBase::InitializerBase::template kMethodsFromMaker<
          Args...>,
      ConstructMethodFromMaker<Args...>};

  template <typename... Args>
  static constexpr Methods kMethodsFromConstMaker = {
      InitializerMovableBase::InitializerBase::template kMethodsFromConstMaker<
          Args...>,
      ConstructMethodFromConstMaker<Args...>};
#else   // !__cpp_aggregate_bases
  static constexpr Methods MakeMethodsDefault() {
    Methods methods;
    static_cast<typename InitializerMovableBase::InitializerBase::Methods&>(
        methods) =
        InitializerMovableBase::InitializerBase::template kMethodsDefault<>;
    methods.construct = ConstructMethodDefault;
    return methods;
  }
  template <typename Dummy = void>
  static constexpr Methods kMethodsDefault = MakeMethodsDefault();

  template <typename Arg>
  static constexpr Methods MakeMethodsFromObject() {
    Methods methods;
    static_cast<typename InitializerMovableBase::InitializerBase::Methods&>(
        methods) =
        InitializerMovableBase::InitializerBase::template kMethodsFromObject<
            Arg>;
    methods.construct = ConstructMethodFromObject<Arg>;
    return methods;
  }
  template <typename Arg>
  static constexpr Methods kMethodsFromObject = MakeMethodsFromObject<Arg>();

  template <typename... Args>
  static constexpr Methods MakeMethodsFromMaker() {
    Methods methods;
    static_cast<typename InitializerMovableBase::InitializerBase::Methods&>(
        methods) =
        InitializerMovableBase::InitializerBase::template kMethodsFromMaker<
            Args...>;
    methods.construct = ConstructMethodFromMaker<Args...>;
    return methods;
  }
  template <typename... Args>
  static constexpr Methods kMethodsFromMaker = MakeMethodsFromMaker<Args...>();

  template <typename... Args>
  static constexpr Methods MakeMethodsFromConstMaker() {
    Methods methods;
    static_cast<typename InitializerMovableBase::InitializerBase::Methods&>(
        methods) = InitializerMovableBase::InitializerBase::
        template kMethodsFromConstMaker<Args...>;
    methods.construct = ConstructMethodFromConstMaker<Args...>;
    return methods;
  }
  template <typename... Args>
  static constexpr Methods kMethodsFromConstMaker =
      MakeMethodsFromConstMaker<Args...>();
#endif  // !__cpp_aggregate_bases
#endif  // !__cpp_guaranteed_copy_elision

  explicit InitializerMovableBase(const Methods* methods)
      : InitializerMovableBase::InitializerBase(methods) {}

  template <typename Arg>
  explicit InitializerMovableBase(const Methods* methods, Arg&& arg)
      : InitializerMovableBase::InitializerBase(methods,
                                                std::forward<Arg>(arg)) {}

  InitializerMovableBase(InitializerMovableBase&& that) = default;
  InitializerMovableBase& operator=(InitializerMovableBase&&) = delete;

#if !__cpp_guaranteed_copy_elision
  const Methods* methods() const {
    return static_cast<const Methods*>(
        InitializerMovableBase::InitializerBase::methods());
  }
#endif  // !__cpp_guaranteed_copy_elision
};

// Part of `Initializer<T>` for `T` being a move-assignable non-reference type.
template <typename T>
class InitializerAssignableBase : public InitializerMovableBase<T> {
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

 protected:
  struct Methods : InitializerAssignableBase::InitializerMovableBase::Methods {
    void (*reset)(TypeErasedRef context, T& dest);
  };

#if __cpp_aggregate_bases
  template <typename Dummy = void>
  static constexpr Methods kMethodsDefault = {
      InitializerAssignableBase::InitializerMovableBase::
          template kMethodsDefault<>,
      ResetMethodDefault};

  template <typename Arg>
  static constexpr Methods kMethodsFromObject = {
      InitializerAssignableBase::InitializerMovableBase::
          template kMethodsFromObject<Arg>,
      ResetMethodFromObject<Arg>};

  template <typename... Args>
  static constexpr Methods kMethodsFromMaker = {
      InitializerAssignableBase::InitializerMovableBase::
          template kMethodsFromMaker<Args...>,
      ResetMethodFromMaker<Args...>};

  template <typename... Args>
  static constexpr Methods kMethodsFromConstMaker = {
      InitializerAssignableBase::InitializerMovableBase::
          template kMethodsFromConstMaker<Args...>,
      ResetMethodFromConstMaker<Args...>};
#else   // !__cpp_aggregate_bases
  static constexpr Methods MakeMethodsDefault() {
    Methods methods;
    static_cast<
        typename InitializerAssignableBase::InitializerMovableBase::Methods&>(
        methods) = InitializerAssignableBase::InitializerMovableBase::
        template kMethodsDefault<>;
    methods.reset = ResetMethodDefault;
    return methods;
  }
  template <typename Dummy = void>
  static constexpr Methods kMethodsDefault = MakeMethodsDefault();

  template <typename Arg>
  static constexpr Methods MakeMethodsFromObject() {
    Methods methods;
    static_cast<
        typename InitializerAssignableBase::InitializerMovableBase::Methods&>(
        methods) = InitializerAssignableBase::InitializerMovableBase::
        template kMethodsFromObject<Arg>;
    methods.reset = ResetMethodFromObject<Arg>;
    return methods;
  }
  template <typename Arg>
  static constexpr Methods kMethodsFromObject = MakeMethodsFromObject<Arg>();

  template <typename... Args>
  static constexpr Methods MakeMethodsFromMaker() {
    Methods methods;
    static_cast<
        typename InitializerAssignableBase::InitializerMovableBase::Methods&>(
        methods) = InitializerAssignableBase::InitializerMovableBase::
        template kMethodsFromMaker<Args...>;
    methods.reset = ResetMethodFromMaker<Args...>;
    return methods;
  }
  template <typename... Args>
  static constexpr Methods kMethodsFromMaker = MakeMethodsFromMaker<Args...>();

  template <typename... Args>
  static constexpr Methods MakeMethodsFromConstMaker() {
    Methods methods;
    static_cast<
        typename InitializerAssignableBase::InitializerMovableBase::Methods&>(
        methods) = InitializerAssignableBase::InitializerMovableBase::
        template kMethodsFromConstMaker<Args...>;
    methods.reset = ResetMethodFromConstMaker<Args...>;
    return methods;
  }
  template <typename... Args>
  static constexpr Methods kMethodsFromConstMaker =
      MakeMethodsFromConstMaker<Args...>();
#endif  // !__cpp_aggregate_bases

  explicit InitializerAssignableBase(const Methods* methods)
      : InitializerAssignableBase::InitializerMovableBase(methods) {}

  template <typename Arg>
  explicit InitializerAssignableBase(const Methods* methods, Arg&& arg)
      : InitializerAssignableBase::InitializerMovableBase(
            methods, std::forward<Arg>(arg)) {}

  InitializerAssignableBase(InitializerAssignableBase&& that) = default;
  InitializerAssignableBase& operator=(InitializerAssignableBase&&) = delete;

  const Methods* methods() const {
    return static_cast<const Methods*>(
        InitializerAssignableBase::InitializerMovableBase::methods());
  }
};

// Part of `Initializer<T>` for `T` being a reference type.
template <typename T>
class InitializerReference {
 public:
  // Constructs the `T`.
  /*implicit*/ operator T() && { return methods()->construct(context()); }

  // `Reference()` can be defined in terms of conversion to `T` because
  // reference storage is never used for reference types.
  //
  // `Initializer<const T&>::Reference()` returns `const T&`.
  //
  // Unused `storage` parameter makes the signature compatible with
  // the non-reference specialization.
  T&& Reference() && ABSL_ATTRIBUTE_LIFETIME_BOUND {
    // `T` is a reference type here, so `T&&` is the same as `T`.
    return std::move(*this);
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

  const Methods* methods() const { return methods_; }
  TypeErasedRef context() const { return context_; }

 private:
  const Methods* methods_;
  TypeErasedRef context_;
};

template <typename T, typename Enable = void>
struct InitializerImpl;

template <typename T>
struct InitializerImpl<
    T, std::enable_if_t<absl::conjunction<
           absl::negation<std::is_reference<T>>,
           absl::negation<std::is_convertible<T&&, T>>>::value>> {
  using type = InitializerBase<T>;
};

template <typename T>
struct InitializerImpl<
    T, std::enable_if_t<absl::conjunction<
           absl::negation<std::is_reference<T>>, std::is_convertible<T&&, T>,
           absl::negation<std::is_move_assignable<T>>>::value>> {
  using type = InitializerMovableBase<T>;
};

template <typename T>
struct InitializerImpl<
    T, std::enable_if_t<absl::conjunction<absl::negation<std::is_reference<T>>,
                                          std::is_convertible<T&&, T>,
                                          std::is_move_assignable<T>>::value>> {
  using type = InitializerAssignableBase<T>;
};

template <typename T>
struct InitializerImpl<T, std::enable_if_t<std::is_reference<T>::value>> {
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

template <typename T, bool allow_explicit = false>
class Initializer : public initializer_internal::InitializerImpl<T>::type {
 private:
  using Base = typename initializer_internal::InitializerImpl<T>::type;

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
  Initializer() : Base(&Base::template kMethodsDefault<>) {}

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
      : Base(&Base::template kMethodsFromObject<Arg>, std::forward<Arg>(arg)) {}

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
      : Base(&Base::template kMethodsFromMaker<Args...>, std::move(args)) {}
  template <typename... Args,
            std::enable_if_t<std::is_constructible<T, const Args&...>::value,
                             int> = 0>
  /*implicit*/ Initializer(
      const MakerType<Args...>& args ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Base(&Base::template kMethodsFromConstMaker<Args...>, args) {}

  // Constructs `Initializer<T>` from constructor arguments for `T` packed in
  // `riegeli::Maker<T>(args...)`.
  template <
      typename... Args,
      std::enable_if_t<std::is_constructible<T, Args&&...>::value, int> = 0>
  /*implicit*/ Initializer(
      MakerTypeFor<T, Args...>&& args ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Base(&Base::template kMethodsFromMaker<Args...>,
             std::move(args).maker()) {}
  // Constructs `Initializer<T>` from constructor arguments for `T` packed in
  // `riegeli::Maker<T>(args...)`.
  template <typename... Args,
            std::enable_if_t<std::is_constructible<T, const Args&...>::value,
                             int> = 0>
  /*implicit*/ Initializer(
      const MakerTypeFor<T, Args...>& args ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Base(&Base::template kMethodsFromConstMaker<Args...>, args.maker()) {}

  template <typename Function, typename... Args,
            std::enable_if_t<
                initializer_internal::IsCompatibleResult<
                    allow_explicit, T,
                    typename InvokerType<Function, Args...>::Result>::value,
                int> = 0>
  /*implicit*/ Initializer(
      InvokerType<Function, Args...>&& invoker ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Base(&Base::template kMethodsFromObject<InvokerType<Function, Args...>>,
             std::move(invoker)) {}
  template <
      typename Function, typename... Args,
      std::enable_if_t<
          initializer_internal::IsCompatibleResult<
              allow_explicit, T,
              typename InvokerType<Function, Args...>::ConstResult>::value,
          int> = 0>
  /*implicit*/ Initializer(const InvokerType<Function, Args...>& invoker
                               ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Base(&Base::template kMethodsFromObject<
                 const InvokerType<Function, Args...>&>,
             invoker) {}

  // Constructs `Initializer<T>` from `Initializer<T>` with a different
  // `allow_explicit` by adopting its state instead of wrapping.
  template <bool other_allow_explicit>
  /*implicit*/ Initializer(Initializer<T, other_allow_explicit> initializer)
      : Base(std::move(initializer)) {}

  Initializer(Initializer&& that) = default;
  Initializer& operator=(Initializer&&) = delete;
};

// `InitializerTarget<T>::type` and `InitializerTargetT<T>` deduce the
// appropriate target type such that `T` is convertible to
// `Initializer<InitializerTargetT<T>>`.
//
// This allows a single template to uniformly handle a `Target` passed directly,
// as `riegeli::Maker<Target>(args...)`,
// as `riegeli::Invoker(function, args...)`, or as `Initializer<Target>`.
// This is also useful for CTAD guides to deduce a template argument as
// `InitializerTargetT<T>`.
//
// They are undefined in the case of `riegeli::Maker(args...)` which requires
// the target type to be specified by the caller, or when the object is not
// usable in the given const and reference context.

namespace initializer_internal {

template <typename Value, typename Reference>
struct InitializerTargetImpl : std::decay<Value> {};

template <typename... Args, typename Reference>
struct InitializerTargetImpl<MakerType<Args...>, Reference> {
  // No `type` member when the target type is unspecified.
};

template <typename Target, typename... Args, typename Reference>
struct InitializerTargetImpl<MakerTypeFor<Target, Args...>, Reference>
    : MakerTarget<Reference> {};

template <typename Function, typename... Args, typename Reference>
struct InitializerTargetImpl<InvokerType<Function, Args...>, Reference>
    : InvokerTarget<Reference> {};

template <typename T, bool allow_explicit, typename Reference>
struct InitializerTargetImpl<Initializer<T, allow_explicit>, Reference> {
  using type = T;
};

};  // namespace initializer_internal

template <typename T>
struct InitializerTarget
    : initializer_internal::InitializerTargetImpl<std::decay_t<T>, T&&> {};

template <typename T>
using InitializerTargetT = typename InitializerTarget<T>::type;

// `InitializerTargetRef<T>::type` and `InitializerTargetRefT<T>` are like
// `InitializerTargetT<T>`, but if the object is already constructed, then they
// are the corresponding reference type instead of the value type. It is still
// true that `T` is convertible to `Initializer<InitializerTargetRefT<T>>`.
//
// This allows to avoid moving or copying the object if a reference to it is
// sufficient.

namespace initializer_internal {

template <typename Value, typename Reference>
struct InitializerTargetRefImpl {
  using type = Reference;
};

template <typename... Args, typename Reference>
struct InitializerTargetRefImpl<MakerType<Args...>, Reference> {
  // No `type` member when the target type is unspecified.
};

template <typename Target, typename... Args, typename Reference>
struct InitializerTargetRefImpl<MakerTypeFor<Target, Args...>, Reference>
    : MakerTarget<Reference> {};

template <typename Function, typename... Args, typename Reference>
struct InitializerTargetRefImpl<InvokerType<Function, Args...>, Reference>
    : InvokerTargetRef<Reference> {};

template <typename T, bool allow_explicit, typename Reference>
struct InitializerTargetRefImpl<Initializer<T, allow_explicit>, Reference> {
  using type = T;
};

};  // namespace initializer_internal

template <typename T>
struct InitializerTargetRef
    : initializer_internal::InitializerTargetRefImpl<std::decay_t<T>, T&&> {};

template <typename T>
using InitializerTargetRefT = typename InitializerTargetRef<T>::type;

// Implementation details follow.

namespace initializer_internal {

template <typename T>
inline InitializerBase<T>::InitializerBase(const Methods* methods)
    : methods_(methods) {}

template <typename T>
template <typename Arg>
inline InitializerBase<T>::InitializerBase(const Methods* methods, Arg&& arg)
    : methods_(methods), context_(std::forward<Arg>(arg)) {}

#if __cpp_guaranteed_copy_elision

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

#else  // !__cpp_guaranteed_copy_elision

template <typename T>
void InitializerBase<T>::ConstructAtMethodDefault(
    ABSL_ATTRIBUTE_UNUSED TypeErasedRef context, void* ptr) {
  new (ptr) T();
}

template <typename T>
template <typename Arg>
void InitializerBase<T>::ConstructAtMethodFromObject(TypeErasedRef context,
                                                     void* ptr) {
  new (ptr) T(context.Cast<Arg>());
}

template <typename T>
template <typename... Args>
void InitializerBase<T>::ConstructAtMethodFromMaker(TypeErasedRef context,
                                                    void* ptr) {
  context.Cast<MakerType<Args...>>().template ConstructAt<T>(ptr);
}

template <typename T>
template <typename... Args>
void InitializerBase<T>::ConstructAtMethodFromConstMaker(TypeErasedRef context,
                                                         void* ptr) {
  context.Cast<const MakerType<Args...>&>().template ConstructAt<T>(ptr);
}

#endif  // !__cpp_guaranteed_copy_elision

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

#if !__cpp_guaranteed_copy_elision

template <typename T>
T InitializerMovableBase<T>::ConstructMethodDefault(
    ABSL_ATTRIBUTE_UNUSED TypeErasedRef context) {
  return T();
}

template <typename T>
template <typename Arg>
T InitializerMovableBase<T>::ConstructMethodFromObject(TypeErasedRef context) {
  return T(context.Cast<Arg>());
}

template <typename T>
template <typename... Args>
T InitializerMovableBase<T>::ConstructMethodFromMaker(TypeErasedRef context) {
  return context.Cast<MakerType<Args...>>().template Construct<T>();
}

template <typename T>
template <typename... Args>
T InitializerMovableBase<T>::ConstructMethodFromConstMaker(
    TypeErasedRef context) {
  return context.Cast<const MakerType<Args...>&>().template Construct<T>();
}

#endif  // !__cpp_guaranteed_copy_elision

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

}  // namespace initializer_internal

}  // namespace riegeli

#endif  // RIEGELI_BASE_INITIALIZER_H_
