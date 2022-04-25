// Copyright 2022 Google LLC
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

#ifndef RIEGELI_BASE_ANY_DEPENDENCY_H_
#define RIEGELI_BASE_ANY_DEPENDENCY_H_

#include <stddef.h>

#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/meta/type_traits.h"
#include "riegeli/base/base.h"
#include "riegeli/base/dependency.h"

namespace riegeli {

template <typename Ptr, size_t inline_size, size_t inline_align>
class AnyDependencyImpl;
template <typename Ptr, size_t inline_size, size_t inline_align>
class AnyDependencyRefImpl;

// `AnyDependency<Ptr>` refers to an optionally owned object which is accessed
// as `Ptr` and stored as some `Manager` type decided when the `AnyDependency`
// is initialized.
//
// Often `Ptr` is some pointer `P*`, and then `Manager` can be e.g.
// `M*` (not owned), `M` (owned), or `std::unique_ptr<M>` (owned), with `M`
// derived from `P`.
//
// `AnyDependency<Ptr>` holds a `Dependency<Ptr, Manager>` for some `Manager`
// type, erasing the `Manager` parameter from the type of the `AnyDependency`,
// or is empty.
//
// The optional `InlineManagers` parameters specify the size of inline storage,
// which allows to avoid heap allocation if `Manager` is among `InlineManagers`
// or if `Dependency<Ptr, Manager>` fits there regarding size and alignment.
// By default inline storage is enough for a pointer or a `Ptr`.
template <typename Ptr, typename... InlineManagers>
using AnyDependency = AnyDependencyImpl<
    Ptr, UnsignedMax(size_t{0}, sizeof(Dependency<Ptr, InlineManagers>)...),
    UnsignedMax(size_t{0}, alignof(Dependency<Ptr, InlineManagers>)...)>;

// `AnyDependencyRef<Ptr>` refers to an optionally owned object which is
// accessed as `Ptr` and was passed as some `Manager` type decided when the
// `AnyDependencyRef` was initialized.
//
// Often `Ptr` is some pointer `P*`, and then `Manager` can be e.g.
// `M&` (not owned), `M&&` (owned), or `std::unique_ptr<M>` (owned), with `M`
// derived from `P`.
//
// `AnyDependencyRef<Ptr>` derives from `AnyDependency<Ptr>`, replacing the
// constructors such that the `Manager` type is deduced from the constructor
// argument as a reference type rather than a value type.
//
// This is meant to be used only when the dependency is a function parameter
// rather than stored in a host object, because such a dependency stores a
// reference to the dependent object, and by convention a reference argument is
// expected to be valid only for the duration of the function call.
//
// This allows to pass an unowned dependency by lvalue reference instead of by
// pointer, which allows for a more idiomatic API for passing an object which
// does not need to be valid after the function returns. And this allows to pass
// an owned dependency by rvalue reference instead of by value, which avoids
// moving it.
//
// Only a subset of operations are provided: the dependency must be initialized,
// assignment is not supported, and initialization from a tuple of constructor
// arguments is not supported.
//
// In contrast to `AnyDependency`, for `AnyDependencyRef` it is rare that
// specifying `InlineManagers` is useful, because a typical
// `Dependency<Ptr, Manager&&>` deduced by `AnyDependencyRef` fits in the
// default inline storage.
template <typename Ptr, typename... InlineManagers>
using AnyDependencyRef = AnyDependencyRefImpl<
    Ptr, UnsignedMax(size_t{0}, sizeof(Dependency<Ptr, InlineManagers>)...),
    UnsignedMax(size_t{0}, alignof(Dependency<Ptr, InlineManagers>)...)>;

// `AnyDependencyTraits<Ptr>` can be specialized to override the default value
// of `Ptr` used by `AnyDependency<Ptr>`.
template <typename Ptr>
struct AnyDependencyTraits {
  static Ptr DefaultPtr() { return Ptr(); }
};

// Specialization of `AnyDependencyTraits<int>`, used for file descriptors.
template <>
struct AnyDependencyTraits<int> {
  static int DefaultPtr() { return -1; }
};

namespace any_dependency_internal {

// Variants of `Repr`:
//  * Empty `AnyDependency`: `Repr` is not used
//  * Held by pointer: `storage` holds `Dependency<Ptr, Manager>*`
//  * Stored inline: `storage` holds `Dependency<Ptr, Manager>`
template <typename Ptr, size_t inline_size, size_t inline_align>
struct Repr {
  alignas(UnsignedMax(
      alignof(void*), alignof(Ptr),
      inline_align)) char storage[UnsignedMax(sizeof(void*), sizeof(Ptr),
                                              inline_size)];
};

// By convention, a parameter of type `Storage` points to
// `Repr<Ptr, inline_size, inline_align>::storage`.
using Storage = char[];

// A `Dependency<Ptr, Manager>` is stored inline in
// `Repr<Ptr, inline_size, inline_align>` if it fits in that storage.
// If `inline_size == 0`, the dependency is also required to be stable.

template <typename Ptr, size_t inline_size, size_t inline_align,
          typename Manager, typename Enable = void>
struct IsInline : std::false_type {};

template <typename Ptr, size_t inline_size, size_t inline_align,
          typename Manager>
struct IsInline<
    Ptr, inline_size, inline_align, Manager,
    std::enable_if_t<
        sizeof(Dependency<Ptr, Manager>) <=
            sizeof(Repr<Ptr, inline_size, inline_align>::storage) &&
        alignof(Dependency<Ptr, Manager>) <=
            alignof(Repr<Ptr, inline_size, inline_align>::storage) &&
        (inline_size > 0 || Dependency<Ptr, Manager>::kIsStable)>>
    : std::true_type {};

// Method pointers.
template <typename Ptr>
struct Methods {
  // Constructs `self` by moving from `that`, and destroys `that`.
  void (*move)(Storage self, Storage that);
  // Destroys `self`.
  void (*destroy)(Storage self);
  Ptr (*get)(const Storage self);
  Ptr (*release)(Storage self);
  bool (*is_owning)(const Storage self);
};

template <typename Ptr>
struct NullMethods;

template <typename Ptr, size_t inline_size, size_t inline_align,
          typename Manager, typename Enable = void>
struct MethodsFor;

// `IsCompatibleAnyDependency` detects `AnyDependencyImpl` or
// `AnyDependencyRefImpl` type with the given `Ptr` and the given or smaller
// `inline_size` and `inline_align`.

template <typename Ptr, size_t inline_size, size_t inline_align, typename T,
          typename Enable = void>
struct IsCompatibleAnyDependency : std::false_type {};

template <typename Ptr, size_t inline_size, size_t inline_align,
          size_t that_inline_size, size_t that_inline_align>
struct IsCompatibleAnyDependency<
    Ptr, inline_size, inline_align,
    AnyDependencyImpl<Ptr, that_inline_size, that_inline_align>,
    std::enable_if_t<
        sizeof(Repr<Ptr, that_inline_size, that_inline_align>::storage) <=
            sizeof(Repr<Ptr, inline_size, inline_align>::storage) &&
        alignof(Repr<Ptr, that_inline_size, that_inline_align>::storage) <=
            alignof(Repr<Ptr, inline_size, inline_align>::storage)>>
    : std::true_type {};

template <typename Ptr, size_t inline_size, size_t inline_align,
          size_t that_inline_size, size_t that_inline_align>
struct IsCompatibleAnyDependency<
    Ptr, inline_size, inline_align,
    AnyDependencyRefImpl<Ptr, that_inline_size, that_inline_align>,
    std::enable_if_t<
        sizeof(Repr<Ptr, that_inline_size, that_inline_align>::storage) <=
            sizeof(Repr<Ptr, inline_size, inline_align>::storage) &&
        alignof(Repr<Ptr, that_inline_size, that_inline_align>::storage) <=
            alignof(Repr<Ptr, inline_size, inline_align>::storage)>>
    : std::true_type {};

}  // namespace any_dependency_internal

// `AnyDependencyImpl` implements `AnyDependency` after `InlineManagers` have
// been reduced to their maximum size and alignment.
template <typename Ptr, size_t inline_size, size_t inline_align = 0>
class AnyDependencyImpl {
 public:
  // Creates an empty `AnyDependencyImpl`.
  AnyDependencyImpl() noexcept;

  // Holds a `Dependency<Ptr, std::decay_t<Manager>>`.
  //
  // The `Manager` type is deduced from the constructor argument.
  template <typename Manager,
            std::enable_if_t<
                absl::conjunction<
                    absl::negation<
                        std::is_same<std::decay_t<Manager>, AnyDependencyImpl>>,
                    IsValidDependency<Ptr, std::decay_t<Manager>>>::value,
                int> = 0>
  /*implicit*/ AnyDependencyImpl(Manager&& manager);
  template <typename Manager,
            std::enable_if_t<
                absl::conjunction<
                    absl::negation<
                        std::is_same<std::decay_t<Manager>, AnyDependencyImpl>>,
                    IsValidDependency<Ptr, std::decay_t<Manager>>>::value,
                int> = 0>
  AnyDependencyImpl& operator=(Manager&& manager);

  // Holds a `Dependency<Ptr, Manager>`.
  //
  // The `Manager` type is specified with a tag (`absl::in_place_type<Manager>`)
  // because constructor templates do not support specifying template arguments
  // explicitly.
  //
  // The dependency is constructed with `std::forward<ManagerArg>(manager_arg)`,
  // which can be a `Manager` to copy or move, or a tuple of its constructor
  // arguments.
  template <typename Manager, typename ManagerArg>
  explicit AnyDependencyImpl(absl::in_place_type_t<Manager>,
                             ManagerArg&& manager_arg);

  AnyDependencyImpl(AnyDependencyImpl&& that) noexcept;
  AnyDependencyImpl& operator=(AnyDependencyImpl&& that) noexcept;

  ~AnyDependencyImpl();

  // Makes `*this` equivalent to a newly constructed `AnyDependencyImpl`. This
  // avoids constructing a temporary `AnyDependencyImpl` and moving from it.
  void Reset();
  template <typename Manager>
  void Reset(Manager&& manager);
  template <typename Manager, typename ManagerArg>
  void Reset(absl::in_place_type_t<Manager>, ManagerArg&& manager_arg);

  // Returns a `Ptr` to the `Manager`, or a default `Ptr` for an empty
  // `AnyDependencyImpl`.
  //
  // A caveat regarding const:
  //
  // This `get()` is a const method, even though it does not require the
  // `Dependency<Ptr, Manager>::get()` to be const.
  //
  // This is because there are two variants of `Dependency<P*, Manager>`
  // specializations. `Dependency<P*, P>` stores `P` by value and thus
  // provides `P* get()` and `const P* get() const`, while some users of
  // `Dependency<P*, Manager>` do not support a `Manager` storing `P` by value
  // anyway and expect `P* get() const` to be available.
  //
  // To avoid having two variants of `AnyDependencyImpl<P*>` based on this
  // subtle distinction, its only variant is more permissive regarding the
  // `Dependency` while also more permissive regarding its usage.
  Ptr get() const { return methods_->get(repr_.storage); }

  // If `Ptr` is `P*`, `AnyDependencyImpl<P*>` can be used as a smart pointer to
  // `P`, for convenience.
  template <typename DependentPtr = Ptr,
            std::enable_if_t<std::is_pointer<DependentPtr>::value, int> = 0>
  std::remove_pointer_t<Ptr>& operator*() const {
    const Ptr ptr = get();
    RIEGELI_ASSERT(ptr != nullptr)
        << "Failed precondition of AnyDependency::operator*: null pointer";
    return *ptr;
  }
  template <typename DependentPtr = Ptr,
            std::enable_if_t<std::is_pointer<DependentPtr>::value, int> = 0>
  Ptr operator->() const {
    const Ptr ptr = get();
    RIEGELI_ASSERT(ptr != nullptr)
        << "Failed precondition of AnyDependency::operator->: null pointer";
    return ptr;
  }

  // If the `Dependency` owns the dependent object and can release it,
  // `Release()` returns the released pointer, otherwise returns `nullptr`
  // or another sentinel `Ptr` value specified with `AnyDependencyTraits<Ptr>`.
  Ptr Release() { return methods_->release(repr_.storage); }

  // If `Ptr` is `P*`, `AnyDependencyImpl<P*>` can be compared against
  // `nullptr`.
  template <typename DependentPtr = Ptr,
            std::enable_if_t<std::is_pointer<DependentPtr>::value, int> = 0>
  friend bool operator==(const AnyDependencyImpl& a, std::nullptr_t) {
    return a.get() == nullptr;
  }
  template <typename DependentPtr = Ptr,
            std::enable_if_t<std::is_pointer<DependentPtr>::value, int> = 0>
  friend bool operator!=(const AnyDependencyImpl& a, std::nullptr_t) {
    return a.get() != nullptr;
  }
  template <typename DependentPtr = Ptr,
            std::enable_if_t<std::is_pointer<DependentPtr>::value, int> = 0>
  friend bool operator==(std::nullptr_t, const AnyDependencyImpl& b) {
    return nullptr == b.get();
  }
  template <typename DependentPtr = Ptr,
            std::enable_if_t<std::is_pointer<DependentPtr>::value, int> = 0>
  friend bool operator!=(std::nullptr_t, const AnyDependencyImpl& b) {
    return nullptr != b.get();
  }

  // If `true`, the `AnyDependencyImpl` owns the dependent object, i.e. closing
  // the host object should close the dependent object.
  bool is_owning() const { return methods_->is_owning(repr_.storage); }

  // If `true`, `get()` stays unchanged when an `AnyDependencyImpl` is moved.
  static constexpr bool kIsStable = inline_size == 0;

 private:
  // For adopting `methods_` and `repr_` from an instantiation with a different
  // `inline_size` and `inline_align.
  template <typename OtherPtr, size_t other_inline_size,
            size_t other_inline_align>
  friend class AnyDependencyImpl;

  using Repr = any_dependency_internal::Repr<Ptr, inline_size, inline_align>;
  using Methods = any_dependency_internal::Methods<Ptr>;
  using NullMethods = any_dependency_internal::NullMethods<Ptr>;
  template <typename Manager>
  using MethodsFor = any_dependency_internal::MethodsFor<Ptr, inline_size,
                                                         inline_align, Manager>;

  template <typename... Args, size_t... indices>
  explicit AnyDependencyImpl(std::tuple<Args...>&& args,
                             std::index_sequence<indices...>)
      : AnyDependencyImpl(std::forward<Args>(std::get<indices>(args))...) {}

  // Initializes `methods_` and `repr_`, avoiding a redundant indirection and
  // adopting them from `manager` instead if `Manager` is already a compatible
  // `AnyDependencyImpl` or `AnyDependencyRefImpl`.
  template <
      typename Manager,
      std::enable_if_t<!std::is_reference<Manager>::value &&
                           !any_dependency_internal::IsCompatibleAnyDependency<
                               Ptr, inline_size, inline_align, Manager>::value,
                       int> = 0>
  void Initialize(const Manager& manager);
  template <
      typename Manager,
      std::enable_if_t<!any_dependency_internal::IsCompatibleAnyDependency<
                           Ptr, inline_size, inline_align, Manager>::value,
                       int> = 0>
  void Initialize(Manager&& manager);
  template <
      typename Manager,
      std::enable_if_t<any_dependency_internal::IsCompatibleAnyDependency<
                           Ptr, inline_size, inline_align, Manager>::value,
                       int> = 0>
  void Initialize(Manager&& manager);
  template <
      typename Manager, typename... ManagerArgs,
      std::enable_if_t<!std::is_reference<Manager>::value &&
                           !any_dependency_internal::IsCompatibleAnyDependency<
                               Ptr, inline_size, inline_align, Manager>::value,
                       int> = 0>
  void Initialize(std::tuple<ManagerArgs...> manager_args);
  template <
      typename Manager, typename... ManagerArgs,
      std::enable_if_t<any_dependency_internal::IsCompatibleAnyDependency<
                           Ptr, inline_size, inline_align, Manager>::value,
                       int> = 0>
  void Initialize(std::tuple<ManagerArgs...> manager_args);

  const Methods* methods_;
  Repr repr_;
};

// Specialization of `DependencyImpl<Ptr, AnyDependencyImpl<Ptr>>`.
template <typename Ptr, size_t inline_size, size_t inline_align>
class DependencyImpl<Ptr, AnyDependencyImpl<Ptr, inline_size, inline_align>>
    : public DependencyBase<AnyDependencyImpl<Ptr, inline_size, inline_align>> {
 public:
  using DependencyBase<
      AnyDependencyImpl<Ptr, inline_size, inline_align>>::DependencyBase;

  Ptr get() const { return this->manager().get(); }
  Ptr Release() { return this->manager().Release(); }

  bool is_owning() const { return this->manager().is_owning(); }
  static constexpr bool kIsStable =
      AnyDependencyImpl<Ptr, inline_size, inline_align>::kIsStable;
};

// `AnyDependencyRefImpl` implements `AnyDependencyRef` after `InlineManagers`
// have been reduced to their maximum size and alignment.
template <typename Ptr, size_t inline_size, size_t inline_align = 0>
class AnyDependencyRefImpl
    : public AnyDependencyImpl<Ptr, inline_size, inline_align> {
 public:
  // Holds a `Dependency<Ptr, Manager&&>` (which collapses to
  // `Dependency<Ptr, Manager&>` if `Manager` is itself an lvalue reference).
  //
  // The `Manager` type is deduced from the constructor argument.
  template <
      typename Manager,
      std::enable_if_t<
          absl::conjunction<absl::negation<std::is_same<std::decay_t<Manager>,
                                                        AnyDependencyRefImpl>>,
                            IsValidDependency<Ptr, Manager&&>>::value,
          int> = 0>
  /*implicit*/ AnyDependencyRefImpl(Manager&& manager)
      : AnyDependencyImpl<Ptr, inline_size, inline_align>(
            absl::in_place_type<Manager&&>, std::forward<Manager>(manager)) {}

  AnyDependencyRefImpl(AnyDependencyRefImpl&& that) noexcept
      : AnyDependencyImpl<Ptr, inline_size, inline_align>(
            static_cast<AnyDependencyImpl<Ptr, inline_size, inline_align>&&>(
                that)) {}
  AnyDependencyRefImpl& operator=(AnyDependencyRefImpl&&) = delete;

  void Reset() = delete;
};

// Specialization of `DependencyImpl<Ptr, AnyDependencyRefImpl<Ptr>>`.
template <typename Ptr, size_t inline_size, size_t inline_align>
class DependencyImpl<Ptr, AnyDependencyRefImpl<Ptr, inline_size, inline_align>>
    : public DependencyBase<
          AnyDependencyRefImpl<Ptr, inline_size, inline_align>> {
 public:
  using DependencyBase<
      AnyDependencyRefImpl<Ptr, inline_size, inline_align>>::DependencyBase;

  Ptr get() const { return this->manager().get(); }
  Ptr Release() { return this->manager().Release(); }

  bool is_owning() const { return this->manager().is_owning(); }
  static constexpr bool kIsStable =
      AnyDependencyRefImpl<Ptr, inline_size, inline_align>::kIsStable;
};

// Implementation details follow.

namespace any_dependency_internal {

// `any_dependency_internal::Release(dep)` calls `dep.Release()` if that is
// defined, otherwise returns `AnyDependencyTraits<Ptr>::DefaultPtr()`.

template <typename T, typename Enable = void>
struct HasRelease : std::false_type {};
template <typename T>
struct HasRelease<T, absl::void_t<decltype(std::declval<T>().Release())>>
    : std::true_type {};

template <
    typename Ptr, typename Manager,
    std::enable_if_t<HasRelease<Dependency<Ptr, Manager>>::value, int> = 0>
Ptr Release(Dependency<Ptr, Manager>& dep) {
  return dep.Release();
}
template <
    typename Ptr, typename Manager,
    std::enable_if_t<!HasRelease<Dependency<Ptr, Manager>>::value, int> = 0>
Ptr Release(Dependency<Ptr, Manager>& dep) {
  return AnyDependencyTraits<Ptr>::DefaultPtr();
}

// `any_dependency_internal::IsOwning(dep)` calls `dep.is_owning()` if that is
// defined, otherwise returns `false`.

template <typename T, typename Enable = void>
struct HasIsOwning : std::false_type {};
template <typename T>
struct HasIsOwning<T,
                   absl::void_t<decltype(std::declval<const T>().is_owning())>>
    : std::true_type {};

template <
    typename Ptr, typename Manager,
    std::enable_if_t<HasIsOwning<Dependency<Ptr, Manager>>::value, int> = 0>
bool IsOwning(const Dependency<Ptr, Manager>& dep) {
  return dep.is_owning();
}
template <
    typename Ptr, typename Manager,
    std::enable_if_t<!HasIsOwning<Dependency<Ptr, Manager>>::value, int> = 0>
bool IsOwning(const Dependency<Ptr, Manager>& dep) {
  return false;
}

template <typename Ptr>
struct NullMethods {
  static const Methods<Ptr> methods;

 private:
  static void Move(Storage self, Storage that) {}
  static void Destroy(Storage self) {}
  static Ptr Get(const Storage self) {
    return AnyDependencyTraits<Ptr>::DefaultPtr();
  }
  static Ptr Release(Storage self) {
    return AnyDependencyTraits<Ptr>::DefaultPtr();
  }
  static bool IsOwning(const Storage self) { return false; }
};

template <typename Ptr>
const Methods<Ptr> NullMethods<Ptr>::methods = {Move, Destroy, Get, Release,
                                                IsOwning};

template <typename Ptr, size_t inline_size, size_t inline_align,
          typename Manager, typename Enable>
struct MethodsFor {
  static const Methods<Ptr> methods;

  template <
      typename DependentManager = Manager,
      std::enable_if_t<!std::is_reference<DependentManager>::value, int> = 0>
  static void Construct(Storage self, const Manager& manager) {
    ptr(self) = new Dependency<Ptr, Manager>(manager);
  }
  static void Construct(Storage self, Manager&& manager) {
    ptr(self) = new Dependency<Ptr, Manager>(std::forward<Manager>(manager));
  }
  template <
      typename... ManagerArgs, typename DependentManager = Manager,
      std::enable_if_t<!std::is_reference<DependentManager>::value, int> = 0>
  static void Construct(Storage self, std::tuple<ManagerArgs...> manager_args) {
    ptr(self) = new Dependency<Ptr, Manager>(std::move(manager_args));
  }

 private:
  static Dependency<Ptr, Manager>*& ptr(Storage self) {
    return *reinterpret_cast<Dependency<Ptr, Manager>**>(self);
  }
  static Dependency<Ptr, Manager>* const& ptr(const Storage self) {
    return *reinterpret_cast<Dependency<Ptr, Manager>* const*>(self);
  }

  static void Move(Storage self, Storage that) { ptr(self) = ptr(that); }
  static void Destroy(Storage self) { delete ptr(self); }
  static Ptr Get(const Storage self) { return ptr(self)->get(); }
  static Ptr Release(Storage self) {
    return any_dependency_internal::Release(*ptr(self));
  }
  static bool IsOwning(const Storage self) {
    return any_dependency_internal::IsOwning(*ptr(self));
  }
};

template <typename Ptr, size_t inline_size, size_t inline_align,
          typename Manager, typename Enable>
const Methods<Ptr>
    MethodsFor<Ptr, inline_size, inline_align, Manager, Enable>::methods = {
        Move, Destroy, Get, Release, IsOwning};

template <typename Ptr, size_t inline_size, size_t inline_align,
          typename Manager>
struct MethodsFor<Ptr, inline_size, inline_align, Manager,
                  std::enable_if_t<IsInline<Ptr, inline_size, inline_align,
                                            Manager>::value>> {
  static const Methods<Ptr> methods;

  template <
      typename DependentManager = Manager,
      std::enable_if_t<!std::is_reference<DependentManager>::value, int> = 0>
  static void Construct(Storage self, const Manager& manager) {
    new (self) Dependency<Ptr, Manager>(manager);
  }
  static void Construct(Storage self, Manager&& manager) {
    new (self) Dependency<Ptr, Manager>(std::forward<Manager>(manager));
  }
  template <
      typename... ManagerArgs, typename DependentManager = Manager,
      std::enable_if_t<!std::is_reference<DependentManager>::value, int> = 0>
  static void Construct(Storage self, std::tuple<ManagerArgs...> manager_args) {
    new (self) Dependency<Ptr, Manager>(std::move(manager_args));
  }

 private:
  static Dependency<Ptr, Manager>& dep(Storage self) {
    return *reinterpret_cast<Dependency<Ptr, Manager>*>(self);
  }
  static const Dependency<Ptr, Manager>& dep(const Storage self) {
    return *reinterpret_cast<const Dependency<Ptr, Manager>*>(self);
  }

  static void Move(Storage self, Storage that) {
    new (self) Dependency<Ptr, Manager>(std::move(dep(that)));
    dep(that).~Dependency<Ptr, Manager>();
  }
  static void Destroy(Storage self) { dep(self).~Dependency<Ptr, Manager>(); }
  static Ptr Get(const Storage self) {
    // See the caveat regarding const at `AnyDependency::get()`.
    return const_cast<Dependency<Ptr, Manager>&>(dep(self)).get();
  }
  static Ptr Release(Storage self) {
    return any_dependency_internal::Release(dep(self));
  }
  static bool IsOwning(const Storage self) {
    return any_dependency_internal::IsOwning(dep(self));
  }
};

template <typename Ptr, size_t inline_size, size_t inline_align,
          typename Manager>
const Methods<Ptr>
    MethodsFor<Ptr, inline_size, inline_align, Manager,
               std::enable_if_t<IsInline<Ptr, inline_size, inline_align,
                                         Manager>::value>>::methods = {
        Move, Destroy, Get, Release, IsOwning};

}  // namespace any_dependency_internal

template <typename Ptr, size_t inline_size, size_t inline_align>
inline AnyDependencyImpl<Ptr, inline_size,
                         inline_align>::AnyDependencyImpl() noexcept
    : methods_(&NullMethods::methods) {}

template <typename Ptr, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<
              absl::conjunction<
                  absl::negation<std::is_same<
                      std::decay_t<Manager>,
                      AnyDependencyImpl<Ptr, inline_size, inline_align>>>,
                  IsValidDependency<Ptr, std::decay_t<Manager>>>::value,
              int>>
inline AnyDependencyImpl<Ptr, inline_size, inline_align>::AnyDependencyImpl(
    Manager&& manager) {
  Initialize<std::decay_t<Manager>>(std::forward<Manager>(manager));
}

template <typename Ptr, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<
              absl::conjunction<
                  absl::negation<std::is_same<
                      std::decay_t<Manager>,
                      AnyDependencyImpl<Ptr, inline_size, inline_align>>>,
                  IsValidDependency<Ptr, std::decay_t<Manager>>>::value,
              int>>
inline AnyDependencyImpl<Ptr, inline_size, inline_align>&
AnyDependencyImpl<Ptr, inline_size, inline_align>::operator=(
    Manager&& manager) {
  methods_->destroy(repr_.storage);
  Initialize<std::decay_t<Manager>>(std::forward<Manager>(manager));
  return *this;
}

template <typename Ptr, size_t inline_size, size_t inline_align>
template <typename Manager, typename ManagerArg>
inline AnyDependencyImpl<Ptr, inline_size, inline_align>::AnyDependencyImpl(
    absl::in_place_type_t<Manager>, ManagerArg&& manager_arg) {
  Initialize<Manager>(std::forward<ManagerArg>(manager_arg));
}

template <typename Ptr, size_t inline_size, size_t inline_align>
inline AnyDependencyImpl<Ptr, inline_size, inline_align>::AnyDependencyImpl(
    AnyDependencyImpl&& that) noexcept
    : methods_(std::exchange(that.methods_, &NullMethods::methods)) {
  methods_->move(repr_.storage, that.repr_.storage);
}

template <typename Ptr, size_t inline_size, size_t inline_align>
inline AnyDependencyImpl<Ptr, inline_size, inline_align>&
AnyDependencyImpl<Ptr, inline_size, inline_align>::operator=(
    AnyDependencyImpl&& that) noexcept {
  if (ABSL_PREDICT_TRUE(&that != this)) {
    methods_->destroy(repr_.storage);
    methods_ = std::exchange(that.methods_, &NullMethods::methods);
    methods_->move(repr_.storage, that.repr_.storage);
  }
  return *this;
}

template <typename Ptr, size_t inline_size, size_t inline_align>
inline AnyDependencyImpl<Ptr, inline_size, inline_align>::~AnyDependencyImpl() {
  methods_->destroy(repr_.storage);
}

template <typename Ptr, size_t inline_size, size_t inline_align>
inline void AnyDependencyImpl<Ptr, inline_size, inline_align>::Reset() {
  methods_->destroy(repr_.storage);
  methods_ = &NullMethods::methods;
}

template <typename Ptr, size_t inline_size, size_t inline_align>
template <typename Manager>
inline void AnyDependencyImpl<Ptr, inline_size, inline_align>::Reset(
    Manager&& manager) {
  methods_->destroy(repr_.storage);
  Initialize<std::decay_t<Manager>>(std::forward<Manager>(manager));
}

template <typename Ptr, size_t inline_size, size_t inline_align>
template <typename Manager, typename ManagerArg>
inline void AnyDependencyImpl<Ptr, inline_size, inline_align>::Reset(
    absl::in_place_type_t<Manager>, ManagerArg&& manager_arg) {
  methods_->destroy(repr_.storage);
  Initialize<Manager>(std::forward<ManagerArg>(manager_arg));
}

template <typename Ptr, size_t inline_size, size_t inline_align>
template <
    typename Manager,
    std::enable_if_t<!std::is_reference<Manager>::value &&
                         !any_dependency_internal::IsCompatibleAnyDependency<
                             Ptr, inline_size, inline_align, Manager>::value,
                     int>>
inline void AnyDependencyImpl<Ptr, inline_size, inline_align>::Initialize(
    const Manager& manager) {
  methods_ = &MethodsFor<Manager>::methods;
  MethodsFor<Manager>::Construct(repr_.storage, manager);
}

template <typename Ptr, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<!any_dependency_internal::IsCompatibleAnyDependency<
                               Ptr, inline_size, inline_align, Manager>::value,
                           int>>
inline void AnyDependencyImpl<Ptr, inline_size, inline_align>::Initialize(
    Manager&& manager) {
  methods_ = &MethodsFor<Manager>::methods;
  MethodsFor<Manager>::Construct(repr_.storage, std::forward<Manager>(manager));
}

template <typename Ptr, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<any_dependency_internal::IsCompatibleAnyDependency<
                               Ptr, inline_size, inline_align, Manager>::value,
                           int>>
inline void AnyDependencyImpl<Ptr, inline_size, inline_align>::Initialize(
    Manager&& manager) {
  // Adopt `manager` instead of wrapping it.
  methods_ = std::exchange(manager.methods_, &NullMethods::methods);
  methods_->move(repr_.storage, manager.repr_.storage);
}

template <typename Ptr, size_t inline_size, size_t inline_align>
template <
    typename Manager, typename... ManagerArgs,
    std::enable_if_t<!std::is_reference<Manager>::value &&
                         !any_dependency_internal::IsCompatibleAnyDependency<
                             Ptr, inline_size, inline_align, Manager>::value,
                     int>>
inline void AnyDependencyImpl<Ptr, inline_size, inline_align>::Initialize(
    std::tuple<ManagerArgs...> manager_args) {
  methods_ = &MethodsFor<Manager>::methods;
  MethodsFor<Manager>::Construct(repr_.storage, std::move(manager_args));
}

template <typename Ptr, size_t inline_size, size_t inline_align>
template <typename Manager, typename... ManagerArgs,
          std::enable_if_t<any_dependency_internal::IsCompatibleAnyDependency<
                               Ptr, inline_size, inline_align, Manager>::value,
                           int>>
inline void AnyDependencyImpl<Ptr, inline_size, inline_align>::Initialize(
    std::tuple<ManagerArgs...> manager_args) {
  AnyDependencyImpl<Ptr, inline_size, inline_align> manager(
      std::move(manager_args), std::index_sequence_for<ManagerArgs...>());
  // Adopt `manager` instead of wrapping it.
  methods_ = std::exchange(manager.methods_, &NullMethods::methods);
  methods_->move(repr_.storage, manager.repr_.storage);
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_ANY_DEPENDENCY_H_
