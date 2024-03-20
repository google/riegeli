// Copyright 2018 Google LLC
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

#ifndef RIEGELI_BASE_DEPENDENCY_H_
#define RIEGELI_BASE_DEPENDENCY_H_

#include <cstddef>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/meta/type_traits.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/dependency_manager.h"
#include "riegeli/base/type_id.h"
#include "riegeli/base/type_traits.h"

namespace riegeli {

// `Dependency<Handle, Manager>` stores or refers to an optionally owned object
// which is stored as type `Manager` and accessed through type `Handle`.
//
// When a dependent object is said to be owned by a host object or function, the
// host is responsible for closing it when done, and certain other operations
// are propagated to it. The host is usually also responsible for destroying the
// owned object.
//
// Often `Handle` is some pointer `Base*`, and then `Manager` can be e.g.
// `T` (owned), `T*` (not owned), `std::unique_ptr<T, Deleter>` (owned),
// or `AnyDependency<T*>` (maybe owned), with some `T` derived from `Base`.
//
// Often `Dependency<Handle, Manager>` is a member of a host class template
// parameterized by `Manager`, with `Handle` fixed by the host class. The member
// is initialized from an argument of a constructor or a resetting function.
// A user of the host class specifies ownership of the dependent object and
// possibly narrows its type by choosing the `Manager` template argument of the
// host class. The `Manager` type can be deduced from a constructor argument
// using CTAD (since C++17), which is usually done by removing any toplevel
// references and `const` qualifiers using `std::decay`.
//
// As an alternative to passing `std::move(manager)`, passing
// `ClosingPtr(&manager)` avoids moving `manager`, but the caller must ensure
// that the dependent object is valid while the host object needs it.
//
// `Manager` can also be `T&` (not owned) or `T&&` (owned). They are primarily
// meant to be used with a host function rather than a host object, because such
// a dependency stores only a reference to the dependent object. By convention a
// reference argument is expected to be valid for the duration of the function
// call but not necessarily after the function returns. The `Manager` type is
// usually deduced from a function argument as a reference type rather than
// using `std::decay`.
//
// `Manager` being `T&` is functionally equivalent to `T*`, but offers a more
// idiomatic API for passing an object which does not need to be valid after the
// function returns.
//
// `Manager` being `T&&` is similar to `ClosingPtrType<T>`. In contrast to a
// host class, a host function does not decay `T&&` to `T` and avoids moving
// the `Manager`, because the dependent object can be expected to be valid for
// the duration of the function call.

// `Dependency<Handle, Manager>` derives from `DependencyImpl<Handle, Manager>`
// which has specializations for various combinations of `Handle` and `Manager`
// types. Some operations of `Dependency` are provided by `DependencyImpl`,
// others are added by `Dependency` in a uniform way.
//
// `DependencyImpl<Handle, Manager>` specializations often derive from
// `DependencyManager<Manager>` or `DependencyBase<Manager>`.
//
// `DependencyManager<Manager>` provides a preliminary interpretation of
// `Manager` independently from `Handle`. This interpretation is then refined by
// `DependencyImpl`.

// Operations of `Dependency<Handle, Manager>`:
//
// ```
//   // Constructs a dummy `Manager` from
//   // `RiegeliDependencySentinel(static_cast<Manager*>(nullptr))`. Used
//   // when the host object is closed and does not need a dependent object.
//   //
//   // Supported optionally.
//   //
//   // Provided by `DependencyBase` and explicitly inherited.
//   Dependency();
//
//   // Copies or moves a `Manager`. Used to specify the initial value of the
//   // dependent object.
//   //
//   // Provided by `DependencyBase` and explicitly inherited.
//   explicit Dependency(Initializer<Manager> manager);
//
//   // Copies the dependency.
//   //
//   // Supported optionally.
//   Dependency(const Dependency& that) noexcept;
//   Dependency& operator=(const Dependency& that) noexcept;
//
//   // Moves the dependency.
//   //
//   // Supported optionally.
//   Dependency(Dependency&& that) noexcept;
//   Dependency& operator=(Dependency&& that) noexcept;
//
//   // Makes `*this` equivalent to a newly constructed Dependency. This avoids
//   // constructing a temporary Dependency and moving from it.
//   //
//   // Supported optionally.
//   //
//   // Provided by `DependencyBase`.
//   ABSL_ATTRIBUTE_REINITIALIZES void Reset();
//   ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Manager> manager);
//
//   // Exposes the stored `Manager`.
//   //
//   // Provided by `DependencyBase` or `DependencyImpl`.
//   Manager& manager();
//   const Manager& manager() const;
//
//   // The type returned by `get()`.
//   //
//   // Provided by `Dependency`, not `DependencyImpl`.
//   using Subhandle = ...;
//
//   // Returns a `Handle` to the `Manager`.
//   //
//   // `get()` might return a subtype of `Handle` which retains more static
//   // type information about `Manager`, e.g. a pointer to a class derived from
//   // what `Handle` points to, or a class derived from `Handle`.
//   //
//   // The result is non-const even if the `Manager` is stored inside the
//   // `Dependency`.
//   //
//   // Provided by `DependencyImpl`.
//   Handle get() const;
//
//   // If `Handle` is `Base*` or another dereferenceable type, `Dependency` can
//   // be used as a smart pointer to `Base`, for convenience.
//   //
//   // Provided by `Dependency`, not `DependencyImpl`.
//   Base& operator*() const { return *get(); }
//   Base* operator->() const { return get(); }
//
//   // If `Handle` is `Base*` or another type comparable against `nullptr`,
//   // `Dependency` can be compared against `nullptr`.
//   //
//   // Provided by `Dependency`, not `DependencyImpl`.
//   friend bool operator==(const Dependency& a, std::nullptr_t) {
//     return a.get() == nullptr;
//   }
//
//   // If `true`, the `Dependency` owns the dependent object, i.e. closing the
//   // host object should close the dependent object.
//   //
//   // Provided by `DependencyManagerImpl`, `DependencyImpl`, or `Dependency`.
//   // In `Dependency` implemented in terms of `kIsOwning`.
//   bool IsOwning() const;
//
//   // The value of `IsOwning()` if known statically or mostly statically.
//   //
//   // This constant is optional.
//   //
//   // If `IsOwning()` returns a statically known constant, `kIsOwning` should
//   // be defined. `Dependency` will provide `IsOwning()`.
//   //
//   // If `IsOwning()` returns `true` except for a sentinel value like
//   // `nullptr`, e.g. for `std::unique_ptr`, `kIsOwning` can still be defined
//   // in addition to `IsOwning()`. This allows to use the static
//   // approximatimation when static selection is needed, with the caveat that
//   // it will return `true` also for the sentinel value.
//   //
//   // Provided by `DependencyManagerImpl` or `DependencyImpl`.
//   static constexpt bool kIsOwning;
//
//   // If `true`, `get()` stays unchanged when a `Dependency` is moved.
//   //
//   // This can be used as an optimization to avoid recomputing values derived
//   // from them when a `Dependency` is moved.
//   //
//   // Provided by `DependencyBase`, `DependencyManagerImpl`, or
//   // `DependencyImpl`.
//   static constexpr bool kIsStable;
//
//   // If the `Manager` has exactly this type or a reference to it, returns
//   // a pointer to the `Manager`. If the `Manager` is an `AnyDependency`
//   // (possibly wrapped in a reference or `std::unique_ptr`), propagates
//   // `GetIf()` to it. Otherwise returns `nullptr`.
//   //
//   // Provided by `DependencyManagerImpl`, `DependencyImpl`, or `Dependency`.
//   // In `Dependency` implemented in terms of `GetIf(TypeId)` if that is
//   // available.
//   template <typename OtherManager>
//   OtherManager* GetIf();
//   template <typename OtherManager>
//   const OtherManager* GetIf() const;
//
//   // A variant of `GetIf()` with the expected type passed as a `TypeId`.
//   //
//   // Provided by `DependencyManagerImpl`, `DependencyImpl`, or `Dependency`.
//   void* GetIf(TypeId type_id);
//   const void* GetIf(TypeId type_id) const;
// ```

// `DependencyImpl` specializations provide what `DependencyBase` provides
// (constructors, `Reset()`, `manager()`, and `kIsStable`), and also `get()`,
// `IsOwning()`, `kIsOwning`, and optionally `GetIf()`.

// This template is specialized but does not have a primary definition.
template <typename Handle, typename Manager, typename Enable = void>
class DependencyImpl;

// Specialization of `DependencyImpl<T*, Manager>` when
// `DependencyManagerPtr<Manager>` is a pointer convertible to `T*`.
template <typename T, typename Manager>
class DependencyImpl<
    T*, Manager,
    std::enable_if_t<absl::conjunction<
        absl::disjunction<
            std::is_pointer<DependencyManagerPtr<Manager>>,
            std::is_same<DependencyManagerPtr<Manager>, std::nullptr_t>>,
        std::is_convertible<DependencyManagerPtr<Manager>, T*>>::value>>
    : public DependencyManager<Manager> {
 public:
  using DependencyImpl::DependencyManager::DependencyManager;

  DependencyManagerPtr<Manager> get() const { return this->ptr(); }

 protected:
  DependencyImpl(const DependencyImpl& that) = default;
  DependencyImpl& operator=(const DependencyImpl& that) = default;

  DependencyImpl(DependencyImpl&& that) = default;
  DependencyImpl& operator=(DependencyImpl&& that) = default;

  ~DependencyImpl() = default;
};

// Specialization of `DependencyImpl<absl::Span<T>, Manager>` when
// `DependencyManagerRef<Manager>` is explicitly convertible to `absl::Span<T>`.
//
// Specialized separately for `get()` to return
// `absl::Span<std::remove_const_t<T>>` if possible.
template <typename T, typename Manager>
class DependencyImpl<
    absl::Span<T>, Manager,
    std::enable_if_t<absl::conjunction<
        std::is_pointer<DependencyManagerPtr<Manager>>,
        std::is_constructible<absl::Span<T>, DependencyManagerRef<Manager>>>::
                         value>> : public DependencyManager<Manager> {
 public:
  using DependencyImpl::DependencyManager::DependencyManager;

  // Return `absl::Span<std::remove_const_t<T>>` when
  // `DependencyManagerRef<Manager>` is convertible to it.
  template <typename DependentManager = Manager,
            std::enable_if_t<std::is_constructible<
                                 absl::Span<std::remove_const_t<T>>,
                                 DependencyManagerRef<DependentManager>>::value,
                             int> = 0>
  absl::Span<std::remove_const_t<T>> get() const {
    return absl::Span<std::remove_const_t<T>>(*this->ptr());
  }
  template <typename DependentManager = Manager,
            std::enable_if_t<!std::is_constructible<
                                 absl::Span<std::remove_const_t<T>>,
                                 DependencyManagerRef<DependentManager>>::value,
                             int> = 0>
  absl::Span<T> get() const {
    return absl::Span<T>(*this->ptr());
  }

  static constexpr bool kIsStable =
      DependencyImpl::DependencyManager::kIsStable ||
      std::is_same<Manager, absl::Span<T>>::value ||
      std::is_same<Manager, absl::Span<std::remove_const_t<T>>>::value;

 protected:
  DependencyImpl(const DependencyImpl& that) = default;
  DependencyImpl& operator=(const DependencyImpl& that) = default;

  DependencyImpl(DependencyImpl&& that) = default;
  DependencyImpl& operator=(DependencyImpl&& that) = default;

  ~DependencyImpl() = default;
};

// Specialization of `DependencyImpl<absl::Span<T>, Manager>` when
// `DependencyManagerPtr<Manager>` is `absl::Span<T>` or
// `absl::Span<std::remove_const_t<T>>`.
//
// Specialized separately for `get()` to return
// `absl::Span<std::remove_const_t<T>>` if possible.
template <typename T, typename Manager>
class DependencyImpl<
    absl::Span<T>, Manager,
    std::enable_if_t<absl::disjunction<
        std::is_same<DependencyManagerPtr<Manager>, absl::Span<T>>,
        std::is_same<DependencyManagerPtr<Manager>,
                     absl::Span<std::remove_const_t<T>>>>::value>>
    : public DependencyManager<Manager> {
 public:
  using DependencyImpl::DependencyManager::DependencyManager;

  DependencyManagerPtr<Manager> get() const { return this->ptr(); }

 protected:
  DependencyImpl(const DependencyImpl& that) = default;
  DependencyImpl& operator=(const DependencyImpl& that) = default;

  DependencyImpl(DependencyImpl&& that) = default;
  DependencyImpl& operator=(DependencyImpl&& that) = default;

  ~DependencyImpl() = default;
};

// Specialization of `DependencyImpl<absl::string_view, Manager>` when
// `DependencyManagerRef<Manager>` is not convertible to `absl::string_view` but
// is explicitly convertible to `absl::Span<const char>`.
//
// Specialized separately because `absl::Span<const char>` provides more
// conversions than `absl::string_view`, e.g. from `std::vector<char>`.
template <typename Manager>
class DependencyImpl<
    absl::string_view, Manager,
    std::enable_if_t<absl::conjunction<
        std::is_pointer<DependencyManagerPtr<Manager>>,
        absl::negation<std::is_constructible<absl::string_view,
                                             DependencyManagerRef<Manager>>>,
        std::is_constructible<absl::Span<const char>,
                              DependencyManagerRef<Manager>>>::value>>
    : public DependencyManager<Manager> {
 public:
  using DependencyImpl::DependencyManager::DependencyManager;

  absl::string_view get() const {
    const absl::Span<const char> span(*this->ptr());
    return absl::string_view(span.data(), span.size());
  }

  static constexpr bool kIsStable =
      DependencyImpl::DependencyManager::kIsStable ||
      std::is_same<Manager, absl::Span<const char>>::value ||
      std::is_same<Manager, absl::Span<char>>::value;

 protected:
  DependencyImpl(const DependencyImpl& that) = default;
  DependencyImpl& operator=(const DependencyImpl& that) = default;

  DependencyImpl(DependencyImpl&& that) = default;
  DependencyImpl& operator=(DependencyImpl&& that) = default;

  ~DependencyImpl() = default;
};

// Specialization of `DependencyImpl<absl::string_view, Manager>` when
// `DependencyManagerPtr<Manager>` is `absl::Span<const char>` or
// `absl::Span<char>`.
//
// Specialized separately because `absl::Span<const char>` is not convertible
// to `absl::string_view` in the regular way.
template <typename Manager>
class DependencyImpl<
    absl::string_view, Manager,
    std::enable_if_t<absl::disjunction<
        std::is_same<DependencyManagerPtr<Manager>, absl::Span<const char>>,
        std::is_same<DependencyManagerPtr<Manager>, absl::Span<char>>>::value>>
    : public DependencyManager<Manager> {
 public:
  using DependencyImpl::DependencyManager::DependencyManager;

  absl::string_view get() const {
    const absl::Span<const char> span = this->ptr();
    return absl::string_view(span.data(), span.size());
  }

 protected:
  DependencyImpl(const DependencyImpl& that) = default;
  DependencyImpl& operator=(const DependencyImpl& that) = default;

  DependencyImpl(DependencyImpl&& that) = default;
  DependencyImpl& operator=(DependencyImpl&& that) = default;

  ~DependencyImpl() = default;
};

namespace dependency_internal {

// `IsValidDependencyImpl<Handle, Manager>::value` is `true` when
// `DependencyImpl<Handle, Manager>` is defined.

template <typename Handle, typename Manager, typename Enable = void>
struct IsValidDependencyImpl : std::false_type {};

template <typename Handle, typename Manager>
struct IsValidDependencyImpl<
    Handle, Manager,
    absl::void_t<
        decltype(std::declval<const DependencyImpl<Handle, Manager>&>().get())>>
    : std::true_type {};

// `DependencyDefault<Handle, Manager>` extends
// `DependencyImpl<Handle, Manager>` with the basic cases when
// `DependencyManagerRef<Manager>` or `DependencyManagerPtr<Manager>` is
// explicitly convertible to `Handle`.

// This template is specialized but does not have a primary definition.
template <typename Handle, typename Manager, typename Enable = void>
class DependencyDefault;

// Specialization of `DependencyDefault<Handle, Manager>` when
// `DependencyImpl<Handle, Manager>` is defined: delegate to it.
template <typename Handle, typename Manager>
class DependencyDefault<
    Handle, Manager,
    std::enable_if_t<IsValidDependencyImpl<Handle, Manager>::value>>
    : public DependencyImpl<Handle, Manager> {
 public:
  using DependencyDefault::DependencyImpl::DependencyImpl;

  static_assert(
      std::is_convertible<
          decltype(std::declval<const DependencyImpl<Handle, Manager>&>()
                       .get()),
          Handle>::value,
      "DependencyImpl<Handle, Manager>::get() must return a subtype of Handle");

 protected:
  DependencyDefault(const DependencyDefault& that) = default;
  DependencyDefault& operator=(const DependencyDefault& that) = default;

  DependencyDefault(DependencyDefault&& that) = default;
  DependencyDefault& operator=(DependencyDefault&& that) = default;

  ~DependencyDefault() = default;
};

// Specialization of `DependencyDefault<Handle, Manager>` when
// `DependencyImpl<Handle, Manager>` is not defined and
// `DependencyManagerRef<Manager>` is explicitly convertible to `Handle`:
// let `get()` return `*ptr()`, as its original type if possible.
template <typename Handle, typename Manager>
class DependencyDefault<
    Handle, Manager,
    std::enable_if_t<absl::conjunction<
        absl::negation<IsValidDependencyImpl<Handle, Manager>>,
        std::is_pointer<DependencyManagerPtr<Manager>>,
        std::is_constructible<Handle, DependencyManagerRef<Manager>>>::value>>
    : public DependencyManager<Manager> {
 public:
  using DependencyDefault::DependencyManager::DependencyManager;

  // Return `DependencyManagerRef<Manager>` when it is a subclass of `Handle`.
  template <typename DependentManager = Manager,
            std::enable_if_t<
                std::is_convertible<DependencyManagerRef<DependentManager>*,
                                    Handle*>::value,
                int> = 0>
  DependencyManagerRef<DependentManager> get() const {
    return *this->ptr();
  }
  template <typename DependentManager = Manager,
            std::enable_if_t<
                !std::is_convertible<DependencyManagerRef<DependentManager>*,
                                     Handle*>::value,
                int> = 0>
  Handle get() const {
    return Handle(*this->ptr());
  }

  static constexpr bool kIsStable =
      DependencyDefault::DependencyManager::kIsStable ||
      std::is_convertible<DependencyManagerRef<Manager>*, Handle*>::value;

 protected:
  DependencyDefault(const DependencyDefault& that) = default;
  DependencyDefault& operator=(const DependencyDefault& that) = default;

  DependencyDefault(DependencyDefault&& that) = default;
  DependencyDefault& operator=(DependencyDefault&& that) = default;

  ~DependencyDefault() = default;
};

// Specialization of `DependencyDefault<Handle, Manager>` when
// `DependencyImpl<Handle, Manager>` is not defined,
// `DependencyManagerRef<Manager>` is not convertible to `Handle`, and
// `DependencyManagerPtr<Manager>` is explicitly convertible to `Handle`:
// let `get()` return `ptr()`, as its original type if possible.
template <typename Handle, typename Manager>
class DependencyDefault<
    Handle, Manager,
    std::enable_if_t<absl::conjunction<
        absl::negation<IsValidDependencyImpl<Handle, Manager>>,
        absl::negation<absl::conjunction<
            std::is_pointer<DependencyManagerPtr<Manager>>,
            std::is_constructible<Handle, DependencyManagerRef<Manager>>>>,
        std::is_constructible<Handle, DependencyManagerPtr<Manager>>>::value>>
    : public DependencyManager<Manager> {
 public:
  using DependencyDefault::DependencyManager::DependencyManager;

  // Return `DependencyManagerPtr<Manager>` when it is a subclass of `Handle`.
  template <typename DependentManager = Manager,
            std::enable_if_t<
                std::is_convertible<DependencyManagerPtr<DependentManager>*,
                                    Handle*>::value,
                int> = 0>
  DependencyManagerPtr<DependentManager> get() const {
    return this->ptr();
  }
  template <typename DependentManager = Manager,
            std::enable_if_t<
                !std::is_convertible<DependencyManagerPtr<DependentManager>*,
                                     Handle*>::value,
                int> = 0>
  Handle get() const {
    return Handle(this->ptr());
  }

  static constexpr bool kIsStable =
      DependencyDefault::DependencyManager::kIsStable ||
      std::is_convertible<DependencyManagerPtr<Manager>*, Handle*>::value;

 protected:
  DependencyDefault(const DependencyDefault& that) = default;
  DependencyDefault& operator=(const DependencyDefault& that) = default;

  DependencyDefault(DependencyDefault&& that) = default;
  DependencyDefault& operator=(DependencyDefault&& that) = default;

  ~DependencyDefault() = default;
};

// `IsValidDependencyDefault<Handle, Manager>::value` is `true` when
// `DependencyDefault<Handle, Manager, Manager&>` is defined.
template <typename Handle, typename Manager>
struct IsValidDependencyDefault
    : absl::disjunction<
          dependency_internal::IsValidDependencyImpl<Handle, Manager>,
          absl::conjunction<
              std::is_pointer<DependencyManagerPtr<Manager>>,
              std::is_constructible<Handle, DependencyManagerRef<Manager>>>,
          std::is_constructible<Handle, DependencyManagerPtr<Manager>>> {};

// `DependencyDeref<Handle, Manager>` extends
// `DependencyDefault<Handle, Manager>` with cases where `Manager` is
// a reference, if `DependencyImpl<Handle, Manager>` is not defined.
//
// If `DependencyImpl<Handle, Manager>` uses `DependencyManager<Manager>`, then
// this is already covered. Custom specializations might not cover this.

// This template is specialized but does not have a primary definition.
template <typename Handle, typename Manager, typename Enable = void>
class DependencyDeref;

// Specialization of `DependencyDeref<Handle, Manager>` when
// `DependencyDefault<Handle, Manager>` is defined: delegate to it.
template <typename Handle, typename Manager>
class DependencyDeref<
    Handle, Manager,
    std::enable_if_t<IsValidDependencyDefault<Handle, Manager>::value>>
    : public DependencyDefault<Handle, Manager> {
 public:
  using DependencyDeref::DependencyDefault::DependencyDefault;

 protected:
  DependencyDeref(const DependencyDeref& that) = default;
  DependencyDeref& operator=(const DependencyDeref& that) = default;

  DependencyDeref(DependencyDeref&& that) = default;
  DependencyDeref& operator=(DependencyDeref&& that) = default;

  ~DependencyDeref() = default;
};

// Specialization of `DependencyDeref<Handle, Manager>` when
// `DependencyDefault<Handle, Manager>` is not defined,
// `Manager` is a reference, and
// `DependencyDefault<Handle, absl::remove_cvref_t<Manager>>`
// is defined: delegate to the latter.
template <typename Handle, typename Manager>
class DependencyDeref<
    Handle, Manager,
    std::enable_if_t<absl::conjunction<
        std::is_reference<Manager>,
        absl::negation<IsValidDependencyDefault<Handle, Manager>>,
        IsValidDependencyDefault<Handle,
                                 absl::remove_cvref_t<Manager>>>::value>>
    : public DependencyDefault<Handle, absl::remove_cvref_t<Manager>> {
 public:
  using DependencyDeref::DependencyDefault::DependencyDefault;

 protected:
  DependencyDeref(const DependencyDeref& that) = default;
  DependencyDeref& operator=(const DependencyDeref& that) = default;

  DependencyDeref(DependencyDeref&& that) = default;
  DependencyDeref& operator=(DependencyDeref&& that) = default;

  ~DependencyDeref() = default;
};

}  // namespace dependency_internal

// `IsValidDependency<Handle, Manager>::value` is `true` when
// `Dependency<Handle, Manager>` is defined.
template <typename Handle, typename Manager>
struct IsValidDependency
    : absl::disjunction<
          dependency_internal::IsValidDependencyDefault<Handle, Manager>,
          absl::conjunction<std::is_reference<Manager>,
                            dependency_internal::IsValidDependencyDefault<
                                Handle, absl::remove_cvref_t<Manager>>>> {};

namespace dependency_internal {

template <bool value>
struct IsConstexprBool : std::true_type {};

}  // namespace dependency_internal

// `HasStaticIsOwning<T>::value` is `true` if `T` defines
// `static constexpr bool kIsOwning`.

template <typename T, typename Enable = void>
struct HasStaticIsOwning : std::false_type {};

template <typename T>
struct HasStaticIsOwning<
    T,
    std::enable_if_t<dependency_internal::IsConstexprBool<T::kIsOwning>::value>>
    : std::true_type {};

// Deriving a class from `PropagateStaticIsOwning<T>` defines
// `static constexpr bool kIsOwning = T::kIsOwning` if `T` defines `kIsOwning`.

template <typename T, typename Enable = void>
class PropagateStaticIsOwning {};

template <typename T>
class PropagateStaticIsOwning<T,
                              std::enable_if_t<HasStaticIsOwning<T>::value>> {
 public:
  static constexpr bool kIsOwning = T::kIsOwning;
};

namespace dependency_internal {

template <typename T, typename Enable = void>
struct HasDynamicIsOwning : std::false_type {};

template <typename T>
struct HasDynamicIsOwning<
    T, std::enable_if_t<std::is_convertible<
           decltype(std::declval<const T&>().IsOwning()), bool>::value>>
    : std::true_type {};

template <typename T, typename OtherManager, typename Enable = void>
struct HasStaticGetIf : std::false_type {};

template <typename T, typename OtherManager>
struct HasStaticGetIf<
    T, OtherManager,
    std::enable_if_t<absl::disjunction<
        std::is_convertible<
            decltype(std::declval<T&>().template GetIf<OtherManager>()),
            OtherManager*>,
        std::is_convertible<
            decltype(std::declval<const T&>().template GetIf<OtherManager>()),
            const OtherManager*>>::value>> : std::true_type {
  static_assert(
      absl::conjunction<
          std::is_convertible<
              decltype(std::declval<T&>().template GetIf<OtherManager>()),
              OtherManager*>,
          std::is_convertible<
              decltype(std::declval<const T&>().template GetIf<OtherManager>()),
              const OtherManager*>>::value,
      "Either both mutable and const or none GetIf() overloads "
      "must be provided by DependencyImpl");
};

template <typename T, typename Enable = void>
struct HasDynamicGetIf : std::false_type {};

template <typename T>
struct HasDynamicGetIf<
    T,
    std::enable_if_t<absl::disjunction<
        std::is_convertible<
            decltype(std::declval<T&>().GetIf(std::declval<TypeId>())), void*>,
        std::is_convertible<decltype(std::declval<const T&>().GetIf(
                                std::declval<TypeId>())),
                            const void*>>::value>> : std::true_type {
  static_assert(absl::conjunction<
                    std::is_convertible<decltype(std::declval<T&>().GetIf(
                                            std::declval<TypeId>())),
                                        void*>,
                    std::is_convertible<decltype(std::declval<const T&>().GetIf(
                                            std::declval<TypeId>())),
                                        const void*>>::value,
                "Either both mutable and const or none GetIf() overloads "
                "must be provided by DependencyImpl");
};

// `DependencyDerived` adds `Dependency` and `StableDependency` operations
// uniformly implemented in terms of other operations: `operator*`,
// `operator->`, comparisons against `nullptr`, and `GetIf()`.
//
// It derives from the template parameter `Base` so that it can be used in
// `Dependency` (applied to `DependencyDeref`) and `StableDependency`
// (applied to `StableDependencyImpl`).
template <typename Base, typename Handle, typename Manager>
class DependencyDerived
    : public Base,
      public WithEqual<DependencyDerived<Base, Handle, Manager>>,
      public ConditionallyAbslNullabilityCompatible<
          IsComparableAgainstNullptr<Handle>::value> {
 public:
  using Base::Base;

  using Subhandle = decltype(std::declval<const Base&>().get());

  template <
      typename DependentSubhandle = Subhandle,
      std::enable_if_t<HasDereference<DependentSubhandle>::value, int> = 0>
  decltype(*std::declval<DependentSubhandle>()) operator*() const {
    Subhandle handle = this->get();
    AssertNotNull(handle,
                  "Failed precondition of Dependency::operator*: null handle");
    return *std::move(handle);
  }

  template <typename DependentSubhandle = Subhandle,
            std::enable_if_t<HasArrow<DependentSubhandle>::value, int> = 0>
  Subhandle operator->() const {
    Subhandle handle = this->get();
    AssertNotNull(handle,
                  "Failed precondition of Dependency::operator->: null handle");
    return handle;
  }

  template <typename DependentSubhandle = Subhandle,
            std::enable_if_t<
                IsComparableAgainstNullptr<DependentSubhandle>::value, int> = 0>
  friend bool operator==(const DependencyDerived& a, std::nullptr_t) {
    return a.get() == nullptr;
  }

  template <typename DependentBase = Base,
            std::enable_if_t<HasDynamicIsOwning<DependentBase>::value, int> = 0>
  bool IsOwning() const {
    return Base::IsOwning();
  }
  template <
      typename DependentBase = Base,
      std::enable_if_t<
          absl::conjunction<absl::negation<HasDynamicIsOwning<DependentBase>>,
                            HasStaticIsOwning<DependentBase>>::value,
          int> = 0>
  bool IsOwning() const {
    return Base::kIsOwning;
  }

  template <
      typename OtherManager,
      std::enable_if_t<IsValidDependency<Handle, OtherManager>::value, int> = 0>
  OtherManager* GetIf() {
    return GetIfImpl<OtherManager>();
  }
  template <
      typename OtherManager,
      std::enable_if_t<IsValidDependency<Handle, OtherManager>::value, int> = 0>
  const OtherManager* GetIf() const {
    return GetIfImpl<OtherManager>();
  }

  void* GetIf(TypeId type_id) { return GetIfImpl(type_id); }
  const void* GetIf(TypeId type_id) const { return GetIfImpl(type_id); }

 protected:
  DependencyDerived(const DependencyDerived& that) = default;
  DependencyDerived& operator=(const DependencyDerived& that) = default;

  DependencyDerived(DependencyDerived&& that) = default;
  DependencyDerived& operator=(DependencyDerived&& that) = default;

  ~DependencyDerived() = default;

 private:
  template <typename DependentSubhandle = Subhandle,
            std::enable_if_t<
                IsComparableAgainstNullptr<DependentSubhandle>::value, int> = 0>
  static void AssertNotNull(Subhandle handle, absl::string_view message) {
    RIEGELI_ASSERT(handle != nullptr) << message;
  }
  template <
      typename DependentSubhandle = Subhandle,
      std::enable_if_t<!IsComparableAgainstNullptr<DependentSubhandle>::value,
                       int> = 0>
  static void AssertNotNull(ABSL_ATTRIBUTE_UNUSED Subhandle handle,
                            ABSL_ATTRIBUTE_UNUSED absl::string_view message) {}

  template <typename OtherManager, typename DependentBase = Base,
            std::enable_if_t<HasStaticGetIf<DependentBase, OtherManager>::value,
                             int> = 0>
  OtherManager* GetIfImpl() {
    return Base::template GetIf<OtherManager>();
  }
  template <typename OtherManager, typename DependentBase = Base,
            std::enable_if_t<
                absl::conjunction<
                    absl::negation<HasStaticGetIf<DependentBase, OtherManager>>,
                    HasDynamicGetIf<DependentBase>>::value,
                int> = 0>
  OtherManager* GetIfImpl() {
    return static_cast<OtherManager*>(Base::GetIf(TypeId::For<OtherManager>()));
  }
  template <
      typename OtherManager, typename DependentBase = Base,
      std::enable_if_t<
          absl::conjunction<
              absl::negation<HasStaticGetIf<DependentBase, OtherManager>>,
              absl::negation<HasDynamicGetIf<DependentBase>>,
              std::is_same<absl::remove_cvref_t<Manager>, OtherManager>>::value,
          int> = 0>
  OtherManager* GetIfImpl() {
    return &this->manager();
  }
  template <typename OtherManager, typename DependentBase = Base,
            std::enable_if_t<
                absl::conjunction<
                    absl::negation<HasStaticGetIf<DependentBase, OtherManager>>,
                    absl::negation<HasDynamicGetIf<DependentBase>>,
                    absl::negation<std::is_same<absl::remove_cvref_t<Manager>,
                                                OtherManager>>>::value,
                int> = 0>
  OtherManager* GetIfImpl() {
    return nullptr;
  }
  template <typename OtherManager, typename DependentBase = Base,
            std::enable_if_t<HasStaticGetIf<DependentBase, OtherManager>::value,
                             int> = 0>
  const OtherManager* GetIfImpl() const {
    return Base::template GetIf<OtherManager>();
  }
  template <typename OtherManager, typename DependentBase = Base,
            std::enable_if_t<
                absl::conjunction<
                    absl::negation<HasStaticGetIf<DependentBase, OtherManager>>,
                    HasDynamicGetIf<DependentBase>>::value,
                int> = 0>
  const OtherManager* GetIfImpl() const {
    return static_cast<const OtherManager*>(
        Base::GetIf(TypeId::For<OtherManager>()));
  }
  template <
      typename OtherManager, typename DependentBase = Base,
      std::enable_if_t<
          absl::conjunction<
              absl::negation<HasStaticGetIf<DependentBase, OtherManager>>,
              absl::negation<HasDynamicGetIf<DependentBase>>,
              std::is_same<absl::remove_cvref_t<Manager>, OtherManager>>::value,
          int> = 0>
  const OtherManager* GetIfImpl() const {
    return &this->manager();
  }
  template <typename OtherManager, typename DependentBase = Base,
            std::enable_if_t<
                absl::conjunction<
                    absl::negation<HasStaticGetIf<DependentBase, OtherManager>>,
                    absl::negation<HasDynamicGetIf<DependentBase>>,
                    absl::negation<std::is_same<absl::remove_cvref_t<Manager>,
                                                OtherManager>>>::value,
                int> = 0>
  const OtherManager* GetIfImpl() const {
    return nullptr;
  }

  template <typename DependentBase = Base,
            std::enable_if_t<HasDynamicGetIf<DependentBase>::value, int> = 0>
  void* GetIfImpl(TypeId type_id) {
    return Base::GetIf(type_id);
  }
  template <typename DependentBase = Base,
            std::enable_if_t<!HasDynamicGetIf<DependentBase>::value, int> = 0>
  void* GetIfImpl(TypeId type_id) {
    if (TypeId::For<absl::remove_cvref_t<Manager>>() == type_id) {
      return const_cast<absl::remove_cvref_t<Manager>*>(&this->manager());
    }
    return nullptr;
  }
  template <typename DependentBase = Base,
            std::enable_if_t<HasDynamicGetIf<DependentBase>::value, int> = 0>
  const void* GetIfImpl(TypeId type_id) const {
    return Base::GetIf(type_id);
  }
  template <typename DependentBase = Base,
            std::enable_if_t<!HasDynamicGetIf<DependentBase>::value, int> = 0>
  const void* GetIfImpl(TypeId type_id) const {
    if (TypeId::For<absl::remove_cvref_t<Manager>>() == type_id) {
      return &this->manager();
    }
    return nullptr;
  }
};

}  // namespace dependency_internal

template <typename Handle, typename Manager>
class Dependency : public dependency_internal::DependencyDerived<
                       dependency_internal::DependencyDeref<Handle, Manager>,
                       Handle, Manager> {
 public:
  using Dependency::DependencyDerived::DependencyDerived;

  Dependency(const Dependency& that) = default;
  Dependency& operator=(const Dependency& that) = default;

  Dependency(Dependency&& that) = default;
  Dependency& operator=(Dependency&& that) = default;
};

namespace dependency_internal {

// `AlwaysFalse<T...>::value` is `false`, but formally depends on `T...`.
// This is useful for `static_assert()`.

template <typename... T>
struct AlwaysFalse : std::false_type {};

}  // namespace dependency_internal

// A placeholder `Dependency` manager to be deduced by CTAD, used to delete CTAD
// for particular constructor argument types.
//
// It takes `ConstructorArgTypes` so that an error message from the
// `static_assert()` can show them.

template <typename... ConstructorArgTypes>
struct DeleteCtad {
  DeleteCtad() = delete;
};

template <typename Handle, typename... ConstructorArgTypes>
class Dependency<Handle, DeleteCtad<ConstructorArgTypes...>> {
  static_assert(dependency_internal::AlwaysFalse<ConstructorArgTypes...>::value,
                "Template arguments must be written explicitly "
                "with these constructor argument types");
};

}  // namespace riegeli

#endif  // RIEGELI_BASE_DEPENDENCY_H_
