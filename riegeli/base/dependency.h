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
#include <string_view>  // IWYU pragma: keep
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/config.h"  // IWYU pragma: keep
#include "absl/base/nullability.h"
#include "absl/meta/type_traits.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/bytes_ref.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/dependency_manager.h"
#include "riegeli/base/initializer.h"
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
// or `Any<T*>` (maybe owned), with some `T` derived from `Base`.
//
// Often `Dependency<Handle, Manager>` is a member of a host class template
// parameterized by `Manager`, with `Handle` fixed by the host class. The member
// is initialized from an argument of a constructor or a resetting function.
// A user of the host class specifies ownership of the dependent object and
// possibly narrows its type by choosing the `Manager` template argument of the
// host class. The `Manager` type can be deduced from a constructor argument
// using CTAD, which is usually done by removing any toplevel references and
// `const` qualifiers using `std::decay`.
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
//   // The overload with no parameters is supported when the corresponding
//   // constructor is supported.
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
// ```

// `DependencyImpl` specializations provide what `DependencyBase` provides
// (constructors, `Reset()`, `manager()`, and `kIsStable`), and also `get()`,
// `IsOwning()`, and `kIsOwning`.

// This template is specialized but does not have a primary definition.
template <typename Handle, typename Manager, typename Enable = void>
class DependencyImpl;

// Specialization of `DependencyImpl<T*, Manager>` when
// `DependencyManagerPtr<Manager>` is a pointer convertible to `T*`.
template <typename T, typename Manager>
class DependencyImpl<
    T*, Manager,
    std::enable_if_t<std::conjunction_v<
        std::disjunction<
            std::is_pointer<DependencyManagerPtr<Manager>>,
            std::is_same<DependencyManagerPtr<Manager>, std::nullptr_t>>,
        std::is_convertible<DependencyManagerPtr<Manager>, T*>>>>
    : public DependencyManager<Manager> {
 public:
  using DependencyImpl::DependencyManager::DependencyManager;

  DependencyManagerPtr<Manager> get() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return this->ptr();
  }

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
    std::enable_if_t<std::conjunction_v<
        std::is_pointer<DependencyManagerPtr<Manager>>,
        std::is_constructible<absl::Span<T>, DependencyManagerRef<Manager>>>>>
    : public DependencyManager<Manager> {
 public:
  using DependencyImpl::DependencyManager::DependencyManager;

  // Return `absl::Span<std::remove_const_t<T>>` when
  // `DependencyManagerRef<Manager>` is convertible to it.
  template <typename DependentManager = Manager,
            std::enable_if_t<
                std::is_constructible_v<absl::Span<std::remove_const_t<T>>,
                                        DependencyManagerRef<DependentManager>>,
                int> = 0>
  absl::Span<std::remove_const_t<T>> get() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return absl::Span<std::remove_const_t<T>>(*this->ptr());
  }
  template <typename DependentManager = Manager,
            std::enable_if_t<!std::is_constructible_v<
                                 absl::Span<std::remove_const_t<T>>,
                                 DependencyManagerRef<DependentManager>>,
                             int> = 0>
  absl::Span<T> get() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return absl::Span<T>(*this->ptr());
  }

  static constexpr bool kIsStable =
      DependencyImpl::DependencyManager::kIsStable ||
      std::is_same_v<Manager, absl::Span<T>> ||
      std::is_same_v<Manager, absl::Span<std::remove_const_t<T>>>;

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
    std::enable_if_t<std::disjunction_v<
        std::is_same<DependencyManagerPtr<Manager>, absl::Span<T>>,
        std::is_same<DependencyManagerPtr<Manager>,
                     absl::Span<std::remove_const_t<T>>>>>>
    : public DependencyManager<Manager> {
 public:
  using DependencyImpl::DependencyManager::DependencyManager;

  DependencyManagerPtr<Manager> get() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return this->ptr();
  }

 protected:
  DependencyImpl(const DependencyImpl& that) = default;
  DependencyImpl& operator=(const DependencyImpl& that) = default;

  DependencyImpl(DependencyImpl&& that) = default;
  DependencyImpl& operator=(DependencyImpl&& that) = default;

  ~DependencyImpl() = default;
};

// Specialization of `DependencyImpl<absl::string_view, Manager>` when
// `DependencyManagerRef<Manager>` is convertible to `BytesRef`.
template <typename Manager>
class DependencyImpl<
    absl::string_view, Manager,
    std::enable_if_t<std::conjunction_v<
        std::is_pointer<DependencyManagerPtr<Manager>>,
        std::is_convertible<DependencyManagerRef<Manager>, BytesRef>>>>
    : public DependencyManager<Manager> {
 public:
  using DependencyImpl::DependencyManager::DependencyManager;

  absl::string_view get() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return BytesRef(*this->ptr());
  }

  static constexpr bool kIsStable =
      DependencyImpl::DependencyManager::kIsStable ||
      std::is_same_v<Manager, absl::string_view> ||
#if !defined(ABSL_USES_STD_STRING_VIEW)
      std::is_same_v<Manager, std::string_view> ||
#endif
      std::is_same_v<Manager, absl::Span<const char>> ||
      std::is_same_v<Manager, absl::Span<char>>;

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
    std::enable_if_t<std::disjunction_v<
        std::is_same<DependencyManagerPtr<Manager>, absl::Span<const char>>,
        std::is_same<DependencyManagerPtr<Manager>, absl::Span<char>>>>>
    : public DependencyManager<Manager> {
 public:
  using DependencyImpl::DependencyManager::DependencyManager;

  absl::string_view get() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
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

// `SupportsDependencyImpl<Handle, Manager>::value` is `true` when
// `DependencyImpl<Handle, Manager>` is defined.

template <typename Handle, typename Manager, typename Enable = void>
struct SupportsDependencyImpl : std::false_type {};

template <typename Handle, typename Manager>
struct SupportsDependencyImpl<
    Handle, Manager,
    std::void_t<
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
    std::enable_if_t<SupportsDependencyImpl<Handle, Manager>::value>>
    : public DependencyImpl<Handle, Manager> {
 public:
  using DependencyDefault::DependencyImpl::DependencyImpl;

  static_assert(
      std::is_convertible_v<
          decltype(std::declval<
                       const typename DependencyDefault::DependencyImpl&>()
                       .get()),
          Handle>,
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
    std::enable_if_t<std::conjunction_v<
        std::negation<SupportsDependencyImpl<Handle, Manager>>,
        std::is_pointer<DependencyManagerPtr<Manager>>,
        std::is_constructible<Handle, DependencyManagerRef<Manager>>>>>
    : public DependencyManager<Manager> {
 public:
  using DependencyDefault::DependencyManager::DependencyManager;

  // Return `DependencyManagerRef<Manager>` when it is a subclass of `Handle`.
  template <
      typename DependentManager = Manager,
      std::enable_if_t<std::is_convertible_v<
                           DependencyManagerPtr<DependentManager>, Handle*>,
                       int> = 0>
  DependencyManagerRef<DependentManager> get() const
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return *this->ptr();
  }
  template <
      typename DependentManager = Manager,
      std::enable_if_t<!std::is_convertible_v<
                           DependencyManagerPtr<DependentManager>, Handle*>,
                       int> = 0>
  Handle get() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return Handle(*this->ptr());
  }

  static constexpr bool kIsStable =
      DependencyDefault::DependencyManager::kIsStable ||
      std::is_convertible_v<DependencyManagerPtr<Manager>, Handle*>;

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
    std::enable_if_t<std::conjunction_v<
        std::negation<SupportsDependencyImpl<Handle, Manager>>,
        std::negation<std::conjunction<
            std::is_pointer<DependencyManagerPtr<Manager>>,
            std::is_constructible<Handle, DependencyManagerRef<Manager>>>>,
        std::is_constructible<Handle, DependencyManagerPtr<Manager>>>>>
    : public DependencyManager<Manager> {
 public:
  using DependencyDefault::DependencyManager::DependencyManager;

  // Return `DependencyManagerPtr<Manager>` when it is a subclass of `Handle`.
  template <
      typename DependentManager = Manager,
      std::enable_if_t<std::is_convertible_v<
                           DependencyManagerPtr<DependentManager>*, Handle*>,
                       int> = 0>
  DependencyManagerPtr<DependentManager> get() const
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return this->ptr();
  }
  template <
      typename DependentManager = Manager,
      std::enable_if_t<!std::is_convertible_v<
                           DependencyManagerPtr<DependentManager>*, Handle*>,
                       int> = 0>
  Handle get() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return Handle(this->ptr());
  }

  static constexpr bool kIsStable =
      DependencyDefault::DependencyManager::kIsStable ||
      std::is_convertible_v<DependencyManagerPtr<Manager>*, Handle*>;

 protected:
  DependencyDefault(const DependencyDefault& that) = default;
  DependencyDefault& operator=(const DependencyDefault& that) = default;

  DependencyDefault(DependencyDefault&& that) = default;
  DependencyDefault& operator=(DependencyDefault&& that) = default;

  ~DependencyDefault() = default;
};

// `SupportsDependencyDefault<Handle, Manager>::value` is `true` when
// `DependencyDefault<Handle, Manager, Manager&>` is defined.
template <typename Handle, typename Manager>
struct SupportsDependencyDefault
    : std::disjunction<
          dependency_internal::SupportsDependencyImpl<Handle, Manager>,
          std::conjunction<
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
    std::enable_if_t<SupportsDependencyDefault<Handle, Manager>::value>>
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
// `DependencyDefault<Handle, absl::remove_cvref_t<Manager>>` is defined:
// delegate to the latter.
template <typename Handle, typename Manager>
class DependencyDeref<
    Handle, Manager,
    std::enable_if_t<std::conjunction_v<
        std::is_reference<Manager>,
        std::negation<SupportsDependencyDefault<Handle, Manager>>,
        SupportsDependencyDefault<Handle, absl::remove_cvref_t<Manager>>>>>
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

// `SupportsDependencyDeref<Handle, Manager>::value` is `true` when
// `DependencyDeref<Handle, Manager>` is defined.
template <typename Handle, typename Manager>
struct SupportsDependencyDeref
    : std::disjunction<
          SupportsDependencyDefault<Handle, Manager>,
          std::conjunction<std::is_reference<Manager>,
                           SupportsDependencyDefault<
                               Handle, absl::remove_cvref_t<Manager>>>> {};

}  // namespace dependency_internal

// `SupportsDependency<Handle, Manager>::value` is `true` when
// `Dependency<Handle, Manager>` is defined and usable, i.e. constructible from
// `Initializer<Manager>`.
//
// An immovable `Manager` is usable when the `Initializer<Manager>` has been
// constructed from `riegeli::Maker()` or `riegeli::Invoker()`, not from an
// already constructed object.
template <typename Handle, typename Manager>
struct SupportsDependency
    : std::conjunction<
          dependency_internal::SupportsDependencyDeref<Handle, Manager>> {};

// `TargetSupportsDependency<Handle, Manager>::value` is `true` when
// `Dependency<Handle, TargetT<Manager>>` is defined and constructible from
// `Manager&&`.
//
// An immovable `TargetT<Manager>` is usable when the `Dependency` has been
// initialized with `riegeli::Maker()` or `riegeli::Invoker()`, possibly behind
// `Initializer`, not from an already constructed object.
template <typename Handle, typename Manager>
struct TargetSupportsDependency
    : std::conjunction<
          SupportsDependency<Handle, TargetT<Manager>>,
          std::is_convertible<Manager&&, Initializer<TargetT<Manager>>>> {};

// `TargetRefSupportsDependency<Handle, Manager>::value` is `true` when
// `DependencyRef<Handle, Manager>` i.e.
// `Dependency<Handle, TargetRefT<Manager>>` is defined and constructible from
// `Manager&&`.
//
// An immovable `TargetRefT<Manager>` is usable when the `Dependency` has been
// initialized with `riegeli::Maker()` or `riegeli::Invoker()`, possibly behind
// `Initializer`, not from an already constructed object.
template <typename Handle, typename Manager>
struct TargetRefSupportsDependency
    : std::conjunction<
          SupportsDependency<Handle, TargetRefT<Manager>>,
          std::is_convertible<Manager&&, Initializer<TargetRefT<Manager>>>> {};

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
    T, std::enable_if_t<std::is_convertible_v<
           decltype(std::declval<const T&>().IsOwning()), bool>>>
    : std::true_type {};

// `DependencyDerived` adds `Dependency` and `StableDependency` operations
// uniformly implemented in terms of other operations: `operator*`,
// `operator->`, and comparisons against `nullptr`.
//
// It derives from the template parameter `Base` so that it can be used in
// `Dependency` (applied to `DependencyDeref`) and `StableDependency`
// (applied to `StableDependencyImpl`).
template <typename Base, typename Handle, typename Manager>
class DependencyDerived
    : public Base,
      public WithEqual<DependencyDerived<Base, Handle, Manager>> {
 public:
  using Base::Base;

  using Subhandle = decltype(std::declval<const Base&>().get());

  template <
      typename DependentSubhandle = Subhandle,
      std::enable_if_t<HasDereference<DependentSubhandle>::value, int> = 0>
  decltype(*std::declval<DependentSubhandle>()) operator*() const
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    Subhandle handle = this->get();
    AssertNotNull(handle,
                  "Failed precondition of Dependency::operator*: null handle");
    return *std::move(handle);
  }

  template <typename DependentSubhandle = Subhandle,
            std::enable_if_t<HasArrow<DependentSubhandle>::value, int> = 0>
  Subhandle operator->() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
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
          std::conjunction_v<std::negation<HasDynamicIsOwning<DependentBase>>,
                             HasStaticIsOwning<DependentBase>>,
          int> = 0>
  bool IsOwning() const {
    return Base::kIsOwning;
  }

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
};

}  // namespace dependency_internal

template <typename Handle, typename Manager>
class
#ifdef ABSL_NULLABILITY_COMPATIBLE
    ABSL_NULLABILITY_COMPATIBLE
#endif
    Dependency : public dependency_internal::DependencyDerived<
                     dependency_internal::DependencyDeref<Handle, Manager>,
                     Handle, Manager> {
 private:
  // For `ABSL_NULLABILITY_COMPATIBLE`.
  using pointer = std::conditional_t<std::is_pointer_v<Handle>, Handle, void*>;

 public:
  using Dependency::DependencyDerived::DependencyDerived;

  Dependency(const Dependency& that) = default;
  Dependency& operator=(const Dependency& that) = default;

  Dependency(Dependency&& that) = default;
  Dependency& operator=(Dependency&& that) = default;
};

// `DependencyRef<Handle, Manager>` is an alias for
// `Dependency<Handle, TargetRefT<Manager>>`.
template <typename Handle, typename Manager>
using DependencyRef = Dependency<Handle, TargetRefT<Manager>>;

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
