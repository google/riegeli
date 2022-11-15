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

#include <stddef.h>

#include <functional>
#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/meta/type_traits.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/reset.h"

namespace riegeli {

// `Dependency<Ptr, Manager>` contains or refers to an optionally owned object
// which is stored as type `Manager` and accessed through type `Ptr`.
//
// When a dependent object is said to be owned by a host object or function, the
// host is responsible for closing it when done, and certain other operations
// are propagated to it. The host is usually also responsible for destroying the
// owned object.
//
// Often `Ptr` is some pointer `P*`, and then `Manager` can be e.g.
// `M*` (not owned), `M` (owned), or `std::unique_ptr<M, Deleter>` (owned),
// with `M` derived from `P`.
//
// Often `Dependency<Ptr, Manager>` is a member of a host class template
// parameterized by `Manager`, with `Ptr` fixed by the host class. The member
// is initialized from an argument of a constructor or a resetting function.
// A user of the host class specifies ownership of the dependent object and
// possibly narrows its type by choosing the `Manager` template argument of the
// host class. The `Manager` type can be deduced from a constructor argument
// using CTAD (since C++17), which is usually done by removing any toplevel
// references and `const` qualifiers using `std::decay`.
//
// `Manager` can also be `M&` (not owned) or `M&&` (owned). They are primarily
// meant to be used with a host function rather than a host object, because such
// a dependency stores only a reference to the dependent object. By convention a
// reference argument is expected to be valid for the duration of the function
// call but not necessarily after the function returns. The `Manager` type is
// usually deduced from a function argument as a reference type rather than
// using `std::decay`.
//
// `Manager` being `M&` is functionally equivalent to `M*`, but offers a more
// idiomatic API for passing an object which does not need to be valid after the
// function returns.
//
// `Manager` being `M&&` is similar to `M`, but the dependent object does not
// need to be moved from its temporary location, which is expected to be valid
// for the duration of the function call. The host function owns the dependent
// object but does not destroy it.
//
// `Manager` being `std::reference_wrapper<M>` is equivalent to `M&&`, but such
// a type survives `std::decay`, and thus can be deduced from `std::ref(m)`
// passed as a constructor argument of the host object. Passing `std::ref(m)`
// instead of `std::move(m)` avoids moving `m`, but the caller must ensure that
// the dependent object is valid while the host object needs it. The host object
// owns the dependent object (in contrast to passing `&m`) but does not destroy
// it.

// `RiegeliDependencySentinel(T*)` specifies how to initialize a default
// `Manager` (for `Dependency`) or `Ptr` (for `AnyDependency`) of type `T`.
//
// To customize that for a class `T`, define a free function
// `friend T RiegeliDependencySentinel(T*)` as a friend of `T` inside class
// definition or in the same namespace as `T`, so that it can be found via ADL.
//
// `RiegeliDependencySentinel(T*)` returns a value of type `T`, or a tuple of
// its constructor arguments to avoid constructing a temporary `T` and moving
// from it.
//
// The argument of `RiegeliDependencySentinel(T*)` is always a null pointer,
// used to choose the right overload based on the type.

inline std::tuple<> RiegeliDependencySentinel(void*) { return {}; }

// Specialization of `RiegeliDependencySentinel(int*)`, used for file
// descriptors.

inline int RiegeliDependencySentinel(int*) { return -1; }

// `Dependency<Ptr, Manager>` derives from `DependencyImpl<Ptr, Manager>` which
// has specializations for various combinations of `Ptr` and `Manager` types.
// Most operations of `Dependency` are provided by `DependencyImpl`.
//
// Operations of `Dependency<Ptr, Manager>`:
//
// ```
//   // Constructs a dummy `Manager` from
//   // `RiegeliDependencySentinel(static_cast<Manager*>(nullptr))`. Used
//   // when the host object is closed and does not need a dependent object.
//   //
//   // This constructor is optional.
//   Dependency();
//
//   // Copies or moves a `Manager`. Used to specify the initial value of the
//   // dependent object.
//   explicit Dependency(const Manager& manager);
//   explicit Dependency(Manager&& manager);
//
//   // Constructs a `Manager` from elements of `manager_args`. Used to specify
//   // the initial value of the dependent object. This avoids constructing a
//   // temporary `Manager` and moving from it.
//   //
//   // This constructor is optional.
//   template <typename... ManagerArgs>
//   explicit Dependency(std::tuple<ManagerArgs...> manager_args);
//
//   // Moves the dependency.
//   //
//   // Assignment operator is optional.
//   Dependency(Dependency&& that) noexcept;
//   Dependency& operator=(Dependency&& that) noexcept;
//
//   // Makes `*this` equivalent to a newly constructed Dependency. This avoids
//   // constructing a temporary Dependency and moving from it.
//   //
//   // These methods are optional.
//   ABSL_ATTRIBUTE_REINITIALIZES void Reset();
//   ABSL_ATTRIBUTE_REINITIALIZES void Reset(const Manager& manager);
//   ABSL_ATTRIBUTE_REINITIALIZES void Reset(Manager&& manager);
//   template <typename... ManagerArgs>
//   ABSL_ATTRIBUTE_REINITIALIZES void Reset(
//       std::tuple<ManagerArgs...> manager_args);
//
//   // Exposes the contained `Manager`.
//   Manager& manager();
//   const Manager& manager() const;
//
//   // Returns a `Ptr` to the `Manager`.
//   //
//   // This method might be const, and if `Ptr` is `P*` then this method might
//   // return a pointer to a derived class.
//   Ptr get();
//
//   // If `Ptr` is `P*`, `get()` has a const variant returning `const P*`,
//   // which is present if `get()` returning `P*` is not const.
//   //
//   // This method might return a pointer to a derived class.
//   const P* get() const;
//
//   // If `Ptr` is `P*`, `Dependency<P*, Manager>` can be used as a smart
//   // pointer to `P`, for convenience.
//   //
//   // These methods might be const, and might return a pointer to a derived
//   // class.
//   //
//   // These methods are provided by `Dependency` itself, not `DependencyImpl`.
//   P& operator*() { return *get(); }
//   const P& operator*() const { return *get(); }
//   P* operator->() { return get(); }
//   const P* operator->() const { return get(); }
//
//   // If `Ptr` is `P*`, or possibly another applicable type, the `Release()`
//   // function is present.
//   //
//   // If the `Dependency` owns the dependent object and can release it,
//   // `Release()` returns the released pointer, otherwise returns `nullptr`
//   // or another sentinel `Ptr` value.
//   //
//   // This method might return a pointer to a derived class.
//   Ptr Release();
//
//   // If `Ptr` is `P*`, the dependency can be compared against `nullptr`.
//   //
//   // These methods are provided by `Dependency` itself, not `DependencyImpl`.
//   friend bool operator==(const Dependency& a, nullptr_t) {
//     return a.get() == nullptr;
//   }
//   friend bool operator!=(const Dependency& a, nullptr_t) {
//     return a.get() != nullptr;
//   }
//   friend bool operator==(nullptr_t, const Dependency& b) {
//     return nullptr == b.get();
//   }
//   friend bool operator!=(nullptr_t, const Dependency& b) {
//     return nullptr != b.get();
//   }
//
//   // If `true`, the `Dependency` owns the dependent object, i.e. closing the
//   // host object should close the dependent object.
//   //
//   // This method is optional.
//   bool is_owning() const;
//
//   // If `true`, `get()` stays unchanged when a `Dependency` is moved.
//   static constexpr bool kIsStable;
// ```

// This template is specialized but does not have a primary definition.
template <typename Ptr, typename Manager, typename Enable = void>
class DependencyImpl;

namespace dependency_internal {

// `IsValidDependencyProbe<Ptr, Manager>::value` is `true` when
// `DependencyImpl<Ptr, Manager>` is defined, as determined by instantiating
// the `DependencyImpl` and calling `get()`.

template <typename Ptr, typename Manager, typename Enable = void>
struct IsValidDependencyProbe : std::false_type {};

template <typename Ptr, typename Manager>
struct IsValidDependencyProbe<
    Ptr, Manager,
    absl::void_t<decltype(std::declval<DependencyImpl<Ptr, Manager>&>().get())>>
    : std::true_type {};

}  // namespace dependency_internal

// `IsValidDependencyImpl<Ptr, Manager>::value` is `true` when
// `DependencyImpl<Ptr, Manager>` is defined.
//
// By default this is determined by instantiating the `DependencyImpl` and
// checking if `get()` is available. If that instantiation might have undesired
// side effects in contexts where `IsValidDependencyImpl` is needed,
// `IsValidDependencyImpl` can also be specialized explicitly.
template <typename Ptr, typename Manager, typename Enable = void>
struct IsValidDependencyImpl
    : dependency_internal::IsValidDependencyProbe<Ptr, Manager> {};

namespace dependency_internal {

// `DependencyMaybeRef<Ptr, Manager>` extends `DependencyImpl<Ptr, Manager>`
// with specializations when `Manager` is `M&` or `M&&`.
//
// If `DependencyImpl<Ptr, Manager>` is not specialized, `Manager` is delegated
// to `std::decay_t<M>`. This handles cases where the `Manager` type is deduced
// from a function parameter as a reference type to avoid moving the dependent
// object, but the function argument is not `P&` or `P&&` but e.g. `P*&` or
// `std::unique_ptr<P>&&`, intended to be treated as the pointer value.

// This template is specialized but does not have a primary definition.
template <typename Ptr, typename Manager, typename Enable = void>
class DependencyMaybeRef;

// Specialization of `DependencyMaybeRef<Ptr, Manager>` when
// `DependencyImpl<Ptr, Manager>` is defined.
template <typename Ptr, typename Manager>
class DependencyMaybeRef<
    Ptr, Manager, std::enable_if_t<IsValidDependencyImpl<Ptr, Manager>::value>>
    : public DependencyImpl<Ptr, Manager> {
  using DependencyMaybeRef::DependencyImpl::DependencyImpl;
};

// Specialization of `DependencyMaybeRef<Ptr, M&>` when
// `DependencyImpl<Ptr, M&>` is not defined: delegate to
// `DependencyImpl<Ptr, std::decay_t<M>>`.
template <typename Ptr, typename M>
class DependencyMaybeRef<
    Ptr, M&,
    std::enable_if_t<
        absl::conjunction<absl::negation<IsValidDependencyImpl<Ptr, M&>>,
                          IsValidDependencyImpl<Ptr, std::decay_t<M>>>::value>>
    : public DependencyImpl<Ptr, std::decay_t<M>> {
 public:
  explicit DependencyMaybeRef(M& manager) noexcept
      : DependencyMaybeRef::DependencyImpl(manager) {}

  DependencyMaybeRef(DependencyMaybeRef&& that) = default;
  DependencyMaybeRef& operator=(DependencyMaybeRef&&) = delete;
};

// Specialization of `DependencyMaybeRef<Ptr, M&&>` when
// `DependencyImpl<Ptr, M&&>` is not defined: delegate to
// `DependencyImpl<Ptr, std::decay_t<M>>`.
template <typename Ptr, typename M>
class DependencyMaybeRef<
    Ptr, M&&,
    std::enable_if_t<
        absl::conjunction<absl::negation<IsValidDependencyImpl<Ptr, M&&>>,
                          IsValidDependencyImpl<Ptr, std::decay_t<M>>>::value>>
    : public DependencyImpl<Ptr, std::decay_t<M>> {
 public:
  explicit DependencyMaybeRef(M&& manager) noexcept
      : DependencyMaybeRef::DependencyImpl(std::move(manager)) {}

  DependencyMaybeRef(DependencyMaybeRef&& that) = default;
  DependencyMaybeRef& operator=(DependencyMaybeRef&&) = delete;
};

}  // namespace dependency_internal

// `IsValidDependency<Ptr, Manager>::value` is `true` when
// `Dependency<Ptr, Manager>` is defined.

template <typename Ptr, typename Manager, typename Enable = void>
struct IsValidDependency : IsValidDependencyImpl<Ptr, Manager> {};

template <typename Ptr, typename M>
struct IsValidDependency<
    Ptr, M&,
    std::enable_if_t<IsValidDependencyImpl<Ptr, std::decay_t<M>>::value>>
    : std::true_type {};

template <typename Ptr, typename M>
struct IsValidDependency<
    Ptr, M&&,
    std::enable_if_t<IsValidDependencyImpl<Ptr, std::decay_t<M>>::value>>
    : std::true_type {};

// Implementation shared between most specializations of `DependencyImpl`.
template <typename Manager>
class DependencyBase {
 public:
  DependencyBase() noexcept
      : DependencyBase(
            RiegeliDependencySentinel(static_cast<Manager*>(nullptr))) {}

  template <typename DependentManager = Manager,
            std::enable_if_t<
                std::is_copy_constructible<DependentManager>::value, int> = 0>
  explicit DependencyBase(const Manager& manager) : manager_(manager) {}
  template <typename DependentManager = Manager,
            std::enable_if_t<
                std::is_move_constructible<DependentManager>::value, int> = 0>
  explicit DependencyBase(Manager&& manager) noexcept
      : manager_(std::move(manager)) {}

  template <typename... ManagerArgs>
  explicit DependencyBase(std::tuple<ManagerArgs...> manager_args)
      :
#if __cpp_guaranteed_copy_elision && __cpp_lib_make_from_tuple
        manager_(std::make_from_tuple<Manager>(std::move(manager_args)))
#else
        DependencyBase(std::move(manager_args),
                       std::index_sequence_for<ManagerArgs...>())
#endif
  {
  }

  ABSL_ATTRIBUTE_REINITIALIZES void Reset() {
    Reset(RiegeliDependencySentinel(static_cast<Manager*>(nullptr)));
  }

  template <typename DependentManager = Manager,
            std::enable_if_t<
                std::is_copy_constructible<DependentManager>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(const Manager& manager) {
    manager_ = manager;
  }
  template <typename DependentManager = Manager,
            std::enable_if_t<
                std::is_move_constructible<DependentManager>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Manager&& manager) {
    manager_ = std::move(manager);
  }

  template <typename... ManagerArgs>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      std::tuple<ManagerArgs...> manager_args) {
#if __cpp_lib_apply
    std::apply(
        [&](ManagerArgs&&... args) {
          riegeli::Reset(manager_, std::forward<ManagerArgs>(args)...);
        },
        std::move(manager_args));
#else
    ResetInternal(std::move(manager_args),
                  std::index_sequence_for<ManagerArgs...>());
#endif
  }

  Manager& manager() { return manager_; }
  const Manager& manager() const { return manager_; }

 protected:
  DependencyBase(DependencyBase&& that) = default;
  DependencyBase& operator=(DependencyBase&& that) = default;

  ~DependencyBase() = default;

 private:
#if !__cpp_guaranteed_copy_elision || !__cpp_lib_make_from_tuple
  template <typename... ManagerArgs, size_t... indices>
  explicit DependencyBase(std::tuple<ManagerArgs...>&& manager_args,
                          std::index_sequence<indices...>)
      : manager_(
            std::forward<ManagerArgs>(std::get<indices>(manager_args))...) {}
#endif

#if !__cpp_lib_apply
  template <typename... ManagerArgs, size_t... indices>
  void ResetInternal(std::tuple<ManagerArgs...>&& manager_args,
                     std::index_sequence<indices...>) {
    riegeli::Reset(manager_, std::forward<ManagerArgs>(
                                 std::get<indices>(manager_args))...);
  }
#endif

  Manager manager_;
};

template <typename Ptr, typename Manager, typename Enable = void>
class Dependency;

template <typename Ptr, typename Manager>
class Dependency<Ptr, Manager,
                 std::enable_if_t<IsValidDependency<Ptr, Manager>::value>>
    : public dependency_internal::DependencyMaybeRef<Ptr, Manager> {
 public:
  using Dependency::DependencyMaybeRef::DependencyMaybeRef;

  // If `Ptr` is `P*`, `Dependency<P*, Manager>` can be used as a smart pointer
  // to `P`, for convenience: it provides `operator*`, `operator->`, and can be
  // compared against `nullptr`.

  template <typename DependentPtr = Ptr,
            std::enable_if_t<std::is_pointer<DependentPtr>::value, int> = 0>
  std::remove_pointer_t<
      decltype(std::declval<
                   dependency_internal::DependencyMaybeRef<Ptr, Manager>>()
                   .get())>&
  operator*() {
    return *this->get();
  }
  template <typename DependentPtr = Ptr,
            std::enable_if_t<std::is_pointer<DependentPtr>::value, int> = 0>
  std::remove_pointer_t<
      decltype(std::declval<const dependency_internal::DependencyMaybeRef<
                   Ptr, Manager>>()
                   .get())>&
  operator*() const {
    return *this->get();
  }
  template <typename DependentPtr = Ptr,
            std::enable_if_t<std::is_pointer<DependentPtr>::value, int> = 0>
  decltype(std::declval<dependency_internal::DependencyMaybeRef<Ptr, Manager>>()
               .get())
  operator->() {
    return this->get();
  }
  template <typename DependentPtr = Ptr,
            std::enable_if_t<std::is_pointer<DependentPtr>::value, int> = 0>
  decltype(std::declval<
               const dependency_internal::DependencyMaybeRef<Ptr, Manager>>()
               .get())
  operator->() const {
    return this->get();
  }

  template <typename DependentPtr = Ptr,
            std::enable_if_t<std::is_pointer<DependentPtr>::value, int> = 0>
  friend bool operator==(const Dependency& a, nullptr_t) {
    return a.get() == nullptr;
  }
  template <typename DependentPtr = Ptr,
            std::enable_if_t<std::is_pointer<DependentPtr>::value, int> = 0>
  friend bool operator!=(const Dependency& a, nullptr_t) {
    return a.get() != nullptr;
  }
  template <typename DependentPtr = Ptr,
            std::enable_if_t<std::is_pointer<DependentPtr>::value, int> = 0>
  friend bool operator==(nullptr_t, const Dependency& b) {
    return nullptr == b.get();
  }
  template <typename DependentPtr = Ptr,
            std::enable_if_t<std::is_pointer<DependentPtr>::value, int> = 0>
  friend bool operator!=(nullptr_t, const Dependency& b) {
    return nullptr != b.get();
  }
};

// Specialization of `DependencyBase` for lvalue references.
//
// Only a subset of operations are provided: the dependency must be initialized,
// assignment is not supported, and initialization from a tuple of constructor
// arguments is not supported.
template <typename Manager>
class DependencyBase<Manager&> {
 public:
  explicit DependencyBase(Manager& manager) noexcept : manager_(manager) {}

  Manager& manager() const { return manager_; }

 protected:
  DependencyBase(DependencyBase&& that) = default;
  DependencyBase& operator=(DependencyBase&&) = delete;

  ~DependencyBase() = default;

 private:
  Manager& manager_;
};

// Specialization of `DependencyBase` for rvalue references.
//
// Only a subset of operations are provided: the dependency must be initialized,
// assignment is not supported, and initialization from a tuple of constructor
// arguments is not supported.
template <typename Manager>
class DependencyBase<Manager&&> {
 public:
  explicit DependencyBase(Manager&& manager) noexcept : manager_(manager) {}

  Manager& manager() const { return manager_; }

 protected:
  DependencyBase(DependencyBase&& that) = default;
  DependencyBase& operator=(DependencyBase&&) = delete;

  ~DependencyBase() = default;

 private:
  Manager& manager_;
};

// Specialization of `DependencyImpl<P*, M*>` when `M*` is convertible to `P*`:
// an unowned dependency passed by pointer.
template <typename P, typename M>
class DependencyImpl<P*, M*,
                     std::enable_if_t<std::is_convertible<M*, P*>::value>>
    : public DependencyBase<M*> {
 public:
  using DependencyImpl::DependencyBase::DependencyBase;

  M* get() const { return this->manager(); }
  M* Release() { return nullptr; }

  bool is_owning() const { return false; }
  static constexpr bool kIsStable = true;
};

// Specialization of `DependencyImpl<P*, nullptr_t>`: an unowned dependency
// passed by pointer, always missing. This is useful for `AnyDependency` and
// `AnyDependencyRef`.
template <typename P>
class DependencyImpl<P*, nullptr_t> : public DependencyBase<nullptr_t> {
 public:
  using DependencyImpl::DependencyBase::DependencyBase;

  nullptr_t get() const { return nullptr; }
  nullptr_t Release() { return nullptr; }

  bool is_owning() const { return false; }
  static constexpr bool kIsStable = true;
};

namespace dependency_internal {

template <typename Manager, typename Enable = void>
struct DereferencedForVoidPtr : std::false_type {};
template <typename M>
struct DereferencedForVoidPtr<M*> : std::true_type {};
template <>
struct DereferencedForVoidPtr<nullptr_t> : std::true_type {};
template <typename M, typename Deleter>
struct DereferencedForVoidPtr<std::unique_ptr<M, Deleter>> : std::true_type {};
template <typename M>
struct DereferencedForVoidPtr<std::reference_wrapper<M>> : std::true_type {};

}  // namespace dependency_internal

// Specialization of `DependencyImpl<P*, M>` when `M*` is convertible to `P*`:
// an owned dependency stored by value.
//
// If `P` is possibly cv-qualified `void`, then `Dependency<P*, Manager>`
// has an ambiguous interpretation for `Manager` being `M*`, `nullptr_t`,
// `std::unique_ptr<M, Deleter>`, or `std::reference_wrapper<M>`. The ambiguity
// is resolved in favor of pointing the `void*` to the dereferenced `M`, not to
// the `Manager` object itself.
template <typename P, typename M>
class DependencyImpl<
    P*, M,
    std::enable_if_t<absl::conjunction<
        std::is_convertible<M*, P*>,
        absl::negation<absl::conjunction<
            std::is_void<P>, dependency_internal::DereferencedForVoidPtr<
                                 std::decay_t<M>>>>>::value>>
    : public DependencyBase<M> {
 public:
  using DependencyImpl::DependencyBase::DependencyBase;

  M* get() { return &this->manager(); }
  const M* get() const { return &this->manager(); }
  M* Release() { return nullptr; }

  bool is_owning() const { return true; }
  static constexpr bool kIsStable = false;
};

// `IsValidDependencyImpl<P*, M>` when `M*` is convertible to `P*` is
// specialized explicitly for a subtle reason:
//
// Consider a type `T` like `BrotliReader<AnyDependency<Reader*>>`.
// Checking `IsValidDependency<Reader*, T>` by instantiating
// `DependencyImpl<Reader*, T>` would try to generate the copy constructor of
// `DependencyImpl<Reader*, T>`, which would try to copy `DependencyBase<T>`,
// which would consider not only its copy constructor but also its constructor
// from `const T&` (even though conversion from `DependencyBase<T>` to `T`
// would ultimately fail), which would check whether `T` is copy constructible,
// which would consider not only its deleted copy constructor but also its
// constructor from `const AnyDependency<Reader*>&`, which would check whether
// `T` is implicitly convertible to `AnyDependency<Reader*>`, which would check
// whether `IsValidDependency<Reader*, T>`, which is still in the process of
// being determined.
template <typename P, typename M>
struct IsValidDependencyImpl<
    P*, M,
    std::enable_if_t<absl::conjunction<
        std::is_convertible<M*, P*>,
        absl::negation<absl::conjunction<
            std::is_void<P>, dependency_internal::DereferencedForVoidPtr<
                                 std::decay_t<M>>>>>::value>> : std::true_type {
};

// Specialization of `DependencyImpl<P*, std::unique_ptr<M, Deleter>>` when `M*`
// is convertible to `P*`: an owned dependency stored by `std::unique_ptr`.
template <typename P, typename M, typename Deleter>
class DependencyImpl<P*, std::unique_ptr<M, Deleter>,
                     std::enable_if_t<std::is_convertible<M*, P*>::value>>
    : public DependencyBase<std::unique_ptr<M, Deleter>> {
 public:
  using DependencyImpl::DependencyBase::DependencyBase;

  M* get() const { return this->manager().get(); }
  M* Release() { return this->manager().release(); }

  bool is_owning() const { return this->manager() != nullptr; }
  static constexpr bool kIsStable = true;
};

// Specialization of `DependencyImpl<P*, M&>` when `M*` is convertible to `P*`:
// an unowned dependency passed by lvalue reference.
template <typename P, typename M>
class DependencyImpl<P*, M&,
                     std::enable_if_t<std::is_convertible<M*, P*>::value>>
    : public DependencyBase<M&> {
 public:
  using DependencyImpl::DependencyBase::DependencyBase;

  M* get() const { return &this->manager(); }
  M* Release() { return nullptr; }

  bool is_owning() const { return false; }
  static constexpr bool kIsStable = true;
};

// Specialization of `DependencyImpl<P*, M&&>` when `M*` is convertible to `P*`:
// an owned dependency passed by rvalue reference.
template <typename P, typename M>
class DependencyImpl<P*, M&&,
                     std::enable_if_t<std::is_convertible<M*, P*>::value>>
    : public DependencyBase<M&&> {
 public:
  using DependencyImpl::DependencyBase::DependencyBase;

  M* get() const { return &this->manager(); }
  M* Release() { return nullptr; }

  bool is_owning() const { return true; }
  static constexpr bool kIsStable = true;
};

// Specialization of `DependencyImpl<P*, std::reference_wrapper<M>>` when `M*`
// is convertible to `P*`: an owned dependency passed by `std::ref()`.
template <typename P, typename M>
class DependencyImpl<P*, std::reference_wrapper<M>,
                     std::enable_if_t<std::is_convertible<M*, P*>::value>>
    : public DependencyBase<std::reference_wrapper<M>> {
 public:
  using DependencyImpl::DependencyBase::DependencyBase;

  M* get() const { return &this->manager().get(); }
  M* Release() { return nullptr; }

  bool is_owning() const { return true; }
  static constexpr bool kIsStable = true;
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

template <typename Ptr, typename... ConstructorArgTypes>
class Dependency<Ptr, DeleteCtad<ConstructorArgTypes...>> {
  static_assert(dependency_internal::AlwaysFalse<ConstructorArgTypes...>::value,
                "Template arguments must be written explicitly "
                "with these constructor argument types");
};

// Specializations of `DependencyImpl<absl::string_view, Manager>`.

namespace string_view_internal {

inline absl::string_view ToStringView(absl::string_view value) { return value; }

// `absl::Span<const char>` is accepted with a template to avoid implicit
// conversions which can be ambiguous against `absl::string_view`
// (e.g. `std::string`).
template <typename T,
          std::enable_if_t<
              std::is_convertible<T, absl::Span<const char>>::value, int> = 0>
inline absl::string_view ToStringView(const T& value) {
  const absl::Span<const char> span = value;
  return absl::string_view(span.data(), span.size());
}

}  // namespace string_view_internal

// Specializations for `absl::string_view`, `absl::Span<const char>`,
// `absl::Span<char>`, `const char*`, and `char*` are defined separately for
// `kIsStable` to be `true`.

template <>
class DependencyImpl<absl::string_view, absl::string_view>
    : public DependencyBase<absl::string_view> {
 public:
  using DependencyImpl::DependencyBase::DependencyBase;

  absl::string_view get() const { return this->manager(); }

  static constexpr bool kIsStable = true;
};

template <>
class DependencyImpl<absl::string_view, absl::Span<const char>>
    : public DependencyBase<absl::Span<const char>> {
 public:
  using DependencyImpl::DependencyBase::DependencyBase;

  absl::string_view get() const {
    return string_view_internal::ToStringView(this->manager());
  }

  static constexpr bool kIsStable = true;
};

template <>
class DependencyImpl<absl::string_view, absl::Span<char>>
    : public DependencyBase<absl::Span<char>> {
 public:
  using DependencyImpl::DependencyBase::DependencyBase;

  absl::string_view get() const {
    return string_view_internal::ToStringView(this->manager());
  }

  static constexpr bool kIsStable = true;
};

template <>
class DependencyImpl<absl::string_view, const char*>
    : public DependencyBase<const char*> {
 public:
  using DependencyImpl::DependencyBase::DependencyBase;

  absl::string_view get() const {
    return string_view_internal::ToStringView(this->manager());
  }

  static constexpr bool kIsStable = true;
};

template <>
class DependencyImpl<absl::string_view, char*> : public DependencyBase<char*> {
 public:
  using DependencyImpl::DependencyBase::DependencyBase;

  absl::string_view get() const {
    return string_view_internal::ToStringView(this->manager());
  }

  static constexpr bool kIsStable = true;
};

template <typename M>
class DependencyImpl<
    absl::string_view, M*,
    std::enable_if_t<std::is_convertible<M, absl::string_view>::value ||
                     std::is_convertible<M, absl::Span<const char>>::value>>
    : public DependencyBase<M*> {
 public:
  using DependencyImpl::DependencyBase::DependencyBase;

  absl::string_view get() const {
    return string_view_internal::ToStringView(*this->manager());
  }

  static constexpr bool kIsStable = true;
};

template <typename M>
class DependencyImpl<
    absl::string_view, M,
    std::enable_if_t<std::is_convertible<M, absl::string_view>::value ||
                     std::is_convertible<M, absl::Span<const char>>::value>>
    : public DependencyBase<M> {
 public:
  using DependencyImpl::DependencyBase::DependencyBase;

  absl::string_view get() const {
    return string_view_internal::ToStringView(this->manager());
  }

  static constexpr bool kIsStable = false;
};

template <typename M, typename Deleter>
class DependencyImpl<
    absl::string_view, std::unique_ptr<M, Deleter>,
    std::enable_if_t<std::is_convertible<M, absl::string_view>::value ||
                     std::is_convertible<M, absl::Span<const char>>::value>>
    : public DependencyBase<std::unique_ptr<M, Deleter>> {
 public:
  using DependencyImpl::DependencyBase::DependencyBase;

  absl::string_view get() const {
    return string_view_internal::ToStringView(*this->manager());
  }

  static constexpr bool kIsStable = true;
};

// Specializations of `DependencyImpl<absl::Span<char>, Manager>`.

// Specialization for `absl::Span<char>` itself is defined separately for
// `kIsStable` to be `true`.
template <>
class DependencyImpl<absl::Span<char>, absl::Span<char>>
    : public DependencyBase<absl::Span<char>> {
 public:
  using DependencyImpl::DependencyBase::DependencyBase;

  absl::Span<char> get() const { return this->manager(); }

  static constexpr bool kIsStable = true;
};

template <typename M>
class DependencyImpl<
    absl::Span<char>, M*,
    std::enable_if_t<std::is_constructible<absl::Span<char>, M&>::value &&
                     !std::is_pointer<M>::value>> : public DependencyBase<M*> {
 public:
  using DependencyImpl::DependencyBase::DependencyBase;

  absl::Span<char> get() const { return absl::Span<char>(*this->manager()); }

  static constexpr bool kIsStable = true;
};

template <typename M>
class DependencyImpl<
    absl::Span<char>, M,
    std::enable_if_t<std::is_constructible<absl::Span<char>, M&>::value &&
                     !std::is_pointer<M>::value>> : public DependencyBase<M> {
 public:
  using DependencyImpl::DependencyBase::DependencyBase;

  absl::Span<char> get() { return absl::Span<char>(this->manager()); }
  absl::Span<const char> get() const {
    return absl::Span<const char>(this->manager());
  }

  static constexpr bool kIsStable = false;
};

template <typename M, typename Deleter>
class DependencyImpl<
    absl::Span<char>, std::unique_ptr<M, Deleter>,
    std::enable_if_t<std::is_constructible<absl::Span<char>, M&>::value &&
                     !std::is_pointer<M>::value>>
    : public DependencyBase<std::unique_ptr<M, Deleter>> {
 public:
  using DependencyImpl::DependencyBase::DependencyBase;

  absl::Span<char> get() const { return absl::Span<char>(*this->manager()); }

  static constexpr bool kIsStable = true;
};

}  // namespace riegeli

#endif  // RIEGELI_BASE_DEPENDENCY_H_
