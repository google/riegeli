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

#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/meta/type_traits.h"
#include "riegeli/base/object.h"
#include "riegeli/base/reset.h"

namespace riegeli {

// A member of type `Dependency<Ptr, Manager>` specifies an optionally owned
// dependent object needed by the host object.
//
// `Ptr` is a non-owning type which refers to the dependent object, usually a
// pointer. `Manager` is a type which provides and possibly owns the dependent
// object.
//
// Typically the host class of `Dependency<Ptr, Manager>` is a class template
// parametrized by `Manager`, with `Ptr` fixed. A user of the host class
// specifies ownership of the dependent object, and sometimes narrows its type,
// by choosing the `Manager` type.
//
// The following operations are typically provided by specializations of
// `Dependency<Ptr, Manager>` (operations may differ depending on `Ptr`;
// whenever `Ptr` is returned, a pointer to a derived class may be returned):
//
// ```
//   // Constructs a dummy Manager: constructed with kClosed if it supports
//   // that, otherwise default-constructed; nullptr for pointers. This is used
//   // when the host object is closed and does not need a dependent object.
//   Dependency();
//
//   // Copies or moves a Manager. This is used to specify the initial value of
//   // the dependent object.
//   explicit Dependency(const Manager& manager);
//   explicit Dependency(Manager&& manager);
//
//   // Constructs a Manager from elements of manager_args. This is used to
//   // specify the initial value of the dependent object. This avoids
//   // constructing a temporary Manager and moving from it.
//   template <typename... ManagerArgs>
//   explicit Dependency(std::tuple<ManagerArgs...> manager_args);
//
//   // Moves the dependency.
//   Dependency(Dependency&& that) noexcept;
//   Dependency& operator=(Dependency&& that) noexcept;
//
//   // Makes *this equivalent to a newly constructed Dependency. This avoids
//   // constructing a temporary Dependency and moving from it.
//   void Reset();
//   void Reset(const Manager& manager);
//   void Reset(Manager&& manager);
//   template <typename... ManagerArgs>
//   void Reset(std::tuple<ManagerArgs...> manager_args);
//
//   // Exposes the contained Manager.
//   Manager& manager();
//   const Manager& manager() const;
//
//   // Returns a Ptr to the Manager.
//   //
//   // A const variant of this method is expected for certain choices of Ptr,
//   // in particular if Ptr is P*.
//   Ptr get();
//
//   // If Ptr is P*, Dependency<P*, Manager> can be used as a smart pointer to
//   // P, for convenience.
//   P& operator*() { return *get(); }
//   const P& operator*() const { return *get(); }
//   P* operator->() { return get(); }
//   const P* operator->() const { return get(); }
//
//   // If Ptr is P*, the Release() function is present.
//   //
//   // If the Dependency owns the dependent object and can release it,
//   // Release() returns the released pointer, otherwise returns nullptr.
//   P* Release();
//
//   // If true, the Dependency owns the dependent object, i.e. closing the host
//   // object should close the dependent object.
//   bool is_owning() const;
//
//   // If true, get() stays unchanged when a Dependency is moved.
//   //
//   // If Ptr is P* and kIsStable() is false, get() must never be nullptr.
//   // This avoids callers having to consider cases which never occur in
//   // practice.
//   static constexpr bool kIsStable();
// ```

// `Manager` can also be an lvalue reference or rvalue reference. This case
// is meant to be used only when the dependency is constructed locally in a
// function, rather than stored in a host object, because such a dependency
// stores the pointer to the dependent object, and by convention a reference
// argument is expected to be valid only for the duration of the function call.
// Typically the `Manager` type is deduced from a function argument.
//
// This case allows to pass an unowned dependency by lvalue reference instead of
// by pointer, which allows for a more idiomatic API for passing an object which
// does not need to be valid after the function returns. And this allows to pass
// an owned dependency by rvalue reference instead of by value, which avoids
// moving it.
//
// Only a subset of operations is provided in this case: the dependency must be
// initialized during construction, and initialization from a tuple of
// constructor arguments is not supported.

// This template is specialized but does not have a primary definition.
template <typename Ptr, typename Manager, typename Enable = void>
class Dependency;

// `IsValidDependency<Ptr, Manager>::value` is `true` when
// `Dependency<Ptr, Manager>` is defined.

template <typename Ptr, typename Manager, typename Enable = void>
struct IsValidDependency : std::false_type {};

template <typename Ptr, typename Manager>
struct IsValidDependency<
    Ptr, Manager,
    absl::void_t<decltype(std::declval<Dependency<Ptr, Manager>>().get())>>
    : std::true_type {};

// Implementation shared between most specializations of `Dependency`.
template <typename Manager>
class DependencyBase {
 public:
  template <typename T = Manager,
            std::enable_if_t<std::is_constructible<T, Closed>::value, int> = 0>
  DependencyBase() noexcept : manager_(kClosed) {}

  template <typename T = Manager,
            std::enable_if_t<!std::is_constructible<T, Closed>::value, int> = 0>
  DependencyBase() noexcept : manager_() {}

  explicit DependencyBase(const Manager& manager) : manager_(manager) {}
  explicit DependencyBase(Manager&& manager) noexcept
      : manager_(std::move(manager)) {}

  template <typename... ManagerArgs>
  explicit DependencyBase(std::tuple<ManagerArgs...> manager_args)
      : DependencyBase(std::move(manager_args),
                       std::index_sequence_for<ManagerArgs...>()) {}

  DependencyBase(DependencyBase&& that) noexcept
      : manager_(std::move(that.manager_)) {}
  DependencyBase& operator=(DependencyBase&& that) noexcept {
    manager_ = std::move(that.manager_);
    return *this;
  }

  template <typename T = Manager,
            std::enable_if_t<std::is_constructible<T, Closed>::value, int> = 0>
  void Reset() {
    riegeli::Reset(manager_, kClosed);
  }

  template <typename T = Manager,
            std::enable_if_t<!std::is_constructible<T, Closed>::value, int> = 0>
  void Reset() {
    riegeli::Reset(manager_);
  }

  void Reset(const Manager& manager) { manager_ = manager; }
  void Reset(Manager&& manager) { manager_ = std::move(manager); }

  template <typename... ManagerArgs>
  void Reset(std::tuple<ManagerArgs...> manager_args) {
    ResetInternal(std::move(manager_args),
                  std::index_sequence_for<ManagerArgs...>());
  }

  Manager& manager() { return manager_; }
  const Manager& manager() const { return manager_; }

 private:
  template <typename... ManagerArgs, size_t... Indices>
  explicit DependencyBase(std::tuple<ManagerArgs...>&& manager_args,
                          std::index_sequence<Indices...>)
      : manager_(
            std::forward<ManagerArgs>(std::get<Indices>(manager_args))...) {}

  template <typename... ManagerArgs, size_t... Indices>
  void ResetInternal(std::tuple<ManagerArgs...>&& manager_args,
                     std::index_sequence<Indices...>) {
    riegeli::Reset(manager_, std::forward<ManagerArgs>(
                                 std::get<Indices>(manager_args))...);
  }

  Manager manager_;
};

// Specialization of `DependencyBase` for lvalue references.
//
// Only a subset of operations are provided: the dependency must be initialized,
// and initialization from a tuple of constructor arguments is not supported.
template <typename Manager>
class DependencyBase<Manager&> {
 public:
  explicit DependencyBase(Manager& manager) noexcept : manager_(&manager) {}

  DependencyBase(DependencyBase&& that) noexcept : manager_(that.manager_) {}
  DependencyBase& operator=(DependencyBase&& that) noexcept {
    manager_ = that.manager_;
    return *this;
  }

  void Reset(Manager& manager) { manager_ = &manager; }

  Manager& manager() const { return *manager_; }

 private:
  Manager* manager_;
};

// Specialization of `DependencyBase` for rvalue references.
//
// Only a subset of operations are provided: the dependency must be initialized,
// and initialization from a tuple of constructor arguments is not supported.
template <typename Manager>
class DependencyBase<Manager&&> {
 public:
  explicit DependencyBase(Manager&& manager) noexcept : manager_(&manager) {}

  DependencyBase(DependencyBase&& that) noexcept : manager_(that.manager_) {}
  DependencyBase& operator=(DependencyBase&& that) noexcept {
    manager_ = that.manager_;
    return *this;
  }

  void Reset(Manager&& manager) { manager_ = &manager; }

  Manager& manager() const { return *manager_; }

 private:
  Manager* manager_;
};

// Specialization of `Dependency<P*, M*>` when `M*` is convertible to `P*`:
// an unowned dependency passed by pointer.
template <typename P, typename M>
class Dependency<P*, M*, std::enable_if_t<std::is_convertible<M*, P*>::value>>
    : public DependencyBase<M*> {
 public:
  using DependencyBase<M*>::DependencyBase;

  M* get() const { return this->manager(); }
  M& operator*() const { return *get(); }
  M* operator->() const { return get(); }
  M* Release() { return nullptr; }

  bool is_owning() const { return false; }
  static constexpr bool kIsStable() { return true; }
};

// Specialization of `Dependency<P*, M>` when `M*` is convertible to `P*`:
// an owned dependency stored by value.
template <typename P, typename M>
class Dependency<P*, M, std::enable_if_t<std::is_convertible<M*, P*>::value>>
    : public DependencyBase<M> {
 public:
  using DependencyBase<M>::DependencyBase;

  M* get() { return &this->manager(); }
  const M* get() const { return &this->manager(); }
  M& operator*() { return *get(); }
  const M& operator*() const { return *get(); }
  M* operator->() { return get(); }
  const M* operator->() const { return get(); }
  M* Release() { return nullptr; }

  bool is_owning() const { return true; }
  static constexpr bool kIsStable() { return false; }
};

// Specialization of `Dependency<P*, std::unique_ptr<M>>` when `M*` is
// convertible to `P*`: an owned dependency stored by `std::unique_ptr`.
template <typename P, typename M, typename Deleter>
class Dependency<P*, std::unique_ptr<M, Deleter>,
                 std::enable_if_t<std::is_convertible<M*, P*>::value>>
    : public DependencyBase<std::unique_ptr<M, Deleter>> {
 public:
  using DependencyBase<std::unique_ptr<M, Deleter>>::DependencyBase;

  M* get() const { return this->manager().get(); }
  M& operator*() const { return *get(); }
  M* operator->() const { return get(); }
  M* Release() { return this->manager().release(); }

  bool is_owning() const { return this->manager() != nullptr; }
  static constexpr bool kIsStable() { return true; }
};

// Specialization of `Dependency<P*, M&>` when `M*` is convertible to `P*`:
// an unowned dependency passed by lvalue reference.
template <typename P, typename M>
class Dependency<P*, M&, std::enable_if_t<std::is_convertible<M*, P*>::value>>
    : public DependencyBase<M&> {
 public:
  using DependencyBase<M&>::DependencyBase;

  M* get() const { return &this->manager(); }
  M& operator*() const { return *get(); }
  M* operator->() const { return get(); }
  M* Release() { return nullptr; }

  bool is_owning() const { return false; }
  static constexpr bool kIsStable() { return true; }
};

// Specialization of `Dependency<P*, M&>` when `M*` is not convertible to `P*`:
// decay to `Dependency<P*, std::decay_t<M>>`.
template <typename P, typename M>
class Dependency<P*, M&, std::enable_if_t<!std::is_convertible<M*, P*>::value>>
    : public Dependency<P*, std::decay_t<M>> {
 public:
  using Dependency<P*, std::decay_t<M>>::Dependency;
};

// Specialization of `Dependency<P*, M&&>` when `M*` is convertible to `P*`:
// an owned dependency passed by rvalue reference.
template <typename P, typename M>
class Dependency<P*, M&&, std::enable_if_t<std::is_convertible<M*, P*>::value>>
    : public DependencyBase<M&&> {
 public:
  using DependencyBase<M&&>::DependencyBase;

  M* get() const { return &this->manager(); }
  M& operator*() const { return *get(); }
  M* operator->() const { return get(); }
  M* Release() { return nullptr; }

  bool is_owning() const { return true; }
  static constexpr bool kIsStable() { return true; }
};

// Specialization of `Dependency<P*, M&&>` when `M*` is not convertible to `P*`:
// decay to `Dependency<P*, std::decay_t<M>>`.
template <typename P, typename M>
class Dependency<P*, M&&, std::enable_if_t<!std::is_convertible<M*, P*>::value>>
    : public Dependency<P*, std::decay_t<M>> {
 public:
  using Dependency<P*, std::decay_t<M>>::Dependency;
};

namespace internal {

// `AlwaysFalse<T...>::value` is `false`, but formally depends on `T...`.
// This is useful for `static_assert()`.

template <typename... T>
struct AlwaysFalse : std::false_type {};

}  // namespace internal

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
  static_assert(internal::AlwaysFalse<ConstructorArgTypes...>::value,
                "Template arguments must be written explicitly "
                "with these constructor argument types");
};

}  // namespace riegeli

#endif  // RIEGELI_BASE_DEPENDENCY_H_
