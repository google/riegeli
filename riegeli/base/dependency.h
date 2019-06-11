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
#include "absl/utility/utility.h"
#include "riegeli/base/resetter.h"

namespace riegeli {

// A member of type Dependency<Ptr, Manager> specifies an optionally owned
// dependent object needed by the host object.
//
// Ptr is a non-owning type which refers to the dependent object, usually a
// pointer. Manager is a type which provides and possibly owns the dependent
// object.
//
// Typically the host class of Dependency<Ptr, Manager> is a class template
// parametrized by Manager, with Ptr fixed. A user of the host class specifies
// ownership of the dependent object, and sometimes narrows its type, by
// choosing the Manager type.
//
// Dependency<Ptr, Manager> uses Resetter<Manager>. An appropriate
// specialization of Resetter can be defined if a Manager can be made equivalent
// to a newly constructed Manager while avoiding constructing a temporary
// Manager and moving from it.
//
// The following operations are typically provided by specializations of
// Dependency<Ptr, Manager> (operations may differ depending on Ptr):
//
//   // Constructs a dummy Manager. This is used when the host object is closed
//   // and does not need a dependent object.
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
//   Dependency(Dependency&& that);
//   Dependency& operator=(Dependency&& that);
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
//   // If true, a Dependency owns the dependent object, i.e. the destructor of
//   // Dependency destroys that object.
//   bool is_owning() const;
//
//   // If true, get() stays unchanged when a Dependency is moved.
//   static constexpr bool kIsStable();
template <typename Ptr, typename Manager, typename Enable = void>
class Dependency;

// IsValidDependency<Ptr, Manager>::value is true when Dependency<Ptr, Manager>
// is defined.

template <typename Ptr, typename Manager, typename Enable = void>
struct IsValidDependency : public std::false_type {};

template <typename Ptr, typename Manager>
struct IsValidDependency<
    Ptr, Manager,
    absl::void_t<decltype(std::declval<Dependency<Ptr, Manager>>().get())>>
    : public std::true_type {};

// Implementation shared between most specializations of Dependency.
template <typename Manager>
class DependencyBase {
 public:
  DependencyBase() noexcept : manager_() {}

  explicit DependencyBase(const Manager& manager) : manager_(manager) {}
  explicit DependencyBase(Manager&& manager) noexcept
      : manager_(std::move(manager)) {}

  template <typename... ManagerArgs>
  explicit DependencyBase(std::tuple<ManagerArgs...> manager_args)
      : DependencyBase(std::move(manager_args),
                       absl::index_sequence_for<ManagerArgs...>()) {}

  DependencyBase(DependencyBase&& that) noexcept
      : manager_(std::move(that.manager_)) {}
  DependencyBase& operator=(DependencyBase&& that) noexcept {
    manager_ = std::move(that.manager_);
    return *this;
  }

  void Reset() { Resetter<Manager>::Reset(&manager_); }

  void Reset(const Manager& manager) { manager_ = manager; }
  void Reset(Manager&& manager) { manager_ = std::move(manager); }

  template <typename... ManagerArgs>
  void Reset(std::tuple<ManagerArgs...> manager_args) {
    Reset(std::move(manager_args), absl::index_sequence_for<ManagerArgs...>());
  }

  Manager& manager() { return manager_; }
  const Manager& manager() const { return manager_; }

 private:
  template <typename... ManagerArgs, size_t... Indices>
  explicit DependencyBase(std::tuple<ManagerArgs...>&& manager_args,
                          absl::index_sequence<Indices...>)
      : manager_(std::get<Indices>(std::move(manager_args))...) {}

  template <typename... ManagerArgs, size_t... Indices>
  void Reset(std::tuple<ManagerArgs...>&& manager_args,
             absl::index_sequence<Indices...>) {
    Resetter<Manager>::Reset(&manager_,
                             std::get<Indices>(std::move(manager_args))...);
  }

  Manager manager_;
};

// Specialization of Dependency<P*, M*> when M* is convertible to P*.
template <typename P, typename M>
class Dependency<P*, M*, absl::enable_if_t<std::is_convertible<M*, P*>::value>>
    : public DependencyBase<M*> {
 public:
  using DependencyBase<M*>::DependencyBase;

  P* get() const { return this->manager(); }
  P& operator*() const { return *get(); }
  P* operator->() const { return get(); }

  bool is_owning() const { return false; }
  static constexpr bool kIsStable() { return true; }
};

// Specialization of Dependency<P*, M> when M* is convertible to P*.
template <typename P, typename M>
class Dependency<P*, M, absl::enable_if_t<std::is_convertible<M*, P*>::value>>
    : public DependencyBase<M> {
 public:
  using DependencyBase<M>::DependencyBase;

  P* get() { return &this->manager(); }
  const P* get() const { return &this->manager(); }
  P& operator*() { return *get(); }
  const P& operator*() const { return *get(); }
  P* operator->() { return get(); }
  const P* operator->() const { return get(); }

  bool is_owning() const { return true; }
  static constexpr bool kIsStable() { return false; }
};

// Specialization of Dependency<P*, unique_ptr<M>> when M* is convertible to P*.
template <typename P, typename M, typename Deleter>
class Dependency<P*, std::unique_ptr<M, Deleter>,
                 absl::enable_if_t<std::is_convertible<M*, P*>::value>>
    : public DependencyBase<std::unique_ptr<M, Deleter>> {
 public:
  using DependencyBase<std::unique_ptr<M, Deleter>>::DependencyBase;

  P* get() const { return this->manager().get(); }
  P& operator*() const { return *get(); }
  P* operator->() const { return get(); }

  bool is_owning() const { return true; }
  static constexpr bool kIsStable() { return true; }
};

}  // namespace riegeli

#endif  // RIEGELI_BASE_DEPENDENCY_H_
