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

#ifndef RIEGELI_BASE_DEPENDENCY_BASE_H_
#define RIEGELI_BASE_DEPENDENCY_BASE_H_

#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/utility/utility.h"
#include "riegeli/base/reset.h"

namespace riegeli {

// `RiegeliDependencySentinel(T*)` specifies how to initialize a default
// `Manager` (for `Dependency`) or `Handle` (for `AnyDependency`) of type `T`.
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

// Implementation shared between most specializations of `DependencyImpl` which
// store `manager()` in a member variable.
//
// Provides constructors, `Reset()`, and `manager()`.
template <typename Manager>
class DependencyBase {
 public:
  DependencyBase() noexcept
      : DependencyBase(
            RiegeliDependencySentinel(static_cast<Manager*>(nullptr))) {}

  template <typename DependentManager = Manager,
            std::enable_if_t<std::is_convertible<const DependentManager&,
                                                 DependentManager>::value,
                             int> = 0>
  explicit DependencyBase(const Manager& manager) : manager_(manager) {}
  template <typename DependentManager = Manager,
            std::enable_if_t<std::is_convertible<DependentManager&&,
                                                 DependentManager>::value,
                             int> = 0>
  explicit DependencyBase(Manager&& manager) noexcept
      : manager_(std::move(manager)) {}

  template <typename... ManagerArgs>
  explicit DependencyBase(std::tuple<ManagerArgs...> manager_args)
      :
#if __cpp_guaranteed_copy_elision
        manager_(absl::make_from_tuple<Manager>(std::move(manager_args)))
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
            std::enable_if_t<std::is_convertible<const DependentManager&,
                                                 DependentManager>::value,
                             int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(const Manager& manager) {
    manager_ = manager;
  }
  template <typename DependentManager = Manager,
            std::enable_if_t<std::is_convertible<DependentManager&&,
                                                 DependentManager>::value,
                             int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Manager&& manager) {
    manager_ = std::move(manager);
  }

  template <typename... ManagerArgs>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      std::tuple<ManagerArgs...> manager_args) {
    absl::apply(
        [&](ManagerArgs&&... args) {
          riegeli::Reset(manager_, std::forward<ManagerArgs>(args)...);
        },
        std::move(manager_args));
  }

  Manager& manager() { return manager_; }
  const Manager& manager() const { return manager_; }

 protected:
  DependencyBase(const DependencyBase& that) = default;
  DependencyBase& operator=(const DependencyBase& that) = default;

  DependencyBase(DependencyBase&& that) = default;
  DependencyBase& operator=(DependencyBase&& that) = default;

  ~DependencyBase() = default;

  Manager& mutable_manager() const { return manager_; }

 private:
#if !__cpp_guaranteed_copy_elision
  template <typename... ManagerArgs, size_t... indices>
  explicit DependencyBase(
      ABSL_ATTRIBUTE_UNUSED std::tuple<ManagerArgs...>&& manager_args,
      std::index_sequence<indices...>)
      : manager_(
            std::forward<ManagerArgs>(std::get<indices>(manager_args))...) {}
#endif

  mutable Manager manager_;
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
  DependencyBase(const DependencyBase& that) = default;
  DependencyBase& operator=(const DependencyBase&) = delete;

  ~DependencyBase() = default;

  Manager& mutable_manager() const { return manager_; }

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
  DependencyBase(const DependencyBase& that) = default;
  DependencyBase& operator=(const DependencyBase&) = delete;

  ~DependencyBase() = default;

  Manager& mutable_manager() const { return manager_; }

 private:
  Manager& manager_;
};

}  // namespace riegeli

#endif  // RIEGELI_BASE_DEPENDENCY_BASE_H_
