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

#include <stddef.h>

#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/meta/type_traits.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/reset.h"

namespace riegeli {

// `RiegeliDependencySentinel(T*)` specifies how to initialize a default
// `Manager` (for `Dependency`) or `Handle` (for `Any`) of type `T`.
//
// To customize that for a class `T`, define a free function
// `friend Result RiegeliDependencySentinel(T*)` as a friend of `T` inside class
// definition or in the same namespace as `T`, so that it can be found via ADL.
//
// `RiegeliDependencySentinel(T*)` returns a value convertible to
// `Initializer<T>`, usually a `MakerType<Args...>`.
//
// The argument of `RiegeliDependencySentinel(T*)` is always a null pointer,
// used to choose the right overload based on the type.

inline MakerType<> RiegeliDependencySentinel(void*) { return {}; }

// Implementation shared between most specializations of `DependencyManagerImpl`
// and `DependencyImpl` which store `manager()` in a member variable.
//
// `DependencyBase` provides constructors, `Reset()`, `manager()`, `kIsStable`,
// and protected `mutable_manager()`.
template <typename Manager>
class DependencyBase {
 public:
  template <typename DependentManager = Manager,
            std::enable_if_t<std::is_convertible<
                                 decltype(RiegeliDependencySentinel(
                                     static_cast<DependentManager*>(nullptr))),
                                 Initializer<DependentManager>>::value,
                             int> = 0>
  DependencyBase() noexcept
      : DependencyBase(
            RiegeliDependencySentinel(static_cast<Manager*>(nullptr))) {}

  explicit DependencyBase(Initializer<Manager> manager)
      : manager_(std::move(manager)) {}

  template <
      typename DependentManager = Manager,
      std::enable_if_t<
          absl::conjunction<
              std::is_convertible<decltype(RiegeliDependencySentinel(
                                      static_cast<DependentManager*>(nullptr))),
                                  Initializer<DependentManager>>,
              std::is_move_assignable<DependentManager>>::value,
          int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset() {
    Reset(RiegeliDependencySentinel(static_cast<Manager*>(nullptr)));
  }
  template <typename DependentManager = Manager,
            std::enable_if_t<std::is_move_assignable<DependentManager>::value,
                             int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Manager> manager) {
    riegeli::Reset(manager_, std::move(manager));
  }

  Manager& manager() ABSL_ATTRIBUTE_LIFETIME_BOUND { return manager_; }
  const Manager& manager() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return manager_;
  }

  static constexpr bool kIsStable = false;

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const DependencyBase* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&self->manager_);
  }

 protected:
  DependencyBase(const DependencyBase& that) = default;
  DependencyBase& operator=(const DependencyBase& that) = default;

  DependencyBase(DependencyBase&& that) = default;
  DependencyBase& operator=(DependencyBase&& that) = default;

  ~DependencyBase() = default;

  Manager& mutable_manager() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return manager_;
  }

 private:
  mutable Manager manager_;
};

// Specialization of `DependencyBase` for lvalue references.
//
// Only a subset of operations is provided: the dependency must be initialized,
// and assignment is not supported.
template <typename Manager>
class DependencyBase<Manager&> {
 public:
  explicit DependencyBase(Initializer<Manager&> manager) noexcept
      : manager_(std::move(manager)) {}

  Manager& manager() const ABSL_ATTRIBUTE_LIFETIME_BOUND { return manager_; }

  static constexpr bool kIsStable = true;

 protected:
  DependencyBase(const DependencyBase& that) = default;
  DependencyBase& operator=(const DependencyBase&) = delete;

  ~DependencyBase() = default;

  Manager& mutable_manager() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return manager_;
  }

 private:
  Manager& manager_;
};

// Specialization of `DependencyBase` for rvalue references.
//
// Only a subset of operations is provided: the dependency must be initialized,
// and assignment is not supported.
template <typename Manager>
class DependencyBase<Manager&&> {
 public:
  explicit DependencyBase(Initializer<Manager&&> manager) noexcept
      : manager_(std::move(manager)) {}

  Manager& manager() const ABSL_ATTRIBUTE_LIFETIME_BOUND { return manager_; }

  static constexpr bool kIsStable = true;

 protected:
  DependencyBase(DependencyBase&& that) = default;
  DependencyBase& operator=(DependencyBase&&) = delete;

  ~DependencyBase() = default;

  Manager& mutable_manager() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return manager_;
  }

 private:
  Manager&& manager_;
};

}  // namespace riegeli

#endif  // RIEGELI_BASE_DEPENDENCY_BASE_H_
