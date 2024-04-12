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
#include "riegeli/base/initializer.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/reset.h"

namespace riegeli {

// `RiegeliDependencySentinel(T*)` specifies how to initialize a default
// `Manager` (for `Dependency`) or `Handle` (for `AnyDependency`) of type `T`.
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
  DependencyBase() noexcept
      : DependencyBase(
            RiegeliDependencySentinel(static_cast<Manager*>(nullptr))) {}

  explicit DependencyBase(Initializer<Manager> manager)
      : manager_(std::move(manager).Construct()) {}

  ABSL_ATTRIBUTE_REINITIALIZES void Reset() {
    Reset(RiegeliDependencySentinel(static_cast<Manager*>(nullptr)));
  }
  template <typename DependentManager = Manager,
            std::enable_if_t<std::is_move_assignable<DependentManager>::value,
                             int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Manager> manager) {
    std::move(manager).AssignTo(manager_);
  }

  Manager& manager() { return manager_; }
  const Manager& manager() const { return manager_; }

  static constexpr bool kIsStable = false;

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

  Manager& mutable_manager() const { return manager_; }

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
      : manager_(std::move(manager).Construct()) {}

  Manager& manager() const { return manager_; }

  static constexpr bool kIsStable = true;

  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(
      ABSL_ATTRIBUTE_UNUSED const DependencyBase* self,
      ABSL_ATTRIBUTE_UNUSED MemoryEstimator& memory_estimator) {}

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
// Only a subset of operations is provided: the dependency must be initialized,
// and assignment is not supported.
template <typename Manager>
class DependencyBase<Manager&&> {
 public:
  explicit DependencyBase(Initializer<Manager&&> manager) noexcept
      : manager_(std::move(manager).Construct()) {}

  Manager& manager() const { return manager_; }

  static constexpr bool kIsStable = true;

  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(
      ABSL_ATTRIBUTE_UNUSED const DependencyBase* self,
      ABSL_ATTRIBUTE_UNUSED MemoryEstimator& memory_estimator) {}

 protected:
  DependencyBase(DependencyBase&& that) = default;
  DependencyBase& operator=(DependencyBase&&) = delete;

  ~DependencyBase() = default;

  Manager& mutable_manager() const { return manager_; }

 private:
  Manager&& manager_;
};

// Specialization of `DependencyBase` for arrays.
//
// Only a subset of operations is provided: default initialization
// value-initializes the array (`RiegeliDependencySentinel()` is not supported),
// and initialization from `Initializer<T[size]>` is not supported.
template <typename T, size_t size>
class DependencyBase<T[size]> {
 public:
  DependencyBase() noexcept : manager_() {}

  template <
      typename DependentT = T,
      std::enable_if_t<std::is_copy_constructible<DependentT>::value, int> = 0>
  explicit DependencyBase(const T (&manager)[size])
      : DependencyBase(manager, std::make_index_sequence<size>()) {}
  template <
      typename DependentT = T,
      std::enable_if_t<std::is_move_constructible<DependentT>::value, int> = 0>
  explicit DependencyBase(T (&&manager)[size]) noexcept
      : DependencyBase(std::move(manager), std::make_index_sequence<size>()) {}

  ABSL_ATTRIBUTE_REINITIALIZES void Reset() {
    for (T& element : manager_) {
      riegeli::Reset(element);
    }
  }

  template <
      typename DependentT = T,
      std::enable_if_t<std::is_copy_assignable<DependentT>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(const T (&manager)[size]) {
    for (size_t i = 0; i < size; ++i) {
      manager_[i] = manager[i];
    }
  }
  template <
      typename DependentT = T,
      std::enable_if_t<std::is_move_assignable<DependentT>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(T (&&manager)[size]) {
    for (size_t i = 0; i < size; ++i) {
      manager_[i] = std::move(manager[i]);
    }
  }

  auto manager() -> T (&)[size] { return manager_; }
  auto manager() const -> const T (&)[size] { return manager_; }

  static constexpr bool kIsStable = false;

  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const DependencyBase* self,
                                        MemoryEstimator& memory_estimator) {
    for (const T& element : self->manager_) {
      memory_estimator.RegisterSubobjects(&element);
    }
  }

 protected:
  DependencyBase(const DependencyBase& that) = default;
  DependencyBase& operator=(const DependencyBase& that) = delete;

  DependencyBase(DependencyBase&& that) = default;
  DependencyBase& operator=(DependencyBase&& that) = delete;

  ~DependencyBase() = default;

  auto mutable_manager() const -> T (&)[size] { return manager_; }

 private:
  template <size_t... indices>
  explicit DependencyBase(ABSL_ATTRIBUTE_UNUSED const T (&manager)[size],
                          std::index_sequence<indices...>)
      : manager_{manager[indices]...} {}
  template <size_t... indices>
  explicit DependencyBase(ABSL_ATTRIBUTE_UNUSED T (&&manager)[size],
                          std::index_sequence<indices...>)
      : manager_{std::move(manager[indices])...} {}

  mutable T manager_[size];
};

}  // namespace riegeli

#endif  // RIEGELI_BASE_DEPENDENCY_BASE_H_
