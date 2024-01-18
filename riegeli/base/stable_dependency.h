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

#ifndef RIEGELI_BASE_STABLE_DEPENDENCY_H_
#define RIEGELI_BASE_STABLE_DEPENDENCY_H_

#include <atomic>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/reset.h"

namespace riegeli {

// Similar to `Dependency<Ptr, Manager>`, but ensures that `Ptr` stays unchanged
// when `StableDependency<Ptr, Manager>` is moved. `StableDependency` can be
// used instead of `Dependency` if `Ptr` stability is required, e.g. if
// background threads access the `Ptr`.
//
// This template is specialized but does not have a primary definition.
template <typename Ptr, typename Manager, typename Enable = void>
class StableDependency;

// Specialization when `Dependency<Ptr, Manager>` is already stable.
template <typename Ptr, typename Manager>
class StableDependency<Ptr, Manager,
                       std::enable_if_t<Dependency<Ptr, Manager>::kIsStable>>
    : public Dependency<Ptr, Manager> {
 public:
  using StableDependency::Dependency::Dependency;

  StableDependency(StableDependency&& other) = default;
  StableDependency& operator=(StableDependency&& other) = default;
};

namespace dependency_internal {

template <typename Ptr, typename Manager>
class StableDependencyImpl {
 public:
  StableDependencyImpl() = default;

  template <typename DependentManager = Manager,
            std::enable_if_t<std::is_convertible<const DependentManager&,
                                                 DependentManager>::value,
                             int> = 0>
  explicit StableDependencyImpl(const Manager& manager)
      : dep_(new Dependency<Ptr, Manager>(manager)) {}
  template <typename DependentManager = Manager,
            std::enable_if_t<std::is_convertible<DependentManager&&,
                                                 DependentManager>::value,
                             int> = 0>
  explicit StableDependencyImpl(Manager&& manager) noexcept
      : dep_(new Dependency<Ptr, Manager>(std::move(manager))) {}

  template <typename... MArgs>
  explicit StableDependencyImpl(std::tuple<MArgs...> manager_args)
      : dep_(new Dependency<Ptr, Manager>(std::move(manager_args))) {}

  StableDependencyImpl(StableDependencyImpl&& that) noexcept
      : dep_(that.dep_.exchange(nullptr, std::memory_order_relaxed)) {}
  StableDependencyImpl& operator=(StableDependencyImpl&& that) noexcept {
    delete dep_.exchange(that.dep_.exchange(nullptr, std::memory_order_relaxed),
                         std::memory_order_relaxed);
    return *this;
  }

  ~StableDependencyImpl() { delete dep_.load(std::memory_order_relaxed); }

  ABSL_ATTRIBUTE_REINITIALIZES void Reset() {
    Dependency<Ptr, Manager>* const dep = dep_.load(std::memory_order_relaxed);
    if (dep != nullptr) riegeli::Reset(*dep);
  }

  template <typename DependentManager = Manager,
            std::enable_if_t<std::is_convertible<const DependentManager&,
                                                 DependentManager>::value,
                             int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(const Manager& manager) {
    Dependency<Ptr, Manager>* const dep = dep_.load(std::memory_order_relaxed);
    if (dep == nullptr) {
      // A race would violate the contract because this is not a const method.
      dep_.store(new Dependency<Ptr, Manager>(manager),
                 std::memory_order_relaxed);
    } else {
      riegeli::Reset(*dep, manager);
    }
  }
  template <typename DependentManager = Manager,
            std::enable_if_t<std::is_convertible<DependentManager&&,
                                                 DependentManager>::value,
                             int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Manager&& manager) {
    Dependency<Ptr, Manager>* const dep = dep_.load(std::memory_order_relaxed);
    if (dep == nullptr) {
      // A race would violate the contract because this is not a const method.
      dep_.store(new Dependency<Ptr, Manager>(std::move(manager)),
                 std::memory_order_relaxed);
    } else {
      riegeli::Reset(*dep, std::move(manager));
    }
  }

  template <typename... ManagerArgs>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      std::tuple<ManagerArgs...> manager_args) {
    Dependency<Ptr, Manager>* const dep = dep_.load(std::memory_order_relaxed);
    if (dep == nullptr) {
      // A race would violate the contract because this is not a const method.
      dep_.store(new Dependency<Ptr, Manager>(std::move(manager_args)),
                 std::memory_order_relaxed);
    } else {
      riegeli::Reset(*dep, std::move(manager_args));
    }
  }

  Manager& manager() { return EnsureAllocated()->manager(); }
  const Manager& manager() const { return EnsureAllocated()->manager(); }

  decltype(std::declval<Dependency<Ptr, Manager>&>().get()) get() {
    return EnsureAllocated()->get();
  }
  decltype(std::declval<const Dependency<Ptr, Manager>&>().get()) get() const {
    return EnsureAllocated()->get();
  }

  bool is_owning() const { return EnsureAllocated()->is_owning(); }

  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const StableDependencyImpl& self,
                                        MemoryEstimator& memory_estimator) {
    Dependency<Ptr, Manager>* const dep =
        self.dep_.load(std::memory_order_acquire);
    if (dep != nullptr) memory_estimator.RegisterDynamicObject(*dep);
  }

 private:
  Dependency<Ptr, Manager>* EnsureAllocated() const {
    Dependency<Ptr, Manager>* const dep = dep_.load(std::memory_order_acquire);
    if (ABSL_PREDICT_TRUE(dep != nullptr)) return dep;
    return EnsureAllocatedSlow();
  }

  Dependency<Ptr, Manager>* EnsureAllocatedSlow() const;

  // Owned. `nullptr` is equivalent to a default constructed `Dependency`.
  mutable std::atomic<Dependency<Ptr, Manager>*> dep_{nullptr};
};

}  // namespace dependency_internal

// Specialization when `Dependency<Ptr, Manager>` is not stable: allocates the
// dependency dynamically.
template <typename Ptr, typename Manager>
class StableDependency<Ptr, Manager,
                       std::enable_if_t<!Dependency<Ptr, Manager>::kIsStable>>
    : public dependency_internal::DependencyDerived<
          dependency_internal::StableDependencyImpl<Ptr, Manager>, Ptr,
          Manager> {
 public:
  using StableDependency::DependencyDerived::DependencyDerived;

  StableDependency(StableDependency&& other) = default;
  StableDependency& operator=(StableDependency&& other) = default;
};

// Implementation details follow.

namespace dependency_internal {

template <typename Ptr, typename Manager>
Dependency<Ptr, Manager>*
StableDependencyImpl<Ptr, Manager>::EnsureAllocatedSlow() const {
  Dependency<Ptr, Manager>* const dep = new Dependency<Ptr, Manager>();
  Dependency<Ptr, Manager>* other_dep = nullptr;
  if (ABSL_PREDICT_FALSE(!dep_.compare_exchange_strong(
          other_dep, dep, std::memory_order_acq_rel))) {
    // We lost the race.
    delete dep;
    return other_dep;
  }
  return dep;
}

}  // namespace dependency_internal

}  // namespace riegeli

#endif  // RIEGELI_BASE_STABLE_DEPENDENCY_H_
