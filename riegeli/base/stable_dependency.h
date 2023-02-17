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

#include <stddef.h>

#include <atomic>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "riegeli/base/assert.h"
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

// Specialization when `Dependency<P*, M>` is already stable.
template <typename P, typename M>
class StableDependency<P*, M, std::enable_if_t<Dependency<P*, M>::kIsStable>>
    : public Dependency<P*, M> {
  using StableDependency::Dependency::Dependency;
};

// Specialization when `Dependency<P*, M>` is not stable: allocates the
// dependency dynamically.
template <typename P, typename M>
class StableDependency<P*, M, std::enable_if_t<!Dependency<P*, M>::kIsStable>>
    : public DependencyGetIfBase<StableDependency<P*, M>> {
 private:
  using DerivedP =
      std::remove_pointer_t<decltype(std::declval<Dependency<P*, M>&>().get())>;
  using ConstDerivedP = std::remove_pointer_t<
      decltype(std::declval<const Dependency<P*, M>&>().get())>;

 public:
  StableDependency() = default;

  template <
      typename DependentM = M,
      std::enable_if_t<std::is_copy_constructible<DependentM>::value, int> = 0>
  explicit StableDependency(const M& manager)
      : dep_(new Dependency<P*, M>(manager)) {}
  template <
      typename DependentM = M,
      std::enable_if_t<std::is_move_constructible<DependentM>::value, int> = 0>
  explicit StableDependency(M&& manager) noexcept
      : dep_(new Dependency<P*, M>(std::move(manager))) {}

  template <typename... MArgs>
  explicit StableDependency(std::tuple<MArgs...> manager_args)
      : dep_(new Dependency<P*, M>(std::move(manager_args))) {}

  StableDependency(StableDependency&& that) noexcept
      : dep_(that.dep_.exchange(nullptr, std::memory_order_relaxed)) {}
  StableDependency& operator=(StableDependency&& that) noexcept {
    delete dep_.exchange(that.dep_.exchange(nullptr, std::memory_order_relaxed),
                         std::memory_order_relaxed);
    return *this;
  }

  ~StableDependency() { delete dep_.load(std::memory_order_relaxed); }

  ABSL_ATTRIBUTE_REINITIALIZES void Reset() {
    Dependency<P*, M>* const dep = dep_.load(std::memory_order_relaxed);
    if (dep != nullptr) riegeli::Reset(*dep);
  }

  template <
      typename DependentM = M,
      std::enable_if_t<std::is_copy_constructible<DependentM>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(const M& manager) {
    Dependency<P*, M>* const dep = dep_.load(std::memory_order_relaxed);
    if (dep == nullptr) {
      // A race would violate the contract because this is not a const method.
      dep_.store(new Dependency<P*, M>(manager), std::memory_order_relaxed);
    } else {
      riegeli::Reset(*dep, manager);
    }
  }
  template <
      typename DependentM = M,
      std::enable_if_t<std::is_move_constructible<DependentM>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(M&& manager) {
    Dependency<P*, M>* const dep = dep_.load(std::memory_order_relaxed);
    if (dep == nullptr) {
      // A race would violate the contract because this is not a const method.
      dep_.store(new Dependency<P*, M>(std::move(manager)),
                 std::memory_order_relaxed);
    } else {
      riegeli::Reset(*dep, std::move(manager));
    }
  }

  template <typename... ManagerArgs>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      std::tuple<ManagerArgs...> manager_args) {
    Dependency<P*, M>* const dep = dep_.load(std::memory_order_relaxed);
    if (dep == nullptr) {
      // A race would violate the contract because this is not a const method.
      dep_.store(new Dependency<P*, M>(std::move(manager_args)),
                 std::memory_order_relaxed);
    } else {
      riegeli::Reset(*dep, std::move(manager_args));
    }
  }

  M& manager() { return EnsureAllocated()->manager(); }
  const M& manager() const { return EnsureAllocated()->manager(); }

  DerivedP* get() { return EnsureAllocated()->get(); }
  ConstDerivedP* get() const { return EnsureAllocated()->get(); }
  DerivedP& operator*() {
    DerivedP* const ptr = get();
    RIEGELI_ASSERT(ptr != nullptr)
        << "Failed precondition of StableDependency::operator*: null pointer";
    return *ptr;
  }
  ConstDerivedP& operator*() const {
    ConstDerivedP* const ptr = get();
    RIEGELI_ASSERT(ptr != nullptr)
        << "Failed precondition of StableDependency::operator*: null pointer";
    return *ptr;
  }
  DerivedP* operator->() {
    DerivedP* const ptr = get();
    RIEGELI_ASSERT(ptr != nullptr)
        << "Failed precondition of StableDependency::operator->: null pointer";
    return ptr;
  }
  ConstDerivedP* operator->() const {
    ConstDerivedP* const ptr = get();
    RIEGELI_ASSERT(ptr != nullptr)
        << "Failed precondition of StableDependency::operator->: null pointer";
    return ptr;
  }

  friend bool operator==(const StableDependency& a, nullptr_t) {
    return a.get() == nullptr;
  }
  friend bool operator!=(const StableDependency& a, nullptr_t) {
    return a.get() != nullptr;
  }
  friend bool operator==(nullptr_t, const StableDependency& a) {
    return nullptr == a.get();
  }
  friend bool operator!=(nullptr_t, const StableDependency& a) {
    return nullptr != a.get();
  }

  bool is_owning() const { return EnsureAllocated()->is_owning(); }

  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const StableDependency& self,
                                        MemoryEstimator& memory_estimator) {
    Dependency<P*, M>* const dep = self.dep_.load(std::memory_order_acquire);
    if (dep != nullptr) memory_estimator.RegisterDynamicObject(*dep);
  }

 private:
  Dependency<P*, M>* EnsureAllocated() const {
    Dependency<P*, M>* const dep = dep_.load(std::memory_order_acquire);
    if (ABSL_PREDICT_TRUE(dep != nullptr)) return dep;
    return EnsureAllocatedSlow();
  }

  Dependency<P*, M>* EnsureAllocatedSlow() const;

  // Owned. `nullptr` is equivalent to a default constructed `Dependency`.
  mutable std::atomic<Dependency<P*, M>*> dep_{nullptr};
};

// Implementation details follow.

template <typename P, typename M>
Dependency<P*, M>*
StableDependency<P*, M, std::enable_if_t<!Dependency<P*, M>::kIsStable>>::
    EnsureAllocatedSlow() const {
  Dependency<P*, M>* const dep = new Dependency<P*, M>();
  Dependency<P*, M>* other_dep = nullptr;
  if (ABSL_PREDICT_FALSE(!dep_.compare_exchange_strong(
          other_dep, dep, std::memory_order_acq_rel))) {
    // We lost the race.
    delete dep;
    return other_dep;
  }
  return dep;
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_STABLE_DEPENDENCY_H_
