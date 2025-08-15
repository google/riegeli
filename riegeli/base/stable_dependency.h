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
#include <memory>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/type_traits.h"

namespace riegeli {

// `StableDependency<Handle, Manager>` is similar to
// `Dependency<Handle, Manager>`, but ensures that `Handle` stays unchanged when
// the `StableDependency<Handle, Manager>` is moved.
//
// `StableDependency` can be used instead of `Dependency` if `Handle` stability
// is required, e.g. if background threads access the `Handle`.

// This template is specialized but does not have a primary definition.
template <typename Handle, typename Manager, typename Enable = void>
class StableDependency;

namespace dependency_internal {

template <typename Handle, typename Manager>
class StableDependencyDefault
    : public PropagateStaticIsOwning<Dependency<Handle, Manager>> {
 public:
  StableDependencyDefault() = default;

  explicit StableDependencyDefault(Initializer<Manager> manager)
      : dep_(new Dependency<Handle, Manager>(std::move(manager))),
        ensure_allocated_(AssumeAllocatedSlow) {}

  StableDependencyDefault(StableDependencyDefault&& that) noexcept
      : dep_(that.dep_.exchange(nullptr, std::memory_order_relaxed)),
        ensure_allocated_(
            std::exchange(that.ensure_allocated_, EnsureAllocatedSlow)) {}
  StableDependencyDefault& operator=(StableDependencyDefault&& that) noexcept {
    delete dep_.exchange(that.dep_.exchange(nullptr, std::memory_order_relaxed),
                         std::memory_order_relaxed);
    ensure_allocated_ =
        std::exchange(that.ensure_allocated_, EnsureAllocatedSlow);
    return *this;
  }

  ~StableDependencyDefault() { delete dep_.load(std::memory_order_relaxed); }

  ABSL_ATTRIBUTE_REINITIALIZES void Reset() {
    Dependency<Handle, Manager>* const dep =
        dep_.load(std::memory_order_relaxed);
    if (dep != nullptr) dep->Reset();
  }

  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Manager> manager) {
    Dependency<Handle, Manager>* const dep =
        dep_.load(std::memory_order_relaxed);
    if (dep == nullptr) {
      // A race would violate the contract because this is not a const method.
      dep_.store(new Dependency<Handle, Manager>(std::move(manager)),
                 std::memory_order_relaxed);
    } else {
      dep->Reset(std::move(manager));
    }
  }

  Manager& manager() ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return EnsureAllocated().manager();
  }
  const Manager& manager() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return EnsureAllocated().manager();
  }

  typename Dependency<Handle, Manager>::Subhandle get() const
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return EnsureAllocated().get();
  }

  bool IsOwning() const { return EnsureAllocated().IsOwning(); }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const StableDependencyDefault* self,
                                        MemoryEstimator& memory_estimator) {
    Dependency<Handle, Manager>* const dep =
        self->dep_.load(std::memory_order_acquire);
    if (dep != nullptr) memory_estimator.RegisterDynamicObject(dep);
  }

 private:
  Dependency<Handle, Manager>& EnsureAllocated() const {
    Dependency<Handle, Manager>* const dep =
        dep_.load(std::memory_order_acquire);
    if (ABSL_PREDICT_TRUE(dep != nullptr)) return *dep;
    return ensure_allocated_(*this);
  }

  static Dependency<Handle, Manager>& EnsureAllocatedSlow(
      const StableDependencyDefault& self);
  static Dependency<Handle, Manager>& AssumeAllocatedSlow(
      const StableDependencyDefault& self);

  // Owned. `nullptr` is equivalent to a default constructed `Dependency`.
  mutable std::atomic<Dependency<Handle, Manager>*> dep_ = nullptr;
  // Handles the case when `dep_ == nullptr`.
  //
  // The indirection allows initialization from `Initializer<Manager>` even if
  // `Dependency<Handle, Manager>` appears to be default-constructible but its
  // default constructor does not compile, by avoiding dead code which would
  // call the default constructor.
  //
  // Invariant:
  //   if `dep_ == nullptr` then `ensure_allocated_ == EnsureAllocatedSlow`
  Dependency<Handle, Manager>& (*ensure_allocated_)(
      const StableDependencyDefault&) = EnsureAllocatedSlow;
};

template <typename Handle, typename Manager>
class StableDependencyNoDefault
    : public PropagateStaticIsOwning<Dependency<Handle, Manager>> {
 public:
  explicit StableDependencyNoDefault(Initializer<Manager> manager)
      : dep_(
            std::make_unique<Dependency<Handle, Manager>>(std::move(manager))) {
  }

  StableDependencyNoDefault(StableDependencyNoDefault&& that) noexcept
      : dep_(std::make_unique<Dependency<Handle, Manager>>(
            std::move(*that.dep_))) {}
  StableDependencyNoDefault& operator=(
      StableDependencyNoDefault&& that) noexcept {
    *dep_ = std::move(*that.dep_);
    return *this;
  }

  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Manager> manager) {
    dep_->Reset(std::move(manager));
  }

  Manager& manager() ABSL_ATTRIBUTE_LIFETIME_BOUND { return dep_->manager(); }
  const Manager& manager() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return dep_->manager();
  }

  typename Dependency<Handle, Manager>::Subhandle get() const
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return dep_->get();
  }

  bool IsOwning() const { return dep_->IsOwning(); }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const StableDependencyNoDefault* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&self->dep_);
  }

 private:
  // Never `nullptr`.
  std::unique_ptr<Dependency<Handle, Manager>> dep_;
};

}  // namespace dependency_internal

// Specialization of `StableDependency<Handle, Manager>` when
// `Dependency<Handle, Manager>` is already stable: delegate to it.
template <typename Handle, typename Manager>
class
#ifdef ABSL_NULLABILITY_COMPATIBLE
    ABSL_NULLABILITY_COMPATIBLE
#endif
    StableDependency<Handle, Manager,
                     std::enable_if_t<Dependency<Handle, Manager>::kIsStable>>
    : public Dependency<Handle, Manager> {
 public:
  using StableDependency::Dependency::Dependency;

  StableDependency(StableDependency&& that) = default;
  StableDependency& operator=(StableDependency&& that) = default;
};

// Specialization of `StableDependency<Handle, Manager>` when
// `Dependency<Handle, Manager>` is not stable but default-constructible:
// allocate the dependency dynamically and conditionally.
template <typename Handle, typename Manager>
class
#ifdef ABSL_NULLABILITY_COMPATIBLE
    ABSL_NULLABILITY_COMPATIBLE
#endif
    StableDependency<
        Handle, Manager,
        std::enable_if_t<std::conjunction_v<
            std::bool_constant<!Dependency<Handle, Manager>::kIsStable>,
            std::is_default_constructible<Dependency<Handle, Manager>>>>>
    : public dependency_internal::DependencyDerived<
          dependency_internal::StableDependencyDefault<Handle, Manager>, Handle,
          Manager> {
 private:
  // For `ABSL_NULLABILITY_COMPATIBLE`.
  using pointer = std::conditional_t<std::is_pointer_v<Handle>, Handle, void*>;

 public:
  using StableDependency::DependencyDerived::DependencyDerived;

  StableDependency(StableDependency&& that) = default;
  StableDependency& operator=(StableDependency&& that) = default;
};

// Specialization of `StableDependency<Handle, Manager>` when
// `Dependency<Handle, Manager>` is not stable and not default-constructible:
// allocate the dependency dynamically and always keep it allocated.
template <typename Handle, typename Manager>
class
#ifdef ABSL_NULLABILITY_COMPATIBLE
    ABSL_NULLABILITY_COMPATIBLE
#endif
    StableDependency<
        Handle, Manager,
        std::enable_if_t<std::conjunction_v<
            std::bool_constant<!Dependency<Handle, Manager>::kIsStable>,
            std::negation<
                std::is_default_constructible<Dependency<Handle, Manager>>>>>>
    : public dependency_internal::DependencyDerived<
          dependency_internal::StableDependencyNoDefault<Handle, Manager>,
          Handle, Manager>,
      public CopyableLike<Dependency<Handle, Manager>> {
 private:
  // For `ABSL_NULLABILITY_COMPATIBLE`.
  using pointer = std::conditional_t<std::is_pointer_v<Handle>, Handle, void*>;

 public:
  using StableDependency::DependencyDerived::DependencyDerived;

  StableDependency(StableDependency&& that) = default;
  StableDependency& operator=(StableDependency&& that) = default;
};

// Implementation details follow.

namespace dependency_internal {

template <typename Handle, typename Manager>
Dependency<Handle, Manager>&
StableDependencyDefault<Handle, Manager>::EnsureAllocatedSlow(
    const StableDependencyDefault<Handle, Manager>& self) {
  Dependency<Handle, Manager>* const dep = new Dependency<Handle, Manager>();
  Dependency<Handle, Manager>* other_dep = nullptr;
  if (ABSL_PREDICT_FALSE(!self.dep_.compare_exchange_strong(
          other_dep, dep, std::memory_order_acq_rel))) {
    // We lost the race.
    delete dep;
    return *other_dep;
  }
  return *dep;
}

template <typename Handle, typename Manager>
Dependency<Handle, Manager>&
StableDependencyDefault<Handle, Manager>::AssumeAllocatedSlow(
    ABSL_ATTRIBUTE_UNUSED const StableDependencyDefault<Handle, Manager>&
        self) {
  RIEGELI_ASSUME_UNREACHABLE()
      << "Failed invariant of StableDependency: "
         "dep_ == nullptr but ensure_allocated_ == AssumeAllocatedSlow";
}

}  // namespace dependency_internal

}  // namespace riegeli

#endif  // RIEGELI_BASE_STABLE_DEPENDENCY_H_
