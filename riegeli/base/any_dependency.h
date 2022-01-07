// Copyright 2022 Google LLC
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

#ifndef RIEGELI_BASE_ANY_DEPENDENCY_H_
#define RIEGELI_BASE_ANY_DEPENDENCY_H_

#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/meta/type_traits.h"
#include "absl/utility/utility.h"
#include "riegeli/base/dependency.h"

namespace riegeli {

// `AnyDependencyTraits<Ptr>` can be specialized to override the default value
// of `Ptr` used by `AnyDependency<Ptr>`.

template <typename Ptr>
struct AnyDependencyTraits {
  static Ptr DefaultPtr() { return Ptr(); }
};

// `AnyDependency<Ptr>` holds a `Dependency<Ptr, Manager>` for some `Manager`
// type, erasing the `Manager` type from the type of the `AnyDependency`, or is
// empty.
template <typename Ptr>
class AnyDependency {
 public:
  // Creates an empty `AnyDependency`.
  AnyDependency() noexcept {}

  // Holds a `Dependency<Ptr, std::decay_t<Manager>>`.
  //
  // The `Manager` type is deduced from the constructor argument.
  template <typename Manager,
            std::enable_if_t<
                absl::conjunction<
                    absl::negation<std::is_same<std::decay_t<Manager>,
                                                AnyDependency<Ptr>>>,
                    IsValidDependency<Ptr, std::decay_t<Manager>>>::value,
                int> = 0>
  explicit AnyDependency(const Manager& manager)
      : impl_(std::make_unique<Impl<std::decay_t<Manager>>>(manager)) {}
  template <typename Manager,
            std::enable_if_t<
                absl::conjunction<
                    absl::negation<std::is_same<std::decay_t<Manager>,
                                                AnyDependency<Ptr>>>,
                    IsValidDependency<Ptr, std::decay_t<Manager>>>::value,
                int> = 0>
  explicit AnyDependency(Manager&& manager)
      : impl_(std::make_unique<Impl<std::decay_t<Manager>>>(
            std::forward<Manager>(manager))) {}

  // Holds a `Dependency<Ptr, Manager>`.
  //
  // The `Manager` type is specified with a tag (`absl::in_place_type<Manager>`)
  // because constructor templates do not support specifying template arguments
  // explicitly.
  template <typename Manager>
  explicit AnyDependency(absl::in_place_type_t<Manager>, const Manager& manager)
      : impl_(std::make_unique<Impl<Manager>>(manager)) {}
  template <typename Manager>
  explicit AnyDependency(absl::in_place_type_t<Manager>, Manager&& manager)
      : impl_(std::make_unique<Impl<Manager>>(std::move(manager))) {}
  template <typename Manager, typename... ManagerArgs>
  explicit AnyDependency(absl::in_place_type_t<Manager>,
                         std::tuple<ManagerArgs...> manager_args)
      : impl_(std::make_unique<Impl<Manager>>(std::move(manager_args))) {}

  AnyDependency(AnyDependency&& that) noexcept;
  AnyDependency& operator=(AnyDependency&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `AnyDependency`. This
  // avoids constructing a temporary `AnyDependency` and moving from it.
  void Reset() { impl_.reset(); }
  template <typename Manager>
  void Reset(const Manager& manager) {
    impl_ = std::make_unique<Impl<std::decay_t<Manager>>>(manager);
  }
  template <typename Manager>
  void Reset(Manager&& manager) {
    impl_ = std::make_unique<Impl<std::decay_t<Manager>>>(
        std::forward<Manager>(manager));
  }
  template <typename Manager, typename... ManagerArgs>
  void Reset(std::tuple<ManagerArgs...> manager_args) {
    impl_ = std::make_unique<Impl<Manager>>(std::move(manager_args));
  }

  // Returns a `Ptr` to the `Manager`, or a default `Ptr` for an empty
  // `AnyDependency`.
  //
  // A caveat regarding const:
  //
  // This `get()` is a const method, even though it does not require the
  // `Dependency<Ptr, Manager>::get()` to be const.
  //
  // This is because there are two variants of `Dependency<P*, Manager>`
  // specializations. `Dependency<P*, P>` stores `P` by value and thus
  // provides `P* get()` and `const P* get() const`, while some users of
  // `Dependency<P*, Manager>` do not support a `Manager` storing `P` by value
  // anyway and expect `P* get() const` to be available.
  //
  // To avoid having two variants of `AnyDependency<P*>` based on this subtle
  // distinction, its only variant is more permissive regarding the `Dependency`
  // while also more permissive regarding its usage.
  Ptr get() const {
    if (ABSL_PREDICT_FALSE(impl_ == nullptr)) {
      return AnyDependencyTraits<Ptr>::DefaultPtr();
    }
    return impl_->get();
  }

  // If `Ptr` is `P*`, `AnyDependency<P*>` can be used as a smart pointer to
  // `P`, for convenience.
  template <typename DependentPtr = Ptr,
            std::enable_if_t<std::is_pointer<DependentPtr>::value, int> = 0>
  std::remove_pointer_t<DependentPtr>& operator*() const {
    return *get();
  }
  template <typename DependentPtr = Ptr,
            std::enable_if_t<std::is_pointer<DependentPtr>::value, int> = 0>
  std::remove_pointer_t<DependentPtr>* operator->() const {
    return get();
  }

  // Returns the released `Ptr` if the `Dependency` owns the dependent object
  // and can release it, otherwise returns a default `Ptr`.
  Ptr Release() {
    if (ABSL_PREDICT_FALSE(impl_ == nullptr)) {
      return AnyDependencyTraits<Ptr>::DefaultPtr();
    }
    return impl_->Release();
  }

  // If `true`, the `AnyDependency` owns the dependent object, i.e. closing the
  // host object should close the dependent object.
  bool is_owning() const {
    if (ABSL_PREDICT_FALSE(impl_ == nullptr)) return false;
    return impl_->is_owning();
  }

 private:
  class ImplBase {
   public:
    virtual ~ImplBase() {}

    virtual Ptr get() = 0;
    virtual Ptr Release() = 0;
    virtual bool is_owning() const = 0;
  };

  template <typename Manager>
  class Impl : public ImplBase {
   public:
    explicit Impl(const Manager& manager) : dependency_(manager) {}
    explicit Impl(Manager&& manager) : dependency_(std::move(manager)) {}
    template <typename... ManagerArgs>
    explicit Impl(std::tuple<ManagerArgs...> manager_args)
        : dependency_(std::move(manager_args)) {}

    Ptr get() override { return dependency_.get(); }
    Ptr Release() override { return ReleaseImpl(); }
    bool is_owning() const override { return dependency_.is_owning(); }

   private:
    template <typename T, typename Enable = void>
    struct HasRelease : std::false_type {};
    template <typename T>
    struct HasRelease<T, absl::void_t<decltype(std::declval<T>().Release())>>
        : std::true_type {};

    template <typename DependentDependency = Dependency<Ptr, Manager>,
              std::enable_if_t<HasRelease<DependentDependency>::value, int> = 0>
    Ptr ReleaseImpl() {
      return dependency_.Release();
    }
    template <
        typename DependentDependency = Dependency<Ptr, Manager>,
        std::enable_if_t<!HasRelease<DependentDependency>::value, int> = 0>
    Ptr ReleaseImpl() {
      return AnyDependencyTraits<Ptr>::DefaultPtr();
    }

    Dependency<Ptr, Manager> dependency_;
  };

  std::unique_ptr<ImplBase> impl_;
};

// Specialization of `Dependency<Ptr, AnyDependency<Ptr>>`.
template <typename Ptr>
class Dependency<Ptr, AnyDependency<Ptr>>
    : public DependencyBase<AnyDependency<Ptr>> {
 public:
  using DependencyBase<AnyDependency<Ptr>>::DependencyBase;

  Ptr get() const { return this->manager().get(); }
  template <typename DependentPtr = Ptr,
            std::enable_if_t<std::is_pointer<DependentPtr>::value, int> = 0>
  std::remove_pointer_t<DependentPtr>& operator*() const {
    return *get();
  }
  template <typename DependentPtr = Ptr,
            std::enable_if_t<std::is_pointer<DependentPtr>::value, int> = 0>
  std::remove_pointer_t<DependentPtr>* operator->() const {
    return get();
  }
  Ptr Release() { return this->manager().Release(); }

  bool is_owning() const { return this->manager().is_owning(); }
  static constexpr bool kIsStable() { return true; }
};

// Implementation details follow.

template <typename Ptr>
inline AnyDependency<Ptr>::AnyDependency(AnyDependency&& that) noexcept
    : impl_(std::move(that.impl_)) {}

template <typename Ptr>
inline AnyDependency<Ptr>& AnyDependency<Ptr>::operator=(
    AnyDependency&& that) noexcept {
  impl_ = std::move(that.impl_);
  return *this;
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_ANY_DEPENDENCY_H_
