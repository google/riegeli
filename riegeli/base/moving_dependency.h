// Copyright 2024 Google LLC
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

#ifndef RIEGELI_BASE_MOVING_DEPENDENCY_H_
#define RIEGELI_BASE_MOVING_DEPENDENCY_H_

#include <stddef.h>

#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/meta/type_traits.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/type_traits.h"

namespace riegeli {

// `MovingDependency<Handle, Manager, Mover>` extends
// `Dependency<Handle, Manager>` with assistance in moving the host object
// (containing the dependency), adjusting the host state while moving the
// dependency, e.g. if moving the dependency can invalidate pointers stored
// in the host object.
//
// The host object should have a defaulted move constructor and assignment,
// so that it is move constructible and/or move assignable whenever the
// dependency is. Customizations of moving belong to its base classes, members,
// and the `Mover` class passed to `MovingDependency`.
//
// If `Dependency<Handle, Manager>::kIsStable`, then it is assumed that
// no assistance is needed, and `MovingDependency<Handle, Manager, Mover>` is
// equivalent to `Dependency<Handle, Manager>`. Otherwise the `Mover` class
// specifies adjustment of the host state. It should have the following members:
//
// ```
//   // Returns a member pointer of the host class to the `MovingDependency`.
//   // This is used to get the type of the host class, and to find the host
//   // object from the `MovingDependency` by following the member pointer in
//   // the reverse direction.
//   static auto member() { return &Host::dep_; }
//
//   // Constructor, called when base classes of the host object are already
//   // moved, but the dependency is not moved yet. The host object is being
//   // moved from `that` to `self`.
//   //
//   // Parameters are optional (possibly only the second one).
//   explicit Mover(Host& self, Host& that);
//
//   // Called when the dependency is already moved. The host object is being
//   // moved to `self`.
//   //
//   // Actions can also be performed in the destructor, but `Done()` receoves
//   // `self`, so that it does not have to be stored in `Mover`.
//   //
//   // This method is optional. The parameter is optional.
//   void Done(Host& self);
// ```
//
// This template is specialized but does not have a primary definition.
template <typename Handle, typename Manager, typename Mover,
          typename Enable = void>
class MovingDependency;

namespace moving_dependency_internal {

template <typename Mover, typename Host,
          std::enable_if_t<std::is_constructible<Mover, Host&, Host&>::value,
                           int> = 0>
Mover MakeMover(Host& self, Host& that) {
  return Mover(self, that);
}

template <typename Mover, typename Host,
          std::enable_if_t<
              absl::conjunction<
                  absl::negation<std::is_constructible<Mover, Host&, Host&>>,
                  std::is_constructible<Mover, Host&>>::value,
              int> = 0>
Mover MakeMover(Host& self, ABSL_ATTRIBUTE_UNUSED Host& that) {
  return Mover(self);
}

template <typename Mover, typename Host,
          std::enable_if_t<
              absl::conjunction<
                  absl::negation<std::is_constructible<Mover, Host&, Host&>>,
                  absl::negation<std::is_constructible<Mover, Host&>>,
                  std::is_default_constructible<Mover>>::value,
              int> = 0>
Mover MakeMover(ABSL_ATTRIBUTE_UNUSED Host& self,
                ABSL_ATTRIBUTE_UNUSED Host& that) {
  return Mover();
}

template <typename Mover, typename Host, typename Enable = void>
struct HasDoneWithSelf : std::false_type {};

template <typename Mover, typename Host>
struct HasDoneWithSelf<
    Mover, Host,
    absl::void_t<decltype(std::declval<Mover&>().Done(std::declval<Host&>()))>>
    : std::true_type {};

template <typename Mover, typename Enable = void>
struct HasDoneWithoutSelf : std::false_type {};

template <typename Mover>
struct HasDoneWithoutSelf<Mover,
                          absl::void_t<decltype(std::declval<Mover&>().Done())>>
    : std::true_type {};

template <typename Mover, typename Host,
          std::enable_if_t<HasDoneWithSelf<Mover, Host>::value, int> = 0>
inline void Done(Mover& mover, Host& self) {
  mover.Done(self);
}

template <typename Mover, typename Host,
          std::enable_if_t<
              absl::conjunction<absl::negation<HasDoneWithSelf<Mover, Host>>,
                                HasDoneWithoutSelf<Mover>>::value,
              int> = 0>
inline void Done(Mover& mover, ABSL_ATTRIBUTE_UNUSED Host& self) {
  mover.Done();
}

template <
    typename Mover, typename Host,
    std::enable_if_t<
        absl::conjunction<absl::negation<HasDoneWithSelf<Mover, Host>>,
                          absl::negation<HasDoneWithoutSelf<Mover>>>::value,
        int> = 0>
inline void Done(ABSL_ATTRIBUTE_UNUSED Mover& mover,
                 ABSL_ATTRIBUTE_UNUSED Host& self) {}

template <typename Handle, typename Manager, typename Mover>
class MovingDependencyImpl : public Dependency<Handle, Manager> {
 public:
  using MovingDependencyImpl::Dependency::Dependency;

  MovingDependencyImpl(MovingDependencyImpl&& that) noexcept
      : MovingDependencyImpl(static_cast<MovingDependencyImpl&&>(that),
                             moving_dependency_internal::MakeMover<Mover>(
                                 this->get_host(Mover::member()),
                                 that.get_host(Mover::member()))) {}

  MovingDependencyImpl& operator=(MovingDependencyImpl&& that) noexcept {
    Mover mover = moving_dependency_internal::MakeMover<Mover>(
        this->get_host(Mover::member()), that.get_host(Mover::member()));
    MovingDependencyImpl::Dependency::operator=(
        static_cast<typename MovingDependencyImpl::Dependency&&>(that));
    moving_dependency_internal::Done(mover, this->get_host(Mover::member()));
    return *this;
  }

 private:
  explicit MovingDependencyImpl(MovingDependencyImpl&& that, Mover mover)
      : MovingDependencyImpl::Dependency(
            static_cast<typename MovingDependencyImpl::Dependency&&>(that)) {
    moving_dependency_internal::Done(mover, this->get_host(Mover::member()));
  }

  template <typename Host>
  Host& get_host(MovingDependency<Handle, Manager, Mover> Host::*member) {
    alignas(alignof(Host)) char storage[sizeof(Host)];
    const size_t offset =
        reinterpret_cast<char*>(&(reinterpret_cast<Host*>(storage)->*member)) -
        storage;
    return *reinterpret_cast<Host*>(reinterpret_cast<char*>(this) - offset);
  }
};

}  // namespace moving_dependency_internal

// Specialization when `Dependency<Handle, Manager>` is stable: delegate to it.
template <typename Handle, typename Manager, typename Mover>
class MovingDependency<Handle, Manager, Mover,
                       std::enable_if_t<Dependency<Handle, Manager>::kIsStable>>
    : public Dependency<Handle, Manager> {
 public:
  using MovingDependency::Dependency::Dependency;

  MovingDependency(MovingDependency&& other) = default;
  MovingDependency& operator=(MovingDependency&& other) = default;
};

// Specialization when `Dependency<Handle, Manager>` is not stable.
template <typename Handle, typename Manager, typename Mover>
class MovingDependency<
    Handle, Manager, Mover,
    std::enable_if_t<!Dependency<Handle, Manager>::kIsStable>>
    : public moving_dependency_internal::MovingDependencyImpl<Handle, Manager,
                                                              Mover>,
      public CopyableLike<Dependency<Handle, Manager>> {
 public:
  using MovingDependency::MovingDependencyImpl::MovingDependencyImpl;

  MovingDependency(MovingDependency&& other) = default;
  MovingDependency& operator=(MovingDependency&& other) = default;
};

}  // namespace riegeli

#endif  // RIEGELI_BASE_MOVING_DEPENDENCY_H_
