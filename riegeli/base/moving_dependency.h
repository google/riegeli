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
//   //
//   // Skip this for `Host` using virtual inheritance.
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
// If `Host` uses virtual inheritance, even indirectly, then the leaf class
// is responsible for moving virtual base classes. `Host` should have move
// constructor and assignment defined explicitly. Their availability can be
// conditional on movability of the `Dependency` only starting from C++20,
// using the `requires` clause. `Mover::member()` should not be defined;
// this way of finding the host object does not work on Windows when virtual
// inheritance is used. The `MovingDependency` should be move-constructed by
// passing `*this, that` as additional parameters to its constructor, and it
// should be move-assigned by calling `Reset()` instead of `operator=` and
// passing `*this, that` as additional parameters.
//
// This template is specialized but does not have a primary definition.
template <typename Handle, typename Manager, typename Mover,
          typename Enable = void>
class MovingDependency;

namespace moving_dependency_internal {

class SimpleClass {
 public:
  int member;
};

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

template <typename Enable, typename T, typename... Args>
struct HasResetImpl : std::false_type {};

template <typename T, typename... Args>
struct HasResetImpl<
    absl::void_t<decltype(std::declval<T&>().Reset(std::declval<Args&&>()...))>,
    T, Args...> : std::true_type {};

template <typename T, typename... Args>
struct HasReset : HasResetImpl<void, T, Args...> {};

template <typename Handle, typename Manager, typename Mover>
class MovingDependencyImpl : public Dependency<Handle, Manager> {
 public:
  using MovingDependencyImpl::Dependency::Dependency;

  // Not supported when `Host` uses virtual inheritance.
  MovingDependencyImpl(MovingDependencyImpl&& that) noexcept
      : MovingDependencyImpl(static_cast<MovingDependencyImpl&&>(that),
                             moving_dependency_internal::MakeMover<Mover>(
                                 this->get_host(Mover::member()),
                                 that.get_host(Mover::member()))) {}

  // Not supported when `Host` uses virtual inheritance.
  MovingDependencyImpl& operator=(MovingDependencyImpl&& that) noexcept {
    Mover mover = moving_dependency_internal::MakeMover<Mover>(
        this->get_host(Mover::member()), that.get_host(Mover::member()));
    MovingDependencyImpl::Dependency::operator=(
        static_cast<typename MovingDependencyImpl::Dependency&&>(that));
    moving_dependency_internal::Done(mover, this->get_host(Mover::member()));
    return *this;
  }

  // Required when `Host` uses virtual inheritance.
  template <typename Host>
  MovingDependencyImpl(MovingDependencyImpl&& that, Host& this_host,
                       Host& that_host) noexcept
      : MovingDependencyImpl(
            static_cast<MovingDependencyImpl&&>(that),
            moving_dependency_internal::MakeMover<Mover>(this_host, that_host),
            this_host) {}

  // Required when `Host` uses virtual inheritance.
  template <typename Host>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(MovingDependencyImpl&& that,
                                          Host& this_host,
                                          Host& that_host) noexcept {
    Mover mover =
        moving_dependency_internal::MakeMover<Mover>(this_host, that_host);
    MovingDependencyImpl::Dependency::operator=(
        static_cast<typename MovingDependencyImpl::Dependency&&>(that));
    moving_dependency_internal::Done(mover, this_host);
  }

  // Not `using MovingDependencyImpl::Dependency::Reset` because it might have
  // no overloads.
  template <typename... Args,
            std::enable_if_t<
                moving_dependency_internal::HasReset<
                    typename MovingDependencyImpl::Dependency, Args...>::value,
                int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Args&&... args) {
    MovingDependencyImpl::Dependency::Reset(std::forward<Args>(args)...);
  }

 private:
  explicit MovingDependencyImpl(MovingDependencyImpl&& that, Mover mover)
      : MovingDependencyImpl::Dependency(
            static_cast<typename MovingDependencyImpl::Dependency&&>(that)) {
    moving_dependency_internal::Done(mover, this->get_host(Mover::member()));
  }

  template <typename Host>
  explicit MovingDependencyImpl(MovingDependencyImpl&& that, Mover mover,
                                Host& this_host)
      : MovingDependencyImpl::Dependency(
            static_cast<typename MovingDependencyImpl::Dependency&&>(that)) {
    moving_dependency_internal::Done(mover, this_host);
  }

  template <typename Host>
  Host& get_host(MovingDependency<Handle, Manager, Mover> Host::*member) {
    // This assertion detects virtual inheritance on Windows.
    static_assert(sizeof(member) == sizeof(&SimpleClass::member),
                  "For a host class using virtual inheritance, "
                  "MovingDependency must have this_host and that_host "
                  "passed explicitly.");
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

  // Not supported when `Host` uses virtual inheritance.
  MovingDependency(MovingDependency&& that) = default;
  // Not supported when `Host` uses virtual inheritance.
  MovingDependency& operator=(MovingDependency&& that) = default;

  // Required when `Host` uses virtual inheritance.
  template <typename Host>
  MovingDependency(MovingDependency&& that,
                   ABSL_ATTRIBUTE_UNUSED Host& this_host,
                   ABSL_ATTRIBUTE_UNUSED Host& that_host) noexcept
      : MovingDependency::Dependency(
            static_cast<typename MovingDependency::Dependency&&>(that)) {}

  // Required when `Host` uses virtual inheritance.
  template <typename Host>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      MovingDependency&& that, ABSL_ATTRIBUTE_UNUSED Host& this_host,
      ABSL_ATTRIBUTE_UNUSED Host& that_host) noexcept {
    MovingDependency::Dependency::operator=(
        static_cast<typename MovingDependency::Dependency&&>(that));
  }

  // Not `using MovingDependency::Dependency::Reset` because it might have
  // no overloads.
  template <typename... Args,
            std::enable_if_t<
                moving_dependency_internal::HasReset<
                    typename MovingDependency::Dependency, Args...>::value,
                int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Args&&... args) {
    MovingDependency::Dependency::Reset(std::forward<Args>(args)...);
  }
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

  MovingDependency(MovingDependency&& that) = default;
  MovingDependency& operator=(MovingDependency&& that) = default;
};

}  // namespace riegeli

#endif  // RIEGELI_BASE_MOVING_DEPENDENCY_H_
