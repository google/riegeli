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

namespace any_dependency_internal {

// Variants of `Repr`:
//  * Empty `AnyDependency`: `Repr` is not used
//  * Held by pointer: `ptr` is `Dependency<Ptr, Manager>*` cast to `void*`
//  * Stored inline: `Dependency<Ptr, Manager>` is inside `storage`
union Repr {
  void* ptr;
  char storage[sizeof(void*)];
};

// A `Dependency<Ptr, Manager>` is stored inline in `Repr` if it fits and is
// stable. In practice this applies mostly to `Dependency<P*, P*>` and
// `Dependency<P*, std::unique_ptr<P>>`

template <typename Ptr, typename Manager, typename Enable = void>
struct IsInline : std::false_type {};

template <typename Ptr, typename Manager>
struct IsInline<
    Ptr, Manager,
    std::enable_if_t<sizeof(Dependency<Ptr, Manager>) <= sizeof(Repr) &&
                     alignof(Dependency<Ptr, Manager>) <= alignof(Repr) &&
                     Dependency<Ptr, Manager>::kIsStable()>> : std::true_type {
};

template <typename Ptr>
struct Methods {
  // Constructs `self` by moving from `that`, and destroys `that`.
  void (*move)(Repr& self, Repr& that);
  // Destroys `self`.
  void (*destroy)(Repr& self);
  Ptr (*get)(const Repr& self);
  Ptr (*release)(Repr& self);
  bool (*is_owning)(const Repr& self);
};

template <typename Ptr>
struct NullMethods;

template <typename Ptr, typename Manager, typename Enable = void>
struct MethodsFor;

}  // namespace any_dependency_internal

// `AnyDependency<Ptr>` holds a `Dependency<Ptr, Manager>` for some `Manager`
// type, erasing the `Manager` type from the type of the `AnyDependency`, or is
// empty.
template <typename Ptr>
class AnyDependency {
 public:
  // Creates an empty `AnyDependency`.
  AnyDependency() noexcept;

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
  explicit AnyDependency(const Manager& manager);
  template <typename Manager,
            std::enable_if_t<
                absl::conjunction<
                    absl::negation<std::is_same<std::decay_t<Manager>,
                                                AnyDependency<Ptr>>>,
                    IsValidDependency<Ptr, std::decay_t<Manager>>>::value,
                int> = 0>
  explicit AnyDependency(Manager&& manager);

  // Holds a `Dependency<Ptr, Manager>`.
  //
  // The `Manager` type is specified with a tag (`absl::in_place_type<Manager>`)
  // because constructor templates do not support specifying template arguments
  // explicitly.
  template <typename Manager>
  explicit AnyDependency(absl::in_place_type_t<Manager>,
                         const Manager& manager);
  template <typename Manager>
  explicit AnyDependency(absl::in_place_type_t<Manager>, Manager&& manager);
  template <typename Manager, typename... ManagerArgs>
  explicit AnyDependency(absl::in_place_type_t<Manager>,
                         std::tuple<ManagerArgs...> manager_args);

  AnyDependency(AnyDependency&& that) noexcept;
  AnyDependency& operator=(AnyDependency&& that) noexcept;

  ~AnyDependency();

  // Makes `*this` equivalent to a newly constructed `AnyDependency`. This
  // avoids constructing a temporary `AnyDependency` and moving from it.
  void Reset();
  template <typename Manager>
  void Reset(const Manager& manager);
  template <typename Manager>
  void Reset(Manager&& manager);
  template <typename Manager, typename... ManagerArgs>
  void Reset(std::tuple<ManagerArgs...> manager_args);

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
  Ptr get() const { return methods_->get(repr_); }

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
  Ptr Release() { return methods_->release(repr_); }

  // If `true`, the `AnyDependency` owns the dependent object, i.e. closing the
  // host object should close the dependent object.
  bool is_owning() const { return methods_->is_owning(repr_); }

 private:
  using Repr = any_dependency_internal::Repr;
  using Methods = any_dependency_internal::Methods<Ptr>;
  using NullMethods = any_dependency_internal::NullMethods<Ptr>;
  template <typename Manager>
  using MethodsFor = any_dependency_internal::MethodsFor<Ptr, Manager>;

  const Methods* methods_;
  Repr repr_;
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

namespace any_dependency_internal {

// `any_dependency_internal::Release(dep)` calls `dep.Release()` if that is
// defined, otherwise returns `AnyDependencyTraits<Ptr>::DefaultPtr()`.

template <typename T, typename Enable = void>
struct HasRelease : std::false_type {};
template <typename T>
struct HasRelease<T, absl::void_t<decltype(std::declval<T>().Release())>>
    : std::true_type {};

template <
    typename Ptr, typename Manager,
    std::enable_if_t<HasRelease<Dependency<Ptr, Manager>>::value, int> = 0>
Ptr Release(Dependency<Ptr, Manager>& dep) {
  return dep.Release();
}
template <
    typename Ptr, typename Manager,
    std::enable_if_t<!HasRelease<Dependency<Ptr, Manager>>::value, int> = 0>
Ptr Release(Dependency<Ptr, Manager>& dep) {
  return AnyDependencyTraits<Ptr>::DefaultPtr();
}

template <typename Ptr>
struct NullMethods {
  static const Methods<Ptr> methods;

 private:
  static void Move(Repr& self, Repr& that) {}
  static void Destroy(Repr& self) {}
  static Ptr Get(const Repr& self) {
    return AnyDependencyTraits<Ptr>::DefaultPtr();
  }
  static Ptr Release(Repr& self) {
    return AnyDependencyTraits<Ptr>::DefaultPtr();
  }
  static bool IsOwning(const Repr& self) { return false; }
};

template <typename Ptr>
const Methods<Ptr> NullMethods<Ptr>::methods = {Move, Destroy, Get, Release,
                                                IsOwning};

template <typename Ptr, typename Manager, typename Enable>
struct MethodsFor {
  static const Methods<Ptr> methods;

  static void Construct(Repr& self, const Manager& manager) {
    self.ptr = new Dependency<Ptr, Manager>(manager);
  }
  static void Construct(Repr& self, Manager&& manager) {
    self.ptr = new Dependency<Ptr, Manager>(std::move(manager));
  }
  template <typename... ManagerArgs>
  static void Construct(Repr& self, std::tuple<ManagerArgs...> manager_args) {
    self.ptr = new Dependency<Ptr, Manager>(std::move(manager_args));
  }

 private:
  static Dependency<Ptr, Manager>* ptr(const Repr& self) {
    return static_cast<Dependency<Ptr, Manager>*>(self.ptr);
  }

  static void Move(Repr& self, Repr& that) { self.ptr = that.ptr; }
  static void Destroy(Repr& self) { delete ptr(self); }
  static Ptr Get(const Repr& self) { return ptr(self)->get(); }
  static Ptr Release(Repr& self) {
    return any_dependency_internal::Release(*ptr(self));
  }
  static bool IsOwning(const Repr& self) { return ptr(self)->is_owning(); }
};

template <typename Ptr, typename Manager, typename Enable>
const Methods<Ptr> MethodsFor<Ptr, Manager, Enable>::methods = {
    Move, Destroy, Get, Release, IsOwning};

template <typename Ptr, typename Manager>
struct MethodsFor<Ptr, Manager,
                  std::enable_if_t<IsInline<Ptr, Manager>::value>> {
  static const Methods<Ptr> methods;

  static void Construct(Repr& self, const Manager& manager) {
    new (self.storage) Dependency<Ptr, Manager>(manager);
  }
  static void Construct(Repr& self, Manager&& manager) {
    new (self.storage) Dependency<Ptr, Manager>(std::move(manager));
  }
  template <typename... ManagerArgs>
  static void Construct(Repr& self, std::tuple<ManagerArgs...> manager_args) {
    new (self.storage) Dependency<Ptr, Manager>(std::move(manager_args));
  }

 private:
  static Dependency<Ptr, Manager>& dep(Repr& self) {
    return *reinterpret_cast<Dependency<Ptr, Manager>*>(self.storage);
  }
  static const Dependency<Ptr, Manager>& dep(const Repr& self) {
    return *reinterpret_cast<const Dependency<Ptr, Manager>*>(self.storage);
  }

  static void Move(Repr& self, Repr& that) {
    new (self.storage) Dependency<Ptr, Manager>(std::move(dep(that)));
    dep(that).~Dependency<Ptr, Manager>();
  }
  static void Destroy(Repr& self) { dep(self).~Dependency<Ptr, Manager>(); }
  static Ptr Get(const Repr& self) {
    // See the caveat regarding const at `AnyDependency::get()`.
    return const_cast<Dependency<Ptr, Manager>&>(dep(self)).get();
  }
  static Ptr Release(Repr& self) {
    return any_dependency_internal::Release(dep(self));
  }
  static bool IsOwning(const Repr& self) { return dep(self).is_owning(); }
};

template <typename Ptr, typename Manager>
const Methods<Ptr> MethodsFor<
    Ptr, Manager, std::enable_if_t<IsInline<Ptr, Manager>::value>>::methods = {
    Move, Destroy, Get, Release, IsOwning};

}  // namespace any_dependency_internal

template <typename Ptr>
inline AnyDependency<Ptr>::AnyDependency() noexcept
    : methods_(&NullMethods::methods) {}

template <typename Ptr>
template <
    typename Manager,
    std::enable_if_t<
        absl::conjunction<absl::negation<std::is_same<std::decay_t<Manager>,
                                                      AnyDependency<Ptr>>>,
                          IsValidDependency<Ptr, std::decay_t<Manager>>>::value,
        int>>
inline AnyDependency<Ptr>::AnyDependency(const Manager& manager)
    : methods_(&MethodsFor<std::decay_t<Manager>>::methods) {
  MethodsFor<std::decay_t<Manager>>::Construct(repr_, manager);
}

template <typename Ptr>
template <
    typename Manager,
    std::enable_if_t<
        absl::conjunction<absl::negation<std::is_same<std::decay_t<Manager>,
                                                      AnyDependency<Ptr>>>,
                          IsValidDependency<Ptr, std::decay_t<Manager>>>::value,
        int>>
inline AnyDependency<Ptr>::AnyDependency(Manager&& manager)
    : methods_(&MethodsFor<std::decay_t<Manager>>::methods) {
  MethodsFor<std::decay_t<Manager>>::Construct(repr_,
                                               std::forward<Manager>(manager));
}

template <typename Ptr>
template <typename Manager>
inline AnyDependency<Ptr>::AnyDependency(absl::in_place_type_t<Manager>,
                                         const Manager& manager)
    : methods_(&MethodsFor<Manager>::methods) {
  MethodsFor<Manager>::Construct(repr_, manager);
}

template <typename Ptr>
template <typename Manager>
inline AnyDependency<Ptr>::AnyDependency(absl::in_place_type_t<Manager>,
                                         Manager&& manager)
    : methods_(&MethodsFor<Manager>::methods) {
  MethodsFor<Manager>::Construct(repr_, std::move(manager));
}

template <typename Ptr>
template <typename Manager, typename... ManagerArgs>
inline AnyDependency<Ptr>::AnyDependency(
    absl::in_place_type_t<Manager>, std::tuple<ManagerArgs...> manager_args)
    : methods_(&MethodsFor<Manager>::methods) {
  MethodsFor<Manager>::Construct(repr_, std::move(manager_args));
}

template <typename Ptr>
inline AnyDependency<Ptr>::AnyDependency(AnyDependency&& that) noexcept
    : methods_(std::exchange(that.methods_, &NullMethods::methods)) {
  methods_->move(repr_, that.repr_);
}

template <typename Ptr>
inline AnyDependency<Ptr>& AnyDependency<Ptr>::operator=(
    AnyDependency&& that) noexcept {
  if (ABSL_PREDICT_TRUE(&that != this)) {
    methods_->destroy(repr_);
    methods_ = std::exchange(that.methods_, &NullMethods::methods);
    methods_->move(repr_, that.repr_);
  }
  return *this;
}

template <typename Ptr>
inline AnyDependency<Ptr>::~AnyDependency() {
  methods_->destroy(repr_);
}

template <typename Ptr>
inline void AnyDependency<Ptr>::Reset() {
  methods_->destroy(repr_);
  methods_ = &NullMethods::methods;
}

template <typename Ptr>
template <typename Manager>
inline void AnyDependency<Ptr>::Reset(const Manager& manager) {
  methods_->destroy(repr_);
  methods_ = &MethodsFor<std::decay_t<Manager>>::methods;
  MethodsFor<std::decay_t<Manager>>::Construct(repr_, manager);
}

template <typename Ptr>
template <typename Manager>
inline void AnyDependency<Ptr>::Reset(Manager&& manager) {
  methods_->destroy(repr_);
  methods_ = &MethodsFor<std::decay_t<Manager>>::methods;
  MethodsFor<std::decay_t<Manager>>::Construct(repr_,
                                               std::forward<Manager>(manager));
}

template <typename Ptr>
template <typename Manager, typename... ManagerArgs>
inline void AnyDependency<Ptr>::Reset(std::tuple<ManagerArgs...> manager_args) {
  methods_->destroy(repr_);
  methods_ = &MethodsFor<Manager>::methods;
  MethodsFor<Manager>::Construct(repr_, std::move(manager_args));
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_ANY_DEPENDENCY_H_
