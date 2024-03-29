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

#include <stddef.h>

#include <algorithm>
#include <cstddef>
#include <memory>
#include <new>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/meta/type_traits.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/any_dependency_internal.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/dependency_base.h"
#include "riegeli/base/dependency_manager.h"
#include "riegeli/base/in_place_template.h"  // IWYU pragma: export
#include "riegeli/base/initializer.h"
#include "riegeli/base/memory_estimator.h"
#include "riegeli/base/type_id.h"
#include "riegeli/base/type_traits.h"

namespace riegeli {

namespace any_dependency_internal {

// Common base class of `AnyDependency` and `AnyDependencyRef`.
//
// `ABSL_ATTRIBUTE_TRIVIAL_ABI` is effective if `inline_size == 0`.
template <typename Handle, size_t inline_size, size_t inline_align>
class
#ifdef ABSL_ATTRIBUTE_TRIVIAL_ABI
    ABSL_ATTRIBUTE_TRIVIAL_ABI
#endif
        AnyDependencyBase
    : public WithEqual<AnyDependencyBase<Handle, inline_size, inline_align>>,
      public ConditionallyAbslNullabilityCompatible<
          IsComparableAgainstNullptr<Handle>::value>,
      public ConditionallyTrivialAbi<inline_size == 0> {
 public:
  // Makes `*this` equivalent to a newly constructed `AnyDependencyBase`. This
  // avoids constructing a temporary `AnyDependencyBase` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset();

  // Returns a `Handle` to the `Manager`, or a default `Handle` for an empty
  // `AnyDependencyBase`.
  Handle get() const { return handle_; }

  // If `Handle` is `Base*`, `AnyDependencyBase<Base*>` can be used as a smart
  // pointer to `Base`, for convenience.
  template <typename DependentHandle = Handle,
            std::enable_if_t<HasDereference<DependentHandle>::value, int> = 0>
  decltype(*std::declval<DependentHandle>()) operator*() const {
    AssertNotNull(
        "Failed precondition of AnyDependencyBase::operator*: null handle");
    return *get();
  }

  template <typename DependentHandle = Handle,
            std::enable_if_t<HasArrow<DependentHandle>::value, int> = 0>
  Handle operator->() const {
    AssertNotNull(
        "Failed precondition of AnyDependencyBase::operator->: null handle");
    return get();
  }

  // If `Handle` is `Base*`, `AnyDependencyBase<Base*>` can be compared against
  // `nullptr`.
  template <typename DependentHandle = Handle,
            std::enable_if_t<IsComparableAgainstNullptr<DependentHandle>::value,
                             int> = 0>
  friend bool operator==(const AnyDependencyBase& a, std::nullptr_t) {
    return a.get() == nullptr;
  }

  // If `true`, the `AnyDependencyBase` owns the dependent object, i.e. closing
  // the host object should close the dependent object.
  bool IsOwning() const { return methods_->is_owning(repr_.storage); }

  // If `true`, `get()` stays unchanged when an `AnyDependencyBase` is moved.
  static constexpr bool kIsStable = inline_size == 0;

  // If the `Manager` has exactly this type or a reference to it, returns a
  // pointer to the `Manager`. If the `Manager` is an `AnyDependencyBase`
  // (possibly wrapped in a reference or `std::unique_ptr`), propagates
  // `GetIf()` to it. Otherwise returns `nullptr`.
  template <
      typename Manager,
      std::enable_if_t<IsValidDependency<Handle, Manager>::value, int> = 0>
  Manager* GetIf();
  template <
      typename Manager,
      std::enable_if_t<IsValidDependency<Handle, Manager>::value, int> = 0>
  const Manager* GetIf() const;

  // A variant of `GetIf()` with the expected type passed as a `TypeId`.
  void* GetIf(TypeId type_id);
  const void* GetIf(TypeId type_id) const;

  friend void RiegeliRegisterSubobjects(const AnyDependencyBase* self,
                                        MemoryEstimator& memory_estimator) {
    self->methods_->register_subobjects(self->repr_.storage, memory_estimator);
  }

 protected:
  // The state is left uninitialized.
  AnyDependencyBase() noexcept {}

  AnyDependencyBase(AnyDependencyBase&& that) noexcept;
  AnyDependencyBase& operator=(AnyDependencyBase&& that) noexcept;

  ~AnyDependencyBase() { Destroy(); }

  // Initializes the state, avoiding a redundant indirection and adopting them
  // from `manager` instead if `Manager` is already a compatible `AnyDependency`
  // or `AnyDependencyRef`.
  void Initialize();
  template <typename Manager,
            std::enable_if_t<!IsAnyDependency<Handle, Manager>::value, int> = 0>
  void Initialize(Manager&& manager);
  template <typename Manager,
            std::enable_if_t<IsAnyDependency<Handle, Manager>::value, int> = 0>
  void Initialize(Manager&& manager);
  template <typename Manager,
            std::enable_if_t<!IsAnyDependency<Handle, Manager>::value, int> = 0>
  void Initialize(Initializer<Manager> manager);
  template <typename Manager,
            std::enable_if_t<IsAnyDependency<Handle, Manager>::value, int> = 0>
  void Initialize(Initializer<Manager> manager);

  // Destroys the state, leaving it uninitialized.
  void Destroy();

  template <typename Manager>
  Manager& GetManager() {
    return MethodsFor<Manager>::GetManager(repr_.storage);
  }

 private:
  // For adopting the state from an instantiation with a different `inline_size`
  // and `inline_align.
  template <typename OtherHandle, size_t other_inline_size,
            size_t other_inline_align>
  friend class AnyDependencyBase;

  using Repr = any_dependency_internal::Repr<Handle, inline_size, inline_align>;
  using Methods = any_dependency_internal::Methods<Handle>;
  using NullMethods = any_dependency_internal::NullMethods<Handle>;
  template <typename Manager>
  using MethodsFor = any_dependency_internal::MethodsFor<
      Handle, Manager,
      any_dependency_internal::IsInline<Handle, inline_size, inline_align,
                                        Manager>::value>;

  template <typename DependentHandle = Handle,
            std::enable_if_t<IsComparableAgainstNullptr<DependentHandle>::value,
                             int> = 0>
  void AssertNotNull(absl::string_view message) const {
    RIEGELI_ASSERT(get() != nullptr) << message;
  }
  template <typename DependentHandle = Handle,
            std::enable_if_t<
                !IsComparableAgainstNullptr<DependentHandle>::value, int> = 0>
  void AssertNotNull(ABSL_ATTRIBUTE_UNUSED absl::string_view message) const {}

  const Methods* methods_;
  // The union disables implicit construction and destruction which is done
  // manually here.
  union {
    Handle handle_;
  };
  Repr repr_;
};

}  // namespace any_dependency_internal

// `AnyDependency<Handle>` refers to an optionally owned object which is
// accessed as `Handle` and stored as some `Manager` type decided when the
// `AnyDependency` is initialized.
//
// Often `Handle` is some pointer `Base*`, and then `Manager` can be e.g.
// `T*` (not owned), `T` (owned), or `std::unique_ptr<T>` (owned), with some `T`
// derived from `Base`.
//
// `AnyDependency<Handle>` holds a `Dependency<Handle, Manager>` for some
// `Manager` type, erasing the `Manager` parameter from the type of the
// `AnyDependency`, or is empty.
template <typename Handle, size_t inline_size = 0, size_t inline_align = 0>
class AnyDependency
    : public any_dependency_internal::AnyDependencyBase<Handle, inline_size,
                                                        inline_align> {
 public:
  // `AnyDependency<Handle>::Inlining<InlineManagers...>` enlarges inline
  // storage of `AnyDependency<Handle>`.
  //
  // `InlineManagers` specify the size of inline storage, which allows to avoid
  // heap allocation if `Manager` is among `InlineManagers`, or if
  // `Dependency<Handle, Manager>` fits there regarding size and alignment.
  // By default inline storage is enough for a pointer.
  template <typename... InlineManagers>
  using Inlining = AnyDependency<
      Handle,
      UnsignedMax(inline_size, sizeof(Dependency<Handle, InlineManagers>)...),
      UnsignedMax(inline_align,
                  alignof(Dependency<Handle, InlineManagers>)...)>;

  // Creates an empty `AnyDependency`.
  AnyDependency() noexcept { this->Initialize(); }

  // Holds a `Dependency<Handle, std::decay_t<Manager>>`.
  //
  // The `Manager` type is deduced from the constructor argument.
  template <typename Manager,
            std::enable_if_t<
                absl::conjunction<
                    absl::negation<
                        std::is_same<std::decay_t<Manager>, AnyDependency>>,
                    IsValidDependency<Handle, std::decay_t<Manager>>,
                    std::is_move_constructible<std::decay_t<Manager>>>::value,
                int> = 0>
  /*implicit*/ AnyDependency(Manager&& manager);
  template <typename Manager,
            std::enable_if_t<
                absl::conjunction<
                    absl::negation<
                        std::is_same<std::decay_t<Manager>, AnyDependency>>,
                    IsValidDependency<Handle, std::decay_t<Manager>>,
                    std::is_move_constructible<std::decay_t<Manager>>>::value,
                int> = 0>
  AnyDependency& operator=(Manager&& manager);

  // Holds a `Dependency<Handle, Manager>`.
  //
  // The `Manager` type is extracted from the `Initializer` type.
  template <
      typename Manager,
      std::enable_if_t<IsValidDependency<Handle, Manager>::value, int> = 0>
  /*implicit*/ AnyDependency(Initializer<Manager> manager);
  template <
      typename Manager,
      std::enable_if_t<IsValidDependency<Handle, Manager>::value, int> = 0>
  AnyDependency& operator=(Initializer<Manager> manager);

  // Holds a `Dependency<Handle, Manager>`.
  //
  // The `Manager` type is specified with a tag (`absl::in_place_type<Manager>`
  // or `in_place_template<ManagerTemplate>`) because constructor templates do
  // not support specifying template arguments explicitly.
  //
  // The `Manager` is constructed from `manager_args`.
  template <
      typename InPlaceTag, typename... ManagerArgs,
      std::enable_if_t<
          IsValidDependency<
              Handle, TypeFromInPlaceTagT<InPlaceTag, ManagerArgs...>>::value,
          int> = 0>
  /*implicit*/ AnyDependency(InPlaceTag, ManagerArgs&&... manager_args);

  AnyDependency(AnyDependency&& that) = default;
  AnyDependency& operator=(AnyDependency&& that) = default;

  using AnyDependency::AnyDependencyBase::Reset;
  template <typename Manager,
            std::enable_if_t<
                absl::conjunction<
                    IsValidDependency<Handle, std::decay_t<Manager>>,
                    std::is_move_constructible<std::decay_t<Manager>>>::value,
                int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Manager&& manager);
  template <
      typename Manager,
      std::enable_if_t<IsValidDependency<Handle, Manager>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Manager> manager);
  template <
      typename InPlaceTag, typename... ManagerArgs,
      std::enable_if_t<
          IsValidDependency<
              Handle, TypeFromInPlaceTagT<InPlaceTag, ManagerArgs...>>::value,
          int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(InPlaceTag,
                                          ManagerArgs&&... manager_args);

  // Holds a `Dependency<Handle, Manager>`.
  //
  // Same as `Reset(absl::in_place_type<Manager>,
  //                std::forward<ManagerArgs>(manager_args)...)`,
  // returning a reference to the constructed `Manager`.
  template <
      typename Manager, typename... ManagerArgs,
      std::enable_if_t<IsValidDependency<Handle, Manager>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES Manager& Emplace(ManagerArgs&&... manager_args);

#if __cpp_deduction_guides
  // Like above, but the exact `Manager` type is deduced using CTAD.
  //
  // Same as `Reset(in_place_template<ManagerTemplate>,
  //                std::forward<ManagerArgs>(manager_args)...)`,
  // returning a reference to the constructed `Manager`.
  template <
      template <typename...> class ManagerTemplate, typename... ManagerArgs,
      std::enable_if_t<
          IsValidDependency<
              Handle, TypeFromInPlaceTagT<in_place_template_t<ManagerTemplate>,
                                          ManagerArgs...>>::value,
          int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES
      TypeFromInPlaceTagT<in_place_template_t<ManagerTemplate>, ManagerArgs...>&
      Emplace(ManagerArgs&&... manager_args);
#endif
};

// Specialization of `DependencyManagerImpl<AnyDependency<Handle>>`:
// a dependency with ownership determined at runtime.
template <typename Handle, size_t inline_size, size_t inline_align,
          typename ManagerStorage>
class DependencyManagerImpl<AnyDependency<Handle, inline_size, inline_align>,
                            ManagerStorage>
    : public DependencyBase<ManagerStorage> {
 public:
  using DependencyManagerImpl::DependencyBase::DependencyBase;

  bool IsOwning() const { return this->manager().IsOwning(); }

  static constexpr bool kIsStable =
      DependencyManagerImpl::DependencyBase::kIsStable ||
      AnyDependency<Handle, inline_size, inline_align>::kIsStable;

  void* GetIf(TypeId type_id) { return this->manager().GetIf(type_id); }
  const void* GetIf(TypeId type_id) const {
    return this->manager().GetIf(type_id);
  }

 protected:
  DependencyManagerImpl(DependencyManagerImpl&& that) = default;
  DependencyManagerImpl& operator=(DependencyManagerImpl&& that) = default;

  ~DependencyManagerImpl() = default;

  Handle ptr() const { return this->manager().get(); }
};

// Specialization of
// `DependencyManagerImpl<std::unique_ptr<AnyDependency<Handle>, Deleter>>`:
// a dependency with ownership determined at runtime.
//
// It covers `ClosingPtrType<AnyDependency<Handle>>`.
template <typename Handle, size_t inline_size, size_t inline_align,
          typename Deleter, typename ManagerStorage>
class DependencyManagerImpl<
    std::unique_ptr<AnyDependency<Handle, inline_size, inline_align>, Deleter>,
    ManagerStorage>
    : public DependencyBase<std::conditional_t<
          std::is_empty<Deleter>::value,
          std::unique_ptr<AnyDependency<Handle, inline_size, inline_align>,
                          Deleter>,
          ManagerStorage>> {
 public:
  using DependencyManagerImpl::DependencyBase::DependencyBase;

  bool IsOwning() const {
    return this->manager() != nullptr && this->manager()->IsOwning();
  }

  static constexpr bool kIsStable = true;

  void* GetIf(TypeId type_id) {
    if (this->manager() == nullptr) return nullptr;
    return this->manager()->GetIf(type_id);
  }
  const void* GetIf(TypeId type_id) const {
    if (this->manager() == nullptr) return nullptr;
    return this->manager()->GetIf(type_id);
  }

 protected:
  DependencyManagerImpl(DependencyManagerImpl&& that) = default;
  DependencyManagerImpl& operator=(DependencyManagerImpl&& that) = default;

  ~DependencyManagerImpl() = default;

  Handle ptr() const { return this->manager()->get(); }
};

// `AnyDependencyRef<Handle>` refers to an optionally owned object which is
// accessed as `Handle` and was passed as some `Manager` type decided when the
// `AnyDependencyRef` was initialized.
//
// Often `Handle` is some pointer `Base*`, and then `Manager` can be e.g.
// `T&` (not owned), `T&&` (owned), or `std::unique_ptr<T>` (owned), with some
// `T` derived from `Base`.
//
// `AnyDependencyRef<Handle>` derives from `AnyDependency<Handle>`, replacing
// the constructors such that the `Manager` type is deduced from the constructor
// argument as a reference type rather than a value type.
//
// This is meant to be used only when the dependency is a function parameter
// rather than stored in a host object, because such a dependency stores a
// reference to the dependent object, and by convention a reference argument is
// expected to be valid only for the duration of the function call.
//
// This allows to pass an unowned dependency by lvalue reference instead of by
// pointer, which allows for a more idiomatic API for passing an object which
// does not need to be valid after the function returns. And this allows to pass
// an owned dependency by rvalue reference instead of by value, which avoids
// moving it.
template <typename Handle>
class AnyDependencyRef
    : public any_dependency_internal::AnyDependencyBase<Handle, 0, 0> {
 public:
  // Creates an empty `AnyDependencyRef`.
  AnyDependencyRef() noexcept { this->Initialize(); }

  // Holds a `Dependency<Handle, Manager&&>` (which collapses to
  // `Dependency<Handle, Manager&>` if `Manager` is itself an lvalue reference).
  //
  // The `Manager` type is deduced from the constructor argument.
  template <typename Manager,
            std::enable_if_t<
                absl::conjunction<absl::negation<std::is_same<
                                      std::decay_t<Manager>, AnyDependencyRef>>,
                                  IsValidDependency<Handle, Manager&&>>::value,
                int> = 0>
  /*implicit*/ AnyDependencyRef(
      Manager&& manager ABSL_ATTRIBUTE_LIFETIME_BOUND);

  // Holds a `Dependency<Handle, Manager&&>` (which collapses to
  // `Dependency<Handle, Manager&>` if `Manager` is itself an lvalue reference).
  //
  // The `Manager` type is extracted from the `Initializer` type.
  //
  // `reference_storage` must outlive usages of the constructed
  // `AnyDependencyRef`.
  template <
      typename Manager,
      std::enable_if_t<IsValidDependency<Handle, Manager&&>::value, int> = 0>
  /*implicit*/ AnyDependencyRef(
      Initializer<Manager> manager ABSL_ATTRIBUTE_LIFETIME_BOUND,
      typename Initializer<Manager>::ReferenceStorage&& reference_storage
          ABSL_ATTRIBUTE_LIFETIME_BOUND =
              typename Initializer<Manager>::ReferenceStorage());

  AnyDependencyRef(AnyDependencyRef&& that) = default;
  AnyDependencyRef& operator=(AnyDependencyRef&& that) = default;
};

// Specialization of `DependencyManagerImpl<AnyDependencyRef<Handle>>`:
// a dependency with ownership determined at runtime.
template <typename Handle, typename ManagerStorage>
class DependencyManagerImpl<AnyDependencyRef<Handle>, ManagerStorage>
    : public DependencyBase<ManagerStorage> {
 public:
  using DependencyManagerImpl::DependencyBase::DependencyBase;

  bool IsOwning() const { return this->manager().IsOwning(); }

  static constexpr bool kIsStable =
      DependencyManagerImpl::DependencyBase::kIsStable ||
      AnyDependencyRef<Handle>::kIsStable;

  void* GetIf(TypeId type_id) { return this->manager().GetIf(type_id); }
  const void* GetIf(TypeId type_id) const {
    return this->manager().GetIf(type_id);
  }

 protected:
  DependencyManagerImpl(DependencyManagerImpl&& that) = default;
  DependencyManagerImpl& operator=(DependencyManagerImpl&& that) = default;

  ~DependencyManagerImpl() = default;

  Handle ptr() const { return this->manager().get(); }
};

// Specialization of
// `DependencyManagerImpl<std::unique_ptr<AnyDependencyRef<Handle>, Deleter>>`:
// a dependency with ownership determined at runtime.
//
// It covers `ClosingPtrType<AnyDependencyRef<Handle>>`.
template <typename Handle, typename Deleter, typename ManagerStorage>
class DependencyManagerImpl<std::unique_ptr<AnyDependencyRef<Handle>, Deleter>,
                            ManagerStorage>
    : public DependencyBase<std::conditional_t<
          std::is_empty<Deleter>::value,
          std::unique_ptr<AnyDependencyRef<Handle>, Deleter>, ManagerStorage>> {
 public:
  using DependencyManagerImpl::DependencyBase::DependencyBase;

  bool IsOwning() const {
    return this->manager() != nullptr && this->manager()->IsOwning();
  }

  static constexpr bool kIsStable = true;

  void* GetIf(TypeId type_id) {
    if (this->manager() == nullptr) return nullptr;
    return this->manager()->GetIf(type_id);
  }
  const void* GetIf(TypeId type_id) const {
    if (this->manager() == nullptr) return nullptr;
    return this->manager()->GetIf(type_id);
  }

 protected:
  DependencyManagerImpl(DependencyManagerImpl&& that) = default;
  DependencyManagerImpl& operator=(DependencyManagerImpl&& that) = default;

  ~DependencyManagerImpl() = default;

  Handle ptr() const { return this->manager()->get(); }
};

// Implementation details follow.

namespace any_dependency_internal {

template <typename Handle, size_t inline_size, size_t inline_align>
inline AnyDependencyBase<Handle, inline_size, inline_align>::AnyDependencyBase(
    AnyDependencyBase&& that) noexcept {
  that.handle_ = SentinelHandle<Handle>();
  methods_ = std::exchange(that.methods_, &NullMethods::kMethods);
  methods_->move(repr_.storage, &handle_, that.repr_.storage);
}

template <typename Handle, size_t inline_size, size_t inline_align>
inline AnyDependencyBase<Handle, inline_size, inline_align>&
AnyDependencyBase<Handle, inline_size, inline_align>::operator=(
    AnyDependencyBase&& that) noexcept {
  if (ABSL_PREDICT_TRUE(&that != this)) {
    Destroy();
    that.handle_ = SentinelHandle<Handle>();
    methods_ = std::exchange(that.methods_, &NullMethods::kMethods);
    methods_->move(repr_.storage, &handle_, that.repr_.storage);
  }
  return *this;
}

template <typename Handle, size_t inline_size, size_t inline_align>
inline void AnyDependencyBase<Handle, inline_size, inline_align>::Initialize() {
  methods_ = &NullMethods::kMethods;
  handle_ = SentinelHandle<Handle>();
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<!IsAnyDependency<Handle, Manager>::value, int>>
inline void AnyDependencyBase<Handle, inline_size, inline_align>::Initialize(
    Manager&& manager) {
  Initialize(Initializer<Manager>(std::forward<Manager>(manager)));
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<IsAnyDependency<Handle, Manager>::value, int>>
inline void AnyDependencyBase<Handle, inline_size, inline_align>::Initialize(
    Manager&& manager) {
  if ((sizeof(typename Manager::Repr) <= sizeof(Repr) ||
       manager.methods_->inline_size_used <= sizeof(Repr)) &&
      (alignof(typename Manager::Repr) <= alignof(Repr) ||
       manager.methods_->inline_align_used <= alignof(Repr))) {
    // Adopt `manager` instead of wrapping it.
    manager.handle_ = SentinelHandle<Handle>();
    methods_ = std::exchange(manager.methods_, &NullMethods::kMethods);
    methods_->move(repr_.storage, &handle_, manager.repr_.storage);
    return;
  }
  methods_ = &MethodsFor<Manager>::kMethods;
  MethodsFor<Manager>::Construct(repr_.storage, &handle_,
                                 std::forward<Manager>(manager));
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<!IsAnyDependency<Handle, Manager>::value, int>>
inline void AnyDependencyBase<Handle, inline_size, inline_align>::Initialize(
    Initializer<Manager> manager) {
  methods_ = &MethodsFor<Manager>::kMethods;
  MethodsFor<Manager>::Construct(repr_.storage, &handle_, std::move(manager));
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<IsAnyDependency<Handle, Manager>::value, int>>
inline void AnyDependencyBase<Handle, inline_size, inline_align>::Initialize(
    Initializer<Manager> manager) {
  // This is called only from `AnyDependency<Handle, inline_size, inline_align>`
  // so the type of `*this` matches.
  if (std::is_same<Manager,
                   AnyDependency<Handle, inline_size, inline_align>>::value) {
    // Adopt `manager` instead of wrapping it. Doing this here if possible
    // avoids creating a temporary `AnyDependency` and moving from it.
    //
    // `*this` is formally already constructed, but nothing was initialized yet.
    new (this) Manager(std::move(manager).Construct());
    return;
  }
  Initialize(std::move(manager).Reference());
}

template <typename Handle, size_t inline_size, size_t inline_align>
inline void AnyDependencyBase<Handle, inline_size, inline_align>::Destroy() {
  handle_.~Handle();
  methods_->destroy(repr_.storage);
}

template <typename Handle, size_t inline_size, size_t inline_align>
inline void AnyDependencyBase<Handle, inline_size, inline_align>::Reset() {
  handle_ = SentinelHandle<Handle>();
  methods_->destroy(repr_.storage);
  methods_ = &NullMethods::kMethods;
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<IsValidDependency<Handle, Manager>::value, int>>
inline Manager* AnyDependencyBase<Handle, inline_size, inline_align>::GetIf() {
  return static_cast<Manager*>(GetIf(TypeId::For<Manager>()));
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<IsValidDependency<Handle, Manager>::value, int>>
inline const Manager*
AnyDependencyBase<Handle, inline_size, inline_align>::GetIf() const {
  return static_cast<const Manager*>(GetIf(TypeId::For<Manager>()));
}

template <typename Handle, size_t inline_size, size_t inline_align>
inline void* AnyDependencyBase<Handle, inline_size, inline_align>::GetIf(
    TypeId type_id) {
  return methods_->mutable_get_if(repr_.storage, type_id);
}

template <typename Handle, size_t inline_size, size_t inline_align>
inline const void* AnyDependencyBase<Handle, inline_size, inline_align>::GetIf(
    TypeId type_id) const {
  return methods_->const_get_if(repr_.storage, type_id);
}

}  // namespace any_dependency_internal

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<
              absl::conjunction<
                  absl::negation<std::is_same<
                      std::decay_t<Manager>,
                      AnyDependency<Handle, inline_size, inline_align>>>,
                  IsValidDependency<Handle, std::decay_t<Manager>>,
                  std::is_move_constructible<std::decay_t<Manager>>>::value,
              int>>
inline AnyDependency<Handle, inline_size, inline_align>::AnyDependency(
    Manager&& manager) {
  this->template Initialize<std::decay_t<Manager>>(
      std::forward<Manager>(manager));
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<
              absl::conjunction<
                  absl::negation<std::is_same<
                      std::decay_t<Manager>,
                      AnyDependency<Handle, inline_size, inline_align>>>,
                  IsValidDependency<Handle, std::decay_t<Manager>>,
                  std::is_move_constructible<std::decay_t<Manager>>>::value,
              int>>
inline AnyDependency<Handle, inline_size, inline_align>&
AnyDependency<Handle, inline_size, inline_align>::operator=(Manager&& manager) {
  this->Destroy();
  this->template Initialize<std::decay_t<Manager>>(
      std::forward<Manager>(manager));
  return *this;
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<IsValidDependency<Handle, Manager>::value, int>>
inline AnyDependency<Handle, inline_size, inline_align>::AnyDependency(
    Initializer<Manager> manager) {
  this->template Initialize<Manager>(std::move(manager));
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<IsValidDependency<Handle, Manager>::value, int>>
inline AnyDependency<Handle, inline_size, inline_align>&
AnyDependency<Handle, inline_size, inline_align>::operator=(
    Initializer<Manager> manager) {
  this->Destroy();
  this->template Initialize<Manager>(std::move(manager));
  return *this;
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename InPlaceTag, typename... ManagerArgs,
          std::enable_if_t<
              IsValidDependency<Handle, TypeFromInPlaceTagT<
                                            InPlaceTag, ManagerArgs...>>::value,
              int>>
inline AnyDependency<Handle, inline_size, inline_align>::AnyDependency(
    InPlaceTag, ManagerArgs&&... manager_args) {
  this->template Initialize<TypeFromInPlaceTagT<InPlaceTag, ManagerArgs...>>(
      std::forward_as_tuple(std::forward<ManagerArgs>(manager_args)...));
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<
              absl::conjunction<
                  IsValidDependency<Handle, std::decay_t<Manager>>,
                  std::is_move_constructible<std::decay_t<Manager>>>::value,
              int>>
inline void AnyDependency<Handle, inline_size, inline_align>::Reset(
    Manager&& manager) {
  this->Destroy();
  this->template Initialize<std::decay_t<Manager>>(
      std::forward<Manager>(manager));
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<IsValidDependency<Handle, Manager>::value, int>>
inline void AnyDependency<Handle, inline_size, inline_align>::Reset(
    Initializer<Manager> manager) {
  this->Destroy();
  this->template Initialize<Manager>(std::move(manager));
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename InPlaceTag, typename... ManagerArgs,
          std::enable_if_t<
              IsValidDependency<Handle, TypeFromInPlaceTagT<
                                            InPlaceTag, ManagerArgs...>>::value,
              int>>
inline void AnyDependency<Handle, inline_size, inline_align>::Reset(
    InPlaceTag, ManagerArgs&&... manager_args) {
  this->Destroy();
  this->template Initialize<TypeFromInPlaceTagT<InPlaceTag, ManagerArgs...>>(
      std::forward_as_tuple(std::forward<ManagerArgs>(manager_args)...));
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager, typename... ManagerArgs,
          std::enable_if_t<IsValidDependency<Handle, Manager>::value, int>>
inline Manager& AnyDependency<Handle, inline_size, inline_align>::Emplace(
    ManagerArgs&&... manager_args) {
  this->Destroy();
  this->template Initialize<Manager>(
      std::forward_as_tuple(std::forward<ManagerArgs>(manager_args)...));
  return this->template GetManager<Manager>();
}

#if __cpp_deduction_guides
template <typename Handle, size_t inline_size, size_t inline_align>
template <
    template <typename...> class ManagerTemplate, typename... ManagerArgs,
    std::enable_if_t<
        IsValidDependency<
            Handle, TypeFromInPlaceTagT<in_place_template_t<ManagerTemplate>,
                                        ManagerArgs...>>::value,
        int>>
inline TypeFromInPlaceTagT<in_place_template_t<ManagerTemplate>,
                           ManagerArgs...>&
AnyDependency<Handle, inline_size, inline_align>::Emplace(
    ManagerArgs&&... manager_args) {
  return Emplace<TypeFromInPlaceTagT<in_place_template_t<ManagerTemplate>,
                                     ManagerArgs...>>(
      std::forward<ManagerArgs>(manager_args)...);
}
#endif

template <typename Handle>
template <
    typename Manager,
    std::enable_if_t<
        absl::conjunction<absl::negation<std::is_same<
                              std::decay_t<Manager>, AnyDependencyRef<Handle>>>,
                          IsValidDependency<Handle, Manager&&>>::value,
        int>>
inline AnyDependencyRef<Handle>::AnyDependencyRef(Manager&& manager) {
  this->template Initialize<Manager&&>(std::forward<Manager>(manager));
}

template <typename Handle>
template <typename Manager,
          std::enable_if_t<IsValidDependency<Handle, Manager&&>::value, int>>
inline AnyDependencyRef<Handle>::AnyDependencyRef(
    Initializer<Manager> manager,
    typename Initializer<Manager>::ReferenceStorage&& reference_storage) {
  this->template Initialize<Manager&&>(
      std::move(manager).Reference(std::move(reference_storage)));
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_ANY_DEPENDENCY_H_
