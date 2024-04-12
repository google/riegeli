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
#include <cstring>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/meta/type_traits.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/any_dependency_initializer.h"
#include "riegeli/base/any_dependency_internal.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/dependency_base.h"
#include "riegeli/base/dependency_manager.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/maker.h"
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
  Handle get() const { return methods_and_handle_.handle; }

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
  bool IsOwning() const {
    return methods_and_handle_.methods->is_owning(repr_.storage);
  }

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
    self->methods_and_handle_.methods->register_subobjects(self->repr_.storage,
                                                           memory_estimator);
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
  void Initialize(AnyDependencyInitializer<Handle> manager);

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
  // For adopting the state from an instantiation held in an
  // `AnyDependencyInitializer`.
  friend class AnyDependencyInitializer<Handle>;

  using Repr = any_dependency_internal::Repr<Handle, inline_size, inline_align>;
  using MethodsAndHandle = any_dependency_internal::MethodsAndHandle<Handle>;
  using NullMethods = any_dependency_internal::NullMethods<Handle>;
  template <typename Manager>
  using MethodsFor = any_dependency_internal::MethodsFor<
      Handle, Manager, IsInline<Handle, Manager, inline_size, inline_align>()>;

  static constexpr size_t kAvailableSize =
      AvailableSize<Handle, inline_size, inline_align>();
  static constexpr size_t kAvailableAlign =
      AvailableAlign<Handle, inline_size, inline_align>();

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

  MethodsAndHandle methods_and_handle_;
  Repr repr_;
};

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if !__cpp_inline_variables
template <typename Handle, size_t inline_size, size_t inline_align>
constexpr size_t
    AnyDependencyBase<Handle, inline_size, inline_align>::kAvailableSize;
constexpr size_t
    AnyDependencyBase<Handle, inline_size, inline_align>::kAvailableAlign;
#endif

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

  // Holds a `Dependency<Handle, InitializerTargetT<Manager>>`.
  template <
      typename Manager,
      std::enable_if_t<
          absl::conjunction<
              absl::negation<
                  std::is_same<std::decay_t<Manager>, AnyDependency>>,
              IsValidDependency<Handle, InitializerTargetT<Manager>>>::value,
          int> = 0>
  /*implicit*/ AnyDependency(Manager&& manager);
  template <
      typename Manager,
      std::enable_if_t<
          absl::conjunction<
              absl::negation<
                  std::is_same<std::decay_t<Manager>, AnyDependency>>,
              IsValidDependency<Handle, InitializerTargetT<Manager>>>::value,
          int> = 0>
  AnyDependency& operator=(Manager&& manager);

  // Holds the `Dependency` specified when the `AnyDependencyInitializer` was
  // constructed.
  /*implicit*/ AnyDependency(AnyDependencyInitializer<Handle> manager);
  AnyDependency& operator=(AnyDependencyInitializer<Handle> manager);

  AnyDependency(AnyDependency&& that) = default;
  AnyDependency& operator=(AnyDependency&& that) = default;

  // Holds a `Dependency<Handle, Manager>`.
  //
  // Same as `*this = riegeli::Maker<Manager>(manager_args...)`, but returning
  // a reference to the constructed `Manager`.
  template <
      typename Manager, typename... ManagerArgs,
      std::enable_if_t<IsValidDependency<Handle, Manager>::value, int> = 0>
  ABSL_DEPRECATED(
      "Assign riegeli::Maker<Manager>(manager_args...) "
      "to the AnyDependency instead")
  Manager& Emplace(ManagerArgs&&... manager_args);

#if __cpp_deduction_guides
  // Like above, but the exact `Manager` type is deduced using CTAD.
  //
  // Only templates with solely type template parameters are supported.
  template <
      template <typename...> class ManagerTemplate, typename... ManagerArgs,
      std::enable_if_t<IsValidDependency<
                           Handle, DeduceClassTemplateArgumentsT<
                                       ManagerTemplate, ManagerArgs...>>::value,
                       int> = 0>
  ABSL_DEPRECATED(
      "Assign riegeli::Maker<ManagerTemplate>(manager_args...) "
      "to the AnyDependency instead")
  DeduceClassTemplateArgumentsT<ManagerTemplate, ManagerArgs...>& Emplace(
      ManagerArgs&&... manager_args);
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
// `AnyDependencyRef<Handle>` holds a `Dependency<Handle, Manager&&>` (which
// collapses to `Dependency<Handle, Manager&>` if `Manager` is itself an lvalue
// reference) for some `Manager` type, erasing the `Manager` parameter from the
// type of the `AnyDependencyRef`, or is empty.
//
// `AnyDependencyRef<Handle>(manager)` does not own `manager`, even if it
// involves temporaries, hence it should be used only as a parameter of a
// function or constructor, so that the temporaries outlive its usage.
// Instead of storing an `AnyDependencyRef<Handle>` in a variable or returning
// it from a function, consider `riegeli::OwningMaker<Manager>()`,
// `MakerTypeFor<Manager, ManagerArgs...>`, or `AnyDependency<Handle>`.
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

  // Holds a `Dependency<Handle, Manager&&>`.
  template <typename Manager,
            std::enable_if_t<
                absl::conjunction<absl::negation<std::is_same<
                                      std::decay_t<Manager>, AnyDependencyRef>>,
                                  IsValidDependency<Handle, Manager&&>>::value,
                int> = 0>
  /*implicit*/ AnyDependencyRef(
      Manager&& manager ABSL_ATTRIBUTE_LIFETIME_BOUND);

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
  if (inline_size == 0) {
    // Replace an indirect call to `methods_and_handle_.methods->move()` with
    // a plain assignment of `methods_and_handle_.handle` and `repr_`.
    methods_and_handle_.methods =
        std::exchange(that.methods_and_handle_.methods, &NullMethods::kMethods);
    methods_and_handle_.handle = std::exchange(that.methods_and_handle_.handle,
                                               SentinelHandle<Handle>());
    repr_ = that.repr_;
  } else {
    that.methods_and_handle_.handle = SentinelHandle<Handle>();
    methods_and_handle_.methods =
        std::exchange(that.methods_and_handle_.methods, &NullMethods::kMethods);
    methods_and_handle_.methods->move(
        repr_.storage, &methods_and_handle_.handle, that.repr_.storage);
  }
}

template <typename Handle, size_t inline_size, size_t inline_align>
inline AnyDependencyBase<Handle, inline_size, inline_align>&
AnyDependencyBase<Handle, inline_size, inline_align>::operator=(
    AnyDependencyBase&& that) noexcept {
  if (ABSL_PREDICT_TRUE(&that != this)) {
    Destroy();
    if (inline_size == 0) {
      // Replace an indirect call to `methods_and_handle_.methods->move()` with
      // a plain assignment of `methods_and_handle_.handle` and `repr_`.
      methods_and_handle_.methods = std::exchange(
          that.methods_and_handle_.methods, &NullMethods::kMethods);
      methods_and_handle_.handle = std::exchange(
          that.methods_and_handle_.handle, SentinelHandle<Handle>());
      repr_ = that.repr_;
    } else {
      that.methods_and_handle_.handle = SentinelHandle<Handle>();
      methods_and_handle_.methods = std::exchange(
          that.methods_and_handle_.methods, &NullMethods::kMethods);
      methods_and_handle_.methods->move(
          repr_.storage, &methods_and_handle_.handle, that.repr_.storage);
    }
  }
  return *this;
}

template <typename Handle, size_t inline_size, size_t inline_align>
inline void AnyDependencyBase<Handle, inline_size, inline_align>::Initialize() {
  methods_and_handle_.methods = &NullMethods::kMethods;
  new (&methods_and_handle_.handle)
      Handle(any_dependency_internal::SentinelHandle<Handle>());
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
  // `manager.methods_and_handle_.methods->used_size <=
  //      Manager::kAvailableSize`, hence if
  // `Manager::kAvailableSize <=
  //      AvailableSize<Handle, inline_size, inline_align>()` then
  // `manager.methods_and_handle_.methods->used_size <=
  //      AvailableSize<Handle, inline_size, inline_align>()`.
  // No need to check possibly at runtime.
  if ((Manager::kAvailableSize <=
           AvailableSize<Handle, inline_size, inline_align>() ||
       manager.methods_and_handle_.methods->used_size <=
           AvailableSize<Handle, inline_size, inline_align>()) &&
      // Same for alignment.
      (Manager::kAvailableAlign <=
           AvailableAlign<Handle, inline_size, inline_align>() ||
       manager.methods_and_handle_.methods->used_align <=
           AvailableAlign<Handle, inline_size, inline_align>())) {
    // Adopt `manager` instead of wrapping it.
    if (Manager::kAvailableSize == 0 || inline_size == 0) {
      // Replace an indirect call to `methods_and_handle_.methods->move()` with
      // a plain assignment of `methods_and_handle_.handle` and a memory copy of
      // `repr_`.
      //
      // This would safe whenever
      // `manager.methods_and_handle_.methods->used_size == 0`, but this is
      // handled specially only if the condition can be determined at compile
      // time.
      methods_and_handle_.methods = std::exchange(
          manager.methods_and_handle_.methods, &NullMethods::kMethods);
      methods_and_handle_.handle = std::exchange(
          manager.methods_and_handle_.handle, SentinelHandle<Handle>());
      std::memcpy(&repr_, &manager.repr_,
                  UnsignedMin(sizeof(repr_), sizeof(manager.repr_)));
    } else {
      manager.methods_and_handle_.handle = SentinelHandle<Handle>();
      methods_and_handle_.methods = std::exchange(
          manager.methods_and_handle_.methods, &NullMethods::kMethods);
      methods_and_handle_.methods->move(
          repr_.storage, &methods_and_handle_.handle, manager.repr_.storage);
    }
    return;
  }
  methods_and_handle_.methods = &MethodsFor<Manager>::kMethods;
  MethodsFor<Manager>::Construct(repr_.storage, &methods_and_handle_.handle,
                                 std::forward<Manager>(manager));
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<!IsAnyDependency<Handle, Manager>::value, int>>
inline void AnyDependencyBase<Handle, inline_size, inline_align>::Initialize(
    Initializer<Manager> manager) {
  methods_and_handle_.methods = &MethodsFor<Manager>::kMethods;
  MethodsFor<Manager>::Construct(repr_.storage, &methods_and_handle_.handle,
                                 std::move(manager));
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
  // Materialize `Manager` to consider adopting its storage.
  Initialize(std::move(manager).Reference());
}

template <typename Handle, size_t inline_size, size_t inline_align>
inline void AnyDependencyBase<Handle, inline_size, inline_align>::Initialize(
    AnyDependencyInitializer<Handle> manager) {
  std::move(manager).Construct(
      methods_and_handle_, repr_.storage,
      AvailableSize<Handle, inline_size, inline_align>(),
      AvailableAlign<Handle, inline_size, inline_align>());
}

template <typename Handle, size_t inline_size, size_t inline_align>
inline void AnyDependencyBase<Handle, inline_size, inline_align>::Destroy() {
  methods_and_handle_.handle.~Handle();
  methods_and_handle_.methods->destroy(repr_.storage);
}

template <typename Handle, size_t inline_size, size_t inline_align>
inline void AnyDependencyBase<Handle, inline_size, inline_align>::Reset() {
  methods_and_handle_.handle = SentinelHandle<Handle>();
  methods_and_handle_.methods->destroy(repr_.storage);
  methods_and_handle_.methods = &NullMethods::kMethods;
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
  return methods_and_handle_.methods->mutable_get_if(repr_.storage, type_id);
}

template <typename Handle, size_t inline_size, size_t inline_align>
inline const void* AnyDependencyBase<Handle, inline_size, inline_align>::GetIf(
    TypeId type_id) const {
  return methods_and_handle_.methods->const_get_if(repr_.storage, type_id);
}

}  // namespace any_dependency_internal

template <typename Handle, size_t inline_size, size_t inline_align>
template <
    typename Manager,
    std::enable_if_t<
        absl::conjunction<
            absl::negation<
                std::is_same<std::decay_t<Manager>,
                             AnyDependency<Handle, inline_size, inline_align>>>,
            IsValidDependency<Handle, InitializerTargetT<Manager>>>::value,
        int>>
inline AnyDependency<Handle, inline_size, inline_align>::AnyDependency(
    Manager&& manager) {
  this->template Initialize<InitializerTargetT<Manager>>(
      std::forward<Manager>(manager));
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <
    typename Manager,
    std::enable_if_t<
        absl::conjunction<
            absl::negation<
                std::is_same<std::decay_t<Manager>,
                             AnyDependency<Handle, inline_size, inline_align>>>,
            IsValidDependency<Handle, InitializerTargetT<Manager>>>::value,
        int>>
inline AnyDependency<Handle, inline_size, inline_align>&
AnyDependency<Handle, inline_size, inline_align>::operator=(Manager&& manager) {
  this->Destroy();
  this->template Initialize<InitializerTargetT<Manager>>(
      std::forward<Manager>(manager));
  return *this;
}

template <typename Handle, size_t inline_size, size_t inline_align>
inline AnyDependency<Handle, inline_size, inline_align>::AnyDependency(
    AnyDependencyInitializer<Handle> manager) {
  this->Initialize(std::move(manager));
}

template <typename Handle, size_t inline_size, size_t inline_align>
inline AnyDependency<Handle, inline_size, inline_align>&
AnyDependency<Handle, inline_size, inline_align>::operator=(
    AnyDependencyInitializer<Handle> manager) {
  this->Destroy();
  this->Initialize(std::move(manager));
  return *this;
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager, typename... ManagerArgs,
          std::enable_if_t<IsValidDependency<Handle, Manager>::value, int>>
inline Manager& AnyDependency<Handle, inline_size, inline_align>::Emplace(
    ManagerArgs&&... manager_args) {
  this->Destroy();
  this->template Initialize<Manager>(
      riegeli::Maker(std::forward<ManagerArgs>(manager_args)...));
  return this->template GetManager<Manager>();
}

#if __cpp_deduction_guides
template <typename Handle, size_t inline_size, size_t inline_align>
template <
    template <typename...> class ManagerTemplate, typename... ManagerArgs,
    std::enable_if_t<
        IsValidDependency<Handle, DeduceClassTemplateArgumentsT<
                                      ManagerTemplate, ManagerArgs...>>::value,
        int>>
inline DeduceClassTemplateArgumentsT<ManagerTemplate, ManagerArgs...>&
AnyDependency<Handle, inline_size, inline_align>::Emplace(
    ManagerArgs&&... manager_args) {
  return Emplace<
      DeduceClassTemplateArgumentsT<ManagerTemplate, ManagerArgs...>>(
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

}  // namespace riegeli

#endif  // RIEGELI_BASE_ANY_DEPENDENCY_H_
