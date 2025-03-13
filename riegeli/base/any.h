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

#ifndef RIEGELI_BASE_ANY_H_
#define RIEGELI_BASE_ANY_H_

#include <stddef.h>

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <memory>
#include <new>  // IWYU pragma: keep
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/meta/type_traits.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/any_initializer.h"
#include "riegeli/base/any_internal.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/closing_ptr.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/dependency_base.h"
#include "riegeli/base/dependency_manager.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/memory_estimator.h"
#include "riegeli/base/temporary_storage.h"
#include "riegeli/base/type_id.h"
#include "riegeli/base/type_traits.h"

namespace riegeli {

namespace any_internal {

// Common base class of `Any` and `AnyRef`.
//
// `ABSL_ATTRIBUTE_TRIVIAL_ABI` is effective if `inline_size == 0`.
template <typename Handle, size_t inline_size, size_t inline_align>
class
#ifdef ABSL_ATTRIBUTE_TRIVIAL_ABI
    ABSL_ATTRIBUTE_TRIVIAL_ABI
#endif
    AnyBase : public WithEqual<AnyBase<Handle, inline_size, inline_align>>,
              public ConditionallyTrivialAbi<inline_size == 0> {
 public:
  // Returns a `Handle` to the `Manager`, or a default `Handle` for an empty
  // `AnyBase`.
  Handle get() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return methods_and_handle_.handle;
  }

  // If `Handle` supports `operator*`, `AnyBase<Handle>` can be used as a smart
  // pointer to the result of `operator*`, for convenience.
  template <typename DependentHandle = Handle,
            std::enable_if_t<HasDereference<DependentHandle>::value, int> = 0>
  decltype(*std::declval<DependentHandle>()) operator*() const
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    AssertNotNull("Failed precondition of AnyBase::operator*: null handle");
    return *get();
  }

  template <typename DependentHandle = Handle,
            std::enable_if_t<HasArrow<DependentHandle>::value, int> = 0>
  Handle operator->() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    AssertNotNull("Failed precondition of AnyBase::operator->: null handle");
    return get();
  }

  // `AnyBase<Handle>` can be compared against `nullptr`. If `Handle` supports
  // `operator==` with `nullptr`, then delegates the comparison to `Handle`,
  // otherwise returns `true` for a non-empty `AnyBase`.
  friend bool operator==(const AnyBase& a, std::nullptr_t) {
    return a.EqualNullptr();
  }

  // If `true`, the `AnyBase` owns the dependent object, i.e. closing the host
  // object should close the dependent object.
  bool IsOwning() const {
    return methods_and_handle_.methods->is_owning(repr_.storage);
  }

  // If `true`, `get()` stays unchanged when an `AnyBase` is moved.
  static constexpr bool kIsStable = inline_size == 0;

  // If the stored `Manager` has exactly this type or a reference to it, returns
  // a pointer to the `Manager`. Otherwise returns `nullptr`.
  template <
      typename Manager,
      std::enable_if_t<SupportsDependency<Handle, Manager&&>::value, int> = 0>
  Manager* GetIf() ABSL_ATTRIBUTE_LIFETIME_BOUND;
  template <
      typename Manager,
      std::enable_if_t<SupportsDependency<Handle, Manager&&>::value, int> = 0>
  const Manager* GetIf() const ABSL_ATTRIBUTE_LIFETIME_BOUND;

  // Returns the `TypeId` corresponding to the stored `Manager` type, stripping
  // any toplevel reference.
  //
  // `GetIf<Manager>() != nullptr` when `type_id() == TypeId::For<Manager>()`.
  TypeId type_id() const { return methods_and_handle_.methods->type_id; }

  // Supports `MemoryEstimator`.
  friend void RiegeliRegisterSubobjects(const AnyBase* self,
                                        MemoryEstimator& memory_estimator) {
    self->methods_and_handle_.methods->register_subobjects(self->repr_.storage,
                                                           memory_estimator);
  }

 protected:
  // The state is left uninitialized.
  AnyBase() noexcept {}

  AnyBase(AnyBase&& that) noexcept;
  AnyBase& operator=(AnyBase&& that) noexcept;

  ~AnyBase() { Destroy(); }

  void Reset(std::nullptr_t = nullptr);

  // Initializes the state.
  //
  // If `Manager` is already a compatible `Any` or `AnyRef`, possibly wrapped in
  // `ClosingPtrType`, or an rvalue reference to it, adopts its storage instead
  // of keeping an indirection. This causes `GetIf()` to see through it.
  void Initialize();
  template <typename Manager,
            std::enable_if_t<
                absl::conjunction<
                    absl::negation<IsAny<Handle, Manager>>,
                    absl::negation<IsAnyClosingPtr<Handle, Manager>>>::value,
                int> = 0>
  void Initialize(Manager&& manager);
  template <typename Manager,
            std::enable_if_t<IsAny<Handle, Manager>::value, int> = 0>
  void Initialize(Manager&& manager);
  template <typename Manager,
            std::enable_if_t<IsAnyClosingPtr<Handle, Manager>::value, int> = 0>
  void Initialize(Manager&& manager);
  template <typename Manager,
            std::enable_if_t<
                absl::conjunction<
                    absl::negation<IsAny<Handle, Manager>>,
                    absl::negation<IsAnyClosingPtr<Handle, Manager>>>::value,
                int> = 0>
  void Initialize(Initializer<Manager> manager);
  template <typename Manager,
            std::enable_if_t<IsAny<Handle, Manager>::value, int> = 0>
  void Initialize(Initializer<Manager> manager);
  template <typename Manager,
            std::enable_if_t<IsAnyClosingPtr<Handle, Manager>::value, int> = 0>
  void Initialize(Initializer<Manager> manager);
  void InitializeFromAnyInitializer(AnyInitializer<Handle> manager);

  template <typename Manager,
            std::enable_if_t<!std::is_reference<Manager>::value, int> = 0>
  void Adopt(Manager&& manager);
  template <typename Manager,
            std::enable_if_t<std::is_rvalue_reference<Manager>::value, int> = 0>
  void Adopt(Manager&& manager);

  // Destroys the state, leaving it uninitialized.
  void Destroy();

 private:
  // For adopting the state from an instantiation with a different `inline_size`
  // and `inline_align`.
  template <typename OtherHandle, size_t other_inline_size,
            size_t other_inline_align>
  friend class AnyBase;
  // For adopting the state from an instantiation held in an `AnyInitializer`.
  friend class AnyInitializer<Handle>;

  using Repr = any_internal::Repr<Handle, inline_size, inline_align>;
  using MethodsAndHandle = any_internal::MethodsAndHandle<Handle>;
  using NullMethods = any_internal::NullMethods<Handle>;
  template <typename Manager>
  using MethodsFor = any_internal::MethodsFor<
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

  template <typename DependentHandle = Handle,
            std::enable_if_t<IsComparableAgainstNullptr<DependentHandle>::value,
                             int> = 0>
  bool EqualNullptr() const {
    return get() == nullptr;
  }
  template <typename DependentHandle = Handle,
            std::enable_if_t<
                !IsComparableAgainstNullptr<DependentHandle>::value, int> = 0>
  bool EqualNullptr() const {
    return methods_and_handle_.methods == &NullMethods::kMethods;
  }

  MethodsAndHandle methods_and_handle_;
  Repr repr_;
};

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if !__cpp_inline_variables
template <typename Handle, size_t inline_size, size_t inline_align>
constexpr size_t AnyBase<Handle, inline_size, inline_align>::kAvailableSize;
constexpr size_t AnyBase<Handle, inline_size, inline_align>::kAvailableAlign;
#endif

}  // namespace any_internal

// `Any<Handle>` refers to an optionally owned object which is accessed as
// `Handle` and stored as some `Manager` type decided when the `Any` is
// initialized.
//
// Often `Handle` is some pointer `Base*`, and then `Manager` can be e.g.
// `T*` (not owned), `T` (owned), or `std::unique_ptr<T>` (owned), with some `T`
// derived from `Base`.
//
// `Any<Handle>` holds a `Dependency<Handle, Manager>` for some `Manager` type,
// erasing the `Manager` parameter from the type of the `Any`, or is empty.
template <typename Handle, size_t inline_size = 0, size_t inline_align = 0>
class
#ifdef ABSL_NULLABILITY_COMPATIBLE
    ABSL_NULLABILITY_COMPATIBLE
#endif
    Any : public any_internal::AnyBase<Handle, inline_size, inline_align> {
 private:
  // Indirection through `InliningImpl` is needed for MSVC for some reason.
  template <typename... InlineManagers>
  struct InliningImpl {
    using type =
        Any<Handle,
            UnsignedMax(inline_size,
                        sizeof(Dependency<Handle, InlineManagers>)...),
            UnsignedMax(inline_align,
                        alignof(Dependency<Handle, InlineManagers>)...)>;
  };

 public:
  // `Any<Handle>::Inlining<InlineManagers...>` enlarges inline storage of
  // `Any<Handle>`.
  //
  // `InlineManagers` specify the size of inline storage, which allows to avoid
  // heap allocation if `Manager` is among `InlineManagers`, or if
  // `Dependency<Handle, Manager>` fits there regarding size and alignment.
  // By default inline storage is enough for a pointer.
  template <typename... InlineManagers>
  using Inlining = typename InliningImpl<InlineManagers...>::type;

  // Creates an empty `Any`.
  Any() noexcept { this->Initialize(); }
  /*implicit*/ Any(std::nullptr_t) { this->Initialize(); }

  // Holds a `Dependency<Handle, TargetT<Manager>>`.
  //
  // If `TargetT<Manager>` is already a compatible `Any` or `AnyRef`, possibly
  // wrapped in `ClosingPtrType`, or an rvalue reference to it, adopts its
  // storage instead of keeping an indirection. This causes `GetIf()` to see
  // through it.
  template <
      typename Manager,
      std::enable_if_t<
          absl::conjunction<NotSelfCopy<Any, TargetT<Manager>>,
                            TargetSupportsDependency<Handle, Manager>>::value,
          int> = 0>
  /*implicit*/ Any(Manager&& manager);
  template <
      typename Manager,
      std::enable_if_t<
          absl::conjunction<NotSelfCopy<Any, TargetT<Manager>>,
                            TargetSupportsDependency<Handle, Manager>>::value,
          int> = 0>
  Any& operator=(Manager&& manager);

  // Holds the `Dependency` specified when the `AnyInitializer` was constructed.
  //
  // `AnyInitializer` is accepted as a template parameter to avoid this
  // constructor triggering implicit conversions of other parameter types to
  // `AnyInitializer`, which causes template instantiation cycles.
  template <typename Manager,
            std::enable_if_t<
                std::is_same<Manager, AnyInitializer<Handle>>::value, int> = 0>
  /*implicit*/ Any(Manager manager);
  template <typename Manager,
            std::enable_if_t<
                std::is_same<Manager, AnyInitializer<Handle>>::value, int> = 0>
  Any& operator=(Manager manager);

  // Assignment operator which materializes `Any` from its `Initializer`.
  template <typename Manager,
            std::enable_if_t<
                absl::conjunction<NotSelfCopy<Any, Manager>,
                                  std::is_same<TargetT<Manager>, Any>>::value,
                int> = 0>
  Any& operator=(Manager&& manager) {
    riegeli::Reset(*this, std::forward<Manager>(manager));
    return *this;
  }

  Any(Any&& that) = default;
  Any& operator=(Any&& that) = default;

  // Makes `*this` equivalent to a newly constructed `Any`. This avoids
  // constructing a temporary `Any` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(std::nullptr_t = nullptr) {
    Any::AnyBase::Reset();
  }
};

// Specialization of `DependencyManagerImpl<Any<Handle>>`:
// a dependency with ownership determined at runtime.
template <typename Handle, size_t inline_size, size_t inline_align,
          typename ManagerStorage>
class DependencyManagerImpl<Any<Handle, inline_size, inline_align>,
                            ManagerStorage>
    : public DependencyBase<ManagerStorage> {
 public:
  using DependencyManagerImpl::DependencyBase::DependencyBase;

  bool IsOwning() const { return this->manager().IsOwning(); }

  static constexpr bool kIsStable =
      DependencyManagerImpl::DependencyBase::kIsStable ||
      Any<Handle, inline_size, inline_align>::kIsStable;

 protected:
  DependencyManagerImpl(DependencyManagerImpl&& that) = default;
  DependencyManagerImpl& operator=(DependencyManagerImpl&& that) = default;

  ~DependencyManagerImpl() = default;

  Handle ptr() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return this->manager().get();
  }
};

// Specialization of
// `DependencyManagerImpl<ClosingPtrType<Any<Handle>>>`:
// a dependency with ownership determined at runtime.
template <typename Handle, size_t inline_size, size_t inline_align,
          typename ManagerStorage>
class DependencyManagerImpl<
    std::unique_ptr<Any<Handle, inline_size, inline_align>, NullDeleter>,
    ManagerStorage>
    : public DependencyBase<
#ifdef ABSL_ATTRIBUTE_TRIVIAL_ABI
          std::conditional_t<
              absl::is_trivially_relocatable<std::unique_ptr<
                  Any<Handle, inline_size, inline_align>, NullDeleter>>::value,
              std::unique_ptr<Any<Handle, inline_size, inline_align>,
                              NullDeleter>,
              ManagerStorage>
#else
          ManagerStorage
#endif
          > {
 public:
  using DependencyManagerImpl::DependencyBase::DependencyBase;

  bool IsOwning() const {
    return this->manager() != nullptr && this->manager()->IsOwning();
  }

  static constexpr bool kIsStable = true;

 protected:
  DependencyManagerImpl(DependencyManagerImpl&& that) = default;
  DependencyManagerImpl& operator=(DependencyManagerImpl&& that) = default;

  ~DependencyManagerImpl() = default;

  Handle ptr() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return this->manager()->get();
  }
};

// `AnyRef<Handle>` refers to an optionally owned object which is accessed as
// `Handle` and was passed as some `Manager` type decided when the `AnyRef` was
// initialized.
//
// Often `Handle` is some pointer `Base*`, and then `Manager` can be e.g.
// `T&` (not owned), `T&&` (owned), or `std::unique_ptr<T>` (owned), with some
// `T` derived from `Base`.
//
// `AnyRef<Handle>` holds a `Dependency<Handle, Manager&&>` (which collapses to
// `Dependency<Handle, Manager&>` if `Manager` is itself an lvalue reference)
// for some `Manager` type, erasing the `Manager` parameter from the type of the
// `AnyRef`, or is empty.
//
// `AnyRef<Handle>(manager)` does not own `manager`, even if it involves
// temporaries, hence it should be used only as a parameter of a function or
// constructor, so that the temporaries outlive its usage. Instead of storing an
// `AnyRef<Handle>` in a variable or returning it from a function, consider
// `riegeli::OwningMaker<Manager>()`, `MakerTypeFor<Manager, ManagerArgs...>`,
// or `Any<Handle>`.
//
// This allows to pass an unowned dependency by lvalue reference instead of by
// pointer, which allows for a more idiomatic API for passing an object which
// does not need to be valid after the function returns. And this allows to pass
// an owned dependency by rvalue reference instead of by value, which avoids
// moving it.
template <typename Handle>
class
#ifdef ABSL_NULLABILITY_COMPATIBLE
    ABSL_NULLABILITY_COMPATIBLE
#endif
    AnyRef : public any_internal::AnyBase<Handle, 0, 0> {
 public:
  // Creates an empty `AnyRef`.
  AnyRef() noexcept { this->Initialize(); }
  /*implicit*/ AnyRef(std::nullptr_t) { this->Initialize(); }

  // Holds a `Dependency<Handle, TargetRefT<Manager>&&>` when
  // `TargetRefT<Manager>` is not a reference.
  //
  // If `TargetT<Manager>` is already a compatible `Any` or `AnyRef`, possibly
  // wrapped in `ClosingPtrType`, points to its storage instead of keeping an
  // indirection. This causes `GetIf()` to see through it.
  template <typename Manager,
            std::enable_if_t<
                absl::conjunction<
                    NotSelfCopy<AnyRef, TargetT<Manager>>,
                    NotSelfCopy<Any<Handle>, TargetT<Manager>>,
                    absl::negation<std::is_reference<TargetRefT<Manager>>>,
                    SupportsDependency<Handle, TargetRefT<Manager>&&>>::value,
                int> = 0>
  /*implicit*/ AnyRef(Manager&& manager ABSL_ATTRIBUTE_LIFETIME_BOUND,
                      TemporaryStorage<TargetRefT<Manager>>&& storage
                          ABSL_ATTRIBUTE_LIFETIME_BOUND = {});

  // Holds a `DependencyRef<Handle, Manager>` when `TargetRefT<Manager>` is a
  // reference.
  //
  // If `TargetT<Manager>` is an rvalue reference to an already a compatible
  // `Any` or `AnyRef`, possibly wrapped in `ClosingPtrType`, points to its
  // storage instead of keeping an indirection. This causes `GetIf()` to see
  // through it.
  //
  // This constructor is separate so that it does not need `storage`.
  template <
      typename Manager,
      std::enable_if_t<absl::conjunction<
                           NotSelfCopy<AnyRef, TargetT<Manager>>,
                           NotSelfCopy<Any<Handle>, TargetT<Manager>>,
                           std::is_reference<TargetRefT<Manager>>,
                           TargetRefSupportsDependency<Handle, Manager>>::value,
                       int> = 0>
  /*implicit*/ AnyRef(Manager&& manager ABSL_ATTRIBUTE_LIFETIME_BOUND);

  // Adopts the `Dependency` from `Any<Handle>` with no inline storage.
  //
  // This constructor is separate so that it does not need temporary storage nor
  // `ABSL_ATTRIBUTE_LIFETIME_BOUND`.
  template <typename Manager,
            std::enable_if_t<std::is_same<TargetT<Manager>, Any<Handle>>::value,
                             int> = 0>
  /*implicit*/ AnyRef(Manager&& manager);

  // Holds the `Dependency` specified when the `AnyInitializer` was constructed.
  //
  // Prefer taking parameters as `AnyRef<Handle>` instead of
  // `AnyInitializer<Handle>` if they are ultimately always converted to
  // `AnyRef<Handle>`, because this constructor may involve heap allocation.
  //
  // `AnyInitializer` is accepted as a template parameter to avoid this
  // constructor triggering implicit conversions of other parameter types to
  // `AnyInitializer`, which causes template instantiation cycles.
  template <typename Manager,
            std::enable_if_t<
                std::is_same<Manager, AnyInitializer<Handle>>::value, int> = 0>
  /*implicit*/ AnyRef(Manager manager);

  AnyRef(AnyRef&& that) = default;
  AnyRef& operator=(AnyRef&&) = delete;
};

// Specialization of `DependencyManagerImpl<AnyRef<Handle>>`:
// a dependency with ownership determined at runtime.
template <typename Handle, typename ManagerStorage>
class DependencyManagerImpl<AnyRef<Handle>, ManagerStorage>
    : public DependencyBase<ManagerStorage> {
 public:
  using DependencyManagerImpl::DependencyBase::DependencyBase;

  bool IsOwning() const { return this->manager().IsOwning(); }

  static constexpr bool kIsStable =
      DependencyManagerImpl::DependencyBase::kIsStable ||
      AnyRef<Handle>::kIsStable;

 protected:
  DependencyManagerImpl(DependencyManagerImpl&& that) = default;
  DependencyManagerImpl& operator=(DependencyManagerImpl&& that) = default;

  ~DependencyManagerImpl() = default;

  Handle ptr() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return this->manager().get();
  }
};

// Specialization of
// `DependencyManagerImpl<ClosingPtrType<AnyRef<Handle>, Deleter>>`:
// a dependency with ownership determined at runtime.
template <typename Handle, typename ManagerStorage>
class DependencyManagerImpl<std::unique_ptr<AnyRef<Handle>, NullDeleter>,
                            ManagerStorage>
    : public DependencyBase<
#ifdef ABSL_ATTRIBUTE_TRIVIAL_ABI
          std::conditional_t<
              absl::is_trivially_relocatable<
                  std::unique_ptr<AnyRef<Handle>, NullDeleter>>::value,
              std::unique_ptr<AnyRef<Handle>, NullDeleter>, ManagerStorage>
#else
          ManagerStorage
#endif
          > {
 public:
  using DependencyManagerImpl::DependencyBase::DependencyBase;

  bool IsOwning() const {
    return this->manager() != nullptr && this->manager()->IsOwning();
  }

  static constexpr bool kIsStable = true;

 protected:
  DependencyManagerImpl(DependencyManagerImpl&& that) = default;
  DependencyManagerImpl& operator=(DependencyManagerImpl&& that) = default;

  ~DependencyManagerImpl() = default;

  Handle ptr() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return this->manager()->get();
  }
};

// Implementation details follow.

namespace any_internal {

template <typename Handle, size_t inline_size, size_t inline_align>
inline AnyBase<Handle, inline_size, inline_align>::AnyBase(
    AnyBase&& that) noexcept {
  if (inline_size == 0) {
    // Replace an indirect call to `move()` with plain assignments.
    methods_and_handle_.methods = that.methods_and_handle_.methods;
    methods_and_handle_.handle = that.methods_and_handle_.handle;
    repr_ = that.repr_;
  } else {
    that.methods_and_handle_.methods->move(that.repr_.storage, repr_.storage,
                                           &methods_and_handle_);
  }
  that.methods_and_handle_.methods = &NullMethods::kMethods;
  that.methods_and_handle_.handle = SentinelHandle<Handle>();
}

template <typename Handle, size_t inline_size, size_t inline_align>
inline AnyBase<Handle, inline_size, inline_align>&
AnyBase<Handle, inline_size, inline_align>::operator=(AnyBase&& that) noexcept {
  if (ABSL_PREDICT_TRUE(&that != this)) {
    Destroy();
    if (inline_size == 0) {
      // Replace an indirect call to `move()` with plain assignments.
      methods_and_handle_.methods = that.methods_and_handle_.methods;
      methods_and_handle_.handle = that.methods_and_handle_.handle;
      repr_ = that.repr_;
    } else {
      that.methods_and_handle_.methods->move(that.repr_.storage, repr_.storage,
                                             &methods_and_handle_);
    }
    that.methods_and_handle_.methods = &NullMethods::kMethods;
    that.methods_and_handle_.handle = SentinelHandle<Handle>();
  }
  return *this;
}

template <typename Handle, size_t inline_size, size_t inline_align>
inline void AnyBase<Handle, inline_size, inline_align>::Initialize() {
  methods_and_handle_.methods = &NullMethods::kMethods;
  new (&methods_and_handle_.handle)
      Handle(any_internal::SentinelHandle<Handle>());
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<
              absl::conjunction<
                  absl::negation<IsAny<Handle, Manager>>,
                  absl::negation<IsAnyClosingPtr<Handle, Manager>>>::value,
              int>>
inline void AnyBase<Handle, inline_size, inline_align>::Initialize(
    Manager&& manager) {
  Initialize<Manager>(Initializer<Manager>(std::forward<Manager>(manager)));
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<IsAny<Handle, Manager>::value, int>>
inline void AnyBase<Handle, inline_size, inline_align>::Initialize(
    Manager&& manager) {
  using ManagerValue = std::remove_reference_t<Manager>;
  // `manager.methods_and_handle_.methods->used_size <=
  //      ManagerValue::kAvailableSize`, hence if
  // `ManagerValue::kAvailableSize <= kAvailableSize` then
  // `manager.methods_and_handle_.methods->used_size <= kAvailableSize`.
  // No need to check possibly at runtime.
  if ((ManagerValue::kAvailableSize <= kAvailableSize ||
       manager.methods_and_handle_.methods->used_size <= kAvailableSize) &&
      // Same for alignment.
      (ManagerValue::kAvailableAlign <= kAvailableAlign ||
       manager.methods_and_handle_.methods->used_align <= kAvailableAlign)) {
    // Adopt `manager` by moving its representation as is.
    if (inline_size == 0 || ManagerValue::kAvailableSize == 0) {
      // Replace an indirect call to `move()` with plain assignments and
      // a memory copy.
      //
      // This would be safe if `ManagerValue::kAvailableSize != 0` while
      // `that.manager.methods_and_handle_.methods->used_size == 0`, but this is
      // handled specially only if the condition can be determined at compile
      // time.
      methods_and_handle_.methods = manager.methods_and_handle_.methods;
      methods_and_handle_.handle = manager.methods_and_handle_.handle;
      std::memcpy(&repr_, &manager.repr_,
                  UnsignedMin(sizeof(repr_), sizeof(manager.repr_)));
    } else {
      manager.methods_and_handle_.methods->move(
          manager.repr_.storage, repr_.storage, &methods_and_handle_);
    }
    manager.methods_and_handle_.methods = &NullMethods::kMethods;
    manager.methods_and_handle_.handle = SentinelHandle<Handle>();
    return;
  }
  // Adopt `manager` by moving its representation to the heap if `Manager` is
  // a value, or referring to it if `Manager` is a reference.
  Adopt<Manager>(std::forward<Manager>(manager));
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<IsAnyClosingPtr<Handle, Manager>::value, int>>
inline void AnyBase<Handle, inline_size, inline_align>::Initialize(
    Manager&& manager) {
  if (manager == nullptr) {
    Initialize();
    return;
  }
  // Adopt `*manager` by referring to its representation.
  manager->methods_and_handle_.methods->make_reference(
      manager->repr_.storage, repr_.storage, &methods_and_handle_);
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<
              absl::conjunction<
                  absl::negation<IsAny<Handle, Manager>>,
                  absl::negation<IsAnyClosingPtr<Handle, Manager>>>::value,
              int>>
inline void AnyBase<Handle, inline_size, inline_align>::Initialize(
    Initializer<Manager> manager) {
  methods_and_handle_.methods = &MethodsFor<Manager>::kMethods;
  MethodsFor<Manager>::Construct(repr_.storage, &methods_and_handle_.handle,
                                 std::move(manager));
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<IsAny<Handle, Manager>::value, int>>
inline void AnyBase<Handle, inline_size, inline_align>::Initialize(
    Initializer<Manager> manager) {
  // Materialize `Manager` to adopt its storage.
  Initialize<Manager>(std::move(manager).Reference());
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<IsAnyClosingPtr<Handle, Manager>::value, int>>
inline void AnyBase<Handle, inline_size, inline_align>::Initialize(
    Initializer<Manager> manager) {
  // Materialize `Manager` to adopt its storage.
  Initialize<Manager>(std::move(manager).Construct());
}

template <typename Handle, size_t inline_size, size_t inline_align>
inline void
AnyBase<Handle, inline_size, inline_align>::InitializeFromAnyInitializer(
    AnyInitializer<Handle> manager) {
  std::move(manager).Construct(methods_and_handle_, repr_.storage,
                               kAvailableSize, kAvailableAlign);
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<!std::is_reference<Manager>::value, int>>
inline void AnyBase<Handle, inline_size, inline_align>::Adopt(
    Manager&& manager) {
  manager.methods_and_handle_.methods->move_to_heap(
      manager.repr_.storage, repr_.storage, &methods_and_handle_);
  manager.methods_and_handle_.methods = &NullMethods::kMethods;
  manager.methods_and_handle_.handle = SentinelHandle<Handle>();
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<std::is_rvalue_reference<Manager>::value, int>>
inline void AnyBase<Handle, inline_size, inline_align>::Adopt(
    Manager&& manager) {
  manager.methods_and_handle_.methods->make_reference(
      manager.repr_.storage, repr_.storage, &methods_and_handle_);
}

template <typename Handle, size_t inline_size, size_t inline_align>
inline void AnyBase<Handle, inline_size, inline_align>::Destroy() {
  methods_and_handle_.methods->destroy(repr_.storage);
  methods_and_handle_.handle.~Handle();
}

template <typename Handle, size_t inline_size, size_t inline_align>
inline void AnyBase<Handle, inline_size, inline_align>::Reset(std::nullptr_t) {
  methods_and_handle_.methods->destroy(repr_.storage);
  methods_and_handle_.methods = &NullMethods::kMethods;
  methods_and_handle_.handle = SentinelHandle<Handle>();
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<SupportsDependency<Handle, Manager&&>::value, int>>
inline Manager* AnyBase<Handle, inline_size, inline_align>::GetIf()
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  if (type_id() != TypeId::For<Manager>()) return nullptr;
  return RIEGELI_ASSUME_NOTNULL(static_cast<Manager*>(const_cast<void*>(
      methods_and_handle_.methods->get_raw_manager(repr_.storage))));
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<SupportsDependency<Handle, Manager&&>::value, int>>
inline const Manager* AnyBase<Handle, inline_size, inline_align>::GetIf() const
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  if (type_id() != TypeId::For<Manager>()) return nullptr;
  return RIEGELI_ASSUME_NOTNULL(static_cast<const Manager*>(
      methods_and_handle_.methods->get_raw_manager(repr_.storage)));
}

}  // namespace any_internal

template <typename Handle, size_t inline_size, size_t inline_align>
template <
    typename Manager,
    std::enable_if_t<
        absl::conjunction<NotSelfCopy<Any<Handle, inline_size, inline_align>,
                                      TargetT<Manager>>,
                          TargetSupportsDependency<Handle, Manager>>::value,
        int>>
inline Any<Handle, inline_size, inline_align>::Any(Manager&& manager) {
  this->template Initialize<TargetT<Manager>>(std::forward<Manager>(manager));
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <
    typename Manager,
    std::enable_if_t<
        absl::conjunction<NotSelfCopy<Any<Handle, inline_size, inline_align>,
                                      TargetT<Manager>>,
                          TargetSupportsDependency<Handle, Manager>>::value,
        int>>
inline Any<Handle, inline_size, inline_align>&
Any<Handle, inline_size, inline_align>::operator=(Manager&& manager) {
  this->Destroy();
  this->template Initialize<TargetT<Manager>>(std::forward<Manager>(manager));
  return *this;
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <
    typename Manager,
    std::enable_if_t<std::is_same<Manager, AnyInitializer<Handle>>::value, int>>
inline Any<Handle, inline_size, inline_align>::Any(Manager manager) {
  this->InitializeFromAnyInitializer(std::move(manager));
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <
    typename Manager,
    std::enable_if_t<std::is_same<Manager, AnyInitializer<Handle>>::value, int>>
inline Any<Handle, inline_size, inline_align>&
Any<Handle, inline_size, inline_align>::operator=(Manager manager) {
  this->Destroy();
  this->InitializeFromAnyInitializer(std::move(manager));
  return *this;
}

template <typename Handle>
template <typename Manager,
          std::enable_if_t<
              absl::conjunction<
                  NotSelfCopy<AnyRef<Handle>, TargetT<Manager>>,
                  NotSelfCopy<Any<Handle>, TargetT<Manager>>,
                  absl::negation<std::is_reference<TargetRefT<Manager>>>,
                  SupportsDependency<Handle, TargetRefT<Manager>&&>>::value,
              int>>
inline AnyRef<Handle>::AnyRef(Manager&& manager ABSL_ATTRIBUTE_LIFETIME_BOUND,
                              TemporaryStorage<TargetRefT<Manager>>&& storage
                                  ABSL_ATTRIBUTE_LIFETIME_BOUND) {
  this->template Initialize<TargetRefT<Manager>&&>(
      Initializer<TargetRefT<Manager>>(std::forward<Manager>(manager))
          .Reference(std::move(storage)));
}

template <typename Handle>
template <
    typename Manager,
    std::enable_if_t<
        absl::conjunction<NotSelfCopy<AnyRef<Handle>, TargetT<Manager>>,
                          NotSelfCopy<Any<Handle>, TargetT<Manager>>,
                          std::is_reference<TargetRefT<Manager>>,
                          TargetRefSupportsDependency<Handle, Manager>>::value,
        int>>
inline AnyRef<Handle>::AnyRef(Manager&& manager ABSL_ATTRIBUTE_LIFETIME_BOUND) {
  this->template Initialize<TargetRefT<Manager>>(
      std::forward<Manager>(manager));
}

template <typename Handle>
template <
    typename Manager,
    std::enable_if_t<std::is_same<TargetT<Manager>, Any<Handle>>::value, int>>
inline AnyRef<Handle>::AnyRef(Manager&& manager) {
  this->template Initialize<TargetT<Manager>>(std::forward<Manager>(manager));
}

template <typename Handle>
template <
    typename Manager,
    std::enable_if_t<std::is_same<Manager, AnyInitializer<Handle>>::value, int>>
inline AnyRef<Handle>::AnyRef(Manager manager) {
  this->InitializeFromAnyInitializer(std::move(manager));
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_ANY_H_
