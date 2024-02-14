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

#include <cstddef>
#include <memory>
#include <new>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/casts.h"
#include "absl/base/optimization.h"
#include "absl/meta/type_traits.h"
#include "absl/strings/string_view.h"
#include "absl/utility/utility.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/dependency_base.h"
#include "riegeli/base/dependency_manager.h"
#include "riegeli/base/memory_estimator.h"
#include "riegeli/base/type_id.h"
#include "riegeli/base/type_traits.h"

namespace riegeli {

#if __cpp_deduction_guides

// Tag type used for in-place construction when the type to construct needs to
// be specified.
//
// Like `absl::in_place_type_t`, but only the class template is specified, with
// the exact type being deduced using CTAD.
//
// Only templates with solely type template parameters are supported.

template <template <typename...> class T>
struct in_place_template_t {};

template <template <typename...> class T>
constexpr in_place_template_t<T> in_place_template = {};

#endif

template <typename Handle, size_t inline_size, size_t inline_align>
class AnyDependencyImpl;
template <typename Handle, size_t inline_size, size_t inline_align>
class AnyDependencyRefImpl;

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
//
// The optional `InlineManagers` parameters specify the size of inline storage,
// which allows to avoid heap allocation if `Manager` is among `InlineManagers`
// or if `Dependency<Handle, Manager>` fits there regarding size and alignment.
// By default inline storage is enough for a pointer.
template <typename Handle, typename... InlineManagers>
using AnyDependency = AnyDependencyImpl<
    Handle,
    UnsignedMax(size_t{0}, sizeof(Dependency<Handle, InlineManagers>)...),
    UnsignedMax(size_t{0}, alignof(Dependency<Handle, InlineManagers>)...)>;

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
//
// In contrast to `AnyDependency`, for `AnyDependencyRef` it is rare that
// specifying `InlineManagers` is useful, because a typical
// `Dependency<Handle, Manager&&>` deduced by `AnyDependencyRef` fits in the
// default inline storage.
template <typename Handle, typename... InlineManagers>
using AnyDependencyRef = AnyDependencyRefImpl<
    Handle,
    UnsignedMax(size_t{0}, sizeof(Dependency<Handle, InlineManagers>)...),
    UnsignedMax(size_t{0}, alignof(Dependency<Handle, InlineManagers>)...)>;

namespace any_dependency_internal {

// Variants of `Repr`:
//  * Empty `AnyDependency`: `Repr` is not used
//  * Held by pointer: `storage` holds `Dependency<Handle, Manager>*`
//  * Stored inline: `storage` holds `Dependency<Handle, Manager>`
template <typename Handle, size_t inline_size, size_t inline_align>
struct Repr {
  alignas(UnsignedMax(
      alignof(void*),
      inline_align)) char storage[UnsignedMax(sizeof(void*), inline_size)];
};

// By convention, a parameter of type `Storage` points to
// `Repr<Handle, inline_size, inline_align>::storage`.
using Storage = char[];

// A `Dependency<Handle, Manager>` is stored inline in
// `Repr<Handle, inline_size, inline_align>` if it fits in that storage and is
// movable. If `inline_size == 0`, the dependency is also required to be stable
// (because then `AnyDependency` declares itself stable) and trivially
// relocatable (because then `AnyDependency` declares itself with trivial ABI).

template <typename Handle, size_t inline_size, size_t inline_align,
          typename Manager, typename Enable = void>
struct IsInline : std::false_type {};

template <typename Handle, size_t inline_size, size_t inline_align,
          typename Manager>
struct IsInline<
    Handle, inline_size, inline_align, Manager,
    std::enable_if_t<absl::conjunction<
        std::integral_constant<
            bool, sizeof(Dependency<Handle, Manager>) <=
                          sizeof(Repr<Handle, inline_size, inline_align>) &&
                      alignof(Dependency<Handle, Manager>) <=
                          alignof(Repr<Handle, inline_size, inline_align>)>,
        std::is_move_constructible<Dependency<Handle, Manager>>,
        absl::disjunction<
            std::integral_constant<bool, (inline_size > 0)>,
            absl::conjunction<
                std::integral_constant<bool,
                                       Dependency<Handle, Manager>::kIsStable>
#ifdef ABSL_ATTRIBUTE_TRIVIAL_ABI
                ,
                absl::is_trivially_relocatable<Dependency<Handle, Manager>>
#endif
                >>>::value>> : std::true_type {
};

// Conditionally make the ABI trivial. To be used as a base class, with the
// derived class having an unconditional `ABSL_ATTRIBUTE_TRIVIAL_ABI` (it will
// not be effective if a base class does not have trivial ABI).
template <bool is_trivial>
class ConditionallyTrivialAbi;

template <>
class ConditionallyTrivialAbi<false> {
 public:
  ~ConditionallyTrivialAbi() {}
};
template <>
class ConditionallyTrivialAbi<true> {};

// Method pointers.
template <typename Handle>
struct Methods {
  // Destroys `self`.
  void (*destroy)(Storage self);
  size_t inline_size_used;   // Or 0 if inline storage is not used.
  size_t inline_align_used;  // Or 0 if inline storage is not used.
  // Constructs `self` and `*self_handle` by moving from `that`, and destroys
  // `that`.
  void (*move)(Storage self, Handle* self_handle, Storage that);
  bool (*is_owning)(const Storage self);
  // Returns the `std::remove_reference_t<Manager>*` if `type_id` matches
  // `std::remove_reference_t<Manager>`, otherwise returns `nullptr`.
  void* (*mutable_get_if)(Storage self, TypeId type_id);
  const void* (*const_get_if)(const Storage self, TypeId type_id);
  void (*register_subobjects)(const Storage self,
                              MemoryEstimator& memory_estimator);
};

template <typename Handle, size_t inline_size, size_t inline_align,
          typename Manager, typename Enable = void>
struct MethodsFor;
template <typename Handle>
struct NullMethods;

// `IsAnyDependency` detects `AnyDependencyImpl` or `AnyDependencyRefImpl` type
// with the given `Handle`.

template <typename Handle, typename T>
struct IsAnyDependency : std::false_type {};

template <typename Handle, size_t inline_size, size_t inline_align>
struct IsAnyDependency<Handle,
                       AnyDependencyImpl<Handle, inline_size, inline_align>>
    : std::true_type {};
template <typename Handle, size_t inline_size, size_t inline_align>
struct IsAnyDependency<Handle,
                       AnyDependencyRefImpl<Handle, inline_size, inline_align>>
    : std::true_type {};

}  // namespace any_dependency_internal

// `AnyDependencyImpl` implements `AnyDependency` after `InlineManagers` have
// been reduced to their maximum size and alignment.
//
// `ABSL_ATTRIBUTE_TRIVIAL_ABI` is effective if `inline_size == 0`.
template <typename Handle, size_t inline_size, size_t inline_align = 0>
class
#ifdef ABSL_ATTRIBUTE_TRIVIAL_ABI
    ABSL_ATTRIBUTE_TRIVIAL_ABI
#endif
        AnyDependencyImpl
    : public WithEqual<AnyDependencyImpl<Handle, inline_size, inline_align>>,
      public ConditionallyAbslNullabilityCompatible<
          IsComparableAgainstNullptr<Handle>::value>,
      public any_dependency_internal::ConditionallyTrivialAbi<inline_size ==
                                                              0> {
 public:
  // Creates an empty `AnyDependencyImpl`.
  AnyDependencyImpl() noexcept;

  // Holds a `Dependency<Handle, std::decay_t<Manager>>`.
  //
  // The `Manager` type is deduced from the constructor argument.
  template <
      typename Manager,
      std::enable_if_t<
          absl::conjunction<
              absl::negation<
                  std::is_same<std::decay_t<Manager>, AnyDependencyImpl>>,
              IsValidDependency<Handle, std::decay_t<Manager>>,
              std::is_convertible<Manager&&, std::decay_t<Manager>>>::value,
          int> = 0>
  /*implicit*/ AnyDependencyImpl(Manager&& manager);
  template <
      typename Manager,
      std::enable_if_t<
          absl::conjunction<
              absl::negation<
                  std::is_same<std::decay_t<Manager>, AnyDependencyImpl>>,
              IsValidDependency<Handle, std::decay_t<Manager>>,
              std::is_convertible<Manager&&, std::decay_t<Manager>>>::value,
          int> = 0>
  AnyDependencyImpl& operator=(Manager&& manager);

  // Holds a `Dependency<Handle, Manager>`.
  //
  // The `Manager` type is specified with a tag (`absl::in_place_type<Manager>`)
  // because constructor templates do not support specifying template arguments
  // explicitly. The `Manager` is constructed from `manager_args`.
  template <
      typename Manager, typename... ManagerArgs,
      std::enable_if_t<IsValidDependency<Handle, Manager>::value, int> = 0>
  /*implicit*/ AnyDependencyImpl(absl::in_place_type_t<Manager>,
                                 ManagerArgs&&... manager_args);

#if __cpp_deduction_guides
  // Like above, but the exact `Manager` type is deduced using CTAD from
  // `ManagerTemplate(std::forward<ManagerArgs>(manager_args)...)`.
  //
  // Only templates with solely type template parameters are supported.
  template <
      template <typename...> class ManagerTemplate, typename... ManagerArgs,
      std::enable_if_t<IsValidDependency<
                           Handle, DeduceClassTemplateArgumentsT<
                                       ManagerTemplate, ManagerArgs...>>::value,
                       int> = 0>
  /*implicit*/ AnyDependencyImpl(in_place_template_t<ManagerTemplate>,
                                 ManagerArgs&&... manager_args);
#endif

  AnyDependencyImpl(AnyDependencyImpl&& that) noexcept;
  AnyDependencyImpl& operator=(AnyDependencyImpl&& that) noexcept;

  ~AnyDependencyImpl();

  // Makes `*this` equivalent to a newly constructed `AnyDependencyImpl`. This
  // avoids constructing a temporary `AnyDependencyImpl` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset();
  template <
      typename Manager,
      std::enable_if_t<
          absl::conjunction<
              IsValidDependency<Handle, std::decay_t<Manager>>,
              std::is_convertible<Manager&&, std::decay_t<Manager>>>::value,
          int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Manager&& manager);
  template <
      typename Manager, typename... ManagerArgs,
      std::enable_if_t<IsValidDependency<Handle, Manager>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(absl::in_place_type_t<Manager>,
                                          ManagerArgs&&... manager_args);
#if __cpp_deduction_guides
  template <
      template <typename...> class ManagerTemplate, typename... ManagerArgs,
      std::enable_if_t<IsValidDependency<
                           Handle, DeduceClassTemplateArgumentsT<
                                       ManagerTemplate, ManagerArgs...>>::value,
                       int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(in_place_template_t<ManagerTemplate>,
                                          ManagerArgs&&... manager_args);
#endif

  // Holds a `Dependency<Handle, Manager>`.
  //
  // The `Manager` is constructed from the given constructor arguments.
  //
  // Same as `Reset(absl::in_place_type<Manager>,
  //                std::forward<ManagerArgs>(manager_args)...)`,
  // returning a reference to the constructed `Manager`.
  template <
      typename Manager, typename... ManagerArgs,
      std::enable_if_t<IsValidDependency<Handle, Manager>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES Manager& Emplace(ManagerArgs&&... manager_args);

#if __cpp_deduction_guides
  // Like above, but the exact `Manager` type is deduced using CTAD from
  // `ManagerTemplate(std::forward<ManagerArgs>(manager_args)...)`.
  //
  // Only templates with solely type template parameters are supported.
  template <
      template <typename...> class ManagerTemplate, typename... ManagerArgs,
      std::enable_if_t<IsValidDependency<
                           Handle, DeduceClassTemplateArgumentsT<
                                       ManagerTemplate, ManagerArgs...>>::value,
                       int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES
      DeduceClassTemplateArgumentsT<ManagerTemplate, ManagerArgs...>&
      Emplace(ManagerArgs&&... manager_args);
#endif

  // Returns a `Handle` to the `Manager`, or a default `Handle` for an empty
  // `AnyDependencyImpl`.
  Handle get() const { return handle_; }

  // If `Handle` is `Base*`, `AnyDependencyImpl<Base*>` can be used as a smart
  // pointer to `Base`, for convenience.
  template <typename DependentHandle = Handle,
            std::enable_if_t<HasDereference<DependentHandle>::value, int> = 0>
  decltype(*std::declval<DependentHandle>()) operator*() const {
    AssertNotNull(
        "Failed precondition of AnyDependency::operator*: null handle");
    return *get();
  }

  template <typename DependentHandle = Handle,
            std::enable_if_t<HasArrow<DependentHandle>::value, int> = 0>
  Handle operator->() const {
    AssertNotNull(
        "Failed precondition of AnyDependency::operator->: null handle");
    return get();
  }

  // If `Handle` is `Base*`, `AnyDependencyImpl<Base*>` can be compared against
  // `nullptr`.
  template <typename DependentHandle = Handle,
            std::enable_if_t<IsComparableAgainstNullptr<DependentHandle>::value,
                             int> = 0>
  friend bool operator==(const AnyDependencyImpl& a, std::nullptr_t) {
    return a.get() == nullptr;
  }

  // If `true`, the `AnyDependencyImpl` owns the dependent object, i.e. closing
  // the host object should close the dependent object.
  bool IsOwning() const { return methods_->is_owning(repr_.storage); }

  // If `true`, `get()` stays unchanged when an `AnyDependencyImpl` is moved.
  static constexpr bool kIsStable = inline_size == 0;

  // If the `Manager` has exactly this type or a reference to it, returns a
  // pointer to the `Manager`. If the `Manager` is an `AnyDependency` (possibly
  // wrapped in a reference or `std::unique_ptr`), propagates `GetIf()` to it.
  // Otherwise returns `nullptr`.
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

  friend void RiegeliRegisterSubobjects(const AnyDependencyImpl& self,
                                        MemoryEstimator& memory_estimator) {
    self.methods_->register_subobjects(self.repr_.storage, memory_estimator);
  }

 private:
  // For adopting `methods_` and `repr_` from an instantiation with a different
  // `inline_size` and `inline_align.
  template <typename OtherHandle, size_t other_inline_size,
            size_t other_inline_align>
  friend class AnyDependencyImpl;

  using Repr = any_dependency_internal::Repr<Handle, inline_size, inline_align>;
  using Methods = any_dependency_internal::Methods<Handle>;
  using NullMethods = any_dependency_internal::NullMethods<Handle>;
  template <typename Manager>
  using MethodsFor = any_dependency_internal::MethodsFor<Handle, inline_size,
                                                         inline_align, Manager>;

  // Initializes `methods_`, `repr_`, and `handle_`, avoiding a redundant
  // indirection and adopting them from `manager` instead if `Manager` is
  // already a compatible `AnyDependencyImpl` or `AnyDependencyRefImpl`.
  template <typename Manager,
            std::enable_if_t<
                absl::conjunction<
                    absl::negation<std::is_rvalue_reference<Manager>>,
                    absl::negation<any_dependency_internal::IsAnyDependency<
                        Handle, Manager>>>::value,
                int> = 0>
  void Initialize(const Manager& manager);
  template <typename Manager,
            std::enable_if_t<
                absl::conjunction<
                    absl::negation<std::is_lvalue_reference<Manager>>,
                    absl::negation<any_dependency_internal::IsAnyDependency<
                        Handle, Manager>>>::value,
                int> = 0>
  void Initialize(Manager&& manager);
  template <typename Manager,
            std::enable_if_t<any_dependency_internal::IsAnyDependency<
                                 Handle, Manager>::value,
                             int> = 0>
  void Initialize(Manager&& manager);
  template <typename Manager, typename... ManagerArgs,
            std::enable_if_t<!std::is_reference<Manager>::value, int> = 0>
  void Initialize(ManagerArgs&&... manager_args);

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

// Specialization of `DependencyManagerImpl<AnyDependency<Handle>>`:
// a dependency with ownership determined at runtime.
template <typename Handle, size_t inline_size, size_t inline_align,
          typename ManagerStorage>
class DependencyManagerImpl<
    AnyDependencyImpl<Handle, inline_size, inline_align>, ManagerStorage>
    : public DependencyBase<ManagerStorage> {
 public:
  using DependencyManagerImpl::DependencyBase::DependencyBase;

  bool IsOwning() const { return this->manager().IsOwning(); }

  static constexpr bool kIsStable =
      DependencyManagerImpl::DependencyBase::kIsStable ||
      AnyDependencyImpl<Handle, inline_size, inline_align>::kIsStable;

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
    std::unique_ptr<AnyDependencyImpl<Handle, inline_size, inline_align>,
                    Deleter>,
    ManagerStorage>
    : public DependencyBase<std::conditional_t<
          std::is_empty<Deleter>::value,
          std::unique_ptr<AnyDependencyImpl<Handle, inline_size, inline_align>,
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

// `AnyDependencyRefImpl` implements `AnyDependencyRef` after `InlineManagers`
// have been reduced to their maximum size and alignment.
template <typename Handle, size_t inline_size, size_t inline_align = 0>
class AnyDependencyRefImpl
    : public AnyDependencyImpl<Handle, inline_size, inline_align> {
 public:
  // Creates an empty `AnyDependencyRefImpl`.
  AnyDependencyRefImpl() = default;

  // Holds a `Dependency<Handle, Manager&&>` (which collapses to
  // `Dependency<Handle, Manager&>` if `Manager` is itself an lvalue reference).
  //
  // The `Manager` type is deduced from the constructor argument.
  template <
      typename Manager,
      std::enable_if_t<
          absl::conjunction<absl::negation<std::is_same<std::decay_t<Manager>,
                                                        AnyDependencyRefImpl>>,
                            IsValidDependency<Handle, Manager&&>>::value,
          int> = 0>
  /*implicit*/ AnyDependencyRefImpl(Manager&& manager)
      : AnyDependencyRefImpl::AnyDependencyImpl(
            absl::in_place_type<Manager&&>, std::forward<Manager>(manager)) {}
  template <
      typename Manager,
      std::enable_if_t<
          absl::conjunction<absl::negation<std::is_same<std::decay_t<Manager>,
                                                        AnyDependencyRefImpl>>,
                            IsValidDependency<Handle, Manager&&>>::value,
          int> = 0>
  AnyDependencyRefImpl& operator=(Manager&& manager) {
    AnyDependencyRefImpl::AnyDependencyImpl::Reset(
        absl::in_place_type<Manager&&>, std::forward<Manager>(manager));
    return *this;
  }

  // Holds a `Dependency<Handle, Manager>`.
  //
  // The `Manager` type is specified with a tag (`absl::in_place_type<Manager>`)
  // because constructor templates do not support specifying template arguments
  // explicitly. The `Manager` is constructed from `manager_args`.
  template <
      typename Manager, typename... ManagerArgs,
      std::enable_if_t<IsValidDependency<Handle, Manager>::value, int> = 0>
  explicit AnyDependencyRefImpl(absl::in_place_type_t<Manager>,
                                ManagerArgs&&... manager_args)
      : AnyDependencyRefImpl::AnyDependencyImpl(
            absl::in_place_type<Manager>,
            std::forward<ManagerArgs>(manager_args)...) {}

#if __cpp_deduction_guides
  // Like above, but the exact `Manager` type is deduced using CTAD from
  // `ManagerTemplate(std::forward<ManagerArgs>(manager_args)...)`.
  //
  // Only templates with solely type template parameters are supported.
  template <
      template <typename...> class ManagerTemplate, typename... ManagerArgs,
      std::enable_if_t<IsValidDependency<
                           Handle, DeduceClassTemplateArgumentsT<
                                       ManagerTemplate, ManagerArgs...>>::value,
                       int> = 0>
  explicit AnyDependencyRefImpl(in_place_template_t<ManagerTemplate>,
                                ManagerArgs&&... manager_args)
      : AnyDependencyRefImpl::AnyDependencyImpl(
            in_place_template<ManagerTemplate>,
            std::forward<ManagerArgs>(manager_args)...) {}
#endif

  AnyDependencyRefImpl(AnyDependencyRefImpl&& that) = default;
  AnyDependencyRefImpl& operator=(AnyDependencyRefImpl&& that) = default;

  // Makes `*this` equivalent to a newly constructed `AnyDependencyRefImpl`.
  // This avoids constructing a temporary `AnyDependencyRefImpl` and moving from
  // it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset() {
    AnyDependencyRefImpl::AnyDependencyImpl::Reset();
  }
  template <
      typename Manager,
      std::enable_if_t<IsValidDependency<Handle, Manager&&>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Manager&& manager) {
    AnyDependencyRefImpl::AnyDependencyImpl::Reset(
        absl::in_place_type<Manager&&>, std::forward<Manager>(manager));
  }
  template <
      typename Manager, typename... ManagerArgs,
      std::enable_if_t<IsValidDependency<Handle, Manager>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(absl::in_place_type_t<Manager>,
                                          ManagerArgs&&... manager_args) {
    AnyDependencyRefImpl::AnyDependencyImpl::Reset(
        absl::in_place_type<Manager>,
        std::forward<ManagerArgs>(manager_args)...);
  }
#if __cpp_deduction_guides
  template <
      template <typename...> class ManagerTemplate, typename... ManagerArgs,
      std::enable_if_t<IsValidDependency<
                           Handle, DeduceClassTemplateArgumentsT<
                                       ManagerTemplate, ManagerArgs...>>::value,
                       int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(in_place_template_t<ManagerTemplate>,
                                          ManagerArgs&&... manager_args) {
    AnyDependencyRefImpl::AnyDependencyImpl::Reset(
        in_place_template<ManagerTemplate>,
        std::forward<ManagerArgs>(manager_args)...);
  }
#endif
};

// Specialization of `DependencyManagerImpl<AnyDependencyRef<Handle>>`:
// a dependency with ownership determined at runtime.
template <typename Handle, size_t inline_size, size_t inline_align,
          typename ManagerStorage>
class DependencyManagerImpl<
    AnyDependencyRefImpl<Handle, inline_size, inline_align>, ManagerStorage>
    : public DependencyBase<ManagerStorage> {
 public:
  using DependencyManagerImpl::DependencyBase::DependencyBase;

  bool IsOwning() const { return this->manager().IsOwning(); }

  static constexpr bool kIsStable =
      DependencyManagerImpl::DependencyBase::kIsStable ||
      AnyDependencyRefImpl<Handle, inline_size, inline_align>::kIsStable;

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
template <typename Handle, size_t inline_size, size_t inline_align,
          typename Deleter, typename ManagerStorage>
class DependencyManagerImpl<
    std::unique_ptr<AnyDependencyRefImpl<Handle, inline_size, inline_align>,
                    Deleter>,
    ManagerStorage>
    : public DependencyBase<std::conditional_t<
          std::is_empty<Deleter>::value,
          std::unique_ptr<
              AnyDependencyRefImpl<Handle, inline_size, inline_align>, Deleter>,
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

// Implementation details follow.

namespace any_dependency_internal {

// `any_dependency_internal::SentinelHandle<Handle>()` returns a sentinel
// `Handle` constructed from
// `RiegeliDependencySentinel(static_cast<Handle*>(nullptr))`.

template <typename Handle>
inline Handle SentinelHandleInternal(const Handle& handle) {
  return handle;
}

template <typename Handle>
inline Handle SentinelHandleInternal(Handle&& handle) {
  // `std::move(handle)` is correct and `std::forward<Handle>(handle)` is not
  // necessary: `Handle` is always specified explicitly and is never an lvalue
  // reference.
  return std::move(handle);
}

template <typename Handle, typename... HandleArgs>
inline Handle SentinelHandleInternal(std::tuple<HandleArgs...> handle_args) {
  return absl::make_from_tuple<Handle>(std::move(handle_args));
}

template <typename Handle>
inline Handle SentinelHandle() {
  return SentinelHandleInternal<Handle>(
      RiegeliDependencySentinel(static_cast<Handle*>(nullptr)));
}

template <typename Handle>
struct NullMethods {
 private:
  static void Destroy(ABSL_ATTRIBUTE_UNUSED Storage self) {}
  static void Move(ABSL_ATTRIBUTE_UNUSED Storage self, Handle* self_handle,
                   ABSL_ATTRIBUTE_UNUSED Storage that) {
    new (self_handle) Handle(SentinelHandle<Handle>());
  }
  static bool IsOwning(ABSL_ATTRIBUTE_UNUSED const Storage self) {
    return false;
  }
  static void* MutableGetIf(ABSL_ATTRIBUTE_UNUSED Storage self,
                            ABSL_ATTRIBUTE_UNUSED TypeId type_id) {
    return nullptr;
  }
  static const void* ConstGetIf(ABSL_ATTRIBUTE_UNUSED const Storage self,
                                ABSL_ATTRIBUTE_UNUSED TypeId type_id) {
    return nullptr;
  }
  static void RegisterSubobjects(
      ABSL_ATTRIBUTE_UNUSED const Storage self,
      ABSL_ATTRIBUTE_UNUSED MemoryEstimator& memory_estimator) {}

 public:
  static constexpr Methods<Handle> kMethods = {
      Destroy,           0, 0, Move, IsOwning, MutableGetIf, ConstGetIf,
      RegisterSubobjects};
};

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if !__cpp_inline_variables
template <typename Handle>
constexpr Methods<Handle> NullMethods<Handle>::kMethods;
#endif

template <typename Handle, size_t inline_size, size_t inline_align,
          typename Manager, typename Enable>
struct MethodsFor {
  template <typename DependentManager = Manager,
            std::enable_if_t<!std::is_rvalue_reference<DependentManager>::value,
                             int> = 0>
  static void Construct(Storage self, Handle* self_handle,
                        const Manager& manager) {
    new (self)
        Dependency<Handle, Manager>*(new Dependency<Handle, Manager>(manager));
    new (self_handle) Handle(dep_ptr(self)->get());
  }
  template <typename DependentManager = Manager,
            std::enable_if_t<!std::is_lvalue_reference<DependentManager>::value,
                             int> = 0>
  static void Construct(Storage self, Handle* self_handle, Manager&& manager) {
    new (self) Dependency<Handle, Manager>*(
        new Dependency<Handle, Manager>(std::move(manager)));
    new (self_handle) Handle(dep_ptr(self)->get());
  }
  template <
      typename... ManagerArgs, typename DependentManager = Manager,
      std::enable_if_t<!std::is_reference<DependentManager>::value, int> = 0>
  static void Construct(Storage self, Handle* self_handle,
                        std::tuple<ManagerArgs...> manager_args) {
    new (self) Dependency<Handle, Manager>*(
        new Dependency<Handle, Manager>(std::move(manager_args)));
    new (self_handle) Handle(dep_ptr(self)->get());
  }

  static Manager& GetManager(Storage self) { return dep_ptr(self)->manager(); }

 private:
  static Dependency<Handle, Manager>* dep_ptr(const Storage self) {
    return *
#if __cpp_lib_launder >= 201606
        std::launder
#endif
        (reinterpret_cast<Dependency<Handle, Manager>* const*>(self));
  }

  static void Destroy(Storage self) { delete dep_ptr(self); }
  static void Move(Storage self, Handle* self_handle, Storage that) {
    new (self) Dependency<Handle, Manager>*(dep_ptr(that));
    new (self_handle) Handle(dep_ptr(self)->get());
  }
  static bool IsOwning(const Storage self) { return dep_ptr(self)->IsOwning(); }
  static void* MutableGetIf(Storage self, TypeId type_id) {
    return dep_ptr(self)->GetIf(type_id);
  }
  static const void* ConstGetIf(const Storage self, TypeId type_id) {
    return absl::implicit_cast<const Dependency<Handle, Manager>*>(
               dep_ptr(self))
        ->GetIf(type_id);
  }
  static void RegisterSubobjects(const Storage self,
                                 MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterDynamicObject(*dep_ptr(self));
  }

 public:
  static constexpr Methods<Handle> kMethods = {
      Destroy,           0, 0, Move, IsOwning, MutableGetIf, ConstGetIf,
      RegisterSubobjects};
};

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if !__cpp_inline_variables
template <typename Handle, size_t inline_size, size_t inline_align,
          typename Manager, typename Enable>
constexpr Methods<Handle>
    MethodsFor<Handle, inline_size, inline_align, Manager, Enable>::kMethods;
#endif

template <typename Handle, size_t inline_size, size_t inline_align,
          typename Manager>
struct MethodsFor<Handle, inline_size, inline_align, Manager,
                  std::enable_if_t<IsInline<Handle, inline_size, inline_align,
                                            Manager>::value>> {
  template <typename DependentManager = Manager,
            std::enable_if_t<!std::is_rvalue_reference<DependentManager>::value,
                             int> = 0>
  static void Construct(Storage self, Handle* self_handle,
                        const Manager& manager) {
    new (self) Dependency<Handle, Manager>(manager);
    new (self_handle) Handle(dep(self).get());
  }
  template <typename DependentManager = Manager,
            std::enable_if_t<!std::is_lvalue_reference<DependentManager>::value,
                             int> = 0>
  static void Construct(Storage self, Handle* self_handle, Manager&& manager) {
    new (self) Dependency<Handle, Manager>(std::move(manager));
    new (self_handle) Handle(dep(self).get());
  }
  template <
      typename... ManagerArgs, typename DependentManager = Manager,
      std::enable_if_t<!std::is_reference<DependentManager>::value, int> = 0>
  static void Construct(Storage self, Handle* self_handle,
                        std::tuple<ManagerArgs...> manager_args) {
    new (self) Dependency<Handle, Manager>(std::move(manager_args));
    new (self_handle) Handle(dep(self).get());
  }

  static Manager& GetManager(Storage self) { return dep(self).manager(); }

 private:
  static Dependency<Handle, Manager>& dep(Storage self) {
    return *
#if __cpp_lib_launder >= 201606
        std::launder
#endif
        (reinterpret_cast<Dependency<Handle, Manager>*>(self));
  }
  static const Dependency<Handle, Manager>& dep(const Storage self) {
    return *
#if __cpp_lib_launder >= 201606
        std::launder
#endif
        (reinterpret_cast<const Dependency<Handle, Manager>*>(self));
  }

  static void Destroy(Storage self) {
    dep(self).~Dependency<Handle, Manager>();
  }
  static void Move(Storage self, Handle* self_handle, Storage that) {
    new (self) Dependency<Handle, Manager>(std::move(dep(that)));
    dep(that).~Dependency<Handle, Manager>();
    new (self_handle) Handle(dep(self).get());
  }
  static bool IsOwning(const Storage self) { return dep(self).IsOwning(); }
  static void* MutableGetIf(Storage self, TypeId type_id) {
    return dep(self).GetIf(type_id);
  }
  static const void* ConstGetIf(const Storage self, TypeId type_id) {
    return dep(self).GetIf(type_id);
  }
  static void RegisterSubobjects(const Storage self,
                                 MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(dep(self));
  }

 public:
  static constexpr Methods<Handle> kMethods = {
      Destroy,
      sizeof(Dependency<Handle, Manager>),
      alignof(Dependency<Handle, Manager>),
      Move,
      IsOwning,
      MutableGetIf,
      ConstGetIf,
      RegisterSubobjects};
};

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if !__cpp_inline_variables
template <typename Handle, size_t inline_size, size_t inline_align,
          typename Manager>
constexpr Methods<Handle>
    MethodsFor<Handle, inline_size, inline_align, Manager,
               std::enable_if_t<IsInline<Handle, inline_size, inline_align,
                                         Manager>::value>>::kMethods;
#endif

}  // namespace any_dependency_internal

template <typename Handle, size_t inline_size, size_t inline_align>
inline AnyDependencyImpl<Handle, inline_size,
                         inline_align>::AnyDependencyImpl() noexcept
    : methods_(&NullMethods::kMethods),
      handle_(any_dependency_internal::SentinelHandle<Handle>()) {}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<
              absl::conjunction<
                  absl::negation<std::is_same<
                      std::decay_t<Manager>,
                      AnyDependencyImpl<Handle, inline_size, inline_align>>>,
                  IsValidDependency<Handle, std::decay_t<Manager>>,
                  std::is_convertible<Manager&&, std::decay_t<Manager>>>::value,
              int>>
inline AnyDependencyImpl<Handle, inline_size, inline_align>::AnyDependencyImpl(
    Manager&& manager) {
  Initialize<std::decay_t<Manager>>(std::forward<Manager>(manager));
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<
              absl::conjunction<
                  absl::negation<std::is_same<
                      std::decay_t<Manager>,
                      AnyDependencyImpl<Handle, inline_size, inline_align>>>,
                  IsValidDependency<Handle, std::decay_t<Manager>>,
                  std::is_convertible<Manager&&, std::decay_t<Manager>>>::value,
              int>>
inline AnyDependencyImpl<Handle, inline_size, inline_align>&
AnyDependencyImpl<Handle, inline_size, inline_align>::operator=(
    Manager&& manager) {
  handle_.~Handle();
  methods_->destroy(repr_.storage);
  Initialize<std::decay_t<Manager>>(std::forward<Manager>(manager));
  return *this;
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager, typename... ManagerArgs,
          std::enable_if_t<IsValidDependency<Handle, Manager>::value, int>>
inline AnyDependencyImpl<Handle, inline_size, inline_align>::AnyDependencyImpl(
    absl::in_place_type_t<Manager>, ManagerArgs&&... manager_args) {
  Initialize<Manager>(std::forward<ManagerArgs>(manager_args)...);
}

#if __cpp_deduction_guides
template <typename Handle, size_t inline_size, size_t inline_align>
template <
    template <typename...> class ManagerTemplate, typename... ManagerArgs,
    std::enable_if_t<
        IsValidDependency<Handle, DeduceClassTemplateArgumentsT<
                                      ManagerTemplate, ManagerArgs...>>::value,
        int>>
inline AnyDependencyImpl<Handle, inline_size, inline_align>::AnyDependencyImpl(
    in_place_template_t<ManagerTemplate>, ManagerArgs&&... manager_args)
    : AnyDependencyImpl(
          absl::in_place_type<
              DeduceClassTemplateArgumentsT<ManagerTemplate, ManagerArgs...>>,
          std::forward<ManagerArgs>(manager_args)...) {}
#endif

template <typename Handle, size_t inline_size, size_t inline_align>
inline AnyDependencyImpl<Handle, inline_size, inline_align>::AnyDependencyImpl(
    AnyDependencyImpl&& that) noexcept {
  that.handle_ = any_dependency_internal::SentinelHandle<Handle>();
  methods_ = std::exchange(that.methods_, &NullMethods::kMethods);
  methods_->move(repr_.storage, &handle_, that.repr_.storage);
}

template <typename Handle, size_t inline_size, size_t inline_align>
inline AnyDependencyImpl<Handle, inline_size, inline_align>&
AnyDependencyImpl<Handle, inline_size, inline_align>::operator=(
    AnyDependencyImpl&& that) noexcept {
  if (ABSL_PREDICT_TRUE(&that != this)) {
    handle_.~Handle();
    methods_->destroy(repr_.storage);
    that.handle_ = any_dependency_internal::SentinelHandle<Handle>();
    methods_ = std::exchange(that.methods_, &NullMethods::kMethods);
    methods_->move(repr_.storage, &handle_, that.repr_.storage);
  }
  return *this;
}

template <typename Handle, size_t inline_size, size_t inline_align>
inline AnyDependencyImpl<Handle, inline_size,
                         inline_align>::~AnyDependencyImpl() {
  handle_.~Handle();
  methods_->destroy(repr_.storage);
}

template <typename Handle, size_t inline_size, size_t inline_align>
inline void AnyDependencyImpl<Handle, inline_size, inline_align>::Reset() {
  handle_ = any_dependency_internal::SentinelHandle<Handle>();
  methods_->destroy(repr_.storage);
  methods_ = &NullMethods::kMethods;
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<
              absl::conjunction<
                  IsValidDependency<Handle, std::decay_t<Manager>>,
                  std::is_convertible<Manager&&, std::decay_t<Manager>>>::value,
              int>>
inline void AnyDependencyImpl<Handle, inline_size, inline_align>::Reset(
    Manager&& manager) {
  handle_.~Handle();
  methods_->destroy(repr_.storage);
  Initialize<std::decay_t<Manager>>(std::forward<Manager>(manager));
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager, typename... ManagerArgs,
          std::enable_if_t<IsValidDependency<Handle, Manager>::value, int>>
inline void AnyDependencyImpl<Handle, inline_size, inline_align>::Reset(
    absl::in_place_type_t<Manager>, ManagerArgs&&... manager_args) {
  handle_.~Handle();
  methods_->destroy(repr_.storage);
  Initialize<Manager>(std::forward<ManagerArgs>(manager_args)...);
}

#if __cpp_deduction_guides
template <typename Handle, size_t inline_size, size_t inline_align>
template <
    template <typename...> class ManagerTemplate, typename... ManagerArgs,
    std::enable_if_t<
        IsValidDependency<Handle, DeduceClassTemplateArgumentsT<
                                      ManagerTemplate, ManagerArgs...>>::value,
        int>>
inline void AnyDependencyImpl<Handle, inline_size, inline_align>::Reset(
    in_place_template_t<ManagerTemplate>, ManagerArgs&&... manager_args) {
  Reset(absl::in_place_type<
            DeduceClassTemplateArgumentsT<ManagerTemplate, ManagerArgs...>>,
        std::forward<ManagerArgs>(manager_args)...);
}
#endif

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager, typename... ManagerArgs,
          std::enable_if_t<IsValidDependency<Handle, Manager>::value, int>>
inline Manager& AnyDependencyImpl<Handle, inline_size, inline_align>::Emplace(
    ManagerArgs&&... manager_args) {
  Reset(absl::in_place_type<Manager>,
        std::forward<ManagerArgs>(manager_args)...);
  return MethodsFor<Manager>::GetManager(repr_.storage);
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
AnyDependencyImpl<Handle, inline_size, inline_align>::Emplace(
    ManagerArgs&&... manager_args) {
  return Emplace<
      DeduceClassTemplateArgumentsT<ManagerTemplate, ManagerArgs...>>(
      std::forward<ManagerArgs>(manager_args)...);
}
#endif

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<
              absl::conjunction<
                  absl::negation<std::is_rvalue_reference<Manager>>,
                  absl::negation<any_dependency_internal::IsAnyDependency<
                      Handle, Manager>>>::value,
              int>>
inline void AnyDependencyImpl<Handle, inline_size, inline_align>::Initialize(
    const Manager& manager) {
  methods_ = &MethodsFor<Manager>::kMethods;
  MethodsFor<Manager>::Construct(repr_.storage, &handle_, manager);
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<
              absl::conjunction<
                  absl::negation<std::is_lvalue_reference<Manager>>,
                  absl::negation<any_dependency_internal::IsAnyDependency<
                      Handle, Manager>>>::value,
              int>>
inline void AnyDependencyImpl<Handle, inline_size, inline_align>::Initialize(
    Manager&& manager) {
  methods_ = &MethodsFor<Manager>::kMethods;
  // `std::move(manager)` is correct and `std::forward<Manager>(manager)` is not
  // necessary: `Manager` is never an lvalue reference because this is excluded
  // in the constraint.
  MethodsFor<Manager>::Construct(repr_.storage, &handle_, std::move(manager));
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <
    typename Manager,
    std::enable_if_t<
        any_dependency_internal::IsAnyDependency<Handle, Manager>::value, int>>
inline void AnyDependencyImpl<Handle, inline_size, inline_align>::Initialize(
    Manager&& manager) {
  if ((sizeof(typename Manager::Repr) <= sizeof(Repr) ||
       manager.methods_->inline_size_used <= sizeof(Repr)) &&
      (alignof(typename Manager::Repr) <= alignof(Repr) ||
       manager.methods_->inline_align_used <= alignof(Repr))) {
    // Adopt `manager` instead of wrapping it.
    manager.handle_ = any_dependency_internal::SentinelHandle<Handle>();
    methods_ = std::exchange(manager.methods_, &NullMethods::kMethods);
    methods_->move(repr_.storage, &handle_, manager.repr_.storage);
    return;
  }
  methods_ = &MethodsFor<Manager>::kMethods;
  MethodsFor<Manager>::Construct(repr_.storage, &handle_,
                                 std::forward<Manager>(manager));
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager, typename... ManagerArgs,
          std::enable_if_t<!std::is_reference<Manager>::value, int>>
inline void AnyDependencyImpl<Handle, inline_size, inline_align>::Initialize(
    ManagerArgs&&... manager_args) {
  methods_ = &MethodsFor<Manager>::kMethods;
  MethodsFor<Manager>::Construct(
      repr_.storage, &handle_,
      std::forward_as_tuple(std::forward<ManagerArgs>(manager_args)...));
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<IsValidDependency<Handle, Manager>::value, int>>
Manager* AnyDependencyImpl<Handle, inline_size, inline_align>::GetIf() {
  return static_cast<Manager*>(GetIf(TypeId::For<Manager>()));
}

template <typename Handle, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<IsValidDependency<Handle, Manager>::value, int>>
const Manager* AnyDependencyImpl<Handle, inline_size, inline_align>::GetIf()
    const {
  return static_cast<const Manager*>(GetIf(TypeId::For<Manager>()));
}

template <typename Handle, size_t inline_size, size_t inline_align>
inline void* AnyDependencyImpl<Handle, inline_size, inline_align>::GetIf(
    TypeId type_id) {
  return methods_->mutable_get_if(repr_.storage, type_id);
}

template <typename Handle, size_t inline_size, size_t inline_align>
inline const void* AnyDependencyImpl<Handle, inline_size, inline_align>::GetIf(
    TypeId type_id) const {
  return methods_->const_get_if(repr_.storage, type_id);
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_ANY_DEPENDENCY_H_
