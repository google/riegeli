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
#include "absl/utility/utility.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/dependency.h"
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

template <typename Ptr, size_t inline_size, size_t inline_align>
class AnyDependencyImpl;
template <typename Ptr, size_t inline_size, size_t inline_align>
class AnyDependencyRefImpl;

// `AnyDependency<Ptr>` refers to an optionally owned object which is accessed
// as `Ptr` and stored as some `Manager` type decided when the `AnyDependency`
// is initialized.
//
// Often `Ptr` is some pointer `P*`, and then `Manager` can be e.g.
// `M*` (not owned), `M` (owned), or `std::unique_ptr<M>` (owned), with `M`
// derived from `P`.
//
// `AnyDependency<Ptr>` holds a `Dependency<Ptr, Manager>` for some `Manager`
// type, erasing the `Manager` parameter from the type of the `AnyDependency`,
// or is empty.
//
// The optional `InlineManagers` parameters specify the size of inline storage,
// which allows to avoid heap allocation if `Manager` is among `InlineManagers`
// or if `Dependency<Ptr, Manager>` fits there regarding size and alignment.
// By default inline storage is enough for a pointer.
template <typename Ptr, typename... InlineManagers>
using AnyDependency = AnyDependencyImpl<
    Ptr, UnsignedMax(size_t{0}, sizeof(Dependency<Ptr, InlineManagers>)...),
    UnsignedMax(size_t{0}, alignof(Dependency<Ptr, InlineManagers>)...)>;

// `AnyDependencyRef<Ptr>` refers to an optionally owned object which is
// accessed as `Ptr` and was passed as some `Manager` type decided when the
// `AnyDependencyRef` was initialized.
//
// Often `Ptr` is some pointer `P*`, and then `Manager` can be e.g.
// `M&` (not owned), `M&&` (owned), or `std::unique_ptr<M>` (owned), with `M`
// derived from `P`.
//
// `AnyDependencyRef<Ptr>` derives from `AnyDependency<Ptr>`, replacing the
// constructors such that the `Manager` type is deduced from the constructor
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
// `Dependency<Ptr, Manager&&>` deduced by `AnyDependencyRef` fits in the
// default inline storage.
template <typename Ptr, typename... InlineManagers>
using AnyDependencyRef = AnyDependencyRefImpl<
    Ptr, UnsignedMax(size_t{0}, sizeof(Dependency<Ptr, InlineManagers>)...),
    UnsignedMax(size_t{0}, alignof(Dependency<Ptr, InlineManagers>)...)>;

namespace any_dependency_internal {

// Variants of `Repr`:
//  * Empty `AnyDependency`: `Repr` is not used
//  * Held by pointer: `storage` holds `Dependency<Ptr, Manager>*`
//  * Stored inline: `storage` holds `Dependency<Ptr, Manager>`
template <typename Ptr, size_t inline_size, size_t inline_align>
struct Repr {
  alignas(UnsignedMax(
      alignof(void*),
      inline_align)) char storage[UnsignedMax(sizeof(void*), inline_size)];
};

// By convention, a parameter of type `Storage` points to
// `Repr<Ptr, inline_size, inline_align>::storage`.
using Storage = char[];

// A `Dependency<Ptr, Manager>` is stored inline in
// `Repr<Ptr, inline_size, inline_align>` if it fits in that storage and is
// movable. If `inline_size == 0`, the dependency is also required to be stable
// (because then `AnyDependency` declares itself stable) and trivially
// relocatable (because then `AnyDependency` declares itself with trivial ABI).

template <typename Ptr, size_t inline_size, size_t inline_align,
          typename Manager, typename Enable = void>
struct IsInline : std::false_type {};

template <typename Ptr, size_t inline_size, size_t inline_align,
          typename Manager>
struct IsInline<
    Ptr, inline_size, inline_align, Manager,
    std::enable_if_t<absl::conjunction<
        std::integral_constant<
            bool, sizeof(Dependency<Ptr, Manager>) <=
                          sizeof(Repr<Ptr, inline_size, inline_align>) &&
                      alignof(Dependency<Ptr, Manager>) <=
                          alignof(Repr<Ptr, inline_size, inline_align>)>,
        std::is_move_constructible<Dependency<Ptr, Manager>>,
        absl::disjunction<
            std::integral_constant<bool, (inline_size > 0)>,
            absl::conjunction<
                std::integral_constant<bool,
                                       Dependency<Ptr, Manager>::kIsStable>
#ifdef ABSL_ATTRIBUTE_TRIVIAL_ABI
                ,
                absl::is_trivially_relocatable<Dependency<Ptr, Manager>>
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
template <typename Ptr>
struct Methods {
  // Destroys `self`.
  void (*destroy)(Storage self);
  size_t inline_size_used;   // Or 0 if inline storage is not used.
  size_t inline_align_used;  // Or 0 if inline storage is not used.
  // Constructs `self` and `*self_ptr` by moving from `that`, and destroys
  // `that`.
  void (*move)(Storage self, Ptr* self_ptr, Storage that);
  bool (*is_owning)(const Storage self);
  // Returns the `std::remove_reference_t<Manager>*` if `type_id` matches
  // `std::remove_reference_t<Manager>`, otherwise returns `nullptr`.
  void* (*mutable_get_if)(Storage self, TypeId type_id);
  const void* (*const_get_if)(const Storage self, TypeId type_id);
  void (*register_subobjects)(const Storage self,
                              MemoryEstimator& memory_estimator);
};

template <typename Ptr, size_t inline_size, size_t inline_align,
          typename Manager, typename Enable = void>
struct MethodsFor;
template <typename Ptr>
struct NullMethods;

// `IsAnyDependency` detects `AnyDependencyImpl` or `AnyDependencyRefImpl` type
// with the given `Ptr`.
template <typename Ptr, typename T>
struct IsAnyDependency : std::false_type {};
template <typename Ptr, size_t inline_size, size_t inline_align>
struct IsAnyDependency<Ptr, AnyDependencyImpl<Ptr, inline_size, inline_align>>
    : std::true_type {};
template <typename Ptr, size_t inline_size, size_t inline_align>
struct IsAnyDependency<Ptr,
                       AnyDependencyRefImpl<Ptr, inline_size, inline_align>>
    : std::true_type {};

}  // namespace any_dependency_internal

// `AnyDependencyImpl` implements `AnyDependency` after `InlineManagers` have
// been reduced to their maximum size and alignment.
//
// `ABSL_ATTRIBUTE_TRIVIAL_ABI` is effective if `inline_size == 0`.
template <typename Ptr, size_t inline_size, size_t inline_align = 0>
class
#ifdef ABSL_ATTRIBUTE_TRIVIAL_ABI
    ABSL_ATTRIBUTE_TRIVIAL_ABI
#endif
        AnyDependencyImpl
    : public WithEqual<AnyDependencyImpl<Ptr, inline_size, inline_align>>,
      public ConditionallyAbslNullabilityCompatible<
          std::is_pointer<Ptr>::value>,
      public any_dependency_internal::ConditionallyTrivialAbi<inline_size ==
                                                              0> {
 public:
  // Creates an empty `AnyDependencyImpl`.
  AnyDependencyImpl() noexcept;

  // Holds a `Dependency<Ptr, std::decay_t<Manager>>`.
  //
  // The `Manager` type is deduced from the constructor argument.
  template <
      typename Manager,
      std::enable_if_t<
          absl::conjunction<
              absl::negation<
                  std::is_same<std::decay_t<Manager>, AnyDependencyImpl>>,
              IsValidDependency<Ptr, std::decay_t<Manager>>,
              std::is_convertible<Manager&&, std::decay_t<Manager>>>::value,
          int> = 0>
  /*implicit*/ AnyDependencyImpl(Manager&& manager);
  template <
      typename Manager,
      std::enable_if_t<
          absl::conjunction<
              absl::negation<
                  std::is_same<std::decay_t<Manager>, AnyDependencyImpl>>,
              IsValidDependency<Ptr, std::decay_t<Manager>>,
              std::is_convertible<Manager&&, std::decay_t<Manager>>>::value,
          int> = 0>
  AnyDependencyImpl& operator=(Manager&& manager);

  // Holds a `Dependency<Ptr, Manager>`.
  //
  // The `Manager` type is specified with a tag (`absl::in_place_type<Manager>`)
  // because constructor templates do not support specifying template arguments
  // explicitly. The `Manager` is constructed from `manager_args`.
  template <typename Manager, typename... ManagerArgs,
            std::enable_if_t<IsValidDependency<Ptr, Manager>::value, int> = 0>
  /*implicit*/ AnyDependencyImpl(absl::in_place_type_t<Manager>,
                                 ManagerArgs&&... manager_args);

#if __cpp_deduction_guides
  // Like above, but the exact `Manager` type is deduced using CTAD from
  // `ManagerTemplate(std::forward<ManagerArgs>(manager_args)...)`.
  //
  // Only templates with solely type template parameters are supported.
  template <
      template <typename...> class ManagerTemplate, typename... ManagerArgs,
      std::enable_if_t<
          IsValidDependency<Ptr, DeduceClassTemplateArgumentsT<
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
  template <typename Manager,
            std::enable_if_t<
                absl::conjunction<IsValidDependency<Ptr, std::decay_t<Manager>>,
                                  std::is_convertible<
                                      Manager&&, std::decay_t<Manager>>>::value,
                int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Manager&& manager);
  template <typename Manager, typename... ManagerArgs,
            std::enable_if_t<IsValidDependency<Ptr, Manager>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(absl::in_place_type_t<Manager>,
                                          ManagerArgs&&... manager_args);
#if __cpp_deduction_guides
  template <
      template <typename...> class ManagerTemplate, typename... ManagerArgs,
      std::enable_if_t<
          IsValidDependency<Ptr, DeduceClassTemplateArgumentsT<
                                     ManagerTemplate, ManagerArgs...>>::value,
          int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(in_place_template_t<ManagerTemplate>,
                                          ManagerArgs&&... manager_args);
#endif

  // Holds a `Dependency<Ptr, Manager>`.
  //
  // The `Manager` is constructed from the given constructor arguments.
  //
  // Same as `Reset(absl::in_place_type<Manager>,
  //                std::forward<ManagerArgs>(manager_args)...)`,
  // returning a reference to the constructed `Manager`.
  template <typename Manager, typename... ManagerArgs,
            std::enable_if_t<IsValidDependency<Ptr, Manager>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES Manager& Emplace(ManagerArgs&&... manager_args);

#if __cpp_deduction_guides
  // Like above, but the exact `Manager` type is deduced using CTAD from
  // `ManagerTemplate(std::forward<ManagerArgs>(manager_args)...)`.
  //
  // Only templates with solely type template parameters are supported.
  template <
      template <typename...> class ManagerTemplate, typename... ManagerArgs,
      std::enable_if_t<
          IsValidDependency<Ptr, DeduceClassTemplateArgumentsT<
                                     ManagerTemplate, ManagerArgs...>>::value,
          int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES
      DeduceClassTemplateArgumentsT<ManagerTemplate, ManagerArgs...>&
      Emplace(ManagerArgs&&... manager_args);
#endif

  // Returns a `Ptr` to the `Manager`, or a default `Ptr` for an empty
  // `AnyDependencyImpl`.
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
  // To avoid having two variants of `AnyDependencyImpl<P*>` based on this
  // subtle distinction, its only variant is more permissive regarding the
  // `Dependency` while also more permissive regarding its usage.
  Ptr get() const { return ptr_; }

  // If `Ptr` is `P*`, `AnyDependencyImpl<P*>` can be used as a smart pointer to
  // `P`, for convenience.
  template <typename DependentPtr = Ptr,
            std::enable_if_t<std::is_pointer<DependentPtr>::value, int> = 0>
  std::remove_pointer_t<DependentPtr>& operator*() const {
    RIEGELI_ASSERT(ptr_ != nullptr)
        << "Failed precondition of AnyDependency::operator*: null pointer";
    return *ptr_;
  }
  template <typename DependentPtr = Ptr,
            std::enable_if_t<std::is_pointer<DependentPtr>::value, int> = 0>
  Ptr operator->() const {
    RIEGELI_ASSERT(ptr_ != nullptr)
        << "Failed precondition of AnyDependency::operator->: null pointer";
    return ptr_;
  }

  // If `Ptr` is `P*`, `AnyDependencyImpl<P*>` can be compared against
  // `nullptr`.
  template <typename DependentPtr = Ptr,
            std::enable_if_t<std::is_pointer<DependentPtr>::value, int> = 0>
  friend bool operator==(const AnyDependencyImpl& a, std::nullptr_t) {
    return a.get() == nullptr;
  }

  // If `true`, the `AnyDependencyImpl` owns the dependent object, i.e. closing
  // the host object should close the dependent object.
  bool is_owning() const { return methods_->is_owning(repr_.storage); }

  // If `true`, `get()` stays unchanged when an `AnyDependencyImpl` is moved.
  static constexpr bool kIsStable = inline_size == 0;

  // If the `Manager` has exactly this type or a reference to it, returns a
  // pointer to the `Manager`. If the `Manager` is an `AnyDependency` (possibly
  // wrapped in an rvalue reference or `std::unique_ptr`), propagates `GetIf()`
  // to it. Otherwise returns `nullptr`.
  template <typename Manager,
            std::enable_if_t<IsValidDependency<Ptr, Manager>::value, int> = 0>
  Manager* GetIf();
  template <typename Manager,
            std::enable_if_t<IsValidDependency<Ptr, Manager>::value, int> = 0>
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
  template <typename OtherPtr, size_t other_inline_size,
            size_t other_inline_align>
  friend class AnyDependencyImpl;

  using Repr = any_dependency_internal::Repr<Ptr, inline_size, inline_align>;
  using Methods = any_dependency_internal::Methods<Ptr>;
  using NullMethods = any_dependency_internal::NullMethods<Ptr>;
  template <typename Manager>
  using MethodsFor = any_dependency_internal::MethodsFor<Ptr, inline_size,
                                                         inline_align, Manager>;

  // Initializes `methods_`, `repr_`, and `ptr_`, avoiding a redundant
  // indirection and adopting them from `manager` instead if `Manager` is
  // already a compatible `AnyDependencyImpl` or `AnyDependencyRefImpl`.
  template <typename Manager,
            std::enable_if_t<
                absl::conjunction<
                    absl::negation<std::is_rvalue_reference<Manager>>,
                    absl::negation<any_dependency_internal::IsAnyDependency<
                        Ptr, Manager>>>::value,
                int> = 0>
  void Initialize(const Manager& manager);
  template <typename Manager,
            std::enable_if_t<
                absl::conjunction<
                    absl::negation<std::is_lvalue_reference<Manager>>,
                    absl::negation<any_dependency_internal::IsAnyDependency<
                        Ptr, Manager>>>::value,
                int> = 0>
  void Initialize(Manager&& manager);
  template <typename Manager,
            std::enable_if_t<
                any_dependency_internal::IsAnyDependency<Ptr, Manager>::value,
                int> = 0>
  void Initialize(Manager&& manager);
  template <typename Manager, typename... ManagerArgs,
            std::enable_if_t<!std::is_reference<Manager>::value, int> = 0>
  void Initialize(ManagerArgs&&... manager_args);

  const Methods* methods_;
  // The union disables implicit construction and destruction which is done
  // manually here.
  union {
    Ptr ptr_;
  };
  Repr repr_;
};

// Specialization of `DependencyImpl<Ptr, AnyDependencyImpl<P>>` when `P` is
// convertible to `Ptr`.
template <typename Ptr, typename P, size_t inline_size, size_t inline_align>
class DependencyImpl<Ptr, AnyDependencyImpl<P, inline_size, inline_align>,
                     std::enable_if_t<std::is_convertible<P, Ptr>::value>>
    : public DependencyBase<AnyDependencyImpl<P, inline_size, inline_align>> {
 public:
  using DependencyImpl::DependencyBase::DependencyBase;

  P get() const { return this->manager().get(); }

  bool is_owning() const { return this->manager().is_owning(); }

  static constexpr bool kIsStable =
      AnyDependencyImpl<Ptr, inline_size, inline_align>::kIsStable;

  void* GetIf(TypeId type_id) { return this->manager().GetIf(type_id); }
  const void* GetIf(TypeId type_id) const {
    return this->manager().GetIf(type_id);
  }

 protected:
  DependencyImpl(DependencyImpl&& that) = default;
  DependencyImpl& operator=(DependencyImpl&& that) = default;

  ~DependencyImpl() = default;
};

// Specialization of `DependencyImpl<Ptr, AnyDependencyImpl<P>&&>` when `P` is
// convertible to `Ptr`.
//
// It is defined explicitly because `AnyDependencyImpl<P>` can be heavy and is
// better kept by reference.
template <typename Ptr, typename P, size_t inline_size, size_t inline_align>
class DependencyImpl<Ptr, AnyDependencyImpl<P, inline_size, inline_align>&&,
                     std::enable_if_t<std::is_convertible<P, Ptr>::value>>
    : public DependencyBase<AnyDependencyImpl<P, inline_size, inline_align>&&> {
 public:
  using DependencyImpl::DependencyBase::DependencyBase;

  P get() const { return this->manager().get(); }

  bool is_owning() const { return this->manager().is_owning(); }

  static constexpr bool kIsStable = true;

  void* GetIf(TypeId type_id) { return this->manager().GetIf(type_id); }
  const void* GetIf(TypeId type_id) const {
    return this->manager().GetIf(type_id);
  }

 protected:
  DependencyImpl(const DependencyImpl& that) = default;
  DependencyImpl& operator=(const DependencyImpl& that) = delete;

  ~DependencyImpl() = default;
};

// Specialization of
// `DependencyImpl<Ptr, std::unique_ptr<AnyDependencyImpl<P>, Deleter>>` when
// `P` is convertible to `Ptr`.
//
// It covers `ClosingPtrType<AnyDependency>`.
template <typename Ptr, typename P, size_t inline_size, size_t inline_align,
          typename Deleter>
class DependencyImpl<
    Ptr,
    std::unique_ptr<AnyDependencyImpl<P, inline_size, inline_align>, Deleter>,
    std::enable_if_t<std::is_convertible<P, Ptr>::value>>
    : public DependencyBase<std::unique_ptr<
          AnyDependencyImpl<P, inline_size, inline_align>, Deleter>> {
 public:
  using DependencyImpl::DependencyBase::DependencyBase;

  P get() const { return this->manager()->get(); }

  bool is_owning() const {
    return this->manager() != nullptr && this->manager()->is_owning();
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
  DependencyImpl(DependencyImpl&& that) = default;
  DependencyImpl& operator=(DependencyImpl&& that) = default;

  ~DependencyImpl() = default;
};

// `AnyDependencyRefImpl` implements `AnyDependencyRef` after `InlineManagers`
// have been reduced to their maximum size and alignment.
template <typename Ptr, size_t inline_size, size_t inline_align = 0>
class AnyDependencyRefImpl
    : public AnyDependencyImpl<Ptr, inline_size, inline_align> {
 public:
  // Creates an empty `AnyDependencyRefImpl`.
  AnyDependencyRefImpl() = default;

  // Holds a `Dependency<Ptr, Manager&&>` (which collapses to
  // `Dependency<Ptr, Manager&>` if `Manager` is itself an lvalue reference).
  //
  // The `Manager` type is deduced from the constructor argument.
  template <
      typename Manager,
      std::enable_if_t<
          absl::conjunction<absl::negation<std::is_same<std::decay_t<Manager>,
                                                        AnyDependencyRefImpl>>,
                            IsValidDependency<Ptr, Manager&&>>::value,
          int> = 0>
  /*implicit*/ AnyDependencyRefImpl(Manager&& manager)
      : AnyDependencyRefImpl::AnyDependencyImpl(
            absl::in_place_type<Manager&&>, std::forward<Manager>(manager)) {}
  template <
      typename Manager,
      std::enable_if_t<
          absl::conjunction<absl::negation<std::is_same<std::decay_t<Manager>,
                                                        AnyDependencyRefImpl>>,
                            IsValidDependency<Ptr, Manager&&>>::value,
          int> = 0>
  AnyDependencyRefImpl& operator=(Manager&& manager) {
    AnyDependencyRefImpl::AnyDependencyImpl::Reset(
        absl::in_place_type<Manager&&>, std::forward<Manager>(manager));
    return *this;
  }

  // Holds a `Dependency<Ptr, Manager>`.
  //
  // The `Manager` type is specified with a tag (`absl::in_place_type<Manager>`)
  // because constructor templates do not support specifying template arguments
  // explicitly. The `Manager` is constructed from `manager_args`.
  template <typename Manager, typename... ManagerArgs,
            std::enable_if_t<IsValidDependency<Ptr, Manager>::value, int> = 0>
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
      std::enable_if_t<
          IsValidDependency<Ptr, DeduceClassTemplateArgumentsT<
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
  template <typename Manager,
            std::enable_if_t<IsValidDependency<Ptr, Manager&&>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Manager&& manager) {
    AnyDependencyRefImpl::AnyDependencyImpl::Reset(
        absl::in_place_type<Manager&&>, std::forward<Manager>(manager));
  }
  template <typename Manager, typename... ManagerArgs,
            std::enable_if_t<IsValidDependency<Ptr, Manager>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(absl::in_place_type_t<Manager>,
                                          ManagerArgs&&... manager_args) {
    AnyDependencyRefImpl::AnyDependencyImpl::Reset(
        absl::in_place_type<Manager>,
        std::forward<ManagerArgs>(manager_args)...);
  }
#if __cpp_deduction_guides
  template <
      template <typename...> class ManagerTemplate, typename... ManagerArgs,
      std::enable_if_t<
          IsValidDependency<Ptr, DeduceClassTemplateArgumentsT<
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

// Specialization of `DependencyImpl<Ptr, AnyDependencyRefImpl<P>>` when `P` is
// convertible to `Ptr`.
template <typename Ptr, typename P, size_t inline_size, size_t inline_align>
class DependencyImpl<Ptr, AnyDependencyRefImpl<P, inline_size, inline_align>,
                     std::enable_if_t<std::is_convertible<P, Ptr>::value>>
    : public DependencyBase<
          AnyDependencyRefImpl<P, inline_size, inline_align>> {
 public:
  using DependencyImpl::DependencyBase::DependencyBase;

  P get() const { return this->manager().get(); }

  bool is_owning() const { return this->manager().is_owning(); }

  static constexpr bool kIsStable =
      AnyDependencyRefImpl<Ptr, inline_size, inline_align>::kIsStable;

  void* GetIf(TypeId type_id) { return this->manager().GetIf(type_id); }
  const void* GetIf(TypeId type_id) const {
    return this->manager().GetIf(type_id);
  }

 protected:
  DependencyImpl(DependencyImpl&& that) = default;
  DependencyImpl& operator=(DependencyImpl&& that) = default;

  ~DependencyImpl() = default;
};

// Specialization of `DependencyImpl<Ptr, AnyDependencyRefImpl<P>&&>` when `P`
// is convertible to `Ptr`.
//
// It is defined explicitly because `AnyDependencyRefImpl<P>` can be heavy and
// is better kept by reference.
template <typename Ptr, typename P, size_t inline_size, size_t inline_align>
class DependencyImpl<Ptr, AnyDependencyRefImpl<P, inline_size, inline_align>&&,
                     std::enable_if_t<std::is_convertible<P, Ptr>::value>>
    : public DependencyBase<
          AnyDependencyRefImpl<P, inline_size, inline_align>&&> {
 public:
  using DependencyImpl::DependencyBase::DependencyBase;

  P get() const { return this->manager().get(); }

  bool is_owning() const { return this->manager().is_owning(); }

  static constexpr bool kIsStable = true;

  void* GetIf(TypeId type_id) { return this->manager().GetIf(type_id); }
  const void* GetIf(TypeId type_id) const {
    return this->manager().GetIf(type_id);
  }

 protected:
  DependencyImpl(const DependencyImpl& that) = default;
  DependencyImpl& operator=(const DependencyImpl& that) = delete;

  ~DependencyImpl() = default;
};

// Specialization of
// `DependencyImpl<Ptr, std::unique_ptr<AnyDependencyRefImpl<P>, Deleter>>` when
// `P` is convertible to `Ptr`.
//
// It covers `ClosingPtrType<AnyDependencyRef>`.
template <typename Ptr, typename P, size_t inline_size, size_t inline_align,
          typename Deleter>
class DependencyImpl<
    Ptr,
    std::unique_ptr<AnyDependencyRefImpl<P, inline_size, inline_align>,
                    Deleter>,
    std::enable_if_t<std::is_convertible<P, Ptr>::value>>
    : public DependencyBase<std::unique_ptr<
          AnyDependencyRefImpl<P, inline_size, inline_align>, Deleter>> {
 public:
  using DependencyImpl::DependencyBase::DependencyBase;

  P get() const { return this->manager()->get(); }

  bool is_owning() const {
    return this->manager() != nullptr && this->manager()->is_owning();
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
  DependencyImpl(DependencyImpl&& that) = default;
  DependencyImpl& operator=(DependencyImpl&& that) = default;

  ~DependencyImpl() = default;
};

// Implementation details follow.

namespace any_dependency_internal {

// `any_dependency_internal::SentinelPtr<Ptr>()` returns a sentinel `Ptr`
// constructed from `RiegeliDependencySentinel(static_cast<Ptr*>(nullptr))`.

template <typename Ptr>
inline Ptr SentinelPtrInternal(const Ptr& ptr) {
  return ptr;
}

template <typename Ptr>
inline Ptr SentinelPtrInternal(Ptr&& ptr) {
  // `std::move(ptr)` is correct and `std::forward<Ptr>(ptr)` is not necessary:
  // `Ptr` is always specified explicitly and is never an lvalue reference.
  return std::move(ptr);
}

#if !__cpp_lib_make_from_tuple
template <typename Ptr, typename... PtrArgs, size_t... indices>
inline Ptr SentinelPtrInternal(
    ABSL_ATTRIBUTE_UNUSED std::tuple<PtrArgs...>&& ptr_args,
    std::index_sequence<indices...>) {
  return Ptr(std::forward<PtrArgs>(std::get<indices>(ptr_args))...);
}
#endif

template <typename Ptr, typename... PtrArgs>
inline Ptr SentinelPtrInternal(std::tuple<PtrArgs...> ptr_args) {
#if __cpp_lib_make_from_tuple
  return std::make_from_tuple<Ptr>(std::move(ptr_args));
#else
  return SentinelPtrInternal<Ptr>(std::move(ptr_args),
                                  std::index_sequence_for<PtrArgs...>());
#endif
}

template <typename Ptr>
inline Ptr SentinelPtr() {
  return SentinelPtrInternal<Ptr>(
      RiegeliDependencySentinel(static_cast<Ptr*>(nullptr)));
}

// `any_dependency_internal::IsOwning(dep)` calls `dep.is_owning()` if that is
// defined, otherwise returns `false`.

template <typename T, typename Enable = void>
struct HasIsOwning : std::false_type {};
template <typename T>
struct HasIsOwning<T,
                   absl::void_t<decltype(std::declval<const T>().is_owning())>>
    : std::true_type {};

template <
    typename Ptr, typename Manager,
    std::enable_if_t<HasIsOwning<Dependency<Ptr, Manager>>::value, int> = 0>
bool IsOwning(const Dependency<Ptr, Manager>& dep) {
  return dep.is_owning();
}
template <
    typename Ptr, typename Manager,
    std::enable_if_t<!HasIsOwning<Dependency<Ptr, Manager>>::value, int> = 0>
bool IsOwning(ABSL_ATTRIBUTE_UNUSED const Dependency<Ptr, Manager>& dep) {
  return false;
}

template <typename Ptr>
struct NullMethods {
  static const Methods<Ptr> methods;

 private:
  static void Destroy(ABSL_ATTRIBUTE_UNUSED Storage self) {}
  static void Move(ABSL_ATTRIBUTE_UNUSED Storage self, Ptr* self_ptr,
                   ABSL_ATTRIBUTE_UNUSED Storage that) {
    new (self_ptr) Ptr(SentinelPtr<Ptr>());
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
};

template <typename Ptr>
const Methods<Ptr> NullMethods<Ptr>::methods = {
    Destroy,           0, 0, Move, IsOwning, MutableGetIf, ConstGetIf,
    RegisterSubobjects};

template <typename Ptr, size_t inline_size, size_t inline_align,
          typename Manager, typename Enable>
struct MethodsFor {
  static const Methods<Ptr> methods;

  template <typename DependentManager = Manager,
            std::enable_if_t<!std::is_rvalue_reference<DependentManager>::value,
                             int> = 0>
  static void Construct(Storage self, Ptr* self_ptr, const Manager& manager) {
    new (self) Dependency<Ptr, Manager>*(new Dependency<Ptr, Manager>(manager));
    new (self_ptr) Ptr(dep_ptr(self)->get());
  }
  template <typename DependentManager = Manager,
            std::enable_if_t<!std::is_lvalue_reference<DependentManager>::value,
                             int> = 0>
  static void Construct(Storage self, Ptr* self_ptr, Manager&& manager) {
    new (self) Dependency<Ptr, Manager>*(
        new Dependency<Ptr, Manager>(std::move(manager)));
    new (self_ptr) Ptr(dep_ptr(self)->get());
  }
  template <
      typename... ManagerArgs, typename DependentManager = Manager,
      std::enable_if_t<!std::is_reference<DependentManager>::value, int> = 0>
  static void Construct(Storage self, Ptr* self_ptr,
                        std::tuple<ManagerArgs...> manager_args) {
    new (self) Dependency<Ptr, Manager>*(
        new Dependency<Ptr, Manager>(std::move(manager_args)));
    new (self_ptr) Ptr(dep_ptr(self)->get());
  }

  static Manager& GetManager(Storage self) { return dep_ptr(self)->manager(); }

 private:
  static Dependency<Ptr, Manager>* dep_ptr(const Storage self) {
    return *
#if __cpp_lib_launder >= 201606
        std::launder
#endif
        (reinterpret_cast<Dependency<Ptr, Manager>* const*>(self));
  }

  static void Destroy(Storage self) { delete dep_ptr(self); }
  static void Move(Storage self, Ptr* self_ptr, Storage that) {
    new (self) Dependency<Ptr, Manager>*(dep_ptr(that));
    new (self_ptr) Ptr(dep_ptr(self)->get());
  }
  static bool IsOwning(const Storage self) {
    return any_dependency_internal::IsOwning(*dep_ptr(self));
  }
  static void* MutableGetIf(Storage self, TypeId type_id) {
    return dep_ptr(self)->GetIf(type_id);
  }
  static const void* ConstGetIf(const Storage self, TypeId type_id) {
    return absl::implicit_cast<const Dependency<Ptr, Manager>*>(dep_ptr(self))
        ->GetIf(type_id);
  }
  static void RegisterSubobjects(const Storage self,
                                 MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterDynamicObject(*dep_ptr(self));
  }
};

template <typename Ptr, size_t inline_size, size_t inline_align,
          typename Manager, typename Enable>
const Methods<Ptr>
    MethodsFor<Ptr, inline_size, inline_align, Manager, Enable>::methods = {
        Destroy,           0, 0, Move, IsOwning, MutableGetIf, ConstGetIf,
        RegisterSubobjects};

template <typename Ptr, size_t inline_size, size_t inline_align,
          typename Manager>
struct MethodsFor<Ptr, inline_size, inline_align, Manager,
                  std::enable_if_t<IsInline<Ptr, inline_size, inline_align,
                                            Manager>::value>> {
  static const Methods<Ptr> methods;

  template <typename DependentManager = Manager,
            std::enable_if_t<!std::is_rvalue_reference<DependentManager>::value,
                             int> = 0>
  static void Construct(Storage self, Ptr* self_ptr, const Manager& manager) {
    new (self) Dependency<Ptr, Manager>(manager);
    new (self_ptr) Ptr(dep(self).get());
  }
  template <typename DependentManager = Manager,
            std::enable_if_t<!std::is_lvalue_reference<DependentManager>::value,
                             int> = 0>
  static void Construct(Storage self, Ptr* self_ptr, Manager&& manager) {
    new (self) Dependency<Ptr, Manager>(std::move(manager));
    new (self_ptr) Ptr(dep(self).get());
  }
  template <
      typename... ManagerArgs, typename DependentManager = Manager,
      std::enable_if_t<!std::is_reference<DependentManager>::value, int> = 0>
  static void Construct(Storage self, Ptr* self_ptr,
                        std::tuple<ManagerArgs...> manager_args) {
    new (self) Dependency<Ptr, Manager>(std::move(manager_args));
    new (self_ptr) Ptr(dep(self).get());
  }

  static Manager& GetManager(Storage self) { return dep(self).manager(); }

 private:
  static Dependency<Ptr, Manager>& dep(Storage self) {
    return *
#if __cpp_lib_launder >= 201606
        std::launder
#endif
        (reinterpret_cast<Dependency<Ptr, Manager>*>(self));
  }
  static const Dependency<Ptr, Manager>& dep(const Storage self) {
    return *
#if __cpp_lib_launder >= 201606
        std::launder
#endif
        (reinterpret_cast<const Dependency<Ptr, Manager>*>(self));
  }

  static void Destroy(Storage self) { dep(self).~Dependency<Ptr, Manager>(); }
  static void Move(Storage self, Ptr* self_ptr, Storage that) {
    new (self) Dependency<Ptr, Manager>(std::move(dep(that)));
    dep(that).~Dependency<Ptr, Manager>();
    new (self_ptr) Ptr(dep(self).get());
  }
  static bool IsOwning(const Storage self) {
    return any_dependency_internal::IsOwning(dep(self));
  }
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
};

template <typename Ptr, size_t inline_size, size_t inline_align,
          typename Manager>
const Methods<Ptr>
    MethodsFor<Ptr, inline_size, inline_align, Manager,
               std::enable_if_t<IsInline<Ptr, inline_size, inline_align,
                                         Manager>::value>>::methods = {
        Destroy,
        sizeof(Dependency<Ptr, Manager>),
        alignof(Dependency<Ptr, Manager>),
        Move,
        IsOwning,
        MutableGetIf,
        ConstGetIf,
        RegisterSubobjects};

}  // namespace any_dependency_internal

template <typename Ptr, size_t inline_size, size_t inline_align>
inline AnyDependencyImpl<Ptr, inline_size,
                         inline_align>::AnyDependencyImpl() noexcept
    : methods_(&NullMethods::methods),
      ptr_(any_dependency_internal::SentinelPtr<Ptr>()) {}

template <typename Ptr, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<
              absl::conjunction<
                  absl::negation<std::is_same<
                      std::decay_t<Manager>,
                      AnyDependencyImpl<Ptr, inline_size, inline_align>>>,
                  IsValidDependency<Ptr, std::decay_t<Manager>>,
                  std::is_convertible<Manager&&, std::decay_t<Manager>>>::value,
              int>>
inline AnyDependencyImpl<Ptr, inline_size, inline_align>::AnyDependencyImpl(
    Manager&& manager) {
  Initialize<std::decay_t<Manager>>(std::forward<Manager>(manager));
}

template <typename Ptr, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<
              absl::conjunction<
                  absl::negation<std::is_same<
                      std::decay_t<Manager>,
                      AnyDependencyImpl<Ptr, inline_size, inline_align>>>,
                  IsValidDependency<Ptr, std::decay_t<Manager>>,
                  std::is_convertible<Manager&&, std::decay_t<Manager>>>::value,
              int>>
inline AnyDependencyImpl<Ptr, inline_size, inline_align>&
AnyDependencyImpl<Ptr, inline_size, inline_align>::operator=(
    Manager&& manager) {
  ptr_.~Ptr();
  methods_->destroy(repr_.storage);
  Initialize<std::decay_t<Manager>>(std::forward<Manager>(manager));
  return *this;
}

template <typename Ptr, size_t inline_size, size_t inline_align>
template <typename Manager, typename... ManagerArgs,
          std::enable_if_t<IsValidDependency<Ptr, Manager>::value, int>>
inline AnyDependencyImpl<Ptr, inline_size, inline_align>::AnyDependencyImpl(
    absl::in_place_type_t<Manager>, ManagerArgs&&... manager_args) {
  Initialize<Manager>(std::forward<ManagerArgs>(manager_args)...);
}

#if __cpp_deduction_guides
template <typename Ptr, size_t inline_size, size_t inline_align>
template <
    template <typename...> class ManagerTemplate, typename... ManagerArgs,
    std::enable_if_t<
        IsValidDependency<Ptr, DeduceClassTemplateArgumentsT<
                                   ManagerTemplate, ManagerArgs...>>::value,
        int>>
inline AnyDependencyImpl<Ptr, inline_size, inline_align>::AnyDependencyImpl(
    in_place_template_t<ManagerTemplate>, ManagerArgs&&... manager_args)
    : AnyDependencyImpl(
          absl::in_place_type<
              DeduceClassTemplateArgumentsT<ManagerTemplate, ManagerArgs...>>,
          std::forward<ManagerArgs>(manager_args)...) {}
#endif

template <typename Ptr, size_t inline_size, size_t inline_align>
inline AnyDependencyImpl<Ptr, inline_size, inline_align>::AnyDependencyImpl(
    AnyDependencyImpl&& that) noexcept {
  that.ptr_ = any_dependency_internal::SentinelPtr<Ptr>();
  methods_ = std::exchange(that.methods_, &NullMethods::methods);
  methods_->move(repr_.storage, &ptr_, that.repr_.storage);
}

template <typename Ptr, size_t inline_size, size_t inline_align>
inline AnyDependencyImpl<Ptr, inline_size, inline_align>&
AnyDependencyImpl<Ptr, inline_size, inline_align>::operator=(
    AnyDependencyImpl&& that) noexcept {
  if (ABSL_PREDICT_TRUE(&that != this)) {
    ptr_.~Ptr();
    methods_->destroy(repr_.storage);
    that.ptr_ = any_dependency_internal::SentinelPtr<Ptr>();
    methods_ = std::exchange(that.methods_, &NullMethods::methods);
    methods_->move(repr_.storage, &ptr_, that.repr_.storage);
  }
  return *this;
}

template <typename Ptr, size_t inline_size, size_t inline_align>
inline AnyDependencyImpl<Ptr, inline_size, inline_align>::~AnyDependencyImpl() {
  ptr_.~Ptr();
  methods_->destroy(repr_.storage);
}

template <typename Ptr, size_t inline_size, size_t inline_align>
inline void AnyDependencyImpl<Ptr, inline_size, inline_align>::Reset() {
  ptr_ = any_dependency_internal::SentinelPtr<Ptr>();
  methods_->destroy(repr_.storage);
  methods_ = &NullMethods::methods;
}

template <typename Ptr, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<
              absl::conjunction<
                  IsValidDependency<Ptr, std::decay_t<Manager>>,
                  std::is_convertible<Manager&&, std::decay_t<Manager>>>::value,
              int>>
inline void AnyDependencyImpl<Ptr, inline_size, inline_align>::Reset(
    Manager&& manager) {
  ptr_.~Ptr();
  methods_->destroy(repr_.storage);
  Initialize<std::decay_t<Manager>>(std::forward<Manager>(manager));
}

template <typename Ptr, size_t inline_size, size_t inline_align>
template <typename Manager, typename... ManagerArgs,
          std::enable_if_t<IsValidDependency<Ptr, Manager>::value, int>>
inline void AnyDependencyImpl<Ptr, inline_size, inline_align>::Reset(
    absl::in_place_type_t<Manager>, ManagerArgs&&... manager_args) {
  ptr_.~Ptr();
  methods_->destroy(repr_.storage);
  Initialize<Manager>(std::forward<ManagerArgs>(manager_args)...);
}

#if __cpp_deduction_guides
template <typename Ptr, size_t inline_size, size_t inline_align>
template <
    template <typename...> class ManagerTemplate, typename... ManagerArgs,
    std::enable_if_t<
        IsValidDependency<Ptr, DeduceClassTemplateArgumentsT<
                                   ManagerTemplate, ManagerArgs...>>::value,
        int>>
inline void AnyDependencyImpl<Ptr, inline_size, inline_align>::Reset(
    in_place_template_t<ManagerTemplate>, ManagerArgs&&... manager_args) {
  Reset(absl::in_place_type<
            DeduceClassTemplateArgumentsT<ManagerTemplate, ManagerArgs...>>,
        std::forward<ManagerArgs>(manager_args)...);
}
#endif

template <typename Ptr, size_t inline_size, size_t inline_align>
template <typename Manager, typename... ManagerArgs,
          std::enable_if_t<IsValidDependency<Ptr, Manager>::value, int>>
inline Manager& AnyDependencyImpl<Ptr, inline_size, inline_align>::Emplace(
    ManagerArgs&&... manager_args) {
  Reset(absl::in_place_type<Manager>,
        std::forward<ManagerArgs>(manager_args)...);
  return MethodsFor<Manager>::GetManager(repr_.storage);
}

#if __cpp_deduction_guides
template <typename Ptr, size_t inline_size, size_t inline_align>
template <
    template <typename...> class ManagerTemplate, typename... ManagerArgs,
    std::enable_if_t<
        IsValidDependency<Ptr, DeduceClassTemplateArgumentsT<
                                   ManagerTemplate, ManagerArgs...>>::value,
        int>>
inline DeduceClassTemplateArgumentsT<ManagerTemplate, ManagerArgs...>&
AnyDependencyImpl<Ptr, inline_size, inline_align>::Emplace(
    ManagerArgs&&... manager_args) {
  return Emplace<
      DeduceClassTemplateArgumentsT<ManagerTemplate, ManagerArgs...>>(
      std::forward<ManagerArgs>(manager_args)...);
}
#endif

template <typename Ptr, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<
              absl::conjunction<
                  absl::negation<std::is_rvalue_reference<Manager>>,
                  absl::negation<any_dependency_internal::IsAnyDependency<
                      Ptr, Manager>>>::value,
              int>>
inline void AnyDependencyImpl<Ptr, inline_size, inline_align>::Initialize(
    const Manager& manager) {
  methods_ = &MethodsFor<Manager>::methods;
  MethodsFor<Manager>::Construct(repr_.storage, &ptr_, manager);
}

template <typename Ptr, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<
              absl::conjunction<
                  absl::negation<std::is_lvalue_reference<Manager>>,
                  absl::negation<any_dependency_internal::IsAnyDependency<
                      Ptr, Manager>>>::value,
              int>>
inline void AnyDependencyImpl<Ptr, inline_size, inline_align>::Initialize(
    Manager&& manager) {
  methods_ = &MethodsFor<Manager>::methods;
  // `std::move(manager)` is correct and `std::forward<Manager>(manager)` is not
  // necessary: `Manager` is never an lvalue reference because this is excluded
  // in the constraint.
  MethodsFor<Manager>::Construct(repr_.storage, &ptr_, std::move(manager));
}

template <typename Ptr, size_t inline_size, size_t inline_align>
template <
    typename Manager,
    std::enable_if_t<
        any_dependency_internal::IsAnyDependency<Ptr, Manager>::value, int>>
inline void AnyDependencyImpl<Ptr, inline_size, inline_align>::Initialize(
    Manager&& manager) {
  if ((sizeof(typename Manager::Repr) <= sizeof(Repr) ||
       manager.methods_->inline_size_used <= sizeof(Repr)) &&
      (alignof(typename Manager::Repr) <= alignof(Repr) ||
       manager.methods_->inline_align_used <= alignof(Repr))) {
    // Adopt `manager` instead of wrapping it.
    manager.ptr_ = any_dependency_internal::SentinelPtr<Ptr>();
    methods_ = std::exchange(manager.methods_, &NullMethods::methods);
    methods_->move(repr_.storage, &ptr_, manager.repr_.storage);
    return;
  }
  methods_ = &MethodsFor<Manager>::methods;
  MethodsFor<Manager>::Construct(repr_.storage, &ptr_,
                                 std::forward<Manager>(manager));
}

template <typename Ptr, size_t inline_size, size_t inline_align>
template <typename Manager, typename... ManagerArgs,
          std::enable_if_t<!std::is_reference<Manager>::value, int>>
inline void AnyDependencyImpl<Ptr, inline_size, inline_align>::Initialize(
    ManagerArgs&&... manager_args) {
  methods_ = &MethodsFor<Manager>::methods;
  MethodsFor<Manager>::Construct(
      repr_.storage, &ptr_,
      std::forward_as_tuple(std::forward<ManagerArgs>(manager_args)...));
}

template <typename Ptr, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<IsValidDependency<Ptr, Manager>::value, int>>
Manager* AnyDependencyImpl<Ptr, inline_size, inline_align>::GetIf() {
  return static_cast<Manager*>(GetIf(TypeId::For<Manager>()));
}

template <typename Ptr, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<IsValidDependency<Ptr, Manager>::value, int>>
const Manager* AnyDependencyImpl<Ptr, inline_size, inline_align>::GetIf()
    const {
  return static_cast<const Manager*>(GetIf(TypeId::For<Manager>()));
}

template <typename Ptr, size_t inline_size, size_t inline_align>
inline void* AnyDependencyImpl<Ptr, inline_size, inline_align>::GetIf(
    TypeId type_id) {
  return methods_->mutable_get_if(repr_.storage, type_id);
}

template <typename Ptr, size_t inline_size, size_t inline_align>
inline const void* AnyDependencyImpl<Ptr, inline_size, inline_align>::GetIf(
    TypeId type_id) const {
  return methods_->const_get_if(repr_.storage, type_id);
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_ANY_DEPENDENCY_H_
