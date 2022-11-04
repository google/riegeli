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

#include <functional>
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
#include "riegeli/base/dependency.h"
#include "riegeli/base/type_id.h"

namespace riegeli {

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
// By default inline storage is enough for a pointer or a `Ptr`.
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
      alignof(void*), alignof(Ptr),
      inline_align)) char storage[UnsignedMax(sizeof(void*), sizeof(Ptr),
                                              inline_size)];
};

// By convention, a parameter of type `Storage` points to
// `Repr<Ptr, inline_size, inline_align>::storage`.
using Storage = char[];

// A `Dependency<Ptr, Manager>` is stored inline in
// `Repr<Ptr, inline_size, inline_align>` if it fits in that storage and is
// movable. If `inline_size == 0`, the dependency is also required to be stable.

template <typename Ptr, size_t inline_size, size_t inline_align,
          typename Manager, typename Enable = void>
struct IsInline : std::false_type {};

template <typename Ptr, size_t inline_size, size_t inline_align,
          typename Manager>
struct IsInline<
    Ptr, inline_size, inline_align, Manager,
    std::enable_if_t<
        sizeof(Dependency<Ptr, Manager>) <=
            sizeof(Repr<Ptr, inline_size, inline_align>) &&
        alignof(Dependency<Ptr, Manager>) <=
            alignof(Repr<Ptr, inline_size, inline_align>) &&
        (inline_size > 0 || Dependency<Ptr, Manager>::kIsStable) &&
        std::is_move_constructible<Dependency<Ptr, Manager>>::value>>
    : std::true_type {};

// Method pointers.
template <typename Ptr>
struct Methods {
  // Constructs `self` and `*self_ptr` by moving from `that`, and destroys
  // `that`.
  void (*move)(Storage self, Ptr* self_ptr, Storage that);
  // Destroys `self`.
  void (*destroy)(Storage self);
  Ptr (*release)(Storage self);
  bool (*is_owning)(const Storage self);
  // Returns the `const std::remove_reference_t<Manager>*` if `type_id` matches
  // `std::remove_reference_t<Manager>`, otherwise returns `nullptr`.
  const void* (*get_if)(const Storage self, TypeId type_id);
  size_t inline_size_used;   // Or 0 if inline storage is not used.
  size_t inline_align_used;  // Or 0 if inline storage is not used.
};

template <typename Ptr>
struct NullMethods;

template <typename Ptr, size_t inline_size, size_t inline_align,
          typename Manager, typename Enable = void>
struct MethodsFor;

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
template <typename Ptr, size_t inline_size, size_t inline_align = 0>
class AnyDependencyImpl {
 public:
  // Creates an empty `AnyDependencyImpl`.
  AnyDependencyImpl() noexcept;

  // Holds a `Dependency<Ptr, std::decay_t<Manager>>`.
  //
  // The `Manager` type is deduced from the constructor argument.
  template <typename Manager,
            std::enable_if_t<
                absl::conjunction<
                    absl::negation<
                        std::is_same<std::decay_t<Manager>, AnyDependencyImpl>>,
                    IsValidDependency<Ptr, std::decay_t<Manager>>>::value,
                int> = 0>
  /*implicit*/ AnyDependencyImpl(Manager&& manager);
  template <typename Manager,
            std::enable_if_t<
                absl::conjunction<
                    absl::negation<
                        std::is_same<std::decay_t<Manager>, AnyDependencyImpl>>,
                    IsValidDependency<Ptr, std::decay_t<Manager>>>::value,
                int> = 0>
  AnyDependencyImpl& operator=(Manager&& manager);

  // Holds a `Dependency<Ptr, Manager>`.
  //
  // The `Manager` type is specified with a tag (`absl::in_place_type<Manager>`)
  // because constructor templates do not support specifying template arguments
  // explicitly.
  //
  // The dependency is constructed with `std::forward<ManagerArg>(manager_arg)`,
  // which can be a `Manager` to copy or move, or a tuple of its constructor
  // arguments.
  template <typename Manager, typename ManagerArg,
            std::enable_if_t<IsValidDependency<Ptr, Manager>::value, int> = 0>
  explicit AnyDependencyImpl(absl::in_place_type_t<Manager>,
                             ManagerArg&& manager_arg);

  AnyDependencyImpl(AnyDependencyImpl&& that) noexcept;
  AnyDependencyImpl& operator=(AnyDependencyImpl&& that) noexcept;

  ~AnyDependencyImpl();

  // Makes `*this` equivalent to a newly constructed `AnyDependencyImpl`. This
  // avoids constructing a temporary `AnyDependencyImpl` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset();
  template <typename Manager,
            std::enable_if_t<
                IsValidDependency<Ptr, std::decay_t<Manager>>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Manager&& manager);
  template <typename Manager, typename ManagerArg,
            std::enable_if_t<IsValidDependency<Ptr, Manager>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(absl::in_place_type_t<Manager>,
                                          ManagerArg&& manager_arg);

  // Holds a `Dependency<Ptr, Manager>`.
  //
  // The `Manager` is constructed from the given constructor arguments.
  //
  // Same as `Reset(absl::in_place_type<Manager>,
  //                std::forward_as_tuple(manager_args...))`.
  template <typename Manager, typename... ManagerArgs,
            std::enable_if_t<IsValidDependency<Ptr, Manager>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Emplace(ManagerArgs&&... manager_args);

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
  ABSL_ATTRIBUTE_REINITIALIZES void Emplace(ManagerArgs&&... manager_args);
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

  // If the `Dependency` owns the dependent object and can release it,
  // `Release()` returns the released pointer, otherwise returns a sentinel
  // `Ptr` constructed from `DependencySentinel(static_cast<Ptr*>(nullptr))`.
  Ptr Release() { return methods_->release(repr_.storage); }

  // If `Ptr` is `P*`, `AnyDependencyImpl<P*>` can be compared against
  // `nullptr`.
  template <typename DependentPtr = Ptr,
            std::enable_if_t<std::is_pointer<DependentPtr>::value, int> = 0>
  friend bool operator==(const AnyDependencyImpl& a, nullptr_t) {
    return a.get() == nullptr;
  }
  template <typename DependentPtr = Ptr,
            std::enable_if_t<std::is_pointer<DependentPtr>::value, int> = 0>
  friend bool operator!=(const AnyDependencyImpl& a, nullptr_t) {
    return a.get() != nullptr;
  }
  template <typename DependentPtr = Ptr,
            std::enable_if_t<std::is_pointer<DependentPtr>::value, int> = 0>
  friend bool operator==(nullptr_t, const AnyDependencyImpl& b) {
    return nullptr == b.get();
  }
  template <typename DependentPtr = Ptr,
            std::enable_if_t<std::is_pointer<DependentPtr>::value, int> = 0>
  friend bool operator!=(nullptr_t, const AnyDependencyImpl& b) {
    return nullptr != b.get();
  }

  // If `true`, the `AnyDependencyImpl` owns the dependent object, i.e. closing
  // the host object should close the dependent object.
  bool is_owning() const { return methods_->is_owning(repr_.storage); }

  // If `true`, `get()` stays unchanged when an `AnyDependencyImpl` is moved.
  static constexpr bool kIsStable = inline_size == 0;

  // If the contained `Manager` has exactly this type or a reference to it,
  // returns a pointer to the contained `Manager`. Otherwise returns `nullptr`.
  //
  // If an `AnyDependencyImpl` gets moved to an `AnyDependencyImpl` with
  // different template arguments, it is unspecified whether `GetIf()`
  // responds to the original type or to the source `AnyDependencyImpl`.
  template <typename Manager,
            std::enable_if_t<IsValidDependency<Ptr, Manager>::value, int> = 0>
  Manager* GetIf();
  template <typename Manager,
            std::enable_if_t<IsValidDependency<Ptr, Manager>::value, int> = 0>
  const Manager* GetIf() const;

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

#if !__cpp_guaranteed_copy_elision || !__cpp_lib_make_from_tuple
  template <typename... Args, size_t... indices>
  explicit AnyDependencyImpl(std::tuple<Args...>&& args,
                             std::index_sequence<indices...>)
      : AnyDependencyImpl(std::forward<Args>(std::get<indices>(args))...) {}
#endif

  // Initializes `methods_`, `repr_`, and `ptr_`, avoiding a redundant
  // indirection and adopting them from `manager` instead if `Manager` is
  // already a compatible `AnyDependencyImpl` or `AnyDependencyRefImpl`.
  template <typename Manager,
            std::enable_if_t<!std::is_reference<Manager>::value &&
                                 !any_dependency_internal::IsAnyDependency<
                                     Ptr, Manager>::value,
                             int> = 0>
  void Initialize(const Manager& manager);
  template <typename Manager,
            std::enable_if_t<
                !any_dependency_internal::IsAnyDependency<Ptr, Manager>::value,
                int> = 0>
  void Initialize(Manager&& manager);
  template <typename Manager,
            std::enable_if_t<
                any_dependency_internal::IsAnyDependency<Ptr, Manager>::value,
                int> = 0>
  void Initialize(Manager&& manager);
  template <typename Manager, typename... ManagerArgs,
            std::enable_if_t<!std::is_reference<Manager>::value &&
                                 !any_dependency_internal::IsAnyDependency<
                                     Ptr, Manager>::value,
                             int> = 0>
  void Initialize(std::tuple<ManagerArgs...> manager_args);
  template <typename Manager, typename... ManagerArgs,
            std::enable_if_t<
                any_dependency_internal::IsAnyDependency<Ptr, Manager>::value,
                int> = 0>
  void Initialize(std::tuple<ManagerArgs...> manager_args);

  const Methods* methods_;
  // The union disables implicit construction and destruction which is done
  // manually here.
  union {
    Ptr ptr_;
  };
  Repr repr_;
};

// Specialization of `DependencyImpl<Ptr, AnyDependencyImpl<Ptr>>`.
template <typename Ptr, size_t inline_size, size_t inline_align>
class DependencyImpl<Ptr, AnyDependencyImpl<Ptr, inline_size, inline_align>>
    : public DependencyBase<AnyDependencyImpl<Ptr, inline_size, inline_align>> {
 public:
  using DependencyBase<
      AnyDependencyImpl<Ptr, inline_size, inline_align>>::DependencyBase;

  Ptr get() const { return this->manager().get(); }
  Ptr Release() { return this->manager().Release(); }

  bool is_owning() const { return this->manager().is_owning(); }
  static constexpr bool kIsStable =
      AnyDependencyImpl<Ptr, inline_size, inline_align>::kIsStable;
};

// Specialization of `DependencyImpl<Ptr, AnyDependencyImpl<Ptr>&&>`.
//
// It is defined explicitly because `AnyDependencyImpl<Ptr>` can be heavy and is
// better kept by reference.
template <typename Ptr, size_t inline_size, size_t inline_align>
class DependencyImpl<Ptr, AnyDependencyImpl<Ptr, inline_size, inline_align>&&>
    : public DependencyBase<
          AnyDependencyImpl<Ptr, inline_size, inline_align>&&> {
 public:
  using DependencyBase<
      AnyDependencyImpl<Ptr, inline_size, inline_align>&&>::DependencyBase;

  Ptr get() const { return this->manager().get(); }
  Ptr Release() { return this->manager().Release(); }

  bool is_owning() const { return this->manager().is_owning(); }
  static constexpr bool kIsStable = true;
};

// Specialization of
// `DependencyImpl<Ptr, std::reference_wrapper<AnyDependencyImpl<Ptr>>>`.
//
// It is defined explicitly because `AnyDependencyImpl<Ptr>` can be heavy and is
// better kept by reference.
template <typename Ptr, size_t inline_size, size_t inline_align>
class DependencyImpl<
    Ptr,
    std::reference_wrapper<AnyDependencyImpl<Ptr, inline_size, inline_align>>>
    : public DependencyBase<std::reference_wrapper<
          AnyDependencyImpl<Ptr, inline_size, inline_align>>> {
 public:
  using DependencyBase<std::reference_wrapper<
      AnyDependencyImpl<Ptr, inline_size, inline_align>>>::DependencyBase;

  Ptr get() const { return this->manager().get().get(); }
  Ptr Release() { return this->manager().get().Release(); }

  bool is_owning() const { return this->manager().get().is_owning(); }
  static constexpr bool kIsStable = true;
};

// `IsValidDependencyImpl<Ptr, std::reference_wrapper<AnyDependencyImpl<Ptr>>>`
// is specialized explicitly for a subtle reason:
//
// Consider a type `T` like `std::reference_wrapper<AnyDependency<Reader*>>`.
// Checking `IsValidDependency<Reader*, T>` by instantiating
// `DependencyImpl<Reader*, T>` would try to generate the copy constructor of
// `DependencyImpl<Reader*, T>`, which would try to copy `DependencyBase<T>`,
// which would try to copy `T`, which would consider not only its copy
// constructor but also its constructor from a compatible reference type, which
// would try to implicitly convert `const T&` to `AnyDependency<Reader*>`, which
// would check whether `IsValidDependency<Reader*, T>`, which is still in the
// process of being determined.
template <typename Ptr, size_t inline_size, size_t inline_align>
struct IsValidDependencyImpl<
    Ptr,
    std::reference_wrapper<AnyDependencyImpl<Ptr, inline_size, inline_align>>>
    : std::true_type {};

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
      : AnyDependencyImpl<Ptr, inline_size, inline_align>(
            absl::in_place_type<Manager&&>, std::forward<Manager>(manager)) {}
  template <
      typename Manager,
      std::enable_if_t<
          absl::conjunction<absl::negation<std::is_same<std::decay_t<Manager>,
                                                        AnyDependencyRefImpl>>,
                            IsValidDependency<Ptr, Manager&&>>::value,
          int> = 0>
  AnyDependencyRefImpl& operator=(Manager&& manager) {
    AnyDependencyImpl<Ptr, inline_size, inline_align>::Reset(
        absl::in_place_type<Manager&&>, std::forward<Manager>(manager));
    return *this;
  }

  // Holds a `Dependency<Ptr, Manager>`.
  //
  // The `Manager` type is specified with a tag (`absl::in_place_type<Manager>`)
  // because constructor templates do not support specifying template arguments
  // explicitly.
  //
  // The dependency is constructed with `std::forward<ManagerArg>(manager_arg)`,
  // which can be a `Manager` to copy or move, or a tuple of its constructor
  // arguments.
  template <typename Manager, typename ManagerArg,
            std::enable_if_t<IsValidDependency<Ptr, Manager>::value, int> = 0>
  explicit AnyDependencyRefImpl(absl::in_place_type_t<Manager>,
                                ManagerArg&& manager_arg)
      : AnyDependencyImpl<Ptr, inline_size, inline_align>(
            absl::in_place_type<Manager>,
            std::forward<ManagerArg>(manager_arg)) {}

  AnyDependencyRefImpl(AnyDependencyRefImpl&& that) = default;
  AnyDependencyRefImpl& operator=(AnyDependencyRefImpl&& that) = default;

  // Makes `*this` equivalent to a newly constructed `AnyDependencyRefImpl`.
  // This avoids constructing a temporary `AnyDependencyRefImpl` and moving from
  // it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset() {
    AnyDependencyImpl<Ptr, inline_size, inline_align>::Reset();
  }
  template <typename Manager,
            std::enable_if_t<IsValidDependency<Ptr, Manager&&>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Manager&& manager) {
    AnyDependencyImpl<Ptr, inline_size, inline_align>::Reset(
        absl::in_place_type<Manager&&>, std::forward<Manager>(manager));
  }
  template <typename Manager, typename ManagerArg,
            std::enable_if_t<IsValidDependency<Ptr, Manager>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(absl::in_place_type_t<Manager>,
                                          ManagerArg&& manager_arg) {
    AnyDependencyImpl<Ptr, inline_size, inline_align>::Reset(
        absl::in_place_type<Manager>, std::forward<ManagerArg>(manager_arg));
  }
};

// Specialization of `DependencyImpl<Ptr, AnyDependencyRefImpl<Ptr>>`.
template <typename Ptr, size_t inline_size, size_t inline_align>
class DependencyImpl<Ptr, AnyDependencyRefImpl<Ptr, inline_size, inline_align>>
    : public DependencyBase<
          AnyDependencyRefImpl<Ptr, inline_size, inline_align>> {
 public:
  using DependencyBase<
      AnyDependencyRefImpl<Ptr, inline_size, inline_align>>::DependencyBase;

  Ptr get() const { return this->manager().get(); }
  Ptr Release() { return this->manager().Release(); }

  bool is_owning() const { return this->manager().is_owning(); }
  static constexpr bool kIsStable =
      AnyDependencyRefImpl<Ptr, inline_size, inline_align>::kIsStable;
};

// Specialization of `DependencyImpl<Ptr, AnyDependencyRefImpl<Ptr>&&>`.
//
// It is defined explicitly because `AnyDependencyRefImpl<Ptr>` can be heavy and
// is better kept by reference.
template <typename Ptr, size_t inline_size, size_t inline_align>
class DependencyImpl<Ptr,
                     AnyDependencyRefImpl<Ptr, inline_size, inline_align>&&>
    : public DependencyBase<
          AnyDependencyRefImpl<Ptr, inline_size, inline_align>&&> {
 public:
  using DependencyBase<
      AnyDependencyRefImpl<Ptr, inline_size, inline_align>&&>::DependencyBase;

  Ptr get() const { return this->manager().get(); }
  Ptr Release() { return this->manager().Release(); }

  bool is_owning() const { return this->manager().is_owning(); }
  static constexpr bool kIsStable = true;
};

// Specialization of
// `DependencyImpl<Ptr, std::reference_wrapper<AnyDependencyRefImpl<Ptr>>>`.
//
// It is defined explicitly because `AnyDependencyRefImpl<Ptr>` can be heavy and
// is better kept by reference.
template <typename Ptr, size_t inline_size, size_t inline_align>
class DependencyImpl<Ptr, std::reference_wrapper<AnyDependencyRefImpl<
                              Ptr, inline_size, inline_align>>>
    : public DependencyBase<std::reference_wrapper<
          AnyDependencyRefImpl<Ptr, inline_size, inline_align>>> {
 public:
  using DependencyBase<std::reference_wrapper<
      AnyDependencyRefImpl<Ptr, inline_size, inline_align>>>::DependencyBase;

  Ptr get() const { return this->manager().get().get(); }
  Ptr Release() { return this->manager().get().Release(); }

  bool is_owning() const { return this->manager().get().is_owning(); }
  static constexpr bool kIsStable = true;
};

// `IsValidDependencyImpl<
//      Ptr, std::reference_wrapper<AnyDependencyRefImpl<Ptr>>>`
// is specialized explicitly for a subtle reason. See the analogous
// specialization with `AnyDependencyImpl` for details.
template <typename Ptr, size_t inline_size, size_t inline_align>
struct IsValidDependencyImpl<Ptr, std::reference_wrapper<AnyDependencyRefImpl<
                                      Ptr, inline_size, inline_align>>>
    : std::true_type {};

// Implementation details follow.

namespace any_dependency_internal {

// `any_dependency_internal::SentinelPtr<Ptr>()` returns a sentinel `Ptr`
// constructed from `DependencySentinel(static_cast<Ptr*>(nullptr))`.

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
inline Ptr SentinelPtrInternal(std::tuple<PtrArgs...>&& ptr_args,
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
      DependencySentinel(static_cast<Ptr*>(nullptr)));
}

// `any_dependency_internal::Release(dep)` calls `dep.Release()` if that is
// defined, otherwise returns a sentinel `Ptr` constructed from
// `DependencySentinel(static_cast<Ptr*>(nullptr))`.

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
  return SentinelPtr<Ptr>();
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
bool IsOwning(const Dependency<Ptr, Manager>& dep) {
  return false;
}

template <typename Ptr>
struct NullMethods {
  static const Methods<Ptr> methods;

 private:
  static void Move(Storage self, Ptr* self_ptr, Storage that) {
    new (self_ptr) Ptr(SentinelPtr<Ptr>());
  }
  static void Destroy(Storage self) {}
  static Ptr Release(Storage self) { return SentinelPtr<Ptr>(); }
  static bool IsOwning(const Storage self) { return false; }
  static const void* GetIf(const Storage self, TypeId type_id) {
    return nullptr;
  }
};

template <typename Ptr>
const Methods<Ptr> NullMethods<Ptr>::methods = {
    Move, Destroy, Release, IsOwning, GetIf, 0, 0};

template <typename Ptr, size_t inline_size, size_t inline_align,
          typename Manager, typename Enable>
struct MethodsFor {
  static const Methods<Ptr> methods;

  template <
      typename DependentManager = Manager,
      std::enable_if_t<!std::is_reference<DependentManager>::value, int> = 0>
  static void Construct(Storage self, Ptr* self_ptr, const Manager& manager) {
    new (self) Dependency<Ptr, Manager>*(new Dependency<Ptr, Manager>(manager));
    new (self_ptr) Ptr(dep_ptr(self)->get());
  }
  static void Construct(Storage self, Ptr* self_ptr, Manager&& manager) {
    new (self) Dependency<Ptr, Manager>*(
        new Dependency<Ptr, Manager>(std::forward<Manager>(manager)));
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

 private:
  static Dependency<Ptr, Manager>* dep_ptr(const Storage self) {
    return *
#if __cpp_lib_launder >= 201606
        std::launder
#endif
        (reinterpret_cast<Dependency<Ptr, Manager>* const*>(self));
  }

  static void Move(Storage self, Ptr* self_ptr, Storage that) {
    new (self) Dependency<Ptr, Manager>*(dep_ptr(that));
    new (self_ptr) Ptr(dep_ptr(self)->get());
  }
  static void Destroy(Storage self) { delete dep_ptr(self); }
  static Ptr Release(Storage self) {
    return any_dependency_internal::Release(*dep_ptr(self));
  }
  static bool IsOwning(const Storage self) {
    return any_dependency_internal::IsOwning(*dep_ptr(self));
  }
  static const void* GetIf(const Storage self, TypeId type_id) {
    if (type_id == TypeId::For<std::remove_reference_t<Manager>>()) {
      return absl::implicit_cast<const std::remove_reference_t<Manager>*>(
          &dep_ptr(self)->manager());
    }
    return nullptr;
  }
};

template <typename Ptr, size_t inline_size, size_t inline_align,
          typename Manager, typename Enable>
const Methods<Ptr>
    MethodsFor<Ptr, inline_size, inline_align, Manager, Enable>::methods = {
        Move, Destroy, Release, IsOwning, GetIf, 0, 0};

template <typename Ptr, size_t inline_size, size_t inline_align,
          typename Manager>
struct MethodsFor<Ptr, inline_size, inline_align, Manager,
                  std::enable_if_t<IsInline<Ptr, inline_size, inline_align,
                                            Manager>::value>> {
  static const Methods<Ptr> methods;

  template <
      typename DependentManager = Manager,
      std::enable_if_t<!std::is_reference<DependentManager>::value, int> = 0>
  static void Construct(Storage self, Ptr* self_ptr, const Manager& manager) {
    new (self) Dependency<Ptr, Manager>(manager);
    new (self_ptr) Ptr(dep(self).get());
  }
  static void Construct(Storage self, Ptr* self_ptr, Manager&& manager) {
    new (self) Dependency<Ptr, Manager>(std::forward<Manager>(manager));
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

  static void Move(Storage self, Ptr* self_ptr, Storage that) {
    new (self) Dependency<Ptr, Manager>(std::move(dep(that)));
    dep(that).~Dependency<Ptr, Manager>();
    new (self_ptr) Ptr(dep(self).get());
  }
  static void Destroy(Storage self) { dep(self).~Dependency<Ptr, Manager>(); }
  static Ptr Release(Storage self) {
    return any_dependency_internal::Release(dep(self));
  }
  static bool IsOwning(const Storage self) {
    return any_dependency_internal::IsOwning(dep(self));
  }
  static const void* GetIf(const Storage self, TypeId type_id) {
    if (type_id == TypeId::For<std::remove_reference_t<Manager>>()) {
      return absl::implicit_cast<const std::remove_reference_t<Manager>*>(
          &dep(self).manager());
    }
    return nullptr;
  }
};

template <typename Ptr, size_t inline_size, size_t inline_align,
          typename Manager>
const Methods<Ptr>
    MethodsFor<Ptr, inline_size, inline_align, Manager,
               std::enable_if_t<IsInline<Ptr, inline_size, inline_align,
                                         Manager>::value>>::methods = {
        Move,
        Destroy,
        Release,
        IsOwning,
        GetIf,
        sizeof(Dependency<Ptr, Manager>),
        alignof(Dependency<Ptr, Manager>)};

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
                  IsValidDependency<Ptr, std::decay_t<Manager>>>::value,
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
                  IsValidDependency<Ptr, std::decay_t<Manager>>>::value,
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
template <typename Manager, typename ManagerArg,
          std::enable_if_t<IsValidDependency<Ptr, Manager>::value, int>>
inline AnyDependencyImpl<Ptr, inline_size, inline_align>::AnyDependencyImpl(
    absl::in_place_type_t<Manager>, ManagerArg&& manager_arg) {
  Initialize<Manager>(std::forward<ManagerArg>(manager_arg));
}

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
template <
    typename Manager,
    std::enable_if_t<IsValidDependency<Ptr, std::decay_t<Manager>>::value, int>>
inline void AnyDependencyImpl<Ptr, inline_size, inline_align>::Reset(
    Manager&& manager) {
  ptr_.~Ptr();
  methods_->destroy(repr_.storage);
  Initialize<std::decay_t<Manager>>(std::forward<Manager>(manager));
}

template <typename Ptr, size_t inline_size, size_t inline_align>
template <typename Manager, typename ManagerArg,
          std::enable_if_t<IsValidDependency<Ptr, Manager>::value, int>>
inline void AnyDependencyImpl<Ptr, inline_size, inline_align>::Reset(
    absl::in_place_type_t<Manager>, ManagerArg&& manager_arg) {
  ptr_.~Ptr();
  methods_->destroy(repr_.storage);
  Initialize<Manager>(std::forward<ManagerArg>(manager_arg));
}

template <typename Ptr, size_t inline_size, size_t inline_align>
template <typename Manager, typename... ManagerArgs,
          std::enable_if_t<IsValidDependency<Ptr, Manager>::value, int>>
inline void AnyDependencyImpl<Ptr, inline_size, inline_align>::Emplace(
    ManagerArgs&&... manager_args) {
  ptr_.~Ptr();
  methods_->destroy(repr_.storage);
  Initialize<Manager>(
      std::forward_as_tuple(std::forward<ManagerArgs>(manager_args)...));
}

#if __cpp_deduction_guides
template <typename Ptr, size_t inline_size, size_t inline_align>
template <
    template <typename...> class ManagerTemplate, typename... ManagerArgs,
    std::enable_if_t<
        IsValidDependency<Ptr, DeduceClassTemplateArgumentsT<
                                   ManagerTemplate, ManagerArgs...>>::value,
        int>>
inline void AnyDependencyImpl<Ptr, inline_size, inline_align>::Emplace(
    ManagerArgs&&... manager_args) {
  ptr_.~Ptr();
  methods_->destroy(repr_.storage);
  Initialize<DeduceClassTemplateArgumentsT<ManagerTemplate, ManagerArgs...>>(
      std::forward_as_tuple(std::forward<ManagerArgs>(manager_args)...));
}
#endif

template <typename Ptr, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<!std::is_reference<Manager>::value &&
                               !any_dependency_internal::IsAnyDependency<
                                   Ptr, Manager>::value,
                           int>>
inline void AnyDependencyImpl<Ptr, inline_size, inline_align>::Initialize(
    const Manager& manager) {
  methods_ = &MethodsFor<Manager>::methods;
  MethodsFor<Manager>::Construct(repr_.storage, &ptr_, manager);
}

template <typename Ptr, size_t inline_size, size_t inline_align>
template <
    typename Manager,
    std::enable_if_t<
        !any_dependency_internal::IsAnyDependency<Ptr, Manager>::value, int>>
inline void AnyDependencyImpl<Ptr, inline_size, inline_align>::Initialize(
    Manager&& manager) {
  methods_ = &MethodsFor<Manager>::methods;
  MethodsFor<Manager>::Construct(repr_.storage, &ptr_,
                                 std::forward<Manager>(manager));
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
          std::enable_if_t<!std::is_reference<Manager>::value &&
                               !any_dependency_internal::IsAnyDependency<
                                   Ptr, Manager>::value,
                           int>>
inline void AnyDependencyImpl<Ptr, inline_size, inline_align>::Initialize(
    std::tuple<ManagerArgs...> manager_args) {
  methods_ = &MethodsFor<Manager>::methods;
  MethodsFor<Manager>::Construct(repr_.storage, &ptr_, std::move(manager_args));
}

template <typename Ptr, size_t inline_size, size_t inline_align>
template <
    typename Manager, typename... ManagerArgs,
    std::enable_if_t<
        any_dependency_internal::IsAnyDependency<Ptr, Manager>::value, int>>
inline void AnyDependencyImpl<Ptr, inline_size, inline_align>::Initialize(
    std::tuple<ManagerArgs...> manager_args) {
#if __cpp_guaranteed_copy_elision && __cpp_lib_make_from_tuple
  AnyDependencyImpl<Ptr, inline_size, inline_align> manager =
      std::make_from_tuple<AnyDependencyImpl<Ptr, inline_size, inline_align>>(
          std::move(manager_args));
#else
  AnyDependencyImpl<Ptr, inline_size, inline_align> manager(
      std::move(manager_args), std::index_sequence_for<ManagerArgs...>());
#endif
  // Adopt `manager` instead of wrapping it.
  manager.ptr_ = any_dependency_internal::SentinelPtr<Ptr>();
  methods_ = std::exchange(manager.methods_, &NullMethods::methods);
  methods_->move(repr_.storage, &ptr_, manager.repr_.storage);
}

template <typename Ptr, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<IsValidDependency<Ptr, Manager>::value, int>>
Manager* AnyDependencyImpl<Ptr, inline_size, inline_align>::GetIf() {
  return const_cast<Manager*>(static_cast<const Manager*>(
      methods_->get_if(repr_.storage, TypeId::For<Manager>())));
}

template <typename Ptr, size_t inline_size, size_t inline_align>
template <typename Manager,
          std::enable_if_t<IsValidDependency<Ptr, Manager>::value, int>>
const Manager* AnyDependencyImpl<Ptr, inline_size, inline_align>::GetIf()
    const {
  return static_cast<const Manager*>(
      methods_->get_if(repr_.storage, TypeId::For<Manager>()));
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_ANY_DEPENDENCY_H_
