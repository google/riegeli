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

#ifndef RIEGELI_BASE_ANY_DEPENDENCY_INTERNAL_H_
#define RIEGELI_BASE_ANY_DEPENDENCY_INTERNAL_H_

#include <stddef.h>

#include <algorithm>
#include <cstddef>
#include <new>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/casts.h"
#include "absl/meta/type_traits.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/dependency_base.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/memory_estimator.h"
#include "riegeli/base/type_id.h"

namespace riegeli {

template <typename Handle, size_t inline_size, size_t inline_align>
class AnyDependency;
template <typename Handle>
class AnyDependencyRef;

namespace any_dependency_internal {

// Variants of `Repr`:
//  * Empty `AnyDependency`: `Repr` is not used
//  * Stored inline: `storage` holds `Dependency<Handle, Manager>`
//  * Held by pointer: `storage` holds `Dependency<Handle, Manager>*`
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
// `Repr<Handle, inline_size, inline_align>` if it is movable and it fits there.
//
// If `inline_size == 0`, the dependency is also required to be stable
// (because then `AnyDependency` declares itself stable) and trivially
// relocatable (because then `AnyDependency` declares itself with trivial ABI
// and optimizes moving to a plain memory copy of the representation).

// Properties of inline storage in an `AnyDepenency` instance are expressed as
// two numbers: `available_size` and `available_align`, while constraints of a
// movable `Dependency` instance on its storage are expressed as two numbers:
// `used_size` and `used_align`, such that
// `used_size <= available_size && used_align <= available_align` implies that
// the movable `Dependency` can be stored inline in the `AnyDependency`.
//
// This formulation allows reevaluating the condition with different values of
// `available_size` and `available_align` when considering adopting the storage
// for a different `AnyDependency` instance, at either compile time or runtime.

// Returns `available_size`: `sizeof` the storage, except that 0 indicates
// `inline_size == 0`, which means the minimal size of any inline storage with
// the given alignment, while also putting additional constraints on the
// `Dependency` (stability and trivial relocatability).
template <typename Handle, size_t inline_size, size_t inline_align>
constexpr size_t AvailableSize() {
  if (inline_size == 0) return 0;
  return sizeof(Repr<Handle, inline_size, inline_align>);
}

// Returns `available_align`: `alignof` the storage, except that 0 means the
// minimal alignment of any inline storage.
template <typename Handle, size_t inline_size, size_t inline_align>
constexpr size_t AvailableAlign() {
  if (alignof(Repr<Handle, inline_size, inline_align>) ==
      alignof(Repr<Handle, 0, 0>)) {
    return 0;
  }
  return alignof(Repr<Handle, inline_size, inline_align>);
}

// Returns `used_size`: `sizeof` the `Dependency`, except that 0 indicates
// compatibility with `inline_size == 0`, which means fitting under the minimal
// size of any inline storage with the given alignment, and satisfying
// additional constraints (stability and trivial relocatability).
template <typename Handle, typename Manager>
constexpr size_t UsedSize() {
  if (sizeof(Dependency<Handle, Manager>) <=
          sizeof(Repr<Handle, 0, alignof(Dependency<Handle, Manager>)>) &&
      Dependency<Handle, Manager>::kIsStable &&
#ifdef ABSL_ATTRIBUTE_TRIVIAL_ABI
      absl::is_trivially_relocatable<Dependency<Handle, Manager>>::value
#else
      std::is_trivially_copyable<Dependency<Handle, Manager>>::value
#endif
  ) {
    return 0;
  }
  return sizeof(Dependency<Handle, Manager>);
}

// Returns `used_align`: `alignof` the storage, except that 0 means fitting
// under the minimal alignment of any inline storage. Making this a special
// case allows to optimize out comparisons of a compile time `used_align`
// against a runtime `available_align`.
template <typename Handle, typename Manager>
constexpr size_t UsedAlign() {
  if (alignof(Dependency<Handle, Manager>) <= alignof(Repr<Handle, 0, 0>)) {
    return 0;
  }
  return alignof(Dependency<Handle, Manager>);
}

template <typename Handle, typename Manager>
constexpr bool ReprIsInline(size_t available_size, size_t available_align) {
  return std::is_move_constructible<Dependency<Handle, Manager>>::value &&
         UsedSize<Handle, Manager>() <= available_size &&
         UsedAlign<Handle, Manager>() <= available_align;
}

template <typename Handle, typename Manager, size_t inline_size,
          size_t inline_align>
constexpr bool IsInline() {
  return ReprIsInline<Handle, Manager>(
      AvailableSize<Handle, inline_size, inline_align>(),
      AvailableAlign<Handle, inline_size, inline_align>());
}

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
  // Constructs `self` and `*self_handle` by moving from `that`, and destroys
  // `that`.
  void (*move)(Storage self, Handle* self_handle, Storage that);
  size_t used_size;
  size_t used_align;
  bool (*is_owning)(const Storage self);
  // Returns the `std::remove_reference_t<Manager>*` if `type_id` matches
  // `std::remove_reference_t<Manager>`, otherwise returns `nullptr`.
  void* (*mutable_get_if)(Storage self, TypeId type_id);
  const void* (*const_get_if)(const Storage self, TypeId type_id);
  void (*register_subobjects)(const Storage self,
                              MemoryEstimator& memory_estimator);
};

// Grouped members so that their address can be passed together.
template <typename Handle>
struct MethodsAndHandle {
  MethodsAndHandle() noexcept {}

  const Methods<Handle>* methods;
  union {
    Handle handle;
  };
};

template <typename Handle, typename Manager, bool is_inline>
struct MethodsFor;
template <typename Handle>
struct NullMethods;

// `IsAnyDependency` detects `AnyDependency` or `AnyDependencyRef` type with the
// given `Handle`.

template <typename Handle, typename T>
struct IsAnyDependency : std::false_type {};

template <typename Handle, size_t inline_size, size_t inline_align>
struct IsAnyDependency<Handle, AnyDependency<Handle, inline_size, inline_align>>
    : std::true_type {};
template <typename Handle>
struct IsAnyDependency<Handle, AnyDependencyRef<Handle>> : std::true_type {};

// Implementation details follow.

template <typename Handle>
inline Handle SentinelHandle() {
  return Initializer<Handle>(
             RiegeliDependencySentinel(static_cast<Handle*>(nullptr)))
      .Construct();
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
      Destroy,  Move,         0,          0,
      IsOwning, MutableGetIf, ConstGetIf, RegisterSubobjects};
};

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if !__cpp_inline_variables
template <typename Handle>
constexpr Methods<Handle> NullMethods<Handle>::kMethods;
#endif

template <typename Handle, typename Manager>
struct MethodsFor<Handle, Manager, true> {
  static void Construct(Storage self, Handle* self_handle,
                        Initializer<Manager> manager) {
    new (self) Dependency<Handle, Manager>(std::move(manager));
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
    memory_estimator.RegisterSubobjects(&dep(self));
  }

 public:
  static constexpr Methods<Handle> kMethods = {Destroy,
                                               Move,
                                               UsedSize<Handle, Manager>(),
                                               UsedAlign<Handle, Manager>(),
                                               IsOwning,
                                               MutableGetIf,
                                               ConstGetIf,
                                               RegisterSubobjects};
};

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if !__cpp_inline_variables
template <typename Handle, typename Manager>
constexpr Methods<Handle> MethodsFor<Handle, Manager, true>::kMethods;
#endif

template <typename Handle, typename Manager>
struct MethodsFor<Handle, Manager, false> {
  static void Construct(Storage self, Handle* self_handle,
                        Initializer<Manager> manager) {
    new (self) Dependency<Handle, Manager>*(
        new Dependency<Handle, Manager>(std::move(manager)));
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
    memory_estimator.RegisterDynamicObject(dep_ptr(self));
  }

 public:
  static constexpr Methods<Handle> kMethods = {
      Destroy,  Move,         0,          0,
      IsOwning, MutableGetIf, ConstGetIf, RegisterSubobjects};
};

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if !__cpp_inline_variables
template <typename Handle, typename Manager>
constexpr Methods<Handle> MethodsFor<Handle, Manager, false>::kMethods;
#endif

}  // namespace any_dependency_internal
}  // namespace riegeli

#endif  // RIEGELI_BASE_ANY_DEPENDENCY_INTERNAL_H_
