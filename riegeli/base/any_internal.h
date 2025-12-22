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

#ifndef RIEGELI_BASE_ANY_INTERNAL_H_
#define RIEGELI_BASE_ANY_INTERNAL_H_

#include <stddef.h>

#include <algorithm>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/meta/type_traits.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/closing_ptr.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/dependency_base.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/memory_estimator.h"
#include "riegeli/base/type_erased_ref.h"
#include "riegeli/base/type_id.h"

namespace riegeli {

template <typename Handle, size_t inline_size, size_t inline_align>
class Any;
template <typename Handle>
class AnyRef;

namespace any_internal {

// Variants of `Repr`:
//  * Empty `Any`: `Repr` is not used
//  * Stored inline: `storage` holds `Dependency<Handle, Manager>`
//  * Held by pointer: `storage` holds `Dependency<Handle, Manager>*`
template <typename Handle, size_t inline_size, size_t inline_align>
struct Repr {
  // clang-format off
  alignas(UnsignedMax(alignof(void*), inline_align))
      char storage[UnsignedMax(sizeof(void*), inline_size)];
  // clang-format on
};

// By convention, a parameter of type `Storage` points to
// `Repr<Handle, inline_size, inline_align>::storage`.
using Storage = char[];

// A `Dependency<Handle, Manager>` is stored inline in
// `Repr<Handle, inline_size, inline_align>` if it is movable and it fits there.
//
// If `inline_size == 0`, the dependency is also required to be stable (because
// then `Any` declares itself stable) and trivially relocatable (because then
// `Any` declares itself with trivial ABI and optimizes moving to a plain memory
// copy of the representation).

// Properties of inline storage in an `Any` instance are expressed as two
// numbers: `available_size` and `available_align`, while constraints of a
// movable `Dependency` instance on its storage are expressed as two numbers:
// `used_size` and `used_align`, such that
// `used_size <= available_size && used_align <= available_align` implies that
// the movable `Dependency` can be stored inline in the `Any`.
//
// This formulation allows reevaluating the condition with different values of
// `available_size` and `available_align` when considering adopting the storage
// for a different `Any` instance, at either compile time or runtime.

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
// size of any inline storage with the given alignment, and being stable.
template <typename Handle, typename Manager>
constexpr size_t UsedSize() {
  if (sizeof(Dependency<Handle, Manager>) <=
          sizeof(Repr<Handle, 0, alignof(Dependency<Handle, Manager>)>) &&
      Dependency<Handle, Manager>::kIsStable) {
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
  return std::is_move_constructible_v<Dependency<Handle, Manager>> &&
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

template <typename Handle>
struct MethodsAndHandle;

// Method pointers.
template <typename Handle>
struct Methods {
  // Destroys `self`.
  void (*destroy)(Storage self);
  // Constructs `dest` with `*dest_handle` by moving from `src`. Destroys `src`.
  void (*move)(Storage src, Storage dest,
               MethodsAndHandle<Handle>* dest_methods_and_handle);
  // Constructs a differently represented `dest` with `*dest_methods_and_handle`
  // by moving from `src` to the heap and pointing `dest` to that. Destroys
  // `src`. Used only if `used_size > 0`.
  void (*move_to_heap)(Storage src, Storage dest,
                       MethodsAndHandle<Handle>* dest_methods_and_handle);
  // Constructs a differently represented `dest` with `*dest_methods_and_handle`
  // by pointing `dest` to `src`.
  void (*make_reference)(Storage src, Storage dest,
                         MethodsAndHandle<Handle>* dest_methods_and_handle);
  size_t used_size;
  size_t used_align;
  TypeId type_id;
  bool (*is_owning)(const Storage self);
  // Returns the `Manager&` stored in `self`, with the `Manager` type
  // corresponding to `type_id`. Used only if `type_id != nullptr`.
  // If `self` is const then `Manager` should be const, otherwise `Manager`
  // can be non-const.
  TypeErasedRef (*get_raw_manager)(const Storage self);
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

template <typename Handle>
struct NullMethods;
template <typename Handle, typename Manager>
struct MethodsForReference;
template <typename Handle, typename Manager, bool is_inline>
struct MethodsFor;

// `IsAny` detects `Any` or `AnyRef` type with the given `Handle`, or an rvalue
// reference to it.

template <typename Handle, typename T>
struct IsAny : std::false_type {};

template <typename Handle, size_t inline_size, size_t inline_align>
struct IsAny<Handle, Any<Handle, inline_size, inline_align>> : std::true_type {
};

template <typename Handle>
struct IsAny<Handle, AnyRef<Handle>> : std::true_type {};

template <typename Handle, typename T>
struct IsAny<Handle, T&&> : IsAny<Handle, T> {};

// `IsAnyClosingPtr` detects `Any` or `AnyRef` type with the given `Handle`,
// wrapped in `ClosingPtrType` or in an rvalue reference to it.

template <typename Handle, typename T>
struct IsAnyClosingPtr : std::false_type {};

template <typename Handle, size_t inline_size, size_t inline_align>
struct IsAnyClosingPtr<
    Handle,
    std::unique_ptr<Any<Handle, inline_size, inline_align>, NullDeleter>>
    : std::true_type {};

template <typename Handle>
struct IsAnyClosingPtr<Handle, std::unique_ptr<AnyRef<Handle>, NullDeleter>>
    : std::true_type {};

template <typename Handle, typename T>
struct IsAnyClosingPtr<Handle, T&&> : IsAnyClosingPtr<Handle, T> {};

template <typename Handle>
inline Handle SentinelHandle() {
  return Initializer<Handle>(
      RiegeliDependencySentinel(static_cast<Handle*>(nullptr)));
}

// Implementation details follow.

template <typename Handle>
struct NullMethods {
 private:
  static void Destroy(ABSL_ATTRIBUTE_UNUSED Storage self) {}
  static void Move(ABSL_ATTRIBUTE_UNUSED Storage src,
                   ABSL_ATTRIBUTE_UNUSED Storage dest,
                   MethodsAndHandle<Handle>* dest_methods_and_handle) {
    dest_methods_and_handle->methods = &kMethods;
    new (&dest_methods_and_handle->handle) Handle(SentinelHandle<Handle>());
  }
  static bool IsOwning(ABSL_ATTRIBUTE_UNUSED const Storage self) {
    return false;
  }
  static void RegisterSubobjects(
      ABSL_ATTRIBUTE_UNUSED const Storage self,
      ABSL_ATTRIBUTE_UNUSED MemoryEstimator& memory_estimator) {}

 public:
  static constexpr Methods<Handle> kMethods = {
      Destroy, Move,    nullptr,  Move,    0,
      0,       nullptr, IsOwning, nullptr, RegisterSubobjects};
};

template <typename Handle, typename Manager>
struct MethodsForReference {
 private:
  static Dependency<Handle, Manager>* dep_ptr(const Storage self) {
    return *std::launder(
        reinterpret_cast<Dependency<Handle, Manager>* const*>(self));
  }

  static void Destroy(ABSL_ATTRIBUTE_UNUSED Storage self) {}
  static void Move(Storage src, Storage dest,
                   MethodsAndHandle<Handle>* dest_methods_and_handle) {
    new (dest) Dependency<Handle, Manager>*(dep_ptr(src));
    dest_methods_and_handle->methods = &kMethods;
    new (&dest_methods_and_handle->handle) Handle(dep_ptr(dest)->get());
  }
  static bool IsOwning(const Storage self) { return dep_ptr(self)->IsOwning(); }
  static TypeErasedRef GetRawManager(const Storage self) {
    return TypeErasedRef(dep_ptr(self)->manager());
  }
  static void RegisterSubobjects(
      ABSL_ATTRIBUTE_UNUSED const Storage self,
      ABSL_ATTRIBUTE_UNUSED MemoryEstimator& memory_estimator) {}

 public:
  static constexpr Methods<Handle> kMethods = {
      Destroy,
      Move,
      nullptr,
      Move,
      0,
      0,
      TypeId::For<absl::remove_cvref_t<Manager>>(),
      IsOwning,
      GetRawManager,
      RegisterSubobjects};
};

template <typename Handle, typename Manager>
struct MethodsFor<Handle, Manager, false> {
  static void Construct(Storage self, Handle* self_handle,
                        Initializer<Manager> manager) {
    new (self) Dependency<Handle, Manager>*(
        new Dependency<Handle, Manager>(std::move(manager)));
    new (self_handle) Handle(dep_ptr(self)->get());
  }

 private:
  static Dependency<Handle, Manager>* dep_ptr(const Storage self) {
    return *std::launder(
        reinterpret_cast<Dependency<Handle, Manager>* const*>(self));
  }

  static void Destroy(Storage self) { delete dep_ptr(self); }
  static void Move(Storage src, Storage dest,
                   MethodsAndHandle<Handle>* dest_methods_and_handle) {
    new (dest) Dependency<Handle, Manager>*(dep_ptr(src));
    dest_methods_and_handle->methods = &kMethods;
    new (&dest_methods_and_handle->handle) Handle(dep_ptr(dest)->get());
  }
  static void MakeReference(Storage src, Storage dest,
                            MethodsAndHandle<Handle>* dest_methods_and_handle) {
    new (dest) Dependency<Handle, Manager>*(dep_ptr(src));
    dest_methods_and_handle->methods =
        &MethodsForReference<Handle, Manager>::kMethods;
    new (&dest_methods_and_handle->handle) Handle(dep_ptr(dest)->get());
  }
  static bool IsOwning(const Storage self) { return dep_ptr(self)->IsOwning(); }
  static TypeErasedRef GetRawManager(const Storage self) {
    return TypeErasedRef(dep_ptr(self)->manager());
  }
  static void RegisterSubobjects(const Storage self,
                                 MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterDynamicObject(dep_ptr(self));
  }

 public:
  static constexpr Methods<Handle> kMethods = {
      Destroy,
      Move,
      nullptr,
      MakeReference,
      0,
      0,
      TypeId::For<absl::remove_cvref_t<Manager>>(),
      IsOwning,
      GetRawManager,
      RegisterSubobjects};
};

template <typename Handle, typename Manager>
struct MethodsFor<Handle, Manager, true> {
  static void Construct(Storage self, Handle* self_handle,
                        Initializer<Manager> manager) {
    new (self) Dependency<Handle, Manager>(std::move(manager));
    new (self_handle) Handle(dep(self).get());
  }

 private:
  static Dependency<Handle, Manager>& dep(Storage self) {
    return *std::launder(reinterpret_cast<Dependency<Handle, Manager>*>(self));
  }
  static const Dependency<Handle, Manager>& dep(const Storage self) {
    return *std::launder(
        reinterpret_cast<const Dependency<Handle, Manager>*>(self));
  }
  static Dependency<Handle, Manager>* dep_ptr(const Storage self) {
    return *std::launder(
        reinterpret_cast<Dependency<Handle, Manager>* const*>(self));
  }

  static void Destroy(Storage self) {
    dep(self).~Dependency<Handle, Manager>();
  }
  static void Move(Storage src, Storage dest,
                   MethodsAndHandle<Handle>* dest_methods_and_handle) {
    new (dest) Dependency<Handle, Manager>(std::move(dep(src)));
    dep(src).~Dependency<Handle, Manager>();
    dest_methods_and_handle->methods = &kMethods;
    new (&dest_methods_and_handle->handle) Handle(dep(dest).get());
  }
  static void MoveToHeap(Storage src, Storage dest,
                         MethodsAndHandle<Handle>* dest_methods_and_handle) {
    new (dest) Dependency<Handle, Manager>*(
        new Dependency<Handle, Manager>(std::move(dep(src))));
    dep(src).~Dependency<Handle, Manager>();
    dest_methods_and_handle->methods =
        &MethodsFor<Handle, Manager, false>::kMethods;
    new (&dest_methods_and_handle->handle) Handle(dep_ptr(dest)->get());
  }
  static void MakeReference(Storage src, Storage dest,
                            MethodsAndHandle<Handle>* dest_methods_and_handle) {
    new (dest) Dependency<Handle, Manager>*(&dep(src));
    dest_methods_and_handle->methods =
        &MethodsForReference<Handle, Manager>::kMethods;
    new (&dest_methods_and_handle->handle) Handle(dep_ptr(dest)->get());
  }
  static bool IsOwning(const Storage self) { return dep(self).IsOwning(); }
  static TypeErasedRef GetRawManager(const Storage self) {
    return TypeErasedRef(dep(self).manager());
  }
  static void RegisterSubobjects(const Storage self,
                                 MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&dep(self));
  }

 public:
  static constexpr Methods<Handle> kMethods = {
      Destroy,
      Move,
      MoveToHeap,
      MakeReference,
      UsedSize<Handle, Manager>(),
      UsedAlign<Handle, Manager>(),
      TypeId::For<absl::remove_cvref_t<Manager>>(),
      IsOwning,
      GetRawManager,
      RegisterSubobjects};
};

}  // namespace any_internal
}  // namespace riegeli

#endif  // RIEGELI_BASE_ANY_INTERNAL_H_
