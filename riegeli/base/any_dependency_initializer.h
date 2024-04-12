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

#ifndef RIEGELI_BASE_ANY_DEPENDENCY_INITIALIZER_H_
#define RIEGELI_BASE_ANY_DEPENDENCY_INITIALIZER_H_

#include <stddef.h>

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <new>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/meta/type_traits.h"
#include "riegeli/base/any_dependency_internal.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"

namespace riegeli {

namespace any_dependency_internal {
template <typename Handle, size_t inline_size, size_t inline_align>
class AnyDependencyBase;
}  // namespace any_dependency_internal

// A parameter of type `AnyDependencyInitializer<Handle>` allows the caller
// to specify an `AnyDependency<Handle>` by passing a value convertible to
// `AnyDependency<Handle>`.
//
// In contrast to accepting `AnyDependency<Handle>` directly, this allows to
// construct the object in-place, avoiding constructing a temporary and moving
// from it. This also avoids specifying `::Inlining<...>` in the interface while
// benefiting from that in the implementation.
//
// This is similar to `Initializer<AnyDependency<Handle>>`, except that it
// efficiently handles `AnyDependency<Handle>` specializations with any inline
// storage constraints.
//
// `AnyDependencyInitializer<Handle>(manager)` does not own `manager`, even if
// it involves temporaries, hence it should be used only as a parameter of a
// function or constructor, so that the temporaries outlive its usage. Instead
// of storing an `AnyDependencyInitializer<Handle>` in a variable or returning
// it from a function, consider
// `riegeli::OwningMaker<Manager>(manager_args...)`,
// `MakerTypeFor<Manager, ManagerArgs...>`, or `AnyDependency<Handle>`.
template <typename Handle>
class AnyDependencyInitializer {
 public:
  // An `AnyDependency` will be empty.
  AnyDependencyInitializer() : construct_(ConstructMethodEmpty) {}

  // An `AnyDependency` will hold a
  // `Dependency<Handle, InitializerTargetT<Manager>>`.
  template <
      typename Manager,
      std::enable_if_t<
          absl::conjunction<
              absl::negation<std::is_same<std::decay_t<Manager>,
                                          AnyDependencyInitializer>>,
              IsValidDependency<Handle, InitializerTargetT<Manager>>>::value,
          int> = 0>
  /*implicit*/ AnyDependencyInitializer(
      Manager&& manager ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : construct_(ConstructMethod<Manager>),
        context_(const_cast<absl::remove_cvref_t<Manager>*>(&manager)) {}

  AnyDependencyInitializer(AnyDependencyInitializer&& other) = default;
  AnyDependencyInitializer& operator=(AnyDependencyInitializer&&) = delete;

 private:
  // For `Construct()`.
  template <typename OtherHandle, size_t inline_size, size_t inline_align>
  friend class any_dependency_internal::AnyDependencyBase;

  using Storage = any_dependency_internal::Storage;
  using MethodsAndHandle = any_dependency_internal::MethodsAndHandle<Handle>;
  using NullMethods = any_dependency_internal::NullMethods<Handle>;
  template <typename Manager, bool is_inline>
  using MethodsFor =
      any_dependency_internal::MethodsFor<Handle, Manager, is_inline>;

  static void ConstructMethodEmpty(void* context,
                                   MethodsAndHandle& methods_and_handle,
                                   Storage storage, size_t available_size,
                                   size_t available_align);

  template <typename Manager,
            std::enable_if_t<!any_dependency_internal::IsAnyDependency<
                                 Handle, InitializerTargetT<Manager>>::value,
                             int> = 0>
  static void ConstructMethod(void* context,
                              MethodsAndHandle& methods_and_handle,
                              Storage storage, size_t available_size,
                              size_t available_align);
  template <typename Manager,
            std::enable_if_t<any_dependency_internal::IsAnyDependency<
                                 Handle, InitializerTargetT<Manager>>::value,
                             int> = 0>
  static void ConstructMethod(void* context,
                              MethodsAndHandle& methods_and_handle,
                              Storage storage, size_t available_size,
                              size_t available_align);

  // Constructs `methods_and_handle` and `storage` by moving from `*this`.
  void Construct(MethodsAndHandle& methods_and_handle, Storage storage,
                 size_t available_size, size_t available_align) && {
    construct_(context_, methods_and_handle, storage, available_size,
               available_align);
  }

  void (*construct_)(void* context, MethodsAndHandle& methods_and_handle,
                     Storage storage, size_t available_size,
                     size_t available_align);
  void* context_;
};

// Implementation details follow.

template <typename Handle>
void AnyDependencyInitializer<Handle>::ConstructMethodEmpty(
    ABSL_ATTRIBUTE_UNUSED void* context, MethodsAndHandle& methods_and_handle,
    ABSL_ATTRIBUTE_UNUSED Storage storage,
    ABSL_ATTRIBUTE_UNUSED size_t available_size,
    ABSL_ATTRIBUTE_UNUSED size_t available_align) {
  methods_and_handle.methods = &NullMethods::kMethods;
  new (&methods_and_handle.handle)
      Handle(any_dependency_internal::SentinelHandle<Handle>());
}

template <typename Handle>
template <typename Manager,
          std::enable_if_t<!any_dependency_internal::IsAnyDependency<
                               Handle, InitializerTargetT<Manager>>::value,
                           int>>
void AnyDependencyInitializer<Handle>::ConstructMethod(
    void* context, MethodsAndHandle& methods_and_handle, Storage storage,
    size_t available_size, size_t available_align) {
  using Target = InitializerTargetT<Manager>;
  // This is equivalent to calling `MethodsFor<Target, true>::Construct()`
  // or `MethodsFor<Target, false>::Construct()`. Separate allocation of
  // `Dependency<Handle, Target>` from its construction, so that the code for
  // construction can be shared between the two cases, reducing the code size.
  const any_dependency_internal::Methods<Handle>* methods_ptr;
  Dependency<Handle, Target>* dep_ptr;
  bool constructed = false;
  if (any_dependency_internal::ReprIsInline<Handle, Target>(available_size,
                                                            available_align)) {
    methods_ptr = &MethodsFor<Target, true>::kMethods;
    dep_ptr = reinterpret_cast<Dependency<Handle, Target>*>(storage);
  } else {
    methods_ptr = &MethodsFor<Target, false>::kMethods;
#if __cpp_aligned_new
    if (alignof(Dependency<Handle, Target>) >
        __STDCPP_DEFAULT_NEW_ALIGNMENT__) {
      dep_ptr = static_cast<Dependency<Handle, Target>*>(operator new(
          sizeof(Dependency<Handle, Target>),
          std::align_val_t(alignof(Dependency<Handle, Target>))));
    }
#else
#ifdef __STDCPP_DEFAULT_NEW_ALIGNMENT__
    constexpr size_t kDefaultNewAlignment = __STDCPP_DEFAULT_NEW_ALIGNMENT__;
#else
    constexpr size_t kDefaultNewAlignment = alignof(max_align_t);
#endif
    if (alignof(Dependency<Handle, Target>) > kDefaultNewAlignment) {
      // Factoring out the code constructing `Dependency<Handle, Target>` is
      // not feasible.
      dep_ptr = new Dependency<Handle, Target>(std::forward<Manager>(
          *static_cast<std::remove_reference_t<Manager>*>(context)));
      constructed = true;
    }
#endif
    else {
      dep_ptr = static_cast<Dependency<Handle, Target>*>(operator new(
          sizeof(Dependency<Handle, Target>)));
    }
    new (storage) Dependency<Handle, Target>*(dep_ptr);
  }
  methods_and_handle.methods = methods_ptr;
  if (!constructed) {
    new (dep_ptr) Dependency<Handle, Target>(std::forward<Manager>(
        *static_cast<std::remove_reference_t<Manager>*>(context)));
  }
  new (&methods_and_handle.handle) Handle(dep_ptr->get());
}

template <typename Handle>
template <typename Manager,
          std::enable_if_t<any_dependency_internal::IsAnyDependency<
                               Handle, InitializerTargetT<Manager>>::value,
                           int>>
void AnyDependencyInitializer<Handle>::ConstructMethod(
    void* context, MethodsAndHandle& methods_and_handle, Storage storage,
    size_t available_size, size_t available_align) {
  using Target = InitializerTargetT<Manager>;
  // Materialize `Target` to consider adopting its storage.
  [&](Target&& target) {
    // `target.methods_and_handle_.methods->used_size <=
    //      Target::kAvailableSize`, hence if `Target::kAvailableSize == 0` then
    // `target.methods_and_handle_.methods->used_size <= available_size`.
    // No need to check possibly at runtime.
    if ((Target::kAvailableSize == 0 ||
         target.methods_and_handle_.methods->used_size <= available_size) &&
        // Same for alignment.
        (Target::kAvailableAlign == 0 ||
         target.methods_and_handle_.methods->used_align <= available_align)) {
      // Adopt `target` instead of wrapping it.
      if (Target::kAvailableSize == 0) {
        // Replace an indirect call to `methods_and_handle_.methods->move()`
        // with a plain assignment of `methods_and_handle_.handle` and a memory
        // copy of `repr_`.
        //
        // This would safe whenever
        // `target.methods_and_handle_.methods->used_size == 0`, but this is
        // handled specially only if the condition can be determined at compile
        // time.
        methods_and_handle.methods = std::exchange(
            target.methods_and_handle_.methods, &NullMethods::kMethods);
        methods_and_handle.handle =
            std::exchange(target.methods_and_handle_.handle,
                          any_dependency_internal::SentinelHandle<Handle>());
        std::memcpy(storage, &target.repr_, sizeof(target.repr_));
      } else {
        target.methods_and_handle_.handle =
            any_dependency_internal::SentinelHandle<Handle>();
        methods_and_handle.methods = std::exchange(
            target.methods_and_handle_.methods, &NullMethods::kMethods);
        methods_and_handle.methods->move(storage, &methods_and_handle.handle,
                                         target.repr_.storage);
      }
      return;
    }
    methods_and_handle.methods =
        &MethodsFor<Target, /*is_inline=*/false>::kMethods;
    // `std::move(target)` is correct and `std::forward<Target>(target)` is not
    // necessary: `Target` is always an `AnyDependency`, never an lvalue
    // reference.
    MethodsFor<Target, /*is_inline=*/false>::Construct(
        storage, &methods_and_handle.handle, std::move(target));
  }(Initializer<InitializerTargetT<Manager>>(
        std::forward<Manager>(
            *static_cast<std::remove_reference_t<Manager>*>(context)))
        .Reference());
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_ANY_DEPENDENCY_INITIALIZER_H_
