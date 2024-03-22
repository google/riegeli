// Copyright 2017 Google LLC
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

#ifndef RIEGELI_BASE_MEMORY_ESTIMATOR_H_
#define RIEGELI_BASE_MEMORY_ESTIMATOR_H_

#include <stddef.h>

#include <array>
#include <memory>
#include <string>
#include <tuple>
#include <type_traits>
#include <typeindex>
#include <typeinfo>  // IWYU pragma: keep
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/container/node_hash_map.h"
#include "absl/container/node_hash_set.h"
#include "absl/meta/type_traits.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "absl/types/variant.h"
#include "riegeli/base/arithmetic.h"

namespace google {
namespace protobuf {
class Message;
}  // namespace protobuf
}  // namespace google

namespace riegeli {

// Estimates the amount of memory used by multiple objects which may share their
// subobjects (e.g. by reference counting).
//
// This is done by traversing the objects and their subobjects, skipping objects
// already seen. Types participating in the traversal should customize how to
// perform their part.
class MemoryEstimator {
 public:
  MemoryEstimator() = default;

  MemoryEstimator(const MemoryEstimator& that);
  MemoryEstimator& operator=(const MemoryEstimator& that);

  MemoryEstimator(MemoryEstimator&& that) noexcept;
  MemoryEstimator& operator=(MemoryEstimator&& that) noexcept;

  ~MemoryEstimator();

  // If `true`, the results are less likely to depend on the behavior of the
  // memory allocator. This is useful for testing.
  void set_deterministic_for_testing(bool deterministic_for_testing) {
    deterministic_for_testing_ = deterministic_for_testing;
  }
  bool deterministic_for_testing() const { return deterministic_for_testing_; }

  // Registers the given amount of memory as used.
  void RegisterMemory(size_t memory);

  // Registers the given length of a block of dynamically allocated memory as
  // used. The length should correspond to a single allocation. The actual
  // registered amount includes estimated overhead of the memory allocator.
  //
  // If the address of the allocated memory is provided, it might be used for a
  // better estimation.
  void RegisterDynamicMemory(const void* ptr, size_t memory);
  void RegisterDynamicMemory(size_t memory);

  // Begins registering an object which might be shared by other objects.
  //
  // The argument should be a pointer which uniquely identifies the object.
  // This is usually the pointer to the object itself, but that might not be
  // possible if the object is not public and is registered by code external
  // to it, in which case some proxy is needed.
  //
  // Returns `true` if this object was not seen yet. Only in this case the
  // caller should register its memory and subobjects.
  //
  // If `ptr == nullptr` then always returns `false`.
  bool RegisterNode(const void* ptr);

  // Adds `T` to the stored set of unknown types, to be returned by
  // `UnknownTypes()`.
  //
  // This indicates that traversal encountered a type for which
  // `RegisterSubobjects()` is not customized and the type is not trivially
  // destructible, which likely indicates that an interesting customization is
  // missing and results are underestimated.
  template <typename T>
  void RegisterUnknownType();

  // Returns `sizeof` the most derived object of which `object` is the base
  // object.
  //
  // To customize `DynamicSizeOf()` for a class `T`, define a free function:
  //   `friend size_t RiegeliDynamicSizeOf(const T* self)`
  // as a friend of `T` inside class definition or in the same namespace as `T`,
  // so that it can be found via ADL. That function typically calls a protected
  // or private virtual function whose overrides return `sizeof(Derived)`.
  //
  // By default returns `sizeof(T)`.
  template <typename T>
  static size_t DynamicSizeOf(const T* object);

  // Registers subobjects of `object`. Does not include memory corresponding to
  // `sizeof(T)`.
  //
  // To customize `RegisterSubobjects()` for a class `T`, define a free
  // function:
  //   `friend void RiegeliRegisterSubobjects(
  //        const T* self, MemoryEstimator& memory_estimator)`
  // as a friend of `T` inside class definition or in the same namespace as `T`,
  // so that it can be found via ADL. `MemoryEstimator` in the parameter type
  // can also be a template parameter to reduce library dependencies.
  //
  // By default does nothing if `T` is trivially destructible, otherwise calls
  // `RegisterUnknownType<T>()`, which likely indicates that an interesting
  // customization is missing and results are underestimated.
  //
  // If `object` might be a member variable stored as a reference (e.g. in
  // `std::tuple`), the type argument `T` must be specified explicitly, because
  // `T` deduced from the argument of `RegisterSubobjects()` would be the
  // corresponding value type. A member variable which is always a reference
  // is trivially destructible and does not need to be registered.
  //
  // Predefined customizations include:
  //  * `std::unique_ptr<T>`
  //  * `std::shared_ptr<T>`
  //  * `std::basic_string<Char, Traits, Alloc>` (`Alloc` is ignored)
  //  * `absl::Cord`
  //  * `absl::optional<T>`
  //  * `absl::variant<T...>`
  //  * `std::pair<T, U>`
  //  * `std::tuple<T...>`
  //  * `std::array<T, size>`
  //  * `std::vector<T, Alloc>` (`Alloc` is ignored)
  //  * `absl::flat_hash_set<T, Eq, Hash, Alloc>` (`Alloc` is ignored)
  //  * `absl::flat_hash_map<K, V, Eq, Hash, Alloc>` (`Alloc` is ignored)
  //  * `absl::node_hash_set<T, Eq, Hash, Alloc>` (`Alloc` is ignored)
  //  * `absl::node_hash_map<K, V, Eq, Hash, Alloc>` (`Alloc` is ignored)
  //  * `google::protobuf::Message`
  template <typename T>
  void RegisterSubobjects(const T* object);
  template <typename T, std::enable_if_t<std::is_reference<T>::value, int> = 0>
  void RegisterSubobjects(const std::remove_reference_t<T>* object);

  // Iterates over elements of `iterable`, registering subobjects of each.
  template <typename T>
  void RegisterSubobjectsOfElements(const T* iterable);

  // A shortcut for `RegisterDynamicMemory(object, DynamicSizeOf(object))`
  // followed by `RegisterSubobjects(object)`.
  template <typename T>
  void RegisterDynamicObject(const T* object);

  // Returns the total amount of memory added.
  size_t TotalMemory() const { return total_memory_; }

  // Returns names of encountered types for which `RegisterSubobjects()` is not
  // customized and which are not trivially destructible. If the result is not
  // empty, this likely indicates that interesting customizations are missing
  // and results are underestimated.
  //
  // If RTTI is not available, "<no rtti>" is returned as a placeholder.
  std::vector<std::string> UnknownTypes() const;

 private:
  void RegisterUnknownTypeImpl(std::type_index index);

  bool deterministic_for_testing_ = false;
  bool unknown_types_no_rtti_ = false;
  size_t total_memory_ = 0;
  absl::flat_hash_set<const void*> objects_seen_;
  absl::flat_hash_set<std::type_index> unknown_types_;
};

// Determines whether `RegisterSubobjects(const T&)` might be considered good:
// either the corresponding `RiegeliRegisterSubobjects()` is defined, or `T` is
// trivially destructible so the default definition which does nothing is likely
// appropriate.
//
// This is not necessarily accurate, in particular traversing a `T` might
// encounter types which do not have a good estimation, but if this yields
// `false`, then `RegisterSubobjects()` is most likely an underestimation and
// the caller might have some different way to estimate memory.
template <typename T>
struct RegisterSubobjectsIsGood;

// Determines whether `RegisterSubobjects(const T&)` definitely does nothing:
// the corresponding `RiegeliRegisterSubobjects()` is not defined and `T` is
// trivially destructible.
//
// This can be used to skip a loop over elements of type `T`.
template <typename T>
struct RegisterSubobjectsIsTrivial;

// Implementation details follow.

inline void MemoryEstimator::RegisterMemory(size_t memory) {
  total_memory_ = SaturatingAdd(total_memory_, memory);
}

template <typename T>
inline void MemoryEstimator::RegisterUnknownType() {
#if __cpp_rtti
  RegisterUnknownTypeImpl(std::type_index(typeid(T)));
#else
  unknown_types_no_rtti_ = true;
#endif
}

namespace memory_estimator_internal {

template <typename T, typename Enable = void>
struct HasRiegeliDynamicSizeOf : std::false_type {};

template <typename T>
struct HasRiegeliDynamicSizeOf<
    T, std::enable_if_t<std::is_convertible<decltype(RiegeliDynamicSizeOf(
                                                std::declval<const T*>())),
                                            size_t>::value>> : std::true_type {
};

template <typename T,
          std::enable_if_t<HasRiegeliDynamicSizeOf<T>::value, int> = 0>
inline size_t DynamicSizeOfImpl(const T* object) {
  return RiegeliDynamicSizeOf(object);
}

template <typename T,
          std::enable_if_t<!HasRiegeliDynamicSizeOf<T>::value, int> = 0>
inline size_t DynamicSizeOfImpl(ABSL_ATTRIBUTE_UNUSED const T* object) {
  return sizeof(T);
}

template <typename T, typename Enable = void>
struct HasRiegeliRegisterSubobjects : std::false_type {};

template <typename T>
struct HasRiegeliRegisterSubobjects<
    T, absl::void_t<decltype(RiegeliRegisterSubobjects(
           std::declval<const T*>(), std::declval<MemoryEstimator&>()))>>
    : std::true_type {};

template <typename T,
          std::enable_if_t<HasRiegeliRegisterSubobjects<T>::value, int> = 0>
inline void RegisterSubobjectsImpl(const T* object,
                                   MemoryEstimator& memory_estimator) {
  RiegeliRegisterSubobjects(object, memory_estimator);
}

template <typename T,
          std::enable_if_t<
              absl::conjunction<absl::negation<HasRiegeliRegisterSubobjects<T>>,
                                std::is_trivially_destructible<T>>::value,
              int> = 0>
inline void RegisterSubobjectsImpl(
    ABSL_ATTRIBUTE_UNUSED const T* object,
    ABSL_ATTRIBUTE_UNUSED MemoryEstimator& memory_estimator) {}

template <typename T,
          std::enable_if_t<
              absl::conjunction<
                  absl::negation<HasRiegeliRegisterSubobjects<T>>,
                  absl::negation<std::is_trivially_destructible<T>>>::value,
              int> = 0>
inline void RegisterSubobjectsImpl(ABSL_ATTRIBUTE_UNUSED const T* object,
                                   MemoryEstimator& memory_estimator) {
  memory_estimator.RegisterUnknownType<T>();
}

}  // namespace memory_estimator_internal

template <typename T>
inline size_t MemoryEstimator::DynamicSizeOf(const T* object) {
  return memory_estimator_internal::DynamicSizeOfImpl(object);
}

template <typename T>
inline void MemoryEstimator::RegisterSubobjects(const T* object) {
  memory_estimator_internal::RegisterSubobjectsImpl(object, *this);
}

template <typename T, std::enable_if_t<std::is_reference<T>::value, int>>
inline void MemoryEstimator::RegisterSubobjects(
    ABSL_ATTRIBUTE_UNUSED const std::remove_reference_t<T>* object) {}

template <typename T>
struct RegisterSubobjectsIsGood
    : absl::disjunction<
          memory_estimator_internal::HasRiegeliRegisterSubobjects<T>,
          std::is_trivially_destructible<T>> {};

template <typename T>
struct RegisterSubobjectsIsTrivial
    : absl::conjunction<
          absl::negation<
              memory_estimator_internal::HasRiegeliRegisterSubobjects<T>>,
          std::is_trivially_destructible<T>> {};

template <typename T>
inline void MemoryEstimator::RegisterSubobjectsOfElements(const T* iterable) {
  using std::begin;
  if (!RegisterSubobjectsIsTrivial<
          std::remove_reference_t<decltype(*begin(*iterable))>>::value) {
    for (const auto& element : *iterable) {
      RegisterSubobjects(&element);
    }
  }
}

template <typename T>
inline void MemoryEstimator::RegisterDynamicObject(const T* object) {
  RegisterDynamicMemory(object, DynamicSizeOf(object));
  RegisterSubobjects(object);
}

template <typename T>
inline void RiegeliRegisterSubobjects(const std::unique_ptr<T>* self,
                                      MemoryEstimator& memory_estimator) {
  if (*self != nullptr) memory_estimator.RegisterDynamicObject(&**self);
}

namespace memory_estimator_internal {

// Reflects the layout of a control block of `std::shared_ptr` from libc++.
struct SharedPtrControlBlock {
  virtual ~SharedPtrControlBlock() = default;
  long shared_count;
  long weak_count;
};

}  // namespace memory_estimator_internal

template <typename T>
inline void RiegeliRegisterSubobjects(const std::shared_ptr<T>* self,
                                      MemoryEstimator& memory_estimator) {
  if (memory_estimator.RegisterNode(self->get())) {
    memory_estimator.RegisterDynamicMemory(
        sizeof(memory_estimator_internal::SharedPtrControlBlock) +
        MemoryEstimator::DynamicSizeOf(&**self));
    memory_estimator.RegisterSubobjects(&**self);
  }
}

template <typename Char, typename Traits, typename Alloc>
inline void RiegeliRegisterSubobjects(
    const std::basic_string<Char, Traits, Alloc>* self,
    MemoryEstimator& memory_estimator) {
  if (self->capacity() > std::basic_string<Char, Traits, Alloc>().capacity()) {
    memory_estimator.RegisterDynamicMemory((self->capacity() + 1) *
                                           sizeof(Char));
  } else {
    // Assume short string optimization.
  }
}

inline void RiegeliRegisterSubobjects(const absl::Cord* self,
                                      MemoryEstimator& memory_estimator) {
  // Scale `self->EstimatedMemoryUsage()` by a fraction corresponding to how
  // much of its memory is newly seen.
  size_t new_bytes = 0;
  size_t total_bytes = 0;
  for (const absl::string_view fragment : self->Chunks()) {
    if (memory_estimator.RegisterNode(fragment.data())) {
      new_bytes += fragment.size();
    }
    total_bytes += fragment.size();
  }
  memory_estimator.RegisterMemory(static_cast<size_t>(
      static_cast<double>(self->EstimatedMemoryUsage() - sizeof(absl::Cord)) *
      (static_cast<double>(new_bytes) / static_cast<double>(total_bytes))));
}

template <typename T>
inline void RiegeliRegisterSubobjects(const absl::optional<T>* self,
                                      MemoryEstimator& memory_estimator) {
  if (*self != absl::nullopt) memory_estimator.RegisterSubobjects(&**self);
}

namespace memory_estimator_internal {

struct RegisterSubobjectsVisitor {
  template <typename T>
  void operator()(const T& object) const {
    memory_estimator.RegisterSubobjects(&object);
  }

  MemoryEstimator& memory_estimator;
};

}  // namespace memory_estimator_internal

template <typename... T>
inline void RiegeliRegisterSubobjects(const absl::variant<T...>* self,
                                      MemoryEstimator& memory_estimator) {
  absl::visit(
      memory_estimator_internal::RegisterSubobjectsVisitor{memory_estimator},
      *self);
}

template <typename T, typename U>
inline void RiegeliRegisterSubobjects(const std::pair<T, U>* self,
                                      MemoryEstimator& memory_estimator) {
  memory_estimator.RegisterSubobjects<T>(&self->first);
  memory_estimator.RegisterSubobjects<U>(&self->second);
}

namespace memory_estimator_internal {

template <size_t index, typename... T,
          std::enable_if_t<(index == sizeof...(T)), int> = 0>
inline void RegisterTupleElements(
    ABSL_ATTRIBUTE_UNUSED const std::tuple<T...>* self,
    ABSL_ATTRIBUTE_UNUSED MemoryEstimator& memory_estimator) {}

template <size_t index, typename... T,
          std::enable_if_t<(index < sizeof...(T)), int> = 0>
inline void RegisterTupleElements(const std::tuple<T...>* self,
                                  MemoryEstimator& memory_estimator) {
  memory_estimator
      .RegisterSubobjects<std::tuple_element_t<index, std::tuple<T...>>>(
          &std::get<index>(*self));
  RegisterTupleElements<index + 1>(self, memory_estimator);
}

}  // namespace memory_estimator_internal

template <typename... T>
inline void RiegeliRegisterSubobjects(const std::tuple<T...>* self,
                                      MemoryEstimator& memory_estimator) {
  memory_estimator_internal::RegisterTupleElements<0>(self, memory_estimator);
}

template <typename T, size_t size>
inline void RiegeliRegisterSubobjects(const std::array<T, size>* self,
                                      MemoryEstimator& memory_estimator) {
  memory_estimator.RegisterSubobjectsOfElements(self);
}

template <typename T, typename Alloc>
inline void RiegeliRegisterSubobjects(const std::vector<T, Alloc>* self,
                                      MemoryEstimator& memory_estimator) {
  if (self->capacity() > 0) {
    memory_estimator.RegisterDynamicMemory(self->capacity() * sizeof(T));
    memory_estimator.RegisterSubobjectsOfElements(self);
  }
}

template <typename Alloc>
inline void RiegeliRegisterSubobjects(const std::vector<bool, Alloc>* self,
                                      MemoryEstimator& memory_estimator) {
  if (self->capacity() > 0) {
    memory_estimator.RegisterDynamicMemory(self->capacity() / 8);
  }
}

template <typename T, typename Eq, typename Hash, typename Alloc>
inline void RiegeliRegisterSubobjects(
    const absl::flat_hash_set<T, Eq, Hash, Alloc>* self,
    MemoryEstimator& memory_estimator) {
  memory_estimator.RegisterMemory(
      absl::container_internal::hashtable_debug_internal::HashtableDebugAccess<
          absl::flat_hash_set<T, Eq, Hash, Alloc>>::AllocatedByteSize(*self));
  memory_estimator.RegisterSubobjectsOfElements(self);
}

template <typename K, typename V, typename Eq, typename Hash, typename Alloc>
inline void RiegeliRegisterSubobjects(
    const absl::flat_hash_map<K, V, Eq, Hash, Alloc>* self,
    MemoryEstimator& memory_estimator) {
  memory_estimator.RegisterMemory(
      absl::container_internal::hashtable_debug_internal::HashtableDebugAccess<
          absl::flat_hash_map<K, V, Eq, Hash,
                              Alloc>>::AllocatedByteSize(*self));
  memory_estimator.RegisterSubobjectsOfElements(self);
}

template <typename T, typename Eq, typename Hash, typename Alloc>
inline void RiegeliRegisterSubobjects(
    const absl::node_hash_set<T, Eq, Hash, Alloc>* self,
    MemoryEstimator& memory_estimator) {
  memory_estimator.RegisterMemory(
      absl::container_internal::hashtable_debug_internal::HashtableDebugAccess<
          absl::node_hash_set<T, Eq, Hash, Alloc>>::AllocatedByteSize(*self));
  memory_estimator.RegisterSubobjectsOfElements(self);
}

template <typename K, typename V, typename Eq, typename Hash, typename Alloc>
inline void RiegeliRegisterSubobjects(
    const absl::node_hash_map<K, V, Eq, Hash, Alloc>* self,
    MemoryEstimator& memory_estimator) {
  memory_estimator.RegisterMemory(
      absl::container_internal::hashtable_debug_internal::HashtableDebugAccess<
          absl::node_hash_map<K, V, Eq, Hash,
                              Alloc>>::AllocatedByteSize(*self));
  memory_estimator.RegisterSubobjectsOfElements(self);
}

template <
    typename T,
    std::enable_if_t<std::is_convertible<T*, google::protobuf::Message*>::value,
                     int> = 0>
inline void RiegeliRegisterSubobjects(const T* self,
                                      MemoryEstimator& memory_estimator) {
  memory_estimator.RegisterMemory(self->SpaceUsedLong() - sizeof(T));
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_MEMORY_ESTIMATOR_H_
