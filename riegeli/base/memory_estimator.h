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
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>
#include <typeindex>
#include <typeinfo>  // IWYU pragma: keep
#include <utility>
#include <variant>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/container/inlined_vector.h"
#include "absl/container/node_hash_map.h"
#include "absl/container/node_hash_set.h"
#include "absl/meta/type_traits.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/estimated_allocated_size.h"

namespace google::protobuf {
class Message;
}  // namespace google::protobuf

namespace riegeli {

class MemoryEstimator;

namespace memory_estimator_internal {

template <typename T, typename Enable = void>
struct HasRiegeliRegisterSubobjects : std::false_type {};

template <typename T>
struct HasRiegeliRegisterSubobjects<
    T, absl::void_t<decltype(RiegeliRegisterSubobjects(
           std::declval<const T*>(), std::declval<MemoryEstimator&>()))>>
    : std::true_type {};

}  // namespace memory_estimator_internal

// Estimates the amount of memory owned by multiple objects.
class MemoryEstimator {
 public:
  // Determines whether `RegisterSubobjects(const T*)` might be considered good:
  // either the corresponding `RiegeliRegisterSubobjects()` is defined, or `T`
  // is trivially destructible so the default definition which does nothing is
  // likely appropriate.
  //
  // This is not necessarily accurate, in particular traversing a `T` might
  // encounter types which do not have a good estimation, but if this yields
  // `false`, then `RegisterSubobjects()` is most likely an underestimation and
  // the caller might need some different way to estimate memory.
  template <typename T>
  struct RegisterSubobjectsIsGood
      : absl::disjunction<
            memory_estimator_internal::HasRiegeliRegisterSubobjects<T>,
            std::is_trivially_destructible<T>> {};

  // Determines whether `RegisterSubobjects(const T*)` definitely does nothing:
  // the corresponding `RiegeliRegisterSubobjects()` is not defined and `T` is
  // trivially destructible.
  //
  // This can be used to skip a loop over elements of type `T`.
  template <typename T>
  struct RegisterSubobjectsIsTrivial
      : absl::conjunction<
            absl::negation<
                memory_estimator_internal::HasRiegeliRegisterSubobjects<T>>,
            std::is_trivially_destructible<T>> {};

  MemoryEstimator() = default;

  MemoryEstimator(const MemoryEstimator&) = delete;
  MemoryEstimator& operator=(const MemoryEstimator&) = delete;

  virtual ~MemoryEstimator() = default;

  // Registers the given amount of memory as used.
  void RegisterMemory(size_t memory) {
    total_memory_ = SaturatingAdd(total_memory_, memory);
  }

  // Registers the given length of a block of dynamically allocated memory as
  // used. The length should correspond to a single allocation. The actual
  // registered amount includes estimated overhead of the memory allocator.
  //
  // If the address of the allocated memory is provided, it might be used for a
  // better estimation.
  void RegisterDynamicMemory(const void* ptr, size_t memory) {
    RegisterDynamicMemoryImpl(ptr, memory);
  }
  void RegisterDynamicMemory(size_t memory) {
    RegisterDynamicMemoryImpl(memory);
  }

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
  bool RegisterNode(const void* ptr) { return RegisterNodeImpl(ptr); }

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
  //  * `T[size]`
  //  * `std::unique_ptr<T, Deleter>`
  //  * `std::shared_ptr<T>`
  //  * `std::basic_string<Char, Traits, Alloc>` (`Alloc` is ignored)
  //  * `absl::Cord`
  //  * `std::optional<T>`
  //  * `std::variant<T...>`
  //  * `std::pair<T, U>`
  //  * `std::tuple<T...>`
  //  * `std::array<T, size>`
  //  * `std::vector<T, Alloc>` (`Alloc` is ignored)
  //  * `absl::InlinedVector<T, N, Alloc>` (`Alloc` is ignored)
  //  * `absl::flat_hash_set<T, Eq, Hash, Alloc>` (`Alloc` is ignored)
  //  * `absl::flat_hash_map<K, V, Eq, Hash, Alloc>` (`Alloc` is ignored)
  //  * `absl::node_hash_set<T, Eq, Hash, Alloc>` (`Alloc` is ignored)
  //  * `absl::node_hash_map<K, V, Eq, Hash, Alloc>` (`Alloc` is ignored)
  //  * `google::protobuf::Message`
  template <typename T>
  void RegisterSubobjects(const T* object);
  template <typename T, std::enable_if_t<std::is_reference_v<T>, int> = 0>
  void RegisterSubobjects(const std::remove_reference_t<T>* object);

  // Registers each element of a range.
  template <typename Iterator>
  void RegisterSubobjects(Iterator begin, Iterator end);

  // A shortcut for `RegisterDynamicMemory(object, DynamicSizeOf(object))`
  // followed by `RegisterSubobjects(object)`.
  template <typename T>
  void RegisterDynamicObject(const T* object);

  // Returns the total amount of memory added.
  size_t TotalMemory() const { return total_memory_; }

 protected:
  virtual void RegisterDynamicMemoryImpl(const void* ptr, size_t memory) = 0;
  virtual void RegisterDynamicMemoryImpl(size_t memory) = 0;
  virtual bool RegisterNodeImpl(const void* ptr) = 0;
  virtual void RegisterUnknownTypeImpl() = 0;
  virtual void RegisterUnknownTypeImpl(std::type_index index) = 0;

 private:
  size_t total_memory_ = 0;
};

// A `MemoryEstimator` which gives a pretty good estimate for known types.
//
//  * Includes memory allocator overhead.
//  * Takes object sharing into account.
//  * Does not report unknown types.
class MemoryEstimatorDefault : public MemoryEstimator {
 public:
  MemoryEstimatorDefault() = default;

  MemoryEstimatorDefault(const MemoryEstimatorDefault& that) = delete;
  MemoryEstimatorDefault& operator=(const MemoryEstimatorDefault& that) =
      delete;

 protected:
  void RegisterDynamicMemoryImpl(const void* ptr, size_t memory) override {
    RegisterMemory(EstimatedAllocatedSize(ptr, memory));
  }
  void RegisterDynamicMemoryImpl(size_t memory) override {
    RegisterMemory(EstimatedAllocatedSize(memory));
  }
  bool RegisterNodeImpl(const void* ptr) override;
  void RegisterUnknownTypeImpl() override {}
  void RegisterUnknownTypeImpl(std::type_index index) override {}

 private:
  absl::flat_hash_set<const void*> objects_seen_;
};

// A faster but less accurate `MemoryEstimator`.
//
//  * Does not include memory allocator overhead.
//  * Does not take object sharing into account (if an object is shared then
//    it is counted multiple times).
//  * Does not report unknown types.
class MemoryEstimatorSimplified : public MemoryEstimator {
 public:
  MemoryEstimatorSimplified() = default;

  MemoryEstimatorSimplified(const MemoryEstimatorSimplified&) = delete;
  MemoryEstimatorSimplified& operator=(const MemoryEstimatorSimplified&) =
      delete;

 protected:
  void RegisterDynamicMemoryImpl(const void* ptr, size_t memory) override {
    RegisterMemory(memory);
  }
  void RegisterDynamicMemoryImpl(size_t memory) override {
    RegisterMemory(memory);
  }
  bool RegisterNodeImpl(const void* ptr) override { return ptr != nullptr; }
  void RegisterUnknownTypeImpl() override {}
  void RegisterUnknownTypeImpl(std::type_index index) override {}
};

// A `MemoryEstimator` which can report encountered types for which
// `RegisterSubobjects()` is not customized and which are not trivially
// destructible, indicating whether interesting customizations are missing and
// results are underestimated. Otherwise behaves like `MemoryEstimatorDefault`.
//
//  * Includes memory allocator overhead.
//  * Takes object sharing into account.
//  * Reports unknown types.
class MemoryEstimatorReportingUnknownTypes : public MemoryEstimatorDefault {
 public:
  MemoryEstimatorReportingUnknownTypes() = default;

  MemoryEstimatorReportingUnknownTypes(
      const MemoryEstimatorReportingUnknownTypes&) = delete;
  MemoryEstimatorReportingUnknownTypes& operator=(
      const MemoryEstimatorReportingUnknownTypes&) = delete;

  // Returns names of encountered types for which `RegisterSubobjects()` is not
  // customized and which are not trivially destructible. If the result is not
  // empty, this likely indicates that interesting customizations are missing
  // and results are underestimated.
  //
  // If RTTI is not available, "<no rtti>" is returned as a placeholder.
  std::vector<std::string> UnknownTypes() const;

 protected:
  void RegisterUnknownTypeImpl() override;
  void RegisterUnknownTypeImpl(std::type_index index) override;

 private:
  bool unknown_types_no_rtti_ = false;
  absl::flat_hash_set<std::type_index> unknown_types_;
};

// Uses `MemoryEstimatorDefault` to estimate memory owned by a single object
// and its subobjects, including `sizeof` the original object.
template <typename T>
inline size_t EstimateMemory(const T& object) {
  MemoryEstimatorDefault memory_estimator;
  memory_estimator.RegisterMemory(MemoryEstimator::DynamicSizeOf(&object));
  memory_estimator.RegisterSubobjects(&object);
  return memory_estimator.TotalMemory();
}

// Uses `MemoryEstimatorSimplified` to estimate memory owned by a single object
// and its subobjects, including `sizeof` the original object.
template <typename T>
inline size_t EstimateMemorySimplified(const T& object) {
  MemoryEstimatorSimplified memory_estimator;
  memory_estimator.RegisterMemory(MemoryEstimator::DynamicSizeOf(&object));
  memory_estimator.RegisterSubobjects(&object);
  return memory_estimator.TotalMemory();
}

// Uses `MemoryEstimatorReportingUnknownTypes` to estimate memory owned by a
// single object and its subobjects, including `sizeof` the original object, and
// unknown types.

struct TotalMemoryWithUnknownTypes {
  size_t total_memory;
  std::vector<std::string> unknown_types;
};

template <typename T>
inline TotalMemoryWithUnknownTypes EstimateMemoryReportingUnknownTypes(
    const T& object) {
  MemoryEstimatorReportingUnknownTypes memory_estimator;
  memory_estimator.RegisterMemory(MemoryEstimator::DynamicSizeOf(&object));
  memory_estimator.RegisterSubobjects(&object);
  return TotalMemoryWithUnknownTypes{memory_estimator.TotalMemory(),
                                     memory_estimator.UnknownTypes()};
}

// Implementation details follow.

template <typename T>
inline void MemoryEstimator::RegisterUnknownType() {
  RegisterUnknownTypeImpl(
#if __cpp_rtti
      std::type_index(typeid(T))
#endif
  );
}

namespace memory_estimator_internal {

template <typename T, typename Enable = void>
struct HasRiegeliDynamicSizeOf : std::false_type {};

template <typename T>
struct HasRiegeliDynamicSizeOf<
    T, std::enable_if_t<std::is_convertible_v<
           decltype(RiegeliDynamicSizeOf(std::declval<const T*>())), size_t>>>
    : std::true_type {};

template <typename T,
          std::enable_if_t<HasRiegeliDynamicSizeOf<T>::value, int> = 0>
inline size_t DynamicSizeOf(const T* object) {
  return RiegeliDynamicSizeOf(object);
}
template <typename T,
          std::enable_if_t<!HasRiegeliDynamicSizeOf<T>::value, int> = 0>
inline size_t DynamicSizeOf(ABSL_ATTRIBUTE_UNUSED const T* object) {
  return sizeof(T);
}

template <typename T,
          std::enable_if_t<HasRiegeliRegisterSubobjects<T>::value, int> = 0>
inline void RegisterSubobjects(const T* object,
                               MemoryEstimator& memory_estimator) {
  RiegeliRegisterSubobjects(object, memory_estimator);
}
template <typename T,
          std::enable_if_t<
              absl::conjunction<absl::negation<HasRiegeliRegisterSubobjects<T>>,
                                std::is_trivially_destructible<T>>::value,
              int> = 0>
inline void RegisterSubobjects(
    ABSL_ATTRIBUTE_UNUSED const T* object,
    ABSL_ATTRIBUTE_UNUSED MemoryEstimator& memory_estimator) {}
template <typename T,
          std::enable_if_t<
              absl::conjunction<
                  absl::negation<HasRiegeliRegisterSubobjects<T>>,
                  absl::negation<std::is_trivially_destructible<T>>>::value,
              int> = 0>
inline void RegisterSubobjects(ABSL_ATTRIBUTE_UNUSED const T* object,
                               MemoryEstimator& memory_estimator) {
  memory_estimator.RegisterUnknownType<T>();
}

}  // namespace memory_estimator_internal

template <typename T>
inline size_t MemoryEstimator::DynamicSizeOf(const T* object) {
  return memory_estimator_internal::DynamicSizeOf(object);
}

template <typename T>
inline void MemoryEstimator::RegisterSubobjects(const T* object) {
  memory_estimator_internal::RegisterSubobjects(object, *this);
}

template <typename T, std::enable_if_t<std::is_reference_v<T>, int>>
inline void MemoryEstimator::RegisterSubobjects(
    ABSL_ATTRIBUTE_UNUSED const std::remove_reference_t<T>* object) {}

template <typename Iterator>
inline void MemoryEstimator::RegisterSubobjects(Iterator begin, Iterator end) {
  if (!RegisterSubobjectsIsTrivial<
          typename std::iterator_traits<Iterator>::value_type>::value) {
    for (; begin != end; ++begin) RegisterSubobjects(&*begin);
  }
}

template <typename T>
inline void MemoryEstimator::RegisterDynamicObject(const T* object) {
  RegisterDynamicMemory(object, MemoryEstimator::DynamicSizeOf(object));
  RegisterSubobjects(object);
}

template <typename T, size_t size>
inline void RiegeliRegisterSubobjects(const T (*self)[size],
                                      MemoryEstimator& memory_estimator) {
  memory_estimator.RegisterSubobjects(*self + 0, *self + size);
}

template <
    typename T, typename Deleter,
    std::enable_if_t<absl::conjunction<absl::negation<std::is_void<T>>,
                                       absl::negation<std::is_array<T>>>::value,
                     int> = 0>
inline void RiegeliRegisterSubobjects(const std::unique_ptr<T, Deleter>* self,
                                      MemoryEstimator& memory_estimator) {
  memory_estimator.RegisterSubobjects<Deleter>(&self->get_deleter());
  if (*self != nullptr) memory_estimator.RegisterDynamicObject(self->get());
}

namespace memory_estimator_internal {

// Reflects the layout of a control block of `std::shared_ptr` from libc++.
struct SharedPtrControlBlock {
  virtual ~SharedPtrControlBlock() = default;
  long shared_count;
  long weak_count;
};

}  // namespace memory_estimator_internal

template <
    typename T,
    std::enable_if_t<absl::conjunction<absl::negation<std::is_void<T>>,
                                       absl::negation<std::is_array<T>>>::value,
                     int> = 0>
inline void RiegeliRegisterSubobjects(const std::shared_ptr<T>* self,
                                      MemoryEstimator& memory_estimator) {
  if (memory_estimator.RegisterNode(self->get())) {
    memory_estimator.RegisterDynamicMemory(
        sizeof(memory_estimator_internal::SharedPtrControlBlock) +
        MemoryEstimator::DynamicSizeOf(&**self));
    memory_estimator.RegisterSubobjects(&**self);
  }
}

template <typename T, size_t size>
inline void RiegeliRegisterSubobjects(const std::shared_ptr<T[size]>* self,
                                      MemoryEstimator& memory_estimator) {
  if (memory_estimator.RegisterNode(self->get())) {
    memory_estimator.RegisterDynamicMemory(
        sizeof(memory_estimator_internal::SharedPtrControlBlock) +
        sizeof(T[size]));
    memory_estimator.RegisterSubobjects(self->get(), self->get() + size);
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
inline void RiegeliRegisterSubobjects(const std::optional<T>* self,
                                      MemoryEstimator& memory_estimator) {
  if (*self != std::nullopt) memory_estimator.RegisterSubobjects(&**self);
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
inline void RiegeliRegisterSubobjects(const std::variant<T...>* self,
                                      MemoryEstimator& memory_estimator) {
  std::visit(
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
  memory_estimator.RegisterSubobjects(self->cbegin(), self->cend());
}

template <typename T, typename Alloc>
inline void RiegeliRegisterSubobjects(const std::vector<T, Alloc>* self,
                                      MemoryEstimator& memory_estimator) {
  if (self->capacity() > 0) {
    memory_estimator.RegisterDynamicMemory(self->capacity() * sizeof(T));
    memory_estimator.RegisterSubobjects(self->cbegin(), self->cend());
  }
}

template <typename Alloc>
inline void RiegeliRegisterSubobjects(const std::vector<bool, Alloc>* self,
                                      MemoryEstimator& memory_estimator) {
  if (self->capacity() > 0) {
    memory_estimator.RegisterDynamicMemory(self->capacity() / 8);
  }
}

template <typename T, size_t N, typename Alloc>
inline void RiegeliRegisterSubobjects(
    const absl::InlinedVector<T, N, Alloc>* self,
    MemoryEstimator& memory_estimator) {
  if (self->capacity() > N) {
    memory_estimator.RegisterDynamicMemory(self->capacity() * sizeof(T));
  }
  memory_estimator.RegisterSubobjects(self->cbegin(), self->cend());
}

template <typename T, typename Eq, typename Hash, typename Alloc>
inline void RiegeliRegisterSubobjects(
    const absl::flat_hash_set<T, Eq, Hash, Alloc>* self,
    MemoryEstimator& memory_estimator) {
  memory_estimator.RegisterMemory(
      absl::container_internal::hashtable_debug_internal::HashtableDebugAccess<
          absl::flat_hash_set<T, Eq, Hash, Alloc>>::AllocatedByteSize(*self));
  memory_estimator.RegisterSubobjects(self->cbegin(), self->cend());
}

template <typename K, typename V, typename Eq, typename Hash, typename Alloc>
inline void RiegeliRegisterSubobjects(
    const absl::flat_hash_map<K, V, Eq, Hash, Alloc>* self,
    MemoryEstimator& memory_estimator) {
  memory_estimator.RegisterMemory(
      absl::container_internal::hashtable_debug_internal::HashtableDebugAccess<
          absl::flat_hash_map<K, V, Eq, Hash,
                              Alloc>>::AllocatedByteSize(*self));
  memory_estimator.RegisterSubobjects(self->cbegin(), self->cend());
}

template <typename T, typename Eq, typename Hash, typename Alloc>
inline void RiegeliRegisterSubobjects(
    const absl::node_hash_set<T, Eq, Hash, Alloc>* self,
    MemoryEstimator& memory_estimator) {
  memory_estimator.RegisterMemory(
      absl::container_internal::hashtable_debug_internal::HashtableDebugAccess<
          absl::node_hash_set<T, Eq, Hash, Alloc>>::AllocatedByteSize(*self));
  memory_estimator.RegisterSubobjects(self->cbegin(), self->cend());
}

template <typename K, typename V, typename Eq, typename Hash, typename Alloc>
inline void RiegeliRegisterSubobjects(
    const absl::node_hash_map<K, V, Eq, Hash, Alloc>* self,
    MemoryEstimator& memory_estimator) {
  memory_estimator.RegisterMemory(
      absl::container_internal::hashtable_debug_internal::HashtableDebugAccess<
          absl::node_hash_map<K, V, Eq, Hash,
                              Alloc>>::AllocatedByteSize(*self));
  memory_estimator.RegisterSubobjects(self->cbegin(), self->cend());
}

template <typename T,
          std::enable_if_t<
              std::is_convertible_v<T*, google::protobuf::Message*>, int> = 0>
inline void RiegeliRegisterSubobjects(const T* self,
                                      MemoryEstimator& memory_estimator) {
  memory_estimator.RegisterMemory(self->SpaceUsedLong() - sizeof(T));
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_MEMORY_ESTIMATOR_H_
