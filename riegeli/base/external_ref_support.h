// Copyright 2024 Google LLC
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

#ifndef RIEGELI_BASE_EXTERNAL_REF_SUPPORT_H_
#define RIEGELI_BASE_EXTERNAL_REF_SUPPORT_H_

#include <stddef.h>

#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/meta/type_traits.h"
#include "riegeli/base/external_data.h"
#include "riegeli/base/to_string_view.h"

namespace riegeli {

// Default implementation for `ExternalRef` support.
inline bool RiegeliExternalCopy(ABSL_ATTRIBUTE_UNUSED const void* self) {
  return false;
}

// Default implementation for `ExternalRef` support.
inline size_t RiegeliExternalMemory(ABSL_ATTRIBUTE_UNUSED const void* self) {
  return 0;
}

// Indicate support for `ExternalRef(std::string&&)`.
void RiegeliSupportsExternalRefWhole(std::string*);

inline size_t RiegeliExternalMemory(const std::string* self) {
  // Do not bother checking for short string optimization. Such strings will
  // likely not be considered wasteful anyway.
  return self->capacity() + 1;
}

// Indicate support for:
//  * `ExternalRef(std::vector<char>&&)`
//  * `ExternalRef(std::vector<T>&&, substr)`
template <typename T>
void RiegeliSupportsExternalRef(std::vector<T>*);

template <typename T>
inline size_t RiegeliExternalMemory(const std::vector<T>* self) {
  return self->capacity() * sizeof(T);
}

// Indicate support for `ExternalRef(std::unique_ptr<T, Deleter>&&, substr)`.
template <typename T, typename Deleter>
void RiegeliSupportsExternalRef(std::unique_ptr<T, Deleter>*);

template <
    typename T, typename Deleter,
    std::enable_if_t<absl::conjunction<absl::negation<std::is_void<T>>,
                                       absl::negation<std::is_array<T>>>::value,
                     int> = 0>
inline size_t RiegeliExternalMemory(const std::unique_ptr<T, Deleter>* self) {
  size_t memory = RiegeliExternalMemory(&self->get_deleter());
  if (*self != nullptr) {
    memory += sizeof(T) + RiegeliExternalMemory(self->get());
  }
  return memory;
}

template <typename T>
inline ExternalStorage RiegeliToExternalStorage(std::unique_ptr<T>* self) {
  return ExternalStorage(const_cast<std::remove_cv_t<T>*>(self->release()),
                         [](void* ptr) { delete static_cast<T*>(ptr); });
}

template <typename T>
inline ExternalStorage RiegeliToExternalStorage(std::unique_ptr<T[]>* self) {
  return ExternalStorage(const_cast<std::remove_cv_t<T>*>(self->release()),
                         [](void* ptr) { delete[] static_cast<T*>(ptr); });
}

// Indicate support for:
//  * `ExternalRef(const std::shared_ptr<T>&, substr)`
//  * `ExternalRef(std::shared_ptr<T>&&, substr)`
template <typename T>
void RiegeliSupportsExternalRef(const std::shared_ptr<T>*);

namespace external_ref_internal {

// Reflects the layout of a control block of `std::shared_ptr` from libc++.
struct SharedPtrControlBlock {
  virtual ~SharedPtrControlBlock() = default;
  long shared_count;
  long weak_count;
};

}  // namespace external_ref_internal

template <
    typename T, typename Deleter,
    std::enable_if_t<absl::conjunction<absl::negation<std::is_void<T>>,
                                       absl::negation<std::is_array<T>>>::value,
                     int> = 0>
inline size_t RiegeliExternalMemory(const std::shared_ptr<T>* self) {
  if (*self == nullptr) return 0;
  return sizeof(external_ref_internal::SharedPtrControlBlock) + sizeof(T) +
         RiegeliExternalMemory(self->get());
}

template <typename T, size_t size>
inline size_t RiegeliExternalMemory(const std::shared_ptr<T[size]>* self) {
  if (*self == nullptr) return 0;
  size_t memory =
      sizeof(external_ref_internal::SharedPtrControlBlock) + sizeof(T[size]);
  for (size_t i = 0; i < size; ++i) {
    memory += RiegeliExternalMemory(&(*self)[i]);
  }
  return memory;
}

namespace external_ref_internal {

template <typename T>
struct PointerType {
  using type = T*;
};
template <typename T>
struct PointerType<T&> {
  using type = const T*;
};
template <typename T>
struct PointerType<T&&> {
  using type = T*;
};

template <typename T>
using PointerTypeT = typename PointerType<T>::type;

template <typename T, typename Enable = void>
struct HasRiegeliSupportsExternalRefWhole : std::false_type {};

template <typename T>
struct HasRiegeliSupportsExternalRefWhole<
    T, absl::void_t<decltype(RiegeliSupportsExternalRefWhole(
           std::declval<PointerTypeT<T>>()))>> : std::true_type {};

template <typename T, typename Enable = void>
struct HasRiegeliSupportsExternalRef : std::false_type {};

template <typename T>
struct HasRiegeliSupportsExternalRef<
    T, absl::void_t<decltype(RiegeliSupportsExternalRef(
           std::declval<PointerTypeT<T>>()))>> : std::true_type {};

}  // namespace external_ref_internal

template <typename T>
struct SupportsExternalRefWhole
    : absl::conjunction<
          absl::disjunction<
              external_ref_internal::HasRiegeliSupportsExternalRefWhole<T>,
              external_ref_internal::HasRiegeliSupportsExternalRef<T>>,
          SupportsToStringView<T>> {};

template <typename T>
struct SupportsExternalRefSubstr
    : external_ref_internal::HasRiegeliSupportsExternalRef<T> {};

}  // namespace riegeli

#endif  // RIEGELI_BASE_EXTERNAL_REF_SUPPORT_H_
