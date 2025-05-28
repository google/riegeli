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
#include "riegeli/base/bytes_ref.h"
#include "riegeli/base/external_data.h"

namespace riegeli {

// Default implementation for `ExternalRef` support.
inline bool RiegeliExternalCopy(ABSL_ATTRIBUTE_UNUSED const void* self) {
  return false;
}

// Indicates support for `ExternalRef(std::string&&)`.
void RiegeliSupportsExternalRefWhole(std::string*);

// Indicates support for:
//  * `ExternalRef(std::vector<char>&&)`
//  * `ExternalRef(std::vector<T>&&, substr)`
template <typename T>
void RiegeliSupportsExternalRef(std::vector<T>*);

// Indicates support for `ExternalRef(std::unique_ptr<T, Deleter>&&, substr)`.
template <typename T, typename Deleter>
void RiegeliSupportsExternalRef(std::unique_ptr<T, Deleter>*);

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

// Indicates support for:
//  * `ExternalRef(const std::shared_ptr<T>&, substr)`
//  * `ExternalRef(std::shared_ptr<T>&&, substr)`
template <typename T>
void RiegeliSupportsExternalRef(const std::shared_ptr<T>*);

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
    T, std::void_t<decltype(RiegeliSupportsExternalRefWhole(
           std::declval<PointerTypeT<T>>()))>> : std::true_type {};

template <typename T, typename Enable = void>
struct HasRiegeliSupportsExternalRef : std::false_type {};

template <typename T>
struct HasRiegeliSupportsExternalRef<
    T, std::void_t<decltype(RiegeliSupportsExternalRef(
           std::declval<PointerTypeT<T>>()))>> : std::true_type {};

}  // namespace external_ref_internal

template <typename T>
struct SupportsExternalRefWhole
    : std::conjunction<
          std::disjunction<
              external_ref_internal::HasRiegeliSupportsExternalRefWhole<T>,
              external_ref_internal::HasRiegeliSupportsExternalRef<T>>,
          std::is_convertible<const T&, BytesRef>> {};

template <typename T>
struct SupportsExternalRefSubstr
    : external_ref_internal::HasRiegeliSupportsExternalRef<T> {};

}  // namespace riegeli

#endif  // RIEGELI_BASE_EXTERNAL_REF_SUPPORT_H_
