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

#ifndef RIEGELI_BASE_TYPE_ERASED_REF_H_
#define RIEGELI_BASE_TYPE_ERASED_REF_H_

#include <cstddef>
#include <memory>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/meta/type_traits.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/type_traits.h"

namespace riegeli {

// `TypeErasedRef` wraps a reference, and allows to recover the original
// reference as long as its original type is provided.
//
// `TypeErasedRef(std::forward<T>(value)).Cast<T>()` recovers the value of
// `std::forward<T>(value)`.
//
// This is like converting the reference to a pointer and casting it to `void*`,
// and casting back for recovery, but this correctly handles const references
// and references to functions.
//
// Specifying `T` or `T&&` for recovery is interchangeable.
class ABSL_NULLABILITY_COMPATIBLE TypeErasedRef
    : public WithEqual<TypeErasedRef> {
 private:
  using pointer = void*;  // For `ABSL_NULLABILITY_COMPATIBLE`.

  template <typename T>
  struct IsFunctionRef : std::false_type {};

  template <typename T>
  struct IsFunctionRef<T&> : std::is_function<T> {};

 public:
  // Creates an empty `TypeErasedRef`.
  TypeErasedRef() = default;
  // There is no conversion from `std::nullptr_t` because that should bind to a
  // reference to `nullptr` instead of being empty.

  // Wraps `std::forward<T>(value)`.
  template <typename T, std::enable_if_t<
                            std::conjunction_v<NotSameRef<TypeErasedRef, T>,
                                               std::negation<IsFunctionRef<T>>>,
                            int> = 0>
  explicit TypeErasedRef(T&& value ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : ptr_(const_cast<absl::remove_cvref_t<T>*>(std::addressof(value))) {}

  // Wraps a function reference.
  //
  // The implementation relies on the assumption that a function pointer can be
  // `reinterpret_cast` to `void*` and back.
  template <typename T,
            std::enable_if_t<
                // `NotSameRef` is not needed because `T` is a function
                // reference, so it is never `TypeErasedRef`.
                IsFunctionRef<T>::value, int> = 0>
  explicit TypeErasedRef(T&& value) : ptr_(reinterpret_cast<void*>(&value)) {}

  TypeErasedRef(const TypeErasedRef& that) = default;
  TypeErasedRef& operator=(const TypeErasedRef& that) = default;

  // Recovers the `T&&`.
  template <typename T, std::enable_if_t<!IsFunctionRef<T>::value, int> = 0>
  T&& Cast() const {
    return std::forward<T>(
        *reinterpret_cast<std::remove_reference_t<T>*>(ptr_));
  }

  // Recovers a function reference.
  template <typename T, std::enable_if_t<IsFunctionRef<T>::value, int> = 0>
  T&& Cast() const {
    return *reinterpret_cast<std::remove_reference_t<T>*>(ptr_);
  }

  friend bool operator==(TypeErasedRef a, std::nullptr_t) {
    return a.ptr_ == nullptr;
  }

 private:
  void* ptr_ = nullptr;
};

}  // namespace riegeli

#endif  // RIEGELI_BASE_TYPE_ERASED_REF_H_
