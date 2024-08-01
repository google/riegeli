// Copyright 2021 Google LLC
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

#ifndef RIEGELI_BASE_INTRUSIVE_SHARED_PTR_H_
#define RIEGELI_BASE_INTRUSIVE_SHARED_PTR_H_

#include <stddef.h>

#include <cstddef>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/meta/type_traits.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/external_data.h"
#include "riegeli/base/external_ref_support.h"  // IWYU pragma: keep
#include "riegeli/base/initializer.h"
#include "riegeli/base/ownership.h"
#include "riegeli/base/reset.h"

namespace riegeli {

namespace intrusive_shared_ptr_internal {

template <typename T, typename Enable = void>
struct HasHasUniqueOwner : std::false_type {};

template <typename T>
struct HasHasUniqueOwner<
    T, std::enable_if_t<std::is_convertible<
           decltype(std::declval<const T&>().HasUniqueOwner()), bool>::value>>
    : std::true_type {};

}  // namespace intrusive_shared_ptr_internal

// `IntrusiveSharedPtr<T>` implements shared ownership of an object of type `T`.
// It can also be empty, with the pointer being `nullptr`.
//
// The actual object can be of a subtype of `T`, as long as `T::Unref()`
// correctly deletes the object in such a case, which typically requires that
// `T` has a virtual destructor.
//
// `T` maintains its own reference count, e.g. as a member of type `RefCount`.
// `T` should support:
//
// ```
//   // Increments the reference count of `*this`.
//   void Ref() const;
//
//   // Decrements the reference count of `*this`. Deletes `this` when the
//   // reference count reaches 0.
//   void Unref() const;
//
//   // Returns `true` if there is only one owner of the object.
//   //
//   // This can be used to check if the object may be modified.
//   //
//   // Optional. Needed for `IntrusiveSharedPtr::IsUnique()`.
//   bool HasUniqueOwner() const;
// ```
//
// `IntrusiveSharedPtr` has a smaller overhead than `std::shared_ptr` (the
// pointer has 1 word instead of 2, the object typically has 1 word of overhead
// instead of 3), but requires cooperation from `T`, and has fewer features
// (e.g. no weak pointers).
//
// `SharedPtr` is easier to use than `IntrusiveSharedPtr` because `SharedPtr`
// does not require the object to maintain its own reference count, but
// `IntrusiveSharedPtr` supports custom allocation and deallocation, and
// conversion to an `IntrusiveSharedPtr` to a non-leftmost or virtual base
// class. Prefer `SharedPtr` unless `IntrusiveSharedPtr` is needed.
template <typename T>
class
#ifdef ABSL_ATTRIBUTE_TRIVIAL_ABI
    ABSL_ATTRIBUTE_TRIVIAL_ABI
#endif
#ifdef ABSL_NULLABILITY_COMPATIBLE
        ABSL_NULLABILITY_COMPATIBLE
#endif
            IntrusiveSharedPtr : public WithEqual<IntrusiveSharedPtr<T>> {
 public:
  // Creates an empty `IntrusiveSharedPtr`.
  constexpr IntrusiveSharedPtr() = default;
  /*implicit*/ constexpr IntrusiveSharedPtr(std::nullptr_t) noexcept {}
  IntrusiveSharedPtr& operator=(std::nullptr_t) {
    Reset();
    return *this;
  }

  // Creates an `IntrusiveSharedPtr` holding `ptr`.
  //
  // Takes ownership of `ptr` unless the second parameter is `kShareOwnership`.
  explicit IntrusiveSharedPtr(T* ptr, PassOwnership = kPassOwnership) noexcept
      : ptr_(ptr) {}
  explicit IntrusiveSharedPtr(T* ptr, ShareOwnership) noexcept
      : ptr_(Ref(ptr)) {}

  // Creates an `IntrusiveSharedPtr` holding a constructed value.
  //
  // The object is constructed with `new`, which means that `T::Unref()` should
  // delete the object with `delete this`.
  explicit IntrusiveSharedPtr(Initializer<T> value)
      : ptr_(new T(std::move(value))) {}

  // Creates an `IntrusiveSharedPtr` holding a constructed value of a compatible
  // type.
  //
  // The object is constructed with `new`, which means that `T::Unref()` should
  // delete the object with `delete this`.
  template <
      typename SubInitializer,
      std::enable_if_t<
          std::is_convertible<InitializerTargetT<SubInitializer>*, T*>::value,
          int> = 0>
  explicit IntrusiveSharedPtr(SubInitializer&& value)
      : ptr_(new InitializerTargetT<SubInitializer>(
            Initializer<InitializerTargetT<SubInitializer>>(
                std::forward<SubInitializer>(value)))) {}

  // Converts from an `IntrusiveSharedPtr` with a compatible type.
  template <typename SubT,
            std::enable_if_t<std::is_convertible<SubT*, T*>::value, int> = 0>
  /*implicit*/ IntrusiveSharedPtr(const IntrusiveSharedPtr<SubT>& that) noexcept
      : ptr_(Ref(that.ptr_)) {}
  template <typename SubT,
            std::enable_if_t<std::is_convertible<SubT*, T*>::value, int> = 0>
  IntrusiveSharedPtr& operator=(const IntrusiveSharedPtr<SubT>& that) noexcept {
    Unref(std::exchange(ptr_, Ref(that.ptr_)));
    return *this;
  }

  // Converts from an `IntrusiveSharedPtr` with a compatible type.
  //
  // The source `IntrusiveSharedPtr` is left empty.
  template <typename SubT,
            std::enable_if_t<std::is_convertible<SubT*, T*>::value, int> = 0>
  /*implicit*/ IntrusiveSharedPtr(IntrusiveSharedPtr<SubT>&& that) noexcept
      : ptr_(that.Release()) {}
  template <typename SubT,
            std::enable_if_t<std::is_convertible<SubT*, T*>::value, int> = 0>
  IntrusiveSharedPtr& operator=(IntrusiveSharedPtr<SubT>&& that) noexcept {
    Unref(std::exchange(ptr_, that.Release()));
    return *this;
  }

  IntrusiveSharedPtr(const IntrusiveSharedPtr& that) noexcept
      : ptr_(Ref(that.ptr_)) {}
  IntrusiveSharedPtr& operator=(const IntrusiveSharedPtr& that) noexcept {
    Unref(std::exchange(ptr_, Ref(that.ptr_)));
    return *this;
  }

  // The source `IntrusiveSharedPtr` is left empty.
  IntrusiveSharedPtr(IntrusiveSharedPtr&& that) noexcept
      : ptr_(that.Release()) {}
  IntrusiveSharedPtr& operator=(IntrusiveSharedPtr&& that) noexcept {
    Unref(std::exchange(ptr_, that.Release()));
    return *this;
  }

  ~IntrusiveSharedPtr() { Unref(ptr_); }

  // Replaces the object, or makes `*this` empty if `ptr == nullptr`.
  //
  // Takes ownership of `ptr` unless the second parameter is `kShareOwnership`.
  //
  // The old object, if any, is destroyed afterwards.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(T* ptr = nullptr,
                                          PassOwnership = kPassOwnership) {
    Unref(std::exchange(ptr_, ptr));
  }
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(T* ptr, ShareOwnership) {
    Unref(std::exchange(ptr_, Ref(ptr)));
  }

  // Replaces the object with a constructed value.
  //
  // The old object, if any, is destroyed afterwards.
  //
  // The object is constructed with `new`, which means that `T::Unref()` should
  // delete the object with `delete this`.
  //
  // If `T` supports `HasUniqueOwner()` and `*this` is the only owner of an
  // object known to have the same move-assignable type, the existing object is
  // assigned or reset instead of allocating and constructing a new object.
  ABSL_ATTRIBUTE_REINITIALIZES
  void Reset(Initializer<T> value) { ResetImpl(std::move(value)); }

  // Replaces the object with a constructed value of a compatible type.
  //
  // The old object, if any, is destroyed afterwards.
  //
  // The object is constructed with `new`, which means that `T::Unref()` should
  // delete the object with `delete this`.
  template <
      typename SubInitializer,
      std::enable_if_t<
          std::is_convertible<InitializerTargetT<SubInitializer>*, T*>::value,
          int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(SubInitializer&& value) {
    Unref(
        std::exchange(ptr_, new InitializerTargetT<SubInitializer>(
                                Initializer<InitializerTargetT<SubInitializer>>(
                                    std::forward<SubInitializer>(value)))));
  }

  // Returns `true` if `*this` is the only owner of the object.
  //
  // This can be used to check if the object may be modified (in contrast to
  // `std::shared_ptr::unique()`).
  //
  // If `*this` is empty, returns `false`.
  //
  // Supported if `T` supports `HasUniqueOwner()`.
  template <typename DependentT = T,
            std::enable_if_t<intrusive_shared_ptr_internal::HasHasUniqueOwner<
                                 DependentT>::value,
                             int> = 0>
  bool IsUnique() const {
    return ptr_ != nullptr && ptr_->HasUniqueOwner();
  }

  // Returns the pointer.
  T* get() const { return ptr_; }

  // Dereferences the pointer.
  T& operator*() const {
    RIEGELI_ASSERT(ptr_ != nullptr)
        << "Failed precondition of IntrusiveSharedPtr::operator*: null pointer";
    return *ptr_;
  }
  T* operator->() const {
    RIEGELI_ASSERT(ptr_ != nullptr)
        << "Failed precondition of IntrusiveSharedPtr::operator->: null "
           "pointer";
    return ptr_;
  }

  // Returns the pointer. This `IntrusiveSharedPtr` is left empty.
  T* Release() { return std::exchange(ptr_, nullptr); }

  template <typename OtherT>
  friend bool operator==(const IntrusiveSharedPtr& a,
                         const IntrusiveSharedPtr<OtherT>& b) {
    return a.get() == b.get();
  }
  friend bool operator==(const IntrusiveSharedPtr& a, std::nullptr_t) {
    return a.get() == nullptr;
  }

  // Allow Nullability annotations on `IntrusiveSharedPtr`.
  using absl_nullability_compatible = void;

  // Indicate support for:
  //  * `ExternalRef(const IntrusiveSharedPtr&, substr)`
  //  * `ExternalRef(IntrusiveSharedPtr&&, substr)`
  friend void RiegeliSupportsExternalRef(const IntrusiveSharedPtr*) {}

  // Support `ExternalRef`.
  friend size_t RiegeliExternalMemory(const IntrusiveSharedPtr* self) {
    if (*self == nullptr) return 0;
    return sizeof(T) + RiegeliExternalMemory(self->get());
  }

  // Support `ExternalRef`.
  friend ExternalStorage RiegeliToExternalStorage(IntrusiveSharedPtr* self) {
    return ExternalStorage(const_cast<std::remove_cv_t<T>*>(self->Release()),
                           [](void* ptr) { Unref(static_cast<T*>(ptr)); });
  }

  // Support `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const IntrusiveSharedPtr* self,
                                        MemoryEstimator& memory_estimator) {
    if (memory_estimator.RegisterNode(self->get())) {
      memory_estimator.RegisterDynamicObject(self->get());
    }
  }

 private:
  // For converting from a `SharedPtr` with a compatible type.
  template <typename SubT>
  friend class IntrusiveSharedPtr;

  template <typename SubT>
  static SubT* Ref(SubT* ptr) {
    if (ptr != nullptr) ptr->Ref();
    return ptr;
  }

  static void Unref(T* ptr) {
    if (ptr != nullptr) ptr->Unref();
  }

  template <
      typename DependentT = T,
      std::enable_if_t<
          absl::conjunction<
              intrusive_shared_ptr_internal::HasHasUniqueOwner<DependentT>,
              absl::disjunction<
                  absl::negation<std::has_virtual_destructor<DependentT>>,
                  std::is_final<DependentT>>,
              std::is_move_assignable<DependentT>>::value,
          int> = 0>
  void ResetImpl(Initializer<T> value) {
    if (IsUnique()) {
      riegeli::Reset(*ptr_, std::move(value));
      return;
    }
    Unref(std::exchange(ptr_, new T(std::move(value))));
  }
  template <
      typename DependentT = T,
      std::enable_if_t<
          absl::disjunction<
              absl::negation<
                  intrusive_shared_ptr_internal::HasHasUniqueOwner<DependentT>>,
              absl::conjunction<std::has_virtual_destructor<DependentT>,
                                absl::negation<std::is_final<DependentT>>>,
              absl::negation<std::is_move_assignable<DependentT>>>::value,
          int> = 0>
  void ResetImpl(Initializer<T> value) {
    Unref(std::exchange(ptr_, new T(std::move(value))));
  }

  T* ptr_ = nullptr;
};

#if __cpp_deduction_guides
template <typename T>
explicit IntrusiveSharedPtr(T* ptr, PassOwnership = kPassOwnership)
    -> IntrusiveSharedPtr<T>;
template <typename T>
explicit IntrusiveSharedPtr(T* ptr, ShareOwnership) -> IntrusiveSharedPtr<T>;
template <typename T, std::enable_if_t<!std::is_pointer<T>::value, int> = 0>
explicit IntrusiveSharedPtr(T&& value)
    -> IntrusiveSharedPtr<InitializerTargetT<T>>;
#endif

}  // namespace riegeli

#endif  // RIEGELI_BASE_INTRUSIVE_SHARED_PTR_H_
