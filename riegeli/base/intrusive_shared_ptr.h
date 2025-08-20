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
#include <memory>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/external_data.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/ownership.h"

namespace riegeli {

namespace intrusive_shared_ptr_internal {

template <typename T, typename Enable = void>
struct HasHasUniqueOwner : std::false_type {};

template <typename T>
struct HasHasUniqueOwner<
    T, std::enable_if_t<std::is_convertible_v<
           decltype(std::declval<const T&>().HasUniqueOwner()), bool>>>
    : std::true_type {};

template <typename T, typename Enable = void>
struct HasGetCount : std::false_type {};

template <typename T>
struct HasGetCount<T,
                   std::enable_if_t<std::is_convertible_v<
                       decltype(std::declval<const T&>().GetCount()), size_t>>>
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
class ABSL_ATTRIBUTE_TRIVIAL_ABI ABSL_NULLABILITY_COMPATIBLE IntrusiveSharedPtr
    : public WithEqual<IntrusiveSharedPtr<T>> {
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
  explicit IntrusiveSharedPtr(T* ptr ABSL_ATTRIBUTE_LIFETIME_BOUND,
                              PassOwnership = kPassOwnership) noexcept
      : ptr_(ptr) {}
  explicit IntrusiveSharedPtr(T* ptr ABSL_ATTRIBUTE_LIFETIME_BOUND,
                              ShareOwnership) noexcept
      : ptr_(Ref(ptr)) {}

  // Creates an `IntrusiveSharedPtr` holding a constructed value.
  //
  // The object is constructed with `new`, which means that `T::Unref()` should
  // delete the object with `delete this`.
  explicit IntrusiveSharedPtr(Initializer<T> value) : ptr_(std::move(value)) {}

  // Creates an `IntrusiveSharedPtr` holding a constructed value of a compatible
  // type.
  //
  // The object is constructed with `new`, which means that `T::Unref()` should
  // delete the object with `delete this`.
  template <typename SubInitializer,
            std::enable_if_t<
                std::is_convertible_v<TargetT<SubInitializer>*, T*>, int> = 0>
  explicit IntrusiveSharedPtr(SubInitializer&& value)
      : ptr_(Initializer<TargetT<SubInitializer>>(
            std::forward<SubInitializer>(value))) {}

  // Converts from an `IntrusiveSharedPtr` with a compatible type.
  template <typename SubT,
            std::enable_if_t<std::is_convertible_v<SubT*, T*>, int> = 0>
  /*implicit*/ IntrusiveSharedPtr(const IntrusiveSharedPtr<SubT>& that) noexcept
      : ptr_(Ref(that.ptr_.get())) {}
  template <typename SubT,
            std::enable_if_t<std::is_convertible_v<SubT*, T*>, int> = 0>
  IntrusiveSharedPtr& operator=(const IntrusiveSharedPtr<SubT>& that) noexcept {
    ptr_.reset(Ref(that.ptr_.get()));
    return *this;
  }

  // Converts from an `IntrusiveSharedPtr` with a compatible type.
  //
  // The source `IntrusiveSharedPtr` is left empty.
  template <typename SubT,
            std::enable_if_t<std::is_convertible_v<SubT*, T*>, int> = 0>
  /*implicit*/ IntrusiveSharedPtr(IntrusiveSharedPtr<SubT>&& that) noexcept
      : ptr_(std::move(that).ptr_) {}
  template <typename SubT,
            std::enable_if_t<std::is_convertible_v<SubT*, T*>, int> = 0>
  IntrusiveSharedPtr& operator=(IntrusiveSharedPtr<SubT>&& that) noexcept {
    ptr_.reset(std::move(that).ptr_);
    return *this;
  }

  IntrusiveSharedPtr(const IntrusiveSharedPtr& that) noexcept
      : ptr_(Ref(that.ptr_.get())) {}
  IntrusiveSharedPtr& operator=(const IntrusiveSharedPtr& that) noexcept {
    ptr_.reset(Ref(that.ptr_.get()));
    return *this;
  }

  // The source `IntrusiveSharedPtr` is left empty.
  IntrusiveSharedPtr(IntrusiveSharedPtr&& that) = default;
  IntrusiveSharedPtr& operator=(IntrusiveSharedPtr&& that) = default;

  // Replaces the object, or makes `*this` empty if `ptr == nullptr`.
  //
  // Takes ownership of `ptr` unless the second parameter is `kShareOwnership`.
  //
  // The old object, if any, is destroyed afterwards.
  ABSL_ATTRIBUTE_REINITIALIZES
  void Reset(T* ptr = nullptr, PassOwnership = kPassOwnership) {
    ptr_.reset(ptr);
  }
  ABSL_ATTRIBUTE_REINITIALIZES
  void Reset(T* ptr, ShareOwnership) { ptr_.reset(Ref(ptr)); }

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
  template <typename SubInitializer,
            std::enable_if_t<
                std::is_convertible_v<TargetT<SubInitializer>*, T*>, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(SubInitializer&& value) {
    ptr_ = Initializer<TargetT<SubInitializer>>(
        std::forward<SubInitializer>(value));
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

  // Returns the current reference count.
  //
  // If the `IntrusiveSharedPtr` is accessed by multiple threads, this is a
  // snapshot of the count which may change asynchronously, hence usage of
  // `GetRefCount()` should be limited to cases not important for correctness,
  // like producing debugging output.
  //
  // The reference count can be reliably compared against 1 with `IsUnique()`.
  //
  // Supported if `T` supports `GetCount()`.
  template <typename DependentT = T,
            std::enable_if_t<
                intrusive_shared_ptr_internal::HasGetCount<DependentT>::value,
                int> = 0>
  size_t GetRefCount() const {
    if (ptr_ == nullptr) return 0;
    return ptr_->GetRefCount();
  }

  // Returns the pointer.
  T* get() const ABSL_ATTRIBUTE_LIFETIME_BOUND { return ptr_.get(); }

  // Dereferences the pointer.
  T& operator*() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT_NE(ptr_, nullptr)
        << "Failed precondition of IntrusiveSharedPtr::operator*: null pointer";
    return *ptr_;
  }
  T* operator->() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT_NE(ptr_, nullptr)
        << "Failed precondition of IntrusiveSharedPtr::operator->: null "
           "pointer";
    return ptr_.get();
  }

  // Returns the pointer. This `IntrusiveSharedPtr` is left empty.
  T* Release() { return ptr_.release(); }

  template <typename OtherT>
  friend bool operator==(const IntrusiveSharedPtr& a,
                         const IntrusiveSharedPtr<OtherT>& b) {
    return a.get() == b.get();
  }
  friend bool operator==(const IntrusiveSharedPtr& a, std::nullptr_t) {
    return a.get() == nullptr;
  }

  // Indicates support for:
  //  * `ExternalRef(const IntrusiveSharedPtr&, substr)`
  //  * `ExternalRef(IntrusiveSharedPtr&&, substr)`
  friend void RiegeliSupportsExternalRef(const IntrusiveSharedPtr*) {}

  // Supports `ExternalRef`.
  friend ExternalStorage RiegeliToExternalStorage(IntrusiveSharedPtr* self) {
    return ExternalStorage(const_cast<std::remove_cv_t<T>*>(self->Release()),
                           [](void* ptr) {
                             if (ptr != nullptr) static_cast<T*>(ptr)->Unref();
                           });
  }

  // Supports `riegeli::Debug()`.
  template <typename DebugStream>
  friend void RiegeliDebug(const IntrusiveSharedPtr& src, DebugStream& dest) {
    dest.Debug(src.get());
  }

  // Supports `MemoryEstimator`.
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

  using pointer = T*;  // For `ABSL_NULLABILITY_COMPATIBLE`.

  struct Unrefer {
    void operator()(T* ptr) const { ptr->Unref(); }
  };

  template <typename SubT>
  static SubT* Ref(SubT* ptr) {
    if (ptr != nullptr) ptr->Ref();
    return ptr;
  }

  template <typename DependentT>
  struct IsAssignable
      : std::conjunction<
            intrusive_shared_ptr_internal::HasHasUniqueOwner<DependentT>,
            std::disjunction<
                std::negation<std::has_virtual_destructor<DependentT>>,
                std::is_final<DependentT>>,
            std::is_move_assignable<DependentT>> {};

  template <typename DependentT = T,
            std::enable_if_t<IsAssignable<DependentT>::value, int> = 0>
  void ResetImpl(Initializer<T> value) {
    if (IsUnique()) {
      *ptr_ = std::move(value);
      return;
    }
    ptr_ = std::move(value);
  }
  template <typename DependentT = T,
            std::enable_if_t<!IsAssignable<DependentT>::value, int> = 0>
  void ResetImpl(Initializer<T> value) {
    ptr_ = std::move(value);
  }

  std::unique_ptr<T, Unrefer> ptr_;
};

template <typename T>
explicit IntrusiveSharedPtr(T* ptr, PassOwnership = kPassOwnership)
    -> IntrusiveSharedPtr<T>;
template <typename T>
explicit IntrusiveSharedPtr(T* ptr, ShareOwnership) -> IntrusiveSharedPtr<T>;
template <typename T, std::enable_if_t<!std::is_pointer_v<T>, int> = 0>
explicit IntrusiveSharedPtr(T&& value) -> IntrusiveSharedPtr<TargetT<T>>;

}  // namespace riegeli

#endif  // RIEGELI_BASE_INTRUSIVE_SHARED_PTR_H_
