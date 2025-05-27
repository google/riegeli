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

#ifndef RIEGELI_BASE_SHARED_PTR_H_
#define RIEGELI_BASE_SHARED_PTR_H_

#include <stddef.h>

#include <cstddef>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/meta/type_traits.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/external_data.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/new_aligned.h"
#include "riegeli/base/ref_count.h"

namespace riegeli {

// `SharedPtr<T>` implements shared ownership of an object of type `T`.
// It can also be empty, with the pointer being `nullptr`.
//
// The actual object can be of a subtype of `T`, as long as `T` has a virtual
// destructor and is a leftmost non-virtual base class. Otherwise the object
// must have the same type as `T`, except for possibly different cv-qualifiers.
//
// `SharedPtr` has a smaller overhead than `std::shared_ptr` (the pointer has 1
// word instead of 2, the allocated header before the object has 1 word if `T`
// does not have a virtual destructor, and 2 words if `T` does have a virtual
// destructor, instead of 3 words in either case), but has fewer features
// (e.g. no custom allocation or deletion, the leftmost non-virtual base class
// restriction, no weak pointers).
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
    SharedPtr : public WithEqual<SharedPtr<T>> {
 private:
  template <typename SubT>
  struct IsCompatibleProperSubtype
      : absl::conjunction<absl::negation<std::is_same<SubT, T>>,
                          std::is_convertible<SubT*, T*>,
                          absl::disjunction<std::is_same<std::remove_cv_t<SubT>,
                                                         std::remove_cv_t<T>>,
                                            std::has_virtual_destructor<T>>> {};

 public:
  // Creates an empty `SharedPtr`.
  constexpr SharedPtr() = default;
  /*implicit*/ constexpr SharedPtr(std::nullptr_t) noexcept {}
  SharedPtr& operator=(std::nullptr_t) {
    Reset();
    return *this;
  }

  // Creates a `SharedPtr` holding a constructed value.
  explicit SharedPtr(Initializer<T> value) : ptr_(New(std::move(value))) {}

  // Creates a `SharedPtr` holding a constructed value of a compatible type.
  template <
      typename SubInitializer,
      std::enable_if_t<
          IsCompatibleProperSubtype<TargetT<SubInitializer>>::value, int> = 0>
  explicit SharedPtr(SubInitializer&& value)
      : ptr_(UpCast(New<TargetT<SubInitializer>>(
            std::forward<SubInitializer>(value)))) {}

  // Converts from a `SharedPtr` with a compatible type.
  template <typename SubT,
            std::enable_if_t<IsCompatibleProperSubtype<SubT>::value, int> = 0>
  /*implicit*/ SharedPtr(const SharedPtr<SubT>& that) noexcept
      : ptr_(UpCast(Ref(that.ptr_.get()))) {}
  template <typename SubT,
            std::enable_if_t<IsCompatibleProperSubtype<SubT>::value, int> = 0>
  SharedPtr& operator=(const SharedPtr<SubT>& that) noexcept {
    ptr_.reset(UpCast(Ref(that.ptr_.get())));
    return *this;
  }

  // Converts from a `SharedPtr` with a compatible type.
  //
  // The source `SharedPtr` is left empty.
  template <typename SubT,
            std::enable_if_t<IsCompatibleProperSubtype<SubT>::value, int> = 0>
  /*implicit*/ SharedPtr(SharedPtr<SubT>&& that) noexcept
      : ptr_(UpCast(that.Release())) {}
  template <typename SubT,
            std::enable_if_t<IsCompatibleProperSubtype<SubT>::value, int> = 0>
  SharedPtr& operator=(SharedPtr<SubT>&& that) noexcept {
    ptr_.reset(UpCast(that.Release()));
    return *this;
  }

  SharedPtr(const SharedPtr& that) noexcept : ptr_(Ref(that.ptr_.get())) {}
  SharedPtr& operator=(const SharedPtr& that) noexcept {
    ptr_.reset(Ref(that.ptr_.get()));
    return *this;
  }

  // The source `SharedPtr` is left empty.
  SharedPtr(SharedPtr&& that) = default;
  SharedPtr& operator=(SharedPtr&& that) = default;

  // Makes `*this` empty.
  //
  // The old object, if any, is destroyed afterwards.
  ABSL_ATTRIBUTE_REINITIALIZES
  void Reset(std::nullptr_t = nullptr) { ptr_.reset(); }

  // Replaces the object with a constructed value.
  //
  // The old object, if any, is destroyed afterwards.
  //
  // If `*this` is the only owner of an object known to have the same
  // move-assignable type, the existing object is assigned or reset instead of
  // allocating and constructing a new object.
  ABSL_ATTRIBUTE_REINITIALIZES
  void Reset(Initializer<T> value) { ResetImpl(std::move(value)); }

  // Replaces the object with a constructed value of a compatible type.
  //
  // The old object, if any, is destroyed afterwards.
  template <
      typename SubInitializer,
      std::enable_if_t<
          IsCompatibleProperSubtype<TargetT<SubInitializer>>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(SubInitializer&& value) {
    ptr_.reset(UpCast(
        New<TargetT<SubInitializer>>(std::forward<SubInitializer>(value))));
  }

  // Returns `true` if `*this` is the only owner of the object.
  //
  // This can be used to check if the object may be modified (in contrast to
  // `std::shared_ptr::unique()`).
  //
  // If `*this` is empty, returns `false`.
  bool IsUnique() const {
    return ptr_ != nullptr && ref_count(ptr_.get()).HasUniqueOwner();
  }

  // Returns the current reference count.
  //
  // If the `SharedPtr` is accessed by multiple threads, this is a snapshot of
  // the count which may change asynchronously, hence usage of `GetRefCount()`
  // should be limited to cases not important for correctness, like producing
  // debugging output.
  //
  // The reference count can be reliably compared against 1 with `IsUnique()`.
  size_t GetRefCount() const {
    if (ptr_ == nullptr) return 0;
    return ref_count(ptr_.get()).GetCount();
  }

  // Returns the pointer.
  T* get() const ABSL_ATTRIBUTE_LIFETIME_BOUND { return ptr_.get(); }

  // Dereferences the pointer.
  T& operator*() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT_NE(ptr_, nullptr)
        << "Failed precondition of SharedPtr::operator*: null pointer";
    return *ptr_;
  }
  T* operator->() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT_NE(ptr_, nullptr)
        << "Failed precondition of SharedPtr::operator->: null pointer";
    return ptr_.get();
  }

  // Returns the pointer, releasing its ownership; the `SharedPtr` is left
  // empty. The returned pointer must be deleted using `DeleteReleased()`.
  //
  // If the returned pointer is `nullptr`, it allowed but not required to call
  // `DeleteReleased()`.
  T* Release() { return ptr_.release(); }

  // Deletes the pointer obtained by `Release()`.
  //
  // Does nothing if `ptr == nullptr`.
  static void DeleteReleased(T* ptr) {
    if (ptr != nullptr) Unrefer()(ptr);
  }

  template <typename OtherT>
  friend bool operator==(const SharedPtr& a, const SharedPtr<OtherT>& b) {
    return a.ptr_ == b.ptr_;
  }
  friend bool operator==(const SharedPtr& a, std::nullptr_t) {
    return a.ptr_ == nullptr;
  }

  // Indicates support for:
  //  * `ExternalRef(const SharedPtr&, substr)`
  //  * `ExternalRef(SharedPtr&&, substr)`
  friend void RiegeliSupportsExternalRef(const SharedPtr*) {}

  // Supports `ExternalRef`.
  friend ExternalStorage RiegeliToExternalStorage(SharedPtr* self) {
    return ExternalStorage(
        const_cast<std::remove_cv_t<T>*>(self->Release()),
        [](void* ptr) { SharedPtr::DeleteReleased(static_cast<T*>(ptr)); });
  }

  // Supports `riegeli::Debug()`.
  template <typename DebugStream>
  friend void RiegeliDebug(const SharedPtr& src, DebugStream& dest) {
    dest.Debug(src.get());
  }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const SharedPtr* self,
                                        MemoryEstimator& memory_estimator) {
    if (memory_estimator.RegisterNode(self->get())) {
      self->RegisterSubobjects(memory_estimator);
    }
  }

 private:
  // For converting from a `SharedPtr` with a compatible type.
  template <typename SubT>
  friend class SharedPtr;

  // An object of type `SubT` is allocated together with `RefCount` if
  // `!std::has_virtual_destructor_v<SubT>`, or `Control` otherwise.
  //
  // `RefCount` or `Control` immediately precede the object. If the object has
  // a higher alignment requirement than `RefCount` or `Control`, there can be
  // padding at the beginning of the allocation, before `RefCount` or `Control`.
  // Hence if `std::has_virtual_destructor_v<SubT>` then the beginning of the
  // allocation is known only to `Control::destroy()`.
  struct Control {
    explicit Control(void (*destroy)(void* ptr)) : destroy(destroy) {}

    void (*destroy)(void* ptr);
    RefCount ref_count;
  };

  struct Unrefer {
    void operator()(T* ptr) const {
      if (ref_count(ptr).Unref()) Delete(ptr);
    }
  };

  template <typename SubT>
  static void DestroyMethod(void* ptr) {
    static_cast<SubT*>(ptr)->SubT::~SubT();
    static constexpr size_t kOffset = RoundUp<alignof(SubT)>(sizeof(Control));
    void* const allocated_ptr = static_cast<char*>(ptr) - kOffset;
    DeleteAligned<void, UnsignedMax(alignof(Control), alignof(SubT))>(
        allocated_ptr, kOffset + sizeof(SubT));
  }

  template <typename SubT>
  static T* UpCast(SubT* ptr) {
    T* const super_ptr = ptr;
    RIEGELI_CHECK(
        static_cast<void*>(const_cast<std::remove_cv_t<T>*>(super_ptr)) ==
        static_cast<void*>(const_cast<std::remove_cv_t<SubT>*>(ptr)))
        << "SharedPtr does not support upcasting "
           "to a non-leftmost or virtual base class";
    return super_ptr;
  }

  template <typename SubT,
            std::enable_if_t<!std::has_virtual_destructor_v<SubT>, int> = 0>
  static SubT* New(Initializer<SubT> value) {
    static constexpr size_t kOffset = RoundUp<alignof(SubT)>(sizeof(RefCount));
    void* const allocated_ptr =
        NewAligned<void, UnsignedMax(alignof(RefCount), alignof(SubT))>(
            kOffset + sizeof(SubT));
    void* const ptr = static_cast<char*>(allocated_ptr) + kOffset;
    new (static_cast<RefCount*>(ptr) - 1) RefCount();
    std::move(value).ConstructAt(ptr);
    return std::launder(static_cast<SubT*>(ptr));
  }
  template <typename SubT,
            std::enable_if_t<std::has_virtual_destructor_v<SubT>, int> = 0>
  static SubT* New(Initializer<SubT> value) {
    static constexpr size_t kOffset = RoundUp<alignof(SubT)>(sizeof(Control));
    void* const allocated_ptr =
        NewAligned<void, UnsignedMax(alignof(Control), alignof(SubT))>(
            kOffset + sizeof(SubT));
    void* const ptr = static_cast<char*>(allocated_ptr) + kOffset;
    new (static_cast<Control*>(ptr) - 1) Control(DestroyMethod<SubT>);
    std::move(value).ConstructAt(ptr);
    return std::launder(static_cast<SubT*>(ptr));
  }

  template <
      typename DependentT = T,
      std::enable_if_t<!std::has_virtual_destructor_v<DependentT>, int> = 0>
  static void Delete(T* ptr) {
    ptr->~T();
    static constexpr size_t kOffset = RoundUp<alignof(T)>(sizeof(RefCount));
    void* const allocated_ptr =
        reinterpret_cast<char*>(const_cast<std::remove_cv_t<T>*>(ptr)) -
        kOffset;
    DeleteAligned<void, UnsignedMax(alignof(RefCount), alignof(T))>(
        allocated_ptr, kOffset + sizeof(T));
  }
  template <typename DependentT = T,
            std::enable_if_t<
                absl::conjunction<std::has_virtual_destructor<DependentT>,
                                  std::is_final<DependentT>>::value,
                int> = 0>
  static void Delete(T* ptr) {
    ptr->~T();
    static constexpr size_t kOffset = RoundUp<alignof(T)>(sizeof(Control));
    void* const allocated_ptr =
        reinterpret_cast<char*>(const_cast<std::remove_cv_t<T>*>(ptr)) -
        kOffset;
    DeleteAligned<void, UnsignedMax(alignof(Control), alignof(T))>(
        allocated_ptr, kOffset + sizeof(T));
  }
  template <
      typename DependentT = T,
      std::enable_if_t<
          absl::conjunction<std::has_virtual_destructor<DependentT>,
                            absl::negation<std::is_final<DependentT>>>::value,
          int> = 0>
  static void Delete(T* ptr) {
    control(ptr).destroy(const_cast<std::remove_cv_t<T>*>(ptr));
  }

  template <typename SubT,
            std::enable_if_t<std::has_virtual_destructor_v<SubT>, int> = 0>
  static Control& control(SubT* ptr) {
    return *std::launder(
        reinterpret_cast<Control*>(const_cast<std::remove_cv_t<SubT>*>(ptr)) -
        1);
  }

  template <typename SubT,
            std::enable_if_t<!std::has_virtual_destructor_v<SubT>, int> = 0>
  static RefCount& ref_count(SubT* ptr) {
    return *std::launder(
        reinterpret_cast<RefCount*>(const_cast<std::remove_cv_t<SubT>*>(ptr)) -
        1);
  }
  template <typename SubT,
            std::enable_if_t<std::has_virtual_destructor_v<SubT>, int> = 0>
  static RefCount& ref_count(SubT* ptr) {
    return control(ptr).ref_count;
  }

  template <typename SubT>
  static SubT* Ref(SubT* ptr) {
    if (ptr != nullptr) ref_count(ptr).Ref();
    return ptr;
  }

  template <typename DependentT>
  struct IsAssignable
      : public absl::conjunction<
            absl::disjunction<
                absl::negation<std::has_virtual_destructor<DependentT>>,
                std::is_final<DependentT>>,
            std::is_move_assignable<DependentT>> {};

  template <typename DependentT = T,
            std::enable_if_t<IsAssignable<DependentT>::value, int> = 0>
  void ResetImpl(Initializer<T> value) {
    if (IsUnique()) {
      *ptr_ = std::move(value);
      return;
    }
    ptr_.reset(New(std::move(value)));
  }
  template <typename DependentT = T,
            std::enable_if_t<!IsAssignable<DependentT>::value, int> = 0>
  void ResetImpl(Initializer<T> value) {
    ptr_.reset(New(std::move(value)));
  }

  template <
      typename MemoryEstimator, typename DependentT = T,
      std::enable_if_t<!std::has_virtual_destructor_v<DependentT>, int> = 0>
  void RegisterSubobjects(MemoryEstimator& memory_estimator) const {
    static constexpr size_t kOffset = RoundUp<alignof(T)>(sizeof(RefCount));
    void* const allocated_ptr =
        reinterpret_cast<char*>(const_cast<std::remove_cv_t<T>*>(ptr_.get())) -
        kOffset;
    memory_estimator.RegisterDynamicMemory(allocated_ptr, kOffset + sizeof(T));
    memory_estimator.RegisterSubobjects(ptr_.get());
  }
  template <typename MemoryEstimator, typename DependentT = T,
            std::enable_if_t<
                absl::conjunction<std::has_virtual_destructor<DependentT>,
                                  std::is_final<DependentT>>::value,
                int> = 0>
  void RegisterSubobjects(MemoryEstimator& memory_estimator) const {
    static constexpr size_t kOffset = RoundUp<alignof(T)>(sizeof(Control));
    void* const allocated_ptr =
        reinterpret_cast<char*>(const_cast<std::remove_cv_t<T>*>(ptr_.get())) -
        kOffset;
    memory_estimator.RegisterDynamicMemory(allocated_ptr, kOffset + sizeof(T));
    memory_estimator.RegisterSubobjects(ptr_.get());
  }
  template <
      typename MemoryEstimator, typename DependentT = T,
      std::enable_if_t<
          absl::conjunction<std::has_virtual_destructor<DependentT>,
                            absl::negation<std::is_final<DependentT>>>::value,
          int> = 0>
  void RegisterSubobjects(MemoryEstimator& memory_estimator) const {
    static constexpr size_t kOffset = RoundUp<alignof(T)>(sizeof(Control));
    // `kOffset` is not necessarily accurate because the object can be of a
    // subtype of `T`, so do not pass `allocated_ptr` to
    // `RegisterDynamicMemory()`.
    memory_estimator.RegisterDynamicMemory(
        kOffset + memory_estimator.DynamicSizeOf(ptr_.get()));
    memory_estimator.RegisterSubobjects(ptr_.get());
  }

  std::unique_ptr<T, Unrefer> ptr_;
};

template <typename T>
explicit SharedPtr(T&& value) -> SharedPtr<TargetT<T>>;

}  // namespace riegeli

#endif  // RIEGELI_BASE_SHARED_PTR_H_
