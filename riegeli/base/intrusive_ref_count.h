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

#ifndef RIEGELI_BASE_INTRUSIVE_REF_COUNT_H_
#define RIEGELI_BASE_INTRUSIVE_REF_COUNT_H_

#include <stddef.h>

#include <atomic>
#include <cstddef>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/compare.h"

namespace riegeli {

// `RefCountedPtr<T>` implements shared ownership of an object of type `T`.
// It can also be empty, with the pointer being `nullptr`.
//
// `RefCountedPtr<T>` has a smaller overhead than `std::shared_ptr<T>`, but
// requires cooperation from `T`.
//
// `T` maintains its own reference count, e.g. as a mutable atomic member.
// Deriving `T` from `RefCountedBase<T>` makes it easier to provide functions
// needed by `RefCountedPtr<T>`. `T` should support:
//
// ```
//   // Increments the reference count of `*this`.
//   void Ref() const;
//
//   // Decrements the reference count of `*this`. Deletes `this` when the
//   // reference count reaches 0.
//   void Unref() const;
// ```
template <typename T>
class
#ifdef ABSL_ATTRIBUTE_TRIVIAL_ABI
    ABSL_ATTRIBUTE_TRIVIAL_ABI
#endif
        RefCountedPtr : public WithEqual<RefCountedPtr<T>> {
 public:
  // Creates an empty `RefCountedPtr`.
  constexpr RefCountedPtr() = default;
  /*implicit*/ constexpr RefCountedPtr(std::nullptr_t) noexcept {}

  // Creates a `RefCountedPtr` taking ownership of `ptr`.
  explicit RefCountedPtr(T* ptr) noexcept : ptr_(ptr) {}

  // Converts from a `RefCountedPtr` with a compatible pointer type.
  template <typename Other,
            std::enable_if_t<std::is_convertible<Other*, T*>::value, int> = 0>
  /*implicit*/ RefCountedPtr(const RefCountedPtr<Other>& that) noexcept;
  template <typename Other,
            std::enable_if_t<std::is_convertible<Other*, T*>::value, int> = 0>
  RefCountedPtr& operator=(const RefCountedPtr<Other>& that) noexcept;

  // The source `RefCountedPtr` is left empty.
  template <typename Other,
            std::enable_if_t<std::is_convertible<Other*, T*>::value, int> = 0>
  /*implicit*/ RefCountedPtr(RefCountedPtr<Other>&& that) noexcept;
  template <typename Other,
            std::enable_if_t<std::is_convertible<Other*, T*>::value, int> = 0>
  RefCountedPtr& operator=(RefCountedPtr<Other>&& that) noexcept;

  RefCountedPtr(const RefCountedPtr& that) noexcept;
  RefCountedPtr& operator=(const RefCountedPtr& that) noexcept;

  // The source `RefCountedPtr` is left empty.
  RefCountedPtr(RefCountedPtr&& that) noexcept;
  RefCountedPtr& operator=(RefCountedPtr&& that) noexcept;

  ~RefCountedPtr();

  // Replaces the pointer.
  ABSL_ATTRIBUTE_REINITIALIZES void reset(T* ptr = nullptr);

  // Returns the pointer.
  T* get() const { return ptr_; }

  // Dereferences the pointer.
  T& operator*() const {
    RIEGELI_ASSERT(ptr_ != nullptr)
        << "Failed precondition of RefCountedPtr::operator*: null pointer";
    return *ptr_;
  }
  T* operator->() const {
    RIEGELI_ASSERT(ptr_ != nullptr)
        << "Failed precondition of RefCountedPtr::operator->: null pointer";
    return ptr_;
  }

  // Returns the pointer. This `RefCountedPtr` is left empty.
  T* release() { return std::exchange(ptr_, nullptr); }

  // Replaces `*this` with `that`, returning the previous value of `*this`.
  template <typename Other,
            std::enable_if_t<std::is_convertible<Other*, T*>::value, int> = 0>
  RefCountedPtr exchange(RefCountedPtr<Other> that) {
    return RefCountedPtr(std::exchange(ptr_, that.release()));
  }

  friend bool operator==(const RefCountedPtr& a, const RefCountedPtr& b) {
    return a.get() == b.get();
  }
  friend bool operator==(const RefCountedPtr& a, std::nullptr_t) {
    return a.get() == nullptr;
  }

  // Allow Nullability annotations on `RefCountedPtr`.
  using absl_nullability_compatible = void;

  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const RefCountedPtr* self,
                                        MemoryEstimator& memory_estimator) {
    if (memory_estimator.RegisterNode(self->get())) {
      memory_estimator.RegisterDynamicObject(&**self);
    }
  }

 private:
  T* ptr_ = nullptr;
};

// Creates an object with `new` and wraps it in `RefCountedPtr`.
//
// `MakeRefCounted()` is to `RefCountedPtr` like `std::make_unique()` is to
// `std::unique_ptr`.
template <typename T, typename... Args>
inline RefCountedPtr<T> MakeRefCounted(Args&&... args) {
  return RefCountedPtr<T>(new T(std::forward<Args>(args)...));
}

// Provides operations on an atomic reference count.
class RefCount {
 public:
  RefCount() = default;

  RefCount(const RefCount&) = delete;
  RefCount& operator=(const RefCount&) = delete;

  // Increments the reference count.
  void Ref() const { ref_count_.fetch_add(1, std::memory_order_relaxed); }

  // Decrements the reference count. Returns `true` when the reference count
  // reaches 0.
  bool Unref() const {
    // Optimization: avoid an expensive atomic read-modify-write operation
    // if the reference count is 1.
    return ref_count_.load(std::memory_order_acquire) == 1 ||
           ref_count_.fetch_sub(1, std::memory_order_acq_rel) == 1;
  }

  // Returns `true` if there is only one owner of the object.
  bool has_unique_owner() const {
    return ref_count_.load(std::memory_order_acquire) == 1;
  }

  // Returns the current count.
  //
  // If the `RefCount` is accessed by multiple threads, this is a snapshot of
  // the count which may change asynchronously, hence usage of `get_count()`
  // should be limited to cases not important for correctness like producing
  // debugging output.
  size_t get_count() const {
    return ref_count_.load(std::memory_order_relaxed);
  }

 private:
  mutable std::atomic<size_t> ref_count_{1};
};

// Deriving `T` from `RefCountedBase<T>` makes it easier to provide functions
// needed by `RefCountedPtr<T>`.
//
// The destructor of `RefCountedBase<T>` is not virtual. The object will be
// deleted by `delete static_cast<T*>(ptr)`. This means that `T` must be the
// actual object type or `T` must define a virtual destructor, and that multiple
// inheritance is not supported.
//
// `RefCountedBase<T>` also provides `has_unique_owner()`.
template <typename T>
class RefCountedBase {
 public:
  RefCountedBase() noexcept {
    static_assert(std::is_base_of<RefCountedBase<T>, T>::value,
                  "The template argument T in RefCountedBase<T> "
                  "must be the class derived from RefCountedBase<T>");
  }

  // Increments the reference count of `*this`.
  void Ref() const { ref_count_.Ref(); }

  // Decrements the reference count of `*this`. Deletes `this` when the
  // reference count reaches 0.
  void Unref() const {
    if (ref_count_.Unref()) delete static_cast<const T*>(this);
  }

  // Returns `true` if there is only one owner of the object.
  bool has_unique_owner() const { return ref_count_.has_unique_owner(); }

 protected:
  ~RefCountedBase() = default;

 private:
  RefCount ref_count_;
};

// Implementation details follow.

template <typename T>
template <typename Other,
          std::enable_if_t<std::is_convertible<Other*, T*>::value, int>>
inline RefCountedPtr<T>::RefCountedPtr(
    const RefCountedPtr<Other>& that) noexcept
    : ptr_(that.get()) {
  if (ptr_ != nullptr) ptr_->Ref();
}

template <typename T>
template <typename Other,
          std::enable_if_t<std::is_convertible<Other*, T*>::value, int>>
inline RefCountedPtr<T>& RefCountedPtr<T>::operator=(
    const RefCountedPtr<Other>& that) noexcept {
  reset(RefCountedPtr<Other>(that).release());
  return *this;
}

template <typename T>
template <typename Other,
          std::enable_if_t<std::is_convertible<Other*, T*>::value, int>>
inline RefCountedPtr<T>::RefCountedPtr(RefCountedPtr<Other>&& that) noexcept
    : ptr_(that.release()) {}

template <typename T>
template <typename Other,
          std::enable_if_t<std::is_convertible<Other*, T*>::value, int>>
inline RefCountedPtr<T>& RefCountedPtr<T>::operator=(
    RefCountedPtr<Other>&& that) noexcept {
  reset(that.release());
  return *this;
}

template <typename T>
inline RefCountedPtr<T>::RefCountedPtr(const RefCountedPtr& that) noexcept
    : ptr_(that.get()) {
  if (ptr_ != nullptr) ptr_->Ref();
}

template <typename T>
inline RefCountedPtr<T>& RefCountedPtr<T>::operator=(
    const RefCountedPtr& that) noexcept {
  reset(RefCountedPtr(that).release());
  return *this;
}

template <typename T>
inline RefCountedPtr<T>::RefCountedPtr(RefCountedPtr&& that) noexcept
    : ptr_(that.release()) {}

template <typename T>
inline RefCountedPtr<T>& RefCountedPtr<T>::operator=(
    RefCountedPtr&& that) noexcept {
  reset(that.release());
  return *this;
}

template <typename T>
inline RefCountedPtr<T>::~RefCountedPtr() {
  if (ptr_ != nullptr) ptr_->Unref();
}

template <typename T>
inline void RefCountedPtr<T>::reset(T* ptr) {
  T* const old_ptr = std::exchange(ptr_, ptr);
  if (old_ptr != nullptr) old_ptr->Unref();
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_INTRUSIVE_REF_COUNT_H_
