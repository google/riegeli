// Copyright 2026 Google LLC
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

#ifndef RIEGELI_BASE_HYBRID_DIRECT_INTERNAL_H_
#define RIEGELI_BASE_HYBRID_DIRECT_INTERNAL_H_

#include <stddef.h>

#include <limits>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "riegeli/base/assert.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli::hybrid_direct_internal {

// Wraps a `T` which is constructed explicitly later, rather than when
// `DelayedConstructor<T>` is constructed.
//
// In contrast to `std::optional<T>`, this avoids the overhead of tracking
// whether the object has been constructed, at the cost of passing this
// responsibility to the caller.
//
// Either `emplace()` or `Abandon()` must be called exactly once.
// If `emplace()` is called, the regular destructor should be called later.
// If `Abandon()` is called, the regular destructor must not be called.
template <typename T>
class DelayedConstructor {
 public:
  // Does not construct the wrapped object yet.
  DelayedConstructor() noexcept {}

  DelayedConstructor(const DelayedConstructor&) = delete;
  DelayedConstructor& operator=(const DelayedConstructor&) = delete;

  // Destroys the wrapped object. It must have been constructed.
  ~DelayedConstructor() { value_.~T(); }

  // Constructs the wrapped object. It must not have been constructed yet.
  template <typename... Args,
            std::enable_if_t<std::is_constructible_v<T, Args&&...>, int> = 0>
  T& emplace(Args&&... args) ABSL_ATTRIBUTE_LIFETIME_BOUND {
    new (&value_) T(std::forward<Args>(args)...);
    return value_;
  }

  // Destroys the `DelayedConstructor`. The wrapped object must not have been
  // constructed. This is needed for `SizedArray`.
  void Abandon() {}

  // Returns the wrapped object. It must have been constructed.
  T& operator*() ABSL_ATTRIBUTE_LIFETIME_BOUND { return value_; }
  const T& operator*() const ABSL_ATTRIBUTE_LIFETIME_BOUND { return value_; }

 private:
  union {
    T value_;
  };
};

// A deleter for `SizedArray<T, supports_abandon>`.
//
// If `supports_abandon` is true, truncation with `AbandonAfter()` is supported,
// at the cost of some overhead.
//
// A moved-from `SizedDeleter` reports a positive size. This helps to trigger
// a null pointer dereference when a moved-from `SizedArray` is used.
template <typename T, bool supports_abandon = false>
class SizedDeleter {
 public:
  static size_t max_size() { return kSizeMask / sizeof(T); }

  SizedDeleter() = default;

  explicit SizedDeleter(size_t size) : size_(size) {}

  SizedDeleter(SizedDeleter&& that) noexcept
      : size_(std::exchange(that.size_, kPoisonedSize)) {}

  SizedDeleter& operator=(SizedDeleter&& that) noexcept {
    size_ = std::exchange(that.size_, kPoisonedSize);
    return *this;
  }

  void operator()(T* ptr) const {
    for (T* iter = ptr + (size_ & kSizeMask); iter != ptr;) {
      --iter;
      iter->~T();
    }
    if (ABSL_PREDICT_FALSE((size_ & kOverallocated) != 0)) {
      // The allocated size is not tracked and sized delete must not be used.
      if constexpr (alignof(T) <= __STDCPP_DEFAULT_NEW_ALIGNMENT__) {
        operator delete[](ptr);
      } else {
        operator delete[](ptr, std::align_val_t(alignof(T)));
      }
      return;
    }
    if constexpr (alignof(T) <= __STDCPP_DEFAULT_NEW_ALIGNMENT__) {
      operator delete[](ptr, size_ * sizeof(T));
    } else {
      operator delete[](ptr, size_ * sizeof(T), std::align_val_t(alignof(T)));
    }
  }

  size_t size() const { return size_ & kSizeMask; }

  // If the pointer associated with this deleter is `nullptr`, returns `true`
  // when this deleter is moved-from. Otherwise the result is meaningless.
  bool IsMovedFromIfNull() const { return size_ == kPoisonedSize; }

  // Reduces the size to `new_size`. Calls `Abandon()` on elements being
  // abandoned. The regular destructor will not be called for them.
  //
  // `SizedDeleter` is optimized for the case when `AbandonAfter()` is never
  // called with a changed size.
  template <bool dependent_supports_abandon = supports_abandon,
            std::enable_if_t<dependent_supports_abandon, int> = 0>
  void AbandonAfter(T* ptr, size_t new_size) {
    RIEGELI_ASSERT_LE(new_size, size_ & kSizeMask)
        << "Failed precondition of SizedDeleter::AbandonAfter(): "
           "array size overflow";
    if (ABSL_PREDICT_TRUE(new_size == size_)) return;
    T* const new_end = ptr + new_size;
    for (T* iter = ptr + (size_ & kSizeMask); iter != new_end;) {
      --iter;
      iter->Abandon();
    }
    size_ = new_size | kOverallocated;
  }

 private:
  // A moved-from `SizedDeleter` has `size_ == kPoisonedSize`. In debug mode
  // this asserts against using a moved-from object. In non-debug mode, if the
  // key is not too large, then this triggers a null pointer dereference with an
  // offset up to 1MB, which is assumed to reliably crash.
  static constexpr size_t kPoisonedSize = (size_t{1} << 20) / sizeof(T);

  // If `supports_abandon` is true, `size_` tracks the current size and whether
  // the original size has been reduced with `AbandonAfter()`. In that case
  // sized delete is not called because the allocated size is not tracked.
  static constexpr size_t kSizeMask =
      std::numeric_limits<size_t>::max() >> (supports_abandon ? 1 : 0);
  static constexpr size_t kOverallocated = ~kSizeMask;

  // The number of elements. If marked with `kOverallocated`, the allocated size
  // is not tracked and sized delete must not be used.
  size_t size_ = 0;
};

// Like `std::unique_ptr<T[]>`, but the size is stored in the deleter.
// It is available as `get_deleter().size()` and used for sized delete.
//
// If `supports_abandon` is true, truncation with `get_deleter().AbandonAfter()`
// is supported, at the cost of some overhead.
//
// A moved-from `SizedArray` is `nullptr` but reports a positive size. This
// helps to trigger a null pointer dereference when a moved-from `SizedArray`
// is used.
template <typename T, bool supports_abandon = false>
using SizedArray = std::unique_ptr<T[], SizedDeleter<T, supports_abandon>>;

// Like `std::make_unique<T[]>(size)`.
//
// If `supports_abandon` is true, truncation with `get_deleter().AbandonAfter()`
// is supported, at the cost of some overhead.
template <typename T, bool supports_abandon = false>
inline SizedArray<T, supports_abandon> MakeSizedArray(size_t size) {
  T* ptr;
  if constexpr (alignof(T) <= __STDCPP_DEFAULT_NEW_ALIGNMENT__) {
    ptr = static_cast<T*>(operator new[](size * sizeof(T)));
  } else {
    ptr = static_cast<T*>(operator new[](size * sizeof(T),
                                         std::align_val_t(alignof(T))));
  }
  T* const end = ptr + size;
  for (T* iter = ptr; iter != end; ++iter) {
    new (iter) T();
  }
  return SizedArray<T, supports_abandon>(
      ptr, SizedDeleter<T, supports_abandon>(size));
}

// Like `std::make_unique_for_overwrite<T[]>(size)`.
//
// If `supports_abandon` is true, truncation with `get_deleter().AbandonAfter()`
// is supported, at the cost of some overhead.
template <typename T, bool supports_abandon = false>
inline SizedArray<T, supports_abandon> MakeSizedArrayForOverwrite(size_t size) {
  T* ptr;
  if constexpr (alignof(T) <= __STDCPP_DEFAULT_NEW_ALIGNMENT__) {
    ptr = static_cast<T*>(operator new[](size * sizeof(T)));
  } else {
    ptr = static_cast<T*>(operator new[](size * sizeof(T),
                                         std::align_val_t(alignof(T))));
  }
  T* const end = ptr + size;
  for (T* iter = ptr; iter != end; ++iter) {
    new (iter) T;
  }
  return SizedArray<T, supports_abandon>(
      ptr, SizedDeleter<T, supports_abandon>(size));
}

// Performs an assignment, but the behavior is undefined if the old value of the
// destination is not null. This allows the compiler skip generating the code
// which deletes the old value.
//
// This is meant for initializing member variables of smart pointer types in
// functions where the compiler cannot determine itself that the old value is
// always null.
template <typename Dest, typename Src>
inline void AssignToAssumedNull(Dest& dest, Src&& src) {
  RIEGELI_ASSUME_EQ(dest, nullptr)
      << "Failed precondition of AssignToAssumedNull(): "
         "old value of destination is not null";
  dest = std::forward<Src>(src);
}

}  // namespace riegeli::hybrid_direct_internal

#endif  // RIEGELI_BASE_HYBRID_DIRECT_INTERNAL_H_
