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

#include <memory>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli::hybrid_direct_internal {

// Wraps a `T` which is constructed explicitly later, rather than when
// `DelayedConstructor<T>` is constructed.
//
// In contrast to `std::optional<T>`, this avoids the overhead of tracking
// whether the object has been constructed, at the cost of passing this
// responsibility to the caller.
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

  // Returns the wrapped object. It must have been constructed.
  T& operator*() ABSL_ATTRIBUTE_LIFETIME_BOUND { return value_; }
  const T& operator*() const ABSL_ATTRIBUTE_LIFETIME_BOUND { return value_; }

 private:
  union {
    T value_;
  };
};

// A deleter for `SizedArray<T>`.
//
// A moved-from `SizedDeleter` reports a positive size. This helps to trigger
// a null pointer dereference when a moved-from `SizedArray` is used.
template <typename T>
class SizedDeleter {
 public:
  static size_t max_size() {
    return std::allocator_traits<std::allocator<T>>::max_size(
        std::allocator<T>());
  }

  SizedDeleter() = default;

  explicit SizedDeleter(size_t size) : size_(size) {}

  SizedDeleter(SizedDeleter&& that) noexcept
      : size_(std::exchange(that.size_, kPoisonedSize)) {}

  SizedDeleter& operator=(SizedDeleter&& that) noexcept {
    size_ = std::exchange(that.size_, kPoisonedSize);
    return *this;
  }

  void operator()(T* ptr) const {
    for (T* iter = ptr + size_; iter != ptr;) {
      --iter;
      iter->~T();
    }
    std::allocator<T>().deallocate(ptr, size_);
  }

  size_t size() const { return size_; }

  // If the pointer associated with this deleter is `nullptr`, returns `true`
  // when this deleter is moved-from. Otherwise the result is meaningless.
  bool IsMovedFromIfNull() const { return size_ == kPoisonedSize; }

 private:
  // A moved-from `SizedDeleter` has `size_ == kPoisonedSize`. In debug mode
  // this asserts against using a moved-from object. In non-debug mode, if the
  // key is not too large, then this triggers a null pointer dereference with an
  // offset up to 1MB, which is assumed to reliably crash.
  static constexpr size_t kPoisonedSize = (size_t{1} << 20) / sizeof(T);

  size_t size_ = 0;
};

// Like `std::unique_ptr<T[]>`, but the size is stored in the deleter.
// It is available as `get_deleter().size()` and used for sized delete.
//
// A moved-from `SizedArray` is `nullptr` but reports a positive size. This
// helps to trigger a null pointer dereference when a moved-from `SizedArray`
// is used.
template <typename T>
using SizedArray = std::unique_ptr<T[], SizedDeleter<T>>;

// Like `std::make_unique<T[]>(size)`.
template <typename T>
inline SizedArray<T> MakeSizedArray(size_t size) {
  T* const ptr = std::allocator<T>().allocate(size);
  T* const end = ptr + size;
  for (T* iter = ptr; iter != end; ++iter) {
    new (iter) T();
  }
  return SizedArray<T>(ptr, SizedDeleter<T>(size));
}

}  // namespace riegeli::hybrid_direct_internal

#endif  // RIEGELI_BASE_HYBRID_DIRECT_INTERNAL_H_
