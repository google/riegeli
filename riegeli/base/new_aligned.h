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

#ifndef RIEGELI_BASE_NEW_ALIGNED_H_
#define RIEGELI_BASE_NEW_ALIGNED_H_

#include <stddef.h>

#include <limits>  // IWYU pragma: keep
#include <new>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/numeric/bits.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"                    // IWYU pragma: keep
#include "riegeli/base/estimated_allocated_size.h"  // IWYU pragma: keep

namespace riegeli {

namespace new_aligned_internal {

template <typename T>
inline void EnsureSpaceForObject(size_t& num_bytes) {
  // Allocate enough space to construct the object, even if the caller does not
  // need the whole tail part of the object.
  num_bytes = UnsignedMax(num_bytes, sizeof(T));
}

template <>
inline void EnsureSpaceForObject<void>(
    ABSL_ATTRIBUTE_UNUSED size_t& num_bytes) {}

template <typename T, typename... Args>
inline void ConstructObject(T* ptr, Args&&... args) {
  new (ptr) T(std::forward<Args>(args)...);
}

template <>
inline void ConstructObject(ABSL_ATTRIBUTE_UNUSED void* ptr) {}

template <typename T>
inline void DestroyObject(T* ptr) {
  ptr->~T();
}

template <>
inline void DestroyObject(ABSL_ATTRIBUTE_UNUSED void* ptr) {}

}  // namespace new_aligned_internal

// `NewAligned()`/`DeleteAligned()` provide memory allocation with the specified
// alignment known at compile time, with the size specified in bytes, and which
// allow deallocation to be faster by knowing the size.
//
// The alignment and size passed to `DeleteAligned()` must be the same as in the
// corresponding `NewAligned()`. Pointer types must be compatible as with new
// and delete expressions.
//
// If `T` is `void`, raw memory is allocated but no object is constructed or
// destroyed.
//
// If the allocated size is given in terms of objects rather than bytes
// and the type is not over-aligned (i.e. its alignment is not larger than
// `alignof(max_align_t))`, it is simpler to use `std::allocator<T>()` instead.
// If the type is over-aligned, `std::allocator<T>()` works correctly only when
// `operator new(size_t, std::align_val_t)` from C++17 is available.

// TODO: Test this with overaligned types.

template <typename T, size_t alignment = alignof(T), typename... Args>
inline T* NewAligned(size_t num_bytes, Args&&... args) {
  static_assert(absl::has_single_bit(alignment),
                "alignment must be a power of 2");
  new_aligned_internal::EnsureSpaceForObject<T>(num_bytes);
  T* ptr;
#if __cpp_aligned_new
  if (alignment <= __STDCPP_DEFAULT_NEW_ALIGNMENT__) {
    ptr = static_cast<T*>(operator new(num_bytes));
  } else {
    ptr = static_cast<T*>(operator new(num_bytes, std::align_val_t(alignment)));
  }
#else
#ifdef __STDCPP_DEFAULT_NEW_ALIGNMENT__
  constexpr size_t kDefaultNewAlignment = __STDCPP_DEFAULT_NEW_ALIGNMENT__;
#else
  constexpr size_t kDefaultNewAlignment = alignof(max_align_t);
#endif
  if (alignment <= kDefaultNewAlignment) {
    ptr = static_cast<T*>(operator new(num_bytes));
  } else {
    RIEGELI_CHECK_LE(num_bytes, std::numeric_limits<size_t>::max() -
                                    sizeof(void*) - alignment +
                                    kDefaultNewAlignment)
        << "Out of memory";
    void* const allocated = operator new(sizeof(void*) + num_bytes + alignment -
                                         kDefaultNewAlignment);
    void* const aligned =
        reinterpret_cast<void*>(RoundUp<alignment>(reinterpret_cast<uintptr_t>(
            static_cast<char*>(allocated) + sizeof(void*))));
    reinterpret_cast<void**>(aligned)[-1] = allocated;
    ptr = static_cast<T*>(aligned);
  }
#endif
  new_aligned_internal::ConstructObject(ptr, std::forward<Args>(args)...);
  return ptr;
}

template <typename T, size_t alignment = alignof(T)>
inline void DeleteAligned(T* ptr, size_t num_bytes) {
  static_assert(absl::has_single_bit(alignment),
                "alignment must be a power of 2");
  new_aligned_internal::EnsureSpaceForObject<T>(num_bytes);
  new_aligned_internal::DestroyObject(ptr);
#if __cpp_aligned_new
#if __cpp_sized_deallocation || __GXX_DELETE_WITH_SIZE__
  if (alignment <= __STDCPP_DEFAULT_NEW_ALIGNMENT__) {
    operator delete(ptr, num_bytes);
  } else {
    operator delete(ptr, num_bytes, std::align_val_t(alignment));
  }
#else
  if (alignment <= __STDCPP_DEFAULT_NEW_ALIGNMENT__) {
    operator delete(ptr);
  } else {
    operator delete(ptr, std::align_val_t(alignment));
  }
#endif
#else
#ifdef __STDCPP_DEFAULT_NEW_ALIGNMENT__
  constexpr size_t kDefaultNewAlignment = __STDCPP_DEFAULT_NEW_ALIGNMENT__;
#else
  constexpr size_t kDefaultNewAlignment = alignof(max_align_t);
#endif
#if __cpp_sized_deallocation || __GXX_DELETE_WITH_SIZE__
  if (alignment <= kDefaultNewAlignment) {
    operator delete(ptr, num_bytes);
  } else {
    void* const allocated = reinterpret_cast<void**>(ptr)[-1];
    RIEGELI_ASSERT(
        ptr ==
        reinterpret_cast<void*>(RoundUp<alignment>(reinterpret_cast<uintptr_t>(
            static_cast<char*>(allocated) + sizeof(void*)))))
        << "Failed precondition of DeleteAligned(): "
           "the pointer was not obtained from NewAligned(), "
           "or alignment does not match, "
           "or memory before the allocated block got corrupted";
    operator delete(allocated, sizeof(void*) + num_bytes + alignment -
                                   kDefaultNewAlignment);
  }
#else
  if (alignment <= kDefaultNewAlignment) {
    operator delete(ptr);
  } else {
    operator delete(reinterpret_cast<void**>(ptr)[-1]);
  }
#endif
#endif
}

// `SizeReturningNewAligned()` is like `NewAligned()`, but it returns the number
// of bytes actually allocated, which can be greater than the requested number
// of bytes.
//
// The object can be freed with `DeleteAligned()`, passing either
// `min_num_bytes` or `*actual_num_bytes`, or anything between.
//
// `*actual_num_bytes` is already set during the constructor call.
template <typename T, size_t alignment = alignof(T), typename... Args>
inline T* SizeReturningNewAligned(size_t min_num_bytes,
                                  size_t* actual_num_bytes, Args&&... args) {
  static_assert(absl::has_single_bit(alignment),
                "alignment must be a power of 2");
  new_aligned_internal::EnsureSpaceForObject<T>(min_num_bytes);
  T* ptr;
  const size_t capacity = EstimatedAllocatedSize(min_num_bytes);
#if __cpp_aligned_new
  if (alignment <= __STDCPP_DEFAULT_NEW_ALIGNMENT__) {
    ptr = static_cast<T*>(operator new(capacity));
  } else {
    ptr = static_cast<T*>(operator new(capacity, std::align_val_t(alignment)));
  }
#else
#ifdef __STDCPP_DEFAULT_NEW_ALIGNMENT__
  constexpr size_t kDefaultNewAlignment = __STDCPP_DEFAULT_NEW_ALIGNMENT__;
#else
  constexpr size_t kDefaultNewAlignment = alignof(max_align_t);
#endif
  if (alignment <= kDefaultNewAlignment) {
    ptr = static_cast<T*>(operator new(capacity));
  } else {
    RIEGELI_CHECK_LE(capacity, std::numeric_limits<size_t>::max() -
                                   sizeof(void*) - alignment +
                                   kDefaultNewAlignment)
        << "Out of memory";
    void* const allocated = operator new(sizeof(void*) + capacity + alignment -
                                         kDefaultNewAlignment);
    void* const aligned =
        reinterpret_cast<void*>(RoundUp<alignment>(reinterpret_cast<uintptr_t>(
            static_cast<char*>(allocated) + sizeof(void*))));
    reinterpret_cast<void**>(aligned)[-1] = allocated;
    ptr = static_cast<T*>(aligned);
  }
#endif
  *actual_num_bytes = capacity;
  new_aligned_internal::ConstructObject(ptr, std::forward<Args>(args)...);
  return ptr;
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_NEW_ALIGNED_H_
