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

#ifndef RIEGELI_BASE_BUFFER_H_
#define RIEGELI_BASE_BUFFER_H_

#include <stddef.h>

#include <utility>

#include "absl/base/attributes.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/estimated_allocated_size.h"

namespace riegeli {

// Dynamically allocated byte buffer.
class Buffer {
 public:
  Buffer() = default;

  // Ensures at least `min_capacity` of space.
  explicit Buffer(size_t min_capacity);

  // The source `Buffer` is left deallocated.
  Buffer(Buffer&& that) noexcept;
  Buffer& operator=(Buffer&& that) noexcept;

  ~Buffer() { DeleteInternal(); }

  // Ensures at least `min_capacity` of space. Existing contents are lost.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(size_t min_capacity);

  // Returns the data pointer.
  char* data() const { return data_; }

  // Returns the usable data size. It can be greater than the requested size.
  size_t capacity() const { return capacity_; }

  // Returns the data pointer, releasing its ownership; the `Buffer` is left
  // deallocated. The returned pointer must be deleted using `DeleteReleased()`.
  //
  // If the returned pointer is `nullptr`, it allowed but not required to call
  // `DeleteReleased()`.
  char* Release();

  // Deletes the pointer obtained by `Release()`.
  //
  // Does nothing if `ptr == nullptr`.
  static void DeleteReleased(void* ptr);

  // Converts `*this` to `absl::Cord`. `substr` must be contained in `*this`.
  // `*this` is left unchanged or deallocated.
  absl::Cord ToCord(absl::string_view substr) &&;

  // Appends `substr` to `dest`. `substr` must be contained in `*this`.
  // `*this` is left unchanged or deallocated.
  void AppendSubstrTo(absl::string_view substr, absl::Cord& dest) &&;

  // Prepends `substr` to `dest`. `substr` must be contained in `*this`.
  // `*this` is left unchanged or deallocated.
  void PrependSubstrTo(absl::string_view substr, absl::Cord& dest) &&;

 private:
  void AllocateInternal(size_t min_capacity);
  void DeleteInternal();

  char* data_ = nullptr;
  size_t capacity_ = 0;
  // Invariant: if `data_ == nullptr` then `capacity_ == 0`
};

// Implementation details follow.

inline Buffer::Buffer(size_t min_capacity) { AllocateInternal(min_capacity); }

inline Buffer::Buffer(Buffer&& that) noexcept
    : data_(std::exchange(that.data_, nullptr)),
      capacity_(std::exchange(that.capacity_, 0)) {}

inline Buffer& Buffer::operator=(Buffer&& that) noexcept {
  // Exchange `that.data_` early to support self-assignment.
  char* const data = std::exchange(that.data_, nullptr);
  DeleteInternal();
  data_ = data;
  capacity_ = std::exchange(that.capacity_, 0);
  return *this;
}

inline void Buffer::Reset(size_t min_capacity) {
  if (data_ != nullptr) {
    if (capacity_ >= min_capacity) return;
    DeleteInternal();
  }
  AllocateInternal(min_capacity);
}

inline void Buffer::AllocateInternal(size_t min_capacity) {
  const size_t capacity = EstimatedAllocatedSize(min_capacity);
  data_ = static_cast<char*>(operator new(capacity));
  capacity_ = capacity;
}

inline void Buffer::DeleteInternal() {
#if __cpp_sized_deallocation || __GXX_DELETE_WITH_SIZE__
  if (data_ != nullptr) operator delete(data_, capacity_);
#else
  if (data_ != nullptr) operator delete(data_);
#endif
}

inline char* Buffer::Release() {
  capacity_ = 0;
  return std::exchange(data_, nullptr);
}

inline void Buffer::DeleteReleased(void* ptr) { operator delete(ptr); }

}  // namespace riegeli

#endif  // RIEGELI_BASE_BUFFER_H_
