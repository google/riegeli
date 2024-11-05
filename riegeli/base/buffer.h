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

#include <iosfwd>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/estimated_allocated_size.h"
#include "riegeli/base/external_data.h"

namespace riegeli {

// Dynamically allocated byte buffer.
class
#ifdef ABSL_ATTRIBUTE_TRIVIAL_ABI
    ABSL_ATTRIBUTE_TRIVIAL_ABI
#endif
        Buffer {
 public:
  Buffer() = default;

  // Ensures at least `min_capacity` of space.
  explicit Buffer(size_t min_capacity);

  // The source `Buffer` is left deallocated.
  Buffer(Buffer&& that) noexcept;
  Buffer& operator=(Buffer&& that) noexcept;

  ~Buffer() { DeleteInternal(); }

  // Ensures at least `min_capacity` of space. Existing contents are lost.
  //
  // Drops the allocation if the resulting capacity would be wasteful for
  // `min_capacity`.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(size_t min_capacity = 0);

  // Returns the data pointer.
  char* data() const ABSL_ATTRIBUTE_LIFETIME_BOUND { return data_; }

  // Returns the usable data size. It can be greater than the requested size.
  size_t capacity() const { return capacity_; }

  // Indicate support for `ExternalRef(Buffer&&, substr)`.
  friend void RiegeliSupportsExternalRef(Buffer*) {}

  // Support `ExternalRef`.
  friend size_t RiegeliExternalMemory(const Buffer* self) {
    return self->capacity();
  }

  // Support `ExternalRef`.
  friend ExternalStorage RiegeliToExternalStorage(Buffer* self) {
    self->capacity_ = 0;
    return ExternalStorage(
        std::exchange(self->data_, nullptr), operator delete);
  }

  // Support `ExternalRef` and `Chain::Block`.
  friend void RiegeliDumpStructure(const Buffer* self, absl::string_view substr,
                                   std::ostream& dest) {
    self->DumpStructure(substr, dest);
  }

  // Support `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const Buffer* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterDynamicMemory(self->data_, self->capacity_);
  }

 private:
  void AllocateInternal(size_t min_capacity);
  void DeleteInternal();
  void DumpStructure(absl::string_view substr, std::ostream& dest) const;

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
    if (capacity_ >= min_capacity && !Wasteful(capacity_, min_capacity)) return;
    DeleteInternal();
    data_ = nullptr;
    capacity_ = 0;
  }
  AllocateInternal(min_capacity);
}

inline void Buffer::AllocateInternal(size_t min_capacity) {
  if (min_capacity > 0) {
    const size_t capacity = EstimatedAllocatedSize(min_capacity);
    data_ = static_cast<char*>(operator new(capacity));
    capacity_ = capacity;
  }
}

inline void Buffer::DeleteInternal() {
#if __cpp_sized_deallocation || __GXX_DELETE_WITH_SIZE__
  if (data_ != nullptr) operator delete(data_, capacity_);
#else
  if (data_ != nullptr) operator delete(data_);
#endif
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_BUFFER_H_
