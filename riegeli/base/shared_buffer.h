// Copyright 2020 Google LLC
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

#ifndef RIEGELI_BASE_SHARED_BUFFER_H_
#define RIEGELI_BASE_SHARED_BUFFER_H_

#include <stddef.h>

#include "absl/base/attributes.h"
#include "absl/strings/cord.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/shared_ptr.h"

namespace riegeli {

// Dynamically allocated byte buffer.
//
// Like `Buffer`, but ownership of the data can be shared.
class SharedBuffer {
 public:
  SharedBuffer() = default;

  // Ensures at least `min_capacity` of space.
  explicit SharedBuffer(size_t min_capacity);

  SharedBuffer(const SharedBuffer& that) = default;
  SharedBuffer& operator=(const SharedBuffer& that) = default;

  // The source `SharedBuffer` is left deallocated.
  SharedBuffer(SharedBuffer&& that) = default;
  SharedBuffer& operator=(SharedBuffer&& that) = default;

  // Ensures at least `min_capacity` of space, and unique ownership of the data
  // if `min_capacity > 0`. Existing contents are lost.
  //
  // Drops the allocation if the resulting capacity would be wasteful for
  // `min_capacity`.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(size_t min_capacity = 0);

  // Returns `true` if `*this` is the only owner of the data.
  //
  // If `capacity() == 0`, returns `false`.
  bool IsUnique() const;

  // Returns the mutable data pointer.
  //
  // Precondition: `IsUnique()`.
  char* mutable_data() const;

  // Returns the const data pointer.
  const char* data() const;

  // Returns the usable data size. It can be greater than the requested size.
  size_t capacity() const;

  // Returns an opaque pointer, which represents a share of ownership of the
  // data; an active share keeps the data alive. The returned pointer must be
  // deleted using `DeleteShared()`.
  //
  // If the returned pointer is `nullptr`, it allowed but not required to call
  // `DeleteShared()`.
  void* Share() const&;
  void* Share() &&;

  // Deletes the pointer obtained by `Share()`.
  //
  // Does nothing if `ptr == nullptr`.
  static void DeleteShared(void* ptr);

  // Converts [`data`..`data + length`) to `absl::Cord`.
  //
  // If `data != nullptr || length > 0` then [`data`..`data + length`) must be
  //  contained in `*this`.
  absl::Cord ToCord(const char* data, size_t length) const&;
  absl::Cord ToCord(const char* data, size_t length) &&;

  // Appends [`data`..`data + length`) to `dest`.
  //
  // If `data != nullptr || length > 0` then [`data`..`data + length`) must be
  // contained in `*this`.
  void AppendSubstrTo(const char* data, size_t length, absl::Cord& dest) const&;
  void AppendSubstrTo(const char* data, size_t length, absl::Cord& dest) &&;

  // Prepends [`data`..`data + length`) to `dest`.
  //
  // If `data != nullptr || length > 0` then [`data`..`data + length`) must be
  // contained in `*this`.
  void PrependSubstrTo(const char* data, size_t length,
                       absl::Cord& dest) const&;
  void PrependSubstrTo(const char* data, size_t length, absl::Cord& dest) &&;

  // Support `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const SharedBuffer* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&self->buffer_);
  }

 private:
  SharedPtr<Buffer> buffer_;  // `nullptr` means `capacity() == 0`.
};

// Implementation details follow.

inline SharedBuffer::SharedBuffer(size_t min_capacity)
    : buffer_(min_capacity == 0
                  ? nullptr
                  : SharedPtr<Buffer>(riegeli::Maker(min_capacity))) {}

inline void SharedBuffer::Reset(size_t min_capacity) {
  if (min_capacity == 0) {
    if (!buffer_.IsUnique()) buffer_.Reset();
  } else {
    buffer_.Reset(riegeli::Maker(min_capacity));
  }
}

inline bool SharedBuffer::IsUnique() const { return buffer_.IsUnique(); }

inline char* SharedBuffer::mutable_data() const {
  RIEGELI_ASSERT(IsUnique())
      << "Failed precondition of SharedBuffer::mutable_data(): "
         "ownership is shared";
  return buffer_->data();
}

inline const char* SharedBuffer::data() const {
  if (buffer_ == nullptr) return nullptr;
  return buffer_->data();
}

inline size_t SharedBuffer::capacity() const {
  if (buffer_ == nullptr) return 0;
  return buffer_->capacity();
}

inline void* SharedBuffer::Share() const& {
  return SharedPtr<Buffer>(buffer_).Release();
}

inline void* SharedBuffer::Share() && { return buffer_.Release(); }

inline void SharedBuffer::DeleteShared(void* ptr) {
  SharedPtr<Buffer>::DeleteReleased(static_cast<Buffer*>(ptr));
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_SHARED_BUFFER_H_
