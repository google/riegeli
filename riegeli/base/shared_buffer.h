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
#include "riegeli/base/intrusive_ref_count.h"

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

  // Ensures at least `min_capacity` of space and unique ownership of the data.
  // Existing contents are lost.
  //
  // Drops the allocation if the resulting capacity would be wasteful for
  // `min_capacity`.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(size_t min_capacity = 0);

  // Returns `true` if this `SharedBuffer` is the only owner of the data.
  bool has_unique_owner() const;

  // Returns the mutable data pointer.
  //
  // Precondition: `has_unique_owner()`.
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

  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const SharedBuffer* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&self->payload_);
  }

 private:
  struct Payload : RefCountedBase<Payload> {
    explicit Payload(size_t min_capacity) : buffer(min_capacity) {}

    template <typename MemoryEstimator>
    friend void RiegeliRegisterSubobjects(const Payload* self,
                                          MemoryEstimator& memory_estimator) {
      memory_estimator.RegisterSubobjects(&self->buffer);
    }

    Buffer buffer;
  };

  void AllocateInternal(size_t min_capacity);

  RefCountedPtr<Payload> payload_;
};

// Implementation details follow.

inline SharedBuffer::SharedBuffer(size_t min_capacity) {
  AllocateInternal(min_capacity);
}

inline void SharedBuffer::Reset(size_t min_capacity) {
  if (payload_ != nullptr) {
    if (payload_->has_unique_owner()) {
      payload_->buffer.Reset(min_capacity);
      return;
    }
    payload_.reset();
  }
  AllocateInternal(min_capacity);
}

inline bool SharedBuffer::has_unique_owner() const {
  return payload_ == nullptr || payload_->has_unique_owner();
}

inline char* SharedBuffer::mutable_data() const {
  RIEGELI_ASSERT(has_unique_owner())
      << "Failed precondition of SharedBuffer::mutable_data(): "
         "ownership is shared";
  if (payload_ == nullptr) return nullptr;
  return payload_->buffer.data();
}

inline const char* SharedBuffer::data() const {
  if (payload_ == nullptr) return nullptr;
  return payload_->buffer.data();
}

inline size_t SharedBuffer::capacity() const {
  if (payload_ == nullptr) return 0;
  return payload_->buffer.capacity();
}

inline void SharedBuffer::AllocateInternal(size_t min_capacity) {
  if (min_capacity > 0) payload_ = MakeRefCounted<Payload>(min_capacity);
}

inline void* SharedBuffer::Share() const& {
  if (payload_ == nullptr) return nullptr;
  payload_->Ref();
  return payload_.get();
}

inline void* SharedBuffer::Share() && { return payload_.release(); }

inline void SharedBuffer::DeleteShared(void* ptr) {
  if (ptr != nullptr) static_cast<Payload*>(ptr)->Unref();
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_SHARED_BUFFER_H_
