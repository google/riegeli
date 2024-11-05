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

#include <iosfwd>

#include "absl/base/attributes.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/external_data.h"
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
  bool IsUnique() const { return buffer_.IsUnique(); }

  // Returns the current reference count.
  //
  // If the `SharedBuffer` is accessed by multiple threads, this is a snapshot
  // of the count which may change asynchronously, hence usage of
  // `GetRefCount()` should be limited to cases not important for correctness,
  // like producing debugging output.
  //
  // The reference count can be reliably compared against 1 with `IsUnique()`.
  size_t GetRefCount() const { return buffer_.GetRefCount(); }

  // Returns the mutable data pointer.
  //
  // Precondition: `IsUnique()`.
  char* mutable_data() const ABSL_ATTRIBUTE_LIFETIME_BOUND;

  // Returns the const data pointer.
  const char* data() const ABSL_ATTRIBUTE_LIFETIME_BOUND;

  // Returns the usable data size. It can be greater than the requested size.
  size_t capacity() const;

  // Indicate support for:
  //  * `ExternalRef(const SharedBuffer&, substr)`
  //  * `ExternalRef(SharedBuffer&&, substr)`
  friend void RiegeliSupportsExternalRef(const SharedBuffer*) {}

  // Support `ExternalRef`.
  friend size_t RiegeliExternalMemory(const SharedBuffer* self) {
    return RiegeliExternalMemory(&self->buffer_);
  }

  // Support `ExternalRef`.
  friend ExternalStorage RiegeliToExternalStorage(SharedBuffer* self) {
    return RiegeliToExternalStorage(&self->buffer_);
  }

  // Support `ExternalRef` and `Chain::Block`.
  friend void RiegeliDumpStructure(const SharedBuffer* self,
                                   absl::string_view substr,
                                   std::ostream& dest) {
    self->DumpStructure(substr, dest);
  }

  // Support `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const SharedBuffer* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&self->buffer_);
  }

 private:
  void DumpStructure(absl::string_view substr, std::ostream& dest) const;

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

inline char* SharedBuffer::mutable_data() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
  RIEGELI_ASSERT(IsUnique())
      << "Failed precondition of SharedBuffer::mutable_data(): "
         "ownership is shared";
  return buffer_->data();
}

inline const char* SharedBuffer::data() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
  if (buffer_ == nullptr) return nullptr;
  return buffer_->data();
}

inline size_t SharedBuffer::capacity() const {
  if (buffer_ == nullptr) return 0;
  return buffer_->capacity();
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_SHARED_BUFFER_H_
