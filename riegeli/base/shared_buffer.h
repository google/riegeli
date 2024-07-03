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

#include <functional>
#include <iosfwd>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/external_data.h"
#include "riegeli/base/external_ref.h"
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

  // Converts a substring of `*this` to `ExternalRef`.
  //
  // `storage` must outlive usages of the returned `ExternalRef`.
  //
  // Precondition:
  //   if `!substr.empty()` then `substr` is a substring of
  //       [`data()`..`data() + capacity()`).
  ExternalRef ToExternalRef(
      absl::string_view substr,
      ExternalRef::StorageSubstr<const SharedBuffer&>&& storage
          ABSL_ATTRIBUTE_LIFETIME_BOUND =
              ExternalRef::StorageSubstr<const SharedBuffer&>()) const& {
    if (!substr.empty()) {
      RIEGELI_ASSERT(std::greater_equal<>()(substr.data(), data()))
          << "Failed precondition of SharedBuffer::ToExternalRef(): "
             "substring not contained in the buffer";
      RIEGELI_ASSERT(std::less_equal<>()(substr.data() + substr.size(),
                                         data() + capacity()))
          << "Failed precondition of SharedBuffer::ToExternalRef(): "
             "substring not contained in the buffer";
    }
    return ExternalRef(*this, substr, std::move(storage));
  }
  ExternalRef ToExternalRef(
      absl::string_view substr,
      ExternalRef::StorageSubstr<SharedBuffer&&>&& storage
          ABSL_ATTRIBUTE_LIFETIME_BOUND =
              ExternalRef::StorageSubstr<SharedBuffer&&>()) && {
    if (!substr.empty()) {
      RIEGELI_ASSERT(std::greater_equal<>()(substr.data(), data()))
          << "Failed precondition of SharedBuffer::ToExternalRef(): "
             "substring not contained in the buffer";
      RIEGELI_ASSERT(std::less_equal<>()(substr.data() + substr.size(),
                                         data() + capacity()))
          << "Failed precondition of SharedBuffer::ToExternalRef(): "
             "substring not contained in the buffer";
    }
    return ExternalRef(std::move(*this), substr, std::move(storage));
  }

  // Support `ExternalRef`.
  friend size_t RiegeliAllocatedMemory(const SharedBuffer* self) {
    if (self->buffer_ == nullptr) return 0;
    return sizeof(size_t) + sizeof(Buffer) + self->buffer_->capacity();
  }

  // Support `ExternalRef`.
  friend ExternalStorage RiegeliToExternalStorage(SharedBuffer* self) {
    return ExternalStorage(self->buffer_.Release(), [](void* ptr) {
      SharedPtr<Buffer>::DeleteReleased(static_cast<Buffer*>(ptr));
    });
  }

  // Support `ExternalRef` and `Chain::Block`.
  friend void RiegeliDumpStructure(const SharedBuffer* self,
                                   absl::string_view substr,
                                   std::ostream& out) {
    self->DumpStructure(substr, out);
  }

  // Support `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const SharedBuffer* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&self->buffer_);
  }

 private:
  void DumpStructure(absl::string_view substr, std::ostream& out) const;

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

}  // namespace riegeli

#endif  // RIEGELI_BASE_SHARED_BUFFER_H_
