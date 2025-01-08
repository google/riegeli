// Copyright 2023 Google LLC
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

#ifndef RIEGELI_BASE_SIZED_SHARED_BUFFER_H_
#define RIEGELI_BASE_SIZED_SHARED_BUFFER_H_

#include <stddef.h>

#include <limits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/shared_buffer.h"

namespace riegeli {

// Dynamically allocated byte buffer.
//
// Like `SharedBuffer`, but keeps track of the substring which is used.
class
#ifdef ABSL_ATTRIBUTE_TRIVIAL_ABI
    ABSL_ATTRIBUTE_TRIVIAL_ABI
#endif
        SizedSharedBuffer {
 public:
  // A sentinel value for the `max_length` parameter of
  // `AppendBuffer()`/`PrependBuffer()`.
  static constexpr size_t kAnyLength = std::numeric_limits<size_t>::max();

  SizedSharedBuffer() = default;

  SizedSharedBuffer(const SizedSharedBuffer& that) = default;
  SizedSharedBuffer& operator=(const SizedSharedBuffer& that) = default;

  // The source `SizedSharedBuffer` is left empty.
  SizedSharedBuffer(SizedSharedBuffer&& that) noexcept;
  SizedSharedBuffer& operator=(SizedSharedBuffer&& that) noexcept;

  // Removes all data.
  ABSL_ATTRIBUTE_REINITIALIZES void Clear();

  explicit operator absl::string_view() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return absl::string_view(data_, size_);
  }

  // Returns `true` if the data size is 0.
  bool empty() const { return size_ == 0; }

  // Returns the data pointer.
  const char* data() const ABSL_ATTRIBUTE_LIFETIME_BOUND { return data_; }

  // Returns the data size.
  size_t size() const { return size_; }

  // Returns the allocated size, to which the `SizedSharedBuffer` can be resized
  // without reallocation.
  size_t capacity() const { return buffer_.capacity(); }

  // Reduces the allocation if the capacity would be wasteful for
  // `max(size(), max_size)`, assuming that `max_size` will be needed later.
  void Shrink(size_t max_size = 0);

  // Removes all data.
  //
  // Drops the allocation if the capacity would be wasteful for `max_size`.
  ABSL_ATTRIBUTE_REINITIALIZES void ClearAndShrink(size_t max_size = 0);

  // Appends/prepends some uninitialized space. The buffer will have length at
  // least `min_length`, preferably `recommended_length`, and at most
  // `max_length`.
  //
  // If `min_length == 0`, returns whatever space was already allocated
  // (possibly an empty buffer) without invalidating existing pointers. If the
  // `SizedSharedBuffer` was empty then the empty contents can be moved.
  //
  // If `recommended_length < min_length`, `recommended_length` is assumed to be
  // `min_length`.
  //
  // If `max_length == kAnyLength`, there is no maximum.
  //
  // Precondition: `min_length <= max_length`
  absl::Span<char> AppendBuffer(
      size_t min_length, size_t recommended_length = 0,
      size_t max_length = kAnyLength) ABSL_ATTRIBUTE_LIFETIME_BOUND;
  absl::Span<char> PrependBuffer(
      size_t min_length, size_t recommended_length = 0,
      size_t max_length = kAnyLength) ABSL_ATTRIBUTE_LIFETIME_BOUND;

  // Equivalent to `AppendBuffer()`/`PrependBuffer()` with
  // `min_length == max_length`.
  absl::Span<char> AppendFixedBuffer(size_t length)
      ABSL_ATTRIBUTE_LIFETIME_BOUND;
  absl::Span<char> PrependFixedBuffer(size_t length)
      ABSL_ATTRIBUTE_LIFETIME_BOUND;

  // Appends/prepends some uninitialized space with the given `length` if this
  // is possible without invalidating existing pointers, otherwise returns an
  // empty buffer. If the `SizedSharedBuffer` was empty then the empty contents
  // can be moved.
  //
  // In contrast to `AppendBuffer(0, length, length)`, the returned buffer has
  // size either 0 or `length`, nothing between.
  absl::Span<char> AppendBufferIfExisting(size_t length)
      ABSL_ATTRIBUTE_LIFETIME_BOUND;
  absl::Span<char> PrependBufferIfExisting(size_t length)
      ABSL_ATTRIBUTE_LIFETIME_BOUND;

  // Removes suffix/prefix of the given length.
  //
  // Precondition: `length <= size()`
  void RemoveSuffix(size_t length);
  void RemovePrefix(size_t length);

  // Indicates support for:
  //  * `ExternalRef(const SizedSharedBuffer&)`
  //  * `ExternalRef(SizedSharedBuffer&&)`
  //  * `ExternalRef(const SizedSharedBuffer&, substr)`
  //  * `ExternalRef(SizedSharedBuffer&&, substr)`
  friend void RiegeliSupportsExternalRef(const SizedSharedBuffer*) {}

  // Supports `ExternalRef`.
  template <typename Callback>
  friend void RiegeliExternalDelegate(SizedSharedBuffer* self,
                                      absl::string_view substr,
                                      Callback&& delegate_to) {
    self->data_ = nullptr;
    self->size_ = 0;
    std::forward<Callback>(delegate_to)(std::move(self->buffer_), substr);
  }
  template <typename Callback>
  friend void RiegeliExternalDelegate(const SizedSharedBuffer* self,
                                      absl::string_view substr,
                                      Callback&& delegate_to) {
    std::forward<Callback>(delegate_to)(self->buffer_, substr);
  }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const SizedSharedBuffer* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&self->buffer_);
  }

 private:
  explicit SizedSharedBuffer(SharedBuffer buffer, char* data, size_t size)
      : buffer_(std::move(buffer)), data_(data), size_(size) {}

  void ShrinkSlow(size_t max_size);

  size_t space_before() const;
  size_t space_after() const;
  bool CanAppendMovingData(size_t length, size_t& min_length_if_not);
  bool CanPrependMovingData(size_t length, size_t& space_after_if_not,
                            size_t& min_length_if_not);

  size_t NewCapacity(size_t extra_space, size_t min_length,
                     size_t recommended_length) const;

  void RemoveSuffixSlow(size_t length);
  void RemovePrefixSlow(size_t length);

  SharedBuffer buffer_;
  // Invariant:
  //   `(data_ == nullptr && size_ == 0) ||
  //    (data_ >= buffer_.data() &&
  //     data_ + size_ <= buffer_.data() + buffer_.capacity())`
  char* data_ = nullptr;
  size_t size_ = 0;
};

// Implementation details follow.

inline SizedSharedBuffer::SizedSharedBuffer(SizedSharedBuffer&& that) noexcept
    : buffer_(std::move(that.buffer_)),
      data_(std::exchange(that.data_, nullptr)),
      size_(std::exchange(that.size_, 0)) {}

inline SizedSharedBuffer& SizedSharedBuffer::operator=(
    SizedSharedBuffer&& that) noexcept {
  buffer_ = std::move(that.buffer_);
  data_ = std::exchange(that.data_, nullptr);
  size_ = std::exchange(that.size_, 0);
  return *this;
}

inline void SizedSharedBuffer::Clear() { size_ = 0; }

inline void SizedSharedBuffer::Shrink(size_t max_size) {
  max_size = UnsignedMax(max_size, size_);
  if (capacity() > max_size && Wasteful(capacity(), max_size)) {
    ShrinkSlow(max_size);
  }
}

inline void SizedSharedBuffer::ClearAndShrink(size_t max_size) {
  size_ = 0;
  if (capacity() > max_size && Wasteful(capacity(), max_size)) {
    buffer_ = SharedBuffer();
    data_ = nullptr;
  }
}

inline absl::Span<char> SizedSharedBuffer::AppendFixedBuffer(size_t length)
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  return AppendBuffer(length, length, length);
}

inline absl::Span<char> SizedSharedBuffer::PrependFixedBuffer(size_t length)
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  return PrependBuffer(length, length, length);
}

inline void SizedSharedBuffer::RemoveSuffix(size_t length) {
  RIEGELI_CHECK_LE(length, size())
      << "Failed precondition of SizedSharedBuffer::RemoveSuffix(): "
      << "length to remove greater than current size";
  size_ -= length;
}

inline void SizedSharedBuffer::RemovePrefix(size_t length) {
  RIEGELI_CHECK_LE(length, size())
      << "Failed precondition of SizedSharedBuffer::RemovePrefix(): "
      << "length to remove greater than current size";
  data_ += length;
  size_ -= length;
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_SIZED_SHARED_BUFFER_H_
