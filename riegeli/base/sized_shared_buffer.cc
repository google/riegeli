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

#include "riegeli/base/sized_shared_buffer.h"

#include <stddef.h>

#include <cstring>
#include <limits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/types/span.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/shared_buffer.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if !__cpp_inline_variables
constexpr size_t SizedSharedBuffer::kAnyLength;
#endif

void SizedSharedBuffer::ShrinkSlow(size_t max_size) {
  RIEGELI_ASSERT_GE(max_size, size_)
      << "Failed precondition of SizedSharedBuffer::ShrinkSlow(): "
         "max_size less than current size";
  if (size_ == 0) {
    buffer_ = SharedBuffer();
    data_ = nullptr;
    return;
  }
  SharedBuffer new_buffer(max_size);
  char* const new_data = new_buffer.mutable_data();
  std::memcpy(new_data, data_, size_);
  data_ = new_data;
  buffer_ = std::move(new_buffer);
}

inline size_t SizedSharedBuffer::space_before() const {
  RIEGELI_ASSERT(data_ != nullptr || buffer_.data() == nullptr)
      << "Failed precondition of SizedSharedBuffer::space_before(): null data_";
  return PtrDistance(buffer_.data(), data_);
}

inline size_t SizedSharedBuffer::space_after() const {
  RIEGELI_ASSERT(data_ != nullptr || buffer_.data() == nullptr)
      << "Failed precondition of SizedSharedBuffer::space_after(): null data_";
  return PtrDistance(data_ + size_, buffer_.data() + buffer_.capacity());
}

inline bool SizedSharedBuffer::CanAppendMovingData(size_t length,
                                                   size_t& min_length_if_not) {
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - size_)
      << "Failed precondition of SizedSharedBuffer::CanAppendMovingData(): "
         "SizedSharedBuffer size overflow";
  if (buffer_.IsUnique()) {
    if (empty()) data_ = buffer_.mutable_data();
    if (space_after() >= length) return true;
    if (size_ + length <= capacity() && 2 * size_ <= capacity()) {
      // Existing array has enough capacity and is at most half full: move
      // contents to the beginning of the array. This is enough to make the
      // amortized cost of adding one element constant as long as prepending
      // leaves space at both ends.
      char* const new_data = buffer_.mutable_data();
      std::memmove(new_data, data_, size_);
      data_ = new_data;
      return true;
    }
    min_length_if_not = UnsignedMin(
        UnsignedMax(length, SaturatingAdd(empty() ? capacity() : space_after(),
                                          capacity() / 2)),
        std::numeric_limits<size_t>::max() - size_);
  } else {
    min_length_if_not = length;
  }
  return false;
}

inline bool SizedSharedBuffer::CanPrependMovingData(size_t length,
                                                    size_t& space_after_if_not,
                                                    size_t& min_length_if_not) {
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - size_)
      << "Failed precondition of SizedSharedBuffer::CanPrependMovingData(): "
         "SizedSharedBuffer size overflow";
  if (buffer_.IsUnique()) {
    if (empty()) data_ = buffer_.mutable_data() + buffer_.capacity();
    if (space_before() >= length) return true;
    if (size_ + length <= capacity() && 2 * size_ <= capacity()) {
      // Existing array has enough capacity and is at most half full: move
      // contents to the middle of the array. This makes the amortized cost of
      // adding one element constant.
      char* const new_data =
          buffer_.mutable_data() + (capacity() - size_ + length) / 2;
      std::memmove(new_data, data_, size_);
      data_ = new_data;
      return true;
    }
    min_length_if_not = UnsignedMin(
        UnsignedMax(length, SaturatingAdd(empty() ? capacity() : space_before(),
                                          capacity() / 2)),
        std::numeric_limits<size_t>::max() - size_);
    space_after_if_not =
        UnsignedMin(space_after(), std::numeric_limits<size_t>::max() - size_ -
                                       min_length_if_not);
  } else {
    min_length_if_not = length;
    space_after_if_not = 0;
  }
  return false;
}

inline size_t SizedSharedBuffer::NewCapacity(size_t extra_space,
                                             size_t min_length,
                                             size_t recommended_length) const {
  RIEGELI_ASSERT_LE(extra_space, std::numeric_limits<size_t>::max() - size_)
      << "Failed precondition of SizedSharedBuffer::NewCapacity(): "
         "SizedSharedBuffer size overflow";
  const size_t existing_space = size_ + extra_space;
  RIEGELI_ASSERT_LE(min_length,
                    std::numeric_limits<size_t>::max() - existing_space)
      << "Failed precondition of SizedSharedBuffer::NewCapacity(): "
         "SizedSharedBuffer size overflow";
  return existing_space +
         UnsignedClamp(recommended_length, min_length,
                       std::numeric_limits<size_t>::max() - existing_space);
}

absl::Span<char> SizedSharedBuffer::AppendBuffer(
    size_t min_length, size_t recommended_length,
    size_t max_length) ABSL_ATTRIBUTE_LIFETIME_BOUND {
  RIEGELI_ASSERT_LE(min_length, max_length)
      << "Failed precondition of SizedSharedBuffer::AppendBuffer(): "
         "min_length > max_length";
  RIEGELI_CHECK_LE(min_length, std::numeric_limits<size_t>::max() - size())
      << "Failed precondition of SizedSharedBuffer::AppendBuffer(): "
         "SizedSharedBuffer size overflow";
  size_t new_min_length;
  if (!CanAppendMovingData(min_length, new_min_length)) {
    if (min_length == 0) return absl::Span<char>();
    // Reallocate the array, without keeping space before the contents. This
    // is enough to make the amortized cost of adding one element constant if
    // prepending leaves space at both ends.
    const size_t new_capacity =
        NewCapacity(0, new_min_length, recommended_length);
    if (empty()) {
      buffer_.Reset(new_capacity);
    } else {
      SharedBuffer new_buffer(new_capacity);
      std::memcpy(new_buffer.mutable_data(), data_, size_);
      buffer_ = std::move(new_buffer);
    }
    data_ = buffer_.mutable_data();
  }
  const size_t length = UnsignedMin(space_after(), max_length);
  const absl::Span<char> buffer(data_ + size_, length);
  size_ += length;
  return buffer;
}

absl::Span<char> SizedSharedBuffer::PrependBuffer(
    size_t min_length, size_t recommended_length,
    size_t max_length) ABSL_ATTRIBUTE_LIFETIME_BOUND {
  RIEGELI_ASSERT_LE(min_length, max_length)
      << "Failed precondition of SizedSharedBuffer::PrependBuffer(): "
         "min_length > max_length";
  RIEGELI_CHECK_LE(min_length, std::numeric_limits<size_t>::max() - size())
      << "Failed precondition of SizedSharedBuffer::PrependBuffer(): "
         "SizedSharedBuffer size overflow";
  size_t space_after, new_min_length;
  if (!CanPrependMovingData(min_length, space_after, new_min_length)) {
    if (min_length == 0) return absl::Span<char>();
    // Reallocate the array, keeping space after the contents unchanged. This
    // makes the amortized cost of adding one element constant.
    const size_t new_capacity =
        NewCapacity(space_after, new_min_length, recommended_length);
    char* new_data;
    if (empty()) {
      buffer_.Reset(new_capacity);
      new_data = buffer_.mutable_data() + buffer_.capacity() - space_after;
    } else {
      SharedBuffer new_buffer(new_capacity);
      new_data = new_buffer.mutable_data() + new_buffer.capacity() -
                 space_after - size_;
      std::memcpy(new_data, data_, size_);
      buffer_ = std::move(new_buffer);
    }
    data_ = new_data;
  }
  const size_t length = UnsignedMin(space_before(), max_length);
  data_ -= length;
  size_ += length;
  return absl::Span<char>(data_, length);
}

absl::Span<char> SizedSharedBuffer::AppendBufferIfExisting(size_t length)
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  size_t new_min_length;
  if (ABSL_PREDICT_FALSE(length >
                         std::numeric_limits<size_t>::max() - size()) ||
      !CanAppendMovingData(length, new_min_length)) {
    return absl::Span<char>();
  }
  const absl::Span<char> buffer(data_ + size_, length);
  size_ += length;
  return buffer;
}

absl::Span<char> SizedSharedBuffer::PrependBufferIfExisting(size_t length)
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  size_t space_after, new_min_length;
  if (ABSL_PREDICT_FALSE(length >
                         std::numeric_limits<size_t>::max() - size()) ||
      !CanPrependMovingData(length, space_after, new_min_length)) {
    return absl::Span<char>();
  }
  data_ -= length;
  size_ += length;
  return absl::Span<char>(data_, length);
}

}  // namespace riegeli
