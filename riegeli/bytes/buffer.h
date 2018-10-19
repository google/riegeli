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

#ifndef RIEGELI_BYTES_BUFFER_H_
#define RIEGELI_BYTES_BUFFER_H_

#include <stddef.h>
#include <memory>

#include "absl/base/optimization.h"
#include "absl/utility/utility.h"
#include "riegeli/base/base.h"

namespace riegeli {
namespace internal {

// Lazily allocated buffer.
class Buffer {
 public:
  Buffer() noexcept {}

  // Remembers the size to be allocated.
  explicit Buffer(size_t size) noexcept : size_(size) {}

  // The source Buffer is left deallocated but with size unchanged.
  Buffer(Buffer&& that) noexcept;
  Buffer& operator=(Buffer&& that) noexcept;

  ~Buffer() { DeleteBuffer(); }

  // If the buffer is not allocated, allocates it. Returns the data pointer.
  //
  // This method is not thread-safe.
  //
  // Precondition: size() > 0
  char* GetData();

  // Returns the data size, or the planned size if not allocated yet.
  const size_t size() const { return size_; }

 private:
  // If the buffer is allocated, deletes it.
  void DeleteBuffer();

  char* data_ = nullptr;
  size_t size_ = 0;
};

// Implementation details follow.

inline Buffer::Buffer(Buffer&& that) noexcept
    : data_(absl::exchange(that.data_, nullptr)), size_(that.size_) {}

inline Buffer& Buffer::operator=(Buffer&& that) noexcept {
  if (that.data_ != nullptr || size_ != that.size_) {
    // Exchange that.data_ early to support self-assignment.
    char* const data = absl::exchange(that.data_, nullptr);
    DeleteBuffer();
    data_ = data;
    size_ = that.size_;
  } else {
    // Keep data_ unchanged, and size_ is already the same.
  }
  return *this;
}

inline void Buffer::DeleteBuffer() {
  if (data_ != nullptr) std::allocator<char>().deallocate(data_, size_);
}

inline char* Buffer::GetData() {
  if (ABSL_PREDICT_FALSE(data_ == nullptr)) {
    RIEGELI_ASSERT_GT(size_, 0u)
        << "Failed precondition of Buffer::GetData(): no buffer size specified";
    data_ = std::allocator<char>().allocate(size_);
  }
  return data_;
}

}  // namespace internal
}  // namespace riegeli

#endif  // RIEGELI_BYTES_BUFFER_H_
