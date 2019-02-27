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

#include "absl/base/optimization.h"
#include "absl/utility/utility.h"
#include "riegeli/base/base.h"
#include "riegeli/base/memory.h"

namespace riegeli {

// Lazily allocated buffer of a fixed size.
class Buffer {
 public:
  Buffer() noexcept {}

  // Stores the minimal size to be allocated. Does not allocate the buffer yet.
  explicit Buffer(size_t size) noexcept : size_(size) {}

  // The source Buffer is left deallocated but with size unchanged.
  Buffer(Buffer&& that) noexcept;
  Buffer& operator=(Buffer&& that) noexcept;

  ~Buffer() { DeleteBuffer(); }

  // If the buffer is not allocated, allocates it; this can increase the stored
  // size to account for size rounding by the memory allocator. Returns the data
  // pointer.
  //
  // This method is not thread-safe.
  //
  // Precondition: size() > 0
  char* GetData();

  // Returns the data size, or the planned size if not allocated yet. The size
  // can increase when GetData() is called.
  const size_t size() const { return size_; }

  // Returns true if the buffer is already allocated and GetData() is fast.
  // Returns false if GetData() would allocate the buffer.
  bool is_allocated() const { return data_ != nullptr; }

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
  // Exchange that.data_ early to support self-assignment.
  char* const data = absl::exchange(that.data_, nullptr);
  DeleteBuffer();
  data_ = data;
  size_ = that.size_;
  return *this;
}

inline void Buffer::DeleteBuffer() {
  if (data_ != nullptr) {
#if __cpp_sized_deallocation || __GXX_DELETE_WITH_SIZE__
    operator delete(data_, size_);
#else
    operator delete(data_);
#endif
  }
}

inline char* Buffer::GetData() {
  if (ABSL_PREDICT_FALSE(data_ == nullptr)) {
    RIEGELI_ASSERT_GT(size_, 0u)
        << "Failed precondition of Buffer::GetData(): no buffer size specified";
    const size_t capacity = EstimatedAllocatedSize(size_);
    data_ = static_cast<char*>(operator new(capacity));
    size_ = capacity;
  }
  return data_;
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_BUFFER_H_
