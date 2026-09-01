// Copyright 2026 Google LLC
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

#ifndef RIEGELI_INTERNED_CONCURRENT_VECTOR_INTERNAL_H_
#define RIEGELI_INTERNED_CONCURRENT_VECTOR_INTERNAL_H_

#include <stddef.h>

#include <atomic>
#include <memory>
#include <new>  // IWYU pragma: keep
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/new_aligned.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli::interned_internal {

// A fixed-capacity buffer for elements of type `T`.
//
// In contrast to `std::vector`, the capacity of `ConcurrentVectorBuffer` is
// fixed upon creation and managed by its caller. Reallocation is handled by
// `ConcurrentVector`.
template <typename T>
class ConcurrentVectorBuffer {
 public:
  ConcurrentVectorBuffer() = default;

  explicit ConcurrentVectorBuffer(size_t min_capacity) {
    RIEGELI_ASSERT_GT(min_capacity, 0u)
        << "Failed precondition of ConcurrentVectorBuffer: zero capacity";
    size_t capacity_bytes;
    data_ = static_cast<T*>(SizeReturningNewAligned<void, alignof(T)>(
        min_capacity * sizeof(T), &capacity_bytes));
    capacity_ = capacity_bytes / sizeof(T);
  }

  ConcurrentVectorBuffer(const ConcurrentVectorBuffer&) = delete;
  ConcurrentVectorBuffer& operator=(const ConcurrentVectorBuffer&) = delete;

  ConcurrentVectorBuffer(ConcurrentVectorBuffer&& that) noexcept
      : data_(std::exchange(that.data_, nullptr)),
        size_(std::exchange(that.size_, 0)),
        capacity_(std::exchange(that.capacity_, 0)) {}

  ConcurrentVectorBuffer& operator=(ConcurrentVectorBuffer&& that) noexcept {
    Delete(std::exchange(data_, std::exchange(that.data_, nullptr)),
           std::exchange(size_, std::exchange(that.size_, 0)),
           std::exchange(capacity_, std::exchange(that.capacity_, 0)));
    return *this;
  }

  ~ConcurrentVectorBuffer() { Delete(data_, size_, capacity_); }

  void clear() {
    for (size_t i = size_; i > 0;) {
      --i;
      data_[i].~T();
    }
    size_ = 0;
  }

  template <typename... Args>
  T& emplace_back(Args&&... args) {
    RIEGELI_ASSERT_LT(size_, capacity_)
        << "Failed precondition of ConcurrentVectorBuffer::emplace_back(): "
           "buffer full";
    T& result = *new (data_ + size_) T(std::forward<Args>(args)...);
    ++size_;
    return result;
  }

  // The caller is responsible for construction or destruction of elements
  // being added or removed.
  void set_size(size_t size) {
    RIEGELI_ASSERT_LE(size, capacity_)
        << "Failed precondition of ConcurrentVectorBuffer::set_size(): "
           "size exceeds capacity";
    size_ = size;
  }

  size_t size() const { return size_; }
  size_t capacity() const { return capacity_; }
  bool empty() const { return size_ == 0; }

  const T& operator[](size_t index) const {
    RIEGELI_ASSERT_LT(index, size_)
        << "Failed precondition of ConcurrentVectorBuffer::operator[]: "
           "index out of bounds";
    return data_[index];
  }
  T& operator[](size_t index) {
    RIEGELI_ASSERT_LT(index, size_)
        << "Failed precondition of ConcurrentVectorBuffer::operator[]: "
           "index out of bounds";
    return data_[index];
  }

  const T& back() const {
    RIEGELI_ASSERT_GT(size_, 0u)
        << "Failed precondition of ConcurrentVectorBuffer::back(): "
           "buffer empty";
    return data_[size_ - 1];
  }
  T& back() {
    RIEGELI_ASSERT_GT(size_, 0u)
        << "Failed precondition of ConcurrentVectorBuffer::back(): "
           "buffer empty";
    return data_[size_ - 1];
  }

  T* absl_nullable data() const { return data_; }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const ConcurrentVectorBuffer* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterDynamicMemory(self->data_,
                                           self->capacity_ * sizeof(T));
    memory_estimator.RegisterSubobjects(self->data_, self->data_ + self->size_);
  }

 private:
  static void Delete(T* absl_nullable data, size_t size, size_t capacity) {
    for (size_t i = size; i > 0;) {
      --i;
      data[i].~T();
    }
    DeleteAligned<void, alignof(T)>(data, capacity * sizeof(T));
  }

  T* absl_nullable data_ = nullptr;
  size_t size_ = 0;
  size_t capacity_ = 0;
};

// An append-only vector optionally supporting lock-free random access.
template <typename T, bool concurrent_reads, size_t initial_capacity = 4>
class ConcurrentVector;

// Non-concurrent specialization of `ConcurrentVector`.
//
// Asymptotic memory usage per element:
//   active: (1.5 - 1) / ln(1.5) = 1.2
//   after `shrink_to_fit()`: 1
template <typename T, size_t initial_capacity>
class ConcurrentVector<T, /*concurrent_reads=*/false, initial_capacity> {
 public:
  ConcurrentVector() = default;

  explicit ConcurrentVector(
      ConcurrentVector<T, /*concurrent_reads=*/true, initial_capacity>&& that)
      : buffer_(std::move(that.current_buffer_)) {
    that.retired_buffers_.reset();
    that.data_.store(nullptr, std::memory_order_relaxed);
    that.size_.store(0, std::memory_order_relaxed);
  }

  ConcurrentVector(ConcurrentVector&& that) = default;
  ConcurrentVector& operator=(ConcurrentVector&& that) = default;

  void clear() { buffer_.clear(); }

  void reserve(size_t min_capacity) {
    if (min_capacity > buffer_.capacity()) Reallocate(min_capacity);
  }

  template <typename... Args>
  T& emplace_back(Args&&... args) {
    if (ABSL_PREDICT_FALSE(buffer_.size() == buffer_.capacity())) {
      Reallocate(buffer_.capacity() == 0
                     ? UnsignedMax(initial_capacity, size_t{1})
                     : buffer_.capacity() + (buffer_.capacity() + 1) / 2);
    }
    return buffer_.emplace_back(std::forward<Args>(args)...);
  }

  void push_back(const T& value) { emplace_back(value); }
  void push_back(T&& value) { emplace_back(std::move(value)); }

  size_t size() const { return buffer_.size(); }
  size_t capacity() const { return buffer_.capacity(); }
  bool empty() const { return buffer_.empty(); }

  const T& operator[](size_t index) const { return buffer_[index]; }
  T& operator[](size_t index) { return buffer_[index]; }

  const T& back() const { return buffer_.back(); }
  T& back() { return buffer_.back(); }

  void pop_back() {
    RIEGELI_ASSERT_GT(size(), 0u)
        << "Failed precondition of ConcurrentVector::pop_back(): empty vector";
    buffer_.set_size(buffer_.size() - 1);
    (buffer_.data() + buffer_.size())->~T();
  }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const ConcurrentVector* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&self->buffer_);
  }

  void shrink_to_fit() {
    if (buffer_.capacity() <= buffer_.size()) return;
    if (buffer_.empty()) {
      buffer_ = ConcurrentVectorBuffer<T>();
    } else {
      Reallocate(buffer_.size());
    }
  }

 private:
  void Reallocate(size_t min_capacity);

  ConcurrentVectorBuffer<T> buffer_;
};

template <typename T, size_t initial_capacity>
void ConcurrentVector<T, /*concurrent_reads=*/false,
                      initial_capacity>::Reallocate(size_t min_capacity) {
  ConcurrentVectorBuffer<T> new_buffer(min_capacity);
  std::uninitialized_move_n(buffer_.data(), buffer_.size(), new_buffer.data());
  new_buffer.set_size(buffer_.size());
  buffer_ = std::move(new_buffer);
}

// Concurrent specialization of `ConcurrentVector`.
//
// `operator[]` and `size()` can be called concurrently with appending without
// locking.
//
// Asymptotic memory usage per element:
//   active: 3 / ln(3) = 2.7
//   after `shrink_to_fit()`: 1
template <typename T, size_t initial_capacity>
class ConcurrentVector<T, /*concurrent_reads=*/true, initial_capacity> {
 public:
  ConcurrentVector() = default;

  ConcurrentVector(const ConcurrentVector&) = delete;
  ConcurrentVector& operator=(const ConcurrentVector&) = delete;

  // A moved-from `ConcurrentVector` is left empty.
  ConcurrentVector(ConcurrentVector&& that) noexcept
      : current_buffer_(std::move(that.current_buffer_)),
        retired_buffers_(std::move(that.retired_buffers_)),
        data_(that.data_.exchange(nullptr, std::memory_order_relaxed)),
        size_(that.size_.exchange(0, std::memory_order_relaxed)) {}

  // A moved-from `ConcurrentVector` is left empty.
  ConcurrentVector& operator=(ConcurrentVector&& that) noexcept {
    current_buffer_ = std::move(that.current_buffer_);
    retired_buffers_ = std::move(that.retired_buffers_);
    data_.store(that.data_.exchange(nullptr, std::memory_order_relaxed),
                std::memory_order_relaxed);
    size_.store(that.size_.exchange(0, std::memory_order_relaxed),
                std::memory_order_relaxed);
    return *this;
  }

  void clear() {
    retired_buffers_.reset();
    current_buffer_.clear();
    size_.store(0, std::memory_order_relaxed);
  }

  void reserve(size_t min_capacity) {
    if (min_capacity > capacity()) Reallocate(min_capacity);
  }

  template <typename... Args>
  T& emplace_back(Args&&... args) {
    if (ABSL_PREDICT_FALSE(current_buffer_.size() ==
                           current_buffer_.capacity())) {
      const size_t old_capacity = capacity();
      // This reaches the minimum at Euler's number; 3 is close enough.
      Reallocate(old_capacity == 0 ? UnsignedMax(initial_capacity, size_t{1})
                                   : old_capacity * 3);
    }
    T* const data = data_.load(std::memory_order_relaxed);
    T* const ptr =
        new (data + current_buffer_.size()) T(std::forward<Args>(args)...);
    current_buffer_.set_size(current_buffer_.size() + 1);
    size_.store(size_.load(std::memory_order_relaxed) + 1,
                std::memory_order_release);
    return *ptr;
  }

  void push_back(const T& value) { emplace_back(value); }
  void push_back(T&& value) { emplace_back(std::move(value)); }

  // Can be called concurrently with appending without locking.
  size_t size() const { return size_.load(std::memory_order_acquire); }

  size_t capacity() const { return current_buffer_.capacity(); }

  // Can be called concurrently with appending without locking.
  bool empty() const { return size() == 0; }

  // Can be called concurrently with appending without locking.
  const T& operator[](size_t index) const {
    RIEGELI_ASSERT_LT(index, size())
        << "Failed precondition of ConcurrentVector::operator[]: "
           "index out of bounds";
    const T* const data = data_.load(std::memory_order_acquire);
    return data[index];
  }
  T& operator[](size_t index) {
    RIEGELI_ASSERT_LT(index, size())
        << "Failed precondition of ConcurrentVector::operator[]: "
           "index out of bounds";
    T* const data = data_.load(std::memory_order_acquire);
    return data[index];
  }

  const T& back() const { return (*this)[size() - 1]; }
  T& back() { return (*this)[size() - 1]; }

  void pop_back() {
    RIEGELI_ASSERT_GT(size(), 0u)
        << "Failed precondition of ConcurrentVector::pop_back(): empty vector";
    size_.store(size_.load(std::memory_order_relaxed) - 1,
                std::memory_order_release);
    current_buffer_.set_size(current_buffer_.size() - 1);
    (current_buffer_.data() + current_buffer_.size())->~T();
  }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const ConcurrentVector* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&self->current_buffer_);
    memory_estimator.RegisterSubobjects(&self->retired_buffers_);
  }

  // Must not be called concurrently with `operator[]` without locking there.
  void shrink_to_fit() {
    retired_buffers_.reset();
    if (capacity() <= size()) return;
    if (empty()) {
      current_buffer_ = ConcurrentVectorBuffer<T>();
      data_.store(nullptr, std::memory_order_relaxed);
    } else {
      Reallocate(size());
    }
  }

 private:
  // For member variables.
  friend ConcurrentVector<T, /*concurrent_reads=*/false, initial_capacity>;

  void Reallocate(size_t min_capacity);

  // Current buffer.
  ConcurrentVectorBuffer<T> current_buffer_;
  // Retired buffers, kept alive to preserve pointer validity for concurrent
  // readers.
  std::unique_ptr<std::vector<ConcurrentVectorBuffer<T>>> retired_buffers_;
  // Pointer to the data array of the current buffer (`current_buffer_.data()`).
  // Read lock-free by `operator[]`.
  std::atomic<T* absl_nullable> data_{nullptr};
  // Number of elements (`current_buffer_.size()`). Read lock-free by `size()`.
  std::atomic<size_t> size_{0};
};

template <typename T, size_t initial_capacity>
void ConcurrentVector<T, /*concurrent_reads=*/true,
                      initial_capacity>::Reallocate(size_t min_capacity) {
  ConcurrentVectorBuffer<T> new_buffer(min_capacity);
  T* const old_data = data_.load(std::memory_order_relaxed);
  if (old_data != nullptr) {
    std::uninitialized_copy_n(old_data, size(), new_buffer.data());
    new_buffer.set_size(size());
    if (retired_buffers_ == nullptr) {
      retired_buffers_ =
          std::make_unique<std::vector<ConcurrentVectorBuffer<T>>>();
    }
    retired_buffers_->push_back(std::move(current_buffer_));
  }
  current_buffer_ = std::move(new_buffer);
  data_.store(current_buffer_.data(), std::memory_order_release);
}

}  // namespace riegeli::interned_internal

#endif  // RIEGELI_INTERNED_CONCURRENT_VECTOR_INTERNAL_H_
