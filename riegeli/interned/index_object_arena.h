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

#ifndef RIEGELI_INTERNED_INDEX_OBJECT_ARENA_H_
#define RIEGELI_INTERNED_INDEX_OBJECT_ARENA_H_

#include <stddef.h>

#include <atomic>
#include <new>  // IWYU pragma: keep
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/base/thread_annotations.h"
#include "absl/numeric/bits.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/interned/concurrent_vector_internal.h"
#include "riegeli/interned/index_object_arena_internal.h"
#include "riegeli/interned/interned_common_internal.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

// Mutex type that does not lock.
using interned_internal::NullMutex;

// Default template parameter for `IndexObjectArena`.
using interned_internal::kDefaultArenaFixedBlockSize;

// `IndexObjectArena` allocates objects of type `T`.
//
// The objects are never moved. They are destroyed when the arena is destroyed.
// Individual deallocation is not supported, except for best-effort undoing of
// the most recent allocation.
//
// Supports lookup by consecutive indices.
//
// See `ObjectArena` for a variant which does not support random access by
// index.
//
// Objects are allocated in fixed-size blocks whose size in bytes is specified
// statically. The default is 4K.
//
// If `concurrent_reads` is `true`, `operator[]` and `size()` can be called
// concurrently with allocation without locking.
//
// Among the template parameters, only `T` should be specified explicitly. Other
// parameters should be specified by nested types `Concurrent`,
// `WithConcurrentReads`, and `WithBlockSize`.
template <typename T, typename Mutex = NullMutex, bool concurrent_reads = false,
          size_t block_size = kDefaultArenaFixedBlockSize>
class IndexObjectArena {
  static_assert(block_size > 0, "block_size must be positive");

 public:
  // Enables concurrency for `IndexObjectArena`.
  //
  // `Mutex` specifies the mutex type, which can be `absl::Mutex` (default)
  // or another type with `lock()`, `unlock()`, `lock_shared()`, and
  // `unlock_shared()`, analogously to `absl::Mutex`.
  template <typename NewMutex = absl::Mutex>
  using Concurrent =
      IndexObjectArena<T, NewMutex, concurrent_reads, block_size>;

  // Allows `operator[]` and `size()` to be called concurrently with allocation
  // without locking.
  template <bool new_concurrent_reads = true>
  using WithConcurrentReads =
      IndexObjectArena<T, Mutex, new_concurrent_reads, block_size>;

  // Configures the block size of the arena, in bytes.
  //
  // Objects are allocated in blocks of this size. A larger block size improves
  // memory locality and reduces the number of allocations, but increases wasted
  // memory if only a small number of objects is allocated.
  template <size_t new_block_size>
  using WithBlockSize =
      IndexObjectArena<T, Mutex, concurrent_reads, new_block_size>;

  // The archive type. See `IndexObjectArena::ExtractArchive()` for details.
  using Archive =
      IndexObjectArena<T, NullMutex, /*concurrent_reads=*/false, block_size>;

  // Creates an empty `IndexObjectArena`.
  IndexObjectArena() = default;

  // A moved-from `IndexObjectArena` is left empty.
  IndexObjectArena(IndexObjectArena&& that) noexcept
      ABSL_NO_THREAD_SAFETY_ANALYSIS
      : size_([&] {
        if constexpr (concurrent_reads) {
          return that.size_.exchange(0, std::memory_order_relaxed);
        } else {
          return std::exchange(that.size_, 0);
        }
      }()),
        cursor_(std::exchange(that.cursor_, nullptr)),
        limit_(std::exchange(that.limit_, nullptr)),
        last_block_(std::exchange(that.last_block_, {})),
        blocks_(InitBlocks(std::move(that.blocks_), &last_block_,
                           &that.last_block_)) {}

  IndexObjectArena& operator=(IndexObjectArena&& that) noexcept
      ABSL_NO_THREAD_SAFETY_ANALYSIS {
    const interned_internal::IndexObjectArenaBlock<T, kBlockCapacity>
        last_block = std::exchange(that.last_block_, {});
    DeleteBlocks(
        std::exchange(blocks_, InitBlocks(std::move(that.blocks_), &last_block_,
                                          &that.last_block_)),
        std::exchange(cursor_, std::exchange(that.cursor_, nullptr)));
    limit_ = std::exchange(that.limit_, nullptr);
    last_block_ = last_block;
    if constexpr (concurrent_reads) {
      size_.store(that.size_.exchange(0, std::memory_order_relaxed),
                  std::memory_order_relaxed);
    } else {
      size_ = std::exchange(that.size_, 0);
    }
    return *this;
  }

  ~IndexObjectArena() { DeleteBlocks(std::move(blocks_), cursor_); }

  // Resets the arena to the empty state.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset() ABSL_NO_THREAD_SAFETY_ANALYSIS {
    if constexpr (!concurrent_reads) {
      if (!blocks_.empty()) {
        for (size_t i = blocks_.size() - 1; i > 0;) {
          --i;
          blocks_[i].DeleteFull();
        }
        last_block_.ClearPartial(cursor_);
        blocks_ = Blocks(&last_block_);
        cursor_ = last_block_.data();
        limit_ = last_block_.limit();
        size_ = 0;
        return;
      }
    }
    DeleteBlocks(std::exchange(blocks_, {}), std::exchange(cursor_, nullptr));
    limit_ = nullptr;
    last_block_ = {};
    if constexpr (concurrent_reads) {
      size_.store(0, std::memory_order_relaxed);
    } else {
      size_ = 0;
    }
  }

  // Prepares the arena for the expected number of objects. This reduces
  // reallocations.
  void Reserve(size_t capacity) {
    if (capacity == 0) return;
    const size_t num_blocks =
        UnsignedMin((capacity - 1) / kBlockCapacity + 1, Blocks::kMaxSize);
    if constexpr (!concurrent_reads) {
      if (num_blocks <= 1) return;
    }
    interned_internal::MutexLock<Mutex> lock(mutex_);
    blocks_.reserve(num_blocks);
  }

  // Allocates and constructs an object of type `T` with `args...`.
  //
  // Returns the index of the allocated object.
  template <typename... Args,
            std::enable_if_t<std::is_constructible_v<T, Args&&...>, int> = 0>
  size_t Allocate(Args&&... args) {
    return AllocateImpl(std::forward<Args>(args)...);
  }

  // Convert `const char*` to `absl::string_view` early to compute `strlen()`
  // once and to avoid separate template instantiations for `char[length + 1]`.
  template <
      typename DependentT = T,
      std::enable_if_t<std::is_constructible_v<DependentT, absl::string_view>,
                       int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE size_t Allocate(const char* arg) {
    return AllocateImpl(absl::string_view(arg));
  }

  // Const `Allocate()` overload enabled only when thread-safe.
  template <typename... Args, typename DependentMutex = Mutex,
            std::enable_if_t<
                std::conjunction_v<
                    std::negation<std::is_same<DependentMutex, NullMutex>>,
                    std::is_constructible<T, Args&&...>>,
                int> = 0>
  size_t Allocate(Args&&... args) const {
    return AllocateImpl(std::forward<Args>(args)...);
  }

  // Convert `const char*` to `absl::string_view` early to compute `strlen()`
  // once and to avoid separate template instantiations for `char[length + 1]`.
  template <typename DependentMutex = Mutex, typename DependentT = T,
            std::enable_if_t<
                std::conjunction_v<
                    std::negation<std::is_same<DependentMutex, NullMutex>>,
                    std::is_constructible<DependentT, absl::string_view>>,
                int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE size_t Allocate(const char* arg) const {
    return AllocateImpl(absl::string_view(arg));
  }

  // Undoes `Allocate()`. This is best-effort, and is effective only for the
  // most recent allocation.
  void UndoAllocate(size_t index) { UndoAllocateImpl(index); }

  // Const `UndoAllocate()` overload enabled only when thread-safe.
  template <
      typename DependentMutex = Mutex,
      std::enable_if_t<!std::is_same_v<DependentMutex, NullMutex>, int> = 0>
  void UndoAllocate(size_t index) const {
    UndoAllocateImpl(index);
  }

  // Returns the number of objects.
  //
  // If `concurrent_reads` is `true`, this can be called concurrently with
  // allocation without locking.
  size_t size() const {
    if constexpr (concurrent_reads) {
      return size_.load(std::memory_order_acquire);
    } else {
      return size_;
    }
  }

  // Returns `true` if empty.
  //
  // If `concurrent_reads` is `true`, this can be called concurrently with
  // allocation without locking.
  bool empty() const { return size() == 0; }

  // Resolves an index to the object.
  //
  // If `concurrent_reads` is `true`, this can be called concurrently with
  // allocation without locking.
  const T& operator[](size_t index) const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT_LT(index, size())
        << "Failed precondition of IndexObjectArena::operator[]: "
           "index out of bounds";
    return blocks_[index / kBlockCapacity][index % kBlockCapacity];
  }
  T& operator[](size_t index) ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT_LT(index, size())
        << "Failed precondition of IndexObjectArena::operator[]: "
           "index out of bounds";
    return blocks_[index / kBlockCapacity][index % kBlockCapacity];
  }

  const T& at(size_t index) const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_CHECK_LT(index, size())
        << "Failed precondition of IndexObjectArena::at(): "
           "index out of bounds";
    return blocks_[index / kBlockCapacity][index % kBlockCapacity];
  }
  T& at(size_t index) ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_CHECK_LT(index, size())
        << "Failed precondition of IndexObjectArena::at(): "
           "index out of bounds";
    return blocks_[index / kBlockCapacity][index % kBlockCapacity];
  }

  const T& front() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT(!empty())
        << "Failed precondition of IndexObjectArena::front(): empty arena";
    return blocks_[0][0];
  }
  T& front() ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT(!empty())
        << "Failed precondition of IndexObjectArena::front(): empty arena";
    return blocks_[0][0];
  }

  const T& back() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT(!empty())
        << "Failed precondition of IndexObjectArena::back(): empty arena";
    return (*this)[size() - 1];
  }
  T& back() ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT(!empty())
        << "Failed precondition of IndexObjectArena::back(): empty arena";
    return (*this)[size() - 1];
  }

  void ShrinkToFit() {
    interned_internal::MutexLock<Mutex> lock(mutex_);
    blocks_.shrink_to_fit();
  }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const IndexObjectArena* self,
                                        MemoryEstimator& memory_estimator) {
    interned_internal::ReaderMutexLock<Mutex> lock(self->mutex_);
    memory_estimator.RegisterSubobjects(&self->blocks_);
    if (!self->blocks_.empty()) {
      for (size_t i = 0; i < self->blocks_.size() - 1; ++i) {
        self->blocks_[i].RegisterSubobjectsFull(memory_estimator);
      }
      self->blocks_.back().RegisterSubobjectsPartial(self->cursor_,
                                                     memory_estimator);
    }
  }

  // Extracts the storage of the objects as an archive, which holds the same
  // objects as `IndexObjectArena`, but does not support concurrency.
  // The `IndexObjectArena` is left empty.
  Archive ExtractArchive() && { return Archive(std::move(*this)); }

 private:
  // For `IndexObjectArena(IndexObjectArena<...>&&)`.
  template <typename TParam, typename OtherMutex, bool other_concurrent_reads,
            size_t block_size_param>
  friend class IndexObjectArena;

  static constexpr size_t kBlockCapacity =
      UnsignedMax(absl::bit_floor(block_size / sizeof(T)), size_t{1});

  using Blocks = interned_internal::ConcurrentVector<
      interned_internal::IndexObjectArenaBlock<T, kBlockCapacity>,
      concurrent_reads>;

  template <bool other_concurrent_reads>
  static Blocks InitBlocks(
      interned_internal::ConcurrentVector<
          interned_internal::IndexObjectArenaBlock<T, kBlockCapacity>,
          other_concurrent_reads>&& that_blocks,
      interned_internal::IndexObjectArenaBlock<T, kBlockCapacity>* last_block,
      const interned_internal::IndexObjectArenaBlock<T, kBlockCapacity>*
          that_last_block) {
    if constexpr (!other_concurrent_reads) {
      static_assert(!concurrent_reads);
      if (that_blocks.data() == that_last_block) {
        that_blocks = {};
        return Blocks(last_block);
      }
    }
    return Blocks(std::move(that_blocks));
  }

  template <typename OtherMutex, bool other_concurrent_reads>
  explicit IndexObjectArena(
      IndexObjectArena<T, OtherMutex, other_concurrent_reads, block_size>&&
          that) ABSL_NO_THREAD_SAFETY_ANALYSIS
      : size_([&] {
        if constexpr (other_concurrent_reads) {
          return that.size_.exchange(0, std::memory_order_relaxed);
        } else {
          return std::exchange(that.size_, 0);
        }
      }()),
        cursor_(std::exchange(that.cursor_, nullptr)),
        limit_(std::exchange(that.limit_, nullptr)),
        last_block_(std::exchange(that.last_block_, {})),
        blocks_(InitBlocks(std::move(that.blocks_), &last_block_,
                           &that.last_block_)) {
    blocks_.shrink_to_fit();
  }

  static void DeleteBlocks(Blocks blocks, T* absl_nullable cursor) {
    if (!blocks.empty()) {
      blocks.back().DeletePartial(cursor);
      for (size_t i = blocks.size() - 1; i > 0;) {
        --i;
        blocks[i].DeleteFull();
      }
    }
  }

  template <typename... Args>
  size_t AllocateImpl(Args&&... args) const;

  ABSL_ATTRIBUTE_NOINLINE void AllocateSlow() const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void UndoAllocateImpl(size_t index) const;

  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS mutable Mutex mutex_;
  // The number of objects. Equal to
  // `blocks_.size() * kBlockCapacity - PtrDistance(cursor_, limit_)`,
  // but stored separately for efficient and concurrent access.
  mutable std::conditional_t<concurrent_reads, std::atomic<size_t>, size_t>
      size_{0};
  // If `limit_ != nullptr`, points to the next object in `last_block_` to
  // allocate. Otherwise `nullptr`.
  mutable T* absl_nullable cursor_ ABSL_GUARDED_BY(mutex_) = nullptr;
  // If `cursor_ != nullptr`, points to the end of `last_block_`.
  // Otherwise `nullptr`.
  mutable T* absl_nullable limit_ ABSL_GUARDED_BY(mutex_) = nullptr;
  mutable interned_internal::IndexObjectArenaBlock<T, kBlockCapacity>
      last_block_ ABSL_GUARDED_BY(mutex_);
  mutable Blocks blocks_;
};

// Implementation details follow.

template <typename T, typename Mutex, bool concurrent_reads, size_t block_size>
template <typename... Args>
inline size_t IndexObjectArena<T, Mutex, concurrent_reads,
                               block_size>::AllocateImpl(Args&&... args) const {
  interned_internal::MutexLock<Mutex> lock(mutex_);
  if (ABSL_PREDICT_FALSE(cursor_ == limit_)) AllocateSlow();
  T* const ptr = cursor_;
  new (ptr) T(std::forward<Args>(args)...);
  ++cursor_;
  size_t index;
  if constexpr (concurrent_reads) {
    index = size_.load(std::memory_order_relaxed);
    size_.store(index + 1, std::memory_order_release);
  } else {
    index = size_;
    ++size_;
  }
  return index;
}

template <typename T, typename Mutex, bool concurrent_reads, size_t block_size>
void IndexObjectArena<T, Mutex, concurrent_reads, block_size>::AllocateSlow()
    const {
  interned_internal::IndexObjectArenaBlock<T, kBlockCapacity>* block;
  if constexpr (!concurrent_reads) {
    if (blocks_.capacity() == 0) {
      last_block_ = interned_internal::IndexObjectArenaBlock<T, kBlockCapacity>(
          std::in_place);
      blocks_ = Blocks(&last_block_);
      block = &last_block_;
    } else {
      block = &blocks_.emplace_back(std::in_place);
      last_block_ = *block;
    }
  } else {
    block = &blocks_.emplace_back(std::in_place);
    last_block_ = *block;
  }
  cursor_ = block->data();
  limit_ = block->limit();
}

template <typename T, typename Mutex, bool concurrent_reads, size_t block_size>
inline void IndexObjectArena<T, Mutex, concurrent_reads,
                             block_size>::UndoAllocateImpl(size_t index) const {
  interned_internal::MutexLock<Mutex> lock(mutex_);
  size_t current_size;
  if constexpr (concurrent_reads) {
    current_size = size_.load(std::memory_order_relaxed);
  } else {
    current_size = size_;
  }
  if (ABSL_PREDICT_TRUE(current_size > 0 && index == current_size - 1 &&
                        cursor_ > last_block_.data())) {
    // This was the most recent allocation. Undo it.
    if constexpr (concurrent_reads) {
      size_.store(current_size - 1, std::memory_order_release);
    } else {
      --size_;
    }
    --cursor_;
    cursor_->~T();
    return;
  }

  // Undoing is not feasible.
  if constexpr (std::is_move_constructible_v<T>) {
    // At least moving the object out is likely to free its memory.
    [[maybe_unused]] T moved =
        std::move(blocks_[index / kBlockCapacity][index % kBlockCapacity]);
  }
}

}  // namespace riegeli

#endif  // RIEGELI_INTERNED_INDEX_OBJECT_ARENA_H_
