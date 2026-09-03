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

#ifndef RIEGELI_INTERNED_OBJECT_ARENA_H_
#define RIEGELI_INTERNED_OBJECT_ARENA_H_

#include <stddef.h>

#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/base/thread_annotations.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/interned/interned_common_internal.h"
#include "riegeli/interned/object_arena_internal.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

// Mutex type that does not lock.
using interned_internal::NullMutex;

// Default block sizes for `ObjectArena`.
using interned_internal::kDefaultArenaMaxBlockSize;
using interned_internal::kDefaultArenaMinBlockSize;

// Allocates objects of type `T`.
//
// The objects are never moved. They are destroyed when the arena is destroyed.
// Individual deallocation is not supported, except for best-effort undoing of
// the most recent allocation.
//
// Objects are allocated in blocks whose size in bytes is specified statically
// or dynamically, and can adaptively grow between `min_block_size` and
// `max_block_size`. The default is a static size range between 256 bytes and
// 64K.
template <typename T, typename Mutex = NullMutex,
          size_t static_min_block_size = kDefaultArenaMinBlockSize,
          size_t static_max_block_size = kDefaultArenaMaxBlockSize>
class ObjectArena;

// Specialization of `ObjectArena` with a dynamic block size. It is also a base
// class of the specialization with a static block size.
template <typename T, typename Mutex>
class ObjectArena<T, Mutex, /*static_min_block_size=*/0,
                  /*static_max_block_size=*/0> {
 public:
  // Enables concurrency for `ObjectArena`.
  //
  // `Mutex` specifies the mutex type, which can be `absl::Mutex` (default)
  // or another type with `lock()`, `unlock()`, `lock_shared()`, and
  // `unlock_shared()`, analogously to `absl::Mutex`.
  template <typename NewMutex = absl::Mutex>
  using Concurrent = ObjectArena<T, NewMutex, /*static_min_block_size=*/0,
                                 /*static_max_block_size=*/0>;

  // Configures the block size of the arena, in bytes.
  //
  // Objects are allocated in blocks of sizes within this range. A larger block
  // size improves memory locality and reduces the number of allocations, but
  // increases wasted memory if only a small number of objects is allocated.
  template <size_t new_static_min_block_size,
            size_t new_static_max_block_size = new_static_min_block_size>
  using WithBlockSize = ObjectArena<T, Mutex, new_static_min_block_size,
                                    new_static_max_block_size>;

  // Configures the block size of the arena to be specified dynamically in the
  // constructor.
  using WithDynamicBlockSize = ObjectArena;

  // The archive type. See `ObjectArena::ExtractArchive()` for details.
  using Archive = ObjectArena<T, NullMutex, /*static_min_block_size=*/0,
                              /*static_max_block_size=*/0>;

  // Creates an empty `ObjectArena` with a fixed block size in bytes.
  explicit ObjectArena(size_t block_size)
      : max_block_objects_(UnsignedMax(block_size / sizeof(T), size_t{1})),
        next_block_objects_(max_block_objects_) {}

  // Creates an empty `ObjectArena` with an adaptive block size between
  // `min_block_size` and `max_block_size` in bytes.
  explicit ObjectArena(size_t min_block_size, size_t max_block_size)
      : max_block_objects_(
            UnsignedMax(UnsignedMax(min_block_size, max_block_size) / sizeof(T),
                        size_t{1})),
        next_block_objects_(
            UnsignedMax(min_block_size / sizeof(T), size_t{1})) {}

  // A moved-from `ObjectArena` is left empty.
  ObjectArena(ObjectArena&& that) noexcept ABSL_NO_THREAD_SAFETY_ANALYSIS
      : max_block_objects_(that.max_block_objects_),
        next_block_objects_(that.next_block_objects_),
        last_block_used_objects_(std::exchange(that.last_block_used_objects_,
                                               0)),
        last_block_(std::exchange(that.last_block_, {})),
        previous_blocks_(std::move(that.previous_blocks_)) {}
  ObjectArena& operator=(ObjectArena&& that) noexcept
      ABSL_NO_THREAD_SAFETY_ANALYSIS {
    max_block_objects_ = that.max_block_objects_;
    next_block_objects_ = that.next_block_objects_;
    DeleteBlocks(
        std::exchange(last_block_, std::exchange(that.last_block_, {})),
        std::exchange(previous_blocks_,
                      std::exchange(that.previous_blocks_, {})),
        std::exchange(last_block_used_objects_,
                      std::exchange(that.last_block_used_objects_, 0)));
    return *this;
  }

  ~ObjectArena() {
    DeleteBlocks(std::move(last_block_), std::move(previous_blocks_),
                 last_block_used_objects_);
  }

  // Resets the arena to the empty state, with a fixed block size in bytes.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(size_t block_size) {
    Reset(block_size, block_size);
  }

  // Resets the arena to the empty state, with an adaptive block size between
  // `min_block_size` and `max_block_size` in bytes.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(size_t min_block_size,
                                          size_t max_block_size)
      ABSL_NO_THREAD_SAFETY_ANALYSIS;

  // Prepares the arena for the expected number of objects. This reduces
  // reallocations.
  void Reserve(size_t capacity);

  // Allocates and constructs an object of type `T` with `args...`.
  template <typename... Args,
            std::enable_if_t<std::is_constructible_v<T, Args&&...>, int> = 0>
  T* Allocate(Args&&... args) ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return AllocateImpl(std::forward<Args>(args)...);
  }

  // Convert `const char*` to `absl::string_view` early to compute `strlen()`
  // once and to avoid separate template instantiations for `char[length + 1]`.
  template <
      typename DependentT = T,
      std::enable_if_t<std::is_constructible_v<DependentT, absl::string_view>,
                       int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE T* Allocate(const char* arg)
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return AllocateImpl(absl::string_view(arg));
  }

  // Const `Allocate()` overload enabled only when thread-safe.
  template <typename... Args, typename DependentMutex = Mutex,
            std::enable_if_t<
                std::conjunction_v<
                    std::negation<std::is_same<DependentMutex, NullMutex>>,
                    std::is_constructible<T, Args&&...>>,
                int> = 0>
  T* Allocate(Args&&... args) const ABSL_ATTRIBUTE_LIFETIME_BOUND {
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
  ABSL_ATTRIBUTE_ALWAYS_INLINE T* Allocate(const char* arg) const
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return AllocateImpl(absl::string_view(arg));
  }

  // Undoes `Allocate()`. This is best-effort, and is effective only for the
  // most recent allocation.
  void UndoAllocate(T* ptr) { UndoAllocateImpl(ptr); }

  // Const `UndoAllocate()` overload enabled only when thread-safe.
  template <
      typename DependentMutex = Mutex,
      std::enable_if_t<!std::is_same_v<DependentMutex, NullMutex>, int> = 0>
  void UndoAllocate(T* ptr) const {
    UndoAllocateImpl(ptr);
  }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const ObjectArena* self,
                                        MemoryEstimator& memory_estimator) {
    interned_internal::ReaderMutexLock<Mutex> lock(self->mutex_);
    memory_estimator.RegisterSubobjects(&self->previous_blocks_);
    for (const auto& block : self->previous_blocks_) {
      block.RegisterSubobjectsFull(memory_estimator);
    }
    if (self->last_block_.is_allocated()) {
      self->last_block_.RegisterSubobjectsPartial(
          self->last_block_used_objects_, memory_estimator);
    }
  }

  // Extracts the storage of the objects as an archive, which holds the same
  // objects as `ObjectArena`, but does not support concurrency.
  // The `ObjectArena` is left empty.
  Archive ExtractArchive() && { return Archive(std::move(*this)); }

 private:
  // For `ObjectArena(ObjectArena<T, OtherMutex, static_min_block_size,
  //                              static_max_block_size>&&)`.
  template <typename TParam, typename OtherMutex,
            size_t static_min_block_size_param,
            size_t static_max_block_size_param>
  friend class ObjectArena;

  template <typename OtherMutex>
  explicit ObjectArena(ObjectArena<T, OtherMutex, /*static_min_block_size=*/0,
                                   /*static_max_block_size=*/0>&& that)
      : max_block_objects_(that.max_block_objects_),
        next_block_objects_(that.next_block_objects_),
        last_block_used_objects_(
            std::exchange(that.last_block_used_objects_, 0)),
        last_block_(std::exchange(that.last_block_, {})),
        previous_blocks_(std::move(that.previous_blocks_)) {}

  static void DeleteBlocks(
      interned_internal::ObjectArenaBlock<T> last_block,
      std::vector<interned_internal::ObjectArenaBlock<T>> previous_blocks,
      size_t last_block_used_objects) ABSL_NO_THREAD_SAFETY_ANALYSIS {
    last_block.DeletePartial(last_block_used_objects);
    for (size_t i = previous_blocks.size(); i > 0;) {
      --i;
      previous_blocks[i].DeleteFull();
    }
  }

  template <typename... Args>
  T* AllocateImpl(Args&&... args) const;

  ABSL_ATTRIBUTE_NOINLINE void AllocateSlow() const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void UndoAllocateImpl(T* ptr) const;

  size_t max_block_objects_;
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS mutable Mutex mutex_;
  mutable size_t next_block_objects_ ABSL_GUARDED_BY(mutex_);
  // If `last_block_.is_allocated()`, the number of used elements in
  // `last_block_`. Otherwise 0.
  mutable size_t last_block_used_objects_ ABSL_GUARDED_BY(mutex_) = 0;
  mutable interned_internal::ObjectArenaBlock<T> last_block_
      ABSL_GUARDED_BY(mutex_);
  mutable std::vector<interned_internal::ObjectArenaBlock<T>> previous_blocks_
      ABSL_GUARDED_BY(mutex_);
};

// Specialization of `ObjectArena` with a static block size.
template <typename T, typename Mutex, size_t static_min_block_size,
          size_t static_max_block_size>
class ObjectArena : public ObjectArena<T, Mutex, /*static_min_block_size=*/0,
                                       /*static_max_block_size=*/0> {
  static_assert(static_min_block_size > 0 && static_max_block_size > 0,
                "static_min_block_size and static_max_block_size "
                "must be both zero or both positive");

 public:
  // Enables concurrency for `ObjectArena`.
  //
  // `Mutex` specifies the mutex type, which can be `absl::Mutex` (default)
  // or another type with `lock()`, `unlock()`, `lock_shared()`, and
  // `unlock_shared()`, analogously to `absl::Mutex`.
  template <typename NewMutex = absl::Mutex>
  using Concurrent =
      ObjectArena<T, NewMutex, static_min_block_size, static_max_block_size>;

  // Configures the block size of the arena, in bytes.
  //
  // Objects are allocated in blocks of sizes within this range. A larger block
  // size improves memory locality and reduces the number of allocations, but
  // increases wasted memory if only a small number of objects is allocated.
  template <size_t new_static_min_block_size,
            size_t new_static_max_block_size = new_static_min_block_size>
  using WithBlockSize = ObjectArena<T, Mutex, new_static_min_block_size,
                                    new_static_max_block_size>;

  // Configures the block size of the arena to be specified dynamically in the
  // constructor.
  using WithDynamicBlockSize =
      ObjectArena<T, Mutex, /*static_min_block_size=*/0,
                  /*static_max_block_size=*/0>;

  // The archive type. See `ObjectArena::ExtractArchive()` for details.
  using Archive =
      ObjectArena<T, NullMutex, static_min_block_size, static_max_block_size>;

  // Creates an empty `ObjectArena` with a static block size in bytes.
  ObjectArena() noexcept
      : ObjectArena<T, Mutex, /*static_min_block_size=*/0,
                    /*static_max_block_size=*/0>(static_min_block_size,
                                                 static_max_block_size) {}

  // A moved-from `ObjectArena` is left empty.
  ObjectArena(ObjectArena&& that) = default;
  ObjectArena& operator=(ObjectArena&& that) = default;

  // Resets the arena to the empty state.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset() {
    this->ObjectArena<
        T, Mutex, /*static_min_block_size=*/0,
        /*static_max_block_size=*/0>::Reset(static_min_block_size,
                                            static_max_block_size);
  }

  // Extracts the storage of the objects as an archive, which holds the same
  // objects as `ObjectArena`, but does not support concurrency.
  // The `ObjectArena` is left empty.
  Archive ExtractArchive() && { return Archive(std::move(*this)); }

 private:
  // For `ObjectArena(ObjectArena<T, OtherMutex, static_min_block_size,
  //                              static_max_block_size>&&)`.
  template <typename TParam, typename OtherMutex,
            size_t static_min_block_size_param,
            size_t static_max_block_size_param>
  friend class ObjectArena;

  template <typename OtherMutex>
  explicit ObjectArena(ObjectArena<T, OtherMutex, static_min_block_size,
                                   static_max_block_size>&& that)
      : ObjectArena<T, Mutex, /*static_min_block_size=*/0,
                    /*static_max_block_size=*/0>(std::move(that)) {}
};

// Implementation details follow.

template <typename T, typename Mutex>
inline void ObjectArena<T, Mutex, 0, 0>::Reset(size_t min_block_size,
                                               size_t max_block_size) {
  max_block_objects_ = UnsignedMax(
      UnsignedMax(min_block_size, max_block_size) / sizeof(T), size_t{1});
  const size_t min_block_objects =
      UnsignedMax(min_block_size / sizeof(T), size_t{1});
  for (size_t i = previous_blocks_.size(); i > 0;) {
    --i;
    previous_blocks_[i].DeleteFull();
  }
  previous_blocks_.clear();
  if (last_block_.is_allocated()) {
    if (last_block_.size() <= max_block_objects_) {
      last_block_.Clear(last_block_used_objects_);
      last_block_used_objects_ = 0;
      next_block_objects_ =
          UnsignedClamp(last_block_.size() + (last_block_.size() + 1) / 2,
                        min_block_objects, max_block_objects_);
      return;
    }
    last_block_.DeletePartial(last_block_used_objects_);
    last_block_ = {};
    last_block_used_objects_ = 0;
  }
  next_block_objects_ = min_block_objects;
}

template <typename T, typename Mutex>
inline void ObjectArena<T, Mutex, 0, 0>::Reserve(size_t capacity) {
  if (capacity == 0) return;
  interned_internal::MutexLock<Mutex> lock(mutex_);
  size_t existing_capacity = last_block_.size();
  for (const interned_internal::ObjectArenaBlock<T>& block : previous_blocks_) {
    existing_capacity += block.size();
  }
  if (capacity <= existing_capacity) return;
  const size_t remaining_to_reserve = capacity - existing_capacity;
  if (remaining_to_reserve <= max_block_objects_) {
    next_block_objects_ =
        UnsignedMax(next_block_objects_, remaining_to_reserve);
  } else {
    next_block_objects_ = max_block_objects_;
    const size_t num_additional_blocks =
        (remaining_to_reserve - 1) / max_block_objects_ + 1;
    if (last_block_.is_allocated()) {
      previous_blocks_.reserve(previous_blocks_.size() + num_additional_blocks);
    } else if (num_additional_blocks > 1) {
      previous_blocks_.reserve(num_additional_blocks - 1);
    }
  }
}

template <typename T, typename Mutex>
template <typename... Args>
inline T* ObjectArena<T, Mutex, 0, 0>::AllocateImpl(Args&&... args) const {
  interned_internal::MutexLock<Mutex> lock(mutex_);
  if (ABSL_PREDICT_FALSE(last_block_used_objects_ == last_block_.size())) {
    AllocateSlow();
  }

  T& result = last_block_.emplace_back(last_block_used_objects_,
                                       std::forward<Args>(args)...);
  ++last_block_used_objects_;
  return &result;
}

template <typename T, typename Mutex>
void ObjectArena<T, Mutex, 0, 0>::AllocateSlow() const {
  const size_t block_objects = next_block_objects_;
  if (last_block_.is_allocated()) {
    previous_blocks_.push_back(std::move(last_block_));
  }
  next_block_objects_ = UnsignedClamp(block_objects + (block_objects + 1) / 2,
                                      next_block_objects_, max_block_objects_);
  last_block_ = interned_internal::ObjectArenaBlock<T>(block_objects);
  last_block_used_objects_ = 0;
}

template <typename T, typename Mutex>
inline void ObjectArena<T, Mutex, 0, 0>::UndoAllocateImpl(T* ptr) const {
  interned_internal::MutexLock<Mutex> lock(mutex_);
  if (ABSL_PREDICT_TRUE(last_block_used_objects_ > 0 &&
                        ptr == &last_block_[last_block_used_objects_ - 1])) {
    // This was the most recent allocation. Undo it.
    last_block_.pop_back(--last_block_used_objects_);
    return;
  }

  // Undoing is not feasible.
  if constexpr (std::is_move_constructible_v<T>) {
    // At least moving the object out is likely to free its memory.
    [[maybe_unused]] T moved = std::move(*ptr);
  }
}

}  // namespace riegeli

#endif  // RIEGELI_INTERNED_OBJECT_ARENA_H_
