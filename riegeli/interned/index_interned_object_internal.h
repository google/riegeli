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

#ifndef RIEGELI_INTERNED_INDEX_INTERNED_OBJECT_INTERNAL_H_
#define RIEGELI_INTERNED_INDEX_INTERNED_OBJECT_INTERNAL_H_

#include <stddef.h>

#include <atomic>
#include <limits>
#include <new>  // IWYU pragma: keep
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_set.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/new_aligned.h"
#include "riegeli/interned/arena_interned_object_internal.h"
#include "riegeli/interned/concurrent_vector_internal.h"
#include "riegeli/interned/interned_common_internal.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli::interned_internal {

template <typename Numeric>
constexpr Numeric kNullNumeric = std::numeric_limits<Numeric>::max();

// Manages a contiguous array of a fixed number of elements of type `T`.
template <typename T, size_t size>
class DirectoryBlock {
 public:
  DirectoryBlock()
      : data_(static_cast<T*>(NewAligned<void, alignof(T)>(size * sizeof(T)))) {
  }

  DirectoryBlock(const DirectoryBlock& that) = default;
  DirectoryBlock& operator=(const DirectoryBlock& that) = default;

  DirectoryBlock(DirectoryBlock&& that) = default;
  DirectoryBlock& operator=(DirectoryBlock&& that) = default;

  void DeleteFull() { DeletePartial(size); }

  void DeletePartial(size_t used) {
    for (size_t i = used; i > 0;) {
      --i;
      data_[i].~T();
    }
    DeleteAligned<void, alignof(T)>(data_, size * sizeof(T));
  }

  template <typename... Args>
  T& emplace_back(size_t used, Args&&... args) {
    new (data_ + used) T(std::forward<Args>(args)...);
    return data_[used];
  }

  const T& operator[](size_t index) const { return data_[index]; }

  template <typename MemoryEstimator>
  void RegisterSubobjectsFull(MemoryEstimator& memory_estimator) const {
    RegisterSubobjectsPartial(size, memory_estimator);
  }

  template <typename MemoryEstimator>
  void RegisterSubobjectsPartial(size_t used,
                                 MemoryEstimator& memory_estimator) const {
    memory_estimator.RegisterDynamicMemory(data_, size * sizeof(T));
    memory_estimator.RegisterSubobjects(data_, data_ + used);
  }

 private:
  T* data_;
};

// Allocates objects of type `T`.
//
// The objects are never moved. They are destroyed when the directory is
// destroyed. Individual deallocation is not supported, except for best-effort
// undoing of the most recent allocation.
//
// Supports lookup by consecutive indices.
//
// Objects are allocated in fixed-size blocks. The block size in bytes is
// specified statically.
//
// If `concurrent_reads` is `true`, `operator[]` and `size()` can be called
// concurrently with allocation without locking.
template <typename T, bool concurrent_reads, size_t block_size>
class Directory {
 public:
  using Archive = Directory<T, /*concurrent_reads=*/false, block_size>;

  Directory() = default;

  Directory(Directory&& that) noexcept
      : blocks_(std::move(that.blocks_)),
        last_block_used_(std::exchange(that.last_block_used_, kBlockCapacity)),
        size_([&] {
          if constexpr (concurrent_reads) {
            return that.size_.exchange(0, std::memory_order_relaxed);
          } else {
            return std::exchange(that.size_, 0);
          }
        }()) {}

  Directory& operator=(Directory&& that) noexcept {
    DeleteBlocks(
        std::exchange(blocks_, std::exchange(that.blocks_, {})),
        std::exchange(last_block_used_,
                      std::exchange(that.last_block_used_, kBlockCapacity)));
    if constexpr (concurrent_reads) {
      size_.store(that.size_.exchange(0, std::memory_order_relaxed),
                  std::memory_order_relaxed);
    } else {
      size_ = std::exchange(that.size_, 0);
    }
    return *this;
  }

  ~Directory() { DeleteBlocks(std::move(blocks_), last_block_used_); }

  void Reset() {
    DeleteBlocks(std::exchange(blocks_, {}),
                 std::exchange(last_block_used_, kBlockCapacity));
    if constexpr (concurrent_reads) {
      size_.store(0, std::memory_order_relaxed);
    } else {
      size_ = 0;
    }
  }

  void Reserve(size_t capacity) {
    RIEGELI_ASSERT_GT(capacity, 0u)
        << "Failed precondition of Directory::Reserve(): capacity is zero";
    blocks_.reserve((capacity - 1) / kBlockCapacity + 1);
  }

  template <typename... Args>
  T& Allocate(Args&&... args) ABSL_ATTRIBUTE_LIFETIME_BOUND;

  size_t size() const {
    if constexpr (concurrent_reads) {
      return size_.load(std::memory_order_acquire);
    } else {
      return size_;
    }
  }

  const T& operator[](size_t index) const {
    RIEGELI_ASSERT_LT(index, size())
        << "Failed precondition of Directory::operator[]: "
           "index out of bounds";
    return blocks_[index / kBlockCapacity][index % kBlockCapacity];
  }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const Directory* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&self->blocks_);
    if (!self->blocks_.empty()) {
      for (size_t i = 0; i < self->blocks_.size() - 1; ++i) {
        self->blocks_[i].RegisterSubobjectsFull(memory_estimator);
      }
      self->blocks_.back().RegisterSubobjectsPartial(self->last_block_used_,
                                                     memory_estimator);
    }
  }

  void ShrinkToFit() { blocks_.shrink_to_fit(); }

  Archive ExtractArchive() && {
    return Archive(typename Archive::Blocks(std::move(blocks_)),
                   std::exchange(last_block_used_, kBlockCapacity), [&] {
                     if constexpr (concurrent_reads) {
                       return size_.exchange(0, std::memory_order_relaxed);
                     } else {
                       return std::exchange(size_, 0);
                     }
                   }());
  }

 private:
  // For `Blocks` and `Directory(Blocks&&, size_t, size_t)`.
  friend class Directory<T, /*concurrent_reads=*/true, block_size>;

  static constexpr size_t kBlockCapacity =
      UnsignedMax(block_size / sizeof(T), size_t{1});

  using Blocks =
      ConcurrentVector<DirectoryBlock<T, kBlockCapacity>, concurrent_reads>;

  explicit Directory(typename Archive::Blocks&& blocks, size_t last_block_used,
                     size_t size)
      : blocks_(std::move(blocks)),
        last_block_used_(last_block_used),
        size_(size) {}

  static void DeleteBlocks(Blocks blocks, size_t last_block_used) {
    if (!blocks.empty()) {
      blocks.back().DeletePartial(last_block_used);
      for (size_t i = blocks.size() - 1; i > 0;) {
        --i;
        blocks[i].DeleteFull();
      }
    }
  }

  Blocks blocks_;
  // If `!blocks_.empty()`, the number of used elements in `blocks_.back()`.
  // Otherwise `kBlockCapacity`, to make conditions simpler.
  size_t last_block_used_ = kBlockCapacity;
  // The number of objects.
  // Equal to `return (blocks_.size() - 1) * kBlockCapacity + last_block_used_`
  // but stored separately for efficient and concurrent access.
  std::conditional_t<concurrent_reads, std::atomic<size_t>, size_t> size_{0};
};

template <typename T, bool concurrent_reads, size_t block_size>
template <typename... Args>
inline T& Directory<T, concurrent_reads, block_size>::Allocate(Args&&... args)
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  if (ABSL_PREDICT_FALSE(last_block_used_ == kBlockCapacity)) {
    blocks_.emplace_back();
    last_block_used_ = 0;
  }
  T& result = blocks_.back().emplace_back(last_block_used_,
                                          std::forward<Args>(args)...);
  ++last_block_used_;
  if constexpr (concurrent_reads) {
    size_.store(size_.load(std::memory_order_relaxed) + 1,
                std::memory_order_release);
  } else {
    ++size_;
  }
  return result;
}

// Supports heterogeneous lookup for resolved object being searched.
// Avoids calling `Hash` again.
template <typename Resolved>
struct IndexKeyForFindResolved {
  Resolved value;
  size_t hash;
};

template <typename Numeric, typename T, typename Hash, bool concurrent_reads,
          size_t block_size>
struct IndexHash {
  using is_transparent = void;

  using Directory = Directory<T, concurrent_reads, block_size>;

  explicit IndexHash(const Directory* directory) : directory(directory) {}

  size_t operator()(Numeric numeric) const {
    return hash((*directory)[IntCast<size_t>(numeric)]);
  }
  template <typename Arg>
  size_t operator()(ObjectArenaKeyForFind<Arg> key) const {
    return key.hash;
  }
  template <typename Resolved>
  size_t operator()(IndexKeyForFindResolved<Resolved> key) const {
    return key.hash;
  }

 private:
  const Directory* directory;
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Hash hash;
};

template <typename Numeric, typename T, typename Eq, bool concurrent_reads,
          size_t block_size>
struct IndexEq {
  using is_transparent = void;

  using Directory = Directory<T, concurrent_reads, block_size>;

  explicit IndexEq(const Directory* directory) : directory(directory) {}

  bool operator()(Numeric a, Numeric b) const { return a == b; }
  template <typename Arg>
  bool operator()(Numeric a, ObjectArenaKeyForFind<Arg> b) const {
    return eq((*directory)[IntCast<size_t>(a)], b.arg);
  }
  template <typename Arg>
  bool operator()(ObjectArenaKeyForFind<Arg> a, Numeric b) const {
    return eq((*directory)[IntCast<size_t>(b)], a.arg);
  }
  template <typename Resolved>
  bool operator()(Numeric a, IndexKeyForFindResolved<Resolved> b) const {
    return &(*directory)[IntCast<size_t>(a)] == b.value.get();
  }
  template <typename Resolved>
  bool operator()(IndexKeyForFindResolved<Resolved> a, Numeric b) const {
    return a.value.get() == &(*directory)[IntCast<size_t>(b)];
  }

 private:
  const Directory* directory;
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Eq eq;
};

template <typename Numeric, typename T, typename Hash, typename Eq,
          typename SetMutex, typename ArenaMutex, size_t block_size>
class alignas(kInternerShardAlignment<SetMutex>) IndexInternerShard {
 private:
  static constexpr bool kConcurrentReads =
      !std::is_same_v<ArenaMutex, NullMutex>;

 public:
  using Directory = Directory<T, kConcurrentReads, block_size>;

  explicit IndexInternerShard(const Directory* directory)
      : indices_(0, IndexHash(directory), IndexEq(directory)) {}

  IndexInternerShard(const IndexInternerShard&) = delete;
  IndexInternerShard& operator=(const IndexInternerShard&) = delete;

  void Reset() ABSL_NO_THREAD_SAFETY_ANALYSIS { indices_.clear(); }

  void Reserve(size_t capacity) {
    RIEGELI_ASSERT_GT(capacity, 0u)
        << "Failed precondition of IndexInternerShard::Reserve(): "
           "capacity is zero";
    MutexLock<SetMutex> set_lock(set_mutex_);
    indices_.reserve(capacity);
  }

  template <typename Arg>
  Numeric Intern(Arg&& arg, size_t hash, Directory& directory,
                 ArenaMutex& arena_mutex, bool& is_new) {
    {
      ReaderMutexLock<SetMutex> set_lock(set_mutex_);
      const auto iter = indices_.find(ObjectArenaKeyForFind<Arg>{arg, hash});
      if (ABSL_PREDICT_TRUE(iter != indices_.end())) {
        is_new = false;
        return *iter;
      }
    }
    return InternSlow(std::forward<Arg>(arg), hash, directory, arena_mutex,
                      is_new);
  }

  template <bool verified_new, typename Arg>
  Numeric InternNew(Arg&& arg, size_t hash, Directory& directory,
                    ArenaMutex& arena_mutex, bool& is_new);

  template <typename Arg>
  Numeric Find(const Arg& arg, size_t hash) const {
    ReaderMutexLock<SetMutex> set_lock(set_mutex_);
    const auto iter = indices_.find(ObjectArenaKeyForFind<Arg>{arg, hash});
    if (iter != indices_.end()) return *iter;
    return kNullNumeric<Numeric>;
  }

  template <typename Resolved>
  Numeric IndexOf(Resolved value, size_t hash) const {
    ReaderMutexLock<SetMutex> set_lock(set_mutex_);
    const auto iter =
        indices_.find(IndexKeyForFindResolved<Resolved>{value, hash});
    RIEGELI_ASSERT(iter != indices_.end())
        << "Failed precondition of IndexInterned::Interner::IndexOf(): "
           "resolved object not found in this interner";
    return *iter;
  }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const IndexInternerShard* self,
                                        MemoryEstimator& memory_estimator) {
    ReaderMutexLock<SetMutex> set_lock(self->set_mutex_);
    memory_estimator.RegisterSubobjects(&self->indices_);
  }

  void Archive() ABSL_NO_THREAD_SAFETY_ANALYSIS {
    indices_ = absl::flat_hash_set<Numeric, IndexHash, IndexEq>(
        0, indices_.hash_function(), indices_.key_eq());
  }

 private:
  using IndexHash = IndexHash<Numeric, T, Hash, kConcurrentReads, block_size>;
  using IndexEq = IndexEq<Numeric, T, Eq, kConcurrentReads, block_size>;

  template <typename Arg>
  ABSL_ATTRIBUTE_NOINLINE Numeric InternSlow(Arg&& arg, size_t hash,
                                             Directory& directory,
                                             ArenaMutex& arena_mutex,
                                             bool& is_new);

  template <bool verified_new, typename Arg>
  Numeric InternNewInternal(Arg&& arg, size_t hash, Directory& directory,
                            ArenaMutex& arena_mutex, bool& is_new);

  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS mutable SetMutex set_mutex_;
  absl::flat_hash_set<Numeric, IndexHash, IndexEq> indices_
      ABSL_GUARDED_BY(set_mutex_);
};

template <typename Numeric, typename T, typename Hash, typename Eq,
          typename SetMutex, typename ArenaMutex, size_t block_size>
template <typename Arg>
Numeric IndexInternerShard<Numeric, T, Hash, Eq, SetMutex, ArenaMutex,
                           block_size>::InternSlow(Arg&& arg, size_t hash,
                                                   Directory& directory,
                                                   ArenaMutex& arena_mutex,
                                                   bool& is_new) {
  return InternNew</*verified_new=*/true>(std::forward<Arg>(arg), hash,
                                          directory, arena_mutex, is_new);
}

template <typename Numeric, typename T, typename Hash, typename Eq,
          typename SetMutex, typename ArenaMutex, size_t block_size>
template <bool verified_new, typename Arg>
inline Numeric
IndexInternerShard<Numeric, T, Hash, Eq, SetMutex, ArenaMutex,
                   block_size>::InternNew(Arg&& arg, size_t hash,
                                          Directory& directory,
                                          ArenaMutex& arena_mutex,
                                          bool& is_new) {
  if constexpr (std::conjunction_v<
                    std::negation<std::is_same<SetMutex, NullMutex>>,
                    std::is_move_constructible<T>,
                    std::negation<std::is_same<Arg, T>>>) {
    // Construct the object outside locks.
    T constructed(std::forward<Arg>(arg));
    return InternNewInternal<verified_new>(std::move(constructed), hash,
                                           directory, arena_mutex, is_new);
  } else {
    return InternNewInternal<verified_new>(std::forward<Arg>(arg), hash,
                                           directory, arena_mutex, is_new);
  }
}

template <typename Numeric, typename T, typename Hash, typename Eq,
          typename SetMutex, typename ArenaMutex, size_t block_size>
template <bool verified_new, typename Arg>
inline Numeric
IndexInternerShard<Numeric, T, Hash, Eq, SetMutex, ArenaMutex,
                   block_size>::InternNewInternal(Arg&& arg, size_t hash,
                                                  Directory& directory,
                                                  ArenaMutex& arena_mutex,
                                                  bool& is_new) {
  if constexpr (verified_new && std::is_same_v<SetMutex, NullMutex>) {
    Numeric next_index;
    {
      MutexLock<ArenaMutex> arena_lock(arena_mutex);
      next_index = IntCast<Numeric>(directory.size());
      if (ABSL_PREDICT_FALSE(next_index == kNullNumeric<Numeric>)) {
        is_new = false;
        return kNullNumeric<Numeric>;
      }
      directory.Allocate(std::forward<Arg>(arg));
    }

    MutexLock<SetMutex> set_lock(set_mutex_);
    RIEGELI_EVAL_ASSERT(indices_.emplace(next_index).second);
    is_new = true;
    return next_index;
  } else {
    // Do not allocate a directory entry before verifying that the object is
    // absent from `indices_`, to ensure that `directory.size()` is monotonic.
    Numeric result;
    is_new = false;
    {
      MutexLock<SetMutex> set_lock(set_mutex_);
      const auto iter = indices_.lazy_emplace(
          ObjectArenaKeyForFind<Arg>{arg, hash}, [&](const auto& ctor) {
            Numeric next_index;
            {
              MutexLock<ArenaMutex> arena_lock(arena_mutex);
              next_index = IntCast<Numeric>(directory.size());
              if (ABSL_PREDICT_TRUE(next_index != kNullNumeric<Numeric>)) {
                directory.Allocate(std::forward<Arg>(arg));
              }
            }
            ctor(next_index);
            is_new = true;
          });
      result = *iter;
      if (ABSL_PREDICT_TRUE(is_new)) {
        if (ABSL_PREDICT_FALSE(result == kNullNumeric<Numeric>)) {
          indices_.erase(iter);
          is_new = false;
        }
      }
    }
    return result;
  }
}

}  // namespace riegeli::interned_internal

#endif  // RIEGELI_INTERNED_INDEX_INTERNED_OBJECT_INTERNAL_H_
