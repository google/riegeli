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

#ifndef RIEGELI_INTERNED_ARENA_INTERNED_OBJECT_INTERNAL_H_
#define RIEGELI_INTERNED_ARENA_INTERNED_OBJECT_INTERNAL_H_

#include <stddef.h>

#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_set.h"
#include "riegeli/base/assert.h"
#include "riegeli/interned/interned_common_internal.h"
#include "riegeli/interned/object_arena.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli::interned_internal {

// Supports heterogeneous lookup for arena element being searched.
// Avoids calling `Hash` again.
template <typename Arg>
struct ObjectArenaKeyForFind {
  const Arg& arg;
  size_t hash;
};

// Hash functor for arena element. Supports heterogeneous lookup with
// `ObjectArenaKeyForFind`.
template <typename T, typename Hash>
struct ObjectArenaElementHash {
  using is_transparent = void;
  size_t operator()(const T* ptr) const { return hash(*ptr); }
  template <typename Arg>
  size_t operator()(ObjectArenaKeyForFind<Arg> key) const {
    return key.hash;
  }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Hash hash;
};

// Equality functor for arena element. Supports heterogeneous lookup with
// `ObjectArenaKeyForFind`.
template <typename T, typename Eq>
struct ObjectArenaElementEq {
  using is_transparent = void;
  bool operator()(const T* a, const T* b) const { return eq(*a, *b); }
  template <typename Arg>
  bool operator()(const T* a, ObjectArenaKeyForFind<Arg> b) const {
    return eq(*a, b.arg);
  }
  template <typename Arg>
  bool operator()(ObjectArenaKeyForFind<Arg> a, const T* b) const {
    return eq(*b, a.arg);
  }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Eq eq;
};

// A single shard of the set of interned objects. The set is sharded by a part
// of the hash.
template <typename T, typename Hash, typename Eq, typename SetMutex>
class alignas(kInternerShardAlignment<SetMutex>) ObjectArenaShard {
 public:
  ObjectArenaShard() = default;

  ObjectArenaShard(ObjectArenaShard&& that) noexcept
      ABSL_NO_THREAD_SAFETY_ANALYSIS : objects_(std::move(that.objects_)) {}
  ObjectArenaShard& operator=(ObjectArenaShard&& that) noexcept
      ABSL_NO_THREAD_SAFETY_ANALYSIS {
    objects_ = std::move(that.objects_);
    return *this;
  }

  void Reset() ABSL_NO_THREAD_SAFETY_ANALYSIS { objects_.clear(); }

  void Reserve(size_t capacity) {
    RIEGELI_ASSERT_GT(capacity, 0u)
        << "Failed precondition of ObjectArenaShard::Reserve(): "
           "capacity is zero";
    MutexLock<SetMutex> set_lock(set_mutex_);
    objects_.reserve(capacity);
  }

  template <typename Arg, typename ArenaMutex, size_t static_min_block_size,
            size_t static_max_block_size>
  T* Intern(Arg&& arg, size_t hash,
            ObjectArena<T, ArenaMutex, static_min_block_size,
                        static_max_block_size>& arena,
            bool& is_new) ABSL_ATTRIBUTE_LIFETIME_BOUND {
    {
      ReaderMutexLock<SetMutex> set_lock(set_mutex_);
      const auto iter = objects_.find(ObjectArenaKeyForFind<Arg>{arg, hash});
      if (ABSL_PREDICT_TRUE(iter != objects_.end())) {
        is_new = false;
        return *iter;
      }
    }
    return InternSlow(std::forward<Arg>(arg), hash, arena, is_new);
  }

  template <bool verified_new, typename Arg, typename ArenaMutex,
            size_t static_min_block_size, size_t static_max_block_size>
  T* InternNew(Arg&& arg, size_t hash,
               ObjectArena<T, ArenaMutex, static_min_block_size,
                           static_max_block_size>& arena,
               bool& is_new) ABSL_ATTRIBUTE_LIFETIME_BOUND;

  template <typename Arg>
  T* absl_nullable Find(const Arg& arg, size_t hash) const {
    ReaderMutexLock<SetMutex> set_lock(set_mutex_);
    const auto iter = objects_.find(ObjectArenaKeyForFind<Arg>{arg, hash});
    if (iter != objects_.end()) return *iter;
    return nullptr;
  }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const ObjectArenaShard* self,
                                        MemoryEstimator& memory_estimator) {
    ReaderMutexLock<SetMutex> set_lock(self->set_mutex_);
    memory_estimator.RegisterSubobjects(&self->objects_);
  }

  void Archive() ABSL_NO_THREAD_SAFETY_ANALYSIS {
    objects_ = absl::flat_hash_set<T*, ObjectArenaElementHash<T, Hash>,
                                   ObjectArenaElementEq<T, Eq>>();
  }

 private:
  template <typename Arg, typename ArenaMutex, size_t static_min_block_size,
            size_t static_max_block_size>
  ABSL_ATTRIBUTE_NOINLINE T* InternSlow(
      Arg&& arg, size_t hash,
      ObjectArena<T, ArenaMutex, static_min_block_size, static_max_block_size>&
          arena,
      bool& is_new);

  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS mutable SetMutex set_mutex_;
  absl::flat_hash_set<T*, ObjectArenaElementHash<T, Hash>,
                      ObjectArenaElementEq<T, Eq>>
      objects_ ABSL_GUARDED_BY(set_mutex_);
};

template <typename T, typename Hash, typename Eq, typename SetMutex>
template <typename Arg, typename ArenaMutex, size_t static_min_block_size,
          size_t static_max_block_size>
T* ObjectArenaShard<T, Hash, Eq, SetMutex>::InternSlow(
    Arg&& arg, size_t hash,
    ObjectArena<T, ArenaMutex, static_min_block_size, static_max_block_size>&
        arena,
    bool& is_new) {
  return InternNew</*verified_new=*/true>(std::forward<Arg>(arg), hash, arena,
                                          is_new);
}

template <typename T, typename Hash, typename Eq, typename SetMutex>
template <bool verified_new, typename Arg, typename ArenaMutex,
          size_t static_min_block_size, size_t static_max_block_size>
inline T* ObjectArenaShard<T, Hash, Eq, SetMutex>::InternNew(
    Arg&& arg, size_t hash,
    ObjectArena<T, ArenaMutex, static_min_block_size, static_max_block_size>&
        arena,
    bool& is_new) {
  T* allocated;
  if constexpr (std::conjunction_v<
                    std::negation<std::is_same<SetMutex, NullMutex>>,
                    std::is_move_constructible<T>,
                    std::negation<std::is_same<Arg, T>>>) {
    // Construct the object outside the arena lock.
    T constructed(std::forward<Arg>(arg));
    allocated = arena.Allocate(std::move(constructed));
  } else {
    allocated = arena.Allocate(std::forward<Arg>(arg));
  }

  T* result;
  is_new = false;
  {
    MutexLock<SetMutex> set_lock(set_mutex_);
    result = *objects_.lazy_emplace(ObjectArenaKeyForFind<T>{*allocated, hash},
                                    [&](const auto& ctor) {
                                      ctor(allocated);
                                      is_new = true;
                                    });
  }
  if constexpr (!verified_new || !std::is_same_v<SetMutex, NullMutex>) {
    if (ABSL_PREDICT_FALSE(!is_new)) {
      // The object is already present. If `verified_new`, this is possible
      // only if another thread has just interned the same object and won the
      // race.
      arena.UndoAllocate(allocated);
    }
  }
  return result;
}

}  // namespace riegeli::interned_internal

#endif  // RIEGELI_INTERNED_ARENA_INTERNED_OBJECT_INTERNAL_H_
