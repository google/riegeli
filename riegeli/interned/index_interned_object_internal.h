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

#include <limits>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_set.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/interned/arena_interned_object_internal.h"
#include "riegeli/interned/index_object_arena.h"
#include "riegeli/interned/interned_common_internal.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli::interned_internal {

template <typename Numeric>
constexpr Numeric kNullNumeric = std::numeric_limits<Numeric>::max();

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

  using Arena = typename IndexObjectArena<T>::template WithConcurrentReads<
      concurrent_reads>::template WithBlockSize<block_size>;

  explicit IndexHash(const Arena* arena) : arena(arena) {}

  size_t operator()(Numeric numeric) const {
    return hash((*arena)[IntCast<size_t>(numeric)]);
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
  const Arena* arena;
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Hash hash;
};

template <typename Numeric, typename T, typename Eq, bool concurrent_reads,
          size_t block_size>
struct IndexEq {
  using is_transparent = void;

  using Arena = typename IndexObjectArena<T>::template WithConcurrentReads<
      concurrent_reads>::template WithBlockSize<block_size>;

  explicit IndexEq(const Arena* arena) : arena(arena) {}

  bool operator()(Numeric a, Numeric b) const { return a == b; }
  template <typename Arg>
  bool operator()(Numeric a, ObjectArenaKeyForFind<Arg> b) const {
    return eq((*arena)[IntCast<size_t>(a)], b.arg);
  }
  template <typename Arg>
  bool operator()(ObjectArenaKeyForFind<Arg> a, Numeric b) const {
    return eq((*arena)[IntCast<size_t>(b)], a.arg);
  }
  template <typename Resolved>
  bool operator()(Numeric a, IndexKeyForFindResolved<Resolved> b) const {
    return &(*arena)[IntCast<size_t>(a)] == b.value.get();
  }
  template <typename Resolved>
  bool operator()(IndexKeyForFindResolved<Resolved> a, Numeric b) const {
    return a.value.get() == &(*arena)[IntCast<size_t>(b)];
  }

 private:
  const Arena* arena;
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Eq eq;
};

template <typename Numeric, typename T, typename Hash, typename Eq,
          typename SetMutex, typename ArenaMutex, size_t block_size>
class alignas(kInternerShardAlignment<SetMutex>) IndexInternerShard {
 private:
  static constexpr bool kConcurrentReads =
      !std::is_same_v<ArenaMutex, NullMutex>;

 public:
  using Arena = typename IndexObjectArena<T>::template WithConcurrentReads<
      kConcurrentReads>::template WithBlockSize<block_size>;

  explicit IndexInternerShard(const Arena* arena)
      : indices_(0, IndexHash(arena), IndexEq(arena)) {}

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
  Numeric Intern(Arg&& arg, size_t hash, Arena& arena, ArenaMutex& arena_mutex,
                 bool& is_new) {
    {
      ReaderMutexLock<SetMutex> set_lock(set_mutex_);
      const auto iter = indices_.find(ObjectArenaKeyForFind<Arg>{arg, hash});
      if (ABSL_PREDICT_TRUE(iter != indices_.end())) {
        is_new = false;
        return *iter;
      }
    }
    return InternSlow(std::forward<Arg>(arg), hash, arena, arena_mutex, is_new);
  }

  template <bool verified_new, typename Arg>
  Numeric InternNew(Arg&& arg, size_t hash, Arena& arena,
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
                                             Arena& arena,
                                             ArenaMutex& arena_mutex,
                                             bool& is_new);

  template <bool verified_new, typename Arg>
  Numeric InternNewInternal(Arg&& arg, size_t hash, Arena& arena,
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
                                                   Arena& arena,
                                                   ArenaMutex& arena_mutex,
                                                   bool& is_new) {
  return InternNew</*verified_new=*/true>(std::forward<Arg>(arg), hash, arena,
                                          arena_mutex, is_new);
}

template <typename Numeric, typename T, typename Hash, typename Eq,
          typename SetMutex, typename ArenaMutex, size_t block_size>
template <bool verified_new, typename Arg>
inline Numeric
IndexInternerShard<Numeric, T, Hash, Eq, SetMutex, ArenaMutex,
                   block_size>::InternNew(Arg&& arg, size_t hash, Arena& arena,
                                          ArenaMutex& arena_mutex,
                                          bool& is_new) {
  if constexpr (std::conjunction_v<
                    std::negation<std::is_same<SetMutex, NullMutex>>,
                    std::is_move_constructible<T>,
                    std::negation<std::is_same<Arg, T>>>) {
    // Construct the object outside locks.
    T constructed(std::forward<Arg>(arg));
    return InternNewInternal<verified_new>(std::move(constructed), hash, arena,
                                           arena_mutex, is_new);
  } else {
    return InternNewInternal<verified_new>(std::forward<Arg>(arg), hash, arena,
                                           arena_mutex, is_new);
  }
}

template <typename Numeric, typename T, typename Hash, typename Eq,
          typename SetMutex, typename ArenaMutex, size_t block_size>
template <bool verified_new, typename Arg>
inline Numeric
IndexInternerShard<Numeric, T, Hash, Eq, SetMutex, ArenaMutex,
                   block_size>::InternNewInternal(Arg&& arg, size_t hash,
                                                  Arena& arena,
                                                  ArenaMutex& arena_mutex,
                                                  bool& is_new) {
  if constexpr (verified_new && std::is_same_v<SetMutex, NullMutex>) {
    Numeric next_index;
    {
      MutexLock<ArenaMutex> arena_lock(arena_mutex);
      next_index = IntCast<Numeric>(arena.size());
      if (ABSL_PREDICT_FALSE(next_index == kNullNumeric<Numeric>)) {
        is_new = false;
        return kNullNumeric<Numeric>;
      }
      RIEGELI_EVAL_ASSERT_EQ(arena.Allocate(std::forward<Arg>(arg)),
                             IntCast<size_t>(next_index));
    }

    MutexLock<SetMutex> set_lock(set_mutex_);
    RIEGELI_EVAL_ASSERT(indices_.emplace(next_index).second);
    is_new = true;
    return next_index;
  } else {
    // Do not allocate in the arena before verifying that the object is absent
    // from `indices_`, to ensure that `arena.size()` is monotonic.
    Numeric result;
    is_new = false;
    {
      MutexLock<SetMutex> set_lock(set_mutex_);
      const auto iter = indices_.lazy_emplace(
          ObjectArenaKeyForFind<Arg>{arg, hash}, [&](const auto& ctor) {
            Numeric next_index;
            {
              MutexLock<ArenaMutex> arena_lock(arena_mutex);
              next_index = IntCast<Numeric>(arena.size());
              if (ABSL_PREDICT_TRUE(next_index != kNullNumeric<Numeric>)) {
                RIEGELI_EVAL_ASSERT_EQ(arena.Allocate(std::forward<Arg>(arg)),
                                       IntCast<size_t>(next_index));
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
