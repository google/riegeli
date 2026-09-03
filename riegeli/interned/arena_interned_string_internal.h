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

#ifndef RIEGELI_INTERNED_ARENA_INTERNED_STRING_INTERNAL_H_
#define RIEGELI_INTERNED_ARENA_INTERNED_STRING_INTERNAL_H_

#include <stddef.h>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_set.h"
#include "riegeli/base/assert.h"
#include "riegeli/interned/interned_common_internal.h"
#include "riegeli/interned/string_arena.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli::interned_internal {

// Supports heterogeneous lookup for `ArenaString` being searched.
// Avoids calling `Hash` again.
template <typename Arg>
struct StringArenaKeyForFind {
  const Arg& arg;
  size_t hash;
};

// Hash functor for `ArenaString`. Supports heterogeneous lookup with
// `StringArenaKeyForFind`.
template <typename Hash, size_t alignment>
struct ArenaStringHash {
  using is_transparent = void;
  size_t operator()(ArenaString::WithAlignment<alignment> element) const {
    return hash(*element);
  }
  template <typename Arg>
  size_t operator()(StringArenaKeyForFind<Arg> key) const {
    return key.hash;
  }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Hash hash;
};

// Equality functor for `ArenaString`. Supports heterogeneous lookup with
// `StringArenaKeyForFind`.
template <typename Eq, size_t alignment>
struct ArenaStringEq {
  using is_transparent = void;
  bool operator()(ArenaString::WithAlignment<alignment> a,
                  ArenaString::WithAlignment<alignment> b) const {
    return a.data() == b.data();
  }
  template <typename Arg>
  bool operator()(ArenaString::WithAlignment<alignment> a,
                  StringArenaKeyForFind<Arg> b) const {
    return eq(*a, b.arg);
  }
  template <typename Arg>
  bool operator()(StringArenaKeyForFind<Arg> a,
                  ArenaString::WithAlignment<alignment> b) const {
    return eq(*b, a.arg);
  }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Eq eq;
};

template <typename Encoder, typename SetMutex, size_t alignment>
class alignas(kInternerShardAlignment<SetMutex>) StringArenaShard {
 public:
  using Element = ArenaString::WithAlignment<alignment>;

  StringArenaShard() = default;

  StringArenaShard(StringArenaShard&& that) noexcept
      ABSL_NO_THREAD_SAFETY_ANALYSIS : objects_(std::move(that.objects_)) {}
  StringArenaShard& operator=(StringArenaShard&& that) noexcept
      ABSL_NO_THREAD_SAFETY_ANALYSIS {
    objects_ = std::move(that.objects_);
    return *this;
  }

  void Reset() ABSL_NO_THREAD_SAFETY_ANALYSIS { objects_.clear(); }

  void Reserve(size_t capacity) {
    RIEGELI_ASSERT_GT(capacity, 0u)
        << "Failed precondition of StringArenaShard::Reserve(): "
           "capacity is zero";
    MutexLock<SetMutex> set_lock(set_mutex_);
    objects_.reserve(capacity);
  }

  template <typename Arg, typename ArenaMutex, size_t static_min_block_size,
            size_t static_max_block_size>
  Element Intern(
      const Arg& value, size_t hash,
      BasicStringArena<ArenaMutex, /*concurrent_reads=*/false,
                       static_min_block_size, static_max_block_size>& arena,
      bool& is_new) ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT(!Encoder::EncodedEmpty(value))
        << "Failed precondition of StringArenaShard::Intern(): value is empty";
    {
      ReaderMutexLock<SetMutex> set_lock(set_mutex_);
      const auto iter = objects_.find(StringArenaKeyForFind<Arg>{value, hash});
      if (ABSL_PREDICT_TRUE(iter != objects_.end())) {
        is_new = false;
        return *iter;
      }
    }
    return InternSlow(value, hash, arena, is_new);
  }

  template <bool verified_new, typename Arg, typename ArenaMutex,
            size_t static_min_block_size, size_t static_max_block_size>
  Element InternNew(
      const Arg& value, size_t hash,
      BasicStringArena<ArenaMutex, /*concurrent_reads=*/false,
                       static_min_block_size, static_max_block_size>& arena,
      bool& is_new) ABSL_ATTRIBUTE_LIFETIME_BOUND;

  template <typename Arg>
  typename Element::Optional Find(const Arg& value, size_t hash) const
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT(!Encoder::EncodedEmpty(value))
        << "Failed precondition of StringArenaShard::Find(): value is empty";
    ReaderMutexLock<SetMutex> set_lock(set_mutex_);
    const auto iter = objects_.find(StringArenaKeyForFind<Arg>{value, hash});
    if (iter != objects_.end()) return *iter;
    return nullptr;
  }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const StringArenaShard* self,
                                        MemoryEstimator& memory_estimator) {
    ReaderMutexLock<SetMutex> set_lock(self->set_mutex_);
    memory_estimator.RegisterSubobjects(&self->objects_);
  }

  void Archive() ABSL_NO_THREAD_SAFETY_ANALYSIS {
    objects_ =
        absl::flat_hash_set<Element,
                            ArenaStringHash<typename Encoder::Hash, alignment>,
                            ArenaStringEq<typename Encoder::Eq, alignment>>();
  }

 private:
  template <typename Arg, typename ArenaMutex, size_t static_min_block_size,
            size_t static_max_block_size>
  ABSL_ATTRIBUTE_NOINLINE Element InternSlow(
      const Arg& value, size_t hash,
      BasicStringArena<ArenaMutex, /*concurrent_reads=*/false,
                       static_min_block_size, static_max_block_size>& arena,
      bool& is_new);

  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS mutable SetMutex set_mutex_;
  absl::flat_hash_set<Element,
                      ArenaStringHash<typename Encoder::Hash, alignment>,
                      ArenaStringEq<typename Encoder::Eq, alignment>>
      objects_ ABSL_GUARDED_BY(set_mutex_);
};

template <typename Encoder, typename SetMutex, size_t alignment>
template <typename Arg, typename ArenaMutex, size_t static_min_block_size,
          size_t static_max_block_size>
auto StringArenaShard<Encoder, SetMutex, alignment>::InternSlow(
    const Arg& value, size_t hash,
    BasicStringArena<ArenaMutex, /*concurrent_reads=*/false,
                     static_min_block_size, static_max_block_size>& arena,
    bool& is_new) -> Element {
  return InternNew</*verified_new=*/true>(value, hash, arena, is_new);
}

template <typename Encoder, typename SetMutex, size_t alignment>
template <bool verified_new, typename Arg, typename ArenaMutex,
          size_t static_min_block_size, size_t static_max_block_size>
inline auto StringArenaShard<Encoder, SetMutex, alignment>::InternNew(
    const Arg& value, size_t hash,
    BasicStringArena<ArenaMutex, /*concurrent_reads=*/false,
                     static_min_block_size, static_max_block_size>& arena,
    bool& is_new) -> Element {
  RIEGELI_ASSERT(!Encoder::EncodedEmpty(value))
      << "Failed precondition of StringArenaShard::InternNew(): "
         "value is empty";
  const Element allocated = arena.template Allocate<alignment, Encoder>(value);

  Element result;
  is_new = false;
  {
    MutexLock<SetMutex> set_lock(set_mutex_);
    result = *objects_.lazy_emplace(StringArenaKeyForFind<Arg>{value, hash},
                                    [&](const auto& ctor)
                                        ABSL_NO_THREAD_SAFETY_ANALYSIS {
                                          ctor(allocated);
                                          is_new = true;
                                        });
  }
  if constexpr (!verified_new || !std::is_same_v<SetMutex, NullMutex>) {
    if (ABSL_PREDICT_FALSE(!is_new)) {
      // The string is already present. If `verified_new`, this is possible
      // only if another thread has just interned the same string and won the
      // race.
      arena.UndoAllocate(allocated);
    }
  }
  return result;
}

}  // namespace riegeli::interned_internal

#endif  // RIEGELI_INTERNED_ARENA_INTERNED_STRING_INTERNAL_H_
