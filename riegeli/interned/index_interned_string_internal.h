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

#ifndef RIEGELI_INTERNED_INDEX_INTERNED_STRING_INTERNAL_H_
#define RIEGELI_INTERNED_INDEX_INTERNED_STRING_INTERNAL_H_

#include <stddef.h>

#include <limits>
#include <type_traits>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_set.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/interned/arena_interned_string_internal.h"
#include "riegeli/interned/concurrent_vector_internal.h"
#include "riegeli/interned/index_interned_object_internal.h"
#include "riegeli/interned/interned_common_internal.h"
#include "riegeli/interned/string_arena.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli::interned_internal {

// Allocates objects of type `T`.
//
// The objects can be moved. They are destroyed when the directory is destroyed.
// Individual deallocation is not supported, except for best-effort undoing of
// the most recent allocation.
//
// Supports lookup by consecutive indices.
//
// If `concurrent_reads` is `true`, `operator[]` and `size()` can be called
// concurrently with allocation without locking.
template <typename T, bool concurrent_reads>
class StringDirectory {
 public:
  using Archive = StringDirectory<T, /*concurrent_reads=*/false>;

  StringDirectory() = default;

  StringDirectory(StringDirectory&& that) = default;
  StringDirectory& operator=(StringDirectory&& that) = default;

  void Reset() { addresses_.clear(); }

  void Reserve(size_t capacity) {
    RIEGELI_ASSERT_GT(capacity, 0u)
        << "Failed precondition of StringDirectory::Reserve(): "
           "capacity is zero";
    addresses_.reserve(capacity);
  }

  template <typename... Args>
  T& Allocate(Args&&... args) {
    return addresses_.emplace_back(std::forward<Args>(args)...);
  }

  size_t size() const { return addresses_.size(); }

  const T& operator[](size_t index) const {
    RIEGELI_ASSERT_LT(index, size())
        << "Failed precondition of StringDirectory::operator[]: "
           "index out of bounds";
    return addresses_[index];
  }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const StringDirectory* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&self->addresses_);
  }

  void ShrinkToFit() { addresses_.shrink_to_fit(); }

  Archive ExtractArchive() && {
    return Archive(typename Archive::Addresses(std::move(addresses_)));
  }

 private:
  // For `StringDirectory(Addresses&&)`.
  friend class StringDirectory<T, /*concurrent_reads=*/true>;

  using Addresses = ConcurrentVector<T, concurrent_reads, 16>;

  explicit StringDirectory(typename Archive::Addresses&& addresses)
      : addresses_(std::move(addresses)) {}

  Addresses addresses_;
};

// Supports heterogeneous lookup for resolved string being searched.
// Avoids calling `Hash` again.
template <typename Resolved>
struct StringIndexKeyForFindResolved {
  Resolved value;
  size_t hash;
};

// Primary template used when `Address` is not `void`.
template <typename Numeric, typename Encoder, typename Address,
          bool concurrent_reads, size_t alignment, size_t static_min_block_size,
          size_t static_max_block_size>
struct IndexStringHash {
  using is_transparent = void;

  using Element = ArenaString::WithAlignment<alignment>;
  using Arena = typename StringArena::WithConcurrentReads<concurrent_reads>::
      template WithBlockSize<static_min_block_size, static_max_block_size>;
  using Directory = StringDirectory<Address, concurrent_reads>;

  explicit IndexStringHash(const Arena* arena, const Directory* directory)
      : arena(arena), directory(directory) {}

  size_t operator()(Numeric numeric) const {
    return hash(Resolve(numeric).value());
  }
  template <typename Arg>
  size_t operator()(StringArenaKeyForFind<Arg> key) const {
    return key.hash;
  }
  template <typename Resolved>
  size_t operator()(StringIndexKeyForFindResolved<Resolved> key) const {
    return key.hash;
  }

 private:
  Element Resolve(Numeric numeric) const {
    return arena->template ResolveAddress<alignment>(
        IntCast<size_t>((*directory)[IntCast<size_t>(numeric)]));
  }

  const Arena* arena;
  const Directory* directory;
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS typename Encoder::Hash hash;
};

// Specialization when `Address` is `void`.
template <typename Numeric, typename Encoder, bool concurrent_reads,
          size_t alignment, size_t static_min_block_size,
          size_t static_max_block_size>
struct IndexStringHash<Numeric, Encoder, /*Address=*/void, concurrent_reads,
                       alignment, static_min_block_size,
                       static_max_block_size> {
 public:
  using is_transparent = void;

  using Element = ArenaString::WithAlignment<alignment>;
  using Directory = StringDirectory<Element, concurrent_reads>;

  explicit IndexStringHash(const void* /*arena*/, const Directory* directory)
      : directory(directory) {}

  size_t operator()(Numeric numeric) const {
    return hash(Resolve(numeric).value());
  }
  template <typename Arg>
  size_t operator()(StringArenaKeyForFind<Arg> key) const {
    return key.hash;
  }
  template <typename Resolved>
  size_t operator()(StringIndexKeyForFindResolved<Resolved> key) const {
    return key.hash;
  }

 private:
  Element Resolve(Numeric numeric) const {
    return (*directory)[IntCast<size_t>(numeric)];
  }

  const Directory* directory;
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS typename Encoder::Hash hash;
};

// Primary template used when `Address` is not `void`.
template <typename Numeric, typename Encoder, typename Address,
          bool concurrent_reads, size_t alignment, size_t static_min_block_size,
          size_t static_max_block_size>
struct IndexStringEq {
 public:
  using is_transparent = void;

  using Element = ArenaString::WithAlignment<alignment>;
  using Arena = typename StringArena::WithConcurrentReads<concurrent_reads>::
      template WithBlockSize<static_min_block_size, static_max_block_size>;
  using Directory = StringDirectory<Address, concurrent_reads>;

  explicit IndexStringEq(const Arena* arena, const Directory* directory)
      : arena(arena), directory(directory) {}

  bool operator()(Numeric a, Numeric b) const { return a == b; }
  template <typename Arg>
  bool operator()(Numeric a, StringArenaKeyForFind<Arg> b) const {
    return eq(Resolve(a).value(), b.arg);
  }
  template <typename Arg>
  bool operator()(StringArenaKeyForFind<Arg> a, Numeric b) const {
    return eq(Resolve(b).value(), a.arg);
  }
  template <typename Resolved>
  bool operator()(Numeric a, StringIndexKeyForFindResolved<Resolved> b) const {
    return Resolve(a).data() == b.value.data();
  }
  template <typename Resolved>
  bool operator()(StringIndexKeyForFindResolved<Resolved> a, Numeric b) const {
    return a.value.data() == Resolve(b).data();
  }

 private:
  Element Resolve(Numeric numeric) const {
    return arena->template ResolveAddress<alignment>(
        IntCast<size_t>((*directory)[IntCast<size_t>(numeric)]));
  }

  const Arena* arena;
  const Directory* directory;
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS typename Encoder::Eq eq;
};

// Specialization when `Address` is `void`.
template <typename Numeric, typename Encoder, bool concurrent_reads,
          size_t alignment, size_t static_min_block_size,
          size_t static_max_block_size>
struct IndexStringEq<Numeric, Encoder, /*Address=*/void, concurrent_reads,
                     alignment, static_min_block_size, static_max_block_size> {
  using is_transparent = void;

  using Element = ArenaString::WithAlignment<alignment>;
  using Directory = StringDirectory<Element, concurrent_reads>;

  explicit IndexStringEq(const void* /*arena*/, const Directory* directory)
      : directory(directory) {}

  bool operator()(Numeric a, Numeric b) const { return a == b; }
  template <typename Arg>
  bool operator()(Numeric a, StringArenaKeyForFind<Arg> b) const {
    return eq(Resolve(a).value(), b.arg);
  }
  template <typename Arg>
  bool operator()(StringArenaKeyForFind<Arg> a, Numeric b) const {
    return eq(Resolve(b).value(), a.arg);
  }
  template <typename Resolved>
  bool operator()(Numeric a, StringIndexKeyForFindResolved<Resolved> b) const {
    return Resolve(a).data() == b.value.data();
  }
  template <typename Resolved>
  bool operator()(StringIndexKeyForFindResolved<Resolved> a, Numeric b) const {
    return a.value.data() == Resolve(b).data();
  }

 private:
  Element Resolve(Numeric numeric) const {
    return (*directory)[IntCast<size_t>(numeric)];
  }

  const Directory* directory;
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS typename Encoder::Eq eq;
};

template <typename Numeric, typename Encoder, typename Address,
          typename SetMutex, typename ArenaMutex, size_t alignment,
          size_t static_min_block_size, size_t static_max_block_size>
class alignas(kInternerShardAlignment<SetMutex>) IndexStringInternerShard {
 private:
  static constexpr bool kDirectoryConcurrentReads =
      !std::is_same_v<ArenaMutex, NullMutex>;
  static constexpr bool kArenaConcurrentReads =
      kDirectoryConcurrentReads && !std::is_void_v<Address>;

 public:
  using Element = ArenaString::WithAlignment<alignment>;
  using Arena =
      typename StringArena::WithConcurrentReads<kArenaConcurrentReads>::
          template WithBlockSize<static_min_block_size, static_max_block_size>;
  using DirectoryElement =
      std::conditional_t<std::is_void_v<Address>, Element, Address>;
  using Directory =
      StringDirectory<DirectoryElement, kDirectoryConcurrentReads>;

  explicit IndexStringInternerShard(const Arena* arena,
                                    const Directory* directory)
      : indices_(0, IndexHash(arena, directory), IndexEq(arena, directory)) {}

  IndexStringInternerShard(const IndexStringInternerShard&) = delete;
  IndexStringInternerShard& operator=(const IndexStringInternerShard&) = delete;

  void Reset() ABSL_NO_THREAD_SAFETY_ANALYSIS { indices_.clear(); }

  void Reserve(size_t capacity) {
    RIEGELI_ASSERT_GT(capacity, 0u)
        << "Failed precondition of IndexStringInternerShard::Reserve(): "
           "capacity is zero";
    MutexLock<SetMutex> set_lock(set_mutex_);
    indices_.reserve(capacity);
  }

  template <typename Arg>
  Numeric Intern(const Arg& value, size_t hash, Arena& arena,
                 Directory& directory, ArenaMutex& arena_mutex, bool& is_new) {
    {
      ReaderMutexLock<SetMutex> set_lock(set_mutex_);
      const auto iter = indices_.find(StringArenaKeyForFind<Arg>{value, hash});
      if (ABSL_PREDICT_TRUE(iter != indices_.end())) {
        is_new = false;
        return *iter;
      }
    }
    return InternSlow(value, hash, arena, directory, arena_mutex, is_new);
  }

  template <bool verified_new, typename Arg>
  Numeric InternNew(const Arg& value, size_t hash, Arena& arena,
                    Directory& directory, ArenaMutex& arena_mutex,
                    bool& is_new);

  template <typename Arg>
  Numeric Find(const Arg& arg, size_t hash) const {
    ReaderMutexLock<SetMutex> set_lock(set_mutex_);
    const auto iter = indices_.find(StringArenaKeyForFind<Arg>{arg, hash});
    if (iter != indices_.end()) return *iter;
    return kNullNumeric<Numeric>;
  }

  template <typename Resolved>
  Numeric IndexOf(Resolved value, size_t hash) const {
    ReaderMutexLock<SetMutex> set_lock(set_mutex_);
    const auto iter =
        indices_.find(StringIndexKeyForFindResolved<Resolved>{value, hash});
    RIEGELI_ASSERT(iter != indices_.end())
        << "Failed precondition of "
           "IndexInternedString::Interner::IndexOf(): "
           "resolved string not found in this interner";
    return *iter;
  }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const IndexStringInternerShard* self,
                                        MemoryEstimator& memory_estimator) {
    ReaderMutexLock<SetMutex> set_lock(self->set_mutex_);
    memory_estimator.RegisterSubobjects(&self->indices_);
  }

  void Archive() ABSL_NO_THREAD_SAFETY_ANALYSIS {
    indices_ = absl::flat_hash_set<Numeric, IndexHash, IndexEq>(
        0, indices_.hash_function(), indices_.key_eq());
  }

 private:
  using IndexHash =
      IndexStringHash<Numeric, Encoder, Address, kDirectoryConcurrentReads,
                      alignment, static_min_block_size, static_max_block_size>;
  using IndexEq =
      IndexStringEq<Numeric, Encoder, Address, kDirectoryConcurrentReads,
                    alignment, static_min_block_size, static_max_block_size>;

  template <typename Arg>
  ABSL_ATTRIBUTE_NOINLINE Numeric InternSlow(const Arg& value, size_t hash,
                                             Arena& arena, Directory& directory,
                                             ArenaMutex& arena_mutex,
                                             bool& is_new);

  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS mutable SetMutex set_mutex_;
  absl::flat_hash_set<Numeric, IndexHash, IndexEq> indices_
      ABSL_GUARDED_BY(set_mutex_);
};

template <typename Numeric, typename Encoder, typename Address,
          typename SetMutex, typename ArenaMutex, size_t alignment,
          size_t static_min_block_size, size_t static_max_block_size>
template <typename Arg>
Numeric IndexStringInternerShard<
    Numeric, Encoder, Address, SetMutex, ArenaMutex, alignment,
    static_min_block_size,
    static_max_block_size>::InternSlow(const Arg& value, size_t hash,
                                       Arena& arena, Directory& directory,
                                       ArenaMutex& arena_mutex, bool& is_new) {
  return InternNew</*verified_new=*/true>(value, hash, arena, directory,
                                          arena_mutex, is_new);
}

template <typename Numeric, typename Encoder, typename Address,
          typename SetMutex, typename ArenaMutex, size_t alignment,
          size_t static_min_block_size, size_t static_max_block_size>
template <bool verified_new, typename Arg>
inline Numeric IndexStringInternerShard<
    Numeric, Encoder, Address, SetMutex, ArenaMutex, alignment,
    static_min_block_size,
    static_max_block_size>::InternNew(const Arg& value, size_t hash,
                                      Arena& arena, Directory& directory,
                                      ArenaMutex& arena_mutex, bool& is_new) {
  if constexpr (verified_new && std::is_same_v<SetMutex, NullMutex>) {
    Numeric next_index;
    {
      MutexLock<ArenaMutex> arena_lock(arena_mutex);
      next_index = IntCast<Numeric>(directory.size());
      if (ABSL_PREDICT_FALSE(next_index == kNullNumeric<Numeric>)) {
        is_new = false;
        return kNullNumeric<Numeric>;
      }
      if constexpr (std::is_void_v<Address>) {
        Element allocated;
        if (!Encoder::EncodedEmpty(value)) {
          allocated = arena.template Allocate<alignment, Encoder>(value);
        }
        directory.Allocate(allocated);
      } else {
        const size_t raw_address =
            arena.template AllocateWithAddress<alignment, Encoder>(value);
        if (ABSL_PREDICT_FALSE(raw_address >
                               std::numeric_limits<Address>::max())) {
          arena.UndoAllocate(
              arena.template ResolveAddress<alignment>(raw_address));
          is_new = false;
          return kNullNumeric<Numeric>;
        }
        directory.Allocate(static_cast<Address>(raw_address));
      }
    }

    MutexLock<SetMutex> set_lock(set_mutex_);
    RIEGELI_EVAL_ASSERT(indices_.emplace(next_index).second);
    is_new = true;
    return next_index;
  } else {
    DirectoryElement allocated;
    size_t raw_address;  // Used if `Address` is not `void`.
    if constexpr (std::is_void_v<Address>) {
      if (!Encoder::EncodedEmpty(value)) {
        MutexLock<ArenaMutex> lock(arena_mutex);
        allocated = arena.template Allocate<alignment, Encoder>(value);
      }
    } else {
      MutexLock<ArenaMutex> lock(arena_mutex);
      raw_address =
          arena.template AllocateWithAddress<alignment, Encoder>(value);
      if (ABSL_PREDICT_FALSE(raw_address >
                             std::numeric_limits<Address>::max())) {
        arena.UndoAllocate(
            arena.template ResolveAddress<alignment>(raw_address));
        is_new = false;
        return kNullNumeric<Numeric>;
      }
      allocated = static_cast<Address>(raw_address);
    }

    // Do not allocate a directory entry before verifying that the string is
    // absent from `indices_`, to ensure that `directory.size()` is monotonic.
    Numeric result;
    is_new = false;
    {
      MutexLock<SetMutex> set_lock(set_mutex_);
      const auto iter = indices_.lazy_emplace(
          StringArenaKeyForFind<Arg>{value, hash}, [&](const auto& ctor) {
            Numeric next_index;
            {
              MutexLock<ArenaMutex> lock(arena_mutex);
              next_index = IntCast<Numeric>(directory.size());
              if (ABSL_PREDICT_TRUE(next_index != kNullNumeric<Numeric>)) {
                directory.Allocate(allocated);
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
    if (ABSL_PREDICT_FALSE(!is_new)) {
      if constexpr (std::is_void_v<Address>) {
        if (!allocated.empty()) {
          MutexLock<ArenaMutex> lock(arena_mutex);
          arena.UndoAllocate(allocated);
        }
      } else {
        MutexLock<ArenaMutex> lock(arena_mutex);
        arena.UndoAllocate(
            arena.template ResolveAddress<alignment>(raw_address));
      }
    }
    return result;
  }
}

}  // namespace riegeli::interned_internal

#endif  // RIEGELI_INTERNED_INDEX_INTERNED_STRING_INTERNAL_H_
