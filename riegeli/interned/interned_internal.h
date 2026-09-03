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

#ifndef RIEGELI_INTERNED_INTERNED_INTERNAL_H_
#define RIEGELI_INTERNED_INTERNED_INTERNAL_H_

#include <stddef.h>

#include <atomic>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_set.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/ownership.h"
#include "riegeli/interned/interned_common_internal.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli::interned_internal {

// Supports heterogeneous lookup for `Element` being searched.
// Avoids calling `Hash` again.
template <typename Arg>
struct KeyForFind {
  const Arg& arg;
  size_t hash;
};

// Supports heterogeneous lookup for `Element` being erased. Avoids calling
// `Hash` and `Eq` again.
struct KeyForErase {
  const void* repr;  // Actually `const InternedRepr*`.
  size_t hash;
};

// The element type stored in the set in a `Shard`.
template <typename Repr>
struct Element {
  typename Repr::UniqueRepr repr;

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const Element* self,
                                        MemoryEstimator& memory_estimator) {
    // This `std::unique_ptr` is actually shared.
    if (memory_estimator.RegisterNode(self->repr.get())) {
      memory_estimator.RegisterDynamicObject(self->repr.get());
    }
  }
};

// Hash functor for `Element`. Supports heterogeneous lookup with `KeyForFind`
// and `KeyForErase`.
template <typename Repr, typename Hash>
struct ElementHash {
  using is_transparent = void;
  size_t operator()(const Element<Repr>& value) const {
    return hash(value.repr->value());
  }
  template <typename Arg>
  size_t operator()(KeyForFind<Arg> value) const {
    return value.hash;
  }
  size_t operator()(KeyForErase value) const { return value.hash; }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Hash hash;
};

// Equality functor for `Element`. Supports heterogeneous lookup with
// `KeyForFind` and `KeyForErase`.
template <typename Repr, typename Eq>
struct ElementEq {
  using is_transparent = void;
  bool operator()(const Element<Repr>& a, const Element<Repr>& b) const {
    return eq(a.repr->value(), b.repr->value());
  }
  template <typename Arg>
  bool operator()(const Element<Repr>& a, KeyForFind<Arg> b) const {
    return eq(a.repr->value(), b.arg);
  }
  bool operator()(const Element<Repr>& a, KeyForErase b) const {
    return a.repr.get() == b.repr;
  }
  template <typename Arg>
  bool operator()(KeyForFind<Arg> a, const Element<Repr>& b) const {
    return eq(b.repr->value(), a.arg);
  }
  bool operator()(KeyForErase a, const Element<Repr>& b) const {
    return b.repr.get() == a.repr;
  }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Eq eq;
};

// A single shard of the set of interned objects. The set is sharded by a part
// of the hash.
//
// `Repr::UniqueRepr` is stored in the set for each object. It is
// `std::unique_ptr<const Repr>` or a similar type.
//
// `SharedRepr` implements the interned handle. It is
// `IntrusiveSharedPtr<const Repr>` or a similar type.
template <typename Repr, typename SharedRepr, typename Hash, typename Eq,
          typename Interner>
class alignas(kInternerShardAlignment<typename Interner::Mutex>) Shard {
 public:
  static constexpr size_t kImmortal = ~size_t{0};

  Shard() = default;

  Shard(const Shard&) = delete;
  Shard& operator=(const Shard&) = delete;

  ~Shard() {
    RIEGELI_ASSERT(objects_.empty())
        << "Failed precondition of Shard::~Shard(): objects remaining";
  }

  void Reserve(size_t capacity) {
    RIEGELI_ASSERT_GT(capacity, 0u)
        << "Failed precondition of Shard::Reserve(): capacity is zero";
    MutexLock<Mutex> lock(mutex_);
    objects_.reserve(capacity);
  }

  template <typename Arg>
  SharedRepr Intern(Arg&& arg, size_t hash, const Interner& interner) {
    {
      ReaderMutexLock<Mutex> lock(mutex_);
      const auto iter = objects_.find(KeyForFind<Arg>{arg, hash});
      if (ABSL_PREDICT_TRUE(iter != objects_.end())) {
        return SharedRepr(iter->repr.get(), kShareOwnership);
      }
    }
    return InternSlow(std::forward<Arg>(arg), hash, interner);
  }

  template <typename Arg>
  absl_nullable SharedRepr Find(const Arg& arg, size_t hash) const {
    ReaderMutexLock<Mutex> lock(mutex_);
    const auto iter = objects_.find(KeyForFind<Arg>{arg, hash});
    if (iter != objects_.end()) {
      return SharedRepr(iter->repr.get(), kShareOwnership);
    }
    return nullptr;
  }

  // Common implementation of `Repr::Ref()` expected by `SharedRepr`.
  //
  // Increments the reference count.
  static void Ref(const Repr& repr) {
    if (repr.ref_count().load(std::memory_order_relaxed) == kImmortal) return;
    repr.ref_count().fetch_add(1, std::memory_order_relaxed);
  }

  // Common implementation of `Repr::Unref()` expected by `SharedRepr`.
  //
  // Decrements the reference count. Erases the object from the shard when the
  // reference count reaches 0.
  static void Unref(const Repr& repr) {
    size_t count = repr.ref_count().load(std::memory_order_relaxed);
    if (count == kImmortal) return;
    for (;;) {
      if (count == 1) {
        UnrefSlow(repr);
        return;
      }
      if (ABSL_PREDICT_TRUE(repr.ref_count().compare_exchange_weak(
              count, count - 1, std::memory_order_release,
              std::memory_order_relaxed))) {
        return;
      }
      // Another thread has just changed the reference count, or a spurious
      // failure of `compare_exchange_weak()` occurred. Retry.
    }
  }

  // Common implementation of `Repr::GetCount()` expected by `SharedRepr`.
  //
  // Returns the reference count reported to the caller for statistics.
  static size_t GetCount(const Repr& repr) {
    const size_t count = repr.ref_count().load(std::memory_order_relaxed);
    if (count == kImmortal) return 0;
    return count;
  }

  size_t NumObjects() const {
    ReaderMutexLock<Mutex> lock(mutex_);
    return objects_.size();
  }

  size_t TotalNumReferences() const {
    ReaderMutexLock<Mutex> lock(mutex_);
    size_t count = 0;
    for (const Element<Repr>& element : objects_) {
      count += element.repr->GetCount();
    }
    return count;
  }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const Shard* self,
                                        MemoryEstimator& memory_estimator) {
    ReaderMutexLock<Mutex> lock(self->mutex_);
    memory_estimator.RegisterSubobjects(&self->objects_);
  }

 private:
  using Mutex = typename Interner::Mutex;

  template <typename Arg>
  ABSL_ATTRIBUTE_NOINLINE SharedRepr InternSlow(Arg&& arg, size_t hash,
                                                const Interner& interner);

  ABSL_ATTRIBUTE_NOINLINE static void UnrefSlow(const Repr& repr);

  mutable Mutex mutex_;
  absl::flat_hash_set<Element<Repr>, ElementHash<Repr, Hash>,
                      ElementEq<Repr, Eq>>
      objects_ ABSL_GUARDED_BY(mutex_);
};

template <typename Repr, typename SharedRepr, typename Hash, typename Eq,
          typename Interner>
template <typename Arg>
auto Shard<Repr, SharedRepr, Hash, Eq, Interner>::InternSlow(
    Arg&& arg, size_t hash, const Interner& interner) -> SharedRepr {
  // Construct the object outside the lock.
  typename Repr::UniqueRepr new_repr =
      Repr::New(std::forward<Arg>(arg), interner);
  const Repr* const result = new_repr.get();

  MutexLock<Mutex> lock(mutex_);
  bool inserted = false;
  const auto iter = objects_.lazy_emplace(
      KeyForFind<decltype(result->value())>{result->value(), hash},
      [&](const auto& ctor) {
        ctor(std::move(new_repr));
        inserted = true;
      });
  if (ABSL_PREDICT_FALSE(!inserted)) {
    // Even though the object was not found before, another thread has just
    // inserted it and won the race.
    return SharedRepr(iter->repr.get(), kShareOwnership);
  }
  return SharedRepr(result);
}

template <typename Repr, typename SharedRepr, typename Hash, typename Eq,
          typename Interner>
void Shard<Repr, SharedRepr, Hash, Eq, Interner>::UnrefSlow(const Repr& repr) {
  const size_t hash = Hash()(repr.value());
  Shard& shard = repr.interner().GetShard(hash);
  shard.mutex_.lock();
  if (ABSL_PREDICT_FALSE(
          repr.ref_count().fetch_sub(1, std::memory_order_release) != 1)) {
    // Even though `count` was 1 before, another thread has just incremented
    // the reference count. The object is still in use after all.
    shard.mutex_.unlock();
    return;
  }

#ifdef THREAD_SANITIZER
  // TSAN does not support `std::atomic_thread_fence()`. Using `load()` instead
  // is less efficient but also correct.
  (void)repr.ref_count().load(std::memory_order_acquire);
#else
  std::atomic_thread_fence(std::memory_order_acquire);
#endif
  [[maybe_unused]] const auto node =
      shard.objects_.extract(KeyForErase{&repr, hash});
  RIEGELI_ASSERT(!node.empty());
  shard.mutex_.unlock();
  // Destroy the object stored in `node` outside the lock.
}

}  // namespace riegeli::interned_internal

#endif  // RIEGELI_INTERNED_INTERNED_INTERNAL_H_
