// Copyright 2019 Google LLC
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

#ifndef RIEGELI_BASE_RECYCLING_POOL_H_
#define RIEGELI_BASE_RECYCLING_POOL_H_

#include <stddef.h>
#include <stdint.h>

#include <array>
#include <atomic>
#include <limits>
#include <list>
#include <memory>
#include <type_traits>  // IWYU pragma: keep
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/inlined_vector.h"
#include "absl/container/node_hash_map.h"
#include "absl/meta/type_traits.h"  // IWYU pragma: keep
#include "absl/numeric/bits.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/background_cleaning.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/no_destructor.h"

namespace riegeli {

// Options for `RecyclingPool` and `KeyedRecyclingPool`.
class RecyclingPoolOptions {
 public:
  RecyclingPoolOptions() = default;

  // `RecyclingPool::global()` and `KeyedRecyclingPool::global()` maintain
  // this many shards of pools, with the shard to use based on the thread which
  // called `global()`.
  //
  // A larger number of shards increases the probability that objects are
  // recycled by the same thread which has created them, and decreases lock
  // contention, at the cost of increasing memory usage.
  //
  // 1 effectively disables the sharding.
  //
  // This option does not affect `RecyclingPool` and `KeyedRecyclingPool`
  // constructors.
  //
  // Default: `kDefaultThreadShards` (16).
  static constexpr size_t kDefaultThreadShards = 16;
  RecyclingPoolOptions& set_thread_shards(size_t thread_shards) & {
    RIEGELI_ASSERT_GT(thread_shards, 0u)
        << "Failed precondition of RecyclingPoolOptions::set_thread_shards(): "
           "zero thread shards";
    thread_shards_ =
        absl::bit_floor(SaturatingIntCast<uint32_t>(thread_shards));
    return *this;
  }
  RecyclingPoolOptions&& set_thread_shards(size_t thread_shards) && {
    return std::move(set_thread_shards(thread_shards));
  }
  size_t thread_shards() const { return thread_shards_; }

  // Maximum number of objects to keep in a pool.
  //
  // 0 effectively disables the pool: objects are destroyed immediately.
  //
  // Default: `kDefaultMaxSize` (16).
  static constexpr size_t kDefaultMaxSize = 16;
  RecyclingPoolOptions& set_max_size(size_t max_size) & {
    max_size_ = SaturatingIntCast<uint32_t>(max_size);
    return *this;
  }
  RecyclingPoolOptions&& set_max_size(size_t max_size) && {
    return std::move(set_max_size(max_size));
  }
  size_t max_size() const { return max_size_; }

  // Maximum time for keeping an object in a pool. Objects idling for more than
  // this will be evicted in a background thread.
  //
  // `absl::InfiniteDuration()` disables time-based eviction.
  //
  // `absl::ZeroDuration()` lets objects be destroyed right after they are no
  // longer needed, but asynchronously, i.e. without blocking the calling
  // thread.
  //
  // Default: `kDefaultMaxAge` (`absl::Minutes(1)`).
  static constexpr absl::Duration kDefaultMaxAge = absl::Minutes(1);
  RecyclingPoolOptions& set_max_age(absl::Duration max_age) & {
    max_age_seconds_ = AgeToSeconds(max_age);
    return *this;
  }
  RecyclingPoolOptions&& set_max_age(absl::Duration max_age) && {
    return std::move(set_max_age(max_age));
  }
  absl::Duration max_age() const {
    return max_age_seconds_ == std::numeric_limits<uint32_t>::max()
               ? absl::InfiniteDuration()
               : absl::Seconds(max_age_seconds_);
  }

 private:
  // For `max_size_` and `max_age_seconds_`.
  template <typename T, typename Deleter>
  friend class RecyclingPool;
  // For `max_size_` and `max_age_seconds_`.
  template <typename T, typename Key, typename Deleter>
  friend class KeyedRecyclingPool;

  static uint32_t AgeToSeconds(absl::Duration age);  // Round up.

  // Use `uint32_t` instead of `size_t` to reduce the object size.
  // This is always a power of 2.
  uint32_t thread_shards_ = IntCast<uint32_t>(kDefaultThreadShards);
  // Use `uint32_t` instead of `size_t` to reduce the object size.
  uint32_t max_size_ = IntCast<uint32_t>(kDefaultMaxSize);
  // Use `uint32_t` instead of `absl::Duration` to reduce the object size.
  // `std::numeric_limits<uint32_t>::max()` means `absl::InfiniteDuration()`.
  uint32_t max_age_seconds_ = 60;  // `AgeToSeconds(kDefaultMaxAge)`
};

// `RecyclingPool<T, Deleter>` keeps a pool of idle objects of type `T`, so that
// instead of creating a new object of type `T`, an existing object can be
// recycled. This is helpful if constructing a new object is more expensive than
// resetting an existing object to the desired state.
//
// Deleter specifies how an object should be eventually deleted, like in
// `std::unique_ptr<T, Deleter>`.
//
// `RecyclingPool` is thread-safe.
template <typename T, typename Deleter = std::default_delete<T>>
class RecyclingPool : public BackgroundCleanee {
 public:
  // A deleter which puts the object back into the pool.
  class Recycler;

  // A `std::unique_ptr` which puts the object back into the pool instead of
  // deleting it. If a particular object is not suitable for recycling,
  // `DoNotRecycle()` can be used.
  using Handle = std::unique_ptr<T, Recycler>;

  // A `std::unique_ptr` which deletes the object. If a particular object is
  // suitable for recycling, it can be put back into the pool using `RawPut()`.
  using RawHandle = std::unique_ptr<T, Deleter>;

  // A refurbisher which does nothing; see `Get()`.
  struct DefaultRefurbisher {
    void operator()(ABSL_ATTRIBUTE_UNUSED T* ptr) const {}
  };

  explicit RecyclingPool(RecyclingPoolOptions options = RecyclingPoolOptions())
      : max_age_seconds_(options.max_age_seconds_),
        ring_buffer_by_age_(options.max_size()) {}

  RecyclingPool(const RecyclingPool&) = delete;
  RecyclingPool& operator=(const RecyclingPool&) = delete;

  ~RecyclingPool();

  // Returns a default global pool specific to template parameters of
  // `RecyclingPool` and `options`.
  static RecyclingPool& global(
      RecyclingPoolOptions options = RecyclingPoolOptions());

  // Uses a different `BackgroundCleaner` than `BackgroundCleaner::global()` for
  // scheduling background cleaning. This is useful for testing.
  //
  // Precondition: `BackgroundCleaner` was not used yet.
  void SetBackgroundCleaner(BackgroundCleaner* cleaner);

  // Deletes all objects stored in the pool.
  void Clear();

  // Creates an object, or returns an existing object from the pool if possible.
  //
  // `factory` takes no arguments and returns `RawHandle`. It is called to
  // create a new object.
  //
  // If `refurbisher` is specified, it takes a `T*` argument and its result is
  // ignored. It is called before returning an existing object.
  template <typename Factory, typename Refurbisher = DefaultRefurbisher>
  Handle Get(Factory&& factory,
             Refurbisher&& refurbisher = DefaultRefurbisher());

  // Deletes the object immediately, does not return it into the pool.
  //
  // This can be called on the result of `Get()` if the object turned out to be
  // not suitable for recycling.
  //
  // Equivalent to calling the original `Deleter` if `object != nullptr`.
  static void DoNotRecycle(Handle object);

  // Like `Get()`, but the object is not returned into the pool by the
  // destructor of its handle. If the object is suitable for recycling, it can
  // be put back into the pool using `RawPut()`.
  template <typename Factory, typename Refurbisher = DefaultRefurbisher>
  RawHandle RawGet(Factory&& factory,
                   Refurbisher&& refurbisher = DefaultRefurbisher());

  // Puts an idle object into the pool for recycling.
  void RawPut(RawHandle object);

 protected:
  void Clean(absl::Time now) override;

 private:
  struct Entry {
    RawHandle object;
    absl::Time deadline;
  };

  uint32_t max_age_seconds_;
  // If not `nullptr` then `this` has been registered at `*cleaner_`.
  BackgroundCleaner* cleaner_ = nullptr;
  BackgroundCleaner::Token cleaner_token_;
  absl::Mutex mutex_;
  // All objects, ordered by age (older to newer).
  uint32_t ring_buffer_end_ ABSL_GUARDED_BY(mutex_) = 0;
  uint32_t ring_buffer_size_ ABSL_GUARDED_BY(mutex_) = 0;
  // `ABSL_GUARDED_BY(mutex_)` for elements but not for `size()`.
  std::vector<Entry> ring_buffer_by_age_;
};

// `KeyedRecyclingPool<T, Key, Deleter>` keeps a pool of idle objects of type
// `T`, so that instead of creating a new object of type `T`, an existing object
// can be recycled. This is helpful if constructing a new object is more
// expensive than resetting an existing object to the desired state.
//
// Deleter specifies how an object should be eventually deleted, like in
// `std::unique_ptr<T, Deleter>`.
//
// The `Key` parameter allows to find an object to reuse only among compatible
// objects, which should be assigned the same key. The `Key` type must be
// equality comparable, hashable (by `absl::Hash`), default constructible, and
// copyable.
//
// `KeyedRecyclingPool` is thread-safe.
template <typename T, typename Key, typename Deleter = std::default_delete<T>>
class KeyedRecyclingPool : public BackgroundCleanee {
 public:
  // A deleter which puts the object back into the pool.
  class Recycler;

  // A `std::unique_ptr` which puts the object back into the pool instead of
  // deleting it. If a particular object is not suitable for recycling,
  // `DoNotRecycle()` can be used.
  using Handle = std::unique_ptr<T, Recycler>;

  // A `std::unique_ptr` which deletes the object. If a particular object is
  // suitable for recycling, it can be put back into the pool using `RawPut()`.
  using RawHandle = std::unique_ptr<T, Deleter>;

  // A refurbisher which does nothing; see `Get()`.
  struct DefaultRefurbisher {
    void operator()(ABSL_ATTRIBUTE_UNUSED T* ptr) const {}
  };

  explicit KeyedRecyclingPool(
      RecyclingPoolOptions options = RecyclingPoolOptions())
      : max_size_(options.max_size_),
        max_age_seconds_(options.max_age_seconds_) {}

  KeyedRecyclingPool(const KeyedRecyclingPool&) = delete;
  KeyedRecyclingPool& operator=(const KeyedRecyclingPool&) = delete;

  ~KeyedRecyclingPool();

  // Returns a default global pool specific to template parameters of
  // `KeyedRecyclingPool` and `options`.
  static KeyedRecyclingPool& global(
      RecyclingPoolOptions options = RecyclingPoolOptions());

  // Uses a different `BackgroundCleaner` than `BackgroundCleaner::global()` for
  // scheduling background cleaning. This is useful for testing.
  //
  // Precondition: `BackgroundCleaner` was not used yet.
  void SetBackgroundCleaner(BackgroundCleaner* cleaner);

  // Deletes all objects stored in the pool.
  void Clear();

  // Creates an object, or returns an existing object from the pool if possible.
  //
  // `factory` takes no arguments and returns `RawHandle`. It is called to
  // create a new object.
  //
  // If `refurbisher` is specified, it takes a `T*` argument and its result is
  // ignored. It is called before returning an existing object.
  template <typename Factory, typename Refurbisher = DefaultRefurbisher>
  Handle Get(Key key, Factory&& factory,
             Refurbisher&& refurbisher = DefaultRefurbisher());

  // Deletes the object immediately, does not return it into the pool.
  //
  // This can be called on the result of `Get()` if the object turned out to be
  // not suitable for recycling.
  //
  // Equivalent to calling the original `Deleter` if `object != nullptr`.
  static void DoNotRecycle(Handle object);

  // Like `Get()`, but the object is not returned into the pool by the
  // destructor of its handle. If the object is suitable for recycling, it can
  // be put back into the pool using `RawPut()`.
  template <typename Factory, typename Refurbisher = DefaultRefurbisher>
  RawHandle RawGet(const Key& key, Factory&& factory,
                   Refurbisher&& refurbisher = DefaultRefurbisher());

  // Puts an idle object into the pool for recycling, possibly under a different
  // key.
  void RawPut(const Key& key, RawHandle object);

 protected:
  void Clean(absl::Time now) override;

 private:
  struct ByAgeEntry {
    explicit ByAgeEntry(const Key& key, absl::Time deadline)
        : key(key), deadline(deadline) {}

    Key key;
    absl::Time deadline;
  };

  // Adding or removing elements in `ByAge` must not invalidate other iterators.
  using ByAge = std::list<ByAgeEntry>;

  struct ByKeyEntry {
    ByKeyEntry(RawHandle object, typename ByAge::iterator by_age_iter)
        : object(std::move(object)), by_age_iter(by_age_iter) {}

    RawHandle object;
    typename ByAge::iterator by_age_iter;
  };

  // `std::list` has a smaller overhead than `std::deque` for short sequences.
  using ByKeyEntries = std::list<ByKeyEntry>;

  using ByKey = absl::flat_hash_map<Key, ByKeyEntries>;

  uint32_t max_size_;
  uint32_t max_age_seconds_;
  // If not `nullptr` then `this` has been registered at `*cleaner_`.
  BackgroundCleaner* cleaner_ = nullptr;
  BackgroundCleaner::Token cleaner_token_;
  absl::Mutex mutex_;
  // The key of each object, ordered by the age of the object (older to
  // newer).
  ByAge by_age_ ABSL_GUARDED_BY(mutex_);
  // Objects grouped by their keys. Within each map value the list of objects is
  // non-empty and is ordered by their age (older to newer). Each object is
  // associated with the matching `by_age_` iterator.
  ByKey by_key_ ABSL_GUARDED_BY(mutex_);
  // Optimization for `Get()` followed by `Put()` with a matching key.
  // If `cache_ != by_key_.end()`, then `cache_->second.back().object` was
  // replaced with `nullptr` instead of erasing the corresponding entries,
  // to avoid allocating them again if a matching objects is put again.
  typename ByKey::iterator cache_ ABSL_GUARDED_BY(mutex_) = by_key_.end();
};

// Implementation details follow.

inline uint32_t RecyclingPoolOptions::AgeToSeconds(absl::Duration age) {
  if (age >= absl::Seconds(std::numeric_limits<uint32_t>::max())) {
    return std::numeric_limits<uint32_t>::max();
  }
  if (age <= absl::ZeroDuration()) return 0;
  int64_t seconds = absl::ToInt64Seconds(age);
  if (age != absl::Seconds(seconds)) ++seconds;  // Round up.
  return IntCast<uint32_t>(seconds);
}

namespace recycling_pool_internal {

template <typename RecyclingPool, typename Deleter, typename Enable = void>
class RecyclerRepr {
 public:
  RecyclerRepr() = default;

  explicit RecyclerRepr(RecyclingPool* pool, Deleter&& deleter)
      : deleter_(std::move(deleter)), pool_(pool) {}

  RecyclingPool* pool() const { return pool_; }

  Deleter& deleter() { return deleter_; }
  const Deleter& deleter() const { return deleter_; }

 private:
#if ABSL_HAVE_CPP_ATTRIBUTE(no_unique_address)
  [[no_unique_address]]
#endif
  Deleter deleter_;
  RecyclingPool* pool_ = nullptr;
};

#if !ABSL_HAVE_CPP_ATTRIBUTE(no_unique_address)

// If `[[no_unique_address]]` is not available, use empty base optimization for
// non-final classes.
template <typename RecyclingPool, typename Deleter>
class RecyclerRepr<
    RecyclingPool, Deleter,
    std::enable_if_t<absl::conjunction<
        std::is_class<Deleter>, absl::negation<std::is_final<Deleter>>>::value>>
    : private Deleter {
 public:
  RecyclerRepr() = default;

  explicit RecyclerRepr(RecyclingPool* pool, Deleter&& deleter)
      : Deleter(std::move(deleter)), pool_(pool) {}

  RecyclingPool* pool() const { return pool_; }

  Deleter& deleter() { return *this; }
  const Deleter& deleter() const { return *this; }

 private:
  RecyclingPool* pool_ = nullptr;
};

#endif

// Returns a number which does not change for the current thread, but is not
// necessarily unique for the current thread. Numbers are assigned densely.
inline size_t CurrentThreadNumber() {
  static std::atomic<size_t> next_thread_number = 0;
  thread_local const size_t current_thread_number =
      next_thread_number.fetch_add(1, std::memory_order_relaxed);
  return current_thread_number;
}

struct RecyclingPoolOptionsKey : WithEqual<RecyclingPoolOptionsKey> {
  explicit RecyclingPoolOptionsKey(uint32_t shard, uint32_t max_size,
                                   uint32_t max_age_seconds)
      : shard(shard), max_size(max_size), max_age_seconds(max_age_seconds) {}

  friend bool operator==(const RecyclingPoolOptionsKey& a,
                         const RecyclingPoolOptionsKey& b) {
    return a.shard == b.shard && a.max_size == b.max_size &&
           a.max_age_seconds == b.max_age_seconds;
  }

  template <typename HashState>
  friend HashState AbslHashValue(HashState hash_state,
                                 const RecyclingPoolOptionsKey& self) {
    return HashState::combine(std::move(hash_state), self.shard, self.max_size,
                              self.max_age_seconds);
  }

  uint32_t shard;
  uint32_t max_size;
  uint32_t max_age_seconds;
};

}  // namespace recycling_pool_internal

template <typename T, typename Deleter>
class RecyclingPool<T, Deleter>::Recycler {
 public:
  Recycler() = default;

  explicit Recycler(RecyclingPool* pool, Deleter&& deleter)
      : repr_(pool, std::move(deleter)) {
    RIEGELI_ASSERT(repr_.pool() != nullptr)
        << "Failed precondition of Recycler: null RecyclingPool pointer";
  }

  void operator()(T* ptr) const {
    RIEGELI_ASSERT(repr_.pool() != nullptr)
        << "Failed precondition of RecyclingPool::Recycler: "
           "default-constructed recycler used with an object";
    repr_.pool()->RawPut(RawHandle(ptr, original_deleter()));
  }

  Deleter& original_deleter() { return repr_.deleter(); }
  const Deleter& original_deleter() const { return repr_.deleter(); }

 private:
  recycling_pool_internal::RecyclerRepr<RecyclingPool, Deleter> repr_;
};

template <typename T, typename Deleter>
RecyclingPool<T, Deleter>::~RecyclingPool() {
  if (cleaner_ != nullptr) cleaner_->Unregister(cleaner_token_);
}

template <typename T, typename Deleter>
RecyclingPool<T, Deleter>& RecyclingPool<T, Deleter>::global(
    RecyclingPoolOptions options) {
  class ABSL_CACHELINE_ALIGNED Pools {
   public:
    RecyclingPool& GetPool(size_t shard, RecyclingPoolOptions options) {
      std::pair<const recycling_pool_internal::RecyclingPoolOptionsKey,
                RecyclingPool>* cached = cache_.load(std::memory_order_acquire);
      const recycling_pool_internal::RecyclingPoolOptionsKey options_key(
          IntCast<uint32_t>(shard), options.max_size_,
          options.max_age_seconds_);
      if (ABSL_PREDICT_FALSE(cached == nullptr ||
                             cached->first != options_key)) {
        absl::MutexLock lock(&mutex_);
        const auto iter = pools_.try_emplace(options_key, options).first;
        cached = &*iter;
        cache_.store(cached, std::memory_order_release);
      }
      return cached->second;
    }

   private:
    // If not `nullptr`, points to the most recently returned node from
    // `pools_`.
    std::atomic<std::pair<
        const recycling_pool_internal::RecyclingPoolOptionsKey, RecyclingPool>*>
        cache_{nullptr};
    absl::Mutex mutex_;
    // Pointer stability required for `GetPool()`, node stability required for
    // `cache_`.
    absl::node_hash_map<recycling_pool_internal::RecyclingPoolOptionsKey,
                        RecyclingPool>
        pools_ ABSL_GUARDED_BY(mutex_);
  };

  RIEGELI_ASSERT(absl::has_single_bit(options.thread_shards()))
      << "Number of shards must be a power of 2: " << options.thread_shards();
  static NoDestructor<
      std::array<Pools, RecyclingPoolOptions::kDefaultThreadShards>>
      kPools;
  const size_t shard = options.thread_shards() <= 1
                           ? 0
                           : recycling_pool_internal::CurrentThreadNumber() &
                                 (options.thread_shards() - 1);
  return (*kPools)[shard % RecyclingPoolOptions::kDefaultThreadShards].GetPool(
      shard, options);
}

template <typename T, typename Deleter>
void RecyclingPool<T, Deleter>::SetBackgroundCleaner(
    BackgroundCleaner* cleaner) {
  RIEGELI_ASSERT(cleaner_ == nullptr)
      << "Failed precondition of RecyclingPool::SetBackgroundCleaner(): "
         "BackgroundCleaner was already used";
  cleaner_ = cleaner;
  cleaner_token_ = cleaner_->Register(this);
}

template <typename T, typename Deleter>
void RecyclingPool<T, Deleter>::Clear() {
  if (cleaner_ != nullptr) cleaner_->CancelCleaning(cleaner_token_);
  absl::InlinedVector<RawHandle, 16> evicted;
  absl::MutexLock lock(&mutex_);
  evicted.reserve(ring_buffer_size_);
  while (ring_buffer_size_ > 0) {
    if (ring_buffer_end_ == 0) {
      ring_buffer_end_ = IntCast<uint32_t>(ring_buffer_by_age_.size());
    }
    --ring_buffer_end_;
    --ring_buffer_size_;
    evicted.push_back(std::move(ring_buffer_by_age_[ring_buffer_end_].object));
  }
  // Destroy `evicted` after releasing `mutex_`.
}

template <typename T, typename Deleter>
template <typename Factory, typename Refurbisher>
typename RecyclingPool<T, Deleter>::Handle RecyclingPool<T, Deleter>::Get(
    Factory&& factory, Refurbisher&& refurbisher) {
  RawHandle returned = RawGet(std::forward<Factory>(factory),
                              std::forward<Refurbisher>(refurbisher));
  return Handle(returned.release(),
                Recycler(this, std::move(returned.get_deleter())));
}

template <typename T, typename Deleter>
void RecyclingPool<T, Deleter>::DoNotRecycle(Handle object) {
  T* const ptr = object.release();
  if (ptr != nullptr) object.get_deleter().original_deleter()(ptr);
}

template <typename T, typename Deleter>
template <typename Factory, typename Refurbisher>
typename RecyclingPool<T, Deleter>::RawHandle RecyclingPool<T, Deleter>::RawGet(
    Factory&& factory, Refurbisher&& refurbisher) {
  RawHandle returned;
  {
    absl::MutexLock lock(&mutex_);
    if (ABSL_PREDICT_TRUE(ring_buffer_size_ > 0)) {
      if (ring_buffer_end_ == 0) {
        ring_buffer_end_ = IntCast<uint32_t>(ring_buffer_by_age_.size());
      }
      --ring_buffer_end_;
      --ring_buffer_size_;
      // Return the newest entry.
      returned = std::move(ring_buffer_by_age_[ring_buffer_end_].object);
    }
  }
  if (ABSL_PREDICT_TRUE(returned != nullptr)) {
    std::forward<Refurbisher>(refurbisher)(returned.get());
  } else {
    returned = std::forward<Factory>(factory)();
  }
  return returned;
}

template <typename T, typename Deleter>
void RecyclingPool<T, Deleter>::RawPut(RawHandle object) {
  if (ABSL_PREDICT_FALSE(ring_buffer_by_age_.empty())) return;
  RawHandle evicted;
  absl::Time deadline = absl::InfiniteFuture();
  {
    absl::MutexLock lock(&mutex_);
    // Add a newest entry. Evict the oldest entry if the pool is full.
    if (max_age_seconds_ != std::numeric_limits<uint32_t>::max()) {
      if (ABSL_PREDICT_FALSE(cleaner_ == nullptr)) {
        cleaner_ = &BackgroundCleaner::global();
        cleaner_token_ = cleaner_->Register(this);
      }
      deadline = cleaner_->TimeNow() + absl::Seconds(max_age_seconds_);
    }
    Entry& entry = ring_buffer_by_age_[ring_buffer_end_];
    evicted = std::exchange(entry.object, std::move(object));
    entry.deadline = deadline;
    ++ring_buffer_end_;
    if (ring_buffer_end_ == ring_buffer_by_age_.size()) ring_buffer_end_ = 0;
    if (ABSL_PREDICT_TRUE(ring_buffer_size_ < ring_buffer_by_age_.size())) {
      ++ring_buffer_size_;
    }
    // If `deadline == absl::InfiniteFuture()` then `cleaner_` might be
    // `nullptr`.
    if (ring_buffer_size_ > 1 || deadline == absl::InfiniteFuture()) {
      // No need to schedule cleaning.
      return;
    }
  }
  // Schedule cleaning and destroy `evicted` after releasing `mutex_`.
  cleaner_->ScheduleCleaning(cleaner_token_, deadline);
}

template <typename T, typename Deleter>
void RecyclingPool<T, Deleter>::Clean(absl::Time now) {
  absl::InlinedVector<RawHandle, 16> evicted;
  absl::Time deadline;
  {
    absl::MutexLock lock(&mutex_);
    size_t index = ring_buffer_end_;
    if (index < ring_buffer_size_) index += ring_buffer_by_age_.size();
    index -= ring_buffer_size_;
    for (;;) {
      if (ring_buffer_size_ == 0) {
        // Everything evicted, no need to schedule cleaning.
        return;
      }
      Entry& entry = ring_buffer_by_age_[index];
      if (entry.deadline > now) {
        // Schedule cleaning for the remaining entries.
        deadline = entry.deadline;
        break;
      }
      // Evict the oldest entry.
      evicted.push_back(std::move(entry.object));
      ++index;
      if (index == ring_buffer_by_age_.size()) index = 0;
      --ring_buffer_size_;
    }
  }
  // Schedule cleaning and destroy `evicted` after releasing `mutex_`.
  cleaner_->ScheduleCleaning(cleaner_token_, deadline);
}

template <typename T, typename Key, typename Deleter>
class KeyedRecyclingPool<T, Key, Deleter>::Recycler {
 public:
  Recycler() = default;

  explicit Recycler(KeyedRecyclingPool* pool, Key&& key, Deleter&& deleter)
      : repr_(pool, std::move(deleter)), key_(std::move(key)) {
    RIEGELI_ASSERT(repr_.pool() != nullptr)
        << "Failed precondition of Recycler: null KeyedRecyclingPool pointer";
  }

  void operator()(T* ptr) const {
    RIEGELI_ASSERT(repr_.pool() != nullptr)
        << "Failed precondition of KeyedRecyclingPool::Recycler: "
           "default-constructed recycler used with an object";
    repr_.pool()->RawPut(key_, RawHandle(ptr, original_deleter()));
  }

  Deleter& original_deleter() { return repr_.deleter(); }
  const Deleter& original_deleter() const { return repr_.deleter(); }

 private:
  recycling_pool_internal::RecyclerRepr<KeyedRecyclingPool, Deleter> repr_;
  Key key_;
};

template <typename T, typename Key, typename Deleter>
KeyedRecyclingPool<T, Key, Deleter>::~KeyedRecyclingPool() {
  if (cleaner_ != nullptr) cleaner_->Unregister(cleaner_token_);
}

template <typename T, typename Key, typename Deleter>
KeyedRecyclingPool<T, Key, Deleter>&
KeyedRecyclingPool<T, Key, Deleter>::global(RecyclingPoolOptions options) {
  class ABSL_CACHELINE_ALIGNED Pools {
   public:
    KeyedRecyclingPool& GetPool(size_t shard, RecyclingPoolOptions options) {
      std::pair<const recycling_pool_internal::RecyclingPoolOptionsKey,
                KeyedRecyclingPool>* cached =
          cache_.load(std::memory_order_acquire);
      const recycling_pool_internal::RecyclingPoolOptionsKey options_key(
          IntCast<uint32_t>(shard), options.max_size_,
          options.max_age_seconds_);
      if (ABSL_PREDICT_FALSE(cached == nullptr ||
                             cached->first != options_key)) {
        absl::MutexLock lock(&mutex_);
        const auto iter = pools_.try_emplace(options_key, options).first;
        cached = &*iter;
        cache_.store(cached, std::memory_order_release);
      }
      return cached->second;
    }

   private:
    // If not `nullptr`, points to the most recently returned node from
    // `pools_`.
    std::atomic<
        std::pair<const recycling_pool_internal::RecyclingPoolOptionsKey,
                  KeyedRecyclingPool>*>
        cache_{nullptr};
    absl::Mutex mutex_;
    // Pointer stability required for `GetPool()`, node stability required for
    // `cache_`.
    absl::node_hash_map<recycling_pool_internal::RecyclingPoolOptionsKey,
                        KeyedRecyclingPool>
        pools_ ABSL_GUARDED_BY(mutex_);
  };

  RIEGELI_ASSERT(absl::has_single_bit(options.thread_shards()))
      << "Number of shards must be a power of 2: " << options.thread_shards();
  static NoDestructor<
      std::array<Pools, RecyclingPoolOptions::kDefaultThreadShards>>
      kPools;
  const size_t shard = options.thread_shards() <= 1
                           ? 0
                           : recycling_pool_internal::CurrentThreadNumber() &
                                 (options.thread_shards() - 1);
  return (*kPools)[shard % RecyclingPoolOptions::kDefaultThreadShards].GetPool(
      shard, options);
}

template <typename T, typename Key, typename Deleter>
void KeyedRecyclingPool<T, Key, Deleter>::SetBackgroundCleaner(
    BackgroundCleaner* cleaner) {
  RIEGELI_ASSERT(cleaner_ == nullptr)
      << "Failed precondition of KeyedRecyclingPool::SetBackgroundCleaner(): "
         "BackgroundCleaner was already used";
  cleaner_ = cleaner;
  cleaner_token_ = cleaner_->Register(this);
}

template <typename T, typename Key, typename Deleter>
void KeyedRecyclingPool<T, Key, Deleter>::Clear() {
  if (cleaner_ != nullptr) cleaner_->CancelCleaning(cleaner_token_);
  ByAge evicted_by_age;
  ByKey evicted_by_key;
  absl::MutexLock lock(&mutex_);
  evicted_by_age = std::exchange(by_age_, ByAge());
  evicted_by_key = std::exchange(by_key_, ByKey());
  cache_ = by_key_.end();
  // Destroy `evicted_by_age` and `evicted_by_key` after releasing `mutex_`.
}

template <typename T, typename Key, typename Deleter>
template <typename Factory, typename Refurbisher>
typename KeyedRecyclingPool<T, Key, Deleter>::Handle
KeyedRecyclingPool<T, Key, Deleter>::Get(Key key, Factory&& factory,
                                         Refurbisher&& refurbisher) {
  RawHandle returned = RawGet(key, std::forward<Factory>(factory),
                              std::forward<Refurbisher>(refurbisher));
  return Handle(
      returned.release(),
      Recycler(this, std::move(key), std::move(returned.get_deleter())));
}

template <typename T, typename Key, typename Deleter>
void KeyedRecyclingPool<T, Key, Deleter>::DoNotRecycle(Handle object) {
  T* const ptr = object.release();
  if (ptr != nullptr) object.get_deleter().original_deleter()(ptr);
}

template <typename T, typename Key, typename Deleter>
template <typename Factory, typename Refurbisher>
typename KeyedRecyclingPool<T, Key, Deleter>::RawHandle
KeyedRecyclingPool<T, Key, Deleter>::RawGet(const Key& key, Factory&& factory,
                                            Refurbisher&& refurbisher) {
  RawHandle returned;
  {
    absl::MutexLock lock(&mutex_);
    if (cache_ != by_key_.end()) {
      // Finish erasing the cached entry.
      ByKeyEntries& by_key_entries = cache_->second;
      RIEGELI_ASSERT(!by_key_entries.empty())
          << "Failed invariant of KeyedRecyclingPool: "
             "empty by_key_ value";
      RIEGELI_ASSERT(by_key_entries.back().object == nullptr)
          << "Failed invariant of KeyedRecyclingPool: "
             "non-nullptr object pointed to by cache_";
      by_age_.erase(by_key_entries.back().by_age_iter);
      by_key_entries.pop_back();
      if (by_key_entries.empty()) by_key_.erase(cache_);
    }
    const typename ByKey::iterator by_key_iter = by_key_.find(key);
    if (ABSL_PREDICT_TRUE(by_key_iter != by_key_.end())) {
      // Return the newest entry with this key.
      ByKeyEntries& by_key_entries = by_key_iter->second;
      RIEGELI_ASSERT(!by_key_entries.empty())
          << "Failed invariant of KeyedRecyclingPool: "
             "empty by_key_ value";
      RIEGELI_ASSERT(by_key_entries.back().object != nullptr)
          << "Failed invariant of KeyedRecyclingPool: "
             "nullptr object not pointed to by cache_";
      returned = std::move(by_key_entries.back().object);
    }
    cache_ = by_key_iter;
  }
  if (ABSL_PREDICT_TRUE(returned != nullptr)) {
    std::forward<Refurbisher>(refurbisher)(returned.get());
  } else {
    returned = std::forward<Factory>(factory)();
  }
  return returned;
}

template <typename T, typename Key, typename Deleter>
void KeyedRecyclingPool<T, Key, Deleter>::RawPut(const Key& key,
                                                 RawHandle object) {
  if (ABSL_PREDICT_FALSE(max_size_ == 0)) return;
  RawHandle evicted;
  absl::Time deadline = absl::InfiniteFuture();
  {
    absl::MutexLock lock(&mutex_);
    // Add a newest entry with this key.
    if (max_age_seconds_ != std::numeric_limits<uint32_t>::max()) {
      if (ABSL_PREDICT_FALSE(cleaner_ == nullptr)) {
        cleaner_ = &BackgroundCleaner::global();
        cleaner_token_ = cleaner_->Register(this);
      }
      deadline = cleaner_->TimeNow() + absl::Seconds(max_age_seconds_);
    }
    if (ABSL_PREDICT_TRUE(cache_ != by_key_.end())) {
      ByKeyEntries& by_key_entries = cache_->second;
      RIEGELI_ASSERT(!by_key_entries.empty())
          << "Failed invariant of KeyedRecyclingPool: "
             "empty by_key_ value";
      RIEGELI_ASSERT(by_key_entries.back().object == nullptr)
          << "Failed invariant of KeyedRecyclingPool: "
             "non-nullptr object pointed to by cache_";
      if (ABSL_PREDICT_TRUE(cache_->first == key)) {
        // `cache_` hit. Set the object pointer again, move the entry to the end
        // of `by_age_`, and update its deadline.
        ByKeyEntry& by_key_entry = by_key_entries.back();
        by_key_entry.object = std::move(object);
        by_age_.splice(by_age_.end(), by_age_, by_key_entry.by_age_iter);
        by_key_entry.by_age_iter->deadline = deadline;
        goto done;
      }
      // `cache_` miss. Finish erasing the cached entry.
      by_age_.erase(by_key_entries.back().by_age_iter);
      by_key_entries.pop_back();
      if (by_key_entries.empty()) by_key_.erase(cache_);
    }
    by_age_.emplace_back(key, deadline);
    // Local scope so that `goto done` does not jump into the scope of
    // `by_age_iter`.
    {
      typename ByAge::iterator by_age_iter = by_age_.end();
      --by_age_iter;
      // This invalidates `by_key_` iterators, including `cache_`.
      by_key_[key].emplace_back(std::move(object), by_age_iter);
    }
    if (ABSL_PREDICT_FALSE(by_age_.size() > max_size_)) {
      // Evict the oldest entry.
      const typename ByKey::iterator by_key_iter =
          by_key_.find(by_age_.front().key);
      RIEGELI_ASSERT(by_key_iter != by_key_.end())
          << "Failed invariant of KeyedRecyclingPool: "
             "a key from by_age_ absent in by_key_";
      ByKeyEntries& by_key_entries = by_key_iter->second;
      RIEGELI_ASSERT(!by_key_entries.empty())
          << "Failed invariant of KeyedRecyclingPool: "
             "empty by_key_ value";
      RIEGELI_ASSERT(by_key_entries.front().object != nullptr)
          << "Failed invariant of KeyedRecyclingPool: "
             "nullptr object not pointed to by cache_";
      evicted = std::move(by_key_entries.front().object);
      by_key_entries.pop_front();
      if (by_key_entries.empty()) by_key_.erase(by_key_iter);
      by_age_.pop_front();
    }
  done:
    cache_ = by_key_.end();
    // If `deadline == absl::InfiniteFuture()` then `cleaner_` might be
    // `nullptr`.
    if (by_age_.size() > 1 || deadline == absl::InfiniteFuture()) {
      // No need to schedule cleaning.
      return;
    }
  }
  // Schedule cleaning and destroy `evicted` after releasing `mutex_`.
  cleaner_->ScheduleCleaning(cleaner_token_, deadline);
}

template <typename T, typename Key, typename Deleter>
void KeyedRecyclingPool<T, Key, Deleter>::Clean(absl::Time now) {
  absl::InlinedVector<RawHandle, 16> evicted;
  absl::Time deadline;
  {
    absl::MutexLock lock(&mutex_);
    for (;; by_age_.pop_front()) {
      if (by_age_.empty()) {
        // Everything evicted, no need to schedule cleaning.
        return;
      }
      const ByAgeEntry& by_age_entry = by_age_.front();
      if (by_age_entry.deadline > now) {
        if (cache_ != by_key_.end() && cache_->first == by_age_entry.key) {
          const ByKeyEntries& by_key_entries = cache_->second;
          RIEGELI_ASSERT(!by_key_entries.empty())
              << "Failed invariant of KeyedRecyclingPool: "
                 "empty by_key_ value";
          if (by_key_entries.front().object == nullptr) {
            // Finish erasing the cached entry.
            RIEGELI_ASSERT_EQ(by_key_entries.size(), 1u)
                << "Failed invariant of KeyedRecyclingPool: "
                   "nullptr object not at the end of by_key_ value";
            by_key_.erase(cache_);
            cache_ = by_key_.end();
            continue;
          }
        }
        // Schedule cleaning for the remaining entries.
        deadline = by_age_entry.deadline;
        break;
      }
      // Evict the oldest entry.
      const typename ByKey::iterator by_key_iter =
          by_key_.find(by_age_entry.key);
      RIEGELI_ASSERT(by_key_iter != by_key_.end())
          << "Failed invariant of KeyedRecyclingPool: "
             "a key from by_age_ absent in by_key_";
      ByKeyEntries& by_key_entries = by_key_iter->second;
      RIEGELI_ASSERT(!by_key_entries.empty())
          << "Failed invariant of KeyedRecyclingPool: "
             "empty by_key_ value";
      if (by_key_entries.front().object == nullptr) {
        // Finish erasing the cached entry.
        RIEGELI_ASSERT(cache_ == by_key_iter)
            << "Failed invariant of KeyedRecyclingPool: "
               "nullptr object not pointed to by cache_";
        RIEGELI_ASSERT(cache_->first == by_age_entry.key)
            << "Failed invariant of KeyedRecyclingPool: "
               "nullptr object not pointed to by cache_";
        RIEGELI_ASSERT_EQ(by_key_entries.size(), 1u)
            << "Failed invariant of KeyedRecyclingPool: "
               "nullptr object not at the end of by_key_ value";
        by_key_.erase(by_key_iter);
        cache_ = by_key_.end();
        continue;
      }
      evicted.push_back(std::move(by_key_entries.front().object));
      by_key_entries.pop_front();
      if (by_key_entries.empty()) by_key_.erase(by_key_iter);
    }
  }
  // Schedule cleaning and destroy `evicted` after releasing `mutex_`.
  cleaner_->ScheduleCleaning(cleaner_token_, deadline);
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_RECYCLING_POOL_H_
