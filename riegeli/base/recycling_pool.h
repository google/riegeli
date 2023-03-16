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

#include <atomic>
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
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/background_cleaning.h"
#include "riegeli/base/no_destructor.h"

namespace riegeli {

// Options for `RecyclingPool` and `KeyedRecyclingPool`.
class RecyclingPoolOptions {
 public:
  RecyclingPoolOptions() = default;

  // Maximum number of objects to keep in a pool.
  //
  // 0 effectively disables the pool: objects are destroyed immediately.
  //
  // Default: `DefaultMaxSize()`,
  //          which is the maximum of 16 and hardware concurrency.
  static size_t DefaultMaxSize();
  RecyclingPoolOptions& set_max_size(size_t max_size) & {
    max_size_ = max_size;
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
  // Default: `absl::InfiniteDuration()`.
  // TODO: Change the default to `absl::Minutes(1)`.
  static constexpr absl::Duration kDefaultMaxAge = absl::InfiniteDuration();
  RecyclingPoolOptions& set_max_age(absl::Duration max_age) & {
    max_age_ = max_age;
    return *this;
  }
  RecyclingPoolOptions&& set_max_age(absl::Duration max_age) && {
    return std::move(set_max_age(max_age));
  }
  absl::Duration max_age() const { return max_age_; }

  friend bool operator==(const RecyclingPoolOptions& a,
                         const RecyclingPoolOptions& b) {
    return a.max_size_ == b.max_size_ && a.max_age_ == b.max_age_;
  }
  friend bool operator!=(const RecyclingPoolOptions& a,
                         const RecyclingPoolOptions& b) {
    return !(a == b);
  }

  template <typename HashState>
  friend HashState AbslHashValue(HashState hash_state,
                                 const RecyclingPoolOptions& self) {
    return HashState::combine(std::move(hash_state), self.max_size_,
                              self.max_age_);
  }

 private:
  static size_t DefaultMaxSizeSlow();

  size_t max_size_ = DefaultMaxSize();
  absl::Duration max_age_ = kDefaultMaxAge;
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
  // deleting it. If a particular object is not suitable for recycling, the
  // `Handle` should have `release()` called and the object can be deleted using
  // the original `Deleter`.
  using Handle = std::unique_ptr<T, Recycler>;

  // A `std::unique_ptr` which deletes the object. If a particular object is
  // suitable for recycling, it can be put back into the pool using `RawPut()`.
  using RawHandle = std::unique_ptr<T, Deleter>;

  // A refurbisher which does nothing; see `Get()`.
  struct DefaultRefurbisher {
    void operator()(T* ptr) const {}
  };

  explicit RecyclingPool(RecyclingPoolOptions options = RecyclingPoolOptions())
      : options_(options), ring_buffer_by_age_(options.max_size()) {}

  ABSL_DEPRECATED("Use RecyclingPoolOptions instead")
  explicit RecyclingPool(size_t max_size)
      : RecyclingPool(RecyclingPoolOptions().set_max_size(max_size)) {}

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

  RecyclingPoolOptions options_;
  // If not `nullptr` then `this` has been registered at `*cleaner_`.
  BackgroundCleaner* cleaner_ = nullptr;
  BackgroundCleaner::Token cleaner_token_;
  absl::Mutex mutex_;
  // All objects, ordered by age (older to newer).
  size_t ring_buffer_end_ ABSL_GUARDED_BY(mutex_) = 0;
  size_t ring_buffer_size_ ABSL_GUARDED_BY(mutex_) = 0;
  // Invariant: `ring_buffer_by_age_.size() == options_.max_size()`
  std::vector<Entry> ring_buffer_by_age_ ABSL_GUARDED_BY(mutex_);
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
  // deleting it. If a particular object is not suitable for recycling, the
  // `Handle` should have `release()` called and the object can be deleted using
  // the original `Deleter`.
  using Handle = std::unique_ptr<T, Recycler>;

  // A `std::unique_ptr` which deletes the object. If a particular object is
  // suitable for recycling, it can be put back into the pool using `RawPut()`.
  using RawHandle = std::unique_ptr<T, Deleter>;

  // A refurbisher which does nothing; see `Get()`.
  struct DefaultRefurbisher {
    void operator()(T* ptr) const {}
  };

  explicit KeyedRecyclingPool(
      RecyclingPoolOptions options = RecyclingPoolOptions())
      : options_(options) {}

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

  // Like `Get()`, but the object is not returned into the pool by the
  // destructor of its handle. If the object is suitable for recycling, it can
  // be put back into the pool using `RawPut()`.
  template <typename Factory, typename Refurbisher = DefaultRefurbisher>
  RawHandle RawGet(const Key& key, Factory&& factory,
                   Refurbisher&& refurbisher = DefaultRefurbisher());

  // Puts an idle object into the pool for recycling.
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

  RecyclingPoolOptions options_;
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

inline size_t RecyclingPoolOptions::DefaultMaxSize() {
  static const size_t kDefaultMaxSize = DefaultMaxSizeSlow();
  return kDefaultMaxSize;
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
  class Pools {
   public:
    RecyclingPool& GetPool(RecyclingPoolOptions options) {
      std::pair<const RecyclingPoolOptions, RecyclingPool>* cached =
          cache_.load(std::memory_order_acquire);
      if (ABSL_PREDICT_FALSE(cached == nullptr || cached->first != options)) {
        absl::MutexLock lock(&mutex_);
        const auto iter = pools_.try_emplace(options, options).first;
        cached = &*iter;
        cache_.store(cached, std::memory_order_release);
      }
      return cached->second;
    }

   private:
    // If not `nullptr`, points to the most recently returned node from
    // `pools_`.
    std::atomic<std::pair<const RecyclingPoolOptions, RecyclingPool>*> cache_{
        nullptr};
    absl::Mutex mutex_;
    // Pointer stability required for `GetPool()`, node stability required for
    // `cache_`.
    absl::node_hash_map<RecyclingPoolOptions, RecyclingPool> pools_
        ABSL_GUARDED_BY(mutex_);
  };

  static NoDestructor<Pools> kPools;
  return kPools->GetPool(options);
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
template <typename Factory, typename Refurbisher>
typename RecyclingPool<T, Deleter>::Handle RecyclingPool<T, Deleter>::Get(
    Factory&& factory, Refurbisher&& refurbisher) {
  RawHandle returned = RawGet(std::forward<Factory>(factory),
                              std::forward<Refurbisher>(refurbisher));
  return Handle(returned.release(),
                Recycler(this, std::move(returned.get_deleter())));
}

template <typename T, typename Deleter>
template <typename Factory, typename Refurbisher>
typename RecyclingPool<T, Deleter>::RawHandle RecyclingPool<T, Deleter>::RawGet(
    Factory&& factory, Refurbisher&& refurbisher) {
  RawHandle returned;
  {
    absl::MutexLock lock(&mutex_);
    if (ABSL_PREDICT_TRUE(ring_buffer_size_ > 0)) {
      if (ring_buffer_end_ == 0) ring_buffer_end_ = options_.max_size();
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
  if (ABSL_PREDICT_FALSE(options_.max_size() == 0)) return;
  RawHandle evicted;
  absl::MutexLock lock(&mutex_);
  // Add a newest entry. Evict the oldest entry if the pool is full.
  absl::Time deadline = absl::InfiniteFuture();
  if (options_.max_age() != absl::InfiniteDuration()) {
    if (ABSL_PREDICT_FALSE(cleaner_ == nullptr)) {
      cleaner_ = &BackgroundCleaner::global();
      cleaner_token_ = cleaner_->Register(this);
    }
    deadline = cleaner_->TimeNow() + options_.max_age();
  }
  Entry& entry = ring_buffer_by_age_[ring_buffer_end_];
  evicted = std::exchange(entry.object, std::move(object));
  entry.deadline = deadline;
  ++ring_buffer_end_;
  if (ring_buffer_end_ == options_.max_size()) ring_buffer_end_ = 0;
  if (ABSL_PREDICT_TRUE(ring_buffer_size_ < options_.max_size())) {
    ++ring_buffer_size_;
  }
  // If `deadline == absl::InfiniteFuture()` then `cleaner_` might be
  // `nullptr`.
  if (ring_buffer_size_ == 1 && deadline != absl::InfiniteFuture()) {
    cleaner_->ScheduleCleaning(cleaner_token_, deadline);
  }
  // Destroy `evicted` after releasing `mutex_`.
}

template <typename T, typename Deleter>
void RecyclingPool<T, Deleter>::Clean(absl::Time now) {
  absl::InlinedVector<RawHandle, 16> evicted;
  absl::MutexLock lock(&mutex_);
  size_t index = ring_buffer_end_;
  if (index < ring_buffer_size_) index += options_.max_size();
  index -= ring_buffer_size_;
  while (ring_buffer_size_ > 0) {
    Entry& entry = ring_buffer_by_age_[index];
    if (entry.deadline > now) {
      cleaner_->ScheduleCleaning(cleaner_token_, entry.deadline);
      break;
    }
    // Evict the oldest entry.
    evicted.push_back(std::move(entry.object));
    ++index;
    if (index == options_.max_size()) index = 0;
    --ring_buffer_size_;
  }
  // Destroy `evicted` after releasing `mutex_`.
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
  class Pools {
   public:
    KeyedRecyclingPool& GetPool(RecyclingPoolOptions options) {
      std::pair<const RecyclingPoolOptions, KeyedRecyclingPool>* cached =
          cache_.load(std::memory_order_acquire);
      if (ABSL_PREDICT_FALSE(cached == nullptr || cached->first != options)) {
        absl::MutexLock lock(&mutex_);
        const auto iter = pools_.try_emplace(options, options).first;
        cached = &*iter;
        cache_.store(cached, std::memory_order_release);
      }
      return cached->second;
    }

   private:
    // If not `nullptr`, points to the most recently returned node from
    // `pools_`.
    std::atomic<std::pair<const RecyclingPoolOptions, KeyedRecyclingPool>*>
        cache_{nullptr};
    absl::Mutex mutex_;
    // Pointer stability required for `GetPool()`, node stability required for
    // `cache_`.
    absl::node_hash_map<RecyclingPoolOptions, KeyedRecyclingPool> pools_
        ABSL_GUARDED_BY(mutex_);
  };

  static NoDestructor<Pools> kPools;
  return kPools->GetPool(options);
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
  if (ABSL_PREDICT_FALSE(options_.max_size() == 0)) return;
  RawHandle evicted;
  absl::MutexLock lock(&mutex_);
  // Add a newest entry with this key.
  absl::Time deadline = absl::InfiniteFuture();
  if (options_.max_age() != absl::InfiniteDuration()) {
    if (ABSL_PREDICT_FALSE(cleaner_ == nullptr)) {
      cleaner_ = &BackgroundCleaner::global();
      cleaner_token_ = cleaner_->Register(this);
    }
    deadline = cleaner_->TimeNow() + options_.max_age();
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
  if (ABSL_PREDICT_FALSE(by_age_.size() > options_.max_size())) {
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
  if (by_age_.size() == 1 && deadline != absl::InfiniteFuture()) {
    cleaner_->ScheduleCleaning(cleaner_token_, deadline);
  }
  // Destroy `evicted` after releasing `mutex_`.
}

template <typename T, typename Key, typename Deleter>
void KeyedRecyclingPool<T, Key, Deleter>::Clean(absl::Time now) {
  absl::InlinedVector<RawHandle, 16> evicted;
  absl::MutexLock lock(&mutex_);
  for (; !by_age_.empty(); by_age_.pop_front()) {
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
      cleaner_->ScheduleCleaning(cleaner_token_, by_age_entry.deadline);
      break;
    }
    // Evict the oldest entry.
    const typename ByKey::iterator by_key_iter = by_key_.find(by_age_entry.key);
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
  // Destroy `evicted` after releasing `mutex_`.
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_RECYCLING_POOL_H_
