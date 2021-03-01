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
#include <deque>
#include <list>
#include <memory>
#include <utility>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "riegeli/base/base.h"
#include "riegeli/base/memory.h"

namespace riegeli {

// `RecyclingPool<T, Deleter>` keeps a pool of idle objects of type `T`, so that
// instead of creating a new object of type `T`, an existing object can be
// recycled. This is helpful if constructing a new object is more expensive than
// resetting an existing object to the desired state.
//
// Deleter specifies how an object should be eventually deleted, like in
// `std::unique_ptr<T, Deleter>`.
//
// `RecyclingPool` is thread-safe.
template <typename T, typename Deleter>
class RecyclingPool {
 public:
  // A deleter which puts the object back into the pool.
  class Recycler : private Deleter {
   public:
    Recycler() {}

    void operator()(T* ptr) const;

    Deleter& original_deleter() { return *this; }
    const Deleter& original_deleter() const { return *this; }

   private:
    friend class RecyclingPool;

    explicit Recycler(RecyclingPool* pool, Deleter&& deleter)
        : Deleter(std::move(deleter)), pool_(pool) {}

    RecyclingPool* pool_ = nullptr;
    // TODO: Use `[[no_unique_address]]` when available instead of
    // relying on empty base optimization.
  };

  // A `std::unique_ptr` which puts the object back into the pool instead of
  // deleting it. If a particular object is not suitable for recycling, the
  // `Handle` should have `release()` called and the object can be deleted using
  // the original `Deleter`.
  using Handle = std::unique_ptr<T, Recycler>;

  // A refurbisher which does nothing; see `Get()`.
  struct DefaultRefurbisher {
    void operator()(T* ptr) const {}
  };

  // The default value of the constructor argument.
  static constexpr size_t kDefaultMaxSize = 16;

  // Creates a pool with the given maximum number of objects to keep.
  explicit RecyclingPool(size_t max_size = kDefaultMaxSize)
      : max_size_(max_size) {}

  RecyclingPool(const RecyclingPool&) = delete;
  RecyclingPool& operator=(const RecyclingPool&) = delete;

  // Returns a default global pool specific to template parameters of
  // `RecyclingPool`.
  //
  // If called multiple times with different `max_size` arguments, the last
  // `max_size` is in effect.
  static RecyclingPool& global(size_t max_size = kDefaultMaxSize);

  // Creates an object, or returns an existing object from the pool if possible.
  //
  // `factory` takes no arguments and returns `std::unique_ptr<T, Deleter>`.
  // It is called to create a new object.
  //
  // If `refurbisher` is specified, it takes a `T*` argument and its result is
  // ignored. It is called before returning an existing object.
  template <typename Factory, typename Refurbisher = DefaultRefurbisher>
  Handle Get(Factory factory, Refurbisher refurbisher = DefaultRefurbisher());

 private:
  void set_max_size(size_t max_size);

  void Put(std::unique_ptr<T, Deleter> object);

  std::atomic<size_t> max_size_;
  absl::Mutex mutex_;
  // All objects, ordered by freshness (older to newer).
  std::deque<std::unique_ptr<T, Deleter>> by_freshness_ ABSL_GUARDED_BY(mutex_);
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
class KeyedRecyclingPool {
 public:
  // A deleter which puts the object back into the pool.
  class Recycler : private Deleter {
   public:
    Recycler() {}

    void operator()(T* ptr) const;

    Deleter& original_deleter() { return *this; }
    const Deleter& original_deleter() const { return *this; }

   private:
    friend class KeyedRecyclingPool;

    explicit Recycler(KeyedRecyclingPool* pool, Key&& key, Deleter&& deleter)
        : Deleter(std::move(deleter)), pool_(pool), key_(std::move(key)) {}

    KeyedRecyclingPool* pool_ = nullptr;
    Key key_;
    // TODO: Use `[[no_unique_address]]` when available instead of
    // relying on empty base optimization.
  };

  // A `std::unique_ptr` which puts the object back into the pool instead of
  // deleting it. If a particular object is not suitable for recycling, the
  // `Handle` should have `release()` called and the object can be deleted using
  // the original `Deleter`.
  using Handle = std::unique_ptr<T, Recycler>;

  // A refurbisher which does nothing; see `Get()`.
  struct DefaultRefurbisher {
    void operator()(T* ptr) const {}
  };

  // The default value of the constructor argument.
  static constexpr size_t kDefaultMaxSize = 16;

  // Creates a pool with the given maximum number of objects to keep.
  explicit KeyedRecyclingPool(size_t max_size = kDefaultMaxSize)
      : max_size_(max_size), cache_(by_key_.end()) {}

  KeyedRecyclingPool(const KeyedRecyclingPool&) = delete;
  KeyedRecyclingPool& operator=(const KeyedRecyclingPool&) = delete;

  // Returns a default global pool specific to template parameters of
  // `KeyedRecyclingPool`.
  //
  // If called multiple times with different `max_size` arguments, the last
  // `max_size` is in effect.
  static KeyedRecyclingPool& global(size_t max_size = kDefaultMaxSize);

  // Creates an object, or returns an existing object from the pool if possible.
  //
  // `factory` takes no arguments and returns `std::unique_ptr<T, Deleter>`.
  // It is called to create a new object.
  //
  // If `refurbisher` is specified, it takes a `T*` argument and its result is
  // ignored. It is called before returning an existing object.
  template <typename Factory, typename Refurbisher = DefaultRefurbisher>
  Handle Get(Key key, Factory factory,
             Refurbisher refurbisher = DefaultRefurbisher());

 private:
  // Adding or removing elements in `ByFreshness` must not invalidate other
  // iterators.
  using ByFreshness = std::list<Key>;

  struct Entry {
    Entry(std::unique_ptr<T, Deleter> object,
          typename ByFreshness::iterator by_freshness_iter)
        : object(std::move(object)), by_freshness_iter(by_freshness_iter) {}

    std::unique_ptr<T, Deleter> object;
    typename ByFreshness::iterator by_freshness_iter;
  };

  // `std::list` has a smaller overhead than `std::deque` for short sequences.
  using Entries = std::list<Entry>;

  using ByKey = absl::flat_hash_map<Key, Entries>;

  void set_max_size(size_t max_size);

  void Put(const Key& key, std::unique_ptr<T, Deleter> object);

  std::atomic<size_t> max_size_;
  absl::Mutex mutex_;
  // The key of each object, ordered by the freshness of the object (older to
  // newer).
  ByFreshness by_freshness_ ABSL_GUARDED_BY(mutex_);
  // Objects grouped by their keys. Within each map value the list of objects is
  // non-empty and is ordered by their freshness (older to newer). Each object
  // is associated with the matching `by_freshness_` iterator.
  ByKey by_key_ ABSL_GUARDED_BY(mutex_);
  // Optimization for `Get()` followed by `Put()` with a matching key.
  // If `cache_ != by_key_.end()`, then `cache_->second.back().object` is
  // replaced with `nullptr` instead of erasing its entries.
  typename ByKey::iterator cache_ ABSL_GUARDED_BY(mutex_);
};

// Implementation details follow.

template <typename T, typename Deleter>
inline void RecyclingPool<T, Deleter>::Recycler::operator()(T* ptr) const {
  RIEGELI_ASSERT(pool_ != nullptr)
      << "Failed precondition of RecyclingPool::Recycler: "
         "default-constructed recycler used with an object";
  pool_->Put(std::unique_ptr<T, Deleter>(ptr, original_deleter()));
}

template <typename T, typename Deleter>
RecyclingPool<T, Deleter>& RecyclingPool<T, Deleter>::global(size_t max_size) {
  static NoDestructor<RecyclingPool> kStaticRecyclingPool(max_size);
  kStaticRecyclingPool->set_max_size(max_size);
  return *kStaticRecyclingPool;
}

template <typename T, typename Deleter>
inline void RecyclingPool<T, Deleter>::set_max_size(size_t max_size) {
  max_size_.store(max_size, std::memory_order_relaxed);
}

template <typename T, typename Deleter>
template <typename Factory, typename Refurbisher>
typename RecyclingPool<T, Deleter>::Handle RecyclingPool<T, Deleter>::Get(
    Factory factory, Refurbisher refurbisher) {
  std::unique_ptr<T, Deleter> returned;
  {
    absl::MutexLock lock(&mutex_);
    if (ABSL_PREDICT_TRUE(!by_freshness_.empty())) {
      // Return the newest entry.
      returned = std::move(by_freshness_.back());
      by_freshness_.pop_back();
    }
  }
  if (ABSL_PREDICT_TRUE(returned != nullptr)) {
    refurbisher(returned.get());
  } else {
    returned = factory();
  }
  return Handle(returned.release(),
                Recycler(this, std::move(returned.get_deleter())));
}

template <typename T, typename Deleter>
void RecyclingPool<T, Deleter>::Put(std::unique_ptr<T, Deleter> object) {
  std::vector<std::unique_ptr<T, Deleter>> evicted;
  absl::MutexLock lock(&mutex_);
  // Add a newest entry.
  by_freshness_.push_back(std::move(object));
  if (ABSL_PREDICT_FALSE(by_freshness_.size() >
                         max_size_.load(std::memory_order_relaxed))) {
    // Evict the oldest entry.
    evicted.push_back(std::move(by_freshness_.front()));
    by_freshness_.pop_front();
  }
  // Destroy `evicted` after releasing `mutex_`.
}

template <typename T, typename Key, typename Deleter>
inline void KeyedRecyclingPool<T, Key, Deleter>::Recycler::operator()(
    T* ptr) const {
  RIEGELI_ASSERT(pool_ != nullptr)
      << "Failed precondition of KeyedRecyclingPool::Recycler: "
         "default-constructed recycler used with an object";
  pool_->Put(key_, std::unique_ptr<T, Deleter>(ptr, original_deleter()));
}

template <typename T, typename Key, typename Deleter>
KeyedRecyclingPool<T, Key, Deleter>&
KeyedRecyclingPool<T, Key, Deleter>::global(size_t max_size) {
  static NoDestructor<KeyedRecyclingPool> kStaticKeyedRecyclingPool(max_size);
  kStaticKeyedRecyclingPool->set_max_size(max_size);
  return *kStaticKeyedRecyclingPool;
}

template <typename T, typename Key, typename Deleter>
inline void KeyedRecyclingPool<T, Key, Deleter>::set_max_size(size_t max_size) {
  max_size_.store(max_size, std::memory_order_relaxed);
}

template <typename T, typename Key, typename Deleter>
template <typename Factory, typename Refurbisher>
typename KeyedRecyclingPool<T, Key, Deleter>::Handle
KeyedRecyclingPool<T, Key, Deleter>::Get(Key key, Factory factory,
                                         Refurbisher refurbisher) {
  std::unique_ptr<T, Deleter> returned;
  {
    absl::MutexLock lock(&mutex_);
    if (cache_ != by_key_.end()) {
      // Finish erasing the cached entry.
      Entries& entries = cache_->second;
      RIEGELI_ASSERT(!entries.empty())
          << "Failed invariant of KeyedRecyclingPool: "
             "empty by_key_ value";
      RIEGELI_ASSERT(entries.back().object == nullptr)
          << "Failed invariant of KeyedRecyclingPool: "
             "non-nullptr object pointed to by cache_";
      by_freshness_.erase(entries.back().by_freshness_iter);
      entries.pop_back();
      if (entries.empty()) by_key_.erase(cache_);
    }
    const typename ByKey::iterator by_key_iter = by_key_.find(key);
    if (ABSL_PREDICT_TRUE(by_key_iter != by_key_.end())) {
      // Return the newest entry with this key.
      Entries& entries = by_key_iter->second;
      RIEGELI_ASSERT(!entries.empty())
          << "Failed invariant of KeyedRecyclingPool: "
             "empty by_key_ value";
      RIEGELI_ASSERT(entries.back().object != nullptr)
          << "Failed invariant of KeyedRecyclingPool: "
             "nullptr object not pointed to by cache_";
      returned = std::move(entries.back().object);
    }
    cache_ = by_key_iter;
  }
  if (ABSL_PREDICT_TRUE(returned != nullptr)) {
    refurbisher(returned.get());
  } else {
    returned = factory();
  }
  return Handle(
      returned.release(),
      Recycler(this, std::move(key), std::move(returned.get_deleter())));
}

template <typename T, typename Key, typename Deleter>
void KeyedRecyclingPool<T, Key, Deleter>::Put(
    const Key& key, std::unique_ptr<T, Deleter> object) {
  std::vector<std::unique_ptr<T, Deleter>> evicted;
  absl::MutexLock lock(&mutex_);
  // Add a newest entry with this key.
  if (ABSL_PREDICT_TRUE(cache_ != by_key_.end())) {
    Entries& entries = cache_->second;
    RIEGELI_ASSERT(!entries.empty())
        << "Failed invariant of KeyedRecyclingPool: "
           "empty by_key_ value";
    if (ABSL_PREDICT_TRUE(cache_->first == key)) {
      // `cache_` hit. Set the object pointer again.
      RIEGELI_ASSERT(entries.back().object == nullptr)
          << "Failed invariant of KeyedRecyclingPool: "
             "non-nullptr object pointed to by cache_";
      entries.back().object = std::move(object);
      cache_ = by_key_.end();
      return;
    }
    // `cache_` miss. Finish erasing the cached entry.
    by_freshness_.erase(entries.back().by_freshness_iter);
    entries.pop_back();
    if (entries.empty()) by_key_.erase(cache_);
  }
  by_freshness_.push_back(key);
  typename ByFreshness::iterator by_freshness_iter = by_freshness_.end();
  --by_freshness_iter;
  // This invalidates `by_key_` iterators, including `cache_`.
  by_key_[key].emplace_back(std::move(object), by_freshness_iter);
  while (ABSL_PREDICT_FALSE(by_freshness_.size() >
                            max_size_.load(std::memory_order_relaxed))) {
    // Evict the oldest entry.
    const Key& evicted_key = by_freshness_.front();
    const typename ByKey::iterator by_key_iter = by_key_.find(evicted_key);
    RIEGELI_ASSERT(by_key_iter != by_key_.end())
        << "Failed invariant of KeyedRecyclingPool: "
           "a key from by_freshness_ absent in by_key_";
    Entries& entries = by_key_iter->second;
    RIEGELI_ASSERT(!entries.empty())
        << "Failed invariant of KeyedRecyclingPool: "
           "empty by_key_ value";
    RIEGELI_ASSERT(entries.back().object != nullptr)
        << "Failed invariant of KeyedRecyclingPool: "
           "nullptr object not pointed to by cache_";
    evicted.push_back(std::move(entries.front().object));
    entries.pop_front();
    if (entries.empty()) by_key_.erase(by_key_iter);
    by_freshness_.pop_front();
  }
  cache_ = by_key_.end();
  // Destroy `evicted` after releasing `mutex_`.
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_RECYCLING_POOL_H_
