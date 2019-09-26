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

#include <deque>
#include <list>
#include <memory>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "riegeli/base/base.h"
#include "riegeli/base/memory.h"

namespace riegeli {

// `RecyclingPool<T, Deleter, Key>` keeps a pool of idle objects of type `T`, so
// that instead of creating a new object of type `T`, an existing object can be
// recycled. This is helpful if constructing a new object is more expensive than
// resetting an existing object to the desired state.
//
// Deleter specifies how an object should be eventually deleted, like in
// `std::unique_ptr<T, Deleter>`.
//
// Specifying `Key` allows to find an object to reuse only among compatible
// objects, which should be assigned the same key. The `Key` type must be
// equality comparable, hashable (by `absl::Hash`), default constructible, and
// copyable. If `Key` is `void`, all objects are considered compatible.
template <typename T, typename Deleter = std::default_delete<T>,
          typename Key = void>
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

    explicit Recycler(RecyclingPool* pool, Key&& key, Deleter&& deleter)
        : Deleter(std::move(deleter)), pool_(pool), key_(std::move(key)) {}

    RecyclingPool* pool_ = nullptr;
    Key key_;
    // TODO: Use `[[no_unique_address]]` when available instead of
    // relying on empty base optimization.
  };

  // An `std::unique_ptr` which puts the object back into the pool instead of
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

  // Creates a pool with the given maximal number of objects to keep.
  explicit RecyclingPool(size_t max_size = kDefaultMaxSize)
      : max_size_(max_size), cache_(by_key_.end()) {}

  RecyclingPool(const RecyclingPool&) = delete;
  RecyclingPool& operator=(const RecyclingPool&) = delete;

  // Returns a default global pool specific to template parameters of
  // `RecyclingPool`.
  static RecyclingPool& global();

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

  void Put(const Key& key, std::unique_ptr<T, Deleter> object);

  size_t max_size_;
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

// Specialization of `RecyclingPool` when `Key` is `void`. In this case `Get()`
// does not take a key as a parameter.
template <typename T, typename Deleter>
class RecyclingPool<T, Deleter, void> {
 public:
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

  using Handle = std::unique_ptr<T, Recycler>;

  struct DefaultRefurbisher {
    void operator()(T* ptr) const {}
  };

  static constexpr size_t kDefaultMaxSize = 16;

  explicit RecyclingPool(size_t max_size = kDefaultMaxSize)
      : max_size_(max_size) {}

  RecyclingPool(const RecyclingPool&) = delete;
  RecyclingPool& operator=(const RecyclingPool&) = delete;

  static RecyclingPool& global();

  template <typename Factory, typename Refurbisher = DefaultRefurbisher>
  Handle Get(Factory factory, Refurbisher refurbisher = DefaultRefurbisher());

 private:
  void Put(std::unique_ptr<T, Deleter> object);

  size_t max_size_;
  absl::Mutex mutex_;
  // All objects, ordered by freshness (older to newer).
  std::deque<std::unique_ptr<T, Deleter>> by_freshness_ ABSL_GUARDED_BY(mutex_);
};

// Implementation details follow.

template <typename T, typename Deleter, typename Key>
inline void RecyclingPool<T, Deleter, Key>::Recycler::operator()(T* ptr) const {
  RIEGELI_ASSERT(pool_ != nullptr)
      << "Failed precondition of RecyclingPool::Recycler: "
         "default-constructed recycler used with an object";
  pool_->Put(key_, std::unique_ptr<T, Deleter>(ptr, original_deleter()));
}

template <typename T, typename Deleter, typename Key>
RecyclingPool<T, Deleter, Key>& RecyclingPool<T, Deleter, Key>::global() {
  static NoDestructor<RecyclingPool> kStaticRecyclingPool;
  return *kStaticRecyclingPool;
}

template <typename T, typename Deleter, typename Key>
template <typename Factory, typename Refurbisher>
typename RecyclingPool<T, Deleter, Key>::Handle
RecyclingPool<T, Deleter, Key>::Get(Key key, Factory factory,
                                    Refurbisher refurbisher) {
  std::unique_ptr<T, Deleter> returned;
  {
    absl::MutexLock lock(&mutex_);
    if (cache_ != by_key_.end()) {
      // Finish erasing the cached entry.
      Entries& entries = cache_->second;
      RIEGELI_ASSERT(!entries.empty()) << "Failed invariant of RecyclingPool: "
                                          "empty by_key_ value";
      RIEGELI_ASSERT(entries.back().object == nullptr)
          << "Failed invariant of RecyclingPool: "
             "non-nullptr object pointed to by cache_";
      by_freshness_.erase(entries.back().by_freshness_iter);
      entries.pop_back();
      if (entries.empty()) by_key_.erase(cache_);
    }
    const typename ByKey::iterator by_key_iter = by_key_.find(key);
    if (ABSL_PREDICT_TRUE(by_key_iter != by_key_.end())) {
      // Return the newest entry with this key.
      Entries& entries = by_key_iter->second;
      RIEGELI_ASSERT(!entries.empty()) << "Failed invariant of RecyclingPool: "
                                          "empty by_key_ value";
      RIEGELI_ASSERT(entries.back().object != nullptr)
          << "Failed invariant of RecyclingPool: "
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

template <typename T, typename Deleter, typename Key>
void RecyclingPool<T, Deleter, Key>::Put(const Key& key,
                                         std::unique_ptr<T, Deleter> object) {
  std::unique_ptr<T, Deleter> evicted;
  absl::MutexLock lock(&mutex_);
  // Add a newest entry with this key.
  if (ABSL_PREDICT_TRUE(cache_ != by_key_.end())) {
    Entries& entries = cache_->second;
    RIEGELI_ASSERT(!entries.empty()) << "Failed invariant of RecyclingPool: "
                                        "empty by_key_ value";
    if (ABSL_PREDICT_TRUE(cache_->first == key)) {
      // `cache_` hit. Set the object pointer again.
      RIEGELI_ASSERT(entries.back().object == nullptr)
          << "Failed invariant of RecyclingPool: "
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
  if (ABSL_PREDICT_FALSE(by_freshness_.size() > max_size_)) {
    // Evict the oldest entry.
    const Key& evicted_key = by_freshness_.front();
    const typename ByKey::iterator by_key_iter = by_key_.find(evicted_key);
    RIEGELI_ASSERT(by_key_iter != by_key_.end())
        << "Failed invariant of RecyclingPool: "
           "a key from by_freshness_ absent in by_key_";
    Entries& entries = by_key_iter->second;
    RIEGELI_ASSERT(!entries.empty()) << "Failed invariant of RecyclingPool: "
                                        "empty by_key_ value";
    RIEGELI_ASSERT(entries.back().object != nullptr)
        << "Failed invariant of RecyclingPool: "
           "nullptr object not pointed to by cache_";
    evicted = std::move(entries.front().object);
    entries.pop_front();
    if (entries.empty()) by_key_.erase(by_key_iter);
    by_freshness_.pop_front();
  }
  cache_ = by_key_.end();
  // Destroy `evicted` after releasing `mutex_`.
}

template <typename T, typename Deleter>
inline void RecyclingPool<T, Deleter>::Recycler::operator()(T* ptr) const {
  RIEGELI_ASSERT(pool_ != nullptr)
      << "Failed precondition of RecyclingPool::Recycler: "
         "default-constructed recycler used with an object";
  pool_->Put(std::unique_ptr<T, Deleter>(ptr, original_deleter()));
}

template <typename T, typename Deleter>
RecyclingPool<T, Deleter>& RecyclingPool<T, Deleter>::global() {
  static NoDestructor<RecyclingPool> kStaticRecyclingPool;
  return *kStaticRecyclingPool;
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
  std::unique_ptr<T, Deleter> evicted;
  absl::MutexLock lock(&mutex_);
  // Add a newest entry.
  by_freshness_.push_back(std::move(object));
  if (ABSL_PREDICT_TRUE(by_freshness_.size() <= max_size_)) return;
  // Evict the oldest entry.
  evicted = std::move(by_freshness_.front());
  by_freshness_.pop_front();
  // Destroy `evicted` after releasing `mutex_`.
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_RECYCLING_POOL_H_
