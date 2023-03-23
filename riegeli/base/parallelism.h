// Copyright 2017 Google LLC
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

#ifndef RIEGELI_BASE_PARALLELISM_H_
#define RIEGELI_BASE_PARALLELISM_H_

#include <stddef.h>

#include <deque>

#include "absl/base/attributes.h"
#include "absl/base/thread_annotations.h"
#include "absl/functional/any_invocable.h"
#include "absl/synchronization/mutex.h"

namespace riegeli {
namespace internal {

// A thread pool with lazily created worker threads, without a thread count
// limit. Worker threads exit after being idle for one minute.
class ThreadPool {
 public:
  ThreadPool() {}

  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

  ~ThreadPool();

  static ThreadPool& global();

  void Schedule(absl::AnyInvocable<void() &&> task);

 private:
  absl::Mutex mutex_;
  bool exiting_ ABSL_GUARDED_BY(mutex_) = false;
  size_t num_threads_ ABSL_GUARDED_BY(mutex_) = 0;
  size_t num_idle_threads_ ABSL_GUARDED_BY(mutex_) = 0;
  std::deque<absl::AnyInvocable<void() &&>> tasks_ ABSL_GUARDED_BY(mutex_);
};

}  // namespace internal
}  // namespace riegeli

#endif  // RIEGELI_BASE_PARALLELISM_H_
