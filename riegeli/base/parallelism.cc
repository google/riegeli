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

#include "riegeli/base/parallelism.h"

#include <stddef.h>

#include <deque>
#include <functional>
#include <thread>
#include <utility>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "riegeli/base/base.h"
#include "riegeli/base/memory.h"

namespace riegeli {
namespace internal {

ThreadPool::~ThreadPool() {
  absl::MutexLock lock(&mutex_);
  exiting_ = true;
  mutex_.Await(absl::Condition(
      +[](size_t* num_threads) { return *num_threads == 0; }, &num_threads_));
}

void ThreadPool::Schedule(std::function<void()> task) {
  {
    absl::MutexLock lock(&mutex_);
    RIEGELI_ASSERT(!exiting_)
        << "Failed precondition of ThreadPool::Schedule(): no new threads may "
           "be scheduled while the thread pool is exiting";
    tasks_.push_back(std::move(task));
    if (num_idle_threads_ >= tasks_.size()) return;
    ++num_threads_;
  }
  std::thread([this] {
    for (;;) {
      absl::ReleasableMutexLock lock(&mutex_);
      ++num_idle_threads_;
      mutex_.AwaitWithTimeout(
          absl::Condition(
              +[](ThreadPool* self) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
                return !self->tasks_.empty() || self->exiting_;
              },
              this),
          absl::Seconds(1));
      --num_idle_threads_;
      if (tasks_.empty() || exiting_) {
        --num_threads_;
        return;
      }
      const std::function<void()> task = std::move(tasks_.front());
      tasks_.pop_front();
      lock.Release();
      task();
    }
  }).detach();
}

ThreadPool& ThreadPool::global() {
  static NoDestructor<ThreadPool> kStaticThreadPool;
  return *kStaticThreadPool;
}

}  // namespace internal
}  // namespace riegeli
