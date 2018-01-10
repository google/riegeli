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

#include <functional>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <utility>

#include "riegeli/base/assert.h"
#include "riegeli/base/memory.h"

namespace riegeli {
namespace internal {

ThreadPool::~ThreadPool() {
  std::unique_lock<std::mutex> lock(mutex_);
  exiting_ = true;
  has_work_.notify_all();
  while (num_threads_ > 0) workers_exited_.wait(lock);
}

void ThreadPool::Schedule(std::function<void()> task) {
  {
    std::unique_lock<std::mutex> lock(mutex_);
    RIEGELI_ASSERT(!exiting_);
    tasks_.push_back(std::move(task));
    if (num_idle_threads_ >= tasks_.size()) {
      has_work_.notify_one();
      return;
    }
    ++num_threads_;
  }
  std::thread([this] {
    for (;;) {
      std::unique_lock<std::mutex> lock(mutex_);
      ++num_idle_threads_;
      while (tasks_.empty() && !exiting_) {
        if (has_work_.wait_for(lock, std::chrono::seconds(60)) ==
            std::cv_status::timeout) {
          break;
        }
      }
      --num_idle_threads_;
      if (tasks_.empty() || exiting_) {
        if (--num_threads_ == 0) workers_exited_.notify_all();
        return;
      }
      const std::function<void()> task = std::move(tasks_.front());
      tasks_.pop_front();
      lock.unlock();
      task();
    }
  })
      .detach();
}

ThreadPool& DefaultThreadPool() {
  static NoDestructor<ThreadPool> kStaticThreadPool;
  return *kStaticThreadPool;
}

}  // namespace internal
}  // namespace riegeli
