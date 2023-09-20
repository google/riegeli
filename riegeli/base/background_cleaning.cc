// Copyright 2023 Google LLC
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

#include "riegeli/base/background_cleaning.h"

#include <list>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/parallelism.h"

namespace riegeli {

BackgroundCleanee::~BackgroundCleanee() = default;  // Key method.

BackgroundCleaner::Token BackgroundCleaner::Register(
    BackgroundCleanee* cleanee) {
  absl::MutexLock lock(&mutex_);
  entries_.emplace_front(cleanee, absl::InfinitePast());
  return Token(entries_.begin());
}

void BackgroundCleaner::Unregister(Token token) {
  absl::MutexLock lock(&mutex_);
  CancelCleaningInternal(token);
  if (next_ == token.iter()) ++next_;
  entries_.erase(token.iter());
}

void BackgroundCleaner::CancelCleaning(Token token) {
  absl::MutexLock lock(&mutex_);
  CancelCleaningInternal(token);
  if (next_ == token.iter()) {
    ++next_;
  } else {
    entries_.splice(entries_.begin(), entries_, token.iter());
  }
}

// Waits until this cleanee is not being cleaned.
inline void BackgroundCleaner::CancelCleaningInternal(Token token) {
  struct Args {
    BackgroundCleanee** current_cleanee;
    BackgroundCleanee* cleanee_to_unregister;
  };
  Args args{&current_cleanee_, token.iter()->cleanee};
  mutex_.Await(absl::Condition(
      +[](Args* args) {
        return *args->current_cleanee != args->cleanee_to_unregister;
      },
      &args));
}

void BackgroundCleaner::ScheduleCleaningSlow(Token token, absl::Time deadline) {
  absl::MutexLock lock(&mutex_);

  // Update `entries_` by moving `token.iter()` into the right place, and update
  // `next_`.
  Entries::iterator iter = entries_.end();
  for (;;) {
    if (iter == next_) {
      RIEGELI_ASSERT(iter != token.iter())
          << "token.iter() staying at its current place "
             "must have been handled by the previous iteration";
      // Insert `token.iter()` before `iter` which is `next_`.
      next_ = token.iter();
      deadline_reduced_ = true;
      break;
    }
  skip:
    const Entries::iterator last_iter = iter;
    --iter;
    if (iter == token.iter()) {
      if (iter == next_) {
        // `token.iter()` stays at its current place which is `next_`.
        if (deadline < token.iter()->deadline) deadline_reduced_ = true;
        goto done;
      }
      // Skip the old place of `token.iter()`.
      goto skip;
    }
    if (deadline >= iter->deadline) {
      // Insert `token.iter()` after `iter`, i.e. before `last_iter`.
      // This might be its old place, then `splice()` does nothing.
      iter = last_iter;
      if (token.iter() == next_) ++next_;
      break;
    }
  }
  entries_.splice(iter, entries_, token.iter());
done:
  RIEGELI_ASSERT(next_ != entries_.end())
      << "next_ must cover at least token.iter()";
  token.iter()->deadline = deadline;

  // Start a background thread if needed.
  if (!no_background_thread_) return;
  no_background_thread_ = false;
  internal::ThreadPool::global().Schedule([this] {
    absl::MutexLock lock(&mutex_);
    BackgroundThread();
    no_background_thread_ = true;
  });
}

inline void BackgroundCleaner::BackgroundThread()
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
  if (next_ == entries_.end()) return;
  for (;;) {
    // Wait until the next deadline.
    do {
      deadline_reduced_ = false;
      mutex_.AwaitWithDeadline(absl::Condition(&deadline_reduced_),
                               next_->deadline);
      if (next_ == entries_.end()) return;
    } while (deadline_reduced_);
    // Schedule cleaning.
    for (;;) {
      const absl::Time now = TimeNow();
      if (next_->deadline > now) break;
      BackgroundCleanee* const cleanee = next_->cleanee;
      ++next_;
      current_cleanee_ = cleanee;
      mutex_.Unlock();
      cleanee->Clean(now);
      mutex_.Lock();
      current_cleanee_ = nullptr;
      if (next_ == entries_.end()) return;
    }
  }
}

BackgroundCleaner::~BackgroundCleaner() {
  RIEGELI_CHECK(entries_.empty())
      << "Failed precondition of BackgroundCleaner::~BackgroundCleaner(): "
         "some cleanees remain registered";
  absl::MutexLock lock(&mutex_);
  // Request the background thread to exit.
  deadline_reduced_ = true;
  mutex_.Await(absl::Condition(&no_background_thread_));
}

}  // namespace riegeli
