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

#ifndef RIEGELI_BASE_BACKGROUND_CLEANING_H_
#define RIEGELI_BASE_BACKGROUND_CLEANING_H_

#include <list>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "riegeli/base/no_destructor.h"

namespace riegeli {

// An interface of objects which need background cleaning.
class BackgroundCleanee {
 public:
  virtual ~BackgroundCleanee();

 protected:
  friend class BackgroundCleaner;  // For `Clean()`.

  // Called from a background thread when a scheduled cleaning time arrived.
  //
  // `now` is the current time, passed as a parameter so that there is no need
  // to call `BackgroundCleaner::TimeNow()` again if that time is needed to
  // decide what to clean.
  virtual void Clean(absl::Time now) = 0;
};

// Manages objects which need background cleaning, scheduling cleaning calls
// from a background thread.
class BackgroundCleaner {
 private:
  struct Entry {
    explicit Entry(BackgroundCleanee* cleanee, absl::Time deadline)
        : cleanee(cleanee), deadline(deadline) {}

    BackgroundCleanee* cleanee;
    absl::Time deadline;
  };
  using Entries = std::list<Entry>;

 public:
  // Registration token of an object which needs background cleaning.
  class Token {
   public:
    Token() = default;

    Token(const Token& that) = default;
    Token& operator=(const Token& that) = default;

   private:
    friend class BackgroundCleaner;  // For `Token()` and `iter()`.

    explicit Token(Entries::iterator iter) : iter_(iter) {}

    Entries::iterator iter() const { return iter_; }

    Entries::iterator iter_{};
  };

  BackgroundCleaner() = default;

  BackgroundCleaner(const BackgroundCleaner&) = delete;
  BackgroundCleaner& operator=(const BackgroundCleaner&) = delete;

  // Precondition: all registered cleanees have been unregistered.
  ~BackgroundCleaner();

  // Returns a default global `BackgroundCleaner`.
  static BackgroundCleaner& global() {
    static NoDestructor<BackgroundCleaner> kStaticBackgroundCleaner;
    return *kStaticBackgroundCleaner;
  }

  // Registers the cleanee, allowing `ScheduleCleaning()` calls.
  //
  // Thread safe.
  Token Register(BackgroundCleanee* cleanee);

  // Unregisters the cleanee corresponding to `token`, invalidating `token` and
  // cancelling any pending cleaning.
  //
  // This might block if the cleanee is being cleaned or will be cleaned soon,
  // so this must not be called under a mutex needed for cleaning.
  //
  // Thread safe.
  void Unregister(Token token);

  // Cancels any pending cleaning corresponding to `token`. Does not unregister
  // the cleanee.
  //
  // This might block if the cleanee is being cleaned or will be cleaned soon,
  // so this must not be called under a mutex needed for cleaning.
  //
  // Thread safe.
  void CancelCleaning(Token token);

  // Schedules cleaning the cleanee corresponding to `token` at `deadline`.
  //
  // If `deadline` is `absl::InfiniteFuture()`, cleaning will never happen.
  // If `deadline` is in the past, cleaning will be scheduled immediately.
  //
  // If `ScheduleCleaning()` is called again for the same cleanee with a pending
  // cleaning, its deadline can be reduced, but extending the deadline has no
  // effect.
  //
  // Thread safe.
  void ScheduleCleaning(Token token, absl::Time deadline);

  // Returns the current time according to the same clock that
  // `BackgroundCleaner` is using.
  //
  // Thread safe.
  absl::Time TimeNow();

 private:
  void CancelCleaningInternal(Token token)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void ScheduleCleaningSlow(Token token, absl::Time deadline);

  void BackgroundThread();

  absl::Mutex mutex_;
  // Registered cleanees, partitioned so that `entries_` before `next_` do not
  // have pending cleaning and have `deadline == absl::InfiniteFuture()`, while
  // `entries_` at and after `next_` have pending cleaning and are sorted by
  // their `deadline` which is never `absl::InfiniteFuture()`.
  Entries entries_ ABSL_GUARDED_BY(mutex_);
  Entries::iterator next_ ABSL_GUARDED_BY(mutex_) = entries_.begin();
  // If not `nullptr`, this cleanee is currently being cleaned. This is used to
  // avoid a race between `Unregister()` and cleaning.
  BackgroundCleanee* current_cleanee_ ABSL_GUARDED_BY(mutex_) = nullptr;
  // If `true`, the next deadline might have been reduced since the background
  // thread started waiting for it. This wakes up the thread and lets it recheck
  // the next deadline.
  //
  // This is also used to request the thread to exit when `next_ == next_end()`.
  bool deadline_reduced_ ABSL_GUARDED_BY(mutex_) = false;
  // If `false`, the background thread is active. This is negated for easier
  // `absl::Condition()`.
  bool no_background_thread_ ABSL_GUARDED_BY(mutex_) = true;
};

// Implementation details follow.

inline void BackgroundCleaner::ScheduleCleaning(Token token,
                                                absl::Time deadline) {
  if (deadline == absl::InfiniteFuture()) return;
  ScheduleCleaningSlow(token, deadline);
}

inline absl::Time BackgroundCleaner::TimeNow() { return absl::Now(); }

}  // namespace riegeli

#endif  // RIEGELI_BASE_BACKGROUND_CLEANING_H_
