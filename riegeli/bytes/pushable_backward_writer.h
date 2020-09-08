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

#ifndef RIEGELI_BYTES_PUSHABLE_BACKWARD_WRITER_H_
#define RIEGELI_BYTES_PUSHABLE_BACKWARD_WRITER_H_

#include <stddef.h>

#include <memory>
#include <utility>

#include "absl/base/optimization.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/backward_writer.h"

namespace riegeli {

// Abstract class `PushableBackwardWriter` helps to implement
// `Writer::PushSlow(min_length, recommended_length)` with `min_length > 1`.
//
// `PushableBackwardWriter` accumulates data to be pushed in a scratch buffer if
// needed.
class PushableBackwardWriter : public BackwardWriter {
 protected:
  // Helps to implement move constructor or move assignment if scratch is used.
  //
  // Moving the destination should be in scope of a `BehindScratch` local
  // variable, unless destination buffer pointers are known to remain unchanged
  // during a move or their change does not need to be reflected elsewhere.
  //
  // This temporarily reveals the relationship between the destination and the
  // buffer pointers, in case it was hidden behind scratch usage. In a
  // `BehindScratch` scope, buffer pointers are set up as if scratch was not
  // used, and the current position might have been temporarily changed.
  class BehindScratch {
   public:
    explicit BehindScratch(PushableBackwardWriter* context);

    BehindScratch(const BehindScratch&) = delete;
    BehindScratch& operator=(const BehindScratch&) = delete;

    ~BehindScratch();

   private:
    void Enter();
    void Leave();

    PushableBackwardWriter* context_;
    size_t written_to_scratch_;
  };

  // Creates a `PushableBackwardWriter` with the given initial state.
  explicit PushableBackwardWriter(InitiallyClosed) noexcept
      : BackwardWriter(kInitiallyClosed) {}
  explicit PushableBackwardWriter(InitiallyOpen) noexcept
      : BackwardWriter(kInitiallyOpen) {}

  PushableBackwardWriter(PushableBackwardWriter&& that) noexcept;
  PushableBackwardWriter& operator=(PushableBackwardWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `PushableBackwardWriter`.
  // This avoids constructing a temporary `PushableBackwardWriter` and moving
  // from it. Derived classes which override `Reset()` should include a call to
  // `PushableBackwardWriter::Reset()`.
  void Reset(InitiallyClosed);
  void Reset(InitiallyOpen);

  // Helps to implement `PushSlow(min_length, recommended_length)`
  // if `min_length > 1` in terms of `PushSlow(1, 0)`,
  // `WriteSlow(absl::string_view)`, and `WriteSlow(Chain&&)`.
  //
  // Typical usage in `PushSlow()`:
  //   ```
  //   if (ABSL_PREDICT_FALSE(
  //           !PushUsingScratch(min_length, recommended_length))) {
  //     return available() >= min_length;
  //   }
  //   ```
  //
  // Return values:
  //  * `true`  - scratch is not used now, `min_length == 1`,
  //              the caller should continue `PushSlow(1, 0)`
  //  * `false` - `PushSlow()` is done,
  //              the caller should return `available() >= min_length`
  //
  // Precondition: `available() < min_length`
  bool PushUsingScratch(size_t min_length, size_t recommended_length);

  // Helps to implement `Done()`, `WriteSlow()`, `WriteZerosSlow()`, `Flush()`,
  // or `Truncate()` if scratch is used, in terms of
  // `WriteSlow(absl::string_view)` and `WriteSlow(Chain&&)`.
  //
  // Typical usage in `Done()`:
  //   ```
  //   if (ABSL_PREDICT_TRUE(healthy())) {
  //     if (ABSL_PREDICT_TRUE(SyncScratch())) {
  //       ...
  //     }
  //   }
  //   PushableBackwardWriter::Done();
  //   ```
  //
  // Typical usage in functions other than `Done()`:
  //   ```
  //   if (ABSL_PREDICT_FALSE(!SyncScratch())) return false;
  //   ```
  //
  // Return values:
  //  * `true`  - scratch is not used now, the caller should continue
  //  * `false` - failure (`!healthy()`), the caller should return `false`
  //              (or skip the rest of flushing in `Done()`)
  bool SyncScratch();

 private:
  struct Scratch {
    ChainBlock buffer;
    char* original_limit = nullptr;
    size_t original_buffer_size = 0;
    size_t original_written_to_buffer = 0;
  };

  bool scratch_used() const;

  void PushFromScratchSlow(size_t min_length, size_t recommended_length);
  bool SyncScratchSlow();

  std::unique_ptr<Scratch> scratch_;

  // Invariants if `scratch_used()`:
  //   `limit() == scratch_->buffer.data()`
  //   `buffer_size() == scratch_->buffer.data()`
};

// Implementation details follow.

inline PushableBackwardWriter::PushableBackwardWriter(
    PushableBackwardWriter&& that) noexcept
    : BackwardWriter(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      scratch_(std::move(that.scratch_)) {}

inline PushableBackwardWriter& PushableBackwardWriter::operator=(
    PushableBackwardWriter&& that) noexcept {
  BackwardWriter::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  scratch_ = std::move(that.scratch_);
  return *this;
}

inline void PushableBackwardWriter::Reset(InitiallyClosed) {
  BackwardWriter::Reset(kInitiallyClosed);
  if (ABSL_PREDICT_FALSE(scratch_used())) scratch_->buffer.Clear();
}

inline void PushableBackwardWriter::Reset(InitiallyOpen) {
  BackwardWriter::Reset(kInitiallyOpen);
  if (ABSL_PREDICT_FALSE(scratch_used())) scratch_->buffer.Clear();
}

inline bool PushableBackwardWriter::scratch_used() const {
  return scratch_ != nullptr && !scratch_->buffer.empty();
}

inline bool PushableBackwardWriter::PushUsingScratch(
    size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of PushableBackwardWriter::PushUsingScratch(): "
         "enough space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(min_length > 1)) {
    PushFromScratchSlow(min_length, recommended_length);
    return false;
  }
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    SyncScratchSlow();
    return available() == 0;
  }
  return true;
}

inline bool PushableBackwardWriter::SyncScratch() {
  if (ABSL_PREDICT_FALSE(scratch_used())) return SyncScratchSlow();
  return true;
}

inline PushableBackwardWriter::BehindScratch::BehindScratch(
    PushableBackwardWriter* context)
    : context_(context) {
  if (ABSL_PREDICT_FALSE(context_->scratch_used())) Enter();
}

inline PushableBackwardWriter::BehindScratch::~BehindScratch() {
  if (ABSL_PREDICT_FALSE(context_->scratch_used())) Leave();
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_PUSHABLE_BACKWARD_WRITER_H_
