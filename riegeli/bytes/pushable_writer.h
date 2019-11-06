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

#ifndef RIEGELI_BYTES_PUSHABLE_WRITER_H_
#define RIEGELI_BYTES_PUSHABLE_WRITER_H_

#include <stddef.h>

#include <memory>
#include <utility>

#include "absl/base/optimization.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Abstract class `PushableWriter` helps to implement
// `Writer::PushSlow(min_length, recommended_length)` with `min_length > 1`.
//
// `PushableWriter` accumulates data to be pushed in a scratch buffer if needed.
class PushableWriter : public Writer {
 protected:
  // Creates a `PushableWriter` with the given initial state.
  explicit PushableWriter(InitiallyClosed) noexcept
      : Writer(kInitiallyClosed) {}
  explicit PushableWriter(InitiallyOpen) noexcept : Writer(kInitiallyOpen) {}

  PushableWriter(PushableWriter&& that) noexcept;
  PushableWriter& operator=(PushableWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `PushableWriter`. This
  // avoids constructing a temporary `PushableWriter` and moving from it.
  // Derived classes which override `Reset()` should include a call to
  // `PushableWriter::Reset()`.
  void Reset(InitiallyClosed);
  void Reset(InitiallyOpen);

  // Helps to implement `PushSlow(min_length, recommended_length)` if
  // `min_length > 1` in terms of `Write(absl::string_view)` and `Write(Chain)`.
  //
  // Return values:
  //  * `true`  - scratch is not used now, `min_length == 1`,
  //              the caller should continue `PushSlow(1, 0)`
  //  * `false` - `PushSlow()` is done,
  //              the caller should return `available() >= min_length`
  //
  // Precondition: `min_length > available()`
  bool PushUsingScratch(size_t min_length);

  // Helps to implement `Done()`, `WriteSlow()`, `Flush()`, or `SeekSlow()` if
  // scratch is used, in terms of `Write(absl::string_view)` and `Write(Chain)`.
  //
  // Return values:
  //  * `true`  - scratch is not used now, the caller should continue
  //  * `false` - failure (`!healthy()`), the caller should return `false`
  //              (or skip the rest of flushing in `Done()`)
  bool SyncScratch();

  // Helps to implement move constructor or move assignment if scratch is used.
  //
  // Moving the source should be surrounded by `SwapScratchBegin()` and
  // `SwapScratchEnd()`.
  //
  // When `SwapScratchBegin()` returns, scratch is not used but the current
  // position might have been changed.
  void SwapScratchBegin();
  void SwapScratchEnd();

 protected:
  void Done() override;

 private:
  struct Scratch {
    ChainBlock buffer;
    char* original_start = nullptr;
    char* original_cursor = nullptr;
    char* original_limit = nullptr;
  };

  bool scratch_used() const;

  void PushFromScratchSlow(size_t min_length);
  bool SyncScratchSlow();
  void SwapScratchBeginSlow();
  void SwapScratchEndSlow();

  std::unique_ptr<Scratch> scratch_;

  // Invariant if `scratch_used()`: `start_ == scratch_->buffer.data()`
};

// Implementation details follow.

inline PushableWriter::PushableWriter(PushableWriter&& that) noexcept
    : Writer(std::move(that)), scratch_(std::move(that.scratch_)) {}

inline PushableWriter& PushableWriter::operator=(
    PushableWriter&& that) noexcept {
  Writer::operator=(std::move(that));
  scratch_ = std::move(that.scratch_);
  return *this;
}

inline void PushableWriter::Reset(InitiallyClosed) {
  Writer::Reset(kInitiallyClosed);
  if (ABSL_PREDICT_FALSE(scratch_used())) scratch_->buffer.Clear();
}

inline void PushableWriter::Reset(InitiallyOpen) {
  Writer::Reset(kInitiallyOpen);
  if (ABSL_PREDICT_FALSE(scratch_used())) scratch_->buffer.Clear();
}

inline bool PushableWriter::scratch_used() const {
  return scratch_ != nullptr && !scratch_->buffer.empty();
}

inline bool PushableWriter::PushUsingScratch(size_t min_length) {
  RIEGELI_ASSERT_GT(min_length, available())
      << "Failed precondition of PushableWriter::PushUsingScratch(): "
         "length too small, use Push() instead";
  if (ABSL_PREDICT_FALSE(min_length > 1)) {
    PushFromScratchSlow(min_length);
    return false;
  }
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    SyncScratchSlow();
    return available() == 0;
  }
  return true;
}

inline bool PushableWriter::SyncScratch() {
  if (ABSL_PREDICT_FALSE(scratch_used())) return SyncScratchSlow();
  return true;
}

inline void PushableWriter::SwapScratchBegin() {
  if (ABSL_PREDICT_FALSE(scratch_used())) SwapScratchBeginSlow();
}

inline void PushableWriter::SwapScratchEnd() {
  if (ABSL_PREDICT_FALSE(scratch_used())) SwapScratchEndSlow();
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_PUSHABLE_WRITER_H_
