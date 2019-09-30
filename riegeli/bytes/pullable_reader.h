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

#ifndef RIEGELI_BYTES_PULLABLE_READER_H_
#define RIEGELI_BYTES_PULLABLE_READER_H_

#include <stddef.h>

#include <limits>
#include <memory>
#include <utility>

#include "absl/base/optimization.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// `PullableReader` helps to implement `Reader::PullSlow()` with
// `min_length > 1` in terms of `Reader::PullSlow()` with `min_length == 1`.
//
// `PullableReader` accumulates pulled data in a scratch buffer if needed.
class PullableReader : public Reader {
 protected:
  // Creates a `PullableReader` with the given initial state.
  explicit PullableReader(InitiallyClosed) noexcept
      : Reader(kInitiallyClosed) {}
  explicit PullableReader(InitiallyOpen) noexcept : Reader(kInitiallyOpen) {}

  PullableReader(PullableReader&& that) noexcept;
  PullableReader& operator=(PullableReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `PullableReader`. This
  // avoids constructing a temporary `PullableReader` and moving from it.
  // Derived classes which override `Reset()` should include a call to
  // `PullableReader::Reset()`.
  void Reset(InitiallyClosed);
  void Reset(InitiallyOpen);

  // Helps to implement `PullSlow(min_length, recommended_length)` if
  // `min_length > 1`.
  //
  // Return values:
  //  * `true`  - scratch is not used now, `min_length == 1`,
  //              the caller should continue `PullSlow(1, 0)`
  //  * `false` - `PullSlow()` is done,
  //              the caller should return `available() >= min_length`
  //
  // Preconditions:
  //   `healthy()`
  //   `min_length > available()`
  bool PullUsingScratch(size_t min_length);

  // Helps to implement `{Read,CopyTo}Slow(dest, *length)` if scratch is used.
  //
  // Return values:
  //  * `true`  - some data have been copied from scratch to `*dest`,
  //              `*length` was appropriately reduced, scratch is not used now,
  //              the caller should continue `{Read,CopyTo}Slow(dest, *length)`
  //              no longer assuming that
  //              `*length > UnsignedMin(available(), kMaxBytesToCopy)`
  //  * `false` - `{Read,CopyTo}Slow()` is done,
  //              the caller should return `*length == 0` for `ReadSlow()`, or
  //              `*length == 0 && dest->healthy()` for `CopyToSlow()`
  //
  // Preconditions for `ReadScratch()`:
  //   `healthy()`
  //   `*length > UnsignedMin(available(), kMaxBytesToCopy)`
  //   `*length <= std::numeric_limits<size_t>::max() - dest->size()`
  //
  // Preconditions for `CopyScratchTo()`:
  //   `healthy()`
  //   `*length > UnsignedMin(available(), kMaxBytesToCopy)`
  bool ReadScratch(Chain* dest, size_t* length);
  bool CopyScratchTo(Writer* dest, Position* length);

  // Helps to implement `SeekSlow()` if scratch is used.
  //
  // When `SyncScratch()` returns, scratch is not used but the current position
  // might have been changed.
  void SyncScratch();

  // Helps to implement move constructor or move assignment if scratch is used.
  //
  // Moving the source should be surrounded by `SwapScratchBegin()` and
  // `SwapScratchEnd()`.
  //
  // When `SwapScratchBegin()` returns, scratch is not used but the current
  // position might have been changed.
  void SwapScratchBegin();
  void SwapScratchEnd();

  // Helps to implement `Size()` if scratch is used.
  //
  // Returns what would be the value of `start_pos()` if scratch was not used,
  // i.e. if `SyncScratch()` was called now.
  Position start_pos_after_scratch() const;

 private:
  struct Scratch {
    ChainBlock buffer;
    const char* original_start = nullptr;
    const char* original_cursor = nullptr;
    const char* original_limit = nullptr;
  };

  bool scratch_used() const;

  // Stops using scratch and returns `true` if all remaining data in scratch
  // come from a single fragment of the original source, and at least
  // `min_length` bytes are available there.
  bool ScratchEnds(size_t min_length);

  void PullToScratchSlow(size_t min_length);
  bool ReadScratchSlow(Chain* dest, size_t* length);
  bool CopyScratchToSlow(Writer* dest, Position* length);
  void SyncScratchSlow();
  void SwapScratchBeginSlow();
  void SwapScratchEndSlow();

  std::unique_ptr<Scratch> scratch_;

  // Invariant if `scratch_used()`: `start_ == scratch_->buffer.data()`
};

// Implementation details follow.

inline PullableReader::PullableReader(PullableReader&& that) noexcept
    : Reader(std::move(that)), scratch_(std::move(that.scratch_)) {}

inline PullableReader& PullableReader::operator=(
    PullableReader&& that) noexcept {
  Reader::operator=(std::move(that));
  scratch_ = std::move(that.scratch_);
  return *this;
}

inline void PullableReader::Reset(InitiallyClosed) {
  Reader::Reset(kInitiallyClosed);
  if (ABSL_PREDICT_FALSE(scratch_used())) scratch_->buffer.Clear();
}

inline void PullableReader::Reset(InitiallyOpen) {
  Reader::Reset(kInitiallyOpen);
  if (ABSL_PREDICT_FALSE(scratch_used())) scratch_->buffer.Clear();
}

inline bool PullableReader::scratch_used() const {
  return scratch_ != nullptr && !scratch_->buffer.empty();
}

inline bool PullableReader::PullUsingScratch(size_t min_length) {
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of PullableReader::PullUsingScratch(): "
      << status();
  RIEGELI_ASSERT_GT(min_length, available())
      << "Failed precondition of PullableReader::PullUsingScratch(): "
         "length too small, use Pull() instead";
  if (ABSL_PREDICT_FALSE(min_length > 1)) {
    PullToScratchSlow(min_length);
    return false;
  }
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    SyncScratchSlow();
    return available() == 0;
  }
  return true;
}

inline bool PullableReader::ReadScratch(Chain* dest, size_t* length) {
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of PullableReader::ReadScratch(Chain*): "
      << status();
  RIEGELI_ASSERT_GT(*length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of PullableReader::ReadScratch(Chain*): "
         "length too small, use Read(Chain*) instead";
  RIEGELI_ASSERT_LE(*length, std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of PullableReader::ReadScratch(Chain*): "
         "Chain size overflow";
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    return ReadScratchSlow(dest, length);
  }
  return true;
}

inline bool PullableReader::CopyScratchTo(Writer* dest, Position* length) {
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of PullableReader::CopyScratchTo(): " << status();
  RIEGELI_ASSERT_GT(*length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of PullableReader::CopyToInScratchSlow(): "
         "length too small, use CopyTo(Writer*) instead";
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    return CopyScratchToSlow(dest, length);
  }
  return true;
}

inline void PullableReader::SyncScratch() {
  if (ABSL_PREDICT_FALSE(scratch_used())) SyncScratchSlow();
}

inline void PullableReader::SwapScratchBegin() {
  if (ABSL_PREDICT_FALSE(scratch_used())) SwapScratchBeginSlow();
}

inline void PullableReader::SwapScratchEnd() {
  if (ABSL_PREDICT_FALSE(scratch_used())) SwapScratchEndSlow();
}

inline Position PullableReader::start_pos_after_scratch() const {
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    const size_t last_scratch_fragment =
        PtrDistance(scratch_->original_start, scratch_->original_cursor);
    RIEGELI_ASSERT_GE(limit_pos_, last_scratch_fragment)
        << "Failed invariant of Reader: negative position of buffer start";
    return limit_pos_ - last_scratch_fragment;
  }
  return start_pos();
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_PULLABLE_READER_H_
