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
#include "absl/strings/cord.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Abstract class `PullableReader` helps to implement
// `Reader::PullSlow(min_length, recommended_length)` with `min_length > 1`.
//
// `PullableReader` accumulates pulled data in a scratch buffer if needed.
class PullableReader : public Reader {
 protected:
  // Helps to implement move constructor or move assignment if scratch is used.
  //
  // Moving the source should be in scope of a `BehindScratch` local variable,
  // unless source buffer pointers are known to remain unchanged during a move
  // or their change does not need to be reflected elsewhere.
  //
  // This temporarily reveals the relationship between the source and the buffer
  // pointers, in case it was hidden behind scratch usage. In a `BehindScratch`
  // scope, buffer pointers are set up as if scratch was not used, and the
  // current position might have been temporarily changed.
  class BehindScratch {
   public:
    explicit BehindScratch(PullableReader* context);

    BehindScratch(const BehindScratch&) = delete;
    BehindScratch& operator=(const BehindScratch&) = delete;

    ~BehindScratch();

   private:
    void Enter();
    void Leave();

    PullableReader* context_;
    size_t read_from_scratch_;
  };

  // Creates a `PullableReader` with the given initial state.
  explicit PullableReader(InitiallyClosed) noexcept
      : Reader(kInitiallyClosed) {}
  explicit PullableReader(InitiallyOpen) noexcept : Reader(kInitiallyOpen) {}

  PullableReader(PullableReader&& that) noexcept;
  PullableReader& operator=(PullableReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `PullableReader`. This
  // avoids constructing a temporary `PullableReader` and moving from it.
  // Derived classes which redefine `Reset()` should include a call to
  // `PullableReader::Reset()`.
  void Reset(InitiallyClosed);
  void Reset(InitiallyOpen);

  // Helps to implement `PullSlow(min_length, recommended_length)` if
  // `min_length > 1` in terms of `PullSlow(1, 0)`.
  //
  // Typical usage in `PullSlow()`:
  //   ```
  //   if (ABSL_PREDICT_FALSE(
  //       !PullUsingScratch(min_length, recommended_length))) {
  //     return available() >= min_length;
  //   }
  //   ```
  //
  // Return values:
  //  * `true`  - scratch is not used now, `min_length == 1`,
  //              the caller should continue `PullSlow(1, 0)`
  //  * `false` - `PullSlow()` is done,
  //              the caller should return `available() >= min_length`
  //
  // Precondition: `available() < min_length`
  bool PullUsingScratch(size_t min_length, size_t recommended_length);

  // Helps to implement `{Read,CopyTo}Slow(length, dest)` if scratch is used.
  //
  // Typical usage in `ReadSlow()`:
  //   ```
  //   if (ABSL_PREDICT_FALSE(!ReadScratch(length, dest))) return length == 0;
  //   ```
  //
  // Typical usage in `CopyToSlow()`:
  //   ```
  //   if (ABSL_PREDICT_FALSE(!CopyScratchTo(length, dest))) {
  //     return length == 0 && dest.healthy();
  //   }
  //   ```
  //
  // Return values:
  //  * `true`  - some data have been copied from scratch to `dest`,
  //              `length` was appropriately reduced, scratch is not used now,
  //              the caller should continue `{Read,CopyTo}Slow(length, dest)`
  //              no longer assuming that
  //              `UnsignedMin(available(), kMaxBytesToCopy) < length`
  //  * `false` - `{Read,CopyTo}Slow()` is done,
  //              the caller should return `length == 0` for `ReadSlow()`, or
  //              `length == 0 && dest.healthy()` for `CopyToSlow()`
  //
  // Preconditions for `ReadScratch()`:
  //   `UnsignedMin(available(), kMaxBytesToCopy) < length`
  //   `length <= std::numeric_limits<size_t>::max() - dest.size()`
  //
  // Precondition for `CopyScratchTo()`:
  //   `UnsignedMin(available(), kMaxBytesToCopy) < length`
  bool ReadScratch(size_t& length, Chain& dest);
  bool ReadScratch(size_t& length, absl::Cord& dest);
  bool CopyScratchTo(Position& length, Writer& dest);

  // Helps to implement `SeekSlow()` if scratch is used.
  //
  // Typical usage in `SeekSlow()`:
  //   ```
  //   if (ABSL_PREDICT_FALSE(!SeekUsingScratch(new_pos))) return true;
  //   ```
  //
  // Return values:
  //  * `true`  - scratch is not used now, the caller should continue
  //              `SeekSlow(new_pos)`
  //  * `false` - `SeekSlow()` is done, the caller should return `true`
  bool SeekUsingScratch(Position new_pos);

  // Helps to implement `Size()` if scratch is used.
  //
  // Returns what would be the value of `start_pos()` if scratch was not used,
  // i.e. if `SyncScratch()` was called now.
  Position start_pos_after_scratch() const;

 private:
  struct Scratch {
    ChainBlock buffer;
    const char* original_start = nullptr;
    size_t original_buffer_size = 0;
    size_t original_read_from_buffer = 0;
  };

  bool scratch_used() const;

  // Stops using scratch and returns `true` if all remaining data in scratch
  // come from a single fragment of the original source.
  bool ScratchEnds();

  void PullToScratchSlow(size_t min_length, size_t recommended_length);
  bool ReadScratchSlow(size_t& length, Chain& dest);
  bool ReadScratchSlow(size_t& length, absl::Cord& dest);
  bool CopyScratchToSlow(Position& length, Writer& dest);
  bool SeekUsingScratchSlow(Position new_pos);
  void SyncScratchSlow();

  std::unique_ptr<Scratch> scratch_;

  // Invariants if `scratch_used()`:
  //   `start() == scratch_->buffer.data()`
  //   `buffer_size() == scratch_->buffer.size()`
};

// Implementation details follow.

inline PullableReader::PullableReader(PullableReader&& that) noexcept
    : Reader(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      scratch_(std::move(that.scratch_)) {}

inline PullableReader& PullableReader::operator=(
    PullableReader&& that) noexcept {
  Reader::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
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

inline bool PullableReader::PullUsingScratch(size_t min_length,
                                             size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of PullableReader::PullUsingScratch(): "
         "enough data available, use Pull() instead";
  if (ABSL_PREDICT_FALSE(min_length > 1)) {
    PullToScratchSlow(min_length, recommended_length);
    return false;
  }
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    SyncScratchSlow();
    return available() == 0;
  }
  return true;
}

inline bool PullableReader::ReadScratch(size_t& length, Chain& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of PullableReader::ReadScratch(Chain&): "
         "enough data available, use Read(Chain&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of PullableReader::ReadScratch(Chain&): "
         "Chain size overflow";
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    return ReadScratchSlow(length, dest);
  }
  return true;
}

inline bool PullableReader::ReadScratch(size_t& length, absl::Cord& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of PullableReader::ReadScratch(Cord&): "
         "enough data available, use Read(Cord&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of PullableReader::ReadScratch(Cord&): "
         "Cord size overflow";
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    return ReadScratchSlow(length, dest);
  }
  return true;
}

inline bool PullableReader::CopyScratchTo(Position& length, Writer& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of PullableReader::CopyScratchTo(): "
         "enough data available, use CopyTo(Writer&) instead";
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    return CopyScratchToSlow(length, dest);
  }
  return true;
}

inline bool PullableReader::SeekUsingScratch(Position new_pos) {
  if (ABSL_PREDICT_FALSE(scratch_used())) return SeekUsingScratchSlow(new_pos);
  return true;
}

inline Position PullableReader::start_pos_after_scratch() const {
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    RIEGELI_ASSERT_GE(limit_pos(), scratch_->original_read_from_buffer)
        << "Failed invariant of Reader: negative position of buffer start";
    return limit_pos() - scratch_->original_read_from_buffer;
  }
  return start_pos();
}

inline PullableReader::BehindScratch::BehindScratch(PullableReader* context)
    : context_(context) {
  if (ABSL_PREDICT_FALSE(context_->scratch_used())) Enter();
}

inline PullableReader::BehindScratch::~BehindScratch() {
  if (ABSL_PREDICT_FALSE(context_->scratch_used())) Leave();
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_PULLABLE_READER_H_
