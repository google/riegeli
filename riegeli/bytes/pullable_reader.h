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

#include <memory>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/functional/function_ref.h"
#include "absl/strings/cord.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/base/sized_shared_buffer.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

class BackwardWriter;
class Writer;

// Abstract class `PullableReader` helps to implement
// `Reader::PullSlow(min_length, recommended_length)` with `min_length > 1`.
//
// `PullableReader` accumulates pulled data in a scratch buffer if needed.
class PullableReader : public Reader {
 protected:
  class BehindScratch;

  using Reader::Reader;

  PullableReader(PullableReader&& that) noexcept;
  PullableReader& operator=(PullableReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `PullableReader`. This
  // avoids constructing a temporary `PullableReader` and moving from it.
  // Derived classes which redefine `Reset()` should include a call to
  // `PullableReader::Reset()`.
  void Reset(Closed);
  void Reset();

  void Done() override;

  // Returns `true` if scratch is used, which means that buffer pointers are
  // temporarily unrelated to the source. This is exposed for assertions.
  bool scratch_used() const;

  // `PullableReader::{Done,SyncImpl}()` seek the source back to the current
  // position if scratch is used but not all data from scratch were read.
  // This is feasible only if `SupportsRandomAccess()`.
  //
  // Warning: if `!SupportsRandomAccess()`, the source will have an
  // unpredictable amount of extra data consumed because of buffering.
  //
  // For propagating `{Close,Sync}()` to dependencies, `{Done,SyncImpl}()`
  // should be overridden to call `PullableReader::{Done,SyncImpl}()` and then
  // close/sync the dependencies.

  // Implementation of `Done()`, called while scratch is not used. This is
  // called before buffer pointers are reset.
  //
  // If scratch was used but not all data from scratch were read and
  // `!SupportsRandomAccess()`, seeking back is not feasible and
  // `DoneBehindScratch()` is not called.
  //
  // By default calls `SyncBehindScratch(SyncType::kFromObject)`, which by
  // default does nothing.
  //
  // Precondition: `!scratch_used()`
  virtual void DoneBehindScratch();

  // Implementation of `PullSlow(1, recommended_length)`, called while scratch
  // is not used.
  //
  // Preconditions:
  //   `available() == 0`
  //   `!scratch_used()`
  virtual bool PullBehindScratch(size_t recommended_length) = 0;

  // Implementation of `ReadSlow()`, `CopySlow()`, `ReadOrPullSomeSlow()`,
  // `ReadHintSlow()`, `SyncImpl()`, and `SeekSlow()`, called while scratch is
  // not used.
  //
  // Regarding `SyncBehindScratch()`, if scratch was used but not all data from
  // scratch were read and `!SupportsRandomAccess()`, seeking back is not
  // feasible and `SyncBehindScratch()` is not called.
  //
  // By default they are implemented analogously to the corresponding `Reader`
  // functions.
  //
  // Preconditions:
  //   like the corresponding `Reader` functions
  //   `!scratch_used()`
  virtual bool ReadBehindScratch(size_t length, char* dest);
  virtual bool ReadBehindScratch(size_t length, Chain& dest);
  virtual bool ReadBehindScratch(size_t length, absl::Cord& dest);
  virtual bool CopyBehindScratch(Position length, Writer& dest);
  virtual bool CopyBehindScratch(size_t length, BackwardWriter& dest);
  virtual bool ReadOrPullSomeBehindScratch(
      size_t max_length, absl::FunctionRef<char*(size_t&)> get_dest);
  virtual void ReadHintBehindScratch(size_t min_length,
                                     size_t recommended_length);
  virtual bool SyncBehindScratch(SyncType sync_type);
  virtual bool SeekBehindScratch(Position new_pos);

  bool PullSlow(size_t min_length, size_t recommended_length) override;
  using Reader::ReadSlow;
  bool ReadSlow(size_t length, char* dest) override;
  bool ReadSlow(size_t length, Chain& dest) override;
  bool ReadSlow(size_t length, absl::Cord& dest) override;
  using Reader::CopySlow;
  bool CopySlow(Position length, Writer& dest) override;
  bool CopySlow(size_t length, BackwardWriter& dest) override;
  using Reader::ReadOrPullSomeSlow;
  bool ReadOrPullSomeSlow(size_t max_length,
                          absl::FunctionRef<char*(size_t&)> get_dest) override;
  void ReadHintSlow(size_t min_length, size_t recommended_length) override;
  bool SyncImpl(SyncType sync_type) override;
  bool SeekSlow(Position new_pos) override;

 private:
  struct Scratch {
    SizedSharedBuffer buffer;
    const char* original_start = nullptr;
    size_t original_start_to_limit = 0;
    size_t original_start_to_cursor = 0;
  };

  void SyncScratch();
  void ClearScratch();

  // Stops using scratch and returns `true` if all remaining data in scratch
  // come from a single fragment of the original source.
  bool ScratchEnds();

  std::unique_ptr<Scratch> scratch_;

  // Invariants if `scratch_used()`:
  //   `start() == scratch_->buffer.data()`
  //   `start_to_limit() == scratch_->buffer.size()`
};

// Helps to implement move constructor or move assignment if scratch is used.
//
// Moving the source should be in scope of a `BehindScratch` object, unless
// source buffer pointers are known to remain unchanged during a move or their
// change does not need to be reflected elsewhere.
//
// This temporarily reveals the relationship between the source and the buffer
// pointers, in case it was hidden behind scratch usage. In a `BehindScratch`
// scope, scratch is not used, and buffer pointers may be changed. The current
// position reflects what has been read from the source and must not be changed.
class PullableReader::BehindScratch {
 public:
  explicit BehindScratch(PullableReader* context);

  BehindScratch(BehindScratch&& that) = default;
  BehindScratch& operator=(BehindScratch&&) = delete;

  ~BehindScratch();

 private:
  void Enter();
  void Leave();

  PullableReader* context_;
  std::unique_ptr<Scratch> scratch_;
  size_t read_from_scratch_;
};

// Implementation details follow.

inline PullableReader::PullableReader(PullableReader&& that) noexcept
    : Reader(static_cast<Reader&&>(that)), scratch_(std::move(that.scratch_)) {}

inline PullableReader& PullableReader::operator=(
    PullableReader&& that) noexcept {
  Reader::operator=(static_cast<Reader&&>(that));
  scratch_ = std::move(that.scratch_);
  return *this;
}

inline void PullableReader::Reset(Closed) {
  Reader::Reset(kClosed);
  scratch_.reset();
}

inline void PullableReader::Reset() {
  Reader::Reset();
  if (ABSL_PREDICT_FALSE(scratch_used())) scratch_->buffer.Clear();
}

inline bool PullableReader::scratch_used() const {
  return scratch_ != nullptr && !scratch_->buffer.empty();
}

inline PullableReader::BehindScratch::BehindScratch(PullableReader* context)
    : context_(context) {
  if (ABSL_PREDICT_FALSE(context_->scratch_used())) Enter();
}

inline PullableReader::BehindScratch::~BehindScratch() {
  if (ABSL_PREDICT_FALSE(scratch_ != nullptr)) Leave();
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_PULLABLE_READER_H_
