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
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

class Reader;

// Abstract class `PushableWriter` helps to implement
// `Writer::PushSlow(min_length, recommended_length)` with `min_length > 1`.
//
// `PushableWriter` accumulates data to be pushed in a scratch buffer if needed.
class PushableWriter : public Writer {
 protected:
  class BehindScratch;

  using Writer::Writer;

  PushableWriter(PushableWriter&& that) noexcept;
  PushableWriter& operator=(PushableWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `PushableWriter`. This
  // avoids constructing a temporary `PushableWriter` and moving from it.
  // Derived classes which redefine `Reset()` should include a call to
  // `PushableWriter::Reset()`.
  void Reset(Closed);
  void Reset();

  void Done() override;
  void OnFail() override;

  // Returns `true` if scratch is used, which means that buffer pointers are
  // temporarily unrelated to the destination. This is exposed for assertions.
  bool scratch_used() const;

  // `PushableWriter::{Done,FlushImpl}()` write the scratch if needed, and call
  // `{Done,Flush}BehindScratch()` to close/flush the destination before buffer
  // pointers are reset.
  //
  // For propagating `{Close,Flush}()` to dependencies, `{Done,FlushImpl}()`
  // should be overridden to call `PushableWriter::{Done,FlushImpl}()` and then
  // close/flush the dependencies.

  // Implementation of `Done()`, called while scratch is not used, and only if
  // writing the scratch succeeded. This is called before buffer pointers are
  // reset.
  //
  // By default calls `FlushBehindScratch(FlushType::kFromObject)`, which by
  // default does nothing.
  //
  // Precondition: `!scratch_used()`
  virtual void DoneBehindScratch();

  // Implementation of `PushSlow(1, recommended_length)`, called while scratch
  // is not used.
  //
  // Preconditions:
  //   `available() == 0`
  //   `!scratch_used()`
  virtual bool PushBehindScratch(size_t recommended_length) = 0;

  // Force using scratch as the buffer.
  //
  // This can be used in the implementation of `PushBehindScratch()` if in some
  // circumstances scratch should be used even when `min_length == 1`.
  //
  // These circumstances should be rare, otherwise performance would be poor
  // because `Push()` would call `PushSlow()` too often.
  //
  // Preconditions:
  //   `available() == 0`
  //   `!scratch_used()`
  //
  // Always returns `true`.
  //
  // Postconditions:
  //   `available() > 0`
  //   `scratch_used()`
  bool ForcePushUsingScratch();

  // Implementation of `WriteSlow()`, `WriteZerosSlow()`, `FlushImpl()`,
  // `SeekSlow()`, `SizeImpl()`, `TruncateImpl()`, and `ReadModeImpl()`,
  // called while scratch is not used.
  //
  // By default they are implemented analogously to the corresponding `Writer`
  // functions.
  //
  // Preconditions:
  //   like the corresponding `Writer` functions
  //   `!scratch_used()`
  virtual bool WriteBehindScratch(absl::string_view src);
  virtual bool WriteBehindScratch(const Chain& src);
  virtual bool WriteBehindScratch(Chain&& src);
  virtual bool WriteBehindScratch(const absl::Cord& src);
  virtual bool WriteBehindScratch(absl::Cord&& src);
  virtual bool WriteZerosBehindScratch(Position length);
  virtual bool FlushBehindScratch(FlushType flush_type);
  virtual bool SeekBehindScratch(Position new_pos);
  virtual absl::optional<Position> SizeBehindScratch();
  virtual bool TruncateBehindScratch(Position new_size);
  virtual Reader* ReadModeBehindScratch(Position initial_pos);

  bool PushSlow(size_t min_length, size_t recommended_length) override;
  bool WriteSlow(absl::string_view src) override;
  bool WriteSlow(const Chain& src) override;
  bool WriteSlow(Chain&& src) override;
  bool WriteSlow(const absl::Cord& src) override;
  bool WriteSlow(absl::Cord&& src) override;
  bool WriteZerosSlow(Position length) override;
  bool FlushImpl(FlushType flush_type) override;
  bool SeekSlow(Position new_pos) override;
  absl::optional<Position> SizeImpl() override;
  bool TruncateImpl(Position new_size) override;
  Reader* ReadModeImpl(Position initial_pos) override;

 private:
  struct Scratch {
    ChainBlock buffer;
    char* original_start = nullptr;
    size_t original_start_to_limit = 0;
    size_t original_start_to_cursor = 0;
  };

  bool SyncScratch();

  std::unique_ptr<Scratch> scratch_;

  // Invariants if `scratch_used()`:
  //   `start() == scratch_->buffer.data()`
  //   `start_to_limit() == scratch_->buffer.size()`
};

// Helps to implement move constructor or move assignment if scratch is used.
//
// Moving the destination should be in scope of a `BehindScratch` local
// variable, unless destination buffer pointers are known to remain unchanged
// during a move or their change does not need to be reflected elsewhere.
//
// This temporarily reveals the relationship between the destination and the
// buffer pointers, in case it was hidden behind scratch usage. In a
// `BehindScratch` scope, scratch is not used, and buffer pointers may be
// changed. The current position reflects what has been written to the
// destination and must not be changed.
class PushableWriter::BehindScratch {
 public:
  explicit BehindScratch(PushableWriter* context);

  BehindScratch(const BehindScratch&) = delete;
  BehindScratch& operator=(const BehindScratch&) = delete;

  ~BehindScratch();

 private:
  void Enter();
  void Leave();

  PushableWriter* context_;
  std::unique_ptr<Scratch> scratch_;
  size_t written_to_scratch_;
};

// Implementation details follow.

inline PushableWriter::PushableWriter(PushableWriter&& that) noexcept
    : Writer(static_cast<Writer&&>(that)), scratch_(std::move(that.scratch_)) {}

inline PushableWriter& PushableWriter::operator=(
    PushableWriter&& that) noexcept {
  Writer::operator=(static_cast<Writer&&>(that));
  scratch_ = std::move(that.scratch_);
  return *this;
}

inline void PushableWriter::Reset(Closed) {
  Writer::Reset(kClosed);
  scratch_.reset();
}

inline void PushableWriter::Reset() {
  Writer::Reset();
  if (ABSL_PREDICT_FALSE(scratch_used())) scratch_->buffer.Clear();
}

inline bool PushableWriter::scratch_used() const {
  return scratch_ != nullptr && !scratch_->buffer.empty();
}

inline PushableWriter::BehindScratch::BehindScratch(PushableWriter* context)
    : context_(context) {
  if (ABSL_PREDICT_FALSE(context_->scratch_used())) Enter();
}

inline PushableWriter::BehindScratch::~BehindScratch() {
  if (ABSL_PREDICT_FALSE(scratch_ != nullptr)) Leave();
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_PUSHABLE_WRITER_H_
