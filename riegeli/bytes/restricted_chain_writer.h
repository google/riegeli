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

#ifndef RIEGELI_BYTES_RESTRICTED_CHAIN_WRITER_H_
#define RIEGELI_BYTES_RESTRICTED_CHAIN_WRITER_H_

#include <stddef.h>

#include <utility>

#include "absl/base/attributes.h"
#include "absl/strings/cord.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// A restricted version of `ChainWriter` with minimal dependencies.
//
// In comparison to `ChainWriter`, `RestrictedChainWriter` always owns the
// destination `Chain` by value, and does not support appending, tuning block
// sizes, effective `SetWriteSizeHint()`, effective `Flush()`, `Seek()`,
// `Size()`, `Truncate()`, nor `ReadMode()`.
//
// It is intended to be used together with `WriterAbslStringifySink` which needs
// only writing.
class RestrictedChainWriter : public Writer {
 public:
  // Creates a closed `RestrictedChainWriter`.
  explicit RestrictedChainWriter(Closed) noexcept : Writer(kClosed) {}

  // Will append to an owned `Chain` which can be accessed by `dest()`.
  RestrictedChainWriter() = default;

  RestrictedChainWriter(RestrictedChainWriter&& that) noexcept;
  RestrictedChainWriter& operator=(RestrictedChainWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `RestrictedChainWriter`.
  // This avoids constructing a temporary `RestrictedChainWriter` and moving
  // from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset();

  // Returns the `Chain` being written to. Unchanged by `Close()`.
  Chain& dest() { return dest_; }
  const Chain& dest() const { return dest_; }

 protected:
  void Done() override;
  bool PushSlow(size_t min_length, size_t recommended_length) override;
  using Writer::WriteSlow;
  bool WriteSlow(const Chain& src) override;
  bool WriteSlow(Chain&& src) override;
  bool WriteSlow(const absl::Cord& src) override;
  bool WriteSlow(absl::Cord&& src) override;
  bool WriteZerosSlow(Position length) override;

 private:
  // Discards uninitialized space from the end of `dest_`, so that it contains
  // only actual data written.
  void SyncBuffer();

  // Appends uninitialized space to `dest_`.
  void MakeBuffer(size_t min_length = 1, size_t recommended_length = 0);

  // Moves `that.dest_` to `dest_`. Buffer pointers are already moved from
  // `dest_` to `*this`; adjust them to match `dest_`.
  void MoveDest(RestrictedChainWriter&& that);

  // Invariants if `ok()`:
  //   `limit() == nullptr || limit() == dest_.blocks().back().data() +
  //                                     dest_.blocks().back().size()`
  //   `limit_pos() == dest_.size()`
  Chain dest_;
};

// Implementation details follow.

inline RestrictedChainWriter::RestrictedChainWriter(
    RestrictedChainWriter&& that) noexcept
    : Writer(static_cast<Writer&&>(that)) {
  MoveDest(std::move(that));
}

inline RestrictedChainWriter& RestrictedChainWriter::operator=(
    RestrictedChainWriter&& that) noexcept {
  Writer::operator=(static_cast<Writer&&>(that));
  MoveDest(std::move(that));
  return *this;
}

inline void RestrictedChainWriter::Reset(Closed) {
  Writer::Reset(kClosed);
  dest_ = Chain();
}

inline void RestrictedChainWriter::Reset() {
  Writer::Reset();
  dest_.Clear();
}

inline void RestrictedChainWriter::MoveDest(RestrictedChainWriter&& that) {
  const size_t cursor_index = start_to_cursor();
  const size_t buffer_size = start_to_limit();
  dest_ = std::move(that.dest_);
  if (start() != nullptr) {
    set_buffer(const_cast<char*>(dest_.blocks().back().data() +
                                 dest_.blocks().back().size()) -
                   buffer_size,
               buffer_size, cursor_index);
  }
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_RESTRICTED_CHAIN_WRITER_H_
