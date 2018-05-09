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

#ifndef RIEGELI_BYTES_CHAIN_BACKWARD_WRITER_H_
#define RIEGELI_BYTES_CHAIN_BACKWARD_WRITER_H_

#include <stddef.h>
#include <limits>
#include <string>
#include <utility>

#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/backward_writer.h"

namespace riegeli {

// A BackwardWriter which prepends to a Chain.
class ChainBackwardWriter final : public BackwardWriter {
 public:
  class Options {
   public:
    Options() noexcept {}

    // Announce in advance the destination size. This may reduce Chain memory
    // usage.
    //
    // If the size hint turns out to not match reality, nothing breaks.
    Options& set_size_hint(Position size_hint) & {
      size_hint_ = size_hint;
      return *this;
    }
    Options&& set_size_hint(Position size_hint) && {
      return std::move(set_size_hint(size_hint));
    }

   private:
    friend class ChainBackwardWriter;

    Position size_hint_ = 0;
  };

  // Creates a closed ChainBackwardWriter.
  ChainBackwardWriter() noexcept : BackwardWriter(State::kClosed) {}

  // Will write to the Chain which is not owned by this ChainBackwardWriter and
  // must be kept alive but not accessed until closing the ChainBackwardWriter.
  explicit ChainBackwardWriter(Chain* dest, Options options = Options());

  ChainBackwardWriter(ChainBackwardWriter&& src) noexcept;
  ChainBackwardWriter& operator=(ChainBackwardWriter&& src) noexcept;

  bool SupportsTruncate() const override { return true; }
  bool Truncate(Position new_size) override;

 protected:
  void Done() override;
  bool PushSlow() override;
  bool WriteSlow(absl::string_view src) override;
  bool WriteSlow(std::string&& src) override;
  bool WriteSlow(const Chain& src) override;
  bool WriteSlow(Chain&& src) override;

 private:
  // Discards uninitialized space from the beginning of *dest_, so that it
  // contains only actual data written. Invalidates buffer pointers and
  // start_pos_.
  void DiscardBuffer();

  // Prepends some uninitialized space to *dest_ if this can be done without
  // allocation. Sets buffer pointers to the uninitialized space and restores
  // start_pos_.
  void MakeBuffer();

  // If healthy(), the Chain being written to, with uninitialized space
  // prepended (possibly empty); cursor_ points to the end of the uninitialized
  // space, except that it can be nullptr if the uninitialized space is empty.
  //
  // Invariant: if healthy() then dest_ != nullptr
  Chain* dest_ = nullptr;
  size_t size_hint_ = 0;

  // Invariants if healthy():
  //   limit_ == nullptr || limit_ == dest_->blocks().front().data()
  //   limit_pos() == dest_->size()
};

// Implementation details follow.

inline ChainBackwardWriter::ChainBackwardWriter(Chain* dest, Options options)
    : BackwardWriter(State::kOpen),
      dest_(RIEGELI_ASSERT_NOTNULL(dest)),
      size_hint_(
          UnsignedMin(options.size_hint_, std::numeric_limits<size_t>::max())) {
  start_pos_ = dest->size();
}

inline ChainBackwardWriter::ChainBackwardWriter(
    ChainBackwardWriter&& src) noexcept
    : BackwardWriter(std::move(src)),
      dest_(riegeli::exchange(src.dest_, nullptr)),
      size_hint_(riegeli::exchange(src.size_hint_, 0)) {}

inline ChainBackwardWriter& ChainBackwardWriter::operator=(
    ChainBackwardWriter&& src) noexcept {
  BackwardWriter::operator=(std::move(src));
  dest_ = riegeli::exchange(src.dest_, nullptr);
  size_hint_ = riegeli::exchange(src.size_hint_, 0);
  return *this;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_CHAIN_BACKWARD_WRITER_H_
