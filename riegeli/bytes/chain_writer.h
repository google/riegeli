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

#ifndef RIEGELI_BYTES_CHAIN_WRITER_H_
#define RIEGELI_BYTES_CHAIN_WRITER_H_

#include <stddef.h>
#include <limits>
#include <string>
#include <utility>

#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// A Writer which appends to a Chain.
class ChainWriter : public Writer {
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
    friend class ChainWriter;

    Position size_hint_ = 0;
  };

  // Creates a closed ChainWriter.
  ChainWriter() noexcept : Writer(State::kClosed) {}

  // Will write to a Chain which is owned by this ChainWriter, available as
  // dest().
  explicit ChainWriter(OwnsDest, Options options = Options());

  // Will write to the Chain which is not owned by this ChainWriter and must be
  // kept alive but not accessed until closing the ChainWriter, except that it
  // is allowed to read it directly after Flush().
  explicit ChainWriter(Chain* dest, Options options = Options());

  ChainWriter(ChainWriter&& src) noexcept;
  ChainWriter& operator=(ChainWriter&& src) noexcept;

  // Returns the Chain being written to. Unchanged by Close().
  Chain& dest() const { return *dest_; }

  bool Flush(FlushType flush_type) override;
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
  // Discards uninitialized space from the end of *dest_, so that it contains
  // only actual data written. Invalidates buffer pointers and start_pos_.
  void DiscardBuffer();

  // Appends some uninitialized space to *dest_ if this can be done without
  // allocation. Sets buffer pointers to the uninitialized space and restores
  // start_pos_.
  void MakeBuffer();

  Chain owned_dest_;
  // The Chain being written to, with uninitialized space appended (possibly
  // empty); cursor_ points to the uninitialized space, except that it can be
  // nullptr if the uninitialized space is empty.
  //
  // Invariant: dest_ != nullptr
  Chain* dest_ = &owned_dest_;
  size_t size_hint_ = 0;

  // Invariants if healthy():
  //   limit_ == nullptr || limit_ == dest_->blocks().back().data() +
  //                                  dest_->blocks().back().size()
  //   limit_pos() == dest_->size()
};

// Implementation details follow.

inline ChainWriter::ChainWriter(OwnsDest, Options options)
    : ChainWriter(&owned_dest_, options) {}

inline ChainWriter::ChainWriter(Chain* dest, Options options)
    : Writer(State::kOpen),
      dest_(RIEGELI_ASSERT_NOTNULL(dest)),
      size_hint_(
          UnsignedMin(options.size_hint_, std::numeric_limits<size_t>::max())) {
  start_pos_ = dest->size();
}

inline ChainWriter::ChainWriter(ChainWriter&& src) noexcept
    : Writer(std::move(src)),
      owned_dest_(std::move(src.owned_dest_)),
      dest_(src.dest_ == &src.owned_dest_
                ? &owned_dest_
                : riegeli::exchange(src.dest_, &src.owned_dest_)),
      size_hint_(riegeli::exchange(src.size_hint_, 0)) {
  if (dest_ == &owned_dest_ && start_ != nullptr) {
    // *dest_ was moved, which invalidated buffer pointers.
    const size_t cursor_index = written_to_buffer();
    limit_ = const_cast<char*>(owned_dest_.blocks().back().data() +
                               owned_dest_.blocks().back().size());
    start_ = limit_ - (owned_dest_.size() - IntCast<size_t>(start_pos_));
    cursor_ = start_ + cursor_index;
  }
}

inline ChainWriter& ChainWriter::operator=(ChainWriter&& src) noexcept {
  Writer::operator=(std::move(src));
  owned_dest_ = std::move(src.owned_dest_);
  dest_ = src.dest_ == &src.owned_dest_
              ? &owned_dest_
              : riegeli::exchange(src.dest_, &src.owned_dest_);
  size_hint_ = riegeli::exchange(src.size_hint_, 0);
  if (dest_ == &owned_dest_ && start_ != nullptr) {
    // *dest_ was moved, which invalidated buffer pointers.
    const size_t cursor_index = written_to_buffer();
    limit_ = const_cast<char*>(owned_dest_.blocks().back().data() +
                               owned_dest_.blocks().back().size());
    start_ = limit_ - (owned_dest_.size() - IntCast<size_t>(start_pos_));
    cursor_ = start_ + cursor_index;
  }
  return *this;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_CHAIN_WRITER_H_
