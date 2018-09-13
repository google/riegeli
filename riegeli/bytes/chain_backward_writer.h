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
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/backward_writer.h"

namespace riegeli {

// Template parameter invariant part of ChainBackwardWriter.
class ChainBackwardWriterBase : public BackwardWriter {
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
    template <typename Dest>
    friend class ChainBackwardWriter;

    Position size_hint_ = 0;
  };

  // Returns the Chain being written to.
  virtual Chain* dest_chain() = 0;
  virtual const Chain* dest_chain() const = 0;

  bool SupportsTruncate() const override { return true; }
  bool Truncate(Position new_size) override;

 protected:
  ChainBackwardWriterBase() noexcept : BackwardWriter(State::kClosed) {}

  explicit ChainBackwardWriterBase(Position size_hint)
      : BackwardWriter(State::kOpen),
        size_hint_(UnsignedMin(size_hint, std::numeric_limits<size_t>::max())) {
  }

  ChainBackwardWriterBase(ChainBackwardWriterBase&& that) noexcept;
  ChainBackwardWriterBase& operator=(ChainBackwardWriterBase&& that) noexcept;

  void Done() override;
  bool PushSlow() override;
  bool WriteSlow(absl::string_view src) override;
  bool WriteSlow(std::string&& src) override;
  bool WriteSlow(const Chain& src) override;
  bool WriteSlow(Chain&& src) override;

 private:
  // Discards uninitialized space from the beginning of *dest, so that it
  // contains only actual data written. Invalidates buffer pointers and
  // start_pos_.
  void DiscardBuffer(Chain* dest);

  // Prepends some uninitialized space to *dest if this can be done without
  // allocation. Sets buffer pointers to the uninitialized space and restores
  // start_pos_.
  void MakeBuffer(Chain* dest);

  size_t size_hint_ = 0;
};

// A BackwardWriter which prepends to a Chain.
//
// The Dest template parameter specifies the type of the object providing and
// possibly owning the Chain being written to. Dest must support
// Dependency<Chain*, Src>, e.g. Chain* (not owned, default), Chain (owned).
//
// The Chain must not be accessed until the ChainBackwardWriter is closed or no
// longer used.
template <typename Dest = Chain*>
class ChainBackwardWriter : public ChainBackwardWriterBase {
 public:
  // Creates a closed ChainBackwardWriter.
  ChainBackwardWriter() noexcept {}

  // Will prepend to the Chain provided by dest.
  explicit ChainBackwardWriter(Dest dest, Options options = Options());

  ChainBackwardWriter(ChainBackwardWriter&& that) noexcept;
  ChainBackwardWriter& operator=(ChainBackwardWriter&& that) noexcept;

  // Returns the object providing and possibly owning the Chain being written
  // to.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  Chain* dest_chain() override { return dest_.ptr(); }
  const Chain* dest_chain() const override { return dest_.ptr(); }

 private:
  void MoveDest(ChainBackwardWriter&& that);

  // The object providing and possibly owning the Chain being written to, with
  // uninitialized space prepended (possibly empty); cursor_ points to the end
  // of the uninitialized space, except that it can be nullptr if the
  // uninitialized space is empty.
  Dependency<Chain*, Dest> dest_;

  // Invariants if healthy():
  //   limit_ == nullptr || limit_ == dest_->blocks().front().data()
  //   limit_pos() == dest_->size()
};

// Implementation details follow.

inline ChainBackwardWriterBase::ChainBackwardWriterBase(
    ChainBackwardWriterBase&& that) noexcept
    : BackwardWriter(std::move(that)),
      size_hint_(riegeli::exchange(that.size_hint_, 0)) {}

inline ChainBackwardWriterBase& ChainBackwardWriterBase::operator=(
    ChainBackwardWriterBase&& that) noexcept {
  BackwardWriter::operator=(std::move(that));
  size_hint_ = riegeli::exchange(that.size_hint_, 0);
  return *this;
}

template <typename Dest>
inline ChainBackwardWriter<Dest>::ChainBackwardWriter(Dest dest,
                                                      Options options)
    : ChainBackwardWriterBase(options.size_hint_), dest_(std::move(dest)) {
  RIEGELI_ASSERT(dest_.ptr() != nullptr)
      << "Failed precondition of "
         "ChainBackwardWriter<Dest>::ChainBackwardWriter(Dest): "
         "null Chain pointer";
  start_pos_ = dest_->size();
}

template <typename Dest>
ChainBackwardWriter<Dest>::ChainBackwardWriter(
    ChainBackwardWriter&& that) noexcept
    : ChainBackwardWriterBase(std::move(that)) {
  MoveDest(std::move(that));
}

template <typename Dest>
ChainBackwardWriter<Dest>& ChainBackwardWriter<Dest>::operator=(
    ChainBackwardWriter&& that) noexcept {
  ChainBackwardWriterBase::operator=(std::move(that));
  MoveDest(std::move(that));
  return *this;
}

template <typename Dest>
void ChainBackwardWriter<Dest>::MoveDest(ChainBackwardWriter&& that) {
  if (dest_.kIsStable()) {
    dest_ = std::move(that.dest_);
  } else {
    const size_t cursor_index = written_to_buffer();
    dest_ = std::move(that.dest_);
    if (start_ != nullptr) {
      limit_ = const_cast<char*>(dest_->blocks().front().data());
      start_ = limit_ + (dest_->size() - IntCast<size_t>(start_pos_));
      cursor_ = start_ - cursor_index;
    }
  }
}

extern template class ChainBackwardWriter<Chain*>;
extern template class ChainBackwardWriter<Chain>;

}  // namespace riegeli

#endif  // RIEGELI_BYTES_CHAIN_BACKWARD_WRITER_H_
