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
#include <tuple>
#include <utility>

#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/resetter.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Template parameter invariant part of ChainWriter.
class ChainWriterBase : public Writer {
 public:
  class Options {
   public:
    Options() noexcept {}

    // Expected final size, or 0 if unknown. This may improve performance and
    // memory usage.
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
    friend class ChainWriter;

    Position size_hint_ = 0;
  };

  // Returns the Chain being written to.
  virtual Chain* dest_chain() = 0;
  virtual const Chain* dest_chain() const = 0;

  bool Flush(FlushType flush_type) override;
  bool SupportsTruncate() const override { return true; }
  bool Truncate(Position new_size) override;

 protected:
  ChainWriterBase() noexcept : Writer(kInitiallyClosed) {}

  explicit ChainWriterBase(Position size_hint);

  ChainWriterBase(ChainWriterBase&& that) noexcept;
  ChainWriterBase& operator=(ChainWriterBase&& that) noexcept;

  void Reset();
  void Reset(Position size_hint);
  void Initialize(Chain* dest);

  void Done() override;
  bool PushSlow(size_t min_length, size_t recommended_length) override;
  using Writer::WriteSlow;
  bool WriteSlow(const Chain& src) override;
  bool WriteSlow(Chain&& src) override;

 private:
  // Discards uninitialized space from the end of *dest, so that it contains
  // only actual data written.
  void SyncBuffer(Chain* dest);

  // Appends uninitialized space to *dest.
  void MakeBuffer(Chain* dest, size_t min_length = 0,
                  size_t recommended_length = 0);

  size_t size_hint_ = 0;

  // Invariants if healthy():
  //   limit_ == nullptr || limit_ == dest_chain()->blocks().back().data() +
  //                                  dest_chain()->blocks().back().size()
  //   limit_pos() == dest_chain()->size()
};

// A Writer which appends to a Chain.
//
// The Dest template parameter specifies the type of the object providing and
// possibly owning the Chain being written to. Dest must support
// Dependency<Chain*, Dest>, e.g. Chain* (not owned, default), Chain (owned).
//
// The Chain must not be accessed until the ChainWriter is closed or no longer
// used, except that it is allowed to read the Chain immediately after Flush().
template <typename Dest = Chain*>
class ChainWriter : public ChainWriterBase {
 public:
  // Creates a closed ChainWriter.
  ChainWriter() noexcept {}

  // Will append to the Chain provided by dest.
  explicit ChainWriter(const Dest& dest, Options options = Options());
  explicit ChainWriter(Dest&& dest, Options options = Options());

  // Will append to the Chain provided by a Dest constructed from elements of
  // dest_args. This avoids constructing a temporary Dest and moving from it.
  template <typename... DestArgs>
  explicit ChainWriter(std::tuple<DestArgs...> dest_args,
                       Options options = Options());

  ChainWriter(ChainWriter&& that) noexcept;
  ChainWriter& operator=(ChainWriter&& that) noexcept;

  // Makes *this equivalent to a newly constructed ChainWriter. This avoids
  // constructing a temporary ChainWriter and moving from it.
  void Reset();
  void Reset(const Dest& dest, Options options = Options());
  void Reset(Dest&& dest, Options options = Options());
  template <typename... DestArgs>
  void Reset(std::tuple<DestArgs...> dest_args, Options options = Options());

  // Returns the object providing and possibly owning the Chain being written
  // to. Unchanged by Close().
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  Chain* dest_chain() override { return dest_.get(); }
  const Chain* dest_chain() const override { return dest_.get(); }

 private:
  void MoveDest(ChainWriter&& that);

  // The object providing and possibly owning the Chain being written to, with
  // uninitialized space appended (possibly empty); cursor_ points to the
  // uninitialized space, except that it can be nullptr if the uninitialized
  // space is empty.
  Dependency<Chain*, Dest> dest_;
};

// Implementation details follow.

inline ChainWriterBase::ChainWriterBase(Position size_hint)
    : Writer(kInitiallyOpen),
      size_hint_(UnsignedMin(size_hint, std::numeric_limits<size_t>::max())) {}

inline ChainWriterBase::ChainWriterBase(ChainWriterBase&& that) noexcept
    : Writer(std::move(that)), size_hint_(that.size_hint_) {}

inline ChainWriterBase& ChainWriterBase::operator=(
    ChainWriterBase&& that) noexcept {
  Writer::operator=(std::move(that));
  size_hint_ = that.size_hint_;
  return *this;
}

inline void ChainWriterBase::Reset() {
  Writer::Reset(kInitiallyClosed);
  size_hint_ = 0;
}

inline void ChainWriterBase::Reset(Position size_hint) {
  Writer::Reset(kInitiallyOpen);
  size_hint_ = UnsignedMin(size_hint, std::numeric_limits<size_t>::max());
}

inline void ChainWriterBase::Initialize(Chain* dest) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of ChainWriter: null Chain pointer";
  start_pos_ = dest->size();
}

template <typename Dest>
inline ChainWriter<Dest>::ChainWriter(const Dest& dest, Options options)
    : ChainWriterBase(options.size_hint_), dest_(dest) {
  Initialize(dest_.get());
}

template <typename Dest>
inline ChainWriter<Dest>::ChainWriter(Dest&& dest, Options options)
    : ChainWriterBase(options.size_hint_), dest_(std::move(dest)) {
  Initialize(dest_.get());
}

template <typename Dest>
template <typename... DestArgs>
inline ChainWriter<Dest>::ChainWriter(std::tuple<DestArgs...> dest_args,
                                      Options options)
    : ChainWriterBase(options.size_hint_), dest_(std::move(dest_args)) {
  Initialize(dest_.get());
}

template <typename Dest>
inline ChainWriter<Dest>::ChainWriter(ChainWriter&& that) noexcept
    : ChainWriterBase(std::move(that)) {
  MoveDest(std::move(that));
}

template <typename Dest>
inline ChainWriter<Dest>& ChainWriter<Dest>::operator=(
    ChainWriter&& that) noexcept {
  ChainWriterBase::operator=(std::move(that));
  MoveDest(std::move(that));
  return *this;
}

template <typename Dest>
inline void ChainWriter<Dest>::Reset() {
  ChainWriterBase::Reset();
  dest_.Reset();
}

template <typename Dest>
inline void ChainWriter<Dest>::Reset(const Dest& dest, Options options) {
  ChainWriterBase::Reset(options.size_hint_);
  dest_.Reset(dest);
  Initialize(dest_.get());
}

template <typename Dest>
inline void ChainWriter<Dest>::Reset(Dest&& dest, Options options) {
  ChainWriterBase::Reset(options.size_hint_);
  dest_.Reset(std::move(dest));
  Initialize(dest_.get());
}

template <typename Dest>
template <typename... DestArgs>
inline void ChainWriter<Dest>::Reset(std::tuple<DestArgs...> dest_args,
                                     Options options) {
  ChainWriterBase::Reset(options.size_hint_);
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get());
}

template <typename Dest>
inline void ChainWriter<Dest>::MoveDest(ChainWriter&& that) {
  if (dest_.kIsStable()) {
    dest_ = std::move(that.dest_);
  } else {
    const size_t cursor_index = written_to_buffer();
    dest_ = std::move(that.dest_);
    if (start_ != nullptr) {
      limit_ = const_cast<char*>(dest_->blocks().back().data() +
                                 dest_->blocks().back().size());
      start_ = limit_ - (dest_->size() - IntCast<size_t>(start_pos_));
      cursor_ = start_ + cursor_index;
    }
  }
}

template <typename Dest>
struct Resetter<ChainWriter<Dest>> : ResetterByReset<ChainWriter<Dest>> {};

}  // namespace riegeli

#endif  // RIEGELI_BYTES_CHAIN_WRITER_H_
