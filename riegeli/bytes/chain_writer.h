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

#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/strings/cord.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/resetter.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Template parameter independent part of `ChainWriter`.
class ChainWriterBase : public Writer {
 public:
  class Options {
   public:
    Options() noexcept {}

    // If `true`, appends to existing contents of the destination.
    //
    // If `false`, the temporary behavior is to `CHECK` that the destination was
    // empty. This allows to make sure that all uses are properly migrated,
    // adding `set_append(true)` if appending to existing contents of the
    // destination is needed. Eventually the behavior will be: If `false`,
    // replaces existing contents of the destination, clearing it first.
    // And this will be the default.
    //
    // Default: `true` (temporarily).
    Options& set_append(bool append) & {
      append_ = append;
      return *this;
    }
    Options&& set_append(bool append) && {
      return std::move(set_append(append));
    }
    bool append() const { return append_; }

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
    Position size_hint() const { return size_hint_; }

    // Minimal size of a block of allocated data.
    //
    // This is used initially, while the destination is small.
    //
    // Default: `kMinBufferSize` (256)
    Options& set_min_block_size(size_t min_block_size) & {
      min_block_size_ = min_block_size;
      return *this;
    }
    Options&& set_min_block_size(size_t min_block_size) && {
      return std::move(set_min_block_size(min_block_size));
    }
    size_t min_block_size() const { return min_block_size_; }

    // Maximal size of a block of allocated data.
    //
    // This does not apply to attached external objects which can be arbitrarily
    // long.
    //
    // Default: `kMaxBufferSize` (64K)
    Options& set_max_block_size(size_t max_block_size) & {
      RIEGELI_ASSERT_GT(max_block_size, 0u)
          << "Failed precondition of "
             "ChainWriterBase::Options::set_max_block_size(): "
             "zero block size";
      max_block_size_ = max_block_size;
      return *this;
    }
    Options&& set_max_block_size(size_t max_block_size) && {
      return std::move(set_max_block_size(max_block_size));
    }
    size_t max_block_size() const { return max_block_size_; }

   private:
    bool append_ = true;
    Position size_hint_ = 0;
    size_t min_block_size_ = kMinBufferSize;
    size_t max_block_size_ = kMaxBufferSize;
  };

  // Returns the `Chain` being written to.
  virtual Chain* dest_chain() = 0;
  virtual const Chain* dest_chain() const = 0;

  bool Flush(FlushType flush_type) override;
  bool SupportsTruncate() const override { return true; }
  bool Truncate(Position new_size) override;

 protected:
  ChainWriterBase() noexcept : Writer(kInitiallyClosed) {}

  explicit ChainWriterBase(const Options& options);

  ChainWriterBase(ChainWriterBase&& that) noexcept;
  ChainWriterBase& operator=(ChainWriterBase&& that) noexcept;

  void Reset();
  void Reset(const Options& options);
  void Initialize(Chain* dest, bool append);

  void Done() override;
  bool PushSlow(size_t min_length, size_t recommended_length) override;
  using Writer::WriteSlow;
  bool WriteSlow(const Chain& src) override;
  bool WriteSlow(Chain&& src) override;
  bool WriteSlow(const absl::Cord& src) override;
  bool WriteSlow(absl::Cord&& src) override;
  bool WriteZerosSlow(Position length) override;

 private:
  // Discards uninitialized space from the end of `dest`, so that it contains
  // only actual data written.
  void SyncBuffer(Chain& dest);

  // Appends uninitialized space to `dest`.
  void MakeBuffer(Chain& dest, size_t min_length = 0,
                  size_t recommended_length = 0);

  Chain::Options options_;

  // Invariants if `healthy()`:
  //   `limit() == nullptr || limit() == dest_chain()->blocks().back().data() +
  //                                     dest_chain()->blocks().back().size()`
  //   `limit_pos() == dest_chain()->size()`
};

// A `Writer` which appends to a `Chain`.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the `Chain` being written to. `Dest` must support
// `Dependency<Chain*, Dest>`, e.g. `Chain*` (not owned, default),
// `Chain` (owned).
//
// The `Chain` must not be accessed until the `ChainWriter` is closed or no
// longer used, except that it is allowed to read the `Chain` immediately after
// `Flush()`.
template <typename Dest = Chain*>
class ChainWriter : public ChainWriterBase {
 public:
  // Creates a closed `ChainWriter`.
  ChainWriter() noexcept {}

  // Will append to the `Chain` provided by `dest`.
  explicit ChainWriter(const Dest& dest, Options options = Options());
  explicit ChainWriter(Dest&& dest, Options options = Options());

  // Will append to the `Chain` provided by a `Dest` constructed from elements
  // of `dest_args`. This avoids constructing a temporary `Dest` and moving from
  // it.
  template <typename... DestArgs>
  explicit ChainWriter(std::tuple<DestArgs...> dest_args,
                       Options options = Options());

  ChainWriter(ChainWriter&& that) noexcept;
  ChainWriter& operator=(ChainWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `ChainWriter`. This avoids
  // constructing a temporary `ChainWriter` and moving from it.
  void Reset();
  void Reset(const Dest& dest, Options options = Options());
  void Reset(Dest&& dest, Options options = Options());
  template <typename... DestArgs>
  void Reset(std::tuple<DestArgs...> dest_args, Options options = Options());

  // Returns the object providing and possibly owning the `Chain` being written
  // to. Unchanged by `Close()`.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  Chain* dest_chain() override { return dest_.get(); }
  const Chain* dest_chain() const override { return dest_.get(); }

 private:
  void MoveDest(ChainWriter&& that);

  // The object providing and possibly owning the `Chain` being written to, with
  // uninitialized space appended (possibly empty); `cursor()` points to the
  // uninitialized space, except that it can be `nullptr` if the uninitialized
  // space is empty.
  Dependency<Chain*, Dest> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
template <typename Dest>
ChainWriter(Dest&& dest,
            ChainWriterBase::Options options = ChainWriterBase::Options())
    -> ChainWriter<std::decay_t<Dest>>;
template <typename... DestArgs>
ChainWriter(std::tuple<DestArgs...> dest_args,
            ChainWriterBase::Options options = ChainWriterBase::Options())
    -> ChainWriter<void>;  // Delete.
#endif

// Implementation details follow.

inline ChainWriterBase::ChainWriterBase(const Options& options)
    : Writer(kInitiallyOpen),
      options_(
          Chain::Options()
              .set_size_hint(SaturatingIntCast<size_t>(options.size_hint()))
              .set_min_block_size(options.min_block_size())
              .set_max_block_size(options.max_block_size())) {}

inline ChainWriterBase::ChainWriterBase(ChainWriterBase&& that) noexcept
    : Writer(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      options_(that.options_) {}

inline ChainWriterBase& ChainWriterBase::operator=(
    ChainWriterBase&& that) noexcept {
  Writer::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  options_ = that.options_;
  return *this;
}

inline void ChainWriterBase::Reset() {
  Writer::Reset(kInitiallyClosed);
  options_ = Chain::Options();
}

inline void ChainWriterBase::Reset(const Options& options) {
  Writer::Reset(kInitiallyOpen);
  options_ = Chain::Options()
                 .set_size_hint(SaturatingIntCast<size_t>(options.size_hint()))
                 .set_min_block_size(options.min_block_size())
                 .set_max_block_size(options.max_block_size());
}

inline void ChainWriterBase::Initialize(Chain* dest, bool append) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of ChainWriter: null Chain pointer";
  if (append) {
    set_start_pos(dest->size());
  } else {
    RIEGELI_CHECK(dest->empty())
        << "Protection against a breaking change in riegeli::ChainWriter: "
           "destination is not empty but "
           "riegeli::ChainWriterBase::Options().set_append(true) is missing";
  }
  const absl::Span<char> buffer =
      dest->AppendBuffer(0, 0, Chain::kAnyLength, options_);
  set_buffer(buffer.data(), buffer.size());
}

template <typename Dest>
inline ChainWriter<Dest>::ChainWriter(const Dest& dest, Options options)
    : ChainWriterBase(options), dest_(dest) {
  Initialize(dest_.get(), options.append());
}

template <typename Dest>
inline ChainWriter<Dest>::ChainWriter(Dest&& dest, Options options)
    : ChainWriterBase(options), dest_(std::move(dest)) {
  Initialize(dest_.get(), options.append());
}

template <typename Dest>
template <typename... DestArgs>
inline ChainWriter<Dest>::ChainWriter(std::tuple<DestArgs...> dest_args,
                                      Options options)
    : ChainWriterBase(options), dest_(std::move(dest_args)) {
  Initialize(dest_.get(), options.append());
}

template <typename Dest>
inline ChainWriter<Dest>::ChainWriter(ChainWriter&& that) noexcept
    : ChainWriterBase(std::move(that)) {
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  MoveDest(std::move(that));
}

template <typename Dest>
inline ChainWriter<Dest>& ChainWriter<Dest>::operator=(
    ChainWriter&& that) noexcept {
  ChainWriterBase::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
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
  ChainWriterBase::Reset(options);
  dest_.Reset(dest);
  Initialize(dest_.get(), options.append());
}

template <typename Dest>
inline void ChainWriter<Dest>::Reset(Dest&& dest, Options options) {
  ChainWriterBase::Reset(options);
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), options.append());
}

template <typename Dest>
template <typename... DestArgs>
inline void ChainWriter<Dest>::Reset(std::tuple<DestArgs...> dest_args,
                                     Options options) {
  ChainWriterBase::Reset(options);
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get(), options.append());
}

template <typename Dest>
inline void ChainWriter<Dest>::MoveDest(ChainWriter&& that) {
  if (dest_.kIsStable()) {
    dest_ = std::move(that.dest_);
  } else {
    const size_t cursor_index = written_to_buffer();
    dest_ = std::move(that.dest_);
    if (start() != nullptr) {
      const size_t buffer_size = dest_->size() - IntCast<size_t>(start_pos());
      set_buffer(const_cast<char*>(dest_->blocks().back().data() +
                                   dest_->blocks().back().size()) -
                     buffer_size,
                 buffer_size, cursor_index);
    }
  }
}

template <typename Dest>
struct Resetter<ChainWriter<Dest>> : ResetterByReset<ChainWriter<Dest>> {};

}  // namespace riegeli

#endif  // RIEGELI_BYTES_CHAIN_WRITER_H_
