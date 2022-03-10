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

#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/strings/cord.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/backward_writer.h"

namespace riegeli {

// Template parameter independent part of `ChainBackwardWriter`.
class ChainBackwardWriterBase : public BackwardWriter {
 public:
  class Options {
   public:
    Options() noexcept {}

    // If `false`, replaces existing contents of the destination, clearing it
    // first.
    //
    // If `true`, prepends to existing contents of the destination.
    //
    // Default: `false`.
    Options& set_prepend(bool prepend) & {
      prepend_ = prepend;
      return *this;
    }
    Options&& set_prepend(bool prepend) && {
      return std::move(set_prepend(prepend));
    }
    bool prepend() const { return prepend_; }

    // Expected final size, or `absl::nullopt` if unknown. This may improve
    // performance and memory usage.
    //
    // If the size hint turns out to not match reality, nothing breaks.
    //
    // Default: `absl::nullopt`.
    Options& set_size_hint(absl::optional<Position> size_hint) & {
      size_hint_ = size_hint;
      return *this;
    }
    Options&& set_size_hint(absl::optional<Position> size_hint) && {
      return std::move(set_size_hint(size_hint));
    }
    absl::optional<Position> size_hint() const { return size_hint_; }

    // Minimal size of a block of allocated data.
    //
    // This is used initially, while the destination is small.
    //
    // Default: `kMinBufferSize` (256).
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
    // This is for performance tuning, not a guarantee: does not apply to
    // objects allocated separately and then written to this
    // `ChainBackwardWriter`.
    //
    // Default: `kMaxBufferSize` (64K).
    Options& set_max_block_size(size_t max_block_size) & {
      RIEGELI_ASSERT_GT(max_block_size, 0u)
          << "Failed precondition of "
             "ChainBackwardWriterBase::Options::set_max_block_size(): "
             "zero block size";
      max_block_size_ = max_block_size;
      return *this;
    }
    Options&& set_max_block_size(size_t max_block_size) && {
      return std::move(set_max_block_size(max_block_size));
    }
    size_t max_block_size() const { return max_block_size_; }

   private:
    bool prepend_ = false;
    absl::optional<Position> size_hint_;
    size_t min_block_size_ = kMinBufferSize;
    size_t max_block_size_ = kMaxBufferSize;
  };

  // Returns the `Chain` being written to.
  virtual Chain* dest_chain() = 0;
  virtual const Chain* dest_chain() const = 0;

  bool SupportsTruncate() override { return true; }

 protected:
  explicit ChainBackwardWriterBase(Closed) noexcept : BackwardWriter(kClosed) {}

  explicit ChainBackwardWriterBase(const Options& options);

  ChainBackwardWriterBase(ChainBackwardWriterBase&& that) noexcept;
  ChainBackwardWriterBase& operator=(ChainBackwardWriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset(const Options& options);
  void Initialize(Chain* dest, bool prepend);

  void Done() override;
  bool PushSlow(size_t min_length, size_t recommended_length) override;
  using BackwardWriter::WriteSlow;
  bool WriteSlow(const Chain& src) override;
  bool WriteSlow(Chain&& src) override;
  bool WriteSlow(const absl::Cord& src) override;
  bool WriteSlow(absl::Cord&& src) override;
  bool WriteZerosSlow(Position length) override;
  bool FlushImpl(FlushType flush_type) override;
  bool TruncateImpl(Position new_size) override;

 private:
  // Discards uninitialized space from the beginning of `dest`, so that it
  // contains only actual data written.
  void SyncBuffer(Chain& dest);

  // Prepends uninitialized space to `dest`.
  void MakeBuffer(Chain& dest, size_t min_length = 0,
                  size_t recommended_length = 0);

  Chain::Options options_;

  // Invariants if `ok()`:
  //   `limit() == nullptr || limit() == dest_chain()->blocks().front().data()`
  //   `limit_pos() == dest_chain()->size()`
};

// A `BackwardWriter` which prepends to a `Chain`.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the `Chain` being written to. `Dest` must support
// `Dependency<Chain*, Dest>`, e.g. `Chain*` (not owned, default),
// `Chain` (owned).
//
// By relying on CTAD the template argument can be deduced as `Chain` if there
// are no constructor arguments or the only argument is `Options`, otherwise as
// the value type of the first constructor argument, except that CTAD is deleted
// if the first constructor argument is a `Chain&` or `const Chain&` (to avoid
// writing to an unintentionally separate copy of an existing object). This
// requires C++17.
//
// The `Chain` must not be accessed until the `ChainBackwardWriter` is closed or
// no longer used.
template <typename Dest = Chain*>
class ChainBackwardWriter : public ChainBackwardWriterBase {
 public:
  // Creates a closed `ChainBackwardWriter`.
  explicit ChainBackwardWriter(Closed) noexcept
      : ChainBackwardWriterBase(kClosed) {}

  // Will append to an owned `Chain` which can be accessed by `dest()`.
  // This constructor is present only if `Dest` is `Chain`.
  template <
      typename DependentDest = Dest,
      std::enable_if_t<std::is_same<DependentDest, Chain>::value, int> = 0>
  explicit ChainBackwardWriter(Options options = Options());

  // Will prepend to the `Chain` provided by `dest`.
  explicit ChainBackwardWriter(const Dest& dest, Options options = Options());
  explicit ChainBackwardWriter(Dest&& dest, Options options = Options());

  // Will prepend to the `Chain` provided by a `Dest` constructed from elements
  // of `dest_args`. This avoids constructing a temporary `Dest` and moving from
  // it.
  template <typename... DestArgs>
  explicit ChainBackwardWriter(std::tuple<DestArgs...> dest_args,
                               Options options = Options());

  ChainBackwardWriter(ChainBackwardWriter&& that) noexcept;
  ChainBackwardWriter& operator=(ChainBackwardWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `ChainBackwardWriter`. This
  // avoids constructing a temporary `ChainBackwardWriter` and moving from it.
  void Reset(Closed);
  template <
      typename DependentDest = Dest,
      std::enable_if_t<std::is_same<DependentDest, Chain>::value, int> = 0>
  void Reset(Options options = Options());
  void Reset(const Dest& dest, Options options = Options());
  void Reset(Dest&& dest, Options options = Options());
  template <typename... DestArgs>
  void Reset(std::tuple<DestArgs...> dest_args, Options options = Options());

  // Returns the object providing and possibly owning the `Chain` being written
  // to.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  Chain* dest_chain() override { return dest_.get(); }
  const Chain* dest_chain() const override { return dest_.get(); }

 private:
  void MoveDest(ChainBackwardWriter&& that);

  // The object providing and possibly owning the `Chain` being written to, with
  // uninitialized space prepended (possibly empty); `cursor()` points to the
  // end of the uninitialized space, except that it can be `nullptr` if the
  // uninitialized space is empty.
  Dependency<Chain*, Dest> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit ChainBackwardWriter(Closed)->ChainBackwardWriter<DeleteCtad<Closed>>;
explicit ChainBackwardWriter(ChainBackwardWriterBase::Options options =
                                 ChainBackwardWriterBase::Options())
    ->ChainBackwardWriter<Chain>;
template <typename Dest>
explicit ChainBackwardWriter(const Dest& dest,
                             ChainBackwardWriterBase::Options options =
                                 ChainBackwardWriterBase::Options())
    -> ChainBackwardWriter<std::conditional_t<
        std::is_convertible<const Dest*, const Chain*>::value,
        DeleteCtad<const Dest&>, std::decay_t<Dest>>>;
template <typename Dest>
explicit ChainBackwardWriter(Dest&& dest,
                             ChainBackwardWriterBase::Options options =
                                 ChainBackwardWriterBase::Options())
    -> ChainBackwardWriter<std::conditional_t<
        std::is_lvalue_reference<Dest>::value &&
            std::is_convertible<std::remove_reference_t<Dest>*,
                                const Chain*>::value,
        DeleteCtad<Dest&&>, std::decay_t<Dest>>>;
template <typename... DestArgs>
explicit ChainBackwardWriter(std::tuple<DestArgs...> dest_args,
                             ChainBackwardWriterBase::Options options =
                                 ChainBackwardWriterBase::Options())
    -> ChainBackwardWriter<DeleteCtad<std::tuple<DestArgs...>>>;
#endif

// Implementation details follow.

inline ChainBackwardWriterBase::ChainBackwardWriterBase(const Options& options)
    : options_(Chain::Options()
                   .set_size_hint(SaturatingIntCast<size_t>(
                       options.size_hint().value_or(0)))
                   .set_min_block_size(options.min_block_size())
                   .set_max_block_size(options.max_block_size())) {}

inline ChainBackwardWriterBase::ChainBackwardWriterBase(
    ChainBackwardWriterBase&& that) noexcept
    : BackwardWriter(static_cast<BackwardWriter&&>(that)),
      options_(that.options_) {}

inline ChainBackwardWriterBase& ChainBackwardWriterBase::operator=(
    ChainBackwardWriterBase&& that) noexcept {
  BackwardWriter::operator=(static_cast<BackwardWriter&&>(that));
  options_ = that.options_;
  return *this;
}

inline void ChainBackwardWriterBase::Reset(Closed) {
  BackwardWriter::Reset(kClosed);
  options_ = Chain::Options();
}

inline void ChainBackwardWriterBase::Reset(const Options& options) {
  BackwardWriter::Reset();
  options_ = Chain::Options()
                 .set_size_hint(
                     SaturatingIntCast<size_t>(options.size_hint().value_or(0)))
                 .set_min_block_size(options.min_block_size())
                 .set_max_block_size(options.max_block_size());
}

inline void ChainBackwardWriterBase::Initialize(Chain* dest, bool prepend) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of ChainBackwardWriter: null Chain pointer";
  if (prepend) {
    set_start_pos(dest->size());
  } else {
    dest->Clear();
  }
  const absl::Span<char> buffer =
      dest->PrependBuffer(0, 0, Chain::kAnyLength, options_);
  set_buffer(buffer.data(), buffer.size());
}

template <typename Dest>
template <typename DependentDest,
          std::enable_if_t<std::is_same<DependentDest, Chain>::value, int>>
inline ChainBackwardWriter<Dest>::ChainBackwardWriter(Options options)
    : ChainBackwardWriter(std::forward_as_tuple(), std::move(options)) {}

template <typename Dest>
inline ChainBackwardWriter<Dest>::ChainBackwardWriter(const Dest& dest,
                                                      Options options)
    : ChainBackwardWriterBase(options), dest_(dest) {
  Initialize(dest_.get(), options.prepend());
}

template <typename Dest>
inline ChainBackwardWriter<Dest>::ChainBackwardWriter(Dest&& dest,
                                                      Options options)
    : ChainBackwardWriterBase(options), dest_(std::move(dest)) {
  Initialize(dest_.get(), options.prepend());
}

template <typename Dest>
template <typename... DestArgs>
inline ChainBackwardWriter<Dest>::ChainBackwardWriter(
    std::tuple<DestArgs...> dest_args, Options options)
    : ChainBackwardWriterBase(options), dest_(std::move(dest_args)) {
  Initialize(dest_.get(), options.prepend());
}

template <typename Dest>
inline ChainBackwardWriter<Dest>::ChainBackwardWriter(
    ChainBackwardWriter&& that) noexcept
    : ChainBackwardWriterBase(static_cast<ChainBackwardWriterBase&&>(that)) {
  MoveDest(std::move(that));
}

template <typename Dest>
inline ChainBackwardWriter<Dest>& ChainBackwardWriter<Dest>::operator=(
    ChainBackwardWriter&& that) noexcept {
  ChainBackwardWriterBase::operator=(
      static_cast<ChainBackwardWriterBase&&>(that));
  MoveDest(std::move(that));
  return *this;
}

template <typename Dest>
inline void ChainBackwardWriter<Dest>::Reset(Closed) {
  ChainBackwardWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
template <typename DependentDest,
          std::enable_if_t<std::is_same<DependentDest, Chain>::value, int>>
inline void ChainBackwardWriter<Dest>::Reset(Options options) {
  Reset(std::forward_as_tuple(), std::move(options));
}

template <typename Dest>
inline void ChainBackwardWriter<Dest>::Reset(const Dest& dest,
                                             Options options) {
  ChainBackwardWriterBase::Reset(options);
  dest_.Reset(dest);
  Initialize(dest_.get(), options.prepend());
}

template <typename Dest>
inline void ChainBackwardWriter<Dest>::Reset(Dest&& dest, Options options) {
  ChainBackwardWriterBase::Reset(options);
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), options.prepend());
}

template <typename Dest>
template <typename... DestArgs>
inline void ChainBackwardWriter<Dest>::Reset(std::tuple<DestArgs...> dest_args,
                                             Options options) {
  ChainBackwardWriterBase::Reset(options);
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get(), options.prepend());
}

template <typename Dest>
inline void ChainBackwardWriter<Dest>::MoveDest(ChainBackwardWriter&& that) {
  if (dest_.kIsStable) {
    dest_ = std::move(that.dest_);
  } else {
    const size_t cursor_index = start_to_cursor();
    dest_ = std::move(that.dest_);
    if (start() != nullptr) {
      set_buffer(const_cast<char*>(dest_->blocks().front().data()),
                 dest_->size() - IntCast<size_t>(start_pos()), cursor_index);
    }
  }
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_CHAIN_BACKWARD_WRITER_H_
