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
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

template <typename Src>
class ChainReader;

// Template parameter independent part of `ChainWriter`.
class ChainWriterBase : public Writer {
 public:
  class Options {
   public:
    Options() noexcept {}

    // If `true`, appends to existing contents of the destination.
    //
    // If `false`, replaces existing contents of the destination, clearing it
    // first.
    //
    // Default: `false`.
    Options& set_append(bool append) & {
      append_ = append;
      return *this;
    }
    Options&& set_append(bool append) && {
      return std::move(set_append(append));
    }
    bool append() const { return append_; }

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
    // objects allocated separately and then written to this `ChainWriter`.
    //
    // Default: `kMaxBufferSize` (64K).
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
    bool append_ = false;
    absl::optional<Position> size_hint_;
    size_t min_block_size_ = kMinBufferSize;
    size_t max_block_size_ = kMaxBufferSize;
  };

  // Returns the `Chain` being written to.
  virtual Chain* dest_chain() = 0;
  virtual const Chain* dest_chain() const = 0;

  bool SupportsSize() override { return true; }
  bool SupportsTruncate() override { return true; }
  bool SupportsReadMode() override { return true; }

 protected:
  explicit ChainWriterBase(Closed) noexcept : Writer(kClosed) {}

  explicit ChainWriterBase(const Options& options);

  ChainWriterBase(ChainWriterBase&& that) noexcept;
  ChainWriterBase& operator=(ChainWriterBase&& that) noexcept;

  void Reset(Closed);
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
  bool FlushImpl(FlushType flush_type) override;
  absl::optional<Position> SizeImpl() override;
  bool TruncateImpl(Position new_size) override;
  Reader* ReadModeImpl(Position initial_pos) override;

 private:
  // Discards uninitialized space from the end of `dest`, so that it contains
  // only actual data written.
  void SyncBuffer(Chain& dest);

  // Appends uninitialized space to `dest`.
  void MakeBuffer(Chain& dest, size_t min_length = 0,
                  size_t recommended_length = 0);

  Chain::Options options_;

  AssociatedReader<ChainReader<const Chain*>> associated_reader_;

  // Invariants if `healthy()`:
  //   `limit() == nullptr || limit() == dest_chain()->blocks().back().data() +
  //                                     dest_chain()->blocks().back().size()`
  //   `limit_pos() == dest_chain()->size()`
};

// A `Writer` which appends to a `Chain`.
//
// It supports `ReadMode()`.
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
// The `Chain` must not be accessed until the `ChainWriter` is closed or no
// longer used, except that it is allowed to read the `Chain` immediately after
// `Flush()`.
template <typename Dest = Chain*>
class ChainWriter : public ChainWriterBase {
 public:
  // Creates a closed `ChainWriter`.
  explicit ChainWriter(Closed) noexcept : ChainWriterBase(kClosed) {}

  // Will append to an owned `Chain` which can be accessed by `dest()`.
  // This constructor is present only if `Dest` is `Chain`.
  template <typename T = Dest,
            std::enable_if_t<std::is_same<T, Chain>::value, int> = 0>
  explicit ChainWriter(Options options = Options());

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
  void Reset(Closed);
  template <typename T = Dest,
            std::enable_if_t<std::is_same<T, Chain>::value, int> = 0>
  void Reset(Options options = Options());
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
explicit ChainWriter(Closed)->ChainWriter<DeleteCtad<Closed>>;
explicit ChainWriter(
    ChainWriterBase::Options options = ChainWriterBase::Options())
    ->ChainWriter<Chain>;
template <typename Dest>
explicit ChainWriter(const Dest& dest, ChainWriterBase::Options options =
                                           ChainWriterBase::Options())
    -> ChainWriter<std::conditional_t<
        std::is_convertible<const Dest*, const Chain*>::value,
        DeleteCtad<const Dest&>, std::decay_t<Dest>>>;
template <typename Dest>
explicit ChainWriter(
    Dest&& dest, ChainWriterBase::Options options = ChainWriterBase::Options())
    -> ChainWriter<std::conditional_t<
        std::is_lvalue_reference<Dest>::value &&
            std::is_convertible<std::remove_reference_t<Dest>*,
                                const Chain*>::value,
        DeleteCtad<Dest&&>, std::decay_t<Dest>>>;
template <typename... DestArgs>
explicit ChainWriter(
    std::tuple<DestArgs...> dest_args,
    ChainWriterBase::Options options = ChainWriterBase::Options())
    -> ChainWriter<DeleteCtad<std::tuple<DestArgs...>>>;
#endif

// Implementation details follow.

inline ChainWriterBase::ChainWriterBase(const Options& options)
    : options_(Chain::Options()
                   .set_size_hint(SaturatingIntCast<size_t>(
                       options.size_hint().value_or(0)))
                   .set_min_block_size(options.min_block_size())
                   .set_max_block_size(options.max_block_size())) {}

inline ChainWriterBase::ChainWriterBase(ChainWriterBase&& that) noexcept
    : Writer(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      options_(that.options_),
      associated_reader_(std::move(that.associated_reader_)) {}

inline ChainWriterBase& ChainWriterBase::operator=(
    ChainWriterBase&& that) noexcept {
  Writer::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  options_ = that.options_;
  associated_reader_ = std::move(that.associated_reader_);
  return *this;
}

inline void ChainWriterBase::Reset(Closed) {
  Writer::Reset(kClosed);
  options_ = Chain::Options();
  associated_reader_.Reset();
}

inline void ChainWriterBase::Reset(const Options& options) {
  Writer::Reset();
  options_ = Chain::Options()
                 .set_size_hint(
                     SaturatingIntCast<size_t>(options.size_hint().value_or(0)))
                 .set_min_block_size(options.min_block_size())
                 .set_max_block_size(options.max_block_size());
  associated_reader_.Reset();
}

inline void ChainWriterBase::Initialize(Chain* dest, bool append) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of ChainWriter: null Chain pointer";
  if (append) {
    set_start_pos(dest->size());
  } else {
    dest->Clear();
  }
  const absl::Span<char> buffer =
      dest->AppendBuffer(0, 0, Chain::kAnyLength, options_);
  set_buffer(buffer.data(), buffer.size());
}

template <typename Dest>
template <typename T, std::enable_if_t<std::is_same<T, Chain>::value, int>>
inline ChainWriter<Dest>::ChainWriter(Options options)
    : ChainWriter(std::forward_as_tuple(), std::move(options)) {}

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
inline void ChainWriter<Dest>::Reset(Closed) {
  ChainWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
template <typename T, std::enable_if_t<std::is_same<T, Chain>::value, int>>
inline void ChainWriter<Dest>::Reset(Options options) {
  Reset(std::forward_as_tuple(), std::move(options));
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
    const size_t cursor_index = start_to_cursor();
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

}  // namespace riegeli

#endif  // RIEGELI_BYTES_CHAIN_WRITER_H_
