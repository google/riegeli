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
#include <stdint.h>

#include <memory>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/meta/type_traits.h"
#include "absl/strings/cord.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

template <typename Src>
class ChainReader;
class Reader;

// Template parameter independent part of `ChainWriter`.
class ChainWriterBase : public Writer {
 public:
  class Options {
   public:
    Options() noexcept {}

    // If `false`, replaces existing contents of the destination, clearing it
    // first.
    //
    // If `true`, appends to existing contents of the destination.
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

    // Minimal size of a block of allocated data.
    //
    // This is used initially, while the destination is small.
    //
    // Default: `kDefaultMinBlockSize` (256).
    Options& set_min_block_size(size_t min_block_size) & {
      min_block_size_ = UnsignedMin(min_block_size, uint32_t{1} << 31);
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
    // Default: `kDefaultMaxBlockSize` (64K).
    Options& set_max_block_size(size_t max_block_size) & {
      RIEGELI_ASSERT_GT(max_block_size, 0u)
          << "Failed precondition of "
             "ChainWriterBase::Options::set_max_block_size(): "
             "zero block size";
      max_block_size_ = UnsignedMin(max_block_size, uint32_t{1} << 31);
      return *this;
    }
    Options&& set_max_block_size(size_t max_block_size) && {
      return std::move(set_max_block_size(max_block_size));
    }
    size_t max_block_size() const { return max_block_size_; }

    // A shortcut for `set_min_block_size(block_size)` with
    // `set_max_block_size(block_size)`.
    Options& set_block_size(size_t block_size) & {
      return set_min_block_size(block_size).set_max_block_size(block_size);
    }
    Options&& set_block_size(size_t block_size) && {
      return std::move(set_block_size(block_size));
    }

   private:
    bool append_ = false;
    // Use `uint32_t` instead of `size_t` to reduce the object size.
    uint32_t min_block_size_ = uint32_t{kDefaultMinBlockSize};
    uint32_t max_block_size_ = uint32_t{kDefaultMaxBlockSize};
  };

  // Returns the `Chain` being written to. Unchanged by `Close()`.
  virtual Chain* DestChain() const = 0;
  Chain& Digest() {
    Flush();
    return *DestChain();
  }

  bool SupportsRandomAccess() override { return true; }
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
  void SetWriteSizeHintImpl(absl::optional<Position> write_size_hint) override;
  bool PushSlow(size_t min_length, size_t recommended_length) override;
  using Writer::WriteSlow;
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
  // Discards uninitialized space from the end of `dest`, so that it contains
  // only actual data written. Ensures that data which follow the current
  // position are separated in `*tail_`.
  void SyncBuffer(Chain& dest);

  // Appends uninitialized space to `dest`.
  void MakeBuffer(Chain& dest, size_t min_length = 1,
                  size_t recommended_length = 0);

  // Moves `length` of data from the beginning of `*tail_` to the end of `dest`.
  void MoveFromTail(size_t length, Chain& dest);

  // Moves `length` of data from the end of `dest` to the beginning of `*tail_`.
  void MoveToTail(size_t length, Chain& dest);

  // Returns `true` if data which follow the current position are appended to
  // `dest`.
  bool HasAppendedTail(const Chain& dest) const;

  // Moves data which follow the current position from being appended to `dest`
  // to being separated in `*tail_`.
  void ExtractTail(Chain& dest);

  // Moves data which follow the current position from being separated in
  // `*tail_` to being appended to `dest`.
  void AppendTail(Chain& dest);

  // Removes a prefix of `*tail_` of the given `length`, staturated at clearing
  // the whole `*tail_`.
  void ShrinkTail(size_t length);

  Chain::Options options_;

  // If `limit_pos() < DestChain()->size()`, then data after the current
  // position are appended to `*DestChain()`, buffer pointers are `nullptr`,
  // and `tail_ == nullptr || tail_->empty()`.
  //
  // Otherwise, if `limit_pos() == DestChain()->size() && tail_ != nullptr`,
  // data after the current position are separated in `*tail_`, ignoring a
  // prefix of `*tail_` with length `start_to_cursor()`, saturated at ignoring
  // the whole `*tail_` (the ignored prefix is being overwritten with buffered
  // data).
  //
  // Otherwise `limit_pos() == DestChain()->size() && tail_ == nullptr`, and
  // there are no data after the current position.
  //
  // `tail_` is stored behind `std::unique_ptr` to reduce the object size in the
  // common case when random access is not used.
  std::unique_ptr<Chain> tail_;

  AssociatedReader<ChainReader<const Chain*>> associated_reader_;

  // Invariants if `ok()`:
  //   `limit() == nullptr || limit() == DestChain()->blocks().back().data() +
  //                                     DestChain()->blocks().back().size()`
  //   `limit_pos() <= DestChain()->size()`
  //   if `limit_pos() < DestChain()->size()` then
  //       `start() == nullptr && (tail_ == nullptr || tail_->empty())`
};

// A `Writer` which appends to a `Chain`.
//
// It supports `Seek()` and `ReadMode()`.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the `Chain` being written to. `Dest` must support
// `Dependency<Chain*, Dest>`, e.g. `Chain*` (not owned, default),
// `Chain` (owned), `AnyDependency<Chain*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as `Chain` if there
// are no constructor arguments or the only argument is `Options`, otherwise as
// `InitializerTargetT` of the type of the first constructor argument, except
// that CTAD is deleted if the first constructor argument is a `Chain&` or
// `const Chain&` (to avoid writing to an unintentionally separate copy of an
// existing object). This requires C++17.
//
// The `Chain` must not be accessed until the `ChainWriter` is closed or no
// longer used, except that it is allowed to read the `Chain` immediately after
// `Flush()`.
template <typename Dest = Chain*>
class ChainWriter : public ChainWriterBase {
 public:
  // Creates a closed `ChainWriter`.
  explicit ChainWriter(Closed) noexcept : ChainWriterBase(kClosed) {}

  // Will append to the `Chain` provided by `dest`.
  explicit ChainWriter(Initializer<Dest> dest, Options options = Options());

  // Will append to an owned `Chain` which can be accessed by `dest()`.
  // This constructor is present only if `Dest` is `Chain`.
  template <
      typename DependentDest = Dest,
      std::enable_if_t<std::is_same<DependentDest, Chain>::value, int> = 0>
  explicit ChainWriter(Options options = Options());

  ChainWriter(ChainWriter&& that) noexcept;
  ChainWriter& operator=(ChainWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `ChainWriter`. This avoids
  // constructing a temporary `ChainWriter` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Dest> dest,
                                          Options options = Options());
  template <
      typename DependentDest = Dest,
      std::enable_if_t<std::is_same<DependentDest, Chain>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Options options = Options());

  // Returns the object providing and possibly owning the `Chain` being written
  // to. Unchanged by `Close()`.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  Chain* DestChain() const override { return dest_.get(); }

 private:
  // Moves `that.dest_` to `dest_`. Buffer pointers are already moved from
  // `dest_` to `*this`; adjust them to match `dest_`.
  void MoveDest(ChainWriter&& that);

  // The object providing and possibly owning the `Chain` being written to, with
  // uninitialized space appended (possibly empty); `cursor()` points to the
  // uninitialized space, except that it can be `nullptr` if the uninitialized
  // space is empty.
  Dependency<Chain*, Dest> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit ChainWriter(Closed) -> ChainWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit ChainWriter(
    Dest&& dest, ChainWriterBase::Options options = ChainWriterBase::Options())
    -> ChainWriter<std::conditional_t<
        absl::conjunction<std::is_lvalue_reference<Dest>,
                          std::is_convertible<std::remove_reference_t<Dest>*,
                                              const Chain*>>::value,
        DeleteCtad<Dest&&>, InitializerTargetT<Dest>>>;
explicit ChainWriter(ChainWriterBase::Options options =
                         ChainWriterBase::Options()) -> ChainWriter<Chain>;
#endif

// Implementation details follow.

inline ChainWriterBase::ChainWriterBase(const Options& options)
    : options_(Chain::Options()
                   .set_min_block_size(options.min_block_size())
                   .set_max_block_size(options.max_block_size())) {}

inline ChainWriterBase::ChainWriterBase(ChainWriterBase&& that) noexcept
    : Writer(static_cast<Writer&&>(that)),
      options_(that.options_),
      tail_(std::move(that.tail_)),
      associated_reader_(std::move(that.associated_reader_)) {}

inline ChainWriterBase& ChainWriterBase::operator=(
    ChainWriterBase&& that) noexcept {
  Writer::operator=(static_cast<Writer&&>(that));
  options_ = that.options_;
  tail_ = std::move(that.tail_);
  associated_reader_ = std::move(that.associated_reader_);
  return *this;
}

inline void ChainWriterBase::Reset(Closed) {
  Writer::Reset(kClosed);
  options_ = Chain::Options();
  tail_.reset();
  associated_reader_.Reset();
}

inline void ChainWriterBase::Reset(const Options& options) {
  Writer::Reset();
  options_ = Chain::Options()
                 .set_min_block_size(options.min_block_size())
                 .set_max_block_size(options.max_block_size());
  if (tail_ != nullptr) tail_->Clear();
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
}

template <typename Dest>
inline ChainWriter<Dest>::ChainWriter(Initializer<Dest> dest, Options options)
    : ChainWriterBase(options), dest_(std::move(dest)) {
  Initialize(dest_.get(), options.append());
}

template <typename Dest>
template <typename DependentDest,
          std::enable_if_t<std::is_same<DependentDest, Chain>::value, int>>
inline ChainWriter<Dest>::ChainWriter(Options options)
    : ChainWriter(riegeli::Maker(), std::move(options)) {}

template <typename Dest>
inline ChainWriter<Dest>::ChainWriter(ChainWriter&& that) noexcept
    : ChainWriterBase(static_cast<ChainWriterBase&&>(that)) {
  MoveDest(std::move(that));
}

template <typename Dest>
inline ChainWriter<Dest>& ChainWriter<Dest>::operator=(
    ChainWriter&& that) noexcept {
  ChainWriterBase::operator=(static_cast<ChainWriterBase&&>(that));
  MoveDest(std::move(that));
  return *this;
}

template <typename Dest>
inline void ChainWriter<Dest>::Reset(Closed) {
  ChainWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void ChainWriter<Dest>::Reset(Initializer<Dest> dest, Options options) {
  ChainWriterBase::Reset(options);
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), options.append());
}

template <typename Dest>
template <typename DependentDest,
          std::enable_if_t<std::is_same<DependentDest, Chain>::value, int>>
inline void ChainWriter<Dest>::Reset(Options options) {
  Reset(riegeli::Maker(), std::move(options));
}

template <typename Dest>
inline void ChainWriter<Dest>::MoveDest(ChainWriter&& that) {
  if (dest_.kIsStable) {
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
