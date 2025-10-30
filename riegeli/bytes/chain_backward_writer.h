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
#include <stdint.h>

#include <optional>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/byte_fill.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/external_ref.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/moving_dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
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
    Options& set_prepend(bool prepend) & ABSL_ATTRIBUTE_LIFETIME_BOUND {
      prepend_ = prepend;
      return *this;
    }
    Options&& set_prepend(bool prepend) && ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_prepend(prepend));
    }
    bool prepend() const { return prepend_; }

    // Minimal size of a block of allocated data.
    //
    // This is used initially, while the destination is small.
    //
    // Default: `kDefaultMinBlockSize` (512).
    Options& set_min_block_size(size_t min_block_size) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      min_block_size_ = UnsignedMin(min_block_size, uint32_t{1} << 31);
      return *this;
    }
    Options&& set_min_block_size(size_t min_block_size) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_min_block_size(min_block_size));
    }
    size_t min_block_size() const { return min_block_size_; }

    // Maximal size of a block of allocated data.
    //
    // This is for performance tuning, not a guarantee: does not apply to
    // objects allocated separately and then written to this
    // `ChainBackwardWriter`.
    //
    // Default: `kDefaultMaxBlockSize` (64K).
    Options& set_max_block_size(size_t max_block_size) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      RIEGELI_ASSERT_GT(max_block_size, 0u)
          << "Failed precondition of "
             "ChainBackwardWriterBase::Options::set_max_block_size(): "
             "zero block size";
      max_block_size_ = UnsignedMin(max_block_size, uint32_t{1} << 31);
      return *this;
    }
    Options&& set_max_block_size(size_t max_block_size) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_max_block_size(max_block_size));
    }
    size_t max_block_size() const { return max_block_size_; }

    // A shortcut for `set_min_block_size(block_size)` with
    // `set_max_block_size(block_size)`.
    Options& set_block_size(size_t block_size) & ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return set_min_block_size(block_size).set_max_block_size(block_size);
    }
    Options&& set_block_size(size_t block_size) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_block_size(block_size));
    }

   private:
    bool prepend_ = false;
    // Use `uint32_t` instead of `size_t` to reduce the object size.
    uint32_t min_block_size_ = uint32_t{kDefaultMinBlockSize};
    uint32_t max_block_size_ = uint32_t{kDefaultMaxBlockSize};
  };

  // Returns the `Chain` being written to. Unchanged by `Close()`.
  virtual Chain* DestChain() const ABSL_ATTRIBUTE_LIFETIME_BOUND = 0;

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
  void SetWriteSizeHintImpl(std::optional<Position> write_size_hint) override;
  bool PushSlow(size_t min_length, size_t recommended_length) override;
  using BackwardWriter::WriteSlow;
  bool WriteSlow(ExternalRef src) override;
  bool WriteSlow(const Chain& src) override;
  bool WriteSlow(Chain&& src) override;
  bool WriteSlow(const absl::Cord& src) override;
  bool WriteSlow(absl::Cord&& src) override;
  bool WriteSlow(ByteFill src) override;
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
  //   `limit() == nullptr || limit() == DestChain()->blocks().front().data()`
  //   `limit_pos() == DestChain()->size()`
};

// A `BackwardWriter` which writes to a `Chain` backwards.
// If `Options::prepend()` is `false` (the default), replaces existing contents
// of the `Chain`, clearing it first. If `Options::prepend()` is `true`,
// prepends to existing contents of the `Chain`.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the `Chain` being written to. `Dest` must support
// `Dependency<Chain*, Dest>`, e.g. `Chain*` (not owned, default),
// `Chain` (owned), `Any<Chain*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as `Chain` if there
// are no constructor arguments or the only argument is `Options`, otherwise as
// `TargetT` of the type of the first constructor argument, except that CTAD
// is deleted if the first constructor argument is a `Chain&` or `const Chain&`
// (to avoid writing to an unintentionally separate copy of an existing object).
//
//
// The `Chain` must not be accessed until the `ChainBackwardWriter` is closed or
// no longer used.
template <typename Dest = Chain*>
class ChainBackwardWriter : public ChainBackwardWriterBase {
 public:
  // Creates a closed `ChainBackwardWriter`.
  explicit ChainBackwardWriter(Closed) noexcept
      : ChainBackwardWriterBase(kClosed) {}

  // Will write to the `Chain` provided by `dest`.
  explicit ChainBackwardWriter(Initializer<Dest> dest,
                               Options options = Options());

  // Will write to an owned `Chain` which can be accessed by `dest()`.
  // This constructor is present only if `Dest` is `Chain`.
  template <typename DependentDest = Dest,
            std::enable_if_t<std::is_same_v<DependentDest, Chain>, int> = 0>
  explicit ChainBackwardWriter(Options options = Options());

  ChainBackwardWriter(ChainBackwardWriter&& that) = default;
  ChainBackwardWriter& operator=(ChainBackwardWriter&& that) = default;

  // Makes `*this` equivalent to a newly constructed `ChainBackwardWriter`. This
  // avoids constructing a temporary `ChainBackwardWriter` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Dest> dest,
                                          Options options = Options());
  template <typename DependentDest = Dest,
            std::enable_if_t<std::is_same_v<DependentDest, Chain>, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Options options = Options());

  // Returns the object providing and possibly owning the `Chain` being written
  // to.
  Dest& dest() ABSL_ATTRIBUTE_LIFETIME_BOUND { return dest_.manager(); }
  const Dest& dest() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return dest_.manager();
  }
  Chain* DestChain() const ABSL_ATTRIBUTE_LIFETIME_BOUND override {
    return dest_.get();
  }

 private:
  class Mover;

  // The object providing and possibly owning the `Chain` being written to, with
  // uninitialized space prepended (possibly empty); `cursor()` points to the
  // end of the uninitialized space, except that it can be `nullptr` if the
  // uninitialized space is empty.
  MovingDependency<Chain*, Dest, Mover> dest_;
};

explicit ChainBackwardWriter(Closed) -> ChainBackwardWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit ChainBackwardWriter(Dest&& dest,
                             ChainBackwardWriterBase::Options options =
                                 ChainBackwardWriterBase::Options())
    -> ChainBackwardWriter<std::conditional_t<
        std::conjunction_v<
            std::is_lvalue_reference<Dest>,
            std::is_convertible<std::remove_reference_t<Dest>*, const Chain*>>,
        DeleteCtad<Dest&&>, TargetT<Dest>>>;
explicit ChainBackwardWriter(ChainBackwardWriterBase::Options options =
                                 ChainBackwardWriterBase::Options())
    -> ChainBackwardWriter<Chain>;

// Implementation details follow.

inline ChainBackwardWriterBase::ChainBackwardWriterBase(const Options& options)
    : options_(Chain::Options()
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
                 .set_min_block_size(options.min_block_size())
                 .set_max_block_size(options.max_block_size());
}

inline void ChainBackwardWriterBase::Initialize(Chain* dest, bool prepend) {
  RIEGELI_ASSERT_NE(dest, nullptr)
      << "Failed precondition of ChainBackwardWriter: null Chain pointer";
  if (prepend) {
    set_start_pos(dest->size());
  } else {
    dest->Clear();
  }
}

template <typename Dest>
class ChainBackwardWriter<Dest>::Mover {
 public:
  static auto member() { return &ChainBackwardWriter::dest_; }

  explicit Mover(ChainBackwardWriter& self, ChainBackwardWriter& that)
      : uses_buffer_(self.start() != nullptr),
        start_to_cursor_(self.start_to_cursor()) {
    if (uses_buffer_) {
      RIEGELI_ASSERT_EQ(that.dest_->blocks().front().data(), self.limit())
          << "ChainBackwardWriter destination changed unexpectedly";
      RIEGELI_ASSERT_EQ(that.dest_->size(), self.limit_pos())
          << "ChainBackwardWriter destination changed unexpectedly";
    }
  }

  void Done(ChainBackwardWriter& self) {
    if (uses_buffer_) {
      const size_t buffer_size =
          self.dest_->size() - IntCast<size_t>(self.start_pos());
      const absl::string_view first_block = self.dest_->blocks().front();
      self.set_buffer(const_cast<char*>(first_block.data()), buffer_size,
                      start_to_cursor_);
    }
  }

 private:
  bool uses_buffer_;
  size_t start_to_cursor_;
};

template <typename Dest>
inline ChainBackwardWriter<Dest>::ChainBackwardWriter(Initializer<Dest> dest,
                                                      Options options)
    : ChainBackwardWriterBase(options), dest_(std::move(dest)) {
  Initialize(dest_.get(), options.prepend());
}

template <typename Dest>
template <typename DependentDest,
          std::enable_if_t<std::is_same_v<DependentDest, Chain>, int>>
inline ChainBackwardWriter<Dest>::ChainBackwardWriter(Options options)
    : ChainBackwardWriter(riegeli::Maker(), std::move(options)) {}

template <typename Dest>
inline void ChainBackwardWriter<Dest>::Reset(Closed) {
  ChainBackwardWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void ChainBackwardWriter<Dest>::Reset(Initializer<Dest> dest,
                                             Options options) {
  ChainBackwardWriterBase::Reset(options);
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), options.prepend());
}

template <typename Dest>
template <typename DependentDest,
          std::enable_if_t<std::is_same_v<DependentDest, Chain>, int>>
inline void ChainBackwardWriter<Dest>::Reset(Options options) {
  Reset(riegeli::Maker(), std::move(options));
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_CHAIN_BACKWARD_WRITER_H_
