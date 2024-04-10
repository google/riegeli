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

#ifndef RIEGELI_BYTES_CHAIN_READER_H_
#define RIEGELI_BYTES_CHAIN_READER_H_

#include <stddef.h>

#include <memory>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/strings/cord.h"
#include "absl/types/optional.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/pullable_reader.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

class BackwardWriter;
class Writer;

// Template parameter independent part of `ChainReader`.
class ChainReaderBase : public PullableReader {
 public:
  // Returns the `Chain` being read from. Unchanged by `Close()`.
  virtual const Chain* SrcChain() const = 0;

  bool ToleratesReadingAhead() override { return true; }
  bool SupportsRandomAccess() override { return true; }
  bool SupportsNewReader() override { return true; }

 protected:
  using PullableReader::PullableReader;

  ChainReaderBase(ChainReaderBase&& that) noexcept;
  ChainReaderBase& operator=(ChainReaderBase&& that) noexcept;

  void Reset(Closed);
  void Reset();
  void Initialize(const Chain* src);
  Chain::BlockIterator iter() const { return iter_; }
  void set_iter(Chain::BlockIterator iter) { iter_ = iter; }

  void Done() override;
  bool PullBehindScratch(size_t recommended_length) override;
  using PullableReader::ReadBehindScratch;
  bool ReadBehindScratch(size_t length, Chain& dest) override;
  bool ReadBehindScratch(size_t length, absl::Cord& dest) override;
  using PullableReader::CopyBehindScratch;
  bool CopyBehindScratch(Position length, Writer& dest) override;
  bool CopyBehindScratch(size_t length, BackwardWriter& dest) override;
  bool SeekBehindScratch(Position new_pos) override;
  absl::optional<Position> SizeImpl() override;
  std::unique_ptr<Reader> NewReaderImpl(Position initial_pos) override;

 private:
  // Invariant: `iter_.chain() == (is_open() ? SrcChain() : nullptr)`
  Chain::BlockIterator iter_;

  // Invariants if `is_open()` and scratch is not used:
  //   `start() ==
  //       (iter_ == SrcChain()->blocks().cend() ? nullptr : iter_->data())`
  //   `start_to_limit() ==
  //       (iter_ == SrcChain()->blocks().cend() ? 0 : iter_->size())`
  //   `start_pos()` is the position of `iter_` in `*SrcChain()`
};

// A `Reader` which reads from a `Chain`.
//
// It supports random access and `NewReader()`.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the `Chain` being read from. `Src` must support
// `Dependency<const Chain*, Src>`, e.g. `const Chain*` (not owned, default),
// `Chain` (owned), `AnyDependency<const Chain*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as
// `InitializerTargetT` of the type of the first constructor argument.
// This requires C++17.
//
// The `Chain` must not be changed until the `ChainReader` is closed or no
// longer used.
template <typename Src = const Chain*>
class ChainReader : public ChainReaderBase {
 public:
  // Creates a closed `ChainReader`.
  explicit ChainReader(Closed) noexcept : ChainReaderBase(kClosed) {}

  // Will read from the `Chain` provided by `src`.
  explicit ChainReader(Initializer<Src> src);

  ChainReader(ChainReader&& that) noexcept;
  ChainReader& operator=(ChainReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `ChainReader`. This avoids
  // constructing a temporary `ChainReader` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Src> src);

  // Returns the object providing and possibly owning the `Chain` being read
  // from. Unchanged by `Close()`.
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  const Chain* SrcChain() const override { return src_.get(); }

 private:
  // Moves `that.src_` to `src_`. Buffer pointers are already moved from `src_`
  // to `*this`; adjust them to match `src_`.
  void MoveSrc(ChainReader&& that);

  // The object providing and possibly owning the `Chain` being read from.
  Dependency<const Chain*, Src> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit ChainReader(Closed) -> ChainReader<DeleteCtad<Closed>>;
template <typename Src>
explicit ChainReader(Src&& src) -> ChainReader<InitializerTargetT<Src>>;
#endif

// Implementation details follow.

inline ChainReaderBase::ChainReaderBase(ChainReaderBase&& that) noexcept
    : PullableReader(static_cast<PullableReader&&>(that)),
      iter_(std::exchange(that.iter_, Chain::BlockIterator())) {}

inline ChainReaderBase& ChainReaderBase::operator=(
    ChainReaderBase&& that) noexcept {
  PullableReader::operator=(static_cast<PullableReader&&>(that));
  iter_ = std::exchange(that.iter_, Chain::BlockIterator());
  return *this;
}

inline void ChainReaderBase::Reset(Closed) {
  PullableReader::Reset(kClosed);
  iter_ = Chain::BlockIterator();
}

inline void ChainReaderBase::Reset() {
  PullableReader::Reset();
  // `iter_` will be set by `Initialize()`.
}

inline void ChainReaderBase::Initialize(const Chain* src) {
  RIEGELI_ASSERT(src != nullptr)
      << "Failed precondition of ChainReader: null Chain pointer";
  iter_ = src->blocks().cbegin();
  if (iter_ != src->blocks().cend()) {
    set_buffer(iter_->data(), iter_->size());
    move_limit_pos(available());
  }
}

template <typename Src>
inline ChainReader<Src>::ChainReader(Initializer<Src> src)
    : src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
inline ChainReader<Src>::ChainReader(ChainReader&& that) noexcept
    : ChainReaderBase(static_cast<ChainReaderBase&&>(that)) {
  MoveSrc(std::move(that));
}

template <typename Src>
inline ChainReader<Src>& ChainReader<Src>::operator=(
    ChainReader&& that) noexcept {
  ChainReaderBase::operator=(static_cast<ChainReaderBase&&>(that));
  MoveSrc(std::move(that));
  return *this;
}

template <typename Src>
inline void ChainReader<Src>::Reset(Closed) {
  ChainReaderBase::Reset(kClosed);
  src_.Reset();
}

template <typename Src>
inline void ChainReader<Src>::Reset(Initializer<Src> src) {
  ChainReaderBase::Reset();
  src_.Reset(std::move(src));
  Initialize(src_.get());
}

template <typename Src>
inline void ChainReader<Src>::MoveSrc(ChainReader&& that) {
  if (src_.kIsStable) {
    src_ = std::move(that.src_);
  } else {
    BehindScratch behind_scratch(this);
    const size_t block_index = iter().block_index();
    const size_t cursor_index = start_to_cursor();
    src_ = std::move(that.src_);
    if (iter().chain() != nullptr) {
      set_iter(Chain::BlockIterator(src_.get(), block_index));
      if (start() != nullptr) {
        set_buffer(iter()->data(), iter()->size(), cursor_index);
      }
    }
  }
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_CHAIN_READER_H_
