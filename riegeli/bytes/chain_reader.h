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
#include <utility>

#include "absl/utility/utility.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Template parameter invariant part of ChainReader.
class ChainReaderBase : public Reader {
 public:
  // Returns the Chain being read from. Unchanged by Close().
  virtual const Chain* src_chain() const = 0;

  bool SupportsRandomAccess() const override { return true; }
  bool Size(Position* size) override;

 protected:
  explicit ChainReaderBase(State state) noexcept : Reader(state) {}

  ChainReaderBase(ChainReaderBase&& that) noexcept;
  ChainReaderBase& operator=(ChainReaderBase&& that) noexcept;

  void Done() override;
  bool PullSlow() override;
  bool ReadSlow(Chain* dest, size_t length) override;
  bool CopyToSlow(Writer* dest, Position length) override;
  bool CopyToSlow(BackwardWriter* dest, size_t length) override;
  bool SeekSlow(Position new_pos) override;

  Chain::BlockIterator iter_;
};

// A Reader which reads from a Chain. It supports random access.
//
// The Src template parameter specifies the type of the object providing and
// possibly owning the Chain being read from. Src must support
// Dependency<const Chain*, Src>, e.g. const Chain* (not owned, default),
// Chain (owned).
//
// The Chain must not be changed until the ChainReader is closed or no longer
// used.
template <typename Src = const Chain*>
class ChainReader : public ChainReaderBase {
 public:
  // Creates a closed ChainReader.
  ChainReader() noexcept : ChainReaderBase(State::kClosed) {}

  // Will read from the Chain provided by src.
  explicit ChainReader(Src src);

  ChainReader(ChainReader&& that) noexcept;
  ChainReader& operator=(ChainReader&& that) noexcept;

  // Returns the object providing and possibly owning the Chain being read from.
  // Unchanged by Close().
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  const Chain* src_chain() const override { return src_.ptr(); }

 private:
  void MoveSrc(ChainReader&& that);

  // The object providing and possibly owning the Chain being read from.
  Dependency<const Chain*, Src> src_;

  // Invariants if healthy():
  //   iter_.chain() == src_.ptr()
  //   start_ == (iter_ == src_->blocks().cend() ? nullptr : iter_->data())
  //   buffer_size() == (iter_ == src_->blocks().cend() ? 0 : iter_->size())
  //   start_pos() is the position of iter_ in *src_
};

// Implementation details follow.

inline ChainReaderBase::ChainReaderBase(ChainReaderBase&& that) noexcept
    : Reader(std::move(that)),
      iter_(absl::exchange(that.iter_, Chain::BlockIterator())) {}

inline ChainReaderBase& ChainReaderBase::operator=(
    ChainReaderBase&& that) noexcept {
  Reader::operator=(std::move(that));
  iter_ = absl::exchange(that.iter_, Chain::BlockIterator());
  return *this;
}

template <typename Src>
inline ChainReader<Src>::ChainReader(Src src)
    : ChainReaderBase(State::kOpen), src_(std::move(src)) {
  RIEGELI_ASSERT(src_.ptr() != nullptr)
      << "Failed precondition of ChainReader<Src>::ChainReader(Src): "
         "null Chain pointer";
  iter_ = src_->blocks().cbegin();
  if (iter_ != src_->blocks().cend()) {
    start_ = iter_->data();
    cursor_ = start_;
    limit_ = start_ + iter_->size();
    limit_pos_ += available();
  }
}

template <typename Src>
inline ChainReader<Src>::ChainReader(ChainReader&& that) noexcept
    : ChainReaderBase(std::move(that)) {
  MoveSrc(std::move(that));
}

template <typename Src>
inline ChainReader<Src>& ChainReader<Src>::operator=(
    ChainReader&& that) noexcept {
  ChainReaderBase::operator=(std::move(that));
  MoveSrc(std::move(that));
  return *this;
}

template <typename Src>
inline void ChainReader<Src>::MoveSrc(ChainReader&& that) {
  if (src_.kIsStable()) {
    src_ = std::move(that.src_);
  } else {
    const size_t block_index = iter_.block_index();
    const size_t cursor_index = read_from_buffer();
    src_ = std::move(that.src_);
    if (iter_.chain() != nullptr) {
      iter_ = Chain::BlockIterator(src_.ptr(), block_index);
      if (start_ != nullptr) {
        start_ = iter_->data();
        cursor_ = start_ + cursor_index;
        limit_ = start_ + iter_->size();
      }
    }
  }
}

extern template class ChainReader<const Chain*>;
extern template class ChainReader<Chain>;

}  // namespace riegeli

#endif  // RIEGELI_BYTES_CHAIN_READER_H_
