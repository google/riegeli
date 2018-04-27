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

#include "absl/base/optimization.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// A Reader which reads from a Chain. It supports random access.
class ChainReader final : public Reader {
 public:
  // Creates a closed ChainReader.
  ChainReader() noexcept : Reader(State::kClosed) {}

  // Will read from the Chain which is owned by this ChainReader.
  explicit ChainReader(Chain src);

  // Will read from the Chain which is not owned by this ChainReader and must be
  // kept alive but not changed until the ChainReader is closed.
  explicit ChainReader(const Chain* src);

  ChainReader(ChainReader&& src) noexcept;
  ChainReader& operator=(ChainReader&& src) noexcept;

  bool SupportsRandomAccess() const override { return true; }
  bool Size(Position* size) const override;

 protected:
  void Done() override;
  bool PullSlow() override;
  bool ReadSlow(Chain* dest, size_t length) override;
  bool CopyToSlow(Writer* dest, Position length) override;
  bool CopyToSlow(BackwardWriter* dest, size_t length) override;
  bool SeekSlow(Position new_pos) override;

 private:
  ChainReader(ChainReader&& src, size_t block_index, size_t cursor_index);

  // Invariant: if !healthy() then owned_src_.empty()
  Chain owned_src_;
  // Invariants:
  //   src_ != nullptr
  //   if !healthy() then src_ == &owned_src_
  const Chain* src_ = &owned_src_;
  // Invariant: iter_ is an iterator into src_->blocks()
  Chain::BlockIterator iter_ = src_->blocks().cbegin();

  // Invariants:
  //   start_ == (iter_ == src_->blocks().cend() ? nullptr : iter_->data())
  //   buffer_size() == (iter_ == src_->blocks().cend() ? 0 : iter_->size())
  //   start_pos() is the position of iter_ in *src_
};

// Implementation details follow.

inline ChainReader::ChainReader(Chain src)
    : Reader(State::kOpen), owned_src_(std::move(src)) {
  if (iter_ != src_->blocks().cend()) {
    start_ = iter_->data();
    cursor_ = start_;
    limit_ = start_ + iter_->size();
    limit_pos_ = buffer_size();
  }
}

inline ChainReader::ChainReader(const Chain* src)
    : Reader(State::kOpen), src_(RIEGELI_ASSERT_NOTNULL(src)) {
  if (iter_ != src_->blocks().cend()) {
    start_ = iter_->data();
    cursor_ = start_;
    limit_ = start_ + iter_->size();
    limit_pos_ = buffer_size();
  }
}

inline ChainReader::ChainReader(ChainReader&& src) noexcept
    : ChainReader(std::move(src),
                  static_cast<size_t>(src.iter_ - src.src_->blocks().cbegin()),
                  src.read_from_buffer()) {}

// block_index and cursor_index are computed early because if src.src_ ==
// &src.owned_src_ then *src.src_ is moved, which invalidates src.iter_ and
// src.cursor_, but block_index and cursor_index depend on src.iter_ and
// src.cursor_.
inline ChainReader::ChainReader(ChainReader&& src, size_t block_index,
                                size_t cursor_index)
    : Reader(std::move(src)),
      owned_src_(std::move(src.owned_src_)),
      src_(src.src_ == &src.owned_src_
               ? &owned_src_
               : riegeli::exchange(src.src_, &src.owned_src_)),
      iter_(src_->blocks().cbegin() + block_index) {
  src.iter_ = src.src_->blocks().cbegin();
  if (iter_ != src_->blocks().cend()) {
    start_ = iter_->data();
    cursor_ = start_ + cursor_index;
    limit_ = start_ + iter_->size();
  }
}

inline ChainReader& ChainReader::operator=(ChainReader&& src) noexcept {
  // block_index and cursor_index are computed early because if src.src_ ==
  // &src.owned_src_ then *src.src_ is moved, which invalidates src.iter_ and
  // src.cursor_, but block_index and cursor_index depend on src.iter_ and
  // src.cursor_.
  const size_t block_index =
      static_cast<size_t>(src.iter_ - src.src_->blocks().cbegin());
  const size_t cursor_index = src.read_from_buffer();
  Reader::operator=(std::move(src));
  owned_src_ = std::move(src.owned_src_);
  src_ = src.src_ == &src.owned_src_
             ? &owned_src_
             : riegeli::exchange(src.src_, &src.owned_src_);
  // Set src.iter_ before iter_ to support self-assignment.
  src.iter_ = src.src_->blocks().cbegin();
  iter_ = src_->blocks().cbegin() + block_index;
  if (iter_ != src_->blocks().cend()) {
    start_ = iter_->data();
    cursor_ = start_ + cursor_index;
    limit_ = start_ + iter_->size();
  }
  return *this;
}

inline bool ChainReader::Size(Position* size) const {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  *size = src_->size();
  return true;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_CHAIN_READER_H_
