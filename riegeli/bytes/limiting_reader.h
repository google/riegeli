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

#ifndef RIEGELI_BYTES_LIMITING_READER_H_
#define RIEGELI_BYTES_LIMITING_READER_H_

#include <stddef.h>
#include <utility>

#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// A Reader which reads from another Reader up to the specified size limit,
// then pretends that the source ends.
//
// When a LimitingReader is closed, its position is synchronized back to its
// source.
class LimitingReader final : public Reader {
 public:
  // Creates a closed LimitingReader.
  LimitingReader() noexcept : Reader(State::kClosed) {}

  // Will read from the Reader which is not owned by this LimitingReader and
  // must be kept alive but not accessed until closing the LimitingReader.
  //
  // Precondition: size_limit >= src->pos()
  LimitingReader(Reader* src, Position size_limit);

  LimitingReader(LimitingReader&& src) noexcept;
  LimitingReader& operator=(LimitingReader&& src) noexcept;

  TypeId GetTypeId() const override;
  bool SupportsRandomAccess() const override;
  bool Size(Position* size) const override;

 protected:
  void Done() override;
  bool PullSlow() override;
  bool ReadSlow(char* dest, size_t length) override;
  bool ReadSlow(Chain* dest, size_t length) override;
  bool CopyToSlow(Writer* dest, Position length) override;
  bool CopyToSlow(BackwardWriter* dest, size_t length) override;
  bool HopeForMoreSlow() const override;
  bool SeekSlow(Position new_pos) override;

 private:
  template <typename Dest>
  bool ReadInternal(Dest* dest, size_t length);
  void SyncBuffer();

  // Invariant: if healthy() then src_ != nullptr
  Reader* src_ = nullptr;
  // Invariant: if src_ == nullptr then size_limit_ == 0
  Position size_limit_ = 0;
  // If not nullptr, the original constructor argument (in this case src_ is
  // wrapped_->src_), remembered here to synchronize the position in Done().
  LimitingReader* wrapped_ = nullptr;

  // Invariants if healthy():
  //   limit_pos_ == UnsignedMin(src_->limit_pos_, size_limit_)
  //   start_ == src_->start_
  //   limit_ == src_->limit_ - (src_->limit_pos_ - limit_pos_)
};

// Implementation details follow.

inline LimitingReader::LimitingReader(LimitingReader&& src) noexcept
    : Reader(std::move(src)),
      src_(riegeli::exchange(src.src_, nullptr)),
      size_limit_(riegeli::exchange(src.size_limit_, 0)),
      wrapped_(riegeli::exchange(src.wrapped_, nullptr)) {}

inline LimitingReader& LimitingReader::operator=(
    LimitingReader&& src) noexcept {
  Reader::operator=(std::move(src));
  src_ = riegeli::exchange(src.src_, nullptr);
  size_limit_ = riegeli::exchange(src.size_limit_, 0);
  wrapped_ = riegeli::exchange(src.wrapped_, nullptr);
  return *this;
}

inline bool LimitingReader::SupportsRandomAccess() const {
  return src_ != nullptr && src_->SupportsRandomAccess();
}

inline bool LimitingReader::Size(Position* size) const {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  if (RIEGELI_UNLIKELY(!src_->Size(size))) return false;
  *size = UnsignedMin(*size, size_limit_);
  return true;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_LIMITING_READER_H_
