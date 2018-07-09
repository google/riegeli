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
#include <limits>
#include <utility>

#include "absl/base/optimization.h"
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
class LimitingReader : public Reader {
 public:
  // An infinite size limit.
  static constexpr Position kNoSizeLimit();

  // Creates a closed LimitingReader.
  LimitingReader() noexcept : Reader(State::kClosed) {}

  // Will read from the Reader which is not owned by this LimitingReader and
  // must be kept alive but not accessed until closing the LimitingReader.
  //
  // Precondition: size_limit >= src->pos()
  LimitingReader(Reader* src, Position size_limit);

  LimitingReader(LimitingReader&& src) noexcept;
  LimitingReader& operator=(LimitingReader&& src) noexcept;

  // Returns the Reader being read from. Unchanged by Close().
  Reader* src() const { return src_; }

  // Change the size limit after construction.
  //
  // Precondition: size_limit >= pos()
  void SetSizeLimit(Position size_limit);
  Position size_limit() const { return size_limit_; }

  TypeId GetTypeId() const override;
  bool SupportsRandomAccess() const override;
  bool Size(Position* size) override;

 protected:
  void Done() override;
  bool PullSlow() override;
  bool ReadSlow(char* dest, size_t length) override;
  bool ReadSlow(Chain* dest, size_t length) override;
  bool CopyToSlow(Writer* dest, Position length) override;
  bool CopyToSlow(BackwardWriter* dest, size_t length) override;
  bool SeekSlow(Position new_pos) override;

 private:
  template <typename Dest>
  bool ReadInternal(Dest* dest, size_t length);
  void SyncBuffer();

  // Invariant: if healthy() then src_ != nullptr
  Reader* src_ = nullptr;
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

inline constexpr Position LimitingReader::kNoSizeLimit() {
  return std::numeric_limits<Position>::max();
}

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

inline void LimitingReader::SetSizeLimit(Position size_limit) {
  RIEGELI_ASSERT_GE(size_limit, pos())
      << "Failed precondition of LimitingReader::SetSizeLimit(): "
         "size limit smaller than current position";
  size_limit_ = size_limit;
  if (ABSL_PREDICT_FALSE(limit_pos_ > size_limit_)) {
    limit_ -= IntCast<size_t>(limit_pos_ - size_limit_);
    limit_pos_ = size_limit_;
  }
}

inline bool LimitingReader::SupportsRandomAccess() const {
  return src_ != nullptr && src_->SupportsRandomAccess();
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_LIMITING_READER_H_
