// Copyright 2018 Google LLC
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

#ifndef RIEGELI_BYTES_LIMITING_WRITER_H_
#define RIEGELI_BYTES_LIMITING_WRITER_H_

#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// A Writer which writes to another Writer up to the specified size limit.
// An attempt to write more fails, leaving destination contents unspecified.
//
// When a LimitingWriter is closed, its position is synchronized back to its
// destination.
class LimitingWriter : public Writer {
 public:
  // Creates a closed LimitingWriter.
  LimitingWriter() noexcept : Writer(State::kClosed) {}

  // Will write to the Writer which is not owned by this LimitingWriter and
  // must be kept alive but not accessed until closing the LimitingWriter,
  // except that it is allowed to read its destination directly after Flush().
  //
  // Precondition: size_limit >= dest->pos()
  LimitingWriter(Writer* dest, Position size_limit);

  LimitingWriter(LimitingWriter&& src) noexcept;
  LimitingWriter& operator=(LimitingWriter&& src) noexcept;

  // Returns the Writer being written to. Unchanged by Close().
  Writer* dest() const { return dest_; }

  bool Flush(FlushType flush_type) override;
  bool SupportsRandomAccess() const override;
  bool Size(Position* size) override;
  bool SupportsTruncate() const override;
  bool Truncate(Position new_size) override;

 protected:
  void Done() override;
  bool PushSlow() override;
  bool WriteSlow(absl::string_view src) override;
  bool WriteSlow(std::string&& src) override;
  bool WriteSlow(const Chain& src) override;
  bool WriteSlow(Chain&& src) override;
  bool SeekSlow(Position new_pos) override;

 private:
  void SyncBuffer();
  template <typename Src>
  bool WriteInternal(Src&& src);

  // Invariant: if healthy() then dest_ != nullptr
  Writer* dest_ = nullptr;
  // Invariant: if dest_ == nullptr then size_limit_ == 0
  Position size_limit_ = 0;

  // Invariants if healthy():
  //   start_pos_ = dest_->start_pos_
  //   limit_pos() <= size_limit_
  //   start_ == dest_->start_
  //   buffer_size() ==
  //       UnsignedMin(dest_->buffer_size(), size_limit_ - start_pos_)
};

// Implementation details follow.

inline LimitingWriter::LimitingWriter(LimitingWriter&& src) noexcept
    : Writer(std::move(src)),
      dest_(riegeli::exchange(src.dest_, nullptr)),
      size_limit_(riegeli::exchange(src.size_limit_, 0)) {}

inline LimitingWriter& LimitingWriter::operator=(
    LimitingWriter&& src) noexcept {
  Writer::operator=(std::move(src));
  dest_ = riegeli::exchange(src.dest_, nullptr);
  size_limit_ = riegeli::exchange(src.size_limit_, 0);
  return *this;
}

inline bool LimitingWriter::SupportsRandomAccess() const {
  return dest_ != nullptr && dest_->SupportsRandomAccess();
}

inline bool LimitingWriter::SupportsTruncate() const {
  return dest_ != nullptr && dest_->SupportsTruncate();
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_LIMITING_WRITER_H_
