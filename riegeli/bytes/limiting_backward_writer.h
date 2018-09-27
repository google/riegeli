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

#ifndef RIEGELI_BYTES_LIMITING_BACKWARD_WRITER_H_
#define RIEGELI_BYTES_LIMITING_BACKWARD_WRITER_H_

#include <string>
#include <utility>

#include "absl/strings/string_view.h"
#include "absl/utility/utility.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/backward_writer.h"

namespace riegeli {

// A BackwardWriter which writes to another BackwardWriter up to the specified
// size limit. An attempt to write more fails, leaving destination contents
// unspecified.
//
// The original BackwardWriter must not be accessed until the
// LimitingBackwardWriter is closed or no longer used. When the
// LimitingBackwardWriter is closed, its position is synchronized back to the
// original BackwardWriter.
class LimitingBackwardWriter : public BackwardWriter {
 public:
  // Creates a closed LimitingBackwardWriter.
  LimitingBackwardWriter() noexcept : BackwardWriter(State::kClosed) {}

  // Will write to dest.
  //
  // Precondition: size_limit >= dest->pos()
  explicit LimitingBackwardWriter(BackwardWriter* dest, Position size_limit);

  LimitingBackwardWriter(LimitingBackwardWriter&& that) noexcept;
  LimitingBackwardWriter& operator=(LimitingBackwardWriter&& that) noexcept;

  // Returns the original BackwardWriter. Unchanged by Close().
  BackwardWriter* dest() const { return dest_; }

  bool SupportsTruncate() const override;
  bool Truncate(Position new_size) override;

 protected:
  void Done() override;
  bool PushSlow() override;
  bool WriteSlow(absl::string_view src) override;
  bool WriteSlow(std::string&& src) override;
  bool WriteSlow(const Chain& src) override;
  bool WriteSlow(Chain&& src) override;

 private:
  void SyncBuffer();
  template <typename Src>
  bool WriteInternal(Src&& src);

  // Invariant: if healthy() then dest_ != nullptr
  BackwardWriter* dest_ = nullptr;
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

inline LimitingBackwardWriter::LimitingBackwardWriter(
    LimitingBackwardWriter&& that) noexcept
    : BackwardWriter(std::move(that)),
      dest_(absl::exchange(that.dest_, nullptr)),
      size_limit_(absl::exchange(that.size_limit_, 0)) {}

inline LimitingBackwardWriter& LimitingBackwardWriter::operator=(
    LimitingBackwardWriter&& that) noexcept {
  BackwardWriter::operator=(std::move(that));
  dest_ = absl::exchange(that.dest_, nullptr);
  size_limit_ = absl::exchange(that.size_limit_, 0);
  return *this;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_LIMITING_BACKWARD_WRITER_H_
