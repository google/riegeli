// Copyright 2019 Google LLC
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

#ifndef RIEGELI_BYTES_NULL_BACKWARD_WRITER_H_
#define RIEGELI_BYTES_NULL_BACKWARD_WRITER_H_

#include <stddef.h>

#include <utility>

#include "absl/strings/cord.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/buffer_options.h"

namespace riegeli {

// A `BackwardWriter` which discards all output.
//
// It tracks `pos()` normally.
class NullBackwardWriter : public BackwardWriter {
 public:
  // Creates a closed `NullBackwardWriter`.
  explicit NullBackwardWriter(Closed) noexcept : BackwardWriter(kClosed) {}

  // Will discard all output.
  NullBackwardWriter() noexcept {}

  NullBackwardWriter(NullBackwardWriter&& that) noexcept;
  NullBackwardWriter& operator=(NullBackwardWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `NullBackwardWriter`. This
  // avoids constructing a temporary `NullBackwardWriter` and moving from it.
  void Reset(Closed);
  void Reset();

  void SetWriteSizeHint(absl::optional<Position> write_size_hint) override {
    buffer_sizer_.set_write_size_hint(pos(), write_size_hint);
  }
  bool PrefersCopying() const override { return true; }
  bool SupportsTruncate() override { return true; }

 protected:
  void Done() override;
  bool PushSlow(size_t min_length, size_t recommended_length) override;
  using BackwardWriter::WriteSlow;
  bool WriteSlow(const Chain& src) override;
  bool WriteSlow(const absl::Cord& src) override;
  bool WriteZerosSlow(Position length) override;
  bool TruncateImpl(Position new_size) override;

 private:
  // Resets buffer pointers to the beginning of the buffer.
  void SyncBuffer();

  // Ensures that the buffer has a sufficient size.
  bool MakeBuffer(size_t min_length = 0, size_t recommended_length = 0);

  WriteBufferSizer buffer_sizer_;
  Buffer buffer_;
};

// Implementation details follow.

inline NullBackwardWriter::NullBackwardWriter(
    NullBackwardWriter&& that) noexcept
    : BackwardWriter(static_cast<BackwardWriter&&>(that)),
      buffer_sizer_(that.buffer_sizer_),
      buffer_(std::move(that.buffer_)) {}

inline NullBackwardWriter& NullBackwardWriter::operator=(
    NullBackwardWriter&& that) noexcept {
  BackwardWriter::operator=(static_cast<BackwardWriter&&>(that));
  buffer_sizer_ = that.buffer_sizer_;
  buffer_ = std::move(that.buffer_);
  return *this;
}

inline void NullBackwardWriter::Reset(Closed) {
  BackwardWriter::Reset(kClosed);
  buffer_sizer_.Reset();
  buffer_ = Buffer();
}

inline void NullBackwardWriter::Reset() {
  BackwardWriter::Reset();
  buffer_sizer_.Reset();
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_NULL_BACKWARD_WRITER_H_
