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
#include "riegeli/base/base.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/backward_writer.h"

namespace riegeli {

// A `BackwardWriter` which discards all output.
class NullBackwardWriter : public BackwardWriter {
 public:
  // Tags for constructor parameters.
  using Object::InitiallyClosed;
  using Object::InitiallyOpen;
  using Object::kInitiallyClosed;
  using Object::kInitiallyOpen;

  // Creates a closed `NullBackwardWriter`.
  explicit NullBackwardWriter(InitiallyClosed) noexcept
      : BackwardWriter(kInitiallyClosed) {}

  // Will discard all output.
  explicit NullBackwardWriter(InitiallyOpen) noexcept
      : BackwardWriter(kInitiallyOpen) {}

  NullBackwardWriter(NullBackwardWriter&& that) noexcept;
  NullBackwardWriter& operator=(NullBackwardWriter&& that) noexcept;

  using BackwardWriter::Reset;

  bool PrefersCopying() const override { return true; }
  bool Flush(FlushType flush_type) override;
  bool SupportsTruncate() override { return true; }
  bool Truncate(Position new_size) override;

 protected:
  using BackwardWriter::Done;
  bool PushSlow(size_t min_length, size_t recommended_length) override;
  using BackwardWriter::WriteSlow;
  bool WriteSlow(const Chain& src) override;
  bool WriteSlow(const absl::Cord& src) override;
  bool WriteZerosSlow(Position length) override;

 private:
  // Resets buffer pointers to the beginning of the buffer.
  void SyncBuffer();

  // Ensures that the buffer has a sufficient size.
  bool MakeBuffer(size_t min_length = 0);

  Buffer buffer_;
};

// Implementation details follow.

inline NullBackwardWriter::NullBackwardWriter(
    NullBackwardWriter&& that) noexcept
    : BackwardWriter(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      buffer_(std::move(that.buffer_)) {}

inline NullBackwardWriter& NullBackwardWriter::operator=(
    NullBackwardWriter&& that) noexcept {
  BackwardWriter::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  buffer_ = std::move(that.buffer_);
  return *this;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_NULL_BACKWARD_WRITER_H_
