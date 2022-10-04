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

#ifndef RIEGELI_BYTES_NULL_WRITER_H_
#define RIEGELI_BYTES_NULL_WRITER_H_

#include <stddef.h>

#include <utility>

#include "absl/base/attributes.h"
#include "absl/strings/cord.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// A `Writer` which discards all output.
//
// It tracks `pos()` normally.
class NullWriter : public Writer {
 public:
  // Creates a closed `NullWriter`.
  explicit NullWriter(Closed) noexcept : Writer(kClosed) {}

  // Will discard all output.
  NullWriter() noexcept {}

  NullWriter(NullWriter&& that) noexcept;
  NullWriter& operator=(NullWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `NullWriter`. This avoids
  // constructing a temporary `NullWriter` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset();

  bool PrefersCopying() const override { return true; }
  bool SupportsRandomAccess() override { return true; }

 protected:
  void Done() override;
  void SetWriteSizeHintImpl(absl::optional<Position> write_size_hint) override;
  bool PushSlow(size_t min_length, size_t recommended_length) override;
  using Writer::WriteSlow;
  bool WriteSlow(const Chain& src) override;
  bool WriteSlow(const absl::Cord& src) override;
  bool WriteZerosSlow(Position length) override;
  bool SeekSlow(Position new_pos) override;
  absl::optional<Position> SizeImpl() override;
  bool TruncateImpl(Position new_size) override;

 private:
  // Resets buffer pointers to the beginning of the buffer.
  void SyncBuffer();

  // Ensures that the buffer has a sufficient size.
  bool MakeBuffer(size_t min_length = 1, size_t recommended_length = 0);

  WriteBufferSizer buffer_sizer_;
  Buffer buffer_;

  // Size of written data is always `UnsignedMax(pos(), written_size_)`.
  // This is used to determine the size after seeking backwards.
  Position written_size_ = 0;
};

// Implementation details follow.

inline NullWriter::NullWriter(NullWriter&& that) noexcept
    : Writer(static_cast<Writer&&>(that)),
      buffer_sizer_(that.buffer_sizer_),
      buffer_(std::move(that.buffer_)),
      written_size_(that.written_size_) {}

inline NullWriter& NullWriter::operator=(NullWriter&& that) noexcept {
  Writer::operator=(static_cast<Writer&&>(that));
  buffer_sizer_ = that.buffer_sizer_;
  buffer_ = std::move(that.buffer_);
  written_size_ = that.written_size_;
  return *this;
}

inline void NullWriter::Reset(Closed) {
  Writer::Reset(kClosed);
  buffer_sizer_.Reset();
  buffer_ = Buffer();
  written_size_ = 0;
}

inline void NullWriter::Reset() {
  Writer::Reset();
  buffer_sizer_.Reset();
  written_size_ = 0;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_NULL_WRITER_H_
