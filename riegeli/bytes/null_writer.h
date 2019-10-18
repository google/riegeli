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

#include "riegeli/base/base.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/base/resetter.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// A `Writer` which discards all output.
class NullWriter : public Writer {
 public:
  // Tags for constructor parameters.
  using Object::InitiallyClosed;
  using Object::InitiallyOpen;
  using Object::kInitiallyClosed;
  using Object::kInitiallyOpen;

  // Creates a closed `NullWriter`.
  explicit NullWriter(InitiallyClosed) noexcept : Writer(kInitiallyClosed) {}

  // Will discard all output.
  explicit NullWriter(InitiallyOpen) noexcept : Writer(kInitiallyOpen) {}

  NullWriter(NullWriter&& that) noexcept;
  NullWriter& operator=(NullWriter&& that) noexcept;

  using Writer::Reset;

  bool Flush(FlushType flush_type) override;
  bool SupportsTruncate() const override { return true; }
  bool Truncate(Position new_size) override;

 protected:
  using Writer::Done;
  bool PushSlow(size_t min_length, size_t recommended_length) override;
  using Writer::WriteSlow;
  bool WriteSlow(const Chain& src) override;

 private:
  // Resets buffer pointers to the beginning of the buffer.
  void SyncBuffer();

  // Ensures that the buffer has a sufficient size.
  bool MakeBuffer(size_t min_length = 0);

  Buffer buffer_;
};

// Implementation details follow.

inline NullWriter::NullWriter(NullWriter&& that) noexcept
    : Writer(std::move(that)), buffer_(std::move(that.buffer_)) {}

inline NullWriter& NullWriter::operator=(NullWriter&& that) noexcept {
  Writer::operator=(std::move(that));
  buffer_ = std::move(that.buffer_);
  return *this;
}

template <>
struct Resetter<NullWriter> : ResetterByReset<NullWriter> {};

}  // namespace riegeli

#endif  // RIEGELI_BYTES_NULL_WRITER_H_
