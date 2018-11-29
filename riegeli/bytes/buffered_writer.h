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

#ifndef RIEGELI_BYTES_BUFFERED_WRITER_H_
#define RIEGELI_BYTES_BUFFERED_WRITER_H_

#include <stddef.h>
#include <utility>

#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/buffer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// BufferedWriter helps to implement a Writer for an underlying destination
// which accepts data by copying from external byte arrays, e.g. like in the
// write syscall.
//
// BufferedWriter accumulates data to be pushed in a flat buffer. Writing a
// large enough array bypasses the buffer.
class BufferedWriter : public Writer {
 protected:
  // Creates a closed BufferedWriter.
  BufferedWriter() noexcept : Writer(State::kClosed) {}

  // Creates a BufferedWriter with the given buffer size.
  explicit BufferedWriter(size_t buffer_size) noexcept;

  BufferedWriter(BufferedWriter&& that) noexcept;
  BufferedWriter& operator=(BufferedWriter&& that) noexcept;

  bool PushSlow() override;
  using Writer::WriteSlow;
  bool WriteSlow(absl::string_view src) override;

  // Writes buffered data to the destination, but unlike PushSlow(), does not
  // ensure that a buffer is allocated.
  //
  // Postcondition: written_to_buffer() == 0
  bool PushInternal();

  // Writes data to the destination, to the physical destination position which
  // is start_pos_.
  //
  // Increments start_pos_ by the length written.
  //
  // Preconditions:
  //   !src.empty()
  //   healthy()
  //   written_to_buffer() == 0
  virtual bool WriteInternal(absl::string_view src) = 0;

 private:
  // Buffered data, to be written directly after the physical destination
  // position which is start_pos_.
  //
  // Invariant: if healthy() then buffer_.size() > 0
  internal::Buffer buffer_;
};

// Implementation details follow.

inline BufferedWriter::BufferedWriter(size_t buffer_size) noexcept
    : Writer(State::kOpen), buffer_(buffer_size) {
  RIEGELI_ASSERT_GT(buffer_size, 0u)
      << "Failed precondition of BufferedWriter::BufferedWriter(size_t): "
         "zero buffer size";
}

inline BufferedWriter::BufferedWriter(BufferedWriter&& that) noexcept
    : Writer(std::move(that)), buffer_(std::move(that.buffer_)) {}

inline BufferedWriter& BufferedWriter::operator=(
    BufferedWriter&& that) noexcept {
  Writer::operator=(std::move(that));
  buffer_ = std::move(that.buffer_);
  return *this;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_BUFFERED_WRITER_H_
