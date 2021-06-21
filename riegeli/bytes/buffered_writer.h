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
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Abstract class `BufferedWriter` helps to implement a `Writer` for an
// underlying destination which accepts data by copying from external byte
// arrays, e.g. like in the `write()` syscall.
//
// `BufferedWriter` accumulates data to be pushed in a flat buffer. Writing a
// large enough array bypasses the buffer.
class BufferedWriter : public Writer {
 public:
  bool PrefersCopying() const override { return true; }

 protected:
  // Creates a closed `BufferedWriter`.
  BufferedWriter() noexcept : Writer(kInitiallyClosed) {}

  // Creates a `BufferedWriter` with the given buffer size and size hint.
  //
  // Size hint is the expected maximum position reached, or `absl::nullopt` if
  // unknown. This avoids allocating a larger buffer than necessary.
  //
  // If the size hint turns out to not match reality, nothing breaks.
  explicit BufferedWriter(
      size_t buffer_size,
      absl::optional<Position> size_hint = absl::nullopt) noexcept;

  BufferedWriter(BufferedWriter&& that) noexcept;
  BufferedWriter& operator=(BufferedWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `BufferedWriter`. This
  // avoids constructing a temporary `BufferedWriter` and moving from it.
  // Derived classes which redefine `Reset()` should include a call to
  // `BufferedWriter::Reset()`.
  void Reset();
  void Reset(size_t buffer_size,
             absl::optional<Position> size_hint = absl::nullopt);

  // Writes data to the destination, to the physical destination position which
  // is `start_pos()`.
  //
  // Increments `start_pos()` by the length written.
  //
  // Preconditions:
  //   `!src.empty()`
  //   `healthy()`
  //   `written_to_buffer() == 0`
  virtual bool WriteInternal(absl::string_view src) = 0;

  bool PushSlow(size_t min_length, size_t recommended_length) override;
  using Writer::WriteSlow;
  bool WriteSlow(absl::string_view src) override;
  bool WriteZerosSlow(Position length) override;
  void WriteHintSlow(size_t length) override;

  // Writes buffered data to the destination, but unlike `PushSlow()`, does not
  // ensure that a buffer is allocated.
  //
  // Postcondition: `written_to_buffer() == 0`
  bool PushInternal();

 private:
  // Minimum length for which it is better to push current contents of `buffer_`
  // and write the data directly than to write the data through `buffer_`.
  size_t LengthToWriteDirectly() const;

  // Invariant: if `is_open()` then `buffer_size_ > 0`
  size_t buffer_size_ = 0;
  Position size_hint_ = 0;
  // Buffered data, to be written directly after the physical destination
  // position which is `start_pos()`.
  Buffer buffer_;
};

// Implementation details follow.

inline BufferedWriter::BufferedWriter(
    size_t buffer_size, absl::optional<Position> size_hint) noexcept
    : Writer(kInitiallyOpen),
      buffer_size_(buffer_size),
      size_hint_(size_hint.value_or(0)) {
  RIEGELI_ASSERT_GT(buffer_size, 0u)
      << "Failed precondition of BufferedWriter::BufferedWriter(size_t): "
         "zero buffer size";
}

inline BufferedWriter::BufferedWriter(BufferedWriter&& that) noexcept
    : Writer(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      buffer_size_(that.buffer_size_),
      size_hint_(that.size_hint_),
      buffer_(std::move(that.buffer_)) {}

inline BufferedWriter& BufferedWriter::operator=(
    BufferedWriter&& that) noexcept {
  Writer::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  buffer_size_ = that.buffer_size_;
  size_hint_ = that.size_hint_;
  buffer_ = std::move(that.buffer_);
  return *this;
}

inline void BufferedWriter::Reset() {
  Writer::Reset(kInitiallyClosed);
  buffer_size_ = 0;
  size_hint_ = 0;
}

inline void BufferedWriter::Reset(size_t buffer_size,
                                  absl::optional<Position> size_hint) {
  RIEGELI_ASSERT_GT(buffer_size, 0u)
      << "Failed precondition of BufferedWriter::Reset(): zero buffer size";
  Writer::Reset(kInitiallyOpen);
  buffer_size_ = buffer_size;
  size_hint_ = size_hint.value_or(0);
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_BUFFERED_WRITER_H_
