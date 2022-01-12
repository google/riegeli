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

class Reader;

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
  explicit BufferedWriter(Closed) noexcept : Writer(kClosed) {}

  // Creates a `BufferedWriter` with the given buffer size and size hint.
  //
  // Size hint is the expected maximum position reached, or `absl::nullopt` if
  // unknown. This avoids allocating a larger buffer than necessary.
  //
  // If the size hint turns out to not match reality, nothing breaks.
  //
  // Precondition: `buffer_size > 0`
  explicit BufferedWriter(
      size_t buffer_size,
      absl::optional<Position> size_hint = absl::nullopt) noexcept;

  BufferedWriter(BufferedWriter&& that) noexcept;
  BufferedWriter& operator=(BufferedWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `BufferedWriter`. This
  // avoids constructing a temporary `BufferedWriter` and moving from it.
  // Derived classes which redefine `Reset()` should include a call to
  // `BufferedWriter::Reset()`.
  void Reset(Closed);
  void Reset(size_t buffer_size,
             absl::optional<Position> size_hint = absl::nullopt);

  // Returns the buffer size option passed to the constructor.
  size_t buffer_size() const { return buffer_size_; }

  // `BufferedWriter::{Done,FlushImpl}()` call `{Done,Flush}BehindBuffer()` to
  // write the last piece of data and close/flush the destination.
  //
  // For propagating `{Close,Flush}()` to dependencies, `{Done,FlushImpl}()`
  // should be overridden to call `BufferedWriter::{Done,FlushImpl}()` and then
  // close/flush the dependencies.

  // Implementation of `Done()`, called with the last piece of data.
  //
  // By default calls `FlushBehindBuffer(FlushType::kFromObject)`, which by
  // default writes data to the destination. Can be overridden if writing
  // coupled with closing can be implemented better.
  //
  // Precondition: `start_to_limit() == 0`
  virtual void DoneBehindBuffer(absl::string_view src);

  // Writes data to the destination, to the physical destination position which
  // is `start_pos()`.
  //
  // Does not use buffer pointers. Increments `start_pos()` by the length
  // written, which must be `src.size()` on success. Returns `true` on success.
  //
  // Preconditions:
  //   `!src.empty()`
  //   `healthy()`
  virtual bool WriteInternal(absl::string_view src) = 0;

  // Implementation of `FlushImpl()`, called with the last piece of data.
  //
  // By default writes data to the destination. Can be overridden if writing
  // coupled with flushing can be implemented better.
  //
  // Precondition: `start_to_limit() == 0`
  virtual bool FlushBehindBuffer(absl::string_view src, FlushType flush_type);

  // Implementation of `SeekSlow()`, `SizeImpl()`, `TruncateImpl()`, and
  // `ReadModeImpl()`, called while no data are buffered.
  //
  // By default they are implemented analogously to the corresponding `Writer`
  // functions.
  //
  // Preconditions:
  //   like the corresponding `Writer` functions
  //   `start_to_limit() == 0`
  virtual bool SeekBehindBuffer(Position new_pos);
  virtual absl::optional<Position> SizeBehindBuffer();
  virtual bool TruncateBehindBuffer(Position new_size);
  virtual Reader* ReadModeBehindBuffer(Position initial_pos);

  void Done() override;
  bool PushSlow(size_t min_length, size_t recommended_length) override;
  using Writer::WriteSlow;
  bool WriteSlow(absl::string_view src) override;
  bool WriteZerosSlow(Position length) override;
  bool FlushImpl(FlushType flush_type) override;
  bool SeekSlow(Position new_pos) override;
  absl::optional<Position> SizeImpl() override;
  bool TruncateImpl(Position new_size) override;
  Reader* ReadModeImpl(Position initial_pos) override;

 private:
  bool SyncBuffer();

  // Minimum length for which it is better to push current contents of `buffer_`
  // and write the data directly than to write the data through `buffer_`.
  size_t LengthToWriteDirectly() const;

  // Invariant: if `is_open()` then `buffer_size_ > 0`
  size_t buffer_size_ = 0;
  Position size_hint_ = 0;
  // Contains buffered data, to be written directly after the physical
  // destination position which is `start_pos()`.
  Buffer buffer_;
};

// Implementation details follow.

inline BufferedWriter::BufferedWriter(
    size_t buffer_size, absl::optional<Position> size_hint) noexcept
    : buffer_size_(buffer_size), size_hint_(size_hint.value_or(0)) {
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

inline void BufferedWriter::Reset(Closed) {
  Writer::Reset(kClosed);
  buffer_size_ = 0;
  size_hint_ = 0;
  buffer_ = Buffer();
}

inline void BufferedWriter::Reset(size_t buffer_size,
                                  absl::optional<Position> size_hint) {
  RIEGELI_ASSERT_GT(buffer_size, 0u)
      << "Failed precondition of BufferedWriter::Reset(): zero buffer size";
  Writer::Reset();
  buffer_size_ = buffer_size;
  size_hint_ = size_hint.value_or(0);
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_BUFFERED_WRITER_H_
