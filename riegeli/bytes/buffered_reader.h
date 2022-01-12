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

#ifndef RIEGELI_BYTES_BUFFERED_READER_H_
#define RIEGELI_BYTES_BUFFERED_READER_H_

#include <stddef.h>

#include <utility>

#include "absl/strings/cord.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

class BackwardWriter;
class Writer;

// Abstract class `BufferedReader` helps to implement a `Reader` for an
// underlying source which provides data by copying to external byte arrays,
// e.g. like in the `read()` syscall.
//
// `BufferedReader` accumulates data which has been pulled in a flat buffer.
// Reading a large enough array bypasses the buffer.
class BufferedReader : public Reader {
 public:
  void VerifyEnd() override;

 protected:
  // Creates a closed `BufferedReader`.
  explicit BufferedReader(Closed) noexcept : Reader(kClosed) {}

  // Creates a `BufferedReader` with the given buffer size and size hint.
  //
  // Size hint is the expected maximum position reached, or `absl::nullopt` if
  // unknown. This avoids allocating a larger buffer than necessary.
  //
  // If the size hint turns out to not match reality, nothing breaks.
  //
  // Precondition: `buffer_size > 0`
  explicit BufferedReader(
      size_t buffer_size,
      absl::optional<Position> size_hint = absl::nullopt) noexcept;

  BufferedReader(BufferedReader&& that) noexcept;
  BufferedReader& operator=(BufferedReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `BufferedReader`. This
  // avoids constructing a temporary `BufferedReader` and moving from it.
  // Derived classes which redefine `Reset()` should include a call to
  // `BufferedReader::Reset()`.
  void Reset(Closed);
  void Reset(size_t buffer_size,
             absl::optional<Position> size_hint = absl::nullopt);

  // Returns the buffer size option passed to the constructor.
  size_t buffer_size() const { return buffer_size_; }

  // Changes the size hint after construction.
  void set_size_hint(absl::optional<Position> size_hint);

  // Returns the current size hint, or 0 if it was unset.
  Position size_hint() const { return size_hint_; }

  // `BufferedReader::{Done,SyncImpl}()` seek the source back to the current
  // position if not all buffered data were read. This is feasible only if
  // `SupportsRandomAccess()`.
  //
  // Warning: if `!SupportsRandomAccess()`, the source will have an
  // unpredictable amount of extra data consumed because of buffering.
  //
  // For propagating `{Close,Sync}()` to dependencies, `{Done,SyncImpl}()`
  // should be overridden to call `BufferedReader::{Done,SyncImpl}()` and then
  // close/sync the dependencies.

  // Reads data from the source, from the physical source position which is
  // `limit_pos()`.
  //
  // Tries to read at most `max_length`, but can return successfully after
  // reading at least `min_length` if less data was available in the source at
  // the moment.
  //
  // Does not use buffer pointers. Increments `limit_pos()` by the length read,
  // which must be in the range [`min_length`..`max_length`] on success. Returns
  // `true` on success.
  //
  // Preconditions:
  //   `0 < min_length <= max_length`
  //   `healthy()`
  virtual bool ReadInternal(size_t min_length, size_t max_length,
                            char* dest) = 0;

  // Implementation of `SeekSlow()`, called while no data are buffered.
  //
  // By default it is implemented analogously to the corresponding `Reader`
  // function.
  //
  // Preconditions:
  //   like the corresponding `Reader` function
  //   `start_to_limit() == 0`
  virtual bool SeekBehindBuffer(Position new_pos);

  void Done() override;
  bool PullSlow(size_t min_length, size_t recommended_length) override;
  using Reader::ReadSlow;
  bool ReadSlow(size_t length, char* dest) override;
  bool ReadSlow(size_t length, Chain& dest) override;
  bool ReadSlow(size_t length, absl::Cord& dest) override;
  using Reader::CopySlow;
  bool CopySlow(Position length, Writer& dest) override;
  bool CopySlow(size_t length, BackwardWriter& dest) override;
  void ReadHintSlow(size_t length) override;
  bool SyncImpl(SyncType sync_type) override;
  bool SeekSlow(Position new_pos) override;

 private:
  // Discards buffer contents and sets buffer pointers to `nullptr`.
  //
  // This can move `pos()` forwards to account for skipping over previously
  // buffered data. `limit_pos()` remains unchanged.
  void SyncBuffer();

  // Minimum length for which it is better to append current contents of
  // `buffer_` and read the remaining data directly than to read the data
  // through `buffer_`.
  size_t LengthToReadDirectly() const;

  // Invariant: if `is_open()` then `buffer_size_ > 0`
  size_t buffer_size_ = 0;
  Position size_hint_ = 0;
  // Buffered data, read directly before the physical source position which is
  // `limit_pos()`.
  ChainBlock buffer_;

  // Invariants:
  //   if `!buffer_.empty()` then `start() == buffer_.data()`
  //   `start_to_limit() == buffer_.size()`
};

// Implementation details follow.

inline BufferedReader::BufferedReader(
    size_t buffer_size, absl::optional<Position> size_hint) noexcept
    : buffer_size_(buffer_size), size_hint_(size_hint.value_or(0)) {
  RIEGELI_ASSERT_GT(buffer_size, 0u)
      << "Failed precondition of BufferedReader::BufferedReader(size_t): "
         "zero buffer size";
}

inline BufferedReader::BufferedReader(BufferedReader&& that) noexcept
    : Reader(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      buffer_size_(that.buffer_size_),
      size_hint_(that.size_hint_),
      buffer_(std::move(that.buffer_)) {}

inline BufferedReader& BufferedReader::operator=(
    BufferedReader&& that) noexcept {
  Reader::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  buffer_size_ = that.buffer_size_;
  size_hint_ = that.size_hint_;
  buffer_ = std::move(that.buffer_);
  return *this;
}

inline void BufferedReader::Reset(Closed) {
  Reader::Reset(kClosed);
  buffer_size_ = 0;
  size_hint_ = 0;
  buffer_ = ChainBlock();
}

inline void BufferedReader::Reset(size_t buffer_size,
                                  absl::optional<Position> size_hint) {
  RIEGELI_ASSERT_GT(buffer_size, 0u)
      << "Failed precondition of BufferedReader::Reset(): zero buffer size";
  Reader::Reset();
  buffer_size_ = buffer_size;
  size_hint_ = size_hint.value_or(0);
  buffer_.Clear();
}

inline void BufferedReader::set_size_hint(absl::optional<Position> size_hint) {
  size_hint_ = size_hint.value_or(0);
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_BUFFERED_READER_H_
