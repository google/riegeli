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

#include "absl/base/attributes.h"
#include "absl/strings/cord.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/buffer_options.h"
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
 protected:
  // Creates a closed `BufferedReader`.
  explicit BufferedReader(Closed) noexcept : Reader(kClosed) {}

  explicit BufferedReader(
      const BufferOptions& buffer_options = BufferOptions()) noexcept;

  ABSL_DEPRECATED("Use BufferOptions")
  explicit BufferedReader(
      size_t buffer_size,
      absl::optional<Position> size_hint = absl::nullopt) noexcept
      : BufferedReader(BufferOptions()
                           .set_buffer_size(buffer_size)
                           .set_size_hint(size_hint)) {}

  BufferedReader(BufferedReader&& that) noexcept;
  BufferedReader& operator=(BufferedReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `BufferedReader`. This
  // avoids constructing a temporary `BufferedReader` and moving from it.
  // Derived classes which redefine `Reset()` should include a call to
  // `BufferedReader::Reset()`.
  void Reset(Closed);
  void Reset(const BufferOptions& buffer_options = BufferOptions());
  ABSL_DEPRECATED("Use BufferOptions")
  void Reset(size_t buffer_size,
             absl::optional<Position> size_hint = absl::nullopt) {
    Reset(
        BufferOptions().set_buffer_size(buffer_size).set_size_hint(size_hint));
  }

  // Returns the options passed to the constructor.
  const BufferOptions& buffer_options() const {
    return buffer_sizer_.buffer_options();
  }

  // Provides access to `size_hint()` after construction.
  void set_size_hint(absl::optional<Position> size) {
    buffer_sizer_.set_size_hint(size);
  }
  absl::optional<Position> size_hint() const {
    return buffer_sizer_.size_hint();
  }

  // In derived classes this must be called during initialization if reading
  // starts from a position greater than 0.
  void BeginRun() { buffer_sizer_.BeginRun(start_pos()); }

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
  //   `ok()`
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
  void ReadHintSlow(size_t min_length, size_t recommended_length) override;
  bool SyncImpl(SyncType sync_type) override;
  bool SeekSlow(Position new_pos) override;

  // Reuses `buffer_` as `reader.buffer_` if `reader.pos()` falls inside.
  void ShareBufferTo(BufferedReader& reader) const;

  // Extracts available data from the buffer, leaving it empty.
  //
  // `SaveBuffer()` is meant to be called in `Done()` to preserve pending data
  // across instances reading from the same source.
  ChainBlock SaveBuffer();

  // Restores available data to the buffer.
  //
  // `RestoreBuffer()` is meant to be called in a constructor or `Reset()` with
  // data previously returned by `SaveBuffer()`, to preserve pending data across
  // instances reading from the same source.
  //
  // Precondition: `start_to_limit() == 0`
  void RestoreBuffer(ChainBlock buffer);

 private:
  // Discards buffer contents and sets buffer pointers to `nullptr`.
  //
  // This can move `pos()` forwards to account for skipping over previously
  // buffered data. `limit_pos()` remains unchanged.
  void SyncBuffer();

  BufferSizer buffer_sizer_;
  // Buffered data, read directly before the physical source position which is
  // `limit_pos()`.
  ChainBlock buffer_;

  // Invariants:
  //   if `!buffer_.empty()` then `start() == buffer_.data()`
  //   `start_to_limit() == buffer_.size()`
};

// Implementation details follow.

inline BufferedReader::BufferedReader(
    const BufferOptions& buffer_options) noexcept
    : buffer_sizer_(buffer_options) {}

inline BufferedReader::BufferedReader(BufferedReader&& that) noexcept
    : Reader(static_cast<Reader&&>(that)),
      buffer_sizer_(that.buffer_sizer_),
      buffer_(std::move(that.buffer_)) {}

inline BufferedReader& BufferedReader::operator=(
    BufferedReader&& that) noexcept {
  Reader::operator=(static_cast<Reader&&>(that));
  buffer_sizer_ = that.buffer_sizer_;
  buffer_ = std::move(that.buffer_);
  return *this;
}

inline void BufferedReader::Reset(Closed) {
  Reader::Reset(kClosed);
  buffer_sizer_.Reset();
  buffer_ = ChainBlock();
}

inline void BufferedReader::Reset(const BufferOptions& buffer_options) {
  Reader::Reset();
  buffer_sizer_.Reset(buffer_options);
  buffer_.Clear();
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_BUFFERED_READER_H_
