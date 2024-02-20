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

#include "absl/functional/function_ref.h"
#include "absl/strings/cord.h"
#include "absl/types/optional.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/base/sized_shared_buffer.h"
#include "riegeli/base/types.h"
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
 public:
  // Derived classes which override `ToleratesReadingAhead()` further should
  // return `true` when `BufferedReader::ToleratesReadingAhead()`, and possibly
  // also in some other cases.
  bool ToleratesReadingAhead() override { return read_all_hint(); }

  bool SupportsSize() override { return exact_size() != absl::nullopt; }

 protected:
  // Creates a closed `BufferedReader`.
  explicit BufferedReader(Closed) noexcept : Reader(kClosed) {}

  explicit BufferedReader(
      BufferOptions buffer_options = BufferOptions()) noexcept;

  BufferedReader(BufferedReader&& that) noexcept;
  BufferedReader& operator=(BufferedReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `BufferedReader`. This
  // avoids constructing a temporary `BufferedReader` and moving from it.
  // Derived classes which redefine `Reset()` should include a call to
  // `BufferedReader::Reset()`.
  void Reset(Closed);
  void Reset(BufferOptions buffer_options = BufferOptions());

  void Done() override;

  // Returns the options passed to the constructor.
  BufferOptions buffer_options() const {
    return buffer_sizer_.buffer_options();
  }

  // Storage for the hint set by `Reader::SetReadAllHint()`.
  //
  // If `true` and `exact_size()` is not `absl::nullptr`, this causes larger
  // buffer sizes to be used before reaching `*exact_size()`.
  bool read_all_hint() const { return buffer_sizer_.read_all_hint(); }

  // Storage for an exact size of the source, as discovered by the `Reader`
  // itself.
  //
  // If not `absl::nullptr` and `read_all_hint()` is `true`, this causes larger
  // buffer sizes to be used before reaching `*exact_size()`.
  //
  // Also, if not `absl::nullptr`, this causes a smaller buffer size to be used
  // when reaching `*exact_size()`.
  void set_exact_size(absl::optional<Position> exact_size) {
    buffer_sizer_.set_exact_size(exact_size);
  }
  absl::optional<Position> exact_size() const {
    return buffer_sizer_.exact_size();
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

  // Copies data from the source, from the physical source position which is
  // `limit_pos()`, to `dest`.
  //
  // Does not use buffer pointers of `*this`. Increments `limit_pos()` by the
  // length read, which must be `length` on success. Returns `true` on success.
  //
  // By default uses `Writer::Push()` and `ReadInternal()`.
  //
  // Preconditions:
  //   `length > 0`
  //   `ok()`
  virtual bool CopyInternal(Position length, Writer& dest);

  // Called when `exact_size()` was reached but reading more is requested.
  // In this case `ReadInternal()` was not called.
  //
  // By default does nothing. This can be overridden e.g. to ensure that a
  // compressed stream is fully consumed after decompressing all data.
  virtual void ExactSizeReached();

  // Implementation of `SeekSlow()`, called while no data are buffered.
  //
  // By default it is implemented analogously to the corresponding `Reader`
  // function.
  //
  // Preconditions:
  //   like the corresponding `Reader` function
  //   `start_to_limit() == 0`
  virtual bool SeekBehindBuffer(Position new_pos);

  void SetReadAllHintImpl(bool read_all_hint) override;
  bool PullSlow(size_t min_length, size_t recommended_length) override;
  using Reader::ReadSlow;
  bool ReadSlow(size_t length, char* dest) override;
  bool ReadSlow(size_t length, Chain& dest) override;
  bool ReadSlow(size_t length, absl::Cord& dest) override;
  using Reader::CopySlow;
  bool CopySlow(Position length, Writer& dest) override;
  bool CopySlow(size_t length, BackwardWriter& dest) override;
  using Reader::ReadOrPullSomeSlow;
  bool ReadOrPullSomeSlow(size_t max_length,
                          absl::FunctionRef<char*(size_t&)> get_dest) override;
  void ReadHintSlow(size_t min_length, size_t recommended_length) override;
  bool SyncImpl(SyncType sync_type) override;
  bool SeekSlow(Position new_pos) override;
  absl::optional<Position> SizeImpl() override;

  // Reuses `buffer_` as `reader.buffer_` if `reader.pos()` falls inside.
  void ShareBufferTo(BufferedReader& reader) const;

  // Extracts available data from the buffer, leaving it empty.
  //
  // `SaveBuffer()` is meant to be called in `Done()` to preserve pending data
  // across instances reading from the same source.
  SizedSharedBuffer SaveBuffer();

  // Restores available data to the buffer.
  //
  // `RestoreBuffer()` is meant to be called in a constructor or `Reset()` with
  // data previously returned by `SaveBuffer()`, to preserve pending data across
  // instances reading from the same source.
  //
  // Precondition: `start_to_limit() == 0`
  void RestoreBuffer(SizedSharedBuffer buffer);

 private:
  // Discards buffer contents and sets buffer pointers to `nullptr`.
  //
  // This can move `pos()` forwards to account for skipping over previously
  // buffered data. `limit_pos()` remains unchanged.
  void SyncBuffer();

  // Implementation of `CopySlow(Writer&)` in terms of `Writer::Push()` and
  // `ReadInternal()`. Does not use buffer pointers.
  //
  // Precondition: `length > 0`
  bool CopyUsingPush(Position length, Writer& dest);

  ReadBufferSizer buffer_sizer_;
  // Buffered data, read directly before the physical source position which is
  // `limit_pos()`.
  SizedSharedBuffer buffer_;

  // Invariants:
  //   if `!buffer_.empty()` then `start() == buffer_.data()`
  //   `start_to_limit() == buffer_.size()`
};

// Implementation details follow.

inline BufferedReader::BufferedReader(BufferOptions buffer_options) noexcept
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
  buffer_ = SizedSharedBuffer();
}

inline void BufferedReader::Reset(BufferOptions buffer_options) {
  Reader::Reset();
  buffer_sizer_.Reset(buffer_options);
  buffer_.Clear();
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_BUFFERED_READER_H_
