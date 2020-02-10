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

#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

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
  BufferedReader() noexcept : Reader(kInitiallyClosed) {}

  // Creates a `BufferedReader` with the given buffer size and size hint.
  //
  // Size hint is the expected maximum position reached, or 0 if unknown.
  // This avoids allocating a larger buffer than necessary.
  //
  // If the size hint turns out to not match reality, nothing breaks.
  explicit BufferedReader(size_t buffer_size, Position size_hint = 0) noexcept;

  BufferedReader(BufferedReader&& that) noexcept;
  BufferedReader& operator=(BufferedReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `BufferedReader`. This
  // avoids constructing a temporary `BufferedReader` and moving from it.
  // Derived classes which override `Reset()` should include a call to
  // `BufferedReader::Reset()`.
  void Reset();
  void Reset(size_t buffer_size, Position size_hint = 0);

  bool PullSlow(size_t min_length, size_t recommended_length) override;
  using Reader::ReadSlow;
  bool ReadSlow(char* dest, size_t length) override;
  bool ReadSlow(Chain* dest, size_t length) override;
  using Reader::CopyToSlow;
  bool CopyToSlow(Writer* dest, Position length) override;
  bool CopyToSlow(BackwardWriter* dest, size_t length) override;
  void ReadHintSlow(size_t length) override;

  // Reads data from the source, from the physical source position which is
  // `limit_pos()`.
  //
  // Tries to read at most `max_length`, but can return successfully after
  // reading at least `min_length` if less data was available in the source at
  // the moment.
  //
  // Increments `limit_pos()` by the length read.
  //
  // Preconditions:
  //   `0 < min_length <= max_length`
  //   `healthy()`
  virtual bool ReadInternal(char* dest, size_t min_length,
                            size_t max_length) = 0;

  // Discards buffer contents.
  void ClearBuffer();

  // Changes the size hint after construction.
  void set_size_hint(Position size_hint) { size_hint_ = size_hint; }

 private:
  // Minimum length for which it is better to append current contents of
  // `buffer_` and read the remaining data directly than to read the data
  // through `buffer_`.
  size_t LengthToReadDirectly() const;

  // Invariant: if `!closed()` then `buffer_size_ > 0`
  size_t buffer_size_ = 0;
  Position size_hint_ = 0;
  // Buffered data, read directly before the physical source position which is
  // `limit_pos()`.
  ChainBlock buffer_;

  // Invariants:
  //   if `!buffer_.empty()` then `start() == buffer_.data()`
  //   `buffer_size() == buffer_.size()`
};

// Implementation details follow.

inline BufferedReader::BufferedReader(size_t buffer_size,
                                      Position size_hint) noexcept
    : Reader(kInitiallyOpen), buffer_size_(buffer_size), size_hint_(size_hint) {
  RIEGELI_ASSERT_GT(buffer_size, 0u)
      << "Failed precondition of BufferedReader::BufferedReader(size_t): "
         "zero buffer size";
}

inline BufferedReader::BufferedReader(BufferedReader&& that) noexcept
    : Reader(std::move(that)),
      buffer_size_(that.buffer_size_),
      size_hint_(that.size_hint_),
      buffer_(std::move(that.buffer_)) {}

inline BufferedReader& BufferedReader::operator=(
    BufferedReader&& that) noexcept {
  Reader::operator=(std::move(that));
  buffer_size_ = that.buffer_size_;
  size_hint_ = that.size_hint_;
  buffer_ = std::move(that.buffer_);
  return *this;
}

inline void BufferedReader::Reset() {
  Reader::Reset(kInitiallyClosed);
  buffer_size_ = 0;
  size_hint_ = 0;
  buffer_.Clear();
}

inline void BufferedReader::Reset(size_t buffer_size, Position size_hint) {
  RIEGELI_ASSERT_GT(buffer_size, 0u)
      << "Failed precondition of BufferedReader::Reset(): zero buffer size";
  Reader::Reset(kInitiallyOpen);
  buffer_size_ = buffer_size;
  size_hint_ = size_hint;
  buffer_.Clear();
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_BUFFERED_READER_H_
