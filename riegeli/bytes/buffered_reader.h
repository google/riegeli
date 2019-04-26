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

#include "absl/utility/utility.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// BufferedReader helps to implement a Reader for an underlying source which
// provides data by copying to external byte arrays, e.g. like in the read
// syscall.
//
// BufferedReader accumulates data which has been pulled in a flat buffer.
// Reading a large enough array bypasses the buffer.
class BufferedReader : public Reader {
 protected:
  // Creates a closed BufferedReader.
  BufferedReader() noexcept : Reader(State::kClosed) {}

  // Creates a BufferedReader with the given buffer size and size hint.
  //
  // Size hint is the expected maximum position reached, or 0 if unknown.
  // This avoids allocating a larger buffer than necessary.
  //
  // If the size hint turns out to not match reality, nothing breaks.
  explicit BufferedReader(size_t buffer_size, Position size_hint = 0) noexcept;

  BufferedReader(BufferedReader&& that) noexcept;
  BufferedReader& operator=(BufferedReader&& that) noexcept;

  void VerifyEnd() override;
  bool PullSlow() override;
  using Reader::ReadSlow;
  bool ReadSlow(char* dest, size_t length) override;
  bool ReadSlow(Chain* dest, size_t length) override;
  using Reader::CopyToSlow;
  bool CopyToSlow(Writer* dest, Position length) override;
  bool CopyToSlow(BackwardWriter* dest, size_t length) override;

  // Reads data from the source, from the physical source position which is
  // limit_pos_.
  //
  // Tries to read at most max_length, but can return successfully after reading
  // at least min_length if less data was available in the source at the moment.
  //
  // Increments limit_pos_ by the length read.
  //
  // Preconditions:
  //   0 < min_length <= max_length
  //   healthy()
  virtual bool ReadInternal(char* dest, size_t min_length,
                            size_t max_length) = 0;

  // Discards buffer contents.
  void ClearBuffer();

  // Changes the size hint after construction.
  void set_size_hint(Position size_hint) { size_hint_ = size_hint; }

 private:
  // The size of buffer_ to use for the next allocation.
  size_t next_buffer_size() const;

  // Minimum length for which it is better to append current contents of buffer_
  // and read the remaining data directly than to read the data through buffer_.
  size_t LengthToReadDirectly() const;

  // Returns true if flat_buffer_size is considered too small to continue
  // reading into it and a new buffer should be allocated instead.
  bool TooSmall(size_t flat_buffer_size) const;

  // Iterator pointing to the block of buffer_ which holds the actual data.
  //
  // Precondition: buffer_.blocks().size() == 1
  Chain::BlockIterator iter() const;

  size_t buffer_size_ = 0;
  Position size_hint_ = 0;
  // Buffered data, read directly before the physical source position which is
  // limit_pos_.
  Chain buffer_;

  // Invariants:
  //   start_ == nullptr ? buffer_.empty() : start_ == iter()->data()
  //   buffer_size() == (start_ == nullptr ? 0 : iter()->size())
};

// Implementation details follow.

inline BufferedReader::BufferedReader(size_t buffer_size,
                                      Position size_hint) noexcept
    : Reader(State::kOpen), buffer_size_(buffer_size), size_hint_(size_hint) {
  RIEGELI_ASSERT_GT(buffer_size, 0u)
      << "Failed precondition of BufferedReader::BufferedReader(size_t): "
         "zero buffer size";
}

inline BufferedReader::BufferedReader(BufferedReader&& that) noexcept
    : Reader(std::move(that)),
      buffer_size_(absl::exchange(that.buffer_size_, 0)),
      size_hint_(absl::exchange(that.size_hint_, 0)),
      buffer_(absl::exchange(that.buffer_, Chain())) {}

inline BufferedReader& BufferedReader::operator=(
    BufferedReader&& that) noexcept {
  Reader::operator=(std::move(that));
  buffer_size_ = absl::exchange(that.buffer_size_, 0);
  size_hint_ = absl::exchange(that.size_hint_, 0);
  buffer_ = absl::exchange(that.buffer_, Chain());
  return *this;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_BUFFERED_READER_H_
