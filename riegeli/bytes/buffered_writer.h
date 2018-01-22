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
#include <memory>
#include <utility>

#include "riegeli/base/base.h"
#include "riegeli/base/string_view.h"
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
  BufferedWriter() : buffer_size_(0) {}

  explicit BufferedWriter(size_t buffer_size) : buffer_size_(buffer_size) {}

  BufferedWriter(BufferedWriter&& src) noexcept;
  BufferedWriter& operator=(BufferedWriter&& src) noexcept;

  ~BufferedWriter();

  // BufferedWriter provides a partial override of Writer::Done().
  // Derived classes must override it further and include a call to
  // BufferedWriter::Done().
  virtual void Done() override = 0;

  bool PushSlow() override;
  bool WriteSlow(string_view src) override;

  // Writes buffered data to the destination, but unlike PushSlow(), does not
  // ensure that a buffer is allocated.
  bool PushInternal();

  // Writes data to the destination, to the physical destination position which
  // is start_pos_.
  //
  // Increments start_pos_ by the length written.
  //
  // Preconditions:
  //   !src.empty()
  //   healthy()
  virtual bool WriteInternal(string_view src) = 0;

  // The size of the buffer that start_ points to, once it is allocated.
  //
  // The buffer is allocated with std::allocator<char>().
  //
  // Invariant:
  //   limit_ == start_ + (start_ == nullptr || !healthy() ? 0 : buffer_size_)
  size_t buffer_size_;
};

// Implementation details follow.

inline BufferedWriter::BufferedWriter(BufferedWriter&& src) noexcept
    : Writer(std::move(src)),
      buffer_size_(riegeli::exchange(src.buffer_size_, 0)) {}

inline BufferedWriter& BufferedWriter::operator=(
    BufferedWriter&& src) noexcept {
  // Exchange src.start_ early to support self-assignment.
  char* const start = riegeli::exchange(src.start_, nullptr);
  if (start_ != nullptr) {
    std::allocator<char>().deallocate(start_, buffer_size_);
  }
  Writer::operator=(std::move(src));
  start_ = start;
  buffer_size_ = riegeli::exchange(src.buffer_size_, 0);
  return *this;
}

inline BufferedWriter::~BufferedWriter() {
  if (start_ != nullptr) {
    std::allocator<char>().deallocate(start_, buffer_size_);
  }
}

inline void BufferedWriter::Done() {
  if (start_ != nullptr) {
    std::allocator<char>().deallocate(start_, buffer_size_);
  }
  buffer_size_ = 0;
  Writer::Done();
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_BUFFERED_WRITER_H_
