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

#include "riegeli/bytes/buffered_reader.h"

#include <stddef.h>
#include <cstring>
#include <utility>

#include "riegeli/base/assert.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/string_view.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

inline bool BufferedReader::TooSmall(Chain::Buffer flat_buffer) const {
  // This is at least as strict as the condition in Chain::Block::wasteful().
  RIEGELI_ASSERT_LE(flat_buffer.size(), buffer_size_);
  return buffer_size_ - flat_buffer.size() > flat_buffer.size();
}

inline Chain::BlockIterator BufferedReader::iter() const {
  RIEGELI_ASSERT(!buffer_.blocks().empty());
  return buffer_.blocks().end() - 1;
}

bool BufferedReader::PullSlow() {
  RIEGELI_ASSERT_EQ(available(), 0u);
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  Chain::Buffer flat_buffer = buffer_.MakeAppendBuffer();
  if (TooSmall(flat_buffer)) {
    // Make a new buffer.
    buffer_.Clear();
    flat_buffer = buffer_.MakeAppendBuffer(buffer_size_, buffer_size_);
    start_ = flat_buffer.data();
    cursor_ = flat_buffer.data();
  }
  RIEGELI_ASSERT(start_ == iter()->data());
  limit_ = flat_buffer.data();
  // Read more data into buffer_.
  const Position pos_before = limit_pos_;
  const bool ok = ReadInternal(flat_buffer.data(), 1, flat_buffer.size());
  RIEGELI_ASSERT_GE(limit_pos_, pos_before);
  const Position length_read = limit_pos_ - pos_before;
  RIEGELI_ASSERT_LE(length_read, flat_buffer.size());
  buffer_.RemoveSuffix(flat_buffer.size() - length_read);
  limit_ += length_read;
  return ok;
}

bool BufferedReader::ReadSlow(char* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, available());
  if (length - available() >= buffer_size_) {
    // If reading through buffer_ would need multiple ReadInternal() calls, it
    // is faster to copy current contents of buffer_ and read the remaining data
    // directly into dest.
    if (RIEGELI_UNLIKELY(!healthy())) return false;
    const size_t available_length = available();
    if (available_length > 0) {  // memcpy(_, nullptr, 0) is undefined.
      std::memcpy(dest, cursor_, available_length);
      dest += available_length;
      length -= available_length;
    }
    ClearBuffer();
    return ReadInternal(dest, length, length);
  }
  return Reader::ReadSlow(dest, length);
}

bool BufferedReader::ReadSlow(Chain* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy()));
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  if (length >= available() && length - available() >= buffer_size_) {
    // If reading through buffer_ would need multiple ReadInternal() calls, it
    // is faster to append current contents of buffer_ and read the remaining
    // data directly into dest.
    //
    // Filling buffer_ first if it is partially filled might be even faster but
    // not necessarily: it would ensure that buffer_ is attached by reference
    // instead of being copied, but it would require additional ReadInternal()
    // calls.
    const size_t available_length = available();
    if (available_length > 0) {  // iter() is undefined if
                                 // buffer_.blocks().empty().
      iter().AppendSubstrTo(string_view(cursor_, available_length), dest);
      length -= available_length;
    }
    ClearBuffer();
    const Chain::Buffer flat_buffer = dest->MakeAppendBuffer(length);
    const Position pos_before = limit_pos_;
    const bool ok = ReadInternal(flat_buffer.data(), length, length);
    RIEGELI_ASSERT_GE(limit_pos_, pos_before);
    const Position length_read = limit_pos_ - pos_before;
    RIEGELI_ASSERT_LE(length_read, length);
    dest->RemoveSuffix(flat_buffer.size() - length_read);
    return ok;
  }

  bool ok = true;
  while (length > available()) {
    Chain::Buffer flat_buffer = buffer_.MakeAppendBuffer();
    if (TooSmall(flat_buffer)) {
      // Append a part of buffer_ to dest and make a new buffer.
      const size_t available_length = available();
      if (available_length > 0) {  // iter() is undefined if
                                   // buffer_.blocks().empty().
        iter().AppendSubstrTo(string_view(cursor_, available_length), dest);
        length -= available_length;
      }
      buffer_.Clear();
      flat_buffer = buffer_.MakeAppendBuffer(buffer_size_, buffer_size_);
      start_ = flat_buffer.data();
      cursor_ = flat_buffer.data();
    }
    RIEGELI_ASSERT(start_ == iter()->data());
    limit_ = flat_buffer.data();
    // Read more data into buffer_.
    const Position pos_before = limit_pos_;
    ok = ReadInternal(flat_buffer.data(), 1, flat_buffer.size());
    RIEGELI_ASSERT_GE(limit_pos_, pos_before);
    const Position length_read = limit_pos_ - pos_before;
    RIEGELI_ASSERT_LE(length_read, flat_buffer.size());
    buffer_.RemoveSuffix(flat_buffer.size() - length_read);
    limit_ += length_read;
    if (RIEGELI_UNLIKELY(!ok)) {
      if (length > available()) {
        length = available();
      } else {
        ok = true;
      }
      break;
    }
  }
  RIEGELI_ASSERT_LE(length, available());
  if (length > 0) {  // iter() is undefined if buffer_.blocks().empty().
    iter().AppendSubstrTo(string_view(cursor_, length), dest);
    cursor_ += length;
  }
  return ok;
}

bool BufferedReader::CopyToSlow(Writer* dest, Position length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy()));
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  bool read_ok = true;
  while (length > available()) {
    Chain::Buffer flat_buffer = buffer_.MakeAppendBuffer();
    if (TooSmall(flat_buffer)) {
      // Write a part of buffer_ to dest and make a new buffer.
      const size_t available_length = available();
      if (available_length > 0) {  // iter() is undefined if
                                   // buffer_.blocks().empty().
        bool write_ok;
        if (available_length == buffer_.size()) {
          write_ok = dest->Write(buffer_);
        } else {
          Chain data;
          iter().AppendSubstrTo(string_view(cursor_, available_length), &data,
                                available_length);
          write_ok = dest->Write(std::move(data));
        }
        if (RIEGELI_UNLIKELY(!write_ok)) {
          cursor_ = limit_;
          return false;
        }
        length -= available_length;
      }
      buffer_.Clear();
      flat_buffer = buffer_.MakeAppendBuffer(buffer_size_, buffer_size_);
      start_ = flat_buffer.data();
      cursor_ = flat_buffer.data();
    }
    RIEGELI_ASSERT(start_ == iter()->data());
    limit_ = flat_buffer.data();
    // Read more data into buffer_.
    const Position pos_before = limit_pos_;
    read_ok = ReadInternal(flat_buffer.data(), 1, flat_buffer.size());
    RIEGELI_ASSERT_GE(limit_pos_, pos_before);
    const Position length_read = limit_pos_ - pos_before;
    RIEGELI_ASSERT_LE(length_read, flat_buffer.size());
    buffer_.RemoveSuffix(flat_buffer.size() - length_read);
    limit_ += length_read;
    if (RIEGELI_UNLIKELY(!read_ok)) {
      if (length > available()) {
        length = available();
      } else {
        read_ok = true;
      }
      break;
    }
  }
  RIEGELI_ASSERT_LE(length, available());
  bool write_ok = true;
  if (length > 0) {  // iter() is undefined if buffer_.blocks().empty().
    if (length == buffer_.size()) {
      write_ok = dest->Write(buffer_);
    } else {
      Chain data;
      iter().AppendSubstrTo(string_view(cursor_, length), &data, length);
      write_ok = dest->Write(std::move(data));
    }
    cursor_ += length;
  }
  return write_ok && read_ok;
}

bool BufferedReader::CopyToSlow(BackwardWriter* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy()));
  if (length <= kMaxBytesToCopy()) {
    char buffer[kMaxBytesToCopy()];
    if (RIEGELI_UNLIKELY(!ReadSlow(buffer, length))) return false;
    return dest->Write(string_view(buffer, length));
  }
  Chain data;
  if (RIEGELI_UNLIKELY(!ReadSlow(&data, length))) return false;
  return dest->Write(std::move(data));
}

void BufferedReader::ClearBuffer() {
  buffer_.Clear();
  Chain::Buffer flat_buffer = buffer_.MakeAppendBuffer();
  start_ = flat_buffer.data();
  cursor_ = flat_buffer.data();
  limit_ = flat_buffer.data();
  buffer_.RemoveSuffix(flat_buffer.size());
}

}  // namespace riegeli
