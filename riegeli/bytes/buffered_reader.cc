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
#include <limits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

inline bool BufferedReader::TooSmall(size_t flat_buffer_size) const {
  // This is at least as strict as the condition in Chain::Block::wasteful().
  const size_t buffer_size = UnsignedMax(buffer_size_, buffer_.size());
  RIEGELI_ASSERT_LE(flat_buffer_size, buffer_size)
      << "Failed precondition of BufferedReader::TooSmall(): "
         "flat buffer larger than buffer size";
  return buffer_size - flat_buffer_size > flat_buffer_size;
}

inline Chain::BlockIterator BufferedReader::iter() const {
  RIEGELI_ASSERT_EQ(buffer_.blocks().size(), 1u)
      << "Failed precondition of BufferedReader::iter(): single block expected";
  return buffer_.blocks().begin();
}

bool BufferedReader::PullSlow() {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of Reader::PullSlow(): "
         "data available, use Pull() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  absl::Span<char> flat_buffer = buffer_.AppendBuffer();
  if (TooSmall(flat_buffer.size())) {
    // Make a new buffer.
    RIEGELI_ASSERT_GT(buffer_size_, 0u)
        << "Failed invariant of BufferedReader: no buffer size specified";
    buffer_.Clear();
    flat_buffer = buffer_.AppendBuffer(buffer_size_, 0, buffer_size_);
    start_ = flat_buffer.data();
    cursor_ = start_;
  } else if (start_ == nullptr) {
    // buffer_ was empty and buffer_.AppendBuffer() returned short data.
    start_ = iter()->data();
    cursor_ = start_;
  }
  RIEGELI_ASSERT(start_ == iter()->data())
      << "Failed invariant of BufferedReader: "
         "buffer pointer does not point to buffer block";
  limit_ = flat_buffer.data();
  // Read more data into buffer_.
  const Position pos_before = limit_pos_;
  const bool ok = ReadInternal(flat_buffer.data(), 1, flat_buffer.size());
  RIEGELI_ASSERT_GE(limit_pos_, pos_before)
      << "BufferedReader::ReadInternal() decreased limit_pos_";
  const Position length_read = limit_pos_ - pos_before;
  RIEGELI_ASSERT_LE(length_read, flat_buffer.size())
      << "BufferedReader::ReadInternal() read more than requested";
  buffer_.RemoveSuffix(flat_buffer.size() - IntCast<size_t>(length_read));
  limit_ += length_read;
  return ok;
}

bool BufferedReader::ReadSlow(char* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, available())
      << "Failed precondition of Reader::ReadSlow(char*): "
         "length too small, use Read(char*) instead";
  if (length - available() >= buffer_size_) {
    // If reading through buffer_ would need multiple ReadInternal() calls, it
    // is faster to copy current contents of buffer_ and read the remaining data
    // directly into dest.
    if (ABSL_PREDICT_FALSE(!healthy())) return false;
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
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy()))
      << "Failed precondition of Reader::ReadSlow(Chain*): "
         "length too small, use Read(Chain*) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Reader::ReadSlow(Chain*): "
         "Chain size overflow";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
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
                                 // buffer_.blocks().size() != 1.
      iter().AppendSubstrTo(absl::string_view(cursor_, available_length), dest);
      length -= available_length;
    }
    ClearBuffer();
    const absl::Span<char> flat_buffer = dest->AppendBuffer(length);
    const Position pos_before = limit_pos_;
    const bool ok = ReadInternal(flat_buffer.data(), length, length);
    RIEGELI_ASSERT_GE(limit_pos_, pos_before)
        << "BufferedReader::ReadInternal() decreased limit_pos_";
    const Position length_read = limit_pos_ - pos_before;
    RIEGELI_ASSERT_LE(length_read, length)
        << "BufferedReader::ReadInternal() read more than requested";
    dest->RemoveSuffix(flat_buffer.size() - IntCast<size_t>(length_read));
    return ok;
  }

  bool ok = true;
  while (length > available()) {
    absl::Span<char> flat_buffer = buffer_.AppendBuffer();
    if (TooSmall(flat_buffer.size())) {
      // Append a part of buffer_ to dest and make a new buffer.
      RIEGELI_ASSERT_GT(buffer_size_, 0u)
          << "Failed invariant of BufferedReader: no buffer size specified";
      const size_t available_length = available();
      if (available_length > 0) {  // iter() is undefined if
                                   // buffer_.blocks().size() != 1.
        iter().AppendSubstrTo(absl::string_view(cursor_, available_length),
                              dest);
        length -= available_length;
      }
      buffer_.Clear();
      flat_buffer = buffer_.AppendBuffer(buffer_size_, 0, buffer_size_);
      start_ = flat_buffer.data();
      cursor_ = start_;
    } else if (start_ == nullptr) {
      // buffer_ was empty and buffer_.AppendBuffer() returned short data.
      start_ = iter()->data();
      cursor_ = start_;
    }
    RIEGELI_ASSERT(start_ == iter()->data())
        << "Failed invariant of BufferedReader: "
           "buffer pointer does not point to buffer block";
    limit_ = flat_buffer.data();
    // Read more data into buffer_.
    const Position pos_before = limit_pos_;
    ok = ReadInternal(flat_buffer.data(), 1, flat_buffer.size());
    RIEGELI_ASSERT_GE(limit_pos_, pos_before)
        << "BufferedReader::ReadInternal() decreased limit_pos_";
    const Position length_read = limit_pos_ - pos_before;
    RIEGELI_ASSERT_LE(length_read, flat_buffer.size())
        << "BufferedReader::ReadInternal() read more than requested";
    buffer_.RemoveSuffix(flat_buffer.size() - IntCast<size_t>(length_read));
    limit_ += length_read;
    if (ABSL_PREDICT_FALSE(!ok)) {
      if (length > available()) {
        length = available();
      } else {
        ok = true;
      }
      break;
    }
  }
  RIEGELI_ASSERT_LE(length, available())
      << "Bug in BufferedReader::ReadSlow(Chain*): "
         "remaining length larger than available data";
  if (length > 0) {  // iter() is undefined if buffer_.blocks().size() != 1.
    iter().AppendSubstrTo(absl::string_view(cursor_, length), dest);
    cursor_ += length;
  }
  return ok;
}

bool BufferedReader::CopyToSlow(Writer* dest, Position length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy()))
      << "Failed precondition of Reader::CopyToSlow(Writer*): "
         "length too small, use CopyTo(Writer*) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  bool read_ok = true;
  while (length > available()) {
    absl::Span<char> flat_buffer = buffer_.AppendBuffer();
    if (TooSmall(flat_buffer.size())) {
      // Write a part of buffer_ to dest and make a new buffer.
      RIEGELI_ASSERT_GT(buffer_size_, 0u)
          << "Failed invariant of BufferedReader: no buffer size specified";
      const size_t available_length = available();
      if (available_length > 0) {  // iter() is undefined if
                                   // buffer_.blocks().size() != 1.
        bool write_ok;
        if (available_length == buffer_.size()) {
          write_ok = dest->Write(buffer_);
        } else {
          Chain data;
          iter().AppendSubstrTo(absl::string_view(cursor_, available_length),
                                &data, available_length);
          write_ok = dest->Write(std::move(data));
        }
        if (ABSL_PREDICT_FALSE(!write_ok)) {
          cursor_ = limit_;
          return false;
        }
        length -= available_length;
      }
      buffer_.Clear();
      flat_buffer = buffer_.AppendBuffer(buffer_size_, 0, buffer_size_);
      start_ = flat_buffer.data();
      cursor_ = start_;
    } else if (start_ == nullptr) {
      // buffer_ was empty and buffer_.AppendBuffer() returned short data.
      start_ = iter()->data();
      cursor_ = start_;
    }
    RIEGELI_ASSERT(start_ == iter()->data())
        << "Failed invariant of BufferedReader: "
           "buffer pointer does not point to buffer block";
    limit_ = flat_buffer.data();
    // Read more data into buffer_.
    const Position pos_before = limit_pos_;
    read_ok = ReadInternal(flat_buffer.data(), 1, flat_buffer.size());
    RIEGELI_ASSERT_GE(limit_pos_, pos_before)
        << "BufferedReader::ReadInternal() decreased limit_pos_";
    const Position length_read = limit_pos_ - pos_before;
    RIEGELI_ASSERT_LE(length_read, flat_buffer.size())
        << "BufferedReader::ReadInternal() read more than requested";
    buffer_.RemoveSuffix(flat_buffer.size() - IntCast<size_t>(length_read));
    limit_ += length_read;
    if (ABSL_PREDICT_FALSE(!read_ok)) {
      if (length > available()) {
        length = available();
      } else {
        read_ok = true;
      }
      break;
    }
  }
  RIEGELI_ASSERT_LE(length, available())
      << "Bug in BufferedReader::CopyToSlow(Writer*): "
         "remaining length larger than available data";
  bool write_ok = true;
  if (length > 0) {  // iter() is undefined if buffer_.blocks().size() != 1.
    if (length == buffer_.size()) {
      write_ok = dest->Write(buffer_);
    } else {
      Chain data;
      iter().AppendSubstrTo(absl::string_view(cursor_, length), &data, length);
      write_ok = dest->Write(std::move(data));
    }
    cursor_ += length;
  }
  return write_ok && read_ok;
}

bool BufferedReader::CopyToSlow(BackwardWriter* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy()))
      << "Failed precondition of Reader::CopyToSlow(Writer*): "
         "length too small, use CopyTo(Writer*) instead";
  if (length <= kMaxBytesToCopy()) {
    char buffer[kMaxBytesToCopy()];
    if (ABSL_PREDICT_FALSE(!ReadSlow(buffer, length))) return false;
    return dest->Write(absl::string_view(buffer, length));
  }
  Chain data;
  if (ABSL_PREDICT_FALSE(!ReadSlow(&data, length))) return false;
  return dest->Write(std::move(data));
}

void BufferedReader::ClearBuffer() {
  buffer_.Clear();
  absl::Span<char> flat_buffer = buffer_.AppendBuffer();
  start_ = flat_buffer.data();
  cursor_ = start_;
  limit_ = start_;
  buffer_.RemoveSuffix(flat_buffer.size());
}

}  // namespace riegeli
