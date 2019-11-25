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

inline size_t BufferedReader::BufferLength(size_t min_length) const {
  RIEGELI_ASSERT_GT(buffer_size_, 0u)
      << "Failed invariant of BufferedReader: no buffer size specified";
  size_t length = buffer_size_;
  if (limit_pos() < size_hint_) {
    // Avoid allocating more than needed for `size_hint_`.
    length = UnsignedMin(length, size_hint_ - limit_pos());
  }
  return UnsignedMax(length, min_length);
}

inline size_t BufferedReader::LengthToReadDirectly() const {
  // Read directly if reading through `buffer_` would need more than one read,
  // or if `buffer_` would be full. Read directly also if `size_hint_` is
  // reached.
  return SaturatingAdd(available(), BufferLength());
}

void BufferedReader::VerifyEnd() {
  // No more data are expected, so allocate a minimal non-empty buffer for
  // verifying that.
  set_size_hint(SaturatingAdd(pos(), Position{1}));
  Reader::VerifyEnd();
}

bool BufferedReader::PullSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_GT(min_length, available())
      << "Failed precondition of Reader::PullSlow(): "
         "length too small, use Pull() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  absl::Span<char> flat_buffer = buffer_.AppendBuffer(0);
  if (flat_buffer.size() < min_length - available()) {
    // Make a new buffer, preserving available data.
    const size_t available_length = available();
    buffer_.RemoveSuffix(flat_buffer.size());
    buffer_.RemovePrefix(buffer_.size() - available_length);
    flat_buffer = buffer_.AppendBuffer(BufferLength(min_length));
    set_buffer(buffer_.data(), PtrDistance(buffer_.data(), flat_buffer.data()));
  } else if (flat_buffer.size() == buffer_.size()) {
    // `buffer_` was empty.
    set_buffer(buffer_.data());
  }
  RIEGELI_ASSERT(start() == buffer_.data())
      << "Bug in BufferedReader::PullSlow(): "
         "start() does not point to buffer_";
  RIEGELI_ASSERT(limit() == flat_buffer.data())
      << "Bug in BufferedReader::PullSlow(): "
         "limit() does not point to flat_buffer";
  // Read more data into `buffer_`.
  const Position pos_before = limit_pos();
  const bool ok = ReadInternal(flat_buffer.data(), min_length - available(),
                               flat_buffer.size());
  RIEGELI_ASSERT_GE(limit_pos(), pos_before)
      << "BufferedReader::ReadInternal() decreased limit_pos()";
  const Position length_read = limit_pos() - pos_before;
  RIEGELI_ASSERT_LE(length_read, flat_buffer.size())
      << "BufferedReader::ReadInternal() read more than requested";
  buffer_.RemoveSuffix(flat_buffer.size() - IntCast<size_t>(length_read));
  // Increment `limit()` by `length_read`.
  set_buffer(start(), buffer_size() + IntCast<size_t>(length_read),
             read_from_buffer());
  return ok;
}

bool BufferedReader::ReadSlow(char* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, available())
      << "Failed precondition of Reader::ReadSlow(char*): "
         "length too small, use Read(char*) instead";
  if (length >= LengthToReadDirectly()) {
    const size_t available_length = available();
    if (
        // `std::memcpy(_, nullptr, 0)` is undefined.
        available_length > 0) {
      std::memcpy(dest, cursor(), available_length);
      dest += available_length;
      length -= available_length;
    }
    ClearBuffer();
    if (ABSL_PREDICT_FALSE(!healthy())) return false;
    return ReadInternal(dest, length, length);
  }
  return Reader::ReadSlow(dest, length);
}

bool BufferedReader::ReadSlow(Chain* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::ReadSlow(Chain*): "
         "length too small, use Read(Chain*) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Reader::ReadSlow(Chain*): "
         "Chain size overflow";
  bool enough_read = true;
  bool ok = healthy();
  while (length > available()) {
    if (ABSL_PREDICT_FALSE(!ok)) {
      // Read as much as is available.
      length = available();
      enough_read = false;
      break;
    }
    if (available() == 0 && length >= LengthToReadDirectly()) {
      ClearBuffer();
      const absl::Span<char> flat_buffer = dest->AppendFixedBuffer(length);
      const Position pos_before = limit_pos();
      if (ABSL_PREDICT_FALSE(!ReadInternal(
              flat_buffer.data(), flat_buffer.size(), flat_buffer.size()))) {
        RIEGELI_ASSERT_GE(limit_pos(), pos_before)
            << "BufferedReader::ReadInternal() decreased limit_pos()";
        const Position length_read = limit_pos() - pos_before;
        RIEGELI_ASSERT_LE(length_read, flat_buffer.size())
            << "BufferedReader::ReadInternal() read more than requested";
        dest->RemoveSuffix(flat_buffer.size() - IntCast<size_t>(length_read));
        return false;
      }
      return true;
    }
    absl::Span<char> flat_buffer = buffer_.AppendBuffer(0);
    if (flat_buffer.empty()) {
      // Append a part of `buffer_` to `*dest` and make a new buffer.
      const size_t available_length = available();
      buffer_.AppendSubstrTo(absl::string_view(cursor(), available_length),
                             dest);
      length -= available_length;
      buffer_.Clear();
      flat_buffer = buffer_.AppendBuffer(BufferLength());
      set_buffer(flat_buffer.data());
    } else if (flat_buffer.size() == buffer_.size()) {
      // `buffer_` was empty.
      set_buffer(buffer_.data());
    }
    RIEGELI_ASSERT(start() == buffer_.data())
        << "Bug in BufferedReader::ReadSlow(Chain*): "
           "start() does not point to buffer_";
    RIEGELI_ASSERT(limit() == flat_buffer.data())
        << "Bug in BufferedReader::ReadSlow(Chain*): "
           "limit() does not point to flat_buffer";
    // Read more data into `buffer_`.
    const Position pos_before = limit_pos();
    ok = ReadInternal(flat_buffer.data(), 1, flat_buffer.size());
    RIEGELI_ASSERT_GE(limit_pos(), pos_before)
        << "BufferedReader::ReadInternal() decreased limit_pos()";
    const Position length_read = limit_pos() - pos_before;
    RIEGELI_ASSERT_LE(length_read, flat_buffer.size())
        << "BufferedReader::ReadInternal() read more than requested";
    buffer_.RemoveSuffix(flat_buffer.size() - IntCast<size_t>(length_read));
    // Increment `limit()` by `length_read`.
    set_buffer(start(), buffer_size() + IntCast<size_t>(length_read),
               read_from_buffer());
  }
  buffer_.AppendSubstrTo(absl::string_view(cursor(), length), dest);
  move_cursor(length);
  return enough_read;
}

bool BufferedReader::CopyToSlow(Writer* dest, Position length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::CopyToSlow(Writer*): "
         "length too small, use CopyTo(Writer*) instead";
  bool enough_read = true;
  bool read_ok = healthy();
  while (length > available()) {
    if (ABSL_PREDICT_FALSE(!read_ok)) {
      // Copy as much as is available.
      length = available();
      enough_read = false;
      break;
    }
    absl::Span<char> flat_buffer = buffer_.AppendBuffer(0);
    if (flat_buffer.empty()) {
      // Write a part of `buffer_` to `*dest` and make a new buffer.
      const size_t available_length = available();
      if (available_length > 0) {
        bool write_ok;
        if (available_length == buffer_.size()) {
          write_ok = dest->Write(Chain(buffer_));
        } else {
          Chain data;
          buffer_.AppendSubstrTo(absl::string_view(cursor(), available_length),
                                 &data, available_length);
          write_ok = dest->Write(std::move(data));
        }
        if (ABSL_PREDICT_FALSE(!write_ok)) {
          move_cursor(available_length);
          return false;
        }
        length -= available_length;
      }
      buffer_.Clear();
      flat_buffer = buffer_.AppendBuffer(BufferLength());
      set_buffer(flat_buffer.data());
    } else if (flat_buffer.size() == buffer_.size()) {
      // `buffer_` was empty.
      set_buffer(buffer_.data());
    }
    RIEGELI_ASSERT(start() == buffer_.data())
        << "Bug in BufferedReader::CopyToSlow(Writer*): "
           "start() does not point to buffer_";
    RIEGELI_ASSERT(limit() == flat_buffer.data())
        << "Bug in BufferedReader::CopyToSlow(Writer*): "
           "limit() does not point to flat_buffer";
    // Read more data into `buffer_`.
    const Position pos_before = limit_pos();
    read_ok = ReadInternal(flat_buffer.data(), 1, flat_buffer.size());
    RIEGELI_ASSERT_GE(limit_pos(), pos_before)
        << "BufferedReader::ReadInternal() decreased limit_pos()";
    const Position length_read = limit_pos() - pos_before;
    RIEGELI_ASSERT_LE(length_read, flat_buffer.size())
        << "BufferedReader::ReadInternal() read more than requested";
    buffer_.RemoveSuffix(flat_buffer.size() - IntCast<size_t>(length_read));
    // Increment `limit()` by `length_read`.
    set_buffer(start(), buffer_size() + IntCast<size_t>(length_read),
               read_from_buffer());
  }
  bool write_ok = true;
  if (length > 0) {
    if (length == buffer_.size()) {
      write_ok = dest->Write(Chain(buffer_));
    } else {
      Chain data;
      buffer_.AppendSubstrTo(
          absl::string_view(cursor(), IntCast<size_t>(length)), &data,
          IntCast<size_t>(length));
      write_ok = dest->Write(std::move(data));
    }
    move_cursor(IntCast<size_t>(length));
  }
  return write_ok && enough_read;
}

bool BufferedReader::CopyToSlow(BackwardWriter* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::CopyToSlow(BackwardWriter*): "
         "length too small, use CopyTo(BackwardWriter*) instead";
  if (length <= kMaxBytesToCopy) {
    if (ABSL_PREDICT_FALSE(!dest->Push(length))) return false;
    dest->move_cursor(length);
    if (ABSL_PREDICT_FALSE(!ReadSlow(dest->cursor(), length))) {
      dest->set_cursor(dest->cursor() + length);
      return false;
    }
    return true;
  }
  Chain data;
  if (ABSL_PREDICT_FALSE(!ReadSlow(&data, length))) return false;
  return dest->Write(std::move(data));
}

void BufferedReader::ClearBuffer() {
  buffer_.Clear();
  set_buffer();
}

}  // namespace riegeli
