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
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void BufferedReader::Done() {
  if (available() > 0) {
    if (!SupportsRandomAccess()) {
      // Seeking back is not feasible.
      Reader::Done();
      buffer_ = ChainBlock();
      return;
    }
    const Position new_pos = pos();
    set_buffer();
    SeekBehindBuffer(new_pos);
  }
  Reader::Done();
  buffer_ = ChainBlock();
}

void BufferedReader::VerifyEnd() {
  // No more data are expected, so allocate a minimum non-empty buffer for
  // verifying that.
  set_size_hint(SaturatingAdd(pos(), Position{1}));
  Reader::VerifyEnd();
}

inline void BufferedReader::SyncBuffer() {
  set_buffer();
  buffer_.Clear();
}

inline size_t BufferedReader::LengthToReadDirectly() const {
  // Read directly at least `buffer_size_` of data. Even if the buffer is
  // partially full, this ensures that at least every other read has length at
  // least `buffer_size_`.
  if (pos() < size_hint_) {
    // Read directly also if `size_hint_` is reached.
    return UnsignedMin(buffer_size_, size_hint_ - pos());
  }
  return buffer_size_;
}

bool BufferedReader::PullSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Reader::PullSlow(): "
         "enough data available, use Pull() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  const size_t available_length = available();
  size_t cursor_index = start_to_cursor();
  const size_t buffer_length =
      BufferLength(min_length, buffer_size_, size_hint_, pos());
  absl::Span<char> flat_buffer = buffer_.AppendBuffer(
      0, buffer_length - available_length,
      SaturatingAdd(buffer_length, buffer_length) - available_length);
  if (flat_buffer.size() < min_length - available_length) {
    // `flat_buffer` is too small. Resize `buffer_`, keeping available data.
    buffer_.RemoveSuffix(flat_buffer.size());
    buffer_.RemovePrefix(cursor_index);
    cursor_index = 0;
    flat_buffer = buffer_.AppendBuffer(
        buffer_length - available_length, buffer_length - available_length,
        SaturatingAdd(buffer_length, buffer_length) - available_length);
  }
  // Read more data into `buffer_`.
  const Position pos_before = limit_pos();
  const bool ok = ReadInternal(min_length - available_length,
                               flat_buffer.size(), flat_buffer.data());
  RIEGELI_ASSERT_GE(limit_pos(), pos_before)
      << "BufferedReader::ReadInternal() decreased limit_pos()";
  const Position length_read = limit_pos() - pos_before;
  RIEGELI_ASSERT_LE(length_read, flat_buffer.size())
      << "BufferedReader::ReadInternal() read more than requested";
  buffer_.RemoveSuffix(flat_buffer.size() - IntCast<size_t>(length_read));
  set_buffer(buffer_.data(), buffer_.size(), cursor_index);
  return ok;
}

bool BufferedReader::SeekBehindBuffer(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of BufferedReader::SeekBehindBuffer(): "
         "position in the buffer, use Seek() instead";
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedReader::SeekBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(new_pos <= limit_pos())) {
    return Fail(
        absl::UnimplementedError("Reader::Seek() backwards not supported"));
  }
  // Seeking forwards.
  do {
    move_cursor(available());
    if (ABSL_PREDICT_FALSE(!PullSlow(1, 0))) return false;
  } while (new_pos > limit_pos());
  const Position available_length = limit_pos() - new_pos;
  RIEGELI_ASSERT_LE(available_length, start_to_limit())
      << "Reader::PullSlow() skipped some data";
  set_cursor(limit() - available_length);
  return true;
}

bool BufferedReader::ReadSlow(size_t length, char* dest) {
  RIEGELI_ASSERT_LT(available(), length)
      << "Failed precondition of Reader::ReadSlow(char*): "
         "enough data available, use Read(char*) instead";
  if (length >= LengthToReadDirectly()) {
    const size_t available_length = available();
    if (
        // `std::memcpy(_, nullptr, 0)` is undefined.
        available_length > 0) {
      std::memcpy(dest, cursor(), available_length);
      dest += available_length;
      length -= available_length;
    }
    SyncBuffer();
    if (ABSL_PREDICT_FALSE(!healthy())) return false;
    return ReadInternal(length, length, dest);
  }
  return Reader::ReadSlow(length, dest);
}

bool BufferedReader::ReadSlow(size_t length, Chain& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::ReadSlow(Chain&): "
         "enough data available, use Read(Chain&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadSlow(Chain&): "
         "Chain size overflow";
  bool enough_read = true;
  bool ok = true;
  while (length > available()) {
    if (ABSL_PREDICT_FALSE(!ok || !healthy())) {
      // Read as much as is available.
      length = available();
      enough_read = false;
      break;
    }
    size_t cursor_index = start_to_cursor();
    const size_t buffer_length =
        BufferLength(0, buffer_size_, size_hint_, limit_pos());
    absl::Span<char> flat_buffer = buffer_.AppendBuffer(
        0, buffer_length, SaturatingAdd(buffer_length, buffer_length));
    if (flat_buffer.empty()) {
      // `flat_buffer` is too small. Append available data to `dest` and make a
      // new buffer.
      length -= available();
      buffer_.AppendSubstrTo(absl::string_view(cursor(), available()), dest);
      buffer_.Clear();
      cursor_index = 0;
      flat_buffer =
          buffer_.AppendBuffer(buffer_length, buffer_length,
                               SaturatingAdd(buffer_length, buffer_length));
    }
    // Read more data into `buffer_`.
    const Position pos_before = limit_pos();
    ok = ReadInternal(UnsignedMin(length, flat_buffer.size()),
                      flat_buffer.size(), flat_buffer.data());
    RIEGELI_ASSERT_GE(limit_pos(), pos_before)
        << "BufferedReader::ReadInternal() decreased limit_pos()";
    const Position length_read = limit_pos() - pos_before;
    RIEGELI_ASSERT_LE(length_read, flat_buffer.size())
        << "BufferedReader::ReadInternal() read more than requested";
    buffer_.RemoveSuffix(flat_buffer.size() - IntCast<size_t>(length_read));
    set_buffer(buffer_.data(), buffer_.size(), cursor_index);
  }
  buffer_.AppendSubstrTo(absl::string_view(cursor(), length), dest);
  move_cursor(length);
  return enough_read;
}

bool BufferedReader::ReadSlow(size_t length, absl::Cord& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::ReadSlow(Cord&): "
         "enough data available, use Read(Cord&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadSlow(Cord&): "
         "Cord size overflow";
  bool enough_read = true;
  bool ok = true;
  while (length > available()) {
    if (ABSL_PREDICT_FALSE(!ok || !healthy())) {
      // Read as much as is available.
      length = available();
      enough_read = false;
      break;
    }
    size_t cursor_index = start_to_cursor();
    const size_t buffer_length =
        BufferLength(0, buffer_size_, size_hint_, limit_pos());
    absl::Span<char> flat_buffer = buffer_.AppendBuffer(
        0, buffer_length, SaturatingAdd(buffer_length, buffer_length));
    if (flat_buffer.empty()) {
      // `flat_buffer` is too small. Append available data to `dest` and make a
      // new buffer.
      length -= available();
      buffer_.AppendSubstrTo(absl::string_view(cursor(), available()), dest);
      buffer_.Clear();
      cursor_index = 0;
      flat_buffer =
          buffer_.AppendBuffer(buffer_length, buffer_length,
                               SaturatingAdd(buffer_length, buffer_length));
    }
    // Read more data into `buffer_`.
    const Position pos_before = limit_pos();
    ok = ReadInternal(UnsignedMin(length, flat_buffer.size()),
                      flat_buffer.size(), flat_buffer.data());
    RIEGELI_ASSERT_GE(limit_pos(), pos_before)
        << "BufferedReader::ReadInternal() decreased limit_pos()";
    const Position length_read = limit_pos() - pos_before;
    RIEGELI_ASSERT_LE(length_read, flat_buffer.size())
        << "BufferedReader::ReadInternal() read more than requested";
    buffer_.RemoveSuffix(flat_buffer.size() - IntCast<size_t>(length_read));
    set_buffer(buffer_.data(), buffer_.size(), cursor_index);
  }
  buffer_.AppendSubstrTo(absl::string_view(cursor(), length), dest);
  move_cursor(length);
  return enough_read;
}

bool BufferedReader::CopySlow(Position length, Writer& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::CopySlow(Writer&): "
         "enough data available, use Copy(Writer&) instead";
  bool enough_read = true;
  bool read_ok = true;
  while (length > available()) {
    if (ABSL_PREDICT_FALSE(!read_ok || !healthy())) {
      // Copy as much as is available.
      length = available();
      enough_read = false;
      break;
    }
    size_t cursor_index = start_to_cursor();
    const size_t buffer_length =
        BufferLength(0, buffer_size_, size_hint_, limit_pos());
    absl::Span<char> flat_buffer = buffer_.AppendBuffer(
        0, buffer_length, SaturatingAdd(buffer_length, buffer_length));
    if (flat_buffer.empty()) {
      // `flat_buffer` is too small. Append available data to `dest` and make a
      // new buffer.
      if (available() > 0) {
        length -= available();
        bool write_ok;
        if (dest.PrefersCopying()) {
          write_ok = dest.Write(cursor(), available());
        } else {
          Chain data;
          buffer_.AppendSubstrTo(absl::string_view(cursor(), available()), data,
                                 Chain::Options().set_size_hint(available()));
          write_ok = dest.Write(std::move(data));
        }
        if (ABSL_PREDICT_FALSE(!write_ok)) {
          move_cursor(available());
          return false;
        }
      }
      buffer_.Clear();
      cursor_index = 0;
      flat_buffer =
          buffer_.AppendBuffer(buffer_length, buffer_length,
                               SaturatingAdd(buffer_length, buffer_length));
    }
    // Read more data into `buffer_`.
    const Position pos_before = limit_pos();
    read_ok = ReadInternal(UnsignedMin(length, flat_buffer.size()),
                           flat_buffer.size(), flat_buffer.data());
    RIEGELI_ASSERT_GE(limit_pos(), pos_before)
        << "BufferedReader::ReadInternal() decreased limit_pos()";
    const Position length_read = limit_pos() - pos_before;
    RIEGELI_ASSERT_LE(length_read, flat_buffer.size())
        << "BufferedReader::ReadInternal() read more than requested";
    buffer_.RemoveSuffix(flat_buffer.size() - IntCast<size_t>(length_read));
    set_buffer(buffer_.data(), buffer_.size(), cursor_index);
  }
  bool write_ok = true;
  if (length > 0) {
    if (dest.PrefersCopying()) {
      write_ok = dest.Write(cursor(), IntCast<size_t>(length));
    } else {
      Chain data;
      buffer_.AppendSubstrTo(
          absl::string_view(cursor(), IntCast<size_t>(length)), data,
          Chain::Options().set_size_hint(IntCast<size_t>(length)));
      write_ok = dest.Write(std::move(data));
    }
    move_cursor(IntCast<size_t>(length));
  }
  return write_ok && enough_read;
}

bool BufferedReader::CopySlow(size_t length, BackwardWriter& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::CopySlow(BackwardWriter&): "
         "enough data available, use Copy(BackwardWriter&) instead";
  if (length <= kMaxBytesToCopy) {
    if (ABSL_PREDICT_FALSE(!dest.Push(length))) return false;
    dest.move_cursor(length);
    if (ABSL_PREDICT_FALSE(!ReadSlow(length, dest.cursor()))) {
      dest.set_cursor(dest.cursor() + length);
      return false;
    }
    return true;
  }
  Chain data;
  if (ABSL_PREDICT_FALSE(!ReadSlow(length, data))) return false;
  return dest.Write(std::move(data));
}

void BufferedReader::ReadHintSlow(size_t min_length,
                                  size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Reader::ReadHintSlow(): "
         "enough data available, use ReadHint() instead";
  PullSlow(min_length, recommended_length);
}

bool BufferedReader::SyncImpl(SyncType sync_type) {
  if (available() > 0) {
    if (!SupportsRandomAccess()) {
      // Seeking back is not feasible.
      return healthy();
    }
    const Position new_pos = pos();
    SyncBuffer();
    SeekBehindBuffer(new_pos);
  }
  return healthy();
}

bool BufferedReader::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of Reader::SeekSlow(): "
         "position in the buffer, use Seek() instead";
  SyncBuffer();
  return SeekBehindBuffer(new_pos);
}

void BufferedReader::ShareBufferTo(BufferedReader& reader) const {
  const Position new_pos = reader.pos();
  if (new_pos >= start_pos() && new_pos < limit_pos()) {
    reader.buffer_ = buffer_;
    reader.set_buffer(start(), start_to_limit(),
                      IntCast<size_t>(new_pos - start_pos()));
    reader.set_limit_pos(limit_pos());
  }
}

}  // namespace riegeli
