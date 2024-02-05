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
#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/sized_shared_buffer.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void BufferedReader::Done() {
  if (available() > 0) {
    if (!SupportsRandomAccess()) {
      // Seeking back is not feasible.
      Reader::Done();
      buffer_ = SizedSharedBuffer();
      return;
    }
    const Position new_pos = pos();
    set_buffer();
    SeekBehindBuffer(new_pos);
  }
  Reader::Done();
  buffer_ = SizedSharedBuffer();
}

inline void BufferedReader::SyncBuffer() {
  set_buffer();
  buffer_.Clear();
}

void BufferedReader::SetReadAllHintImpl(bool read_all_hint) {
  buffer_sizer_.set_read_all_hint(read_all_hint);
}

void BufferedReader::ExactSizeReached() {}

bool BufferedReader::PullSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Reader::PullSlow(): "
         "enough data available, use Pull() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  const size_t available_length = available();
  const size_t buffer_length = buffer_sizer_.BufferLength(
      limit_pos(), min_length - available_length,
      SaturatingSub(recommended_length, available_length));
  if (ABSL_PREDICT_FALSE(buffer_length == 0)) {
    ExactSizeReached();
    return false;
  }
  size_t cursor_index = start_to_cursor();
  absl::Span<char> flat_buffer = buffer_.AppendBufferIfExisting(buffer_length);
  if (flat_buffer.empty()) {
    // Not enough space in `buffer_`. Resize `buffer_`, keeping available data.
    buffer_.RemovePrefix(cursor_index);
    buffer_.Shrink(available_length + buffer_length);
    cursor_index = 0;
    flat_buffer = buffer_.AppendFixedBuffer(buffer_length);
  }
  // Read more data into `buffer_`.
  const size_t min_length_to_read =
      ToleratesReadingAhead()
          ? buffer_length
          : UnsignedMin(min_length - available_length, buffer_length);
  const Position pos_before = limit_pos();
  const bool read_ok =
      ReadInternal(min_length_to_read, buffer_length, flat_buffer.data());
  RIEGELI_ASSERT_GE(limit_pos(), pos_before)
      << "BufferedReader::ReadInternal() decreased limit_pos()";
  const Position length_read = limit_pos() - pos_before;
  RIEGELI_ASSERT_LE(length_read, buffer_length)
      << "BufferedReader::ReadInternal() read more than requested";
  if (read_ok) {
    RIEGELI_ASSERT_GE(length_read, min_length_to_read)
        << "BufferedReader::ReadInternal() succeeded but "
           "read less than requested";
  } else {
    RIEGELI_ASSERT_LT(length_read, min_length_to_read)
        << "BufferedReader::ReadInternal() failed but read enough";
  }
  buffer_.RemoveSuffix(flat_buffer.size() - IntCast<size_t>(length_read));
  set_buffer(buffer_.data(), buffer_.size(), cursor_index);
  return available() >= min_length;
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
  if (length >= buffer_sizer_.BufferLength(pos())) {
    // Read directly to `dest`.
    const size_t available_length = available();
    // `std::memcpy(_, nullptr, 0)` is undefined.
    if (available_length > 0) {
      std::memcpy(dest, cursor(), available_length);
      dest += available_length;
      length -= available_length;
    }
    SyncBuffer();
    if (ABSL_PREDICT_FALSE(!ok())) return false;
    size_t length_to_read = length;
    if (exact_size() != absl::nullopt) {
      if (ABSL_PREDICT_FALSE(limit_pos() >= *exact_size())) {
        ExactSizeReached();
        return false;
      }
      length_to_read = UnsignedMin(length_to_read, *exact_size() - limit_pos());
    }
    if (ABSL_PREDICT_FALSE(
            !ReadInternal(length_to_read, length_to_read, dest))) {
      return false;
    }
    return length_to_read >= length;
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
  while (length > available()) {
    size_t available_length = available();
    if (ABSL_PREDICT_FALSE(!ok())) {
      // Read as much as is available.
      enough_read = false;
      length = available_length;
      break;
    }
    const size_t buffer_length =
        buffer_sizer_.BufferLength(limit_pos(), 1, length - available_length);
    size_t cursor_index = start_to_cursor();
    absl::Span<char> flat_buffer =
        buffer_.AppendBufferIfExisting(buffer_length);
    if (flat_buffer.empty()) {
      // Not enough space in `buffer_`. Append available data to `dest` and make
      // a new buffer.
      dest.Append(std::move(buffer_).Substr(cursor(), available_length));
      length -= available_length;
      buffer_.ClearAndShrink(buffer_length);
      if (ABSL_PREDICT_FALSE(buffer_length == 0)) {
        set_buffer();
        ExactSizeReached();
        return false;
      }
      available_length = 0;
      cursor_index = 0;
      flat_buffer = buffer_.AppendFixedBuffer(buffer_length);
    }
    // Read more data into `buffer_`.
    const size_t min_length_to_read =
        ToleratesReadingAhead()
            ? buffer_length
            : UnsignedMin(length - available_length, buffer_length);
    const Position pos_before = limit_pos();
    const bool read_ok =
        ReadInternal(min_length_to_read, buffer_length, flat_buffer.data());
    RIEGELI_ASSERT_GE(limit_pos(), pos_before)
        << "BufferedReader::ReadInternal() decreased limit_pos()";
    const Position length_read = limit_pos() - pos_before;
    RIEGELI_ASSERT_LE(length_read, buffer_length)
        << "BufferedReader::ReadInternal() read more than requested";
    if (read_ok) {
      RIEGELI_ASSERT_GE(length_read, min_length_to_read)
          << "BufferedReader::ReadInternal() succeeded but "
             "read less than requested";
    } else {
      RIEGELI_ASSERT_LT(length_read, min_length_to_read)
          << "BufferedReader::ReadInternal() failed but read enough";
    }
    buffer_.RemoveSuffix(flat_buffer.size() - IntCast<size_t>(length_read));
    set_buffer(buffer_.data(), buffer_.size(), cursor_index);
    if (ABSL_PREDICT_FALSE(!read_ok)) {
      // Read as much as is available.
      enough_read = available() >= length;
      if (ABSL_PREDICT_FALSE(!enough_read)) length = available();
      break;
    }
  }
  dest.Append(buffer_.Substr(cursor(), length));
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
  while (length > available()) {
    size_t available_length = available();
    if (ABSL_PREDICT_FALSE(!ok())) {
      // Read as much as is available.
      enough_read = false;
      length = available_length;
      break;
    }
    const size_t buffer_length =
        buffer_sizer_.BufferLength(limit_pos(), 1, length - available_length);
    size_t cursor_index = start_to_cursor();
    absl::Span<char> flat_buffer =
        buffer_.AppendBufferIfExisting(buffer_length);
    if (flat_buffer.empty()) {
      // Not enough space in `buffer_`. Append available data to `dest` and make
      // a new buffer.
      std::move(buffer_).Substr(cursor(), available_length).AppendTo(dest);
      length -= available_length;
      buffer_.ClearAndShrink(buffer_length);
      if (ABSL_PREDICT_FALSE(buffer_length == 0)) {
        set_buffer();
        ExactSizeReached();
        return false;
      }
      available_length = 0;
      cursor_index = 0;
      flat_buffer = buffer_.AppendFixedBuffer(buffer_length);
    }
    // Read more data into `buffer_`.
    const size_t min_length_to_read =
        ToleratesReadingAhead()
            ? buffer_length
            : UnsignedMin(length - available_length, buffer_length);
    const Position pos_before = limit_pos();
    const bool read_ok =
        ReadInternal(min_length_to_read, buffer_length, flat_buffer.data());
    RIEGELI_ASSERT_GE(limit_pos(), pos_before)
        << "BufferedReader::ReadInternal() decreased limit_pos()";
    const Position length_read = limit_pos() - pos_before;
    RIEGELI_ASSERT_LE(length_read, buffer_length)
        << "BufferedReader::ReadInternal() read more than requested";
    if (read_ok) {
      RIEGELI_ASSERT_GE(length_read, min_length_to_read)
          << "BufferedReader::ReadInternal() succeeded but "
             "read less than requested";
    } else {
      RIEGELI_ASSERT_LT(length_read, min_length_to_read)
          << "BufferedReader::ReadInternal() failed but read enough";
    }
    buffer_.RemoveSuffix(flat_buffer.size() - IntCast<size_t>(length_read));
    set_buffer(buffer_.data(), buffer_.size(), cursor_index);
    if (ABSL_PREDICT_FALSE(!read_ok)) {
      // Read as much as is available.
      enough_read = available() >= length;
      if (ABSL_PREDICT_FALSE(!enough_read)) length = available();
      break;
    }
  }
  buffer_.Substr(cursor(), length).AppendTo(dest);
  move_cursor(length);
  return enough_read;
}

bool BufferedReader::CopySlow(Position length, Writer& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::CopySlow(Writer&): "
         "enough data available, use Copy(Writer&) instead";
  bool enough_read = true;
  while (length > available()) {
    size_t available_length = available();
    if (ABSL_PREDICT_FALSE(!ok())) {
      // Copy as much as is available.
      enough_read = false;
      length = available_length;
      break;
    }
    const bool read_directly = length >= buffer_sizer_.BufferLength(pos());
    if (read_directly) {
      if (available_length <= kMaxBytesToCopy || dest.PrefersCopying()) {
        if (ABSL_PREDICT_FALSE(
                !dest.Write(absl::string_view(cursor(), available_length)))) {
          move_cursor(available_length);
          return false;
        }
        length -= available_length;
        SyncBuffer();
        return CopyUsingPush(length, dest);
      }
      // It is better to write available data from `buffer_` as a `Chain` before
      // reading directly to `dest`. Before that, `buffer_` might need to be
      // filled more to avoid attaching a wasteful `Chain`.
    }
    const size_t buffer_length =
        buffer_sizer_.BufferLength(limit_pos(), 1, length - available_length);
    size_t cursor_index = start_to_cursor();
    absl::Span<char> flat_buffer =
        buffer_.AppendBufferIfExisting(buffer_length);
    if (flat_buffer.empty()) {
      // Not enough space in `buffer_`. Append available data to `dest` and make
      // a new buffer.
      if (available_length > 0) {
        const bool write_ok =
            available_length <= kMaxBytesToCopy || dest.PrefersCopying()
                ? dest.Write(absl::string_view(cursor(), available_length))
                : dest.Write(Chain(
                      std::move(buffer_).Substr(cursor(), available_length)));
        if (ABSL_PREDICT_FALSE(!write_ok)) {
          buffer_.ClearAndShrink(buffer_length);
          set_buffer();
          return false;
        }
        length -= available_length;
      }
      buffer_.ClearAndShrink(buffer_length);
      if (ABSL_PREDICT_FALSE(buffer_length == 0)) {
        set_buffer();
        ExactSizeReached();
        return false;
      }
      if (read_directly) {
        set_buffer();
        return CopyUsingPush(length, dest);
      }
      available_length = 0;
      cursor_index = 0;
      flat_buffer = buffer_.AppendFixedBuffer(buffer_length);
    }
    // Read more data into `buffer_`.
    const size_t min_length_to_read =
        ToleratesReadingAhead()
            ? buffer_length
            : UnsignedMin(length - available_length, buffer_length);
    const Position pos_before = limit_pos();
    const bool read_ok =
        ReadInternal(min_length_to_read, buffer_length, flat_buffer.data());
    RIEGELI_ASSERT_GE(limit_pos(), pos_before)
        << "BufferedReader::ReadInternal() decreased limit_pos()";
    const Position length_read = limit_pos() - pos_before;
    RIEGELI_ASSERT_LE(length_read, buffer_length)
        << "BufferedReader::ReadInternal() read more than requested";
    if (read_ok) {
      RIEGELI_ASSERT_GE(length_read, min_length_to_read)
          << "BufferedReader::ReadInternal() succeeded but "
             "read less than requested";
    } else {
      RIEGELI_ASSERT_LT(length_read, min_length_to_read)
          << "BufferedReader::ReadInternal() failed but read enough";
    }
    buffer_.RemoveSuffix(flat_buffer.size() - IntCast<size_t>(length_read));
    set_buffer(buffer_.data(), buffer_.size(), cursor_index);
    if (ABSL_PREDICT_FALSE(!read_ok)) {
      // Copy as much as is available.
      enough_read = available() >= length;
      if (ABSL_PREDICT_FALSE(!enough_read)) length = available();
      break;
    }
  }
  const bool write_ok =
      IntCast<size_t>(length) <= kMaxBytesToCopy || dest.PrefersCopying()
          ? dest.Write(absl::string_view(cursor(), IntCast<size_t>(length)))
          : dest.Write(
                Chain(buffer_.Substr(cursor(), IntCast<size_t>(length))));
  move_cursor(IntCast<size_t>(length));
  return write_ok && enough_read;
}

inline bool BufferedReader::CopyUsingPush(Position length, Writer& dest) {
  RIEGELI_ASSERT_GT(length, 0u)
      << "Failed precondition of BufferedReader::CopyUsingPush(): "
         "nothing to copy";
  RIEGELI_ASSERT(ok())
      << "Failed precondition of BufferedReader::CopyUsingPush(): " << status();
  Position length_to_read = length;
  if (exact_size() != absl::nullopt) {
    if (ABSL_PREDICT_FALSE(limit_pos() >= *exact_size())) {
      ExactSizeReached();
      return false;
    }
    length_to_read = UnsignedMin(length_to_read, *exact_size() - limit_pos());
  }
  return CopyInternal(length_to_read, dest) && length_to_read == length;
}

bool BufferedReader::CopyInternal(Position length, Writer& dest) {
  RIEGELI_ASSERT_GT(length, 0u)
      << "Failed precondition of BufferedReader::CopyInternal(): "
         "nothing to copy";
  RIEGELI_ASSERT(ok())
      << "Failed precondition of BufferedReader::CopyInternal(): " << status();
  size_t length_to_read = SaturatingIntCast<size_t>(length);
  // In the first iteration `exact_size()` was taken into account by
  // `CopyUsingPush()`, so that `CopyInternal()` overrides do not need to.
  for (;;) {
    if (ABSL_PREDICT_FALSE(!dest.Push(1, length_to_read))) return false;
    const size_t length_to_copy = UnsignedMin(length_to_read, dest.available());
    const Position pos_before = limit_pos();
    const bool read_ok =
        ReadInternal(length_to_copy, length_to_copy, dest.cursor());
    RIEGELI_ASSERT_GE(limit_pos(), pos_before)
        << "BufferedReader::ReadInternal() decreased limit_pos()";
    const Position length_read = limit_pos() - pos_before;
    RIEGELI_ASSERT_LE(length_read, length_to_copy)
        << "BufferedReader::ReadInternal() read more than requested";
    if (read_ok) {
      RIEGELI_ASSERT_GE(length_read, length_to_copy)
          << "BufferedReader::ReadInternal() succeeded but "
             "read less than requested";
    } else {
      RIEGELI_ASSERT_LT(length_read, length_to_copy)
          << "BufferedReader::ReadInternal() failed but read enough";
    }
    dest.move_cursor(IntCast<size_t>(length_read));
    if (ABSL_PREDICT_FALSE(!read_ok)) return false;
    length -= IntCast<size_t>(length_read);
    if (length == 0) return true;
    // `ReadInternal()` might have set `exact_size()`, so this implementation of
    // `CopyInternal()` needs to take `exact_size()` into account for remaining
    // iterations.
    length_to_read = SaturatingIntCast<size_t>(length);
    if (exact_size() != absl::nullopt) {
      if (ABSL_PREDICT_FALSE(limit_pos() >= *exact_size())) {
        ExactSizeReached();
        return false;
      }
      length_to_read = UnsignedMin(length_to_read, *exact_size() - limit_pos());
    }
  }
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

bool BufferedReader::ReadOrPullSomeSlow(
    size_t max_length, absl::FunctionRef<char*(size_t&)> get_dest) {
  RIEGELI_ASSERT_GT(max_length, 0u)
      << "Failed precondition of Reader::ReadOrPullSomeSlow(): "
         "nothing to read, use ReadOrPullSome() instead";
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of Reader::ReadOrPullSomeSlow(): "
         "some data available, use ReadOrPullSome() instead";
  if (max_length >= buffer_sizer_.BufferLength(limit_pos())) {
    // Read directly to `get_dest(max_length)`.
    SyncBuffer();
    if (ABSL_PREDICT_FALSE(!ok())) return false;
    if (exact_size() != absl::nullopt) {
      if (ABSL_PREDICT_FALSE(limit_pos() >= *exact_size())) {
        ExactSizeReached();
        return false;
      }
      max_length = UnsignedMin(max_length, *exact_size() - limit_pos());
    }
    char* const dest = get_dest(max_length);
    if (ABSL_PREDICT_FALSE(max_length == 0)) return false;
    const Position pos_before = limit_pos();
    ReadInternal(ToleratesReadingAhead() ? max_length : 1, max_length, dest);
    RIEGELI_ASSERT_GE(limit_pos(), pos_before)
        << "BufferedReader::ReadInternal() decreased limit_pos()";
    return limit_pos() > pos_before;
  }
  return PullSlow(1, max_length);
}

void BufferedReader::ReadHintSlow(size_t min_length,
                                  size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Reader::ReadHintSlow(): "
         "enough data available, use ReadHint() instead";
  PullSlow(min_length, recommended_length);
}

bool BufferedReader::SyncImpl(SyncType sync_type) {
  if (available() > 0 && !SupportsRandomAccess()) {
    // Seeking back is not feasible.
    return ok();
  }
  const Position new_pos = pos();
  buffer_sizer_.EndRun(new_pos);
  SyncBuffer();
  const bool result = new_pos == limit_pos() ? ok() : SeekBehindBuffer(new_pos);
  buffer_sizer_.BeginRun(start_pos());
  return result;
}

bool BufferedReader::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of Reader::SeekSlow(): "
         "position in the buffer, use Seek() instead";
  if (ABSL_PREDICT_FALSE(!SupportsRandomAccess())) {
    SyncBuffer();
    return SeekBehindBuffer(new_pos);
  }
  buffer_sizer_.EndRun(pos());
  SyncBuffer();
  const bool result = SeekBehindBuffer(new_pos);
  buffer_sizer_.BeginRun(start_pos());
  return result;
}

absl::optional<Position> BufferedReader::SizeImpl() {
  if (ABSL_PREDICT_FALSE(!ok())) return absl::nullopt;
  if (ABSL_PREDICT_FALSE(exact_size() == absl::nullopt)) {
    // Delegate to the base class to avoid repeating the error message.
    return Reader::SizeImpl();
  }
  return *exact_size();
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

SizedSharedBuffer BufferedReader::SaveBuffer() {
  set_limit_pos(pos());
  buffer_.RemovePrefix(start_to_cursor());
  set_buffer();
  return std::move(buffer_);
}

void BufferedReader::RestoreBuffer(SizedSharedBuffer buffer) {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedReader::RestoreBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!ok())) return;
  buffer_ = std::move(buffer);
  set_buffer(buffer_.data(), buffer_.size());
  move_limit_pos(available());
}

}  // namespace riegeli
