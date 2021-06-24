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

#include "riegeli/bytes/reader.h"

#include <stddef.h>

#include <cstring>
#include <limits>
#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void Reader::VerifyEnd() {
  if (ABSL_PREDICT_FALSE(Pull())) {
    Fail(absl::DataLossError(absl::StrCat("End of data expected")));
  }
}

void Reader::AnnotateFailure(absl::Status& status) {
  RIEGELI_ASSERT(!status.ok())
      << "Failed precondition of Object::AnnotateFailure(): status not failed";
  status = Annotate(status, absl::StrCat("at byte ", pos()));
}

bool Reader::FailOverflow() {
  return Fail(absl::ResourceExhaustedError("Reader position overflow"));
}

bool Reader::FailMaxLengthExceeded(Position max_length) {
  return Fail(absl::ResourceExhaustedError(
      absl::StrCat("Maximum length exceeded: ", max_length)));
}

bool Reader::ReadSlow(size_t length, char* dest) {
  RIEGELI_ASSERT_LT(available(), length)
      << "Failed precondition of Reader::ReadSlow(char*): "
         "enough data available, use Read(char*) instead";
  do {
    const size_t available_length = available();
    if (
        // `std::memcpy(_, nullptr, 0)` is undefined.
        available_length > 0) {
      std::memcpy(dest, cursor(), available_length);
      move_cursor(available_length);
      dest += available_length;
      length -= available_length;
    }
    if (ABSL_PREDICT_FALSE(!PullSlow(1, length))) return false;
  } while (length > available());
  std::memcpy(dest, cursor(), length);
  move_cursor(length);
  return true;
}

bool Reader::ReadSlow(size_t length, std::string& dest) {
  RIEGELI_ASSERT_LT(available(), length)
      << "Failed precondition of Reader::ReadSlow(string&): "
         "enough data available, use Read(string&) instead";
  RIEGELI_ASSERT_LE(length, dest.max_size() - dest.size())
      << "Failed precondition of Reader::ReadSlow(string&): "
         "string size overflow";
  const size_t dest_pos = dest.size();
  dest.resize(dest_pos + length);
  const Position pos_before = pos();
  if (ABSL_PREDICT_FALSE(!ReadSlow(length, &dest[dest_pos]))) {
    RIEGELI_ASSERT_GE(pos(), pos_before)
        << "Reader::ReadSlow(char*) decreased pos()";
    const Position length_read = pos() - pos_before;
    RIEGELI_ASSERT_LE(length_read, length)
        << "Reader::ReadSlow(char*) read more than requested";
    dest.erase(dest_pos + IntCast<size_t>(length_read));
    return false;
  }
  return true;
}

bool Reader::ReadSlow(size_t length, Chain& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::ReadSlow(Chain&): "
         "enough data available, use Read(Chain&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadSlow(Chain&): "
         "Chain size overflow";
  do {
    const absl::Span<char> buffer = dest.AppendBuffer(1, length, length);
    const Position pos_before = pos();
    if (ABSL_PREDICT_FALSE(!Read(buffer.size(), buffer.data()))) {
      RIEGELI_ASSERT_GE(pos(), pos_before)
          << "Reader::Read(char*) decreased pos()";
      const Position length_read = pos() - pos_before;
      RIEGELI_ASSERT_LE(length_read, buffer.size())
          << "Reader::Read(char*) read more than requested";
      dest.RemoveSuffix(buffer.size() - IntCast<size_t>(length_read));
      return false;
    }
    length -= buffer.size();
  } while (length > 0);
  return true;
}

bool Reader::ReadSlow(size_t length, absl::Cord& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::ReadSlow(Cord&): "
         "enough data available, use Read(Cord&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadSlow(Cord&): "
         "Cord size overflow";
  while (length > available()) {
    const size_t available_length = available();
    dest.Append(absl::string_view(cursor(), available_length));
    move_cursor(available_length);
    length -= available_length;
    if (ABSL_PREDICT_FALSE(!PullSlow(1, length))) return false;
  }
  dest.Append(absl::string_view(cursor(), length));
  move_cursor(length);
  return true;
}

bool Reader::CopySlow(Position length, Writer& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::CopySlow(Writer&): "
         "enough data available, use Copy(Writer&) instead";
  while (length > available()) {
    const absl::string_view data(cursor(), available());
    move_cursor(data.size());
    if (ABSL_PREDICT_FALSE(!dest.Write(data))) return false;
    length -= data.size();
    if (ABSL_PREDICT_FALSE(!PullSlow(1, length))) return false;
  }
  const absl::string_view data(cursor(), IntCast<size_t>(length));
  move_cursor(IntCast<size_t>(length));
  return dest.Write(data);
}

bool Reader::CopySlow(size_t length, BackwardWriter& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::CopySlow(BackwardWriter&): "
         "enough data available, use Copy(BackwardWriter&) instead";
  if (length <= available()) {
    const absl::string_view data(cursor(), length);
    move_cursor(length);
    return dest.Write(data);
  }
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

void Reader::ReadHintSlow(size_t length) {
  RIEGELI_ASSERT_LT(available(), length)
      << "Failed precondition of Reader::ReadHintSlow(): "
         "enough data available, use ReadHint() instead";
}

bool Reader::ReadAll(absl::string_view& dest, size_t max_length) {
  max_length = UnsignedMin(max_length, dest.max_size());
  if (SupportsSize()) {
    const absl::optional<Position> size = Size();
    if (ABSL_PREDICT_FALSE(size == absl::nullopt)) {
      dest = absl::string_view();
      return false;
    }
    const Position remaining = SaturatingSub(*size, pos());
    if (ABSL_PREDICT_FALSE(remaining > max_length)) {
      if (ABSL_PREDICT_FALSE(!Read(max_length, dest))) {
        if (ABSL_PREDICT_FALSE(!healthy())) return false;
      }
      return FailMaxLengthExceeded(max_length);
    }
    if (ABSL_PREDICT_FALSE(!Read(IntCast<size_t>(remaining), dest))) {
      return healthy();
    }
    return true;
  } else {
    do {
      if (ABSL_PREDICT_FALSE(available() > max_length)) {
        dest = absl::string_view(cursor(), max_length);
        move_cursor(max_length);
        return FailMaxLengthExceeded(max_length);
      }
    } while (Pull(available() + 1));
    dest = absl::string_view(cursor(), available());
    move_cursor(available());
    return healthy();
  }
}

bool Reader::ReadAll(std::string& dest, size_t max_length) {
  dest.clear();
  return ReadAndAppendAll(dest, max_length);
}

bool Reader::ReadAll(Chain& dest, size_t max_length) {
  dest.Clear();
  return ReadAndAppendAll(dest, max_length);
}

bool Reader::ReadAll(absl::Cord& dest, size_t max_length) {
  dest.Clear();
  return ReadAndAppendAll(dest, max_length);
}

bool Reader::ReadAndAppendAll(std::string& dest, size_t max_length) {
  max_length = UnsignedMin(max_length, dest.max_size() - dest.size());
  if (SupportsSize()) {
    const absl::optional<Position> size = Size();
    if (ABSL_PREDICT_FALSE(size == absl::nullopt)) return false;
    const Position remaining = SaturatingSub(*size, pos());
    if (ABSL_PREDICT_FALSE(remaining > max_length)) {
      if (ABSL_PREDICT_FALSE(!ReadAndAppend(max_length, dest))) {
        if (ABSL_PREDICT_FALSE(!healthy())) return false;
      }
      return FailMaxLengthExceeded(max_length);
    }
    if (ABSL_PREDICT_FALSE(!ReadAndAppend(IntCast<size_t>(remaining), dest))) {
      return healthy();
    }
    return true;
  } else {
    size_t remaining_max_length = max_length;
    for (;;) {
      if (ABSL_PREDICT_FALSE(remaining_max_length == 0)) {
        if (!Pull()) break;
        return FailMaxLengthExceeded(max_length);
      }
      if (dest.capacity() - dest.size() <= available()) {
        // `dest` has not enough space to fit currently available data and to
        // determine whether the source ends.
        dest.reserve(UnsignedMin(
            UnsignedMax(SaturatingAdd(dest.size(), available(), size_t{1}),
                        // Ensure amortized constant time of a reallocation.
                        SaturatingAdd(dest.capacity(), dest.capacity() / 2)),
            dest.size() + remaining_max_length));
      }
      // Try to fill all remaining space in `dest`.
      const size_t dest_pos = dest.size();
      const size_t length =
          UnsignedMin(dest.capacity() - dest_pos, remaining_max_length);
      dest.resize(dest_pos + length);
      const Position pos_before = pos();
      if (!Read(length, &dest[dest_pos])) {
        RIEGELI_ASSERT_GE(pos(), pos_before)
            << "Reader::Read(char*) decreased pos()";
        const Position length_read = pos() - pos_before;
        RIEGELI_ASSERT_LE(length_read, length)
            << "Reader::Read(char*) read more than requested";
        dest.erase(dest_pos + IntCast<size_t>(length_read));
        break;
      }
      remaining_max_length -= length;
    }
    return healthy();
  }
}

bool Reader::ReadAndAppendAll(Chain& dest, size_t max_length) {
  max_length =
      UnsignedMin(max_length, std::numeric_limits<size_t>::max() - dest.size());
  if (SupportsSize()) {
    const absl::optional<Position> size = Size();
    if (ABSL_PREDICT_FALSE(size == absl::nullopt)) return false;
    const Position remaining = SaturatingSub(*size, pos());
    if (ABSL_PREDICT_FALSE(remaining > max_length)) {
      if (ABSL_PREDICT_FALSE(!ReadAndAppend(max_length, dest))) {
        if (ABSL_PREDICT_FALSE(!healthy())) return false;
      }
      return FailMaxLengthExceeded(max_length);
    }
    if (ABSL_PREDICT_FALSE(!ReadAndAppend(IntCast<size_t>(remaining), dest))) {
      return healthy();
    }
    return true;
  } else {
    size_t remaining_max_length = max_length;
    do {
      if (ABSL_PREDICT_FALSE(available() > remaining_max_length)) {
        ReadAndAppend(remaining_max_length, dest);
        return FailMaxLengthExceeded(max_length);
      }
      remaining_max_length -= available();
      ReadAndAppend(available(), dest);
    } while (Pull());
    return healthy();
  }
}

bool Reader::ReadAndAppendAll(absl::Cord& dest, size_t max_length) {
  max_length =
      UnsignedMin(max_length, std::numeric_limits<size_t>::max() - dest.size());
  if (SupportsSize()) {
    const absl::optional<Position> size = Size();
    if (ABSL_PREDICT_FALSE(size == absl::nullopt)) return false;
    const Position remaining = SaturatingSub(*size, pos());
    if (ABSL_PREDICT_FALSE(remaining > max_length)) {
      if (ABSL_PREDICT_FALSE(!ReadAndAppend(max_length, dest))) {
        if (ABSL_PREDICT_FALSE(!healthy())) return false;
      }
      return FailMaxLengthExceeded(max_length);
    }
    if (ABSL_PREDICT_FALSE(!ReadAndAppend(IntCast<size_t>(remaining), dest))) {
      return healthy();
    }
    return true;
  } else {
    size_t remaining_max_length = max_length;
    do {
      if (ABSL_PREDICT_FALSE(available() > remaining_max_length)) {
        ReadAndAppend(remaining_max_length, dest);
        return FailMaxLengthExceeded(max_length);
      }
      remaining_max_length -= available();
      ReadAndAppend(available(), dest);
    } while (Pull());
    return healthy();
  }
}

bool Reader::CopyAll(Writer& dest, Position max_length) {
  if (SupportsSize()) {
    const absl::optional<Position> size = Size();
    if (ABSL_PREDICT_FALSE(size == absl::nullopt)) return false;
    const Position remaining = SaturatingSub(*size, pos());
    if (ABSL_PREDICT_FALSE(remaining > max_length)) {
      if (ABSL_PREDICT_FALSE(!Copy(max_length, dest))) {
        return dest.healthy() && healthy();
      }
      return FailMaxLengthExceeded(max_length);
    }
    if (ABSL_PREDICT_FALSE(!Copy(remaining, dest))) {
      return dest.healthy() && healthy();
    }
    return true;
  } else {
    Position remaining_max_length = max_length;
    do {
      if (ABSL_PREDICT_FALSE(available() > remaining_max_length)) {
        if (ABSL_PREDICT_FALSE(!Copy(remaining_max_length, dest))) {
          if (ABSL_PREDICT_FALSE(!dest.healthy())) return false;
        }
        return FailMaxLengthExceeded(max_length);
      }
      remaining_max_length -= available();
      if (ABSL_PREDICT_FALSE(!Copy(available(), dest))) {
        if (ABSL_PREDICT_FALSE(!dest.healthy())) return false;
      }
    } while (Pull());
    return healthy();
  }
}

bool Reader::CopyAll(BackwardWriter& dest, size_t max_length) {
  if (SupportsSize()) {
    const absl::optional<Position> size = Size();
    if (ABSL_PREDICT_FALSE(size == absl::nullopt)) return false;
    const Position remaining = SaturatingSub(*size, pos());
    if (ABSL_PREDICT_FALSE(remaining > max_length)) {
      if (ABSL_PREDICT_FALSE(!Skip(max_length))) {
        if (ABSL_PREDICT_FALSE(!healthy())) return false;
      }
      return FailMaxLengthExceeded(max_length);
    }
    if (ABSL_PREDICT_FALSE(!Copy(IntCast<size_t>(remaining), dest))) {
      return dest.healthy() && healthy();
    }
    return true;
  } else {
    size_t remaining_max_length = max_length;
    Chain data;
    do {
      if (ABSL_PREDICT_FALSE(available() > remaining_max_length)) {
        move_cursor(remaining_max_length);
        return FailMaxLengthExceeded(max_length);
      }
      remaining_max_length -= available();
      ReadAndAppend(available(), data);
    } while (Pull());
    if (ABSL_PREDICT_FALSE(!healthy())) return false;
    return dest.Write(std::move(data));
  }
}

bool Reader::SyncImpl(SyncType sync_type) { return healthy(); }

bool Reader::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of Reader::SeekSlow(): "
         "position in the buffer, use Seek() instead";
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
  RIEGELI_ASSERT_LE(available_length, buffer_size())
      << "Reader::PullSlow() skipped some data";
  set_cursor(limit() - available_length);
  return true;
}

absl::optional<Position> Reader::SizeImpl() {
  Fail(absl::UnimplementedError("Reader::Size() not supported"));
  return absl::nullopt;
}

}  // namespace riegeli
