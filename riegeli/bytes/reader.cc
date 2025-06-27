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
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/cord_buffer.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/cord_utils.h"
#include "riegeli/base/status.h"
#include "riegeli/base/string_utils.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void Reader::VerifyEndImpl() {
  if (ABSL_PREDICT_FALSE(Pull())) {
    absl::Status status = absl::InvalidArgumentError("End of data expected");
    if (SupportsSize()) {
      const std::optional<Position> size = Size();
      if (size != std::nullopt) {
        status = Annotate(status, absl::StrCat("remaining length: ",
                                               SaturatingSub(*size, pos())));
      }
    }
    Fail(std::move(status));
  }
}

absl::Status Reader::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) return Annotate(status, absl::StrCat("at byte ", pos()));
  return status;
}

bool Reader::FailOverflow() {
  return Fail(absl::ResourceExhaustedError("Reader position overflow"));
}

bool Reader::Read(size_t length, std::string& dest, size_t* length_read) {
  if (ABSL_PREDICT_TRUE(available() >= length)) {
    // `std::string::assign()` checks for size overflow.
    dest.assign(cursor(), length);
    move_cursor(length);
    if (length_read != nullptr) *length_read = length;
    return true;
  }
  dest.clear();
  if (length_read != nullptr) return ReadSlow(length, dest, *length_read);
  return ReadSlow(length, dest);
}

bool Reader::Read(size_t length, Chain& dest, size_t* length_read) {
  if (ABSL_PREDICT_TRUE(available() >= length && length <= kMaxBytesToCopy)) {
    dest.Reset(absl::string_view(cursor(), length));
    move_cursor(length);
    if (length_read != nullptr) *length_read = length;
    return true;
  }
  dest.Clear();
  if (length_read != nullptr) return ReadSlow(length, dest, *length_read);
  return ReadSlow(length, dest);
}

bool Reader::Read(size_t length, absl::Cord& dest, size_t* length_read) {
  if (ABSL_PREDICT_TRUE(available() >= length && length <= kMaxBytesToCopy)) {
    dest = absl::string_view(cursor(), length);
    move_cursor(length);
    if (length_read != nullptr) *length_read = length;
    return true;
  }
  dest.Clear();
  if (length_read != nullptr) return ReadSlow(length, dest, *length_read);
  return ReadSlow(length, dest);
}

bool Reader::ReadAndAppend(size_t length, std::string& dest,
                           size_t* length_read) {
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadAndAppend(string&): "
         "string size overflow";
  if (ABSL_PREDICT_TRUE(available() >= length)) {
    // `std::string::append()` checks for size overflow.
    dest.append(cursor(), length);
    move_cursor(length);
    if (length_read != nullptr) *length_read = length;
    return true;
  }
  if (length_read != nullptr) return ReadSlow(length, dest, *length_read);
  return ReadSlow(length, dest);
}

bool Reader::ReadAndAppend(size_t length, Chain& dest, size_t* length_read) {
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadAndAppend(Chain&): "
         "Chain size overflow";
  if (ABSL_PREDICT_TRUE(available() >= length && length <= kMaxBytesToCopy)) {
    // `Chain::Append()` checks for size overflow.
    dest.Append(absl::string_view(cursor(), length));
    move_cursor(length);
    if (length_read != nullptr) *length_read = length;
    return true;
  }
  // Check the size before calling virtual `ReadSlow(Chain&)`.
  if (length_read != nullptr) {
    return ReadSlowWithSizeCheck(length, dest, *length_read);
  }
  return ReadSlowWithSizeCheck(length, dest);
}

bool Reader::ReadAndAppend(size_t length, absl::Cord& dest,
                           size_t* length_read) {
  if (ABSL_PREDICT_TRUE(
          available() >= length && length <= kMaxBytesToCopy &&
          // `absl::Cord::Append()` does not check for size overflow.
          length <= std::numeric_limits<size_t>::max() - dest.size())) {
    dest.Append(absl::string_view(cursor(), length));
    move_cursor(length);
    if (length_read != nullptr) *length_read = length;
    return true;
  }
  // Check the size in case it would overflow in the fast path, and before
  // calling virtual `ReadSlow(absl::Cord&)`.
  if (length_read != nullptr) {
    return ReadSlowWithSizeCheck(length, dest, *length_read);
  }
  return ReadSlowWithSizeCheck(length, dest);
}

bool Reader::Copy(Position length, Writer& dest, Position* length_read) {
  if (ABSL_PREDICT_TRUE(available() >= length && length <= kMaxBytesToCopy)) {
    const absl::string_view data(cursor(), IntCast<size_t>(length));
    move_cursor(IntCast<size_t>(length));
    if (length_read != nullptr) *length_read = length;
    return dest.Write(data);
  }
  if (length_read != nullptr) return CopySlow(length, dest, *length_read);
  return CopySlow(length, dest);
}

bool Reader::Copy(size_t length, BackwardWriter& dest) {
  if (ABSL_PREDICT_TRUE(available() >= length && length <= kMaxBytesToCopy)) {
    const absl::string_view data(cursor(), length);
    move_cursor(length);
    return dest.Write(data);
  }
  return CopySlow(length, dest);
}

bool Reader::ReadSome(size_t max_length, std::string& dest,
                      size_t* length_read) {
  if (ABSL_PREDICT_TRUE(available() >= max_length)) {
    dest.assign(cursor(), max_length);
    move_cursor(max_length);
    if (length_read != nullptr) *length_read = max_length;
    return true;
  }
  dest.clear();
  if (length_read != nullptr) {
    return ReadSomeSlow(max_length, dest, *length_read);
  }
  return ReadSomeSlow(max_length, dest);
}

bool Reader::ReadSome(size_t max_length, Chain& dest, size_t* length_read) {
  dest.Clear();
  if (ABSL_PREDICT_TRUE(available() >= max_length &&
                        max_length <= kMaxBytesToCopy)) {
    dest.Append(absl::string_view(cursor(), max_length));
    move_cursor(max_length);
    if (length_read != nullptr) *length_read = max_length;
    return true;
  }
  if (length_read != nullptr) {
    return ReadSomeSlow(max_length, dest, *length_read);
  }
  return ReadSomeSlow(max_length, dest);
}

bool Reader::ReadSome(size_t max_length, absl::Cord& dest,
                      size_t* length_read) {
  if (ABSL_PREDICT_TRUE(available() >= max_length &&
                        max_length <= kMaxBytesToCopy)) {
    dest = absl::string_view(cursor(), max_length);
    move_cursor(max_length);
    if (length_read != nullptr) *length_read = max_length;
    return true;
  }
  dest.Clear();
  if (length_read != nullptr) {
    return ReadSomeSlow(max_length, dest, *length_read);
  }
  return ReadSomeSlow(max_length, dest);
}

bool Reader::ReadAndAppendSome(size_t max_length, std::string& dest,
                               size_t* length_read) {
  if (ABSL_PREDICT_TRUE(available() >= max_length &&
                        max_length <=
                            std::numeric_limits<size_t>::max() - dest.size())) {
    dest.append(cursor(), max_length);
    move_cursor(max_length);
    if (length_read != nullptr) *length_read = max_length;
    return true;
  }
  if (length_read != nullptr) {
    return ReadSomeSlow(max_length, dest, *length_read);
  }
  return ReadSomeSlow(max_length, dest);
}

bool Reader::ReadAndAppendSome(size_t max_length, Chain& dest,
                               size_t* length_read) {
  if (ABSL_PREDICT_TRUE(
          available() >= max_length && max_length <= kMaxBytesToCopy &&
          max_length <= std::numeric_limits<size_t>::max() - dest.size())) {
    dest.Append(absl::string_view(cursor(), max_length));
    move_cursor(max_length);
    if (length_read != nullptr) *length_read = max_length;
    return true;
  }
  if (length_read != nullptr) {
    return ReadSomeSlow(max_length, dest, *length_read);
  }
  return ReadSomeSlow(max_length, dest);
}

bool Reader::ReadAndAppendSome(size_t max_length, absl::Cord& dest,
                               size_t* length_read) {
  if (ABSL_PREDICT_TRUE(
          available() >= max_length && max_length <= kMaxBytesToCopy &&
          max_length <= std::numeric_limits<size_t>::max() - dest.size())) {
    dest.Append(absl::string_view(cursor(), max_length));
    move_cursor(max_length);
    if (length_read != nullptr) *length_read = max_length;
    return true;
  }
  if (length_read != nullptr) {
    return ReadSomeSlow(max_length, dest, *length_read);
  }
  return ReadSomeSlow(max_length, dest);
}

bool Reader::CopySome(size_t max_length, Writer& dest, size_t* length_read) {
  if (ABSL_PREDICT_TRUE(available() >= max_length &&
                        max_length <= kMaxBytesToCopy)) {
    const absl::string_view data(cursor(), max_length);
    move_cursor(max_length);
    if (length_read != nullptr) *length_read = max_length;
    return dest.Write(data);
  }
  if (length_read != nullptr) {
    return CopySomeSlow(max_length, dest, *length_read);
  }
  return CopySomeSlow(max_length, dest);
}

bool Reader::ReadOrPullSome(size_t max_length,
                            absl::FunctionRef<char*(size_t&)> get_dest,
                            size_t* length_read) {
  if (ABSL_PREDICT_TRUE(available() > 0) ||
      ABSL_PREDICT_FALSE(max_length == 0)) {
    if (length_read != nullptr) *length_read = 0;
    return true;
  }
  if (length_read != nullptr) {
    return ReadOrPullSomeSlow(max_length, get_dest, *length_read);
  }
  return ReadOrPullSomeSlow(max_length, get_dest);
}

bool Reader::ReadSlow(size_t length, char* dest) {
  RIEGELI_ASSERT_LT(available(), length)
      << "Failed precondition of Reader::ReadSlow(char*): "
         "enough data available, use Read(char*) instead";
  do {
    const size_t available_length = available();
    // `std::memcpy(_, nullptr, 0)` is undefined.
    if (available_length > 0) {
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

bool Reader::ReadSlow(size_t length, char* dest, size_t& length_read) {
  RIEGELI_ASSERT_LT(available(), length)
      << "Failed precondition of Reader::ReadSlow(char*): "
         "enough data available, use Read(char*) instead";
  const Position pos_before = pos();
  const bool read_ok = ReadSlow(length, dest);
  RIEGELI_ASSERT_GE(pos(), pos_before)
      << "Reader::ReadSlow(char*) decreased pos()";
  RIEGELI_ASSERT_LE(pos() - pos_before, length)
      << "Reader::ReadSlow(char*) read more than requested";
  if (ABSL_PREDICT_FALSE(!read_ok)) {
    length_read = IntCast<size_t>(pos() - pos_before);
    return false;
  }
  RIEGELI_ASSERT_EQ(pos() - pos_before, length)
      << "Reader::ReadSlow(char*) succeeded but read less than requested";
  length_read = length;
  return true;
}

bool Reader::ReadSlow(size_t length, std::string& dest) {
  RIEGELI_ASSERT_LT(available(), length)
      << "Failed precondition of Reader::ReadSlow(string&): "
         "enough data available, use Read(string&) instead";
  const size_t dest_pos = dest.size();
  RIEGELI_CHECK_LE(length, std::numeric_limits<size_t>::max() - dest_pos)
      << "Failed precondition of Reader::ReadSlow(string&): "
         "string size overflow";
  ResizeStringAmortized(dest, dest_pos + length);
  size_t length_read;
  if (ABSL_PREDICT_FALSE(!ReadSlow(length, &dest[dest_pos], length_read))) {
    dest.erase(dest_pos + length_read);
    return false;
  }
  return true;
}

bool Reader::ReadSlow(size_t length, std::string& dest, size_t& length_read) {
  RIEGELI_ASSERT_LT(available(), length)
      << "Failed precondition of Reader::ReadSlow(string&): "
         "enough data available, use Read(string&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadSlow(string&): "
         "string size overflow";
  const Position pos_before = pos();
  const bool read_ok = ReadSlow(length, dest);
  RIEGELI_ASSERT_GE(pos(), pos_before)
      << "Reader::ReadSlow(string&) decreased pos()";
  RIEGELI_ASSERT_LE(pos() - pos_before, length)
      << "Reader::ReadSlow(string&) read more than requested";
  if (ABSL_PREDICT_FALSE(!read_ok)) {
    length_read = IntCast<size_t>(pos() - pos_before);
    return false;
  }
  RIEGELI_ASSERT_EQ(pos() - pos_before, length)
      << "Reader::ReadSlow(string&) succeeded but read less than requested";
  length_read = length;
  return true;
}

bool Reader::ReadSlowWithSizeCheck(size_t length, Chain& dest) {
  RIEGELI_CHECK_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadAndAppend(Chain&): "
         "Chain size overflow";
  return ReadSlow(length, dest);
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
    size_t length_read;
    if (ABSL_PREDICT_FALSE(!Read(buffer.size(), buffer.data(), &length_read))) {
      dest.RemoveSuffix(buffer.size() - length_read);
      return false;
    }
    length -= length_read;
  } while (length > 0);
  return true;
}

bool Reader::ReadSlowWithSizeCheck(size_t length, Chain& dest,
                                   size_t& length_read) {
  RIEGELI_CHECK_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadAndAppend(Chain&): "
         "Chain size overflow";
  return ReadSlow(length, dest, length_read);
}

bool Reader::ReadSlow(size_t length, Chain& dest, size_t& length_read) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::ReadSlow(Chain&): "
         "enough data available, use Read(Chain&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadSlow(Chain&): "
         "Chain size overflow";
  const Position pos_before = pos();
  const bool read_ok = ReadSlow(length, dest);
  RIEGELI_ASSERT_GE(pos(), pos_before)
      << "Reader::ReadSlow(Chain&) decreased pos()";
  RIEGELI_ASSERT_LE(pos() - pos_before, length)
      << "Reader::ReadSlow(Chain&) read more than requested";
  if (ABSL_PREDICT_FALSE(!read_ok)) {
    length_read = IntCast<size_t>(pos() - pos_before);
    return false;
  }
  RIEGELI_ASSERT_EQ(pos() - pos_before, length)
      << "Reader::ReadSlow(Chain&) succeeded but read less than requested";
  length_read = length;
  return true;
}

bool Reader::ReadSlowWithSizeCheck(size_t length, absl::Cord& dest) {
  RIEGELI_CHECK_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadAndAppend(Cord&): "
         "Cord size overflow";
  return ReadSlow(length, dest);
}

bool Reader::ReadSlow(size_t length, absl::Cord& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::ReadSlow(Cord&): "
         "enough data available, use Read(Cord&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadSlow(Cord&): "
         "Cord size overflow";
  absl::CordBuffer buffer = dest.GetCustomAppendBuffer(
      cord_internal::kCordBufferBlockSize, length, 1);
  absl::Span<char> span = buffer.available_up_to(length);
  if (buffer.capacity() < kDefaultMinBlockSize && length > span.size()) {
    absl::CordBuffer new_buffer = absl::CordBuffer::CreateWithCustomLimit(
        cord_internal::kCordBufferBlockSize, buffer.length() + length);
    std::memcpy(new_buffer.data(), buffer.data(), buffer.length());
    new_buffer.SetLength(buffer.length());
    buffer = std::move(new_buffer);
    span = buffer.available_up_to(length);
  }
  for (;;) {
    size_t length_read;
    const bool read_ok = Read(span.size(), span.data(), &length_read);
    buffer.IncreaseLengthBy(length_read);
    dest.Append(std::move(buffer));
    if (ABSL_PREDICT_FALSE(!read_ok)) return false;
    length -= length_read;
    if (length == 0) return true;
    buffer = absl::CordBuffer::CreateWithCustomLimit(
        cord_internal::kCordBufferBlockSize, length);
    span = buffer.available_up_to(length);
  }
}

bool Reader::ReadSlowWithSizeCheck(size_t length, absl::Cord& dest,
                                   size_t& length_read) {
  RIEGELI_CHECK_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadAndAppend(Cord&): "
         "Cord size overflow";
  return ReadSlow(length, dest, length_read);
}

bool Reader::ReadSlow(size_t length, absl::Cord& dest, size_t& length_read) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::ReadSlow(Cord&): "
         "enough data available, use Read(Cord&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadSlow(Cord&): "
         "Cord size overflow";
  const Position pos_before = pos();
  const bool read_ok = ReadSlow(length, dest);
  RIEGELI_ASSERT_GE(pos(), pos_before)
      << "Reader::ReadSlow(Cord&) decreased pos()";
  RIEGELI_ASSERT_LE(pos() - pos_before, length)
      << "Reader::ReadSlow(Cord&) read more than requested";
  if (ABSL_PREDICT_FALSE(!read_ok)) {
    length_read = IntCast<size_t>(pos() - pos_before);
    return false;
  }
  RIEGELI_ASSERT_EQ(pos() - pos_before, length)
      << "Reader::ReadSlow(Cord&) succeeded but read less than requested";
  length_read = length;
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

bool Reader::CopySlow(Position length, Writer& dest, Position& length_read) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::CopySlow(Writer&): "
         "enough data available, use Copy(Writer&) instead";
  const Position pos_before = pos();
  const bool copy_ok = CopySlow(length, dest);
  RIEGELI_ASSERT_GE(pos(), pos_before)
      << "Reader::CopySlow(Writer&) decreased pos()";
  RIEGELI_ASSERT_LE(pos() - pos_before, length)
      << "Reader::CopySlow(Writer&) read more than requested";
  if (ABSL_PREDICT_FALSE(!copy_ok)) {
    length_read = pos() - pos_before;
    return false;
  }
  RIEGELI_ASSERT_EQ(pos() - pos_before, length)
      << "Reader::CopySlow(Writer&) succeeded but read less than requested";
  length_read = length;
  return true;
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

bool Reader::ReadSomeSlow(size_t max_length, char* dest) {
  RIEGELI_ASSERT_LT(available(), max_length)
      << "Failed precondition of Reader::ReadSomeSlow(char*): "
         "enough data available, use ReadSome(char*) instead";
  size_t length_read;
  const bool read_ok = ReadOrPullSome(
      max_length, [dest](ABSL_ATTRIBUTE_UNUSED size_t& length) { return dest; },
      &length_read);
  if (length_read == 0) {
    if (ABSL_PREDICT_FALSE(!read_ok)) return false;
    RIEGELI_ASSERT_GT(available(), 0u)
        << "Reader::ReadOrPullSome() succeeded but read none and pulled none";
    max_length = UnsignedMin(max_length, available());
    std::memcpy(dest, cursor(), max_length);
    move_cursor(max_length);
  }
  return true;
}

bool Reader::ReadSomeSlow(size_t max_length, char* dest, size_t& length_read) {
  RIEGELI_ASSERT_LT(available(), max_length)
      << "Failed precondition of Reader::ReadSomeSlow(char*): "
         "enough data available, use ReadSome(char*) instead";
  const Position pos_before = pos();
  const bool read_ok = ReadSomeSlow(max_length, dest);
  RIEGELI_ASSERT_GE(pos(), pos_before)
      << "Reader::ReadSomeSlow(char*) decreased pos()";
  RIEGELI_ASSERT_LE(pos() - pos_before, max_length)
      << "Reader::ReadSomeSlow(char*) read more than requested";
  length_read = IntCast<size_t>(pos() - pos_before);
  if (!read_ok) {
    RIEGELI_ASSERT_EQ(length_read, 0u)
        << "Reader::ReadSomeSlow(char*) failed but read some";
  } else {
    RIEGELI_ASSERT_GT(length_read, 0u)
        << "Reader::ReadSomeSlow(char*) succeeded but read none";
  }
  return read_ok;
}

bool Reader::ReadSomeSlow(size_t max_length, std::string& dest) {
  RIEGELI_ASSERT_LT(available(), max_length)
      << "Failed precondition of Reader::ReadSomeSlow(string&): "
         "enough data available, use ReadSome(string&) instead";
  const size_t dest_size_before = dest.size();
  const size_t remaining =
      std::numeric_limits<size_t>::max() - dest_size_before;
  RIEGELI_CHECK_GT(remaining, 0u)
      << "Failed precondition of Reader::ReadSome(string&): "
         "string size overflow";
  max_length = UnsignedMin(max_length, remaining);
  size_t length_read;
  const bool read_ok = ReadOrPullSome(
      max_length,
      [&dest, dest_size_before](size_t& length) {
        ResizeStringAmortized(dest, dest_size_before + length);
        return &dest[dest_size_before];
      },
      &length_read);
  dest.erase(dest_size_before + length_read);
  if (length_read == 0) {
    if (ABSL_PREDICT_FALSE(!read_ok)) return false;
    RIEGELI_ASSERT_GT(available(), 0u)
        << "Reader::ReadOrPullSome() succeeded but read none and pulled none";
    max_length = UnsignedMin(max_length, available());
    dest.append(cursor(), max_length);
    move_cursor(max_length);
  }
  return true;
}

bool Reader::ReadSomeSlow(size_t max_length, std::string& dest,
                          size_t& length_read) {
  RIEGELI_ASSERT_LT(available(), max_length)
      << "Failed precondition of Reader::ReadSomeSlow(string&): "
         "enough data available, use ReadSome(string&) instead";
  const Position pos_before = pos();
  const bool read_ok = ReadSomeSlow(max_length, dest);
  RIEGELI_ASSERT_GE(pos(), pos_before)
      << "Reader::ReadSomeSlow(string&) decreased pos()";
  RIEGELI_ASSERT_LE(pos() - pos_before, max_length)
      << "Reader::ReadSomeSlow(string&) read more than requested";
  length_read = IntCast<size_t>(pos() - pos_before);
  if (!read_ok) {
    RIEGELI_ASSERT_EQ(length_read, 0u)
        << "Reader::ReadSomeSlow(string&) failed but read some";
  } else {
    RIEGELI_ASSERT_GT(length_read, 0u)
        << "Reader::ReadSomeSlow(string&) succeeded but read none";
  }
  return read_ok;
}

bool Reader::ReadSomeSlow(size_t max_length, Chain& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), max_length)
      << "Failed precondition of Reader::ReadSomeSlow(Chain&): "
         "enough data available, use ReadSome(Chain&) instead";
  const size_t remaining = std::numeric_limits<size_t>::max() - dest.size();
  RIEGELI_CHECK_GT(remaining, 0u)
      << "Failed precondition of Reader::ReadSome(Chain&): "
         "Chain size overflow";
  max_length = UnsignedMin(max_length, remaining);
  if (ABSL_PREDICT_FALSE(!Pull(1, max_length))) return false;
  // Should always succeed.
  return ReadAndAppend(UnsignedMin(max_length, available()), dest);
}

bool Reader::ReadSomeSlow(size_t max_length, Chain& dest, size_t& length_read) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), max_length)
      << "Failed precondition of Reader::ReadSomeSlow(Chain&): "
         "enough data available, use ReadSome(Chain&) instead";
  const Position pos_before = pos();
  const bool read_ok = ReadSomeSlow(max_length, dest);
  RIEGELI_ASSERT_GE(pos(), pos_before)
      << "Reader::ReadSomeSlow(Chain&) decreased pos()";
  RIEGELI_ASSERT_LE(pos() - pos_before, max_length)
      << "Reader::ReadSomeSlow(Chain&) read more than requested";
  length_read = IntCast<size_t>(pos() - pos_before);
  if (!read_ok) {
    RIEGELI_ASSERT_EQ(length_read, 0u)
        << "Reader::ReadSomeSlow(Chain&) failed but read some";
  } else {
    RIEGELI_ASSERT_GT(length_read, 0u)
        << "Reader::ReadSomeSlow(Chain&) succeeded but read none";
  }
  return read_ok;
}

bool Reader::ReadSomeSlow(size_t max_length, absl::Cord& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), max_length)
      << "Failed precondition of Reader::ReadSomeSlow(Cord&): "
         "enough data available, use ReadSome(Cord&) instead";
  const size_t remaining = std::numeric_limits<size_t>::max() - dest.size();
  RIEGELI_CHECK_GT(remaining, 0u)
      << "Failed precondition of Reader::ReadSome(Cord&): "
         "Cord size overflow";
  max_length = UnsignedMin(max_length, remaining);
  if (ABSL_PREDICT_FALSE(!Pull(1, max_length))) return false;
  // Should always succeed.
  return ReadAndAppend(UnsignedMin(max_length, available()), dest);
}

bool Reader::ReadSomeSlow(size_t max_length, absl::Cord& dest,
                          size_t& length_read) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), max_length)
      << "Failed precondition of Reader::ReadSomeSlow(Cord&): "
         "enough data available, use ReadSome(Cord&) instead";
  const Position pos_before = pos();
  const bool read_ok = ReadSomeSlow(max_length, dest);
  RIEGELI_ASSERT_GE(pos(), pos_before)
      << "Reader::ReadSomeSlow(Cord&) decreased pos()";
  RIEGELI_ASSERT_LE(pos() - pos_before, max_length)
      << "Reader::ReadSomeSlow(Cord&) read more than requested";
  length_read = IntCast<size_t>(pos() - pos_before);
  if (!read_ok) {
    RIEGELI_ASSERT_EQ(length_read, 0u)
        << "Reader::ReadSomeSlow(Cord&) failed but read some";
  } else {
    RIEGELI_ASSERT_GT(length_read, 0u)
        << "Reader::ReadSomeSlow(Cord&) succeeded but read none";
  }
  return read_ok;
}

bool Reader::CopySomeSlow(size_t max_length, Writer& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), max_length)
      << "Failed precondition of Reader::CopySomeSlow(Writer&): "
         "enough data available, use CopySome(Writer&) instead";
  size_t length_read;
  const bool read_ok = ReadOrPullSome(
      max_length,
      [&dest](size_t& length) {
        dest.Push(1, length);
        length = UnsignedMin(length, dest.available());
        return dest.cursor();
      },
      &length_read);
  if (length_read > 0) {
    dest.move_cursor(length_read);
    return true;
  }
  if (ABSL_PREDICT_FALSE(!read_ok)) return false;
  RIEGELI_ASSERT_GT(available(), 0u)
      << "Reader::ReadOrPullSome() succeeded but read none and pulled none";
  // Should succeed unless `dest` fails.
  return Copy(UnsignedMin(max_length, available()), dest);
}

bool Reader::CopySomeSlow(size_t max_length, Writer& dest,
                          size_t& length_read) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), max_length)
      << "Failed precondition of Reader::CopySomeSlow(Writer&): "
         "enough data available, use CopySome(Writer&) instead";
  const Position pos_before = pos();
  const bool copy_ok = CopySomeSlow(max_length, dest);
  RIEGELI_ASSERT_GE(pos(), pos_before)
      << "Reader::CopySomeSlow(Writer&) decreased pos()";
  RIEGELI_ASSERT_LE(pos() - pos_before, max_length)
      << "Reader::CopySomeSlow(Writer&) read more than requested";
  length_read = IntCast<size_t>(pos() - pos_before);
  if (!copy_ok) {
    if (dest.ok()) {
      RIEGELI_ASSERT_EQ(length_read, 0u)
          << "Reader::CopySomeSlow(Writer&) failed but read some";
    }
  } else {
    RIEGELI_ASSERT_GT(length_read, 0u)
        << "Reader::CopySomeSlow(Writer&) succeeded but read none";
  }
  return copy_ok;
}

bool Reader::ReadOrPullSomeSlow(size_t max_length,
                                absl::FunctionRef<char*(size_t&)> get_dest) {
  RIEGELI_ASSERT_GT(max_length, 0u)
      << "Failed precondition of Reader::ReadOrPullSomeSlow(): "
         "nothing to read, use ReadOrPullSome() instead";
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of Reader::ReadOrPullSomeSlow(): "
         "some data available, use ReadOrPullSome() instead";
  return PullSlow(1, max_length);
}

bool Reader::ReadOrPullSomeSlow(size_t max_length,
                                absl::FunctionRef<char*(size_t&)> get_dest,
                                size_t& length_read) {
  RIEGELI_ASSERT_GT(max_length, 0u)
      << "Failed precondition of Reader::ReadOrPullSomeSlow(): "
         "nothing to read, use ReadOrPullSome() instead";
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of Reader::ReadOrPullSomeSlow(): "
         "some data available, use ReadOrPullSome() instead";
  const Position pos_before = limit_pos();
  const bool read_ok = ReadOrPullSomeSlow(max_length, get_dest);
  RIEGELI_ASSERT_GE(pos(), pos_before)
      << "Reader::ReadOrPullSomeSlow() decreased pos()";
  RIEGELI_ASSERT_LE(pos() - pos_before, max_length)
      << "Reader::ReadOrPullSomeSlow() read more than requested";
  length_read = IntCast<size_t>(pos() - pos_before);
  if (!read_ok) {
    RIEGELI_ASSERT_EQ(length_read, 0u)
        << "Reader::ReadOrPullSomeSlow() failed but read some";
  } else if (length_read == 0) {
    RIEGELI_ASSERT_GT(available(), 0u)
        << "Reader::ReadOrPullSomeSlow() succeeded but "
           "read none and pulled none";
  } else {
    RIEGELI_ASSERT_EQ(available(), 0u)
        << "Reader::ReadOrPullSomeSlow() succeeded but "
           "read some and pulled some";
  }
  return read_ok;
}

void Reader::ReadHintSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Reader::ReadHintSlow(): "
         "enough data available, use ReadHint() instead";
}

bool Reader::SyncImpl(SyncType sync_type) { return ok(); }

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
  RIEGELI_ASSERT_LE(available_length, start_to_limit())
      << "Reader::PullSlow() skipped some data";
  set_cursor(limit() - available_length);
  return true;
}

std::optional<Position> Reader::SizeImpl() {
  Fail(absl::UnimplementedError("Reader::Size() not supported"));
  return std::nullopt;
}

std::unique_ptr<Reader> Reader::NewReader(Position initial_pos) {
  return NewReaderImpl(initial_pos);
}

std::unique_ptr<Reader> Reader::NewReaderImpl(Position initial_pos) {
  Fail(absl::UnimplementedError("Reader::NewReader() not supported"));
  return nullptr;
}

}  // namespace riegeli
