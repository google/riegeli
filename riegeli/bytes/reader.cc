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
#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/cord_buffer.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/chain.h"
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
      const absl::optional<Position> size = Size();
      if (size != absl::nullopt) {
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
  ResizeStringAmortized(dest, dest_pos + length);
  const Position pos_before = pos();
  const bool read_ok = ReadSlow(length, &dest[dest_pos]);
  RIEGELI_ASSERT_GE(pos(), pos_before)
      << "Reader::ReadSlow(char*) decreased pos()";
  RIEGELI_ASSERT_LE(pos() - pos_before, length)
      << "Reader::ReadSlow(char*) read more than requested";
  if (ABSL_PREDICT_FALSE(!read_ok)) {
    dest.erase(dest_pos + IntCast<size_t>(pos() - pos_before));
    return false;
  }
  RIEGELI_ASSERT_EQ(pos() - pos_before, length)
      << "Reader::ReadSlow(char*) succeeded but read less than requested";
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
    size_t length_read;
    if (ABSL_PREDICT_FALSE(!Read(buffer.size(), buffer.data(), &length_read))) {
      dest.RemoveSuffix(buffer.size() - length_read);
      return false;
    }
    length -= length_read;
  } while (length > 0);
  return true;
}

// `absl::Cord::GetCustomAppendBuffer()` was introduced after Abseil LTS version
// 20220623. Use it if available, with fallback code if not.

namespace {

template <typename T, typename Enable = void>
struct HasGetCustomAppendBuffer : std::false_type {};

template <typename T>
struct HasGetCustomAppendBuffer<
    T, absl::void_t<decltype(std::declval<T&>().GetCustomAppendBuffer(
           std::declval<size_t>(), std::declval<size_t>(),
           std::declval<size_t>()))>> : std::true_type {};

template <
    typename DependentCord = absl::Cord,
    std::enable_if_t<HasGetCustomAppendBuffer<DependentCord>::value, int> = 0>
inline bool ReadSlowToCord(Reader& src, size_t length, DependentCord& dest) {
  static constexpr size_t kCordBufferBlockSize =
      UnsignedMin(kDefaultMaxBlockSize, absl::CordBuffer::kCustomLimit);
  absl::CordBuffer buffer =
      dest.GetCustomAppendBuffer(kCordBufferBlockSize, length, 1);
  absl::Span<char> span = buffer.available_up_to(length);
  if (buffer.capacity() < kDefaultMinBlockSize && length > span.size()) {
    absl::CordBuffer new_buffer = absl::CordBuffer::CreateWithCustomLimit(
        kCordBufferBlockSize, buffer.length() + length);
    std::memcpy(new_buffer.data(), buffer.data(), buffer.length());
    new_buffer.SetLength(buffer.length());
    buffer = std::move(new_buffer);
    span = buffer.available_up_to(length);
  }
  for (;;) {
    size_t length_read;
    const bool read_ok = src.Read(span.size(), span.data(), &length_read);
    buffer.IncreaseLengthBy(length_read);
    dest.Append(std::move(buffer));
    if (ABSL_PREDICT_FALSE(!read_ok)) return false;
    length -= length_read;
    if (length == 0) return true;
    buffer =
        absl::CordBuffer::CreateWithCustomLimit(kCordBufferBlockSize, length);
    span = buffer.available_up_to(length);
  }
}

template <
    typename DependentCord = absl::Cord,
    std::enable_if_t<!HasGetCustomAppendBuffer<DependentCord>::value, int> = 0>
inline bool ReadSlowToCord(Reader& src, size_t length, DependentCord& dest) {
  Buffer buffer;
  do {
    buffer.Reset(UnsignedMin(length, kDefaultMaxBlockSize));
    const size_t length_to_read = UnsignedMin(length, buffer.capacity());
    size_t length_read;
    const bool read_ok = src.Read(length_to_read, buffer.data(), &length_read);
    const char* const data = buffer.data();
    std::move(buffer).AppendSubstrTo(data, length_read, dest);
    if (ABSL_PREDICT_FALSE(!read_ok)) return false;
    length -= length_read;
  } while (length > 0);
  return true;
}

}  // namespace

bool Reader::ReadSlow(size_t length, absl::Cord& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::ReadSlow(Cord&): "
         "enough data available, use Read(Cord&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadSlow(Cord&): "
         "Cord size overflow";
  return ReadSlowToCord(*this, length, dest);
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

absl::optional<Position> Reader::SizeImpl() {
  Fail(absl::UnimplementedError("Reader::Size() not supported"));
  return absl::nullopt;
}

std::unique_ptr<Reader> Reader::NewReader(Position initial_pos) {
  return NewReaderImpl(initial_pos);
}

std::unique_ptr<Reader> Reader::NewReaderImpl(Position initial_pos) {
  Fail(absl::UnimplementedError("Reader::NewReader() not supported"));
  return nullptr;
}

}  // namespace riegeli
