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

#include "riegeli/bytes/buffered_writer.h"

#include <stddef.h>

#include <cstring>
#include <limits>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/byte_fill.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void BufferedWriter::Done() {
  const absl::string_view src(start(),
                              UnsignedMax(start_to_cursor(), written_));
  const Position new_pos = pos();
  set_buffer();
  written_ = 0;
  DoneBehindBuffer(src);
  if (ABSL_PREDICT_FALSE(start_pos() != new_pos) && ABSL_PREDICT_TRUE(ok())) {
    SeekBehindBuffer(new_pos);
  }
  Writer::Done();
  buffer_ = Buffer();
}

void BufferedWriter::DoneBehindBuffer(absl::string_view src) {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::DoneBehindBuffer(): "
         "buffer not empty";
  FlushBehindBuffer(src, FlushType::kFromObject);
}

inline bool BufferedWriter::SyncBuffer() {
  const absl::string_view data(start(),
                               UnsignedMax(start_to_cursor(), written_));
  const Position new_pos = pos();
  set_buffer();
  written_ = 0;
  if (data.empty()) return true;
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (ABSL_PREDICT_FALSE(!WriteInternal(data))) return false;
  if (ABSL_PREDICT_FALSE(start_pos() != new_pos)) {
    return SeekBehindBuffer(new_pos);
  }
  return true;
}

void BufferedWriter::SetWriteSizeHintImpl(
    absl::optional<Position> write_size_hint) {
  buffer_sizer_.set_write_size_hint(pos(), write_size_hint);
}

bool BufferedWriter::PushSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Writer::PushSlow(): "
         "enough space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(!SyncBuffer())) return false;
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (ABSL_PREDICT_FALSE(min_length >
                         std::numeric_limits<Position>::max() - start_pos())) {
    return FailOverflow();
  }
  const size_t buffer_length = UnsignedMin(
      buffer_sizer_.BufferLength(start_pos(), min_length, recommended_length),
      std::numeric_limits<Position>::max() - start_pos());
  buffer_.Reset(buffer_length);
  set_buffer(buffer_.data(), buffer_length);
  return true;
}

bool BufferedWriter::FlushBehindBuffer(absl::string_view src,
                                       FlushType flush_type) {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::FlushBehindBuffer(): "
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (src.empty()) return true;
  return WriteInternal(src);
}

bool BufferedWriter::SeekBehindBuffer(Position new_pos) {
  RIEGELI_ASSERT_NE(new_pos, pos())
      << "Failed precondition of BufferedWriter::SeekBehindBuffer(): "
         "position unchanged, use Seek() instead";
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::SeekBehindBuffer(): "
         "buffer not empty";
  return Fail(absl::UnimplementedError("Writer::Seek() not supported"));
}

absl::optional<Position> BufferedWriter::SizeBehindBuffer() {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::SizeBehindBuffer(): "
         "buffer not empty";
  Fail(absl::UnimplementedError("Writer::Size() not supported"));
  return absl::nullopt;
}

bool BufferedWriter::TruncateBehindBuffer(Position new_size) {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::TruncateBehindBuffer(): "
         "buffer not empty";
  return Fail(absl::UnimplementedError("Writer::Truncate() not supported"));
}

Reader* BufferedWriter::ReadModeBehindBuffer(Position initial_pos) {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::ReadModeBehindBuffer(): "
         "buffer not empty";
  Fail(absl::UnimplementedError("Writer::ReadMode() not supported"));
  return nullptr;
}

bool BufferedWriter::WriteSlow(absl::string_view src) {
  RIEGELI_ASSERT_LT(available(), src.size())
      << "Failed precondition of Writer::WriteSlow(string_view): "
         "enough space available, use Write(string_view) instead";
  if (src.size() >= buffer_sizer_.BufferLength(pos())) {
    // Write directly from `src`.
    if (ABSL_PREDICT_FALSE(!SyncBuffer())) return false;
    if (ABSL_PREDICT_FALSE(!ok())) return false;
    return WriteInternal(src);
  }
  return Writer::WriteSlow(src);
}

bool BufferedWriter::WriteSlow(ByteFill src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(ByteFill): "
         "enough space available, use Write(ByteFill) instead";
  for (const absl::string_view fragment : src.blocks()) {
    if (ABSL_PREDICT_FALSE(!Write(fragment))) return false;
  }
  return true;
}

bool BufferedWriter::FlushImpl(FlushType flush_type) {
  buffer_sizer_.EndRun(start_pos() + UnsignedMax(start_to_cursor(), written_));
  const absl::string_view src(start(),
                              UnsignedMax(start_to_cursor(), written_));
  const Position new_pos = pos();
  set_buffer();
  written_ = 0;
  if (ABSL_PREDICT_FALSE(!FlushBehindBuffer(src, flush_type))) return false;
  const bool result = start_pos() == new_pos || SeekBehindBuffer(new_pos);
  buffer_sizer_.BeginRun(start_pos());
  return result;
}

bool BufferedWriter::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT_NE(new_pos, pos())
      << "Failed precondition of Writer::SeekSlow(): "
         "position unchanged, use Seek() instead";
  if (ABSL_PREDICT_TRUE(
          SupportsRandomAccess() && new_pos >= start_pos() &&
          new_pos <= start_pos() + UnsignedMax(start_to_cursor(), written_))) {
    written_ = UnsignedMax(start_to_cursor(), written_);
    set_cursor(start() + IntCast<size_t>(new_pos - start_pos()));
    return true;
  }
  buffer_sizer_.EndRun(start_pos() + UnsignedMax(start_to_cursor(), written_));
  if (ABSL_PREDICT_FALSE(!SyncBuffer())) return false;
  const bool result = SeekBehindBuffer(new_pos);
  buffer_sizer_.BeginRun(start_pos());
  return result;
}

absl::optional<Position> BufferedWriter::SizeImpl() {
  buffer_sizer_.EndRun(start_pos() + UnsignedMax(start_to_cursor(), written_));
  if (ABSL_PREDICT_FALSE(!SyncBuffer())) return absl::nullopt;
  const absl::optional<Position> size = SizeBehindBuffer();
  if (ABSL_PREDICT_FALSE(size == absl::nullopt)) return absl::nullopt;
  buffer_sizer_.BeginRun(start_pos());
  return *size;
}

bool BufferedWriter::TruncateImpl(Position new_size) {
  buffer_sizer_.EndRun(start_pos() + UnsignedMax(start_to_cursor(), written_));
  if (ABSL_PREDICT_FALSE(!SyncBuffer())) return false;
  const bool result = TruncateBehindBuffer(new_size);
  buffer_sizer_.BeginRun(start_pos());
  return result;
}

Reader* BufferedWriter::ReadModeImpl(Position initial_pos) {
  buffer_sizer_.EndRun(start_pos() + UnsignedMax(start_to_cursor(), written_));
  if (ABSL_PREDICT_FALSE(!SyncBuffer())) return nullptr;
  Reader* const reader = ReadModeBehindBuffer(initial_pos);
  if (ABSL_PREDICT_FALSE(reader == nullptr)) return nullptr;
  buffer_sizer_.BeginRun(start_pos());
  return reader;
}

}  // namespace riegeli
