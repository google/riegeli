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

#include <array>
#include <limits>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/memory.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void BufferedWriter::DoneBehindBuffer(absl::string_view src) {
  RIEGELI_ASSERT_EQ(start_to_limit(), 0u)
      << "Failed precondition of BufferedWriter::DoneBehindBuffer(): "
         "buffer not empty";
  FlushBehindBuffer(src, FlushType::kFromObject);
}

void BufferedWriter::Done() {
  const absl::string_view src(start(), start_to_cursor());
  set_buffer();
  DoneBehindBuffer(src);
  Writer::Done();
  buffer_ = Buffer();
}

bool BufferedWriter::SyncBuffer() {
  const absl::string_view data(start(), start_to_cursor());
  set_buffer();
  if (data.empty()) return true;
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  return WriteInternal(data);
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
  const size_t buffer_length =
      buffer_sizer_.BufferLength(start_pos(), min_length, recommended_length);
  buffer_.Reset(buffer_length);
  set_buffer(buffer_.data(),
             UnsignedMin(buffer_.capacity(),
                         SaturatingAdd(buffer_length, buffer_length),
                         std::numeric_limits<Position>::max() - start_pos()));
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
  if (src.size() >= buffer_sizer_.LengthToWriteDirectly(pos(), start_to_limit(),
                                                        available())) {
    if (ABSL_PREDICT_FALSE(!SyncBuffer())) return false;
    if (ABSL_PREDICT_FALSE(!ok())) return false;
    return WriteInternal(src);
  }
  return Writer::WriteSlow(src);
}

bool BufferedWriter::WriteZerosSlow(Position length) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Writer::WriteZerosSlow(): "
         "enough space available, use WriteZeros() instead";
  while (length > kArrayOfZeros.size()) {
    const absl::string_view zeros(kArrayOfZeros.data(), kArrayOfZeros.size());
    if (ABSL_PREDICT_FALSE(!Write(zeros))) return false;
    length -= zeros.size();
  }
  const absl::string_view zeros(kArrayOfZeros.data(), IntCast<size_t>(length));
  return Write(zeros);
}

bool BufferedWriter::FlushImpl(FlushType flush_type) {
  buffer_sizer_.EndRun(pos());
  const absl::string_view src(start(), start_to_cursor());
  set_buffer();
  const bool flush_ok = FlushBehindBuffer(src, flush_type);
  buffer_sizer_.BeginRun(start_pos());
  return flush_ok;
}

bool BufferedWriter::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT_NE(new_pos, pos())
      << "Failed precondition of Writer::SeekSlow(): "
         "position unchanged, use Seek() instead";
  buffer_sizer_.EndRun(pos());
  if (ABSL_PREDICT_FALSE(!SyncBuffer())) return false;
  const bool seek_ok = SeekBehindBuffer(new_pos);
  buffer_sizer_.BeginRun(start_pos());
  return seek_ok;
}

absl::optional<Position> BufferedWriter::SizeImpl() {
  if (ABSL_PREDICT_FALSE(!SyncBuffer())) return false;
  return SizeBehindBuffer();
}

bool BufferedWriter::TruncateImpl(Position new_size) {
  buffer_sizer_.EndRun(pos());
  if (ABSL_PREDICT_FALSE(!SyncBuffer())) return false;
  const bool truncate_ok = TruncateBehindBuffer(new_size);
  buffer_sizer_.BeginRun(start_pos());
  return truncate_ok;
}

Reader* BufferedWriter::ReadModeImpl(Position initial_pos) {
  if (ABSL_PREDICT_FALSE(!SyncBuffer())) return nullptr;
  return ReadModeBehindBuffer(initial_pos);
}

}  // namespace riegeli
