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
#include "riegeli/bytes/writer.h"

namespace riegeli {

void BufferedWriter::DoneBehindBuffer(absl::string_view src) {
  RIEGELI_ASSERT_EQ(buffer_size(), 0u)
      << "Failed precondition of BufferedWriter::DoneBehindBuffer():"
         "buffer not empty";
  FlushBehindBuffer(src, FlushType::kFromObject);
}

void BufferedWriter::Done() {
  const absl::string_view src(start(), written_to_buffer());
  set_buffer();
  DoneBehindBuffer(src);
  Writer::Done();
  buffer_ = Buffer();
}

bool BufferedWriter::SyncBuffer() {
  const absl::string_view data(start(), written_to_buffer());
  set_buffer();
  if (data.empty()) return true;
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  return WriteInternal(data);
}

inline size_t BufferedWriter::LengthToWriteDirectly() const {
  // Write directly at least `buffer_size_` of data. Even if the buffer is
  // partially full, this ensures that at least every other write has length at
  // least `buffer_size_`.
  if (pos() < size_hint_ &&
      (written_to_buffer() == 0 || limit_pos() < size_hint_)) {
    // Write directly also if `size_hint_` is reached, as long as the number of
    // writes is not increased.
    return UnsignedMin(buffer_size_, size_hint_ - pos());
  }
  return buffer_size_;
}

bool BufferedWriter::PushSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Writer::PushSlow(): "
         "enough space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(!SyncBuffer())) return false;
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(min_length >
                         std::numeric_limits<Position>::max() - start_pos())) {
    return FailOverflow();
  }
  const size_t buffer_length =
      BufferLength(UnsignedMax(min_length, recommended_length), buffer_size_,
                   size_hint_, start_pos());
  buffer_.Reset(buffer_length);
  set_buffer(buffer_.data(),
             UnsignedMin(buffer_.capacity(),
                         SaturatingAdd(buffer_length, buffer_length),
                         std::numeric_limits<Position>::max() - start_pos()));
  return true;
}

bool BufferedWriter::FlushBehindBuffer(absl::string_view src,
                                       FlushType flush_type) {
  RIEGELI_ASSERT_EQ(buffer_size(), 0u)
      << "Failed precondition of BufferedWriter::FlushBehindBuffer():"
         "buffer not empty";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (src.empty()) return true;
  return WriteInternal(src);
}

bool BufferedWriter::SeekBehindBuffer(Position new_pos) {
  RIEGELI_ASSERT_EQ(buffer_size(), 0u)
      << "Failed precondition of BufferedWriter::SeekBehindBuffer():"
         "buffer not empty";
  return Fail(absl::UnimplementedError("Writer::Seek() not supported"));
}

absl::optional<Position> BufferedWriter::SizeBehindBuffer() {
  RIEGELI_ASSERT_EQ(buffer_size(), 0u)
      << "Failed precondition of BufferedWriter::SizeBehindBuffer():"
         "buffer not empty";
  Fail(absl::UnimplementedError("Writer::Size() not supported"));
  return absl::nullopt;
}

bool BufferedWriter::TruncateBehindBuffer(Position new_size) {
  RIEGELI_ASSERT_EQ(buffer_size(), 0u)
      << "Failed precondition of BufferedWriter::TruncateBehindBuffer():"
         "buffer not empty";
  return Fail(absl::UnimplementedError("Writer::Truncate() not supported"));
}

bool BufferedWriter::WriteSlow(absl::string_view src) {
  RIEGELI_ASSERT_LT(available(), src.size())
      << "Failed precondition of Writer::WriteSlow(string_view): "
         "enough space available, use Write(string_view) instead";
  if (src.size() >= LengthToWriteDirectly()) {
    if (ABSL_PREDICT_FALSE(!SyncBuffer())) return false;
    if (ABSL_PREDICT_FALSE(!healthy())) return false;
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
  const absl::string_view src(start(), written_to_buffer());
  set_buffer();
  return FlushBehindBuffer(src, flush_type);
}

bool BufferedWriter::SeekImpl(Position new_pos) {
  if (ABSL_PREDICT_FALSE(!SyncBuffer())) return false;
  return SeekBehindBuffer(new_pos);
}

absl::optional<Position> BufferedWriter::SizeImpl() {
  if (ABSL_PREDICT_FALSE(!SyncBuffer())) return false;
  return SizeBehindBuffer();
}

bool BufferedWriter::TruncateImpl(Position new_size) {
  if (ABSL_PREDICT_FALSE(!SyncBuffer())) return false;
  return TruncateBehindBuffer(new_size);
}

}  // namespace riegeli
