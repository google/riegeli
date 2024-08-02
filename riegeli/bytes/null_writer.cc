// Copyright 2019 Google LLC
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

#include "riegeli/bytes/null_writer.h"

#include <stddef.h>

#include <limits>

#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/byte_fill.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/external_ref.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if !__cpp_inline_variables
constexpr size_t NullWriter::Options::kDefaultMinBufferSize;
constexpr size_t NullWriter::Options::kDefaultMaxBufferSize;
#endif

void NullWriter::Done() {
  Writer::Done();
  buffer_ = Buffer();
}

inline void NullWriter::SyncBuffer() {
  set_start_pos(pos());
  set_cursor(start());
}

inline bool NullWriter::MakeBuffer(size_t min_length,
                                   size_t recommended_length) {
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

void NullWriter::SetWriteSizeHintImpl(
    absl::optional<Position> write_size_hint) {
  buffer_sizer_.set_write_size_hint(pos(), write_size_hint);
}

bool NullWriter::PushSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Writer::PushSlow(): "
         "enough space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  SyncBuffer();
  return MakeBuffer(min_length, recommended_length);
}

bool NullWriter::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain): "
         "enough space available, use Write(Chain) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  SyncBuffer();
  if (ABSL_PREDICT_FALSE(src.size() >
                         std::numeric_limits<Position>::max() - start_pos())) {
    return FailOverflow();
  }
  move_start_pos(src.size());
  return MakeBuffer();
}

bool NullWriter::WriteSlow(const absl::Cord& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Cord): "
         "enough space available, use Write(Cord) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  SyncBuffer();
  if (ABSL_PREDICT_FALSE(src.size() >
                         std::numeric_limits<Position>::max() - start_pos())) {
    return FailOverflow();
  }
  move_start_pos(src.size());
  return MakeBuffer();
}

bool NullWriter::WriteSlow(ExternalRef src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(ExternalRef): "
         "enough space available, use Write(ExternalRef) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  SyncBuffer();
  if (ABSL_PREDICT_FALSE(src.size() >
                         std::numeric_limits<Position>::max() - start_pos())) {
    return FailOverflow();
  }
  move_start_pos(src.size());
  return MakeBuffer();
}

bool NullWriter::WriteSlow(ByteFill src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(ByteFill): "
         "enough space available, use Write(ByteFill) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  SyncBuffer();
  if (ABSL_PREDICT_FALSE(src.size() >
                         std::numeric_limits<Position>::max() - start_pos())) {
    return FailOverflow();
  }
  move_start_pos(src.size());
  return MakeBuffer();
}

bool NullWriter::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT_NE(new_pos, pos())
      << "Failed precondition of Writer::SeekSlow(): "
         "position unchanged, use Seek() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  const Position size = UnsignedMax(pos(), written_size_);
  if (new_pos >= start_pos() && new_pos <= pos()) {
    written_size_ = size;
    set_cursor(start() + IntCast<size_t>(new_pos - start_pos()));
    return true;
  }
  buffer_sizer_.EndRun(pos());
  if (ABSL_PREDICT_FALSE(new_pos > size)) {
    set_start_pos(size);
    buffer_sizer_.BeginRun(start_pos());
    MakeBuffer();
    return false;
  }
  written_size_ = size;
  set_start_pos(new_pos);
  buffer_sizer_.BeginRun(start_pos());
  return MakeBuffer();
}

absl::optional<Position> NullWriter::SizeImpl() {
  if (ABSL_PREDICT_FALSE(!ok())) return absl::nullopt;
  return UnsignedMax(pos(), written_size_);
}

bool NullWriter::TruncateImpl(Position new_size) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  const Position size = UnsignedMax(pos(), written_size_);
  if (new_size >= start_pos() && new_size <= pos()) {
    written_size_ = new_size;
    set_cursor(start() + IntCast<size_t>(new_size - start_pos()));
    return true;
  }
  buffer_sizer_.EndRun(pos());
  if (ABSL_PREDICT_FALSE(new_size > size)) {
    set_start_pos(size);
    buffer_sizer_.BeginRun(start_pos());
    MakeBuffer();
    return false;
  }
  written_size_ = new_size;
  set_start_pos(new_size);
  buffer_sizer_.BeginRun(start_pos());
  return MakeBuffer();
}

}  // namespace riegeli
