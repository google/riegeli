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

#include "riegeli/bytes/cord_backward_writer.h"

#include <stddef.h>

#include <limits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/backward_writer.h"

namespace riegeli {

void CordBackwardWriterBase::Done() {
  if (ABSL_PREDICT_TRUE(healthy())) {
    absl::Cord* const dest = dest_cord();
    RIEGELI_ASSERT_EQ(start_pos(), dest->size())
        << "CordBackwardWriter destination changed unexpectedly";
    SyncBuffer(dest);
  }
  BackwardWriter::Done();
}

bool CordBackwardWriterBase::PushSlow(size_t min_length,
                                      size_t recommended_length) {
  RIEGELI_ASSERT_GT(min_length, available())
      << "Failed precondition of BackwardWriter::PushSlow(): "
         "length too small, use Push() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  absl::Cord* const dest = dest_cord();
  RIEGELI_ASSERT_EQ(start_pos(), dest->size())
      << "CordBackwardWriter destination changed unexpectedly";
  SyncBuffer(dest);
  return MakeBuffer(dest, min_length, recommended_length);
}

bool CordBackwardWriterBase::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_GT(src.size(), UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of BackwardWriter::WriteSlow(Chain): "
         "length too small, use Write(Chain) instead";
  if (src.size() <= kMaxBytesToCopy) return BackwardWriter::WriteSlow(src);
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  absl::Cord* const dest = dest_cord();
  RIEGELI_ASSERT_EQ(start_pos(), dest->size())
      << "CordBackwardWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  SyncBuffer(dest);
  move_start_pos(src.size());
  src.PrependTo(dest);
  return MakeBuffer(dest);
}

bool CordBackwardWriterBase::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_GT(src.size(), UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of BackwardWriter::WriteSlow(Chain&&): "
         "length too small, use Write(Chain&&) instead";
  if (src.size() <= kMaxBytesToCopy) {
    // Not `std::move(src)`: forward to
    // `BackwardWriter::WriteSlow(const Chain&)`, because
    // `BackwardWriter::WriteSlow(Chain&&)` would forward to
    // `CordBackwardWriter::WriteSlow(const Chain&)`.
    return BackwardWriter::WriteSlow(src);
  }
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  absl::Cord* const dest = dest_cord();
  RIEGELI_ASSERT_EQ(start_pos(), dest->size())
      << "CordBackwardWriter destination changed unexpectedly";
  SyncBuffer(dest);
  move_start_pos(src.size());
  std::move(src).PrependTo(dest);
  return MakeBuffer(dest);
}

bool CordBackwardWriterBase::WriteSlow(const absl::Cord& src) {
  RIEGELI_ASSERT_GT(src.size(), UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of BackwardWriter::WriteSlow(Cord): "
         "length too small, use Write(Cord) instead";
  if (src.size() <= kMaxBytesToCopy) return BackwardWriter::WriteSlow(src);
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  absl::Cord* const dest = dest_cord();
  RIEGELI_ASSERT_EQ(start_pos(), dest->size())
      << "CordBackwardWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  SyncBuffer(dest);
  move_start_pos(src.size());
  dest->Prepend(src);
  return MakeBuffer(dest);
}

bool CordBackwardWriterBase::Flush(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  absl::Cord* const dest = dest_cord();
  RIEGELI_ASSERT_EQ(start_pos(), dest->size())
      << "CordBackwardWriter destination changed unexpectedly";
  SyncBuffer(dest);
  return true;
}

bool CordBackwardWriterBase::Truncate(Position new_size) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  absl::Cord* const dest = dest_cord();
  RIEGELI_ASSERT_EQ(start_pos(), dest->size())
      << "CordBackwardWriter destination changed unexpectedly";
  if (new_size >= start_pos()) {
    if (ABSL_PREDICT_FALSE(new_size > pos())) return false;
    set_cursor(start() - (new_size - start_pos()));
    return true;
  }
  set_start_pos(new_size);
  dest->RemovePrefix(dest->size() - IntCast<size_t>(new_size));
  set_cursor(start());
  return true;
}

inline void CordBackwardWriterBase::SyncBuffer(absl::Cord* dest) {
  const size_t buffered_length = written_to_buffer();
  if (buffered_length == 0) return;
  set_start_pos(pos());
  dest->Prepend(
      BufferToCord(absl::string_view(cursor(), buffered_length), &buffer_));
  if (!buffer_.is_allocated()) set_buffer();
  set_cursor(start());
}

inline bool CordBackwardWriterBase::MakeBuffer(absl::Cord* dest,
                                               size_t min_length,
                                               size_t recommended_length) {
  if (ABSL_PREDICT_FALSE(min_length >
                         std::numeric_limits<size_t>::max() - dest->size())) {
    return FailOverflow();
  }
  buffer_.Resize(BufferLength(
      min_length, max_block_size_, size_hint_, start_pos(),
      UnsignedMax(recommended_length, start_pos(), min_block_size_)));
  char* const buffer = buffer_.GetData();
  set_buffer(buffer,
             UnsignedMin(buffer_.size(),
                         std::numeric_limits<size_t>::max() - dest->size()));
  return true;
}

}  // namespace riegeli
