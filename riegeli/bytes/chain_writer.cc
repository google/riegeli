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

#include "riegeli/bytes/chain_writer.h"

#include <stddef.h>

#include <limits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/types/span.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void ChainWriterBase::Done() {
  if (ABSL_PREDICT_TRUE(healthy())) {
    Chain& dest = *dest_chain();
    RIEGELI_ASSERT_EQ(limit_pos(), dest.size())
        << "ChainWriter destination changed unexpectedly";
    SyncBuffer(dest);
  }
  Writer::Done();
}

bool ChainWriterBase::PushSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Writer::PushSlow(): "
         "enough space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Chain& dest = *dest_chain();
  RIEGELI_ASSERT_EQ(limit_pos(), dest.size())
      << "ChainWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(min_length >
                         std::numeric_limits<size_t>::max() - dest.size())) {
    return FailOverflow();
  }
  SyncBuffer(dest);
  MakeBuffer(dest, min_length, recommended_length);
  return true;
}

bool ChainWriterBase::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain): "
         "enough space available, use Write(Chain) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Chain& dest = *dest_chain();
  RIEGELI_ASSERT_EQ(limit_pos(), dest.size())
      << "ChainWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  SyncBuffer(dest);
  move_start_pos(src.size());
  dest.Append(src, options_);
  MakeBuffer(dest);
  return true;
}

bool ChainWriterBase::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain&&): "
         "enough space available, use Write(Chain&&) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Chain& dest = *dest_chain();
  RIEGELI_ASSERT_EQ(limit_pos(), dest.size())
      << "ChainWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  SyncBuffer(dest);
  move_start_pos(src.size());
  dest.Append(std::move(src), options_);
  MakeBuffer(dest);
  return true;
}

bool ChainWriterBase::WriteSlow(const absl::Cord& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Cord): "
         "enough space available, use Write(Cord) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Chain& dest = *dest_chain();
  RIEGELI_ASSERT_EQ(limit_pos(), dest.size())
      << "ChainWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  SyncBuffer(dest);
  move_start_pos(src.size());
  dest.Append(src, options_);
  MakeBuffer(dest);
  return true;
}

bool ChainWriterBase::WriteSlow(absl::Cord&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Cord&&): "
         "enough space available, use Write(Cord&&) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Chain& dest = *dest_chain();
  RIEGELI_ASSERT_EQ(limit_pos(), dest.size())
      << "ChainWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  SyncBuffer(dest);
  move_start_pos(src.size());
  dest.Append(std::move(src), options_);
  MakeBuffer(dest);
  return true;
}

bool ChainWriterBase::WriteZerosSlow(Position length) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Writer::WriteZerosSlow(): "
         "enough space available, use WriteZeros() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Chain& dest = *dest_chain();
  RIEGELI_ASSERT_EQ(limit_pos(), dest.size())
      << "ChainWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(length > std::numeric_limits<size_t>::max() -
                                      IntCast<size_t>(pos()))) {
    return FailOverflow();
  }
  SyncBuffer(dest);
  move_start_pos(length);
  dest.Append(ChainOfZeros(IntCast<size_t>(length)), options_);
  MakeBuffer(dest);
  return true;
}

bool ChainWriterBase::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Chain& dest = *dest_chain();
  RIEGELI_ASSERT_EQ(limit_pos(), dest.size())
      << "ChainWriter destination changed unexpectedly";
  SyncBuffer(dest);
  return true;
}

bool ChainWriterBase::Truncate(Position new_size) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Chain& dest = *dest_chain();
  RIEGELI_ASSERT_EQ(limit_pos(), dest.size())
      << "ChainWriter destination changed unexpectedly";
  if (new_size >= start_pos()) {
    if (ABSL_PREDICT_FALSE(new_size > pos())) return false;
    set_cursor(start() + (new_size - start_pos()));
    return true;
  }
  set_start_pos(new_size);
  dest.RemoveSuffix(dest.size() - IntCast<size_t>(new_size));
  set_buffer();
  return true;
}

inline void ChainWriterBase::SyncBuffer(Chain& dest) {
  set_start_pos(pos());
  dest.RemoveSuffix(available());
  set_buffer();
}

inline void ChainWriterBase::MakeBuffer(Chain& dest, size_t min_length,
                                        size_t recommended_length) {
  const absl::Span<char> buffer = dest.AppendBuffer(
      min_length, recommended_length, Chain::kAnyLength, options_);
  set_buffer(buffer.data(), buffer.size());
}

}  // namespace riegeli
