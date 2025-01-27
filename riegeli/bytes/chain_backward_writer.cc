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

#include "riegeli/bytes/chain_backward_writer.h"

#include <stddef.h>

#include <limits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/byte_fill.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/external_ref.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/backward_writer.h"

namespace riegeli {

void ChainBackwardWriterBase::Done() {
  ChainBackwardWriterBase::FlushImpl(FlushType::kFromObject);
  BackwardWriter::Done();
}

inline void ChainBackwardWriterBase::SyncBuffer(Chain& dest) {
  set_start_pos(pos());
  dest.RemovePrefix(available(), options_);
  set_buffer();
}

inline void ChainBackwardWriterBase::MakeBuffer(Chain& dest, size_t min_length,
                                                size_t recommended_length) {
  const absl::Span<char> buffer = dest.PrependBuffer(
      min_length, recommended_length, Chain::kAnyLength, options_);
  set_buffer(buffer.data(), buffer.size());
}

void ChainBackwardWriterBase::SetWriteSizeHintImpl(
    absl::optional<Position> write_size_hint) {
  if (write_size_hint == absl::nullopt) {
    options_.set_size_hint(absl::nullopt);
  } else {
    options_.set_size_hint(
        SaturatingIntCast<size_t>(SaturatingAdd(pos(), *write_size_hint)));
  }
}

bool ChainBackwardWriterBase::PushSlow(size_t min_length,
                                       size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of BackwardWriter::PushSlow(): "
         "enough space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Chain& dest = *DestChain();
  RIEGELI_ASSERT_EQ(limit_pos(), dest.size())
      << "ChainBackwardWriter destination changed unexpectedly";
  SyncBuffer(dest);
  if (ABSL_PREDICT_FALSE(min_length > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(start_pos()))) {
    return FailOverflow();
  }
  MakeBuffer(dest, min_length, recommended_length);
  return true;
}

bool ChainBackwardWriterBase::WriteSlow(ExternalRef src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(ExternalRef): "
         "enough space available, use Write(ExternalRef) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Chain& dest = *DestChain();
  RIEGELI_ASSERT_EQ(limit_pos(), dest.size())
      << "ChainBackwardWriter destination changed unexpectedly";
  SyncBuffer(dest);
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(start_pos()))) {
    return FailOverflow();
  }
  move_start_pos(src.size());
  dest.Prepend(std::move(src), options_);
  MakeBuffer(dest);
  return true;
}

bool ChainBackwardWriterBase::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Chain): "
         "enough space available, use Write(Chain) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Chain& dest = *DestChain();
  RIEGELI_ASSERT_EQ(limit_pos(), dest.size())
      << "ChainBackwardWriter destination changed unexpectedly";
  SyncBuffer(dest);
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(start_pos()))) {
    return FailOverflow();
  }
  move_start_pos(src.size());
  dest.Prepend(src, options_);
  MakeBuffer(dest);
  return true;
}

bool ChainBackwardWriterBase::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Chain&&): "
         "enough space available, use Write(Chain&&) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Chain& dest = *DestChain();
  RIEGELI_ASSERT_EQ(limit_pos(), dest.size())
      << "ChainBackwardWriter destination changed unexpectedly";
  SyncBuffer(dest);
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(start_pos()))) {
    return FailOverflow();
  }
  move_start_pos(src.size());
  dest.Prepend(std::move(src), options_);
  MakeBuffer(dest);
  return true;
}

bool ChainBackwardWriterBase::WriteSlow(const absl::Cord& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Cord): "
         "enough space available, use Write(Cord) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Chain& dest = *DestChain();
  RIEGELI_ASSERT_EQ(limit_pos(), dest.size())
      << "ChainBackwardWriter destination changed unexpectedly";
  SyncBuffer(dest);
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(start_pos()))) {
    return FailOverflow();
  }
  move_start_pos(src.size());
  dest.Prepend(src, options_);
  MakeBuffer(dest);
  return true;
}

bool ChainBackwardWriterBase::WriteSlow(ByteFill src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(ByteFill): "
         "enough space available, use Write(ByteFill) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Chain& dest = *DestChain();
  RIEGELI_ASSERT_EQ(limit_pos(), dest.size())
      << "ChainBackwardWriter destination changed unexpectedly";
  SyncBuffer(dest);
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(start_pos()))) {
    return FailOverflow();
  }
  move_start_pos(src.size());
  src.PrependTo(dest, options_);
  MakeBuffer(dest);
  return true;
}

bool ChainBackwardWriterBase::WriteSlow(absl::Cord&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Cord&&): "
         "enough space available, use Write(Cord&&) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Chain& dest = *DestChain();
  RIEGELI_ASSERT_EQ(limit_pos(), dest.size())
      << "ChainBackwardWriter destination changed unexpectedly";
  SyncBuffer(dest);
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(start_pos()))) {
    return FailOverflow();
  }
  move_start_pos(src.size());
  dest.Prepend(std::move(src), options_);
  MakeBuffer(dest);
  return true;
}

bool ChainBackwardWriterBase::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Chain& dest = *DestChain();
  RIEGELI_ASSERT_EQ(limit_pos(), dest.size())
      << "ChainBackwardWriter destination changed unexpectedly";
  SyncBuffer(dest);
  return true;
}

bool ChainBackwardWriterBase::TruncateImpl(Position new_size) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Chain& dest = *DestChain();
  RIEGELI_ASSERT_EQ(limit_pos(), dest.size())
      << "ChainBackwardWriter destination changed unexpectedly";
  if (new_size >= start_pos()) {
    if (ABSL_PREDICT_FALSE(new_size > pos())) return false;
    set_cursor(start() - (new_size - start_pos()));
    return true;
  }
  set_start_pos(new_size);
  dest.RemovePrefix(dest.size() - IntCast<size_t>(new_size), options_);
  set_buffer();
  return true;
}

}  // namespace riegeli
