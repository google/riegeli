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

#include "riegeli/bytes/restricted_chain_writer.h"

#include <stddef.h>

#include <limits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/types/span.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void RestrictedChainWriter::Done() {
  if (ABSL_PREDICT_TRUE(ok())) {
    RIEGELI_ASSERT_EQ(limit_pos(), dest_.size())
        << "RestrictedChainWriter destination changed unexpectedly";
    SyncBuffer();
  }
  Writer::Done();
}

inline void RestrictedChainWriter::SyncBuffer() {
  set_start_pos(pos());
  dest_.RemoveSuffix(available());
  set_buffer();
}

inline void RestrictedChainWriter::MakeBuffer(size_t min_length,
                                              size_t recommended_length) {
  const absl::Span<char> buffer =
      dest_.AppendBuffer(min_length, recommended_length, Chain::kAnyLength);
  set_buffer(buffer.data(), buffer.size());
}

bool RestrictedChainWriter::PushSlow(size_t min_length,
                                     size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Writer::PushSlow(): "
         "enough space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  RIEGELI_ASSERT_EQ(limit_pos(), dest_.size())
      << "RestrictedChainWriter destination changed unexpectedly";
  SyncBuffer();
  if (ABSL_PREDICT_FALSE(min_length > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(start_pos()))) {
    return FailOverflow();
  }
  MakeBuffer(min_length, recommended_length);
  return true;
}

bool RestrictedChainWriter::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain): "
         "enough space available, use Write(Chain) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  RIEGELI_ASSERT_EQ(limit_pos(), dest_.size())
      << "RestrictedChainWriter destination changed unexpectedly";
  SyncBuffer();
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(start_pos()))) {
    return FailOverflow();
  }
  move_start_pos(src.size());
  dest_.Append(src);
  MakeBuffer();
  return true;
}

bool RestrictedChainWriter::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain&&): "
         "enough space available, use Write(Chain&&) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  RIEGELI_ASSERT_EQ(limit_pos(), dest_.size())
      << "RestrictedChainWriter destination changed unexpectedly";
  SyncBuffer();
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(start_pos()))) {
    return FailOverflow();
  }
  move_start_pos(src.size());
  dest_.Append(std::move(src));
  MakeBuffer();
  return true;
}

bool RestrictedChainWriter::WriteSlow(const absl::Cord& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Cord): "
         "enough space available, use Write(Cord) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  RIEGELI_ASSERT_EQ(limit_pos(), dest_.size())
      << "RestrictedChainWriter destination changed unexpectedly";
  SyncBuffer();
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(start_pos()))) {
    return FailOverflow();
  }
  move_start_pos(src.size());
  dest_.Append(src);
  MakeBuffer();
  return true;
}

bool RestrictedChainWriter::WriteSlow(absl::Cord&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Cord&&): "
         "enough space available, use Write(Cord&&) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  RIEGELI_ASSERT_EQ(limit_pos(), dest_.size())
      << "RestrictedChainWriter destination changed unexpectedly";
  SyncBuffer();
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(start_pos()))) {
    return FailOverflow();
  }
  move_start_pos(src.size());
  dest_.Append(std::move(src));
  MakeBuffer();
  return true;
}

bool RestrictedChainWriter::WriteZerosSlow(Position length) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Writer::WriteZerosSlow(): "
         "enough space available, use WriteZeros() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  RIEGELI_ASSERT_EQ(limit_pos(), dest_.size())
      << "RestrictedChainWriter destination changed unexpectedly";
  SyncBuffer();
  if (ABSL_PREDICT_FALSE(length > std::numeric_limits<size_t>::max() -
                                      IntCast<size_t>(start_pos()))) {
    return FailOverflow();
  }
  move_start_pos(length);
  dest_.Append(ChainOfZeros(IntCast<size_t>(length)));
  MakeBuffer();
  return true;
}

}  // namespace riegeli
