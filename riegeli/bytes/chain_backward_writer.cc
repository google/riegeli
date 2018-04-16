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
#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/backward_writer.h"

namespace riegeli {

void ChainBackwardWriter::Done() {
  if (ABSL_PREDICT_TRUE(healthy())) {
    RIEGELI_ASSERT_EQ(limit_pos(), dest_->size())
        << "ChainBackwardWriter destination changed unexpectedly";
    DiscardBuffer();
  }
  dest_ = nullptr;
  BackwardWriter::Done();
}

bool ChainBackwardWriter::PushSlow() {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of BackwardWriter::PushSlow(): "
         "space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  RIEGELI_ASSERT_EQ(limit_pos(), dest_->size())
      << "ChainBackwardWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(dest_->size() == std::numeric_limits<size_t>::max())) {
    cursor_ = start_;
    limit_ = start_;
    return FailOverflow();
  }
  start_pos_ = dest_->size();
  const absl::Span<char> buffer = dest_->PrependBuffer(1, 0, size_hint_);
  start_ = buffer.data() + buffer.size();
  cursor_ = buffer.data() + buffer.size();
  limit_ = buffer.data();
  return true;
}

bool ChainBackwardWriter::WriteSlow(absl::string_view src) {
  RIEGELI_ASSERT_GT(src.size(), available())
      << "Failed precondition of BackwardWriter::WriteSlow(string_view): "
         "length too small, use Write(string_view) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  RIEGELI_ASSERT_EQ(limit_pos(), dest_->size())
      << "ChainBackwardWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    cursor_ = start_;
    limit_ = start_;
    return FailOverflow();
  }
  DiscardBuffer();
  dest_->Prepend(src, size_hint_);
  MakeBuffer();
  return true;
}

bool ChainBackwardWriter::WriteSlow(std::string&& src) {
  RIEGELI_ASSERT_GT(src.size(), UnsignedMin(available(), kMaxBytesToCopy()))
      << "Failed precondition of BackwardWriter::WriteSlow(string&&): "
         "length too small, use Write(string&&) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  RIEGELI_ASSERT_EQ(limit_pos(), dest_->size())
      << "ChainBackwardWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    cursor_ = start_;
    limit_ = start_;
    return FailOverflow();
  }
  DiscardBuffer();
  dest_->Prepend(std::move(src), size_hint_);
  MakeBuffer();
  return true;
}

bool ChainBackwardWriter::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_GT(src.size(), UnsignedMin(available(), kMaxBytesToCopy()))
      << "Failed precondition of BackwardWriter::WriteSlow(Chain): "
         "length too small, use Write(Chain) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  RIEGELI_ASSERT_EQ(limit_pos(), dest_->size())
      << "ChainBackwardWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max() -
                                          IntCast<size_t>(pos()))) {
    cursor_ = start_;
    limit_ = start_;
    return FailOverflow();
  }
  DiscardBuffer();
  dest_->Prepend(src, size_hint_);
  MakeBuffer();
  return true;
}

bool ChainBackwardWriter::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_GT(src.size(), UnsignedMin(available(), kMaxBytesToCopy()))
      << "Failed precondition of BackwardWriter::WriteSlow(Chain&&): "
         "length too small, use Write(Chain&&) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  DiscardBuffer();
  dest_->Prepend(std::move(src), size_hint_);
  MakeBuffer();
  return true;
}

inline void ChainBackwardWriter::DiscardBuffer() {
  dest_->RemovePrefix(available());
}

inline void ChainBackwardWriter::MakeBuffer() {
  start_pos_ = dest_->size();
  const absl::Span<char> buffer = dest_->PrependBuffer(0, 0, size_hint_);
  start_ = buffer.data() + buffer.size();
  cursor_ = buffer.data() + buffer.size();
  limit_ = buffer.data();
}

}  // namespace riegeli
