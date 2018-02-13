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
#include <string>
#include <utility>

#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/string_view.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void ChainWriter::Done() {
  if (RIEGELI_LIKELY(healthy())) {
    RIEGELI_ASSERT_EQ(limit_pos(), dest_->size())
        << "ChainWriter destination changed unexpectedly";
    DiscardBuffer();
  }
  dest_ = nullptr;
  Writer::Done();
}

bool ChainWriter::PushSlow() {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of Writer::PushSlow(): "
         "space available, use Push() instead";
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  RIEGELI_ASSERT_EQ(limit_pos(), dest_->size())
      << "ChainWriter destination changed unexpectedly";
  if (RIEGELI_UNLIKELY(dest_->size() == std::numeric_limits<size_t>::max())) {
    cursor_ = start_;
    limit_ = start_;
    return FailOverflow();
  }
  start_pos_ = dest_->size();
  const Chain::Buffer buffer = dest_->MakeAppendBuffer(1, size_hint_);
  start_ = buffer.data();
  cursor_ = buffer.data();
  limit_ = buffer.data() + buffer.size();
  return true;
}

bool ChainWriter::WriteSlow(string_view src) {
  RIEGELI_ASSERT_GT(src.size(), available())
      << "Failed precondition of Writer::WriteSlow(string_view): "
         "length too small, use Write(string_view) instead";
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  RIEGELI_ASSERT_EQ(limit_pos(), dest_->size())
      << "ChainWriter destination changed unexpectedly";
  if (RIEGELI_UNLIKELY(src.size() > std::numeric_limits<size_t>::max() -
                                        IntCast<size_t>(pos()))) {
    cursor_ = start_;
    limit_ = start_;
    return FailOverflow();
  }
  DiscardBuffer();
  dest_->Append(src, size_hint_);
  MakeBuffer();
  return true;
}

bool ChainWriter::WriteSlow(std::string&& src) {
  RIEGELI_ASSERT_GT(src.size(), UnsignedMin(available(), kMaxBytesToCopy()))
      << "Failed precondition of Writer::WriteSlow(string&&): "
         "length too small, use Write(string&&) instead";
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  RIEGELI_ASSERT_EQ(limit_pos(), dest_->size())
      << "ChainWriter destination changed unexpectedly";
  if (RIEGELI_UNLIKELY(src.size() > std::numeric_limits<size_t>::max() -
                                        IntCast<size_t>(pos()))) {
    cursor_ = start_;
    limit_ = start_;
    return FailOverflow();
  }
  DiscardBuffer();
  dest_->Append(std::move(src), size_hint_);
  MakeBuffer();
  return true;
}

bool ChainWriter::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_GT(src.size(), UnsignedMin(available(), kMaxBytesToCopy()))
      << "Failed precondition of Writer::WriteSlow(Chain): "
         "length too small, use Write(Chain) instead";
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  RIEGELI_ASSERT_EQ(limit_pos(), dest_->size())
      << "ChainWriter destination changed unexpectedly";
  if (RIEGELI_UNLIKELY(src.size() > std::numeric_limits<size_t>::max() -
                                        IntCast<size_t>(pos()))) {
    cursor_ = start_;
    limit_ = start_;
    return FailOverflow();
  }
  DiscardBuffer();
  dest_->Append(src, size_hint_);
  MakeBuffer();
  return true;
}

bool ChainWriter::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_GT(src.size(), UnsignedMin(available(), kMaxBytesToCopy()))
      << "Failed precondition of Writer::WriteSlow(Chain&&): "
         "length too small, use Write(Chain&&) instead";
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  DiscardBuffer();
  dest_->Append(std::move(src), size_hint_);
  MakeBuffer();
  return true;
}

bool ChainWriter::Flush(FlushType flush_type) {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  RIEGELI_ASSERT_EQ(limit_pos(), dest_->size())
      << "ChainWriter destination changed unexpectedly";
  DiscardBuffer();
  start_pos_ = dest_->size();
  start_ = nullptr;
  cursor_ = nullptr;
  limit_ = nullptr;
  return true;
}

inline void ChainWriter::DiscardBuffer() { dest_->RemoveSuffix(available()); }

inline void ChainWriter::MakeBuffer() {
  start_pos_ = dest_->size();
  const Chain::Buffer buffer = dest_->MakeAppendBuffer();
  start_ = buffer.data();
  cursor_ = buffer.data();
  limit_ = buffer.data() + buffer.size();
}

}  // namespace riegeli
