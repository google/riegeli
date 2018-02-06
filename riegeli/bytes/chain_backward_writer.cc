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

#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/base/string_view.h"
#include "riegeli/bytes/backward_writer.h"

namespace riegeli {

ChainBackwardWriter::ChainBackwardWriter() noexcept
    : BackwardWriter(State::kClosed) {}

ChainBackwardWriter::ChainBackwardWriter(Chain* dest, Options options)
    : BackwardWriter(State::kOpen),
      dest_(RIEGELI_ASSERT_NOTNULL(dest)),
      size_hint_(
          UnsignedMin(options.size_hint_, std::numeric_limits<size_t>::max())) {
  start_pos_ = dest->size();
}

ChainBackwardWriter::ChainBackwardWriter(ChainBackwardWriter&& src) noexcept
    : BackwardWriter(std::move(src)),
      dest_(riegeli::exchange(src.dest_, nullptr)),
      size_hint_(riegeli::exchange(src.size_hint_, 0)) {}

ChainBackwardWriter& ChainBackwardWriter::operator=(
    ChainBackwardWriter&& src) noexcept {
  BackwardWriter::operator=(std::move(src));
  dest_ = riegeli::exchange(src.dest_, nullptr);
  size_hint_ = riegeli::exchange(src.size_hint_, 0);
  return *this;
}

ChainBackwardWriter::~ChainBackwardWriter() = default;

void ChainBackwardWriter::Done() {
  if (RIEGELI_LIKELY(healthy())) {
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
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  RIEGELI_ASSERT_EQ(limit_pos(), dest_->size())
      << "ChainBackwardWriter destination changed unexpectedly";
  if (RIEGELI_UNLIKELY(dest_->size() == std::numeric_limits<size_t>::max())) {
    cursor_ = start_;
    limit_ = start_;
    return FailOverflow();
  }
  start_pos_ = dest_->size();
  const Chain::Buffer buffer = dest_->MakePrependBuffer(1, size_hint_);
  start_ = buffer.data() + buffer.size();
  cursor_ = buffer.data() + buffer.size();
  limit_ = buffer.data();
  return true;
}

bool ChainBackwardWriter::WriteSlow(string_view src) {
  RIEGELI_ASSERT_GT(src.size(), available())
      << "Failed precondition of BackwardWriter::WriteSlow(string_view): "
         "length too small, use Write(string_view) instead";
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  RIEGELI_ASSERT_EQ(limit_pos(), dest_->size())
      << "ChainBackwardWriter destination changed unexpectedly";
  if (RIEGELI_UNLIKELY(src.size() > std::numeric_limits<size_t>::max() -
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
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  RIEGELI_ASSERT_EQ(limit_pos(), dest_->size())
      << "ChainBackwardWriter destination changed unexpectedly";
  if (RIEGELI_UNLIKELY(src.size() > std::numeric_limits<size_t>::max() -
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
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  RIEGELI_ASSERT_EQ(limit_pos(), dest_->size())
      << "ChainBackwardWriter destination changed unexpectedly";
  if (RIEGELI_UNLIKELY(src.size() > std::numeric_limits<size_t>::max() -
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
  if (RIEGELI_UNLIKELY(!healthy())) return false;
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
  const Chain::Buffer buffer = dest_->MakePrependBuffer();
  start_ = buffer.data() + buffer.size();
  cursor_ = buffer.data() + buffer.size();
  limit_ = buffer.data();
}

}  // namespace riegeli
