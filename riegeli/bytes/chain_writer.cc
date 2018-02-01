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

#include "riegeli/base/assert.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/base/string_view.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

ChainWriter::ChainWriter() noexcept : Writer(State::kClosed) {}

ChainWriter::ChainWriter(Chain* dest, Options options)
    : Writer(State::kOpen),
      dest_(RIEGELI_ASSERT_NOTNULL(dest)),
      size_hint_(
          UnsignedMin(options.size_hint_, std::numeric_limits<size_t>::max())) {
  start_pos_ = dest->size();
}

ChainWriter::ChainWriter(ChainWriter&& src) noexcept
    : Writer(std::move(src)),
      dest_(riegeli::exchange(src.dest_, nullptr)),
      size_hint_(riegeli::exchange(src.size_hint_, 0)) {}

ChainWriter& ChainWriter::operator=(ChainWriter&& src) noexcept {
  Writer::operator=(std::move(src));
  dest_ = riegeli::exchange(src.dest_, nullptr);
  size_hint_ = riegeli::exchange(src.size_hint_, 0);
  return *this;
}

ChainWriter::~ChainWriter() = default;

void ChainWriter::Done() {
  if (RIEGELI_LIKELY(healthy())) DiscardBuffer();
  dest_ = nullptr;
  Writer::Done();
}

bool ChainWriter::PushSlow() {
  RIEGELI_ASSERT_EQ(available(), 0u);
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  start_pos_ = dest_->size();
  const Chain::Buffer buffer = dest_->MakeAppendBuffer(1, size_hint_);
  start_ = buffer.data();
  cursor_ = buffer.data();
  limit_ = buffer.data() + buffer.size();
  return true;
}

bool ChainWriter::WriteSlow(string_view src) {
  RIEGELI_ASSERT_GT(src.size(), available());
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  DiscardBuffer();
  dest_->Append(src, size_hint_);
  MakeBuffer();
  return true;
}

bool ChainWriter::WriteSlow(std::string&& src) {
  RIEGELI_ASSERT_GT(src.size(), UnsignedMin(available(), kMaxBytesToCopy()));
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  DiscardBuffer();
  dest_->Append(std::move(src), size_hint_);
  MakeBuffer();
  return true;
}

bool ChainWriter::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_GT(src.size(), UnsignedMin(available(), kMaxBytesToCopy()));
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  DiscardBuffer();
  dest_->Append(src, size_hint_);
  MakeBuffer();
  return true;
}

bool ChainWriter::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_GT(src.size(), UnsignedMin(available(), kMaxBytesToCopy()));
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  DiscardBuffer();
  dest_->Append(std::move(src), size_hint_);
  MakeBuffer();
  return true;
}

bool ChainWriter::Flush(FlushType flush_type) {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
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
