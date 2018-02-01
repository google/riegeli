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

#include "riegeli/bytes/string_writer.h"

#include <stddef.h>
#include <limits>

#include "riegeli/base/assert.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/base/string_view.h"

namespace riegeli {

StringWriter::StringWriter() noexcept : Writer(State::kClosed) {}

StringWriter::StringWriter(std::string* dest, Options options)
    : Writer(State::kOpen), dest_(RIEGELI_ASSERT_NOTNULL(dest)) {
  if (options.size_hint_ > 0 &&
      RIEGELI_LIKELY(options.size_hint_ <=
                     std::numeric_limits<size_t>::max())) {
    dest_->reserve(options.size_hint_);
  }
  start_ = &(*dest_)[0];
  cursor_ = &(*dest_)[dest_->size()];
  limit_ = cursor_;
}

StringWriter::StringWriter(StringWriter&& src) noexcept
    : Writer(std::move(src)), dest_(riegeli::exchange(src.dest_, nullptr)) {}

StringWriter& StringWriter::operator=(StringWriter&& src) noexcept {
  Writer::operator=(std::move(src));
  dest_ = riegeli::exchange(src.dest_, nullptr);
  return *this;
}

StringWriter::~StringWriter() = default;

void StringWriter::Done() {
  if (RIEGELI_LIKELY(healthy())) DiscardBuffer();
  dest_ = nullptr;
  Writer::Done();
}

bool StringWriter::PushSlow() {
  RIEGELI_ASSERT_EQ(available(), 0u);
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  RIEGELI_ASSERT_EQ(pos(), dest_->size());
  if (dest_->capacity() == dest_->size()) dest_->reserve(dest_->size() + 1);
  MakeBuffer();
  return true;
}

bool StringWriter::WriteSlow(string_view src) {
  RIEGELI_ASSERT_GT(src.size(), available());
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  DiscardBuffer();
  dest_->append(src.data(), src.size());
  MakeBuffer();
  return true;
}

bool StringWriter::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_GT(src.size(), UnsignedMin(available(), kMaxBytesToCopy()));
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  DiscardBuffer();
  src.AppendTo(dest_);
  MakeBuffer();
  return true;
}

bool StringWriter::Flush(FlushType flush_type) {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  DiscardBuffer();
  start_ = &(*dest_)[0];
  cursor_ = &(*dest_)[dest_->size()];
  limit_ = cursor_;
  return true;
}

inline void StringWriter::DiscardBuffer() {
  RIEGELI_ASSERT_LE(pos(), dest_->size());
  dest_->resize(pos());
}

inline void StringWriter::MakeBuffer() {
  const size_t size_before = dest_->size();
  // Do not resize by too much because the work of filling the space could be
  // wasted by Flush().
  dest_->resize(
      UnsignedMin(dest_->capacity(), size_before + kDefaultBufferSize()));
  start_ = &(*dest_)[0];
  cursor_ = &(*dest_)[size_before];
  limit_ = &(*dest_)[dest_->size()];
}

}  // namespace riegeli
