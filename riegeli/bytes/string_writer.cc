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
#include <string>

#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/string_view.h"

namespace riegeli {

void StringWriter::Done() {
  if (RIEGELI_LIKELY(healthy())) {
    RIEGELI_ASSERT_EQ(buffer_size(), dest_->size())
        << "StringWriter destination changed unexpectedly";
    DiscardBuffer();
  }
  dest_ = nullptr;
  Writer::Done();
}

bool StringWriter::PushSlow() {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of Writer::PushSlow(): "
         "space available, use Push() instead";
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  RIEGELI_ASSERT_EQ(buffer_size(), dest_->size())
      << "StringWriter destination changed unexpectedly";
  if (RIEGELI_UNLIKELY(dest_->size() == dest_->max_size())) {
    cursor_ = start_;
    limit_ = start_;
    return FailOverflow();
  }
  const size_t cursor_pos = dest_->size();
  if (dest_->capacity() == dest_->size()) dest_->push_back('\0');
  MakeBuffer(cursor_pos);
  return true;
}

bool StringWriter::WriteSlow(string_view src) {
  RIEGELI_ASSERT_GT(src.size(), available())
      << "Failed precondition of Writer::WriteSlow(string_view): "
         "length too small, use Write(string_view) instead";
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  RIEGELI_ASSERT_EQ(buffer_size(), dest_->size())
      << "StringWriter destination changed unexpectedly";
  if (RIEGELI_UNLIKELY(src.size() > dest_->max_size() - written_to_buffer())) {
    cursor_ = start_;
    limit_ = start_;
    return FailOverflow();
  }
  DiscardBuffer();
  dest_->append(src.data(), src.size());
  MakeBuffer(dest_->size());
  return true;
}

bool StringWriter::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_GT(src.size(), UnsignedMin(available(), kMaxBytesToCopy()))
      << "Failed precondition of Writer::WriteSlow(Chain): "
         "length too small, use Write(Chain) instead";
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  RIEGELI_ASSERT_EQ(buffer_size(), dest_->size())
      << "StringWriter destination changed unexpectedly";
  if (RIEGELI_UNLIKELY(src.size() > dest_->max_size() - written_to_buffer())) {
    cursor_ = start_;
    limit_ = start_;
    return FailOverflow();
  }
  DiscardBuffer();
  src.AppendTo(dest_);
  MakeBuffer(dest_->size());
  return true;
}

bool StringWriter::Flush(FlushType flush_type) {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  RIEGELI_ASSERT_EQ(buffer_size(), dest_->size())
      << "StringWriter destination changed unexpectedly";
  DiscardBuffer();
  start_ = &(*dest_)[0];
  cursor_ = &(*dest_)[dest_->size()];
  limit_ = cursor_;
  return true;
}

inline void StringWriter::DiscardBuffer() {
  dest_->resize(written_to_buffer());
}

inline void StringWriter::MakeBuffer(size_t cursor_pos) {
  dest_->resize(dest_->capacity());
  start_ = &(*dest_)[0];
  cursor_ = &(*dest_)[cursor_pos];
  limit_ = &(*dest_)[dest_->size()];
}

}  // namespace riegeli
