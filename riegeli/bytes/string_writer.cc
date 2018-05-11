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

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"

namespace riegeli {

void StringWriter::Done() {
  if (ABSL_PREDICT_TRUE(healthy())) {
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
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  RIEGELI_ASSERT_EQ(buffer_size(), dest_->size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(dest_->size() == dest_->max_size())) {
    cursor_ = start_;
    limit_ = start_;
    return FailOverflow();
  }
  const size_t cursor_pos = dest_->size();
  if (dest_->capacity() == dest_->size()) dest_->push_back('\0');
  MakeBuffer(cursor_pos);
  return true;
}

bool StringWriter::WriteSlow(absl::string_view src) {
  RIEGELI_ASSERT_GT(src.size(), available())
      << "Failed precondition of Writer::WriteSlow(string_view): "
         "length too small, use Write(string_view) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  RIEGELI_ASSERT_EQ(buffer_size(), dest_->size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() >
                         dest_->max_size() - written_to_buffer())) {
    cursor_ = start_;
    limit_ = start_;
    return FailOverflow();
  }
  DiscardBuffer();
  dest_->append(src.data(), src.size());
  MakeBuffer(dest_->size());
  return true;
}

bool StringWriter::WriteSlow(std::string&& src) {
  RIEGELI_ASSERT_GT(src.size(), UnsignedMin(available(), kMaxBytesToCopy()))
      << "Failed precondition of Writer::WriteSlow(string&&): "
         "length too small, use Write(string&&) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  RIEGELI_ASSERT_EQ(buffer_size(), dest_->size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() >
                         dest_->max_size() - written_to_buffer())) {
    cursor_ = start_;
    limit_ = start_;
    return FailOverflow();
  }
  DiscardBuffer();
  if (dest_->empty() && dest_->capacity() <= src.capacity()) {
    *dest_ = std::move(src);
  } else {
    dest_->append(src);
  }
  MakeBuffer(dest_->size());
  return true;
}

bool StringWriter::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_GT(src.size(), UnsignedMin(available(), kMaxBytesToCopy()))
      << "Failed precondition of Writer::WriteSlow(Chain): "
         "length too small, use Write(Chain) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  RIEGELI_ASSERT_EQ(buffer_size(), dest_->size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() >
                         dest_->max_size() - written_to_buffer())) {
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
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  RIEGELI_ASSERT_EQ(buffer_size(), dest_->size())
      << "StringWriter destination changed unexpectedly";
  DiscardBuffer();
  start_ = &(*dest_)[0];
  cursor_ = start_ + dest_->size();
  limit_ = cursor_;
  return true;
}

bool StringWriter::Truncate(Position new_size) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  RIEGELI_ASSERT_EQ(buffer_size(), dest_->size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(new_size > written_to_buffer())) return false;
  cursor_ = start_ + new_size;
  return true;
}

inline void StringWriter::DiscardBuffer() {
  dest_->resize(written_to_buffer());
}

inline void StringWriter::MakeBuffer(size_t cursor_pos) {
  const size_t dest_size = dest_->capacity();
  dest_->resize(dest_size);
  start_ = &(*dest_)[0];
  cursor_ = start_ + cursor_pos;
  limit_ = start_ + dest_size;
}

}  // namespace riegeli
