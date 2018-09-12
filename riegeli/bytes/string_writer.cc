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

void StringWriterBase::Done() {
  if (ABSL_PREDICT_TRUE(healthy())) {
    std::string* const dest = dest_string();
    RIEGELI_ASSERT_EQ(buffer_size(), dest->size())
        << "StringWriter destination changed unexpectedly";
    DiscardBuffer(dest);
    start_pos_ = dest->size();
  }
  Writer::Done();
}

bool StringWriterBase::PushSlow() {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of Writer::PushSlow(): "
         "space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  std::string* const dest = dest_string();
  RIEGELI_ASSERT_EQ(buffer_size(), dest->size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(dest->size() == dest->max_size())) {
    cursor_ = start_;
    limit_ = start_;
    return FailOverflow();
  }
  const size_t cursor_pos = dest->size();
  if (dest->capacity() == dest->size()) dest->push_back('\0');
  MakeBuffer(dest, cursor_pos);
  return true;
}

bool StringWriterBase::WriteSlow(absl::string_view src) {
  RIEGELI_ASSERT_GT(src.size(), available())
      << "Failed precondition of Writer::WriteSlow(string_view): "
         "length too small, use Write(string_view) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  std::string* const dest = dest_string();
  RIEGELI_ASSERT_EQ(buffer_size(), dest->size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > dest->max_size() - written_to_buffer())) {
    cursor_ = start_;
    limit_ = start_;
    return FailOverflow();
  }
  DiscardBuffer(dest);
  dest->append(src.data(), src.size());
  MakeBuffer(dest);
  return true;
}

bool StringWriterBase::WriteSlow(std::string&& src) {
  RIEGELI_ASSERT_GT(src.size(), UnsignedMin(available(), kMaxBytesToCopy()))
      << "Failed precondition of Writer::WriteSlow(string&&): "
         "length too small, use Write(string&&) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  std::string* const dest = dest_string();
  RIEGELI_ASSERT_EQ(buffer_size(), dest->size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > dest->max_size() - written_to_buffer())) {
    cursor_ = start_;
    limit_ = start_;
    return FailOverflow();
  }
  DiscardBuffer(dest);
  if (dest->empty() && dest->capacity() <= src.capacity()) {
    *dest = std::move(src);
  } else {
    dest->append(src);
  }
  MakeBuffer(dest);
  return true;
}

bool StringWriterBase::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_GT(src.size(), UnsignedMin(available(), kMaxBytesToCopy()))
      << "Failed precondition of Writer::WriteSlow(Chain): "
         "length too small, use Write(Chain) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  std::string* const dest = dest_string();
  RIEGELI_ASSERT_EQ(buffer_size(), dest->size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(src.size() > dest->max_size() - written_to_buffer())) {
    cursor_ = start_;
    limit_ = start_;
    return FailOverflow();
  }
  DiscardBuffer(dest);
  src.AppendTo(dest);
  MakeBuffer(dest);
  return true;
}

bool StringWriterBase::Flush(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  std::string* const dest = dest_string();
  RIEGELI_ASSERT_EQ(buffer_size(), dest->size())
      << "StringWriter destination changed unexpectedly";
  DiscardBuffer(dest);
  start_ = &(*dest)[0];
  cursor_ = start_ + dest->size();
  limit_ = cursor_;
  return true;
}

bool StringWriterBase::Truncate(Position new_size) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  std::string* const dest = dest_string();
  RIEGELI_ASSERT_EQ(buffer_size(), dest->size())
      << "StringWriter destination changed unexpectedly";
  if (ABSL_PREDICT_FALSE(new_size > written_to_buffer())) return false;
  cursor_ = start_ + new_size;
  return true;
}

inline void StringWriterBase::DiscardBuffer(std::string* dest) {
  dest->resize(written_to_buffer());
}

inline void StringWriterBase::MakeBuffer(std::string* dest, size_t cursor_pos) {
  const size_t dest_size = dest->capacity();
  dest->resize(dest_size);
  start_ = &(*dest)[0];
  cursor_ = start_ + cursor_pos;
  limit_ = start_ + dest_size;
}

template class StringWriter<std::string*>;
template class StringWriter<std::string>;

}  // namespace riegeli
