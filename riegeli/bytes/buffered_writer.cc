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

#include "riegeli/bytes/buffered_writer.h"

#include <stddef.h>

#include <limits>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

inline size_t BufferedWriter::LengthToWriteDirectly() const {
  size_t length = buffer_.size();
  if (written_to_buffer() > 0) {
    // Two writes are needed because current contents of buffer_ must be pushed.
    // Write directly if writing through buffer_ would need more than two
    // writes, or if buffer would be full for the second write.
    if (limit_pos() < size_hint_) {
      // Write directly also if size_hint_ is reached.
      length = UnsignedMin(length, size_hint_ - limit_pos());
    }
    length = SaturatingAdd(available(), length);
  } else {
    // Write directly if writing through buffer_ would need more than one write,
    // or if buffer would be full.
    if (start_pos_ < size_hint_) {
      // Write directly also if size_hint_ is reached.
      length = UnsignedMin(length, size_hint_ - start_pos_);
    }
  }
  return length;
}

bool BufferedWriter::PushSlow() {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of Writer::PushSlow(): "
         "space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(!PushInternal())) return false;
  if (ABSL_PREDICT_FALSE(start_pos_ == std::numeric_limits<Position>::max())) {
    return FailOverflow();
  }
  if (ABSL_PREDICT_FALSE(!buffer_.is_allocated())) {
    if (start_pos_ < size_hint_ && buffer_.size() > size_hint_ - start_pos_) {
      // Avoid allocating more than needed for size_hint_.
      buffer_ = Buffer(size_hint_ - start_pos_);
    }
  } else if (ABSL_PREDICT_FALSE(buffer_.size() < buffer_size_)) {
    // buffer_ is too small. It must have been tuned for size_hint_ but more
    // data are being written.
    buffer_ = Buffer(buffer_size_);
  }
  start_ = buffer_.GetData();
  cursor_ = start_;
  limit_ =
      start_ + UnsignedMin(buffer_.size(),
                           std::numeric_limits<Position>::max() - start_pos_);
  return true;
}

bool BufferedWriter::PushInternal() {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  const size_t buffered_length = written_to_buffer();
  if (buffered_length == 0) return true;
  cursor_ = start_;
  return WriteInternal(absl::string_view(start_, buffered_length));
}

bool BufferedWriter::WriteSlow(absl::string_view src) {
  RIEGELI_ASSERT_GT(src.size(), available())
      << "Failed precondition of Writer::WriteSlow(string_view): "
         "length too small, use Write(string_view) instead";
  if (src.size() >= LengthToWriteDirectly()) {
    if (ABSL_PREDICT_FALSE(!PushInternal())) return false;
    return WriteInternal(src);
  }
  return Writer::WriteSlow(src);
}

}  // namespace riegeli
