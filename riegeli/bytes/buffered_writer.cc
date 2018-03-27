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
#include <memory>

#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

bool BufferedWriter::PushSlow() {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of Writer::PushSlow(): "
         "space available, use Push() instead";
  if (RIEGELI_UNLIKELY(!PushInternal())) return false;
  if (RIEGELI_UNLIKELY(start_ == nullptr)) {
    RIEGELI_ASSERT_GT(buffer_size_, 0u)
        << "Failed invariant of BufferedWriter: no buffer size specified";
    if (RIEGELI_UNLIKELY(buffer_size_ >
                         std::numeric_limits<Position>::max() - start_pos_)) {
      return FailOverflow();
    }
    start_ = std::allocator<char>().allocate(buffer_size_);
    cursor_ = start_;
    limit_ = start_ + buffer_size_;
  }
  return true;
}

bool BufferedWriter::PushInternal() {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  const size_t buffered_length = written_to_buffer();
  if (buffered_length == 0) return true;
  cursor_ = start_;
  return WriteInternal(absl::string_view(start_, buffered_length));
}

bool BufferedWriter::WriteSlow(absl::string_view src) {
  RIEGELI_ASSERT_GT(src.size(), available())
      << "Failed precondition of Writer::WriteSlow(string_view): "
         "length too small, use Write(string_view) instead";
  if (written_to_buffer() == 0 ? src.size() >= buffer_size_
                               : src.size() - available() >= buffer_size_) {
    // If writing through the buffer would need multiple WriteInternal() calls,
    // it is faster to push current contents of the buffer and write the
    // remaining data directly from src.
    if (RIEGELI_UNLIKELY(!PushInternal())) return false;
    return WriteInternal(src);
  }
  return Writer::WriteSlow(src);
}

}  // namespace riegeli
