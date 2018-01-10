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

#include <memory>
#include <utility>

#include "riegeli/base/assert.h"
#include "riegeli/base/base.h"
#include "riegeli/base/string_view.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

BufferedWriter::BufferedWriter(BufferedWriter&& src) noexcept
    : Writer(std::move(src)),
      buffer_size_(riegeli::exchange(src.buffer_size_, 0)) {}

void BufferedWriter::operator=(BufferedWriter&& src) noexcept {
  RIEGELI_ASSERT(&src != this);
  Writer::operator=(std::move(src));
  buffer_size_ = riegeli::exchange(src.buffer_size_, 0);
}

bool BufferedWriter::PushSlow() {
  RIEGELI_ASSERT_EQ(available(), 0u);
  if (RIEGELI_UNLIKELY(!PushInternal())) return false;
  if (RIEGELI_UNLIKELY(start_ == nullptr)) {
    RIEGELI_ASSERT_GT(buffer_size_, 0u);
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
  return WriteInternal(string_view(start_, buffered_length));
}

bool BufferedWriter::WriteSlow(string_view src) {
  RIEGELI_ASSERT_GT(src.size(), available());
  if (written_to_buffer() == 0 ? src.size() >= buffer_size_
                               : src.size() - available() >= buffer_size_) {
    // If writing through buffer_ would need multiple WriteInternal() calls, it
    // is faster to push current contents of buffer_ and write the remaining
    // data directly from src.
    if (RIEGELI_UNLIKELY(!PushInternal())) return false;
    return WriteInternal(src);
  }
  return Writer::WriteSlow(src);
}

}  // namespace riegeli
