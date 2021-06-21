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

#include <array>
#include <limits>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/memory.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

bool BufferedWriter::PushInternal() {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  const absl::string_view data(start(), written_to_buffer());
  set_buffer();
  return data.empty() || WriteInternal(data);
}

inline size_t BufferedWriter::LengthToWriteDirectly() const {
  size_t length = buffer_size_;
  if (written_to_buffer() > 0) {
    // Two writes are needed because current contents of `buffer_` must be
    // pushed. Write directly if writing through `buffer_` would need more than
    // two writes, or if `buffer_` would be full for the second write.
    if (limit_pos() < size_hint_) {
      // Write directly also if `size_hint_` is reached.
      length = UnsignedMin(length, size_hint_ - limit_pos());
    }
    length = SaturatingAdd(available(), length);
  } else {
    // Write directly if writing through `buffer_` would need more than one
    // write, or if `buffer_` would be full.
    if (start_pos() < size_hint_) {
      // Write directly also if `size_hint_` is reached.
      length = UnsignedMin(length, size_hint_ - start_pos());
    }
  }
  return length;
}

bool BufferedWriter::PushSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Writer::PushSlow(): "
         "enough space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(!PushInternal())) return false;
  if (ABSL_PREDICT_FALSE(min_length >
                         std::numeric_limits<Position>::max() - start_pos())) {
    return FailOverflow();
  }
  const size_t buffer_length =
      UnsignedMin(BufferLength(UnsignedMax(min_length, recommended_length),
                               buffer_size_, size_hint_, start_pos()),
                  std::numeric_limits<Position>::max() - start_pos());
  buffer_.Reset(buffer_length);
  set_buffer(buffer_.data(), buffer_length);
  return true;
}

bool BufferedWriter::WriteSlow(absl::string_view src) {
  RIEGELI_ASSERT_LT(available(), src.size())
      << "Failed precondition of Writer::WriteSlow(string_view): "
         "enough space available, use Write(string_view) instead";
  if (src.size() >= LengthToWriteDirectly()) {
    if (ABSL_PREDICT_FALSE(!PushInternal())) return false;
    return WriteInternal(src);
  }
  return Writer::WriteSlow(src);
}

bool BufferedWriter::WriteZerosSlow(Position length) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Writer::WriteZerosSlow(): "
         "enough space available, use WriteZeros() instead";
  while (length > kArrayOfZeros.size()) {
    const absl::string_view zeros(kArrayOfZeros.data(), kArrayOfZeros.size());
    if (ABSL_PREDICT_FALSE(!Write(zeros))) return false;
    length -= zeros.size();
  }
  const absl::string_view zeros(kArrayOfZeros.data(), IntCast<size_t>(length));
  return Write(zeros);
}

void BufferedWriter::WriteHintSlow(size_t length) {
  RIEGELI_ASSERT_LT(available(), length)
      << "Failed precondition of Writer::WriteHintSlow(): "
         "enough space available, use WriteHint() instead";
  if (ABSL_PREDICT_FALSE(!PushInternal())) return;
  const size_t buffer_length =
      UnsignedMin(BufferLength(length, buffer_size_, size_hint_, start_pos()),
                  std::numeric_limits<Position>::max() - start_pos());
  buffer_.Reset(buffer_length);
  set_buffer(buffer_.data(), buffer_length);
}

}  // namespace riegeli
