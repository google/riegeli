// Copyright 2019 Google LLC
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

#include "riegeli/bytes/null_writer.h"

#include <stddef.h>

#include <limits>

#include "absl/base/optimization.h"
#include "riegeli/base/base.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/chain.h"

namespace riegeli {

bool NullWriter::PushSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_GT(min_length, available())
      << "Failed precondition of Writer::PushSlow(): "
         "length too small, use Push() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  SyncBuffer();
  return MakeBuffer(min_length);
}

bool NullWriter::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_GT(src.size(), UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Writer::WriteSlow(Chain): "
         "length too small, use Write(Chain) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(src.size() >
                         std::numeric_limits<Position>::max() - pos())) {
    return FailOverflow();
  }
  SyncBuffer();
  start_pos_ += src.size();
  return MakeBuffer();
}

bool NullWriter::Flush(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  return true;
}

bool NullWriter::Truncate(Position new_size) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (new_size >= start_pos_) {
    if (ABSL_PREDICT_FALSE(new_size > pos())) return false;
    cursor_ = start_ + (new_size - start_pos_);
    return true;
  }
  start_pos_ = new_size;
  cursor_ = start_;
  return true;
}

inline void NullWriter::SyncBuffer() {
  start_pos_ = pos();
  cursor_ = start_;
}

inline bool NullWriter::MakeBuffer(size_t min_length) {
  if (ABSL_PREDICT_FALSE(min_length >
                         std::numeric_limits<Position>::max() - pos())) {
    return FailOverflow();
  }
  buffer_.Resize(UnsignedMax(kDefaultBufferSize, min_length));
  start_ = buffer_.GetData();
  cursor_ = start_;
  limit_ =
      start_ + UnsignedMin(buffer_.size(),
                           std::numeric_limits<Position>::max() - start_pos_);
  return true;
}

}  // namespace riegeli
