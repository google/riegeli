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

#include "riegeli/bytes/pushable_writer.h"

#include <stddef.h>

#include <memory>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void PushableWriter::Done() {
  if (ABSL_PREDICT_TRUE(healthy())) SyncScratch();
  Writer::Done();
}

void PushableWriter::PushFromScratchSlow(size_t min_length) {
  RIEGELI_ASSERT_GT(min_length, 1u)
      << "Failed precondition of PushableWriter::PushFromScratchSlow(): "
         "trivial min_length";
  if (scratch_ == nullptr) {
    scratch_ = std::make_unique<Scratch>();
  } else {
    if (ABSL_PREDICT_FALSE(!SyncScratch())) return;
  }
  const absl::Span<char> flat_buffer =
      scratch_->buffer.AppendFixedBuffer(min_length);
  start_pos_ += written_to_buffer();
  scratch_->original_start = start_;
  scratch_->original_cursor = cursor_;
  scratch_->original_limit = limit_;
  start_ = flat_buffer.data();
  cursor_ = start_;
  limit_ = start_ + flat_buffer.size();
}

bool PushableWriter::SyncScratchSlow() {
  RIEGELI_ASSERT(start_ == scratch_->buffer.data())
      << "Failed invariant of PushableWriter: "
         "scratch used but buffer pointers do not point to scratch";
  const size_t length_to_write = written_to_buffer();
  start_ = scratch_->original_start;
  cursor_ = scratch_->original_cursor;
  limit_ = scratch_->original_limit;
  start_pos_ -= written_to_buffer();
  bool ok;
  if (length_to_write == scratch_->buffer.size()) {
    ok = Write(Chain(std::move(scratch_->buffer)));
  } else if (length_to_write <= kMaxBytesToCopy) {
    ok = Write(absl::string_view(scratch_->buffer.data(), length_to_write));
  } else {
    Chain data;
    scratch_->buffer.AppendSubstrTo(
        absl::string_view(scratch_->buffer.data(), length_to_write), &data);
    ok = Write(std::move(data));
  }
  scratch_->buffer.Clear();
  return ok;
}

void PushableWriter::SwapScratchBeginSlow() {
  using std::swap;
  swap(start_, scratch_->original_start);
  swap(cursor_, scratch_->original_cursor);
  swap(limit_, scratch_->original_limit);
  start_pos_ -= written_to_buffer();
}

void PushableWriter::SwapScratchEndSlow() {
  start_pos_ += written_to_buffer();
  using std::swap;
  swap(start_, scratch_->original_start);
  swap(cursor_, scratch_->original_cursor);
  swap(limit_, scratch_->original_limit);
}

}  // namespace riegeli
