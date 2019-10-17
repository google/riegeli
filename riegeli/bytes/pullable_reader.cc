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

#include "riegeli/bytes/pullable_reader.h"

#include <stddef.h>

#include <limits>
#include <memory>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

inline bool PullableReader::ScratchEnds(size_t min_length) {
  RIEGELI_ASSERT(scratch_used())
      << "Failed precondition of PullableReader::ScratchEnds(): "
         "scratch not used";
  const size_t available_length = available();
  if (PtrDistance(scratch_->original_start, scratch_->original_cursor) >=
          available_length &&
      PtrDistance(scratch_->original_cursor - available_length,
                  scratch_->original_limit) >= min_length) {
    SyncScratchSlow();
    cursor_ -= available_length;
    return true;
  }
  return false;
}

void PullableReader::PullToScratchSlow(size_t min_length) {
  RIEGELI_ASSERT_GT(min_length, 1u)
      << "Failed precondition of PullableReader::PullToScratchSlow(): "
         "trivial min_length";
  if (scratch_used() && ScratchEnds(min_length)) return;
  std::unique_ptr<Scratch> new_scratch;
  if (scratch_ != nullptr && (scratch_->buffer.empty() || available() == 0)) {
    // Scratch is allocated but not used or no longer needed. Reuse it.
    SyncScratch();
    new_scratch = std::move(scratch_);
  } else {
    // Allocate a new scratch. If scratch is currently used, some data from it
    // will be copied to the new scratch, so it must be preserved.
    new_scratch = absl::make_unique<Scratch>();
  }
  absl::Span<char> flat_buffer =
      new_scratch->buffer.AppendFixedBuffer(min_length);
  const Position pos_before = pos();
  if (ABSL_PREDICT_FALSE(!ReadSlow(flat_buffer.data(), flat_buffer.size()))) {
    RIEGELI_ASSERT_GE(pos(), pos_before)
        << "Reader::ReadSlow(char*) decreased pos()";
    const Position length_read = pos() - pos_before;
    RIEGELI_ASSERT_LE(length_read, flat_buffer.size())
        << "Reader::ReadSlow(char*) read more than requested";
    flat_buffer.remove_suffix(flat_buffer.size() -
                              IntCast<size_t>(length_read));
  }
  limit_pos_ -= available();
  new_scratch->original_start = start_;
  new_scratch->original_cursor = cursor_;
  new_scratch->original_limit = limit_;
  scratch_ = std::move(new_scratch);
  start_ = flat_buffer.data();
  cursor_ = start_;
  limit_ = start_ + flat_buffer.size();
}

bool PullableReader::ReadScratchSlow(Chain* dest, size_t* length) {
  RIEGELI_ASSERT(scratch_used())
      << "Failed precondition of PullableReader::ReadScratchSlow(Chain*): "
         "scratch not used";
  if (ScratchEnds(*length)) return true;
  const size_t length_to_read = UnsignedMin(*length, available());
  scratch_->buffer.AppendSubstrTo(absl::string_view(cursor_, length_to_read),
                                  dest);
  *length -= length_to_read;
  cursor_ += length_to_read;
  if (available() == 0) {
    SyncScratchSlow();
    return true;
  }
  return false;
}

bool PullableReader::CopyScratchToSlow(Writer* dest, Position* length) {
  RIEGELI_ASSERT(scratch_used())
      << "Failed precondition of PullableReader::CopyToInScratchSlow(): "
         "scratch not used";
  if (*length <= std::numeric_limits<size_t>::max() &&
      ScratchEnds(IntCast<size_t>(*length))) {
    return true;
  }
  const size_t length_to_copy = UnsignedMin(*length, available());
  bool ok;
  if (length_to_copy == scratch_->buffer.size()) {
    ok = dest->Write(Chain(scratch_->buffer));
  } else if (length_to_copy <= kMaxBytesToCopy) {
    ok = dest->Write(absl::string_view(cursor_, length_to_copy));
  } else {
    Chain data;
    scratch_->buffer.AppendSubstrTo(absl::string_view(cursor_, length_to_copy),
                                    &data);
    ok = dest->Write(std::move(data));
  }
  *length -= length_to_copy;
  cursor_ += length_to_copy;
  if (available() == 0) {
    SyncScratchSlow();
    return ok;
  }
  return false;
}

void PullableReader::SyncScratchSlow() {
  scratch_->buffer.Clear();
  start_ = scratch_->original_start;
  cursor_ = scratch_->original_cursor;
  limit_ = scratch_->original_limit;
  limit_pos_ += available();
}

void PullableReader::SwapScratchBeginSlow() {
  using std::swap;
  swap(start_, scratch_->original_start);
  swap(cursor_, scratch_->original_cursor);
  swap(limit_, scratch_->original_limit);
  limit_pos_ += available();
}

void PullableReader::SwapScratchEndSlow() {
  limit_pos_ -= available();
  using std::swap;
  swap(start_, scratch_->original_start);
  swap(cursor_, scratch_->original_cursor);
  swap(limit_, scratch_->original_limit);
}

}  // namespace riegeli
