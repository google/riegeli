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

#include <cstring>
#include <memory>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

inline bool PullableReader::ScratchEnds() {
  RIEGELI_ASSERT(scratch_used())
      << "Failed precondition of PullableReader::ScratchEnds(): "
         "scratch not used";
  const size_t available_length = available();
  if (scratch_->original_read_from_buffer >= available_length) {
    SyncScratchSlow();
    set_cursor(cursor() - available_length);
    return true;
  }
  return false;
}

void PullableReader::PullToScratchSlow(size_t min_length,
                                       size_t recommended_length) {
  if (scratch_used()) {
    RIEGELI_ASSERT(start() == scratch_->buffer.data())
        << "Failed invariant of PullableReader: "
           "scratch used but buffer pointers do not point to scratch";
    RIEGELI_ASSERT_EQ(buffer_size(), scratch_->buffer.size())
        << "Failed invariant of PullableReader: "
           "scratch used but buffer pointers do not point to scratch";
  }
  RIEGELI_ASSERT_GT(min_length, 1u)
      << "Failed precondition of PullableReader::PullToScratchSlow(): "
         "trivial min_length";
  if (scratch_used() && ScratchEnds() && available() >= min_length) return;
  if (available() == 0) {
    if (ABSL_PREDICT_FALSE(!PullSlow(1, 0))) return;
    if (available() >= min_length) return;
  }
  recommended_length = UnsignedMax(min_length, recommended_length);
  size_t max_length = SaturatingAdd(recommended_length, recommended_length);
  std::unique_ptr<Scratch> new_scratch;
  if (ABSL_PREDICT_FALSE(scratch_ == nullptr)) {
    new_scratch = std::make_unique<Scratch>();
  } else {
    new_scratch = std::move(scratch_);
    if (!new_scratch->buffer.empty()) {
      // Scratch is used but it does have enough data after the cursor.
      new_scratch->buffer.RemovePrefix(read_from_buffer());
      min_length -= new_scratch->buffer.size();
      recommended_length -= new_scratch->buffer.size();
      max_length -= new_scratch->buffer.size();
      set_buffer(new_scratch->original_start, new_scratch->original_buffer_size,
                 new_scratch->original_read_from_buffer);
      move_limit_pos(available());
    }
  }
  const absl::Span<char> flat_buffer = new_scratch->buffer.AppendBuffer(
      min_length, recommended_length, max_length);
  char* dest = flat_buffer.data();
  char* const min_limit = flat_buffer.data() + min_length;
  char* const max_limit = flat_buffer.data() + flat_buffer.size();
  do {
    const size_t length =
        UnsignedMin(available(), PtrDistance(dest, max_limit));
    if (
        // `std::memcpy(_, nullptr, 0)` is undefined.
        length > 0) {
      std::memcpy(dest, cursor(), length);
      move_cursor(length);
      dest += length;
      if (dest >= min_limit) break;
    }
  } while (PullSlow(1, 0));
  new_scratch->buffer.RemoveSuffix(PtrDistance(dest, max_limit));
  set_limit_pos(pos());
  new_scratch->original_start = start();
  new_scratch->original_buffer_size = buffer_size();
  new_scratch->original_read_from_buffer = read_from_buffer();
  scratch_ = std::move(new_scratch);
  set_buffer(scratch_->buffer.data(), scratch_->buffer.size());
}

bool PullableReader::ReadScratchSlow(size_t& length, Chain& dest) {
  RIEGELI_ASSERT(scratch_used())
      << "Failed precondition of PullableReader::ReadScratchSlow(Chain&): "
         "scratch not used";
  RIEGELI_ASSERT(start() == scratch_->buffer.data())
      << "Failed invariant of PullableReader: "
         "scratch used but buffer pointers do not point to scratch";
  RIEGELI_ASSERT_EQ(buffer_size(), scratch_->buffer.size())
      << "Failed invariant of PullableReader: "
         "scratch used but buffer pointers do not point to scratch";
  if (ScratchEnds()) return true;
  const size_t length_to_read = UnsignedMin(length, available());
  scratch_->buffer.AppendSubstrTo(absl::string_view(cursor(), length_to_read),
                                  dest);
  length -= length_to_read;
  move_cursor(length_to_read);
  if (available() == 0) {
    SyncScratchSlow();
    return true;
  }
  return false;
}

bool PullableReader::ReadScratchSlow(size_t& length, absl::Cord& dest) {
  RIEGELI_ASSERT(scratch_used())
      << "Failed precondition of PullableReader::ReadScratchSlow(Cord&): "
         "scratch not used";
  RIEGELI_ASSERT(start() == scratch_->buffer.data())
      << "Failed invariant of PullableReader: "
         "scratch used but buffer pointers do not point to scratch";
  RIEGELI_ASSERT_EQ(buffer_size(), scratch_->buffer.size())
      << "Failed invariant of PullableReader: "
         "scratch used but buffer pointers do not point to scratch";
  if (ScratchEnds()) return true;
  const size_t length_to_read = UnsignedMin(length, available());
  scratch_->buffer.AppendSubstrTo(absl::string_view(cursor(), length_to_read),
                                  dest);
  length -= length_to_read;
  move_cursor(length_to_read);
  if (available() == 0) {
    SyncScratchSlow();
    return true;
  }
  return false;
}

bool PullableReader::CopyScratchToSlow(Position& length, Writer& dest) {
  RIEGELI_ASSERT(scratch_used())
      << "Failed precondition of PullableReader::CopyScratchToSlow(): "
         "scratch not used";
  RIEGELI_ASSERT(start() == scratch_->buffer.data())
      << "Failed invariant of PullableReader: "
         "scratch used but buffer pointers do not point to scratch";
  RIEGELI_ASSERT_EQ(buffer_size(), scratch_->buffer.size())
      << "Failed invariant of PullableReader: "
         "scratch used but buffer pointers do not point to scratch";
  if (ScratchEnds()) return true;
  const size_t length_to_copy = UnsignedMin(length, available());
  bool ok;
  if (length_to_copy <= kMaxBytesToCopy || dest.PrefersCopying()) {
    ok = dest.Write(absl::string_view(cursor(), length_to_copy));
  } else if (length_to_copy == scratch_->buffer.size()) {
    ok = dest.Write(Chain(scratch_->buffer));
  } else {
    Chain data;
    scratch_->buffer.AppendSubstrTo(absl::string_view(cursor(), length_to_copy),
                                    data);
    ok = dest.Write(std::move(data));
  }
  length -= length_to_copy;
  move_cursor(length_to_copy);
  if (available() == 0) {
    SyncScratchSlow();
    return ok;
  }
  return false;
}

bool PullableReader::SeekUsingScratchSlow(Position new_pos) {
  SyncScratchSlow();
  if (new_pos >= start_pos() && new_pos <= limit_pos()) {
    set_cursor(limit() - (limit_pos() - new_pos));
    return false;
  }
  return true;
}

void PullableReader::SyncScratchSlow() {
  RIEGELI_ASSERT(scratch_used())
      << "Failed precondition of PullableReader::SyncScratchSlow(): "
         "scratch not used";
  RIEGELI_ASSERT(start() == scratch_->buffer.data())
      << "Failed invariant of PullableReader: "
         "scratch used but buffer pointers do not point to scratch";
  RIEGELI_ASSERT_EQ(buffer_size(), scratch_->buffer.size())
      << "Failed invariant of PullableReader: "
         "scratch used but buffer pointers do not point to scratch";
  scratch_->buffer.Clear();
  set_buffer(scratch_->original_start, scratch_->original_buffer_size,
             scratch_->original_read_from_buffer);
  move_limit_pos(available());
}

void PullableReader::BehindScratch::Enter() {
  RIEGELI_ASSERT(context_->scratch_used())
      << "Failed precondition of PullableReader::BehindScratch::Enter(): "
         "scratch not used";
  RIEGELI_ASSERT(context_->start() == context_->scratch_->buffer.data())
      << "Failed invariant of PullableReader: "
         "scratch used but buffer pointers do not point to scratch";
  RIEGELI_ASSERT_EQ(context_->buffer_size(), context_->scratch_->buffer.size())
      << "Failed invariant of PullableReader: "
         "scratch used but buffer pointers do not point to scratch";
  read_from_scratch_ = context_->read_from_buffer();
  context_->set_buffer(context_->scratch_->original_start,
                       context_->scratch_->original_buffer_size,
                       context_->scratch_->original_read_from_buffer);
  context_->move_limit_pos(context_->available());
}

void PullableReader::BehindScratch::Leave() {
  RIEGELI_ASSERT(context_->scratch_used())
      << "Failed precondition of PullableReader::BehindScratch::Leave(): "
         "scratch not used";
  context_->set_limit_pos(context_->pos());
  context_->scratch_->original_start = context_->start();
  context_->scratch_->original_buffer_size = context_->buffer_size();
  context_->scratch_->original_read_from_buffer = context_->read_from_buffer();
  context_->set_buffer(context_->scratch_->buffer.data(),
                       context_->scratch_->buffer.size(), read_from_scratch_);
}

}  // namespace riegeli
