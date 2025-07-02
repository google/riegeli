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
#include <limits>
#include <memory>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/cord_buffer.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/cord_utils.h"
#include "riegeli/base/external_ref.h"
#include "riegeli/base/null_safe_memcpy.h"
#include "riegeli/base/sized_shared_buffer.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void PullableReader::Done() {
  if (ABSL_PREDICT_FALSE(scratch_used()) && !ScratchEnds()) {
    if (!SupportsRandomAccess()) {
      // Seeking back is not feasible.
      Reader::Done();
      scratch_.reset();
      return;
    }
    const Position new_pos = pos();
    SyncScratch();
    Seek(new_pos);
  }
  DoneBehindScratch();
  Reader::Done();
  scratch_.reset();
}

void PullableReader::DoneBehindScratch() {
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::DoneBehindScratch(): "
         "scratch used";
  SyncBehindScratch(SyncType::kFromObject);
}

inline void PullableReader::SyncScratch() {
  RIEGELI_ASSERT(scratch_used())
      << "Failed precondition of PullableReader::SyncScratch(): "
         "scratch not used";
  RIEGELI_ASSERT_EQ(start(), scratch_->buffer.data())
      << "Failed invariant of PullableReader: "
         "scratch used but buffer pointers do not point to scratch";
  RIEGELI_ASSERT_EQ(start_to_limit(), scratch_->buffer.size())
      << "Failed invariant of PullableReader: "
         "scratch used but buffer pointers do not point to scratch";
  ClearScratch();
}

inline void PullableReader::ClearScratch() {
  scratch_->buffer.ClearAndShrink();
  set_buffer(scratch_->original_start, scratch_->original_start_to_limit,
             scratch_->original_start_to_cursor);
  move_limit_pos(available());
}

inline bool PullableReader::ScratchEnds() {
  RIEGELI_ASSERT(scratch_used())
      << "Failed precondition of PullableReader::ScratchEnds(): "
         "scratch not used";
  const size_t available_length = available();
  if (scratch_->original_start_to_cursor >= available_length) {
    SyncScratch();
    set_cursor(cursor() - available_length);
    return true;
  }
  return false;
}

bool PullableReader::PullSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Reader::PullSlow(): "
         "enough data available, use Pull() instead";
  if (ABSL_PREDICT_TRUE(min_length == 1)) {
    if (ABSL_PREDICT_FALSE(scratch_used())) {
      SyncScratch();
      if (available() > 0) return true;
    }
    return PullBehindScratch(recommended_length);
  }
  if (scratch_used() && ScratchEnds() && available() >= min_length) return true;
  if (available() == 0) {
    RIEGELI_ASSERT(!scratch_used())
        << "Scratch should have ended but is still used";
    if (ABSL_PREDICT_FALSE(!PullBehindScratch(recommended_length))) {
      return false;
    }
    if (available() >= min_length) return true;
  }
  size_t remaining_min_length = min_length;
  recommended_length = UnsignedMax(min_length, recommended_length);
  std::unique_ptr<Scratch> new_scratch;
  if (ABSL_PREDICT_FALSE(scratch_ == nullptr)) {
    new_scratch = std::make_unique<Scratch>();
  } else {
    new_scratch = std::move(scratch_);
    if (!new_scratch->buffer.empty()) {
      // Scratch is used but it does not have enough data after the cursor.
      new_scratch->buffer.RemovePrefix(start_to_cursor());
      new_scratch->buffer.Shrink(recommended_length);
      remaining_min_length -= new_scratch->buffer.size();
      recommended_length -= new_scratch->buffer.size();
      set_buffer(new_scratch->original_start,
                 new_scratch->original_start_to_limit,
                 new_scratch->original_start_to_cursor);
      move_limit_pos(available());
    }
  }
  const absl::Span<char> flat_buffer = new_scratch->buffer.AppendBuffer(
      remaining_min_length, recommended_length);
  char* dest = flat_buffer.data();
  char* const min_limit = flat_buffer.data() + remaining_min_length;
  char* const recommended_limit = flat_buffer.data() + recommended_length;
  char* const max_limit = flat_buffer.data() + flat_buffer.size();
  do {
    const size_t length =
        UnsignedMin(available(), PtrDistance(dest, max_limit));
    riegeli::null_safe_memcpy(dest, cursor(), length);
    move_cursor(length);
    dest += length;
    if (dest >= min_limit) break;
    if (ABSL_PREDICT_FALSE(scratch_used())) {
      SyncScratch();
      if (available() > 0) continue;
    }
  } while (PullBehindScratch(PtrDistance(dest, recommended_limit)));
  new_scratch->buffer.RemoveSuffix(PtrDistance(dest, max_limit));
  set_limit_pos(pos());
  new_scratch->original_start = start();
  new_scratch->original_start_to_limit = start_to_limit();
  new_scratch->original_start_to_cursor = start_to_cursor();
  scratch_ = std::move(new_scratch);
  set_buffer(scratch_->buffer.data(), scratch_->buffer.size());
  return available() >= min_length;
}

bool PullableReader::ReadBehindScratch(size_t length, char* dest) {
  RIEGELI_ASSERT_LT(available(), length)
      << "Failed precondition of PullableReader::ReadBehindScratch(char*): "
         "enough data available, use Read(char*) instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::ReadBehindScratch(char*): "
         "scratch used";
  do {
    const size_t available_length = available();
    riegeli::null_safe_memcpy(dest, cursor(), available_length);
    move_cursor(available_length);
    dest += available_length;
    length -= available_length;
    if (ABSL_PREDICT_FALSE(!PullBehindScratch(length))) return false;
  } while (length > available());
  std::memcpy(dest, cursor(), length);
  move_cursor(length);
  return true;
}

bool PullableReader::ReadBehindScratch(size_t length, Chain& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of PullableReader::ReadBehindScratch(Chain&): "
         "enough data available, use Read(Chain&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of PullableReader::ReadBehindScratch(Chain&): "
         "Chain size overflow";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::ReadBehindScratch(Chain&): "
         "scratch used";
  do {
    const absl::Span<char> buffer = dest.AppendBuffer(1, length, length);
    size_t length_read;
    if (ABSL_PREDICT_FALSE(!Read(buffer.size(), buffer.data(), &length_read))) {
      dest.RemoveSuffix(buffer.size() - length_read);
      return false;
    }
    length -= length_read;
  } while (length > 0);
  return true;
}

bool PullableReader::ReadBehindScratch(size_t length, absl::Cord& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of PullableReader::ReadBehindScratch(Cord&): "
         "enough data available, use Read(Cord&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of PullableReader::ReadBehindScratch(Cord&): "
         "Cord size overflow";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::ReadBehindScratch(Cord&): "
         "scratch used";
  absl::CordBuffer buffer = dest.GetCustomAppendBuffer(
      cord_internal::kCordBufferBlockSize, length, 1);
  absl::Span<char> span = buffer.available_up_to(length);
  if (buffer.capacity() < kDefaultMinBlockSize && length > span.size()) {
    absl::CordBuffer new_buffer = absl::CordBuffer::CreateWithCustomLimit(
        cord_internal::kCordBufferBlockSize, buffer.length() + length);
    std::memcpy(new_buffer.data(), buffer.data(), buffer.length());
    new_buffer.SetLength(buffer.length());
    buffer = std::move(new_buffer);
    span = buffer.available_up_to(length);
  }
  for (;;) {
    size_t length_read;
    const bool read_ok = Read(span.size(), span.data(), &length_read);
    buffer.IncreaseLengthBy(length_read);
    dest.Append(std::move(buffer));
    if (ABSL_PREDICT_FALSE(!read_ok)) return false;
    length -= length_read;
    if (length == 0) return true;
    buffer = absl::CordBuffer::CreateWithCustomLimit(
        cord_internal::kCordBufferBlockSize, length);
    span = buffer.available_up_to(length);
  }
}

bool PullableReader::CopyBehindScratch(Position length, Writer& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of PullableReader::CopyBehindScratch(Writer&): "
         "enough data available, use Copy(Writer&) instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::CopyBehindScratch(Writer&): "
         "scratch used";
  while (length > available()) {
    const absl::string_view data(cursor(), available());
    move_cursor(data.size());
    if (ABSL_PREDICT_FALSE(!dest.Write(data))) return false;
    length -= data.size();
    if (ABSL_PREDICT_FALSE(!PullBehindScratch(length))) return false;
  }
  const absl::string_view data(cursor(), IntCast<size_t>(length));
  move_cursor(IntCast<size_t>(length));
  return dest.Write(data);
}

bool PullableReader::CopyBehindScratch(size_t length, BackwardWriter& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of "
         "PullableReader::CopyBehindScratch(BackwardWriter&): "
         "enough data available, use Copy(BackwardWriter&) instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of "
         "PullableReader::CopyBehindScratch(BackwardWriter&): "
         "scratch used";
  if (length <= available()) {
    const absl::string_view data(cursor(), length);
    move_cursor(length);
    return dest.Write(data);
  }
  if (length <= kMaxBytesToCopy) {
    if (ABSL_PREDICT_FALSE(!dest.Push(length))) return false;
    dest.move_cursor(length);
    if (ABSL_PREDICT_FALSE(!ReadBehindScratch(length, dest.cursor()))) {
      dest.set_cursor(dest.cursor() + length);
      return false;
    }
    return true;
  }
  Chain data;
  if (ABSL_PREDICT_FALSE(!ReadBehindScratch(length, data))) return false;
  return dest.Write(std::move(data));
}

bool PullableReader::ReadOrPullSomeBehindScratch(
    size_t max_length, absl::FunctionRef<char*(size_t&)> get_dest) {
  RIEGELI_ASSERT_GT(max_length, 0u)
      << "Failed precondition of "
         "PullableReader::ReadOrPullSomeBehindScratch(): "
         "nothing to read, use ReadOrPullSome() instead";
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of "
         "PullableReader::ReadOrPullSomeBehindScratch(): "
         "some data available, use ReadOrPullSome() instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of "
         "PullableReader::ReadOrPullSomeBehindScratch(): "
         "scratch used";
  return PullBehindScratch(max_length);
}

void PullableReader::ReadHintBehindScratch(size_t min_length,
                                           size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of PullableReader::ReadHintBehindScratch(): "
         "enough data available, use ReadHint() instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::ReadHintBehindScratch(): "
         "scratch used";
}

bool PullableReader::SyncBehindScratch(SyncType sync_type) {
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::SyncBehindScratch(): "
         "scratch used";
  return ok();
}

bool PullableReader::SeekBehindScratch(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of PullableReader::SeekBehindScratch(): "
         "position in the buffer, use Seek() instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::SeekBehindScratch(): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(new_pos <= limit_pos())) {
    return Fail(
        absl::UnimplementedError("Reader::Seek() backwards not supported"));
  }
  // Seeking forwards.
  do {
    move_cursor(available());
    if (ABSL_PREDICT_FALSE(!PullBehindScratch(0))) return false;
  } while (new_pos > limit_pos());
  const Position available_length = limit_pos() - new_pos;
  RIEGELI_ASSERT_LE(available_length, start_to_limit())
      << "PullableReader::PullBehindScratch() skipped some data";
  set_cursor(limit() - available_length);
  return true;
}

bool PullableReader::ReadSlow(size_t length, char* dest) {
  RIEGELI_ASSERT_LT(available(), length)
      << "Failed precondition of Reader::ReadSlow(char*): "
         "enough data available, use Read(char*) instead";
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    if (!ScratchEnds()) {
      const size_t length_to_read = available();
      std::memcpy(dest, cursor(), length_to_read);
      dest += length_to_read;
      length -= length_to_read;
      move_cursor(length_to_read);
      SyncScratch();
    }
    if (available() >= length) {
      riegeli::null_safe_memcpy(dest, cursor(), length);
      move_cursor(length);
      return true;
    }
  }
  return ReadBehindScratch(length, dest);
}

bool PullableReader::ReadSlow(size_t length, Chain& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::ReadSlow(Chain&): "
         "enough data available, use Read(Chain&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadSlow(Chain&): "
         "Chain size overflow";
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    if (!ScratchEnds()) {
      if (available() >= length) {
        dest.Append(
            ExternalRef(scratch_->buffer, absl::string_view(cursor(), length)));
        move_cursor(length);
        return true;
      }
      length -= available();
      dest.Append(ExternalRef(std::move(scratch_->buffer),
                              absl::string_view(cursor(), available())));
      ClearScratch();
    }
    if (available() >= length && length <= kMaxBytesToCopy) {
      dest.Append(absl::string_view(cursor(), length));
      move_cursor(length);
      return true;
    }
  }
  return ReadBehindScratch(length, dest);
}

bool PullableReader::ReadSlow(size_t length, absl::Cord& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::ReadSlow(Cord&): "
         "enough data available, use Read(Cord&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadSlow(Cord&): "
         "Cord size overflow";
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    if (!ScratchEnds()) {
      if (available() >= length) {
        ExternalRef(scratch_->buffer, absl::string_view(cursor(), length))
            .AppendTo(dest);
        move_cursor(length);
        return true;
      }
      length -= available();
      ExternalRef(std::move(scratch_->buffer),
                  absl::string_view(cursor(), available()))
          .AppendTo(dest);
      ClearScratch();
    }
    if (available() >= length && length <= kMaxBytesToCopy) {
      dest.Append(absl::string_view(cursor(), length));
      move_cursor(length);
      return true;
    }
  }
  return ReadBehindScratch(length, dest);
}

bool PullableReader::CopySlow(Position length, Writer& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::CopySlow(Writer&): "
         "enough data available, use Copy(Writer&) instead";
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    if (!ScratchEnds()) {
      if (available() >= length) {
        const bool write_ok = dest.Write(
            ExternalRef(scratch_->buffer, absl::string_view(cursor(), length)));
        move_cursor(length);
        return write_ok;
      }
      length -= available();
      const bool write_ok =
          dest.Write(ExternalRef(std::move(scratch_->buffer),
                                 absl::string_view(cursor(), available())));
      ClearScratch();
      if (ABSL_PREDICT_FALSE(!write_ok)) return false;
    }
    if (available() >= length && length <= kMaxBytesToCopy) {
      const absl::string_view data(cursor(), IntCast<size_t>(length));
      move_cursor(IntCast<size_t>(length));
      return dest.Write(data);
    }
  }
  return CopyBehindScratch(length, dest);
}

bool PullableReader::CopySlow(size_t length, BackwardWriter& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::CopySlow(BackwardWriter&): "
         "enough data available, use Copy(BackwardWriter&) instead";
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    Chain from_scratch;
    if (!ScratchEnds()) {
      if (available() >= length) {
        const bool write_ok = dest.Write(
            ExternalRef(scratch_->buffer, absl::string_view(cursor(), length)));
        move_cursor(length);
        return write_ok;
      }
      length -= available();
      from_scratch =
          Chain(ExternalRef(std::move(scratch_->buffer),
                            absl::string_view(cursor(), available())));
      ClearScratch();
    }
    if (available() >= length && length <= kMaxBytesToCopy) {
      const absl::string_view data(cursor(), length);
      move_cursor(length);
      if (ABSL_PREDICT_FALSE(!dest.Write(data))) return false;
    } else {
      if (ABSL_PREDICT_FALSE(!CopyBehindScratch(length, dest))) return false;
    }
    return dest.Write(std::move(from_scratch));
  }
  return CopyBehindScratch(length, dest);
}

bool PullableReader::ReadOrPullSomeSlow(
    size_t max_length, absl::FunctionRef<char*(size_t&)> get_dest) {
  RIEGELI_ASSERT_GT(max_length, 0u)
      << "Failed precondition of Reader::ReadOrPullSomeSlow(): "
         "nothing to read, use ReadOrPullSome() instead";
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of Reader::ReadOrPullSomeSlow(): "
         "some data available, use ReadOrPullSome() instead";
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    SyncScratch();
    if (available() > 0) return true;
  }
  return ReadOrPullSomeBehindScratch(max_length, get_dest);
}

void PullableReader::ReadHintSlow(size_t min_length,
                                  size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Reader::ReadHintSlow(): "
         "enough data available, use ReadHint() instead";
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    if (!ScratchEnds()) {
      recommended_length = UnsignedMax(recommended_length, min_length);
      min_length -= available();
      recommended_length -= available();
      BehindScratch behind_scratch(this);
      if (available() < min_length) {
        ReadHintBehindScratch(min_length, recommended_length);
      }
      return;
    }
    if (available() >= min_length) return;
  }
  ReadHintBehindScratch(min_length, recommended_length);
}

bool PullableReader::SyncImpl(SyncType sync_type) {
  if (ABSL_PREDICT_FALSE(scratch_used()) && !ScratchEnds()) {
    if (!SupportsRandomAccess()) {
      // Seeking back is not feasible.
      return ok();
    }
    const Position new_pos = pos();
    SyncScratch();
    Seek(new_pos);
  }
  return SyncBehindScratch(sync_type);
}

bool PullableReader::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of Reader::SeekSlow(): "
         "position in the buffer, use Seek() instead";
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    SyncScratch();
    if (new_pos >= start_pos() && new_pos <= limit_pos()) {
      set_cursor(limit() - (limit_pos() - new_pos));
      return true;
    }
  }
  return SeekBehindScratch(new_pos);
}

void PullableReader::BehindScratch::Enter() {
  RIEGELI_ASSERT(context_->scratch_used())
      << "Failed precondition of PullableReader::BehindScratch::Enter(): "
         "scratch not used";
  RIEGELI_ASSERT_EQ(context_->start(), context_->scratch_->buffer.data())
      << "Failed invariant of PullableReader: "
         "scratch used but buffer pointers do not point to scratch";
  RIEGELI_ASSERT_EQ(context_->start_to_limit(),
                    context_->scratch_->buffer.size())
      << "Failed invariant of PullableReader: "
         "scratch used but buffer pointers do not point to scratch";
  scratch_ = std::move(context_->scratch_);
  read_from_scratch_ = context_->start_to_cursor();
  context_->set_buffer(scratch_->original_start,
                       scratch_->original_start_to_limit,
                       scratch_->original_start_to_cursor);
  context_->move_limit_pos(context_->available());
}

void PullableReader::BehindScratch::Leave() {
  RIEGELI_ASSERT_NE(scratch_, nullptr)
      << "Failed precondition of PullableReader::BehindScratch::Leave(): "
         "scratch not used";
  context_->set_limit_pos(context_->pos());
  scratch_->original_start = context_->start();
  scratch_->original_start_to_limit = context_->start_to_limit();
  scratch_->original_start_to_cursor = context_->start_to_cursor();
  context_->set_buffer(scratch_->buffer.data(), scratch_->buffer.size(),
                       read_from_scratch_);
  context_->scratch_ = std::move(scratch_);
}

}  // namespace riegeli
