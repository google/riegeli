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

#include <cstring>
#include <memory>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void PushableWriter::Done() {
  if (ABSL_PREDICT_TRUE(!scratch_used()) || ABSL_PREDICT_TRUE(SyncScratch())) {
    DoneBehindScratch();
  }
  Writer::Done();
  scratch_.reset();
}

void PushableWriter::OnFail() {
  Writer::OnFail();
  scratch_.reset();
}

void PushableWriter::DoneBehindScratch() {
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableWriter::DoneBehindScratch(): "
         "scratch used";
  FlushBehindScratch(FlushType::kFromObject);
}

inline bool PushableWriter::SyncScratch() {
  RIEGELI_ASSERT(scratch_used())
      << "Failed precondition of PushableWriter::SyncScratch(): "
         "scratch not used";
  RIEGELI_ASSERT(start() == scratch_->buffer.data())
      << "Failed invariant of PushableWriter: "
         "scratch used but buffer pointers do not point to scratch";
  RIEGELI_ASSERT_EQ(start_to_limit(), scratch_->buffer.size())
      << "Failed invariant of PushableWriter: "
         "scratch used but buffer pointers do not point to scratch";
  const size_t length_to_write = start_to_cursor();
  set_buffer(scratch_->original_start, scratch_->original_start_to_limit,
             scratch_->original_start_to_cursor);
  set_start_pos(start_pos() - start_to_cursor());
  ChainBlock buffer = std::move(scratch_->buffer);
  RIEGELI_ASSERT(!scratch_used())
      << "Moving should have left the source ChainBlock cleared";
  if (length_to_write <= kMaxBytesToCopy || PrefersCopying()) {
    if (ABSL_PREDICT_FALSE(!Write(buffer.data(), length_to_write))) {
      return false;
    }
    // Restore buffer allocation.
    buffer.Clear();
    scratch_->buffer = std::move(buffer);
    return true;
  } else if (length_to_write == buffer.size()) {
    return Write(Chain(std::move(buffer)));
  } else {
    Chain data;
    buffer.AppendSubstrTo(absl::string_view(buffer.data(), length_to_write),
                          data);
    return Write(std::move(data));
  }
}

bool PushableWriter::PushSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Writer::PushSlow(): "
         "enough space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    RIEGELI_ASSERT(start() == scratch_->buffer.data())
        << "Failed invariant of PushableWriter: "
           "scratch used but buffer pointers do not point to scratch";
    RIEGELI_ASSERT_EQ(start_to_limit(), scratch_->buffer.size())
        << "Failed invariant of PushableWriter: "
           "scratch used but buffer pointers do not point to scratch";
    if (ABSL_PREDICT_FALSE(!SyncScratch())) return false;
    if (available() >= min_length) return true;
  }
  if (ABSL_PREDICT_TRUE(min_length == 1)) {
    return PushBehindScratch(recommended_length);
  }
  if (available() == 0) {
    if (ABSL_PREDICT_FALSE(!PushBehindScratch(recommended_length))) {
      return false;
    }
    if (available() >= min_length) return true;
    if (ABSL_PREDICT_FALSE(scratch_used())) {
      // `PushBehindScratch()` must have called `ForcePushUsingScratch()` but
      // scratch is too small.
      if (ABSL_PREDICT_FALSE(!SyncScratch())) return false;
      if (available() >= min_length) return true;
    }
  }
  if (ABSL_PREDICT_FALSE(scratch_ == nullptr)) {
    scratch_ = std::make_unique<Scratch>();
  }
  const absl::Span<char> flat_buffer =
      scratch_->buffer.AppendBuffer(min_length, recommended_length);
  set_start_pos(pos());
  scratch_->original_start = start();
  scratch_->original_start_to_limit = start_to_limit();
  scratch_->original_start_to_cursor = start_to_cursor();
  set_buffer(flat_buffer.data(), flat_buffer.size());
  return true;
}

bool PushableWriter::ForcePushUsingScratch() {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of PushableWriter::ForcePushUsingScratch(): "
         "some space available, nothing to do";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableWriter::ForcePushUsingScratch(): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(scratch_ == nullptr)) {
    scratch_ = std::make_unique<Scratch>();
  }
  const absl::Span<char> flat_buffer = scratch_->buffer.AppendBuffer(1);
  set_start_pos(pos());
  scratch_->original_start = start();
  scratch_->original_start_to_limit = start_to_limit();
  scratch_->original_start_to_cursor = start_to_cursor();
  set_buffer(flat_buffer.data(), flat_buffer.size());
  return true;
}

bool PushableWriter::WriteBehindScratch(absl::string_view src) {
  RIEGELI_ASSERT_LT(available(), src.size())
      << "Failed precondition of "
         "PushableWriter::WriteBehindScratch(string_view): "
         "enough space available, use Write(string_view) instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of "
         "PushableWriter::WriteBehindScratch(string_view): "
         "scratch used";
  do {
    const size_t available_length = available();
    if (
        // `std::memcpy(nullptr, _, 0)` is undefined.
        available_length > 0) {
      std::memcpy(cursor(), src.data(), available_length);
      move_cursor(available_length);
      src.remove_prefix(available_length);
    }
    if (ABSL_PREDICT_FALSE(!PushBehindScratch(src.size()))) return false;
  } while (src.size() > available());
  std::memcpy(cursor(), src.data(), src.size());
  move_cursor(src.size());
  return true;
}

bool PushableWriter::WriteBehindScratch(const Chain& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of PushableWriter::WriteBehindScratch(Chain): "
         "enough space available, use Write(Chain) instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableWriter::WriteBehindScratch(Chain): "
         "scratch used";
  for (const absl::string_view fragment : src.blocks()) {
    if (ABSL_PREDICT_FALSE(!Write(fragment))) return false;
  }
  return true;
}

bool PushableWriter::WriteBehindScratch(Chain&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of PushableWriter::WriteBehindScratch(Chain&&): "
         "enough space available, use Write(Chain&&) instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableWriter::WriteBehindScratch(Chain&&): "
         "scratch used";
  // Not `std::move(src)`: forward to `WriteBehindScratch(const Chain&)`.
  return WriteBehindScratch(src);
}

bool PushableWriter::WriteBehindScratch(const absl::Cord& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of PushableWriter::WriteBehindScratch(Cord): "
         "enough space available, use Write(Cord) instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableWriter::WriteBehindScratch(Cord): "
         "scratch used";
  if (const absl::optional<absl::string_view> flat = src.TryFlat()) {
    return Write(*flat);
  }
  for (const absl::string_view fragment : src.Chunks()) {
    if (ABSL_PREDICT_FALSE(!Write(fragment))) return false;
  }
  return true;
}

bool PushableWriter::WriteBehindScratch(absl::Cord&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of PushableWriter::WriteBehindScratch(Cord&&): "
         "enough space available, use Write(Cord&&) instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableWriter::WriteBehindScratch(Cord&&): "
         "scratch used";
  // Not `std::move(src)`: forward to `WriteBehindScratch(const absl::Cord&)`.
  return WriteBehindScratch(src);
}

bool PushableWriter::WriteZerosBehindScratch(Position length) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of PushableWriter::WriteZerosBehindScratch(): "
         "enough space available, use WriteZeros() instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableWriter::WriteZerosBehindScratch(): "
         "scratch used";
  while (length > available()) {
    const size_t available_length = available();
    if (
        // `std::memset(nullptr, _, 0)` is undefined.
        available_length > 0) {
      std::memset(cursor(), 0, available_length);
      move_cursor(available_length);
      length -= available_length;
    }
    if (ABSL_PREDICT_FALSE(
            !PushBehindScratch(SaturatingIntCast<size_t>(length)))) {
      return false;
    }
  }
  std::memset(cursor(), 0, IntCast<size_t>(length));
  move_cursor(IntCast<size_t>(length));
  return true;
}

bool PushableWriter::FlushBehindScratch(FlushType flush_type) {
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableWriter::FlushBehindScratch(): "
         "scratch used";
  return ok();
}

bool PushableWriter::SeekBehindScratch(Position new_pos) {
  RIEGELI_ASSERT_NE(new_pos, pos())
      << "Failed precondition of PushableWriter::SeekBehindScratch(): "
         "position unchanged, use Seek() instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableWriter::SeekBehindScratch(): "
         "scratch used";
  return Fail(absl::UnimplementedError("Writer::Seek() not supported"));
}

absl::optional<Position> PushableWriter::SizeBehindScratch() {
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableWriter::SizeBehindScratch(): "
         "scratch used";
  Fail(absl::UnimplementedError("Writer::Size() not supported"));
  return absl::nullopt;
}

bool PushableWriter::TruncateBehindScratch(Position new_size) {
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableWriter::TruncateBehindScratch(): "
         "scratch used";
  return Fail(absl::UnimplementedError("Writer::Truncate() not supported"));
}

Reader* PushableWriter::ReadModeBehindScratch(Position initial_pos) {
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableWriter::ReadModeBehindScratch(): "
         "scratch used";
  Fail(absl::UnimplementedError("Writer::ReadMode() not supported"));
  return nullptr;
}

bool PushableWriter::WriteSlow(absl::string_view src) {
  RIEGELI_ASSERT_LT(available(), src.size())
      << "Failed precondition of Writer::WriteSlow(string_view): "
         "enough space available, use Write(string_view) instead";
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    if (ABSL_PREDICT_FALSE(!SyncScratch())) return false;
    if (available() >= src.size()) {
      if (ABSL_PREDICT_TRUE(
              // `std::memcpy(nullptr, _, 0)` and `std::memcpy(_, nullptr, 0)`
              // are undefined.
              !src.empty())) {
        std::memcpy(cursor(), src.data(), src.size());
        move_cursor(src.size());
      }
      return true;
    }
  }
  return WriteBehindScratch(src);
}

bool PushableWriter::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain): "
         "enough space available, use Write(Chain) instead";
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    if (ABSL_PREDICT_FALSE(!SyncScratch())) return false;
    if (available() >= src.size() && src.size() <= kMaxBytesToCopy) {
      src.CopyTo(cursor());
      move_cursor(src.size());
      return true;
    }
  }
  return WriteBehindScratch(src);
}

bool PushableWriter::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain&&): "
         "enough space available, use Write(Chain&&) instead";
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    if (ABSL_PREDICT_FALSE(!SyncScratch())) return false;
    if (available() >= src.size() && src.size() <= kMaxBytesToCopy) {
      src.CopyTo(cursor());
      move_cursor(src.size());
      return true;
    }
  }
  return WriteBehindScratch(std::move(src));
}

bool PushableWriter::WriteSlow(const absl::Cord& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Cord): "
         "enough space available, use Write(Cord) instead";
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    if (ABSL_PREDICT_FALSE(!SyncScratch())) return false;
    if (available() >= src.size() && src.size() <= kMaxBytesToCopy) {
      char* dest = cursor();
      for (const absl::string_view fragment : src.Chunks()) {
        std::memcpy(dest, fragment.data(), fragment.size());
        dest += fragment.size();
      }
      set_cursor(dest);
      return true;
    }
  }
  return WriteBehindScratch(src);
}

bool PushableWriter::WriteSlow(absl::Cord&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Cord&&): "
         "enough space available, use Write(Cord&&) instead";
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    if (ABSL_PREDICT_FALSE(!SyncScratch())) return false;
    if (available() >= src.size() && src.size() <= kMaxBytesToCopy) {
      char* dest = cursor();
      for (const absl::string_view fragment : src.Chunks()) {
        std::memcpy(dest, fragment.data(), fragment.size());
        dest += fragment.size();
      }
      set_cursor(dest);
      return true;
    }
  }
  return WriteBehindScratch(std::move(src));
}

bool PushableWriter::WriteZerosSlow(Position length) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Writer::WriteZerosSlow(): "
         "enough space available, use WriteZeros() instead";
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    if (ABSL_PREDICT_FALSE(!SyncScratch())) return false;
    if (available() >= length && length <= kMaxBytesToCopy) {
      if (ABSL_PREDICT_TRUE(
              // `std::memset(nullptr, _, 0)` is undefined.
              length > 0)) {
        std::memset(cursor(), 0, IntCast<size_t>(length));
        move_cursor(IntCast<size_t>(length));
      }
      return true;
    }
  }
  return WriteZerosBehindScratch(length);
}

bool PushableWriter::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    if (ABSL_PREDICT_FALSE(!SyncScratch())) return false;
  }
  return FlushBehindScratch(flush_type);
}

bool PushableWriter::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT_NE(new_pos, pos())
      << "Failed precondition of Writer::SeekSlow(): "
         "position unchanged, use Seek() instead";
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    if (ABSL_PREDICT_FALSE(!SyncScratch())) return false;
  }
  return SeekBehindScratch(new_pos);
}

absl::optional<Position> PushableWriter::SizeImpl() {
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    if (ABSL_PREDICT_FALSE(!SyncScratch())) return false;
  }
  return SizeBehindScratch();
}

bool PushableWriter::TruncateImpl(Position new_size) {
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    if (ABSL_PREDICT_FALSE(!SyncScratch())) return false;
  }
  return TruncateBehindScratch(new_size);
}

Reader* PushableWriter::ReadModeImpl(Position initial_pos) {
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    if (ABSL_PREDICT_FALSE(!SyncScratch())) return nullptr;
  }
  return ReadModeBehindScratch(initial_pos);
}

void PushableWriter::BehindScratch::Enter() {
  RIEGELI_ASSERT(context_->scratch_used())
      << "Failed precondition of PushableWriter::BehindScratch::Enter(): "
         "scratch not used";
  RIEGELI_ASSERT(context_->start() == context_->scratch_->buffer.data())
      << "Failed invariant of PushableWriter: "
         "scratch used but buffer pointers do not point to scratch";
  RIEGELI_ASSERT_EQ(context_->start_to_limit(),
                    context_->scratch_->buffer.size())
      << "Failed invariant of PushableWriter: "
         "scratch used but buffer pointers do not point to scratch";
  scratch_ = std::move(context_->scratch_);
  written_to_scratch_ = context_->start_to_cursor();
  context_->set_buffer(scratch_->original_start,
                       scratch_->original_start_to_limit,
                       scratch_->original_start_to_cursor);
  context_->set_start_pos(context_->start_pos() - context_->start_to_cursor());
}

void PushableWriter::BehindScratch::Leave() {
  RIEGELI_ASSERT(scratch_ != nullptr)
      << "Failed precondition of PushableWriter::BehindScratch::Leave(): "
         "scratch not used";
  context_->set_start_pos(context_->pos());
  scratch_->original_start = context_->start();
  scratch_->original_start_to_limit = context_->start_to_limit();
  scratch_->original_start_to_cursor = context_->start_to_cursor();
  context_->set_buffer(const_cast<char*>(scratch_->buffer.data()),
                       scratch_->buffer.size(), written_to_scratch_);
  context_->scratch_ = std::move(scratch_);
}

}  // namespace riegeli
