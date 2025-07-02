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

#include "riegeli/bytes/pushable_backward_writer.h"

#include <stddef.h>

#include <cstring>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/byte_fill.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/cord_utils.h"
#include "riegeli/base/external_ref.h"
#include "riegeli/base/null_safe_memcpy.h"
#include "riegeli/base/sized_shared_buffer.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/backward_writer.h"

namespace riegeli {

void PushableBackwardWriter::Done() {
  if (ABSL_PREDICT_TRUE(!scratch_used()) || ABSL_PREDICT_TRUE(SyncScratch())) {
    DoneBehindScratch();
  }
  BackwardWriter::Done();
  scratch_.reset();
}

void PushableBackwardWriter::OnFail() {
  BackwardWriter::OnFail();
  scratch_.reset();
}

void PushableBackwardWriter::DoneBehindScratch() {
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableBackwardWriter::DoneBehindScratch(): "
         "scratch used";
  FlushBehindScratch(FlushType::kFromObject);
}

inline bool PushableBackwardWriter::SyncScratch() {
  RIEGELI_ASSERT(scratch_used())
      << "Failed precondition of PushableBackwardWriter::SyncScratch(): "
         "scratch not used";
  RIEGELI_ASSERT_EQ(limit(), scratch_->buffer.data())
      << "Failed invariant of PushableBackwardWriter: "
         "scratch used but buffer pointers do not point to scratch";
  RIEGELI_ASSERT_EQ(start_to_limit(), scratch_->buffer.size())
      << "Failed invariant of PushableBackwardWriter: "
         "scratch used but buffer pointers do not point to scratch";
  const size_t length_to_write = start_to_cursor();
  set_buffer(scratch_->original_limit, scratch_->original_start_to_limit,
             scratch_->original_start_to_cursor);
  set_start_pos(start_pos() - start_to_cursor());
  SizedSharedBuffer buffer = std::move(scratch_->buffer);
  RIEGELI_ASSERT(!scratch_used())
      << "Moving should have left the source SizedSharedBuffer cleared";
  const char* const data = buffer.data() + buffer.size() - length_to_write;
  if (ABSL_PREDICT_FALSE(!Write(ExternalRef(
          std::move(buffer), absl::string_view(data, length_to_write))))) {
    return false;
  }
  RIEGELI_ASSERT(!scratch_used())
      << "WriteSlow(absl::string_view) must not start using scratch, "
         "in particular if PushBehindScratch() calls ForcePushUsingScratch() "
         "then WriteSlow(absl::string_view) must be overridden to avoid "
         "indirectly calling ForcePushUsingScratch()";
  // Restore buffer allocation.
  buffer.ClearAndShrink();
  scratch_->buffer = std::move(buffer);
  return true;
}

bool PushableBackwardWriter::PushSlow(size_t min_length,
                                      size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of BackwardWriter::PushSlow(): "
         "enough space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    RIEGELI_ASSERT_EQ(limit(), scratch_->buffer.data())
        << "Failed invariant of PushableBackwardWriter: "
           "scratch used but buffer pointers do not point to scratch";
    RIEGELI_ASSERT_EQ(start_to_limit(), scratch_->buffer.size())
        << "Failed invariant of PushableBackwardWriter: "
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
      scratch_->buffer.PrependBuffer(min_length, recommended_length);
  set_start_pos(pos());
  scratch_->original_limit = limit();
  scratch_->original_start_to_limit = start_to_limit();
  scratch_->original_start_to_cursor = start_to_cursor();
  set_buffer(flat_buffer.data(), flat_buffer.size());
  return true;
}

bool PushableBackwardWriter::ForcePushUsingScratch() {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of "
         "PushableBackwardWriter::ForcePushUsingScratch(): "
         "some space available, nothing to do";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of "
         "PushableBackwardWriter::ForcePushUsingScratch(): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(scratch_ == nullptr)) {
    scratch_ = std::make_unique<Scratch>();
  }
  const absl::Span<char> flat_buffer = scratch_->buffer.PrependBuffer(1);
  set_start_pos(pos());
  scratch_->original_limit = limit();
  scratch_->original_start_to_limit = start_to_limit();
  scratch_->original_start_to_cursor = start_to_cursor();
  set_buffer(flat_buffer.data(), flat_buffer.size());
  return true;
}

bool PushableBackwardWriter::WriteBehindScratch(absl::string_view src) {
  RIEGELI_ASSERT_LT(available(), src.size())
      << "Failed precondition of "
         "PushableBackwardWriter::WriteBehindScratch(string_view): "
         "enough space available, use Write(string_view) instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of "
         "PushableBackwardWriter::WriteBehindScratch(string_view): "
         "scratch used";
  do {
    const size_t available_length = available();
    move_cursor(available_length);
    riegeli::null_safe_memcpy(
        cursor(), src.data() + src.size() - available_length, available_length);
    src.remove_suffix(available_length);
    if (ABSL_PREDICT_FALSE(!PushBehindScratch(src.size()))) return false;
  } while (src.size() > available());
  move_cursor(src.size());
  std::memcpy(cursor(), src.data(), src.size());
  return true;
}

bool PushableBackwardWriter::WriteBehindScratch(ExternalRef src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of "
         "PushableBackwardWriter::WriteBehindScratch(ExternalRef): "
         "enough space available, use Write(ExternalRef) instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of "
         "PushableBackwardWriter::WriteBehindScratch(ExternalRef): "
         "scratch used";
  return Write(absl::string_view(src));
}

bool PushableBackwardWriter::WriteBehindScratch(const Chain& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of "
         "PushableBackwardWriter::WriteBehindScratch(Chain): "
         "enough space available, use Write(Chain) instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of "
         "PushableBackwardWriter::WriteBehindScratch(Chain): "
         "scratch used";
  for (Chain::Blocks::const_reverse_iterator iter = src.blocks().crbegin();
       iter != src.blocks().crend(); ++iter) {
    if (ABSL_PREDICT_FALSE(!Write(absl::string_view(*iter)))) return false;
  }
  return true;
}

bool PushableBackwardWriter::WriteBehindScratch(Chain&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of "
         "PushableBackwardWriter::WriteBehindScratch(Chain&&): "
         "enough space available, use Write(Chain&&) instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of "
         "PushableBackwardWriter::WriteBehindScratch(Chain&&): "
         "scratch used";
  // Not `std::move(src)`: forward to `WriteBehindScratch(const Chain&)`.
  return WriteBehindScratch(src);
}

bool PushableBackwardWriter::WriteBehindScratch(const absl::Cord& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of "
         "PushableBackwardWriter::WriteBehindScratch(Cord): "
         "enough space available, use Write(Cord) instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of "
         "PushableBackwardWriter::WriteBehindScratch(Cord): "
         "scratch used";
  if (const std::optional<absl::string_view> flat = src.TryFlat();
      flat != std::nullopt) {
    return Write(*flat);
  }
  if (src.size() <= available()) {
    move_cursor(src.size());
    cord_internal::CopyCordToArray(src, cursor());
    return true;
  }
  std::vector<absl::string_view> fragments(src.chunk_begin(), src.chunk_end());
  for (std::vector<absl::string_view>::const_reverse_iterator iter =
           fragments.crbegin();
       iter != fragments.crend(); ++iter) {
    if (ABSL_PREDICT_FALSE(!Write(*iter))) return false;
  }
  return true;
}

bool PushableBackwardWriter::WriteBehindScratch(absl::Cord&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of "
         "PushableBackwardWriter::WriteBehindScratch(Cord&&): "
         "enough space available, use Write(Cord&&) instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of "
         "PushableBackwardWriter::WriteBehindScratch(Cord&&): "
         "scratch used";
  // Not `std::move(src)`: forward to `WriteBehindScratch(const absl::Cord&)`.
  return WriteBehindScratch(src);
}

bool PushableBackwardWriter::WriteBehindScratch(ByteFill src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of "
         "PushableBackwardWriter::WriteBehindScratch(ByteFill): "
         "enough space available, use Write(ByteFill) instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of "
         "PushableBackwardWriter::WriteBehindScratch(ByteFill): "
         "scratch used";
  while (src.size() > available()) {
    const size_t available_length = available();
    move_cursor(available_length);
    riegeli::null_safe_memset(cursor(), src.fill(), available_length);
    src.Extract(available_length);
    if (ABSL_PREDICT_FALSE(
            !PushBehindScratch(SaturatingIntCast<size_t>(src.size())))) {
      return false;
    }
  }
  move_cursor(IntCast<size_t>(src.size()));
  std::memset(cursor(), src.fill(), IntCast<size_t>(src.size()));
  return true;
}

bool PushableBackwardWriter::FlushBehindScratch(FlushType flush_type) {
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableBackwardWriter::FlushBehindScratch(): "
         "scratch used";
  return ok();
}

bool PushableBackwardWriter::TruncateBehindScratch(Position new_size) {
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of "
         "PushableBackwardWriter::TruncateBehindScratch(): "
         "scratch used";
  return Fail(
      absl::UnimplementedError("BackwardWriter::Truncate() not supported"));
}

bool PushableBackwardWriter::WriteSlow(absl::string_view src) {
  RIEGELI_ASSERT_LT(available(), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(string_view): "
         "enough space available, use Write(string_view) instead";
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    if (ABSL_PREDICT_FALSE(!SyncScratch())) return false;
    if (available() >= src.size()) {
      move_cursor(src.size());
      riegeli::null_safe_memcpy(cursor(), src.data(), src.size());
      return true;
    }
  }
  return WriteBehindScratch(src);
}

bool PushableBackwardWriter::WriteSlow(ExternalRef src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(ExternalRef): "
         "enough space available, use Write(ExternalRef) instead";
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    if (ABSL_PREDICT_FALSE(!SyncScratch())) return false;
    if (available() >= src.size() && src.size() <= kMaxBytesToCopy) {
      move_cursor(src.size());
      riegeli::null_safe_memcpy(cursor(), src.data(), src.size());
      return true;
    }
  }
  return WriteBehindScratch(std::move(src));
}

bool PushableBackwardWriter::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Chain): "
         "enough space available, use Write(Chain) instead";
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    if (ABSL_PREDICT_FALSE(!SyncScratch())) return false;
    if (available() >= src.size() && src.size() <= kMaxBytesToCopy) {
      move_cursor(src.size());
      src.CopyTo(cursor());
      return true;
    }
  }
  return WriteBehindScratch(src);
}

bool PushableBackwardWriter::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Chain&&): "
         "enough space available, use Write(Chain&&) instead";
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    if (ABSL_PREDICT_FALSE(!SyncScratch())) return false;
    if (available() >= src.size() && src.size() <= kMaxBytesToCopy) {
      move_cursor(src.size());
      src.CopyTo(cursor());
      return true;
    }
  }
  return WriteBehindScratch(std::move(src));
}

bool PushableBackwardWriter::WriteSlow(const absl::Cord& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Cord): "
         "enough space available, use Write(Cord) instead";
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    if (ABSL_PREDICT_FALSE(!SyncScratch())) return false;
    if (available() >= src.size() && src.size() <= kMaxBytesToCopy) {
      move_cursor(src.size());
      cord_internal::CopyCordToArray(src, cursor());
      return true;
    }
  }
  return WriteBehindScratch(src);
}

bool PushableBackwardWriter::WriteSlow(absl::Cord&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Cord&&): "
         "enough space available, use Write(Cord&&) instead";
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    if (ABSL_PREDICT_FALSE(!SyncScratch())) return false;
    if (available() >= src.size() && src.size() <= kMaxBytesToCopy) {
      move_cursor(src.size());
      cord_internal::CopyCordToArray(src, cursor());
      return true;
    }
  }
  return WriteBehindScratch(std::move(src));
}

bool PushableBackwardWriter::WriteSlow(ByteFill src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(ByteFill): "
         "enough space available, use Write(ByteFill) instead";
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    if (ABSL_PREDICT_FALSE(!SyncScratch())) return false;
    if (available() >= src.size() && src.size() <= kMaxBytesToCopy) {
      riegeli::null_safe_memset(cursor(), src.fill(),
                                IntCast<size_t>(src.size()));
      move_cursor(IntCast<size_t>(src.size()));
      return true;
    }
  }
  return WriteBehindScratch(src);
}

bool PushableBackwardWriter::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    if (ABSL_PREDICT_FALSE(!SyncScratch())) return false;
  }
  return FlushBehindScratch(flush_type);
}

bool PushableBackwardWriter::TruncateImpl(Position new_size) {
  if (ABSL_PREDICT_FALSE(scratch_used())) {
    if (ABSL_PREDICT_FALSE(!SyncScratch())) return false;
  }
  return TruncateBehindScratch(new_size);
}

void PushableBackwardWriter::BehindScratch::Enter() {
  RIEGELI_ASSERT(context_->scratch_used())
      << "Failed precondition of "
         "PushableBackwardWriter::BehindScratch::Enter(): "
         "scratch not used";
  RIEGELI_ASSERT_EQ(context_->limit(), context_->scratch_->buffer.data())
      << "Failed invariant of PushableBackwardWriter: "
         "scratch used but buffer pointers do not point to scratch";
  RIEGELI_ASSERT_EQ(context_->start_to_limit(),
                    context_->scratch_->buffer.size())
      << "Failed invariant of PushableBackwardWriter: "
         "scratch used but buffer pointers do not point to scratch";
  scratch_ = std::move(context_->scratch_);
  written_to_scratch_ = context_->start_to_cursor();
  context_->set_buffer(scratch_->original_limit,
                       scratch_->original_start_to_limit,
                       scratch_->original_start_to_cursor);
  context_->set_start_pos(context_->start_pos() - context_->start_to_cursor());
}

void PushableBackwardWriter::BehindScratch::Leave() {
  RIEGELI_ASSERT_NE(scratch_, nullptr)
      << "Failed precondition of "
         "PushableBackwardWriter::BehindScratch::Leave(): "
         "scratch not used";
  context_->set_start_pos(context_->pos());
  scratch_->original_limit = context_->limit();
  scratch_->original_start_to_limit = context_->start_to_limit();
  scratch_->original_limit = context_->limit();
  context_->set_buffer(const_cast<char*>(scratch_->buffer.data()),
                       scratch_->buffer.size(), written_to_scratch_);
  context_->scratch_ = std::move(scratch_);
}

}  // namespace riegeli
