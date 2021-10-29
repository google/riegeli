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

#include "riegeli/bytes/cord_reader.h"

#include <stddef.h>

#include <limits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/pullable_reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void CordReaderBase::Done() {
  PullableReader::Done();
  iter_ = absl::nullopt;
}

bool CordReaderBase::PullBehindScratch() {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of PullableReader::PullBehindScratch(): "
         "enough data available, use Pull() instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::PullBehindScratch(): "
         "scratch used";
  if (iter_ == absl::nullopt) return false;
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  const absl::Cord& src = *src_cord();
  RIEGELI_ASSERT_LE(limit_pos(), src.size())
      << "CordReader source changed unexpectedly";
  absl::Cord::Advance(&*iter_, start_to_cursor());
  if (ABSL_PREDICT_FALSE(*iter_ == src.char_end())) {
    set_buffer();
    return false;
  }
  const absl::string_view fragment = absl::Cord::ChunkRemaining(*iter_);
  set_buffer(fragment.data(), fragment.size());
  move_limit_pos(available());
  return true;
}

bool CordReaderBase::ReadBehindScratch(size_t length, Chain& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of PullableReader::ReadBehindScratch(Chain&): "
         "enough data available, use Read(Chain&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of PullableReader::ReadBehindScratch(Chain&): "
         "Chain size overflow";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::ReadBehindScratch(Chain&): "
         "scratch used";
  if (iter_ == absl::nullopt) {
    return PullableReader::ReadBehindScratch(length, dest);
  }
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  const absl::Cord& src = *src_cord();
  RIEGELI_ASSERT_LE(limit_pos(), src.size())
      << "CordReader source changed unexpectedly";
  SyncBuffer();
  const size_t length_to_read = UnsignedMin(length, src.size() - limit_pos());
  dest.AppendFrom(*iter_, length_to_read);
  move_limit_pos(length_to_read);
  MakeBuffer(src);
  return length_to_read == length;
}

bool CordReaderBase::ReadBehindScratch(size_t length, absl::Cord& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of PullableReader::ReadBehindScratch(Cord&): "
         "enough data available, use Read(Cord&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of PullableReader::ReadBehindScratch(Cord&): "
         "Cord size overflow";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::ReadBehindScratch(Cord&): "
         "scratch used";
  if (iter_ == absl::nullopt) {
    return PullableReader::ReadBehindScratch(length, dest);
  }
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  const absl::Cord& src = *src_cord();
  RIEGELI_ASSERT_LE(limit_pos(), src.size())
      << "CordReader source changed unexpectedly";
  SyncBuffer();
  const size_t length_to_read = UnsignedMin(length, src.size() - limit_pos());
  if (length_to_read == src.size()) {
    dest.Append(src);
    *iter_ = src.char_end();
  } else {
    dest.Append(absl::Cord::AdvanceAndRead(&*iter_, length_to_read));
  }
  move_limit_pos(length_to_read);
  MakeBuffer(src);
  return length_to_read == length;
}

bool CordReaderBase::CopyBehindScratch(Position length, Writer& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of PullableReader::CopyBehindScratch(Writer&): "
         "enough data available, use Copy(Writer&) instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::CopyBehindScratch(Writer&): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  const absl::Cord& src = *src_cord();
  RIEGELI_ASSERT_LE(limit_pos(), src.size())
      << "CordReader source changed unexpectedly";
  const size_t length_to_copy =
      UnsignedMin(length, src.size() - IntCast<size_t>(pos()));
  if (length_to_copy == src.size()) {
    if (!Skip(length_to_copy)) {
      RIEGELI_ASSERT_UNREACHABLE() << "CordReader::Skip() failed";
    }
    if (ABSL_PREDICT_FALSE(!dest.Write(src))) return false;
  } else if (length_to_copy <= kMaxBytesToCopy) {
    if (ABSL_PREDICT_FALSE(!dest.Push(length_to_copy))) return false;
    if (!Read(length_to_copy, dest.cursor())) {
      RIEGELI_ASSERT_UNREACHABLE() << "CordReader::Read(char*) failed";
    }
    dest.move_cursor(length_to_copy);
  } else {
    absl::Cord data;
    if (!Read(length_to_copy, data)) {
      RIEGELI_ASSERT_UNREACHABLE() << "CordReader::Read(Cord&) failed";
    }
    if (ABSL_PREDICT_FALSE(!dest.Write(std::move(data)))) return false;
  }
  return length_to_copy == length;
}

bool CordReaderBase::CopyBehindScratch(size_t length, BackwardWriter& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of "
         "PullableReader::CopyBehindScratch(BackwardWriter&): "
         "enough data available, use Copy(BackwardWriter&) instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of "
         "PullableReader::CopyBehindScratch(BackwardWriter&): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  const absl::Cord& src = *src_cord();
  RIEGELI_ASSERT_LE(limit_pos(), src.size())
      << "CordReader source changed unexpectedly";
  if (ABSL_PREDICT_FALSE(length > src.size() - pos())) {
    if (!Seek(src.size())) {
      RIEGELI_ASSERT_UNREACHABLE() << "CordReader::Seek() failed";
    }
    return false;
  }
  if (length == src.size()) {
    if (!Skip(length)) {
      RIEGELI_ASSERT_UNREACHABLE() << "CordReader::Skip() failed";
    }
    return dest.Write(src);
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
  absl::Cord data;
  if (!ReadBehindScratch(length, data)) {
    RIEGELI_ASSERT_UNREACHABLE()
        << "CordReader::ReadBehindScratch(Cord&) failed";
  }
  return dest.Write(std::move(data));
}

bool CordReaderBase::SeekBehindScratch(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of PullableReader::SeekBehindScratch(): "
         "position in the buffer, use Seek() instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::SeekBehindScratch(): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (iter_ == absl::nullopt) {
    RIEGELI_ASSERT_EQ(start_pos(), 0u)
        << "Failed invariant of CordReaderBase: "
           "no Cord iterator but non-zero position of buffer start";
    // Seeking forwards. Source ends.
    set_cursor(limit());
    return false;
  }
  const absl::Cord& src = *src_cord();
  RIEGELI_ASSERT_LE(limit_pos(), src.size())
      << "CordReader source changed unexpectedly";
  size_t length;
  if (new_pos > limit_pos()) {
    // Seeking forwards.
    if (new_pos >= src.size()) {
      // Source ends.
      *iter_ = src.char_end();
      set_buffer();
      set_limit_pos(src.size());
      return new_pos == src.size();
    }
    length = IntCast<size_t>(new_pos - start_pos());
  } else {
    // Seeking backwards.
    *iter_ = src.char_begin();
    length = IntCast<size_t>(new_pos);
  }
  absl::Cord::Advance(&*iter_, length);
  set_limit_pos(new_pos);
  MakeBuffer(src);
  return true;
}

absl::optional<Position> CordReaderBase::SizeImpl() {
  if (ABSL_PREDICT_FALSE(!healthy())) return absl::nullopt;
  const absl::Cord& src = *src_cord();
  return src.size();
}

inline void CordReaderBase::SyncBuffer() {
  RIEGELI_ASSERT(iter_ != absl::nullopt)
      << "Failed precondition of CordReaderBase::SyncBuffer(): "
         "no Cord iterator";
  set_limit_pos(pos());
  absl::Cord::Advance(&*iter_, start_to_cursor());
  set_buffer();
}

}  // namespace riegeli
