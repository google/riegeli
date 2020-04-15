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

bool CordReaderBase::PullSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_GT(min_length, available())
      << "Failed precondition of Reader::PullSlow(): "
         "length too small, use Pull() instead";
  if (ABSL_PREDICT_FALSE(!PullUsingScratch(min_length, recommended_length))) {
    return available() >= min_length;
  }
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  const absl::Cord* const src = src_cord();
  RIEGELI_ASSERT_LE(limit_pos(), src->size())
      << "CordReader source changed unexpectedly";
  absl::Cord::Advance(&iter_, read_from_buffer());
  if (ABSL_PREDICT_FALSE(iter_ == src->char_end())) {
    set_buffer();
    return false;
  }
  const absl::string_view fragment = absl::Cord::ChunkRemaining(iter_);
  set_buffer(fragment.data(), fragment.size());
  move_limit_pos(available());
  return true;
}

bool CordReaderBase::ReadSlow(Chain* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::ReadSlow(Chain*): "
         "length too small, use Read(Chain*) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Reader::ReadSlow(Chain*): "
         "Chain size overflow";
  if (ABSL_PREDICT_FALSE(!ReadScratch(dest, &length))) return length == 0;
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  const absl::Cord* const src = src_cord();
  RIEGELI_ASSERT_LE(limit_pos(), src->size())
      << "CordReader source changed unexpectedly";
  SyncBuffer();
  const size_t length_to_read = UnsignedMin(length, src->size() - limit_pos());
  dest->AppendFrom(&iter_, length_to_read);
  move_limit_pos(length_to_read);
  MakeBuffer(src);
  return length_to_read == length;
}

bool CordReaderBase::ReadSlow(absl::Cord* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::ReadSlow(Cord*): "
         "length too small, use Read(Cord*) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Reader::ReadSlow(Cord*): "
         "Cord size overflow";
  if (ABSL_PREDICT_FALSE(!ReadScratch(dest, &length))) return length == 0;
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  const absl::Cord* const src = src_cord();
  RIEGELI_ASSERT_LE(limit_pos(), src->size())
      << "CordReader source changed unexpectedly";
  SyncBuffer();
  const size_t length_to_read = UnsignedMin(length, src->size() - limit_pos());
  if (length_to_read == src->size()) {
    dest->Append(*src);
    iter_ = src->char_end();
  } else {
    dest->Append(absl::Cord::AdvanceAndRead(&iter_, length_to_read));
  }
  move_limit_pos(length_to_read);
  MakeBuffer(src);
  return length_to_read == length;
}

bool CordReaderBase::CopyToSlow(Writer* dest, Position length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::CopyToSlow(Writer*): "
         "length too small, use CopyTo(Writer*) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  const absl::Cord* const src = src_cord();
  RIEGELI_ASSERT_LE(limit_pos(), src->size())
      << "CordReader source changed unexpectedly";
  const Position length_to_copy = UnsignedMin(length, src->size() - pos());
  bool ok;
  if (length_to_copy == src->size()) {
    if (!Skip(length_to_copy)) {
      RIEGELI_ASSERT_UNREACHABLE() << "CordReader::Skip() failed";
    }
    ok = dest->Write(*src);
  } else if (length_to_copy <= kMaxBytesToCopy) {
    if (ABSL_PREDICT_FALSE(!dest->Push(IntCast<size_t>(length_to_copy)))) {
      return false;
    }
    if (!Read(dest->cursor(), IntCast<size_t>(length_to_copy))) {
      RIEGELI_ASSERT_UNREACHABLE() << "CordReader::Read(char*) failed";
    }
    dest->move_cursor(IntCast<size_t>(length_to_copy));
    ok = true;
  } else {
    absl::Cord data;
    if (!Read(&data, IntCast<size_t>(length_to_copy))) {
      RIEGELI_ASSERT_UNREACHABLE() << "CordReader::Read(Cord*) failed";
    }
    ok = dest->Write(std::move(data));
  }
  return ok && length_to_copy == length;
}

bool CordReaderBase::CopyToSlow(BackwardWriter* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::CopyToSlow(BackwardWriter*): "
         "length too small, use CopyTo(BackwardWriter*) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  const absl::Cord* const src = src_cord();
  RIEGELI_ASSERT_LE(limit_pos(), src->size())
      << "CordReader source changed unexpectedly";
  if (ABSL_PREDICT_FALSE(length > src->size() - pos())) {
    if (!Seek(src->size())) {
      RIEGELI_ASSERT_UNREACHABLE() << "CordReader::Seek() failed";
    }
    return false;
  }
  if (length == src->size()) {
    if (!Skip(length)) {
      RIEGELI_ASSERT_UNREACHABLE() << "CordReader::Skip() failed";
    }
    return dest->Write(*src);
  }
  if (length <= kMaxBytesToCopy) {
    if (ABSL_PREDICT_FALSE(!dest->Push(length))) return false;
    dest->move_cursor(length);
    if (ABSL_PREDICT_FALSE(!ReadSlow(dest->cursor(), length))) {
      dest->set_cursor(dest->cursor() + length);
      return false;
    }
    return true;
  }
  absl::Cord data;
  if (!ReadSlow(&data, length)) {
    RIEGELI_ASSERT_UNREACHABLE() << "CordReader::ReadSlow(Cord*) failed";
  }
  return dest->Write(std::move(data));
}

bool CordReaderBase::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of Reader::SeekSlow(): "
         "position in the buffer, use Seek() instead";
  if (ABSL_PREDICT_FALSE(!SeekUsingScratch(new_pos))) return true;
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  const absl::Cord* const src = src_cord();
  RIEGELI_ASSERT_LE(limit_pos(), src->size())
      << "CordReader source changed unexpectedly";
  size_t length;
  if (new_pos > limit_pos()) {
    // Seeking forwards.
    if (ABSL_PREDICT_FALSE(new_pos > src->size())) {
      // Source ends.
      iter_ = src->char_end();
      set_buffer();
      set_limit_pos(src->size());
      return false;
    }
    length = IntCast<size_t>(new_pos - start_pos());
  } else {
    // Seeking backwards.
    iter_ = src->char_begin();
    length = IntCast<size_t>(new_pos);
  }
  absl::Cord::Advance(&iter_, length);
  set_limit_pos(new_pos);
  MakeBuffer(src);
  return true;
}

absl::optional<Position> CordReaderBase::Size() {
  if (ABSL_PREDICT_FALSE(!healthy())) return absl::nullopt;
  const absl::Cord* const src = src_cord();
  return src->size();
}

inline void CordReaderBase::SyncBuffer() {
  set_limit_pos(pos());
  absl::Cord::Advance(&iter_, read_from_buffer());
  set_buffer();
}

}  // namespace riegeli
