// Copyright 2021 Google LLC
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

#include "riegeli/bytes/prefix_limiting_writer.h"

#include <stddef.h>

#include <limits>
#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/prefix_limiting_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void PrefixLimitingWriterBase::Done() {
  if (ABSL_PREDICT_TRUE(ok())) {
    Writer& dest = *DestWriter();
    SyncBuffer(dest);
  }
  Writer::Done();
  associated_reader_.Reset();
}

absl::Status PrefixLimitingWriterBase::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) {
    Writer& dest = *DestWriter();
    SyncBuffer(dest);
    status = dest.AnnotateStatus(std::move(status));
    MakeBuffer(dest);
  }
  // The status might have been annotated by `dest` with the original position.
  // Clarify that the current position is the relative position instead of
  // delegating to `Writer::AnnotateStatusImpl()`.
  return AnnotateOverDest(std::move(status));
}

absl::Status PrefixLimitingWriterBase::AnnotateOverDest(absl::Status status) {
  if (is_open() && base_pos_ > 0) {
    return Annotate(status,
                    absl::StrCat("with relative position at byte ", pos()));
  }
  return status;
}

bool PrefixLimitingWriterBase::PushSlow(size_t min_length,
                                        size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Writer::PushSlow(): "
         "enough space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Writer& dest = *DestWriter();
  SyncBuffer(dest);
  const bool push_ok = dest.Push(min_length, recommended_length);
  MakeBuffer(dest);
  return push_ok;
}

bool PrefixLimitingWriterBase::WriteSlow(absl::string_view src) {
  RIEGELI_ASSERT_LT(available(), src.size())
      << "Failed precondition of Writer::WriteSlow(string_view): "
         "enough space available, use Write(string_view) instead";
  return WriteInternal(src);
}

bool PrefixLimitingWriterBase::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain): "
         "enough space available, use Write(Chain) instead";
  return WriteInternal(src);
}

bool PrefixLimitingWriterBase::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain&&): "
         "enough space available, use Write(Chain&&) instead";
  return WriteInternal(std::move(src));
}

bool PrefixLimitingWriterBase::WriteSlow(const absl::Cord& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Cord): "
         "enough space available, use Write(Cord) instead";
  return WriteInternal(src);
}

bool PrefixLimitingWriterBase::WriteSlow(absl::Cord&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Cord&&): "
         "enough space available, use Write(Cord&&) instead";
  return WriteInternal(std::move(src));
}

template <typename Src>
inline bool PrefixLimitingWriterBase::WriteInternal(Src&& src) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Writer& dest = *DestWriter();
  SyncBuffer(dest);
  const bool write_ok = dest.Write(std::forward<Src>(src));
  MakeBuffer(dest);
  return write_ok;
}

bool PrefixLimitingWriterBase::WriteZerosSlow(Position length) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Writer::WriteZerosSlow(): "
         "enough space available, use WriteZeros() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Writer& dest = *DestWriter();
  SyncBuffer(dest);
  const bool write_ok = dest.WriteZeros(length);
  MakeBuffer(dest);
  return write_ok;
}

bool PrefixLimitingWriterBase::SupportsRandomAccess() {
  Writer* const dest = DestWriter();
  return dest != nullptr && dest->SupportsRandomAccess();
}

bool PrefixLimitingWriterBase::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT_NE(new_pos, pos())
      << "Failed precondition of Writer::SeekSlow(): "
         "position unchanged, use Seek() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Writer& dest = *DestWriter();
  SyncBuffer(dest);
  bool seek_ok;
  if (ABSL_PREDICT_FALSE(new_pos >
                         std::numeric_limits<Position>::max() - base_pos_)) {
    dest.Seek(std::numeric_limits<Position>::max());
    seek_ok = false;
  } else {
    seek_ok = dest.Seek(new_pos + base_pos_);
  }
  MakeBuffer(dest);
  return seek_ok;
}

bool PrefixLimitingWriterBase::PrefersCopying() const {
  const Writer* const dest = DestWriter();
  return dest != nullptr && dest->PrefersCopying();
}

absl::optional<Position> PrefixLimitingWriterBase::SizeImpl() {
  if (ABSL_PREDICT_FALSE(!ok())) return absl::nullopt;
  Writer& dest = *DestWriter();
  SyncBuffer(dest);
  const absl::optional<Position> size = dest.Size();
  MakeBuffer(dest);
  if (ABSL_PREDICT_FALSE(size == absl::nullopt)) return absl::nullopt;
  return SaturatingSub(*size, base_pos_);
}

bool PrefixLimitingWriterBase::SupportsTruncate() {
  Writer* const dest = DestWriter();
  return dest != nullptr && dest->SupportsTruncate();
}

bool PrefixLimitingWriterBase::TruncateImpl(Position new_size) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Writer& dest = *DestWriter();
  SyncBuffer(dest);
  bool truncate_ok;
  if (ABSL_PREDICT_FALSE(new_size >
                         std::numeric_limits<Position>::max() - base_pos_)) {
    dest.Seek(std::numeric_limits<Position>::max());
    truncate_ok = false;
  } else {
    truncate_ok = dest.Truncate(new_size + base_pos_);
  }
  MakeBuffer(dest);
  return truncate_ok;
}

bool PrefixLimitingWriterBase::SupportsReadMode() {
  Writer* const dest = DestWriter();
  return dest != nullptr && dest->SupportsReadMode();
}

Reader* PrefixLimitingWriterBase::ReadModeImpl(Position initial_pos) {
  if (ABSL_PREDICT_FALSE(!ok())) return nullptr;
  Writer& dest = *DestWriter();
  SyncBuffer(dest);
  Reader* const reader = dest.ReadMode(SaturatingAdd(initial_pos, base_pos_));
  MakeBuffer(dest);
  if (ABSL_PREDICT_FALSE(reader == nullptr)) return nullptr;
  return associated_reader_.ResetReader(
      reader, PrefixLimitingReaderBase::Options().set_base_pos(base_pos_));
}

}  // namespace riegeli
