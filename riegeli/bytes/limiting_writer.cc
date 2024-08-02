// Copyright 2018 Google LLC
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

#include "riegeli/bytes/limiting_writer.h"

#include <stddef.h>

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
#include "riegeli/base/byte_fill.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/external_ref.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void LimitingWriterBase::Done() {
  Writer& dest = *DestWriter();
  if (ABSL_PREDICT_TRUE(ok())) SyncBuffer(dest);
  if (exact_ && ABSL_PREDICT_FALSE(pos() < max_pos_)) {
    // Do not call `Fail()` because `AnnotateStatusImpl()` synchronizes the
    // buffer again.
    FailWithoutAnnotation(dest.AnnotateStatus(absl::InvalidArgumentError(
        absl::StrCat("Not enough data: expected ", max_pos_))));
  }
  Writer::Done();
}

bool LimitingWriterBase::FailLimitExceeded() {
  Writer& dest = *DestWriter();
  return FailLimitExceeded(dest);
}

bool LimitingWriterBase::FailLimitExceeded(Writer& dest) {
  set_start_pos(max_pos_);
  set_buffer();
  // Do not call `Fail()` because `AnnotateStatusImpl()` synchronizes the buffer
  // again.
  return FailWithoutAnnotation(dest.AnnotateStatus(
      absl::ResourceExhaustedError("Position limit exceeded")));
}

void LimitingWriterBase::FailLengthOverflow(Position max_length) {
  Fail(absl::InvalidArgumentError(
      absl::StrCat("Not enough data: expected ", pos(), " + ", max_length,
                   " which overflows the Writer position")));
}

absl::Status LimitingWriterBase::AnnotateStatusImpl(absl::Status status) {
  // Fully delegate annotations to `*DestWriter()`.
  if (is_open()) {
    Writer& dest = *DestWriter();
    const bool sync_buffer_ok = SyncBuffer(dest);
    status = dest.AnnotateStatus(std::move(status));
    if (ABSL_PREDICT_TRUE(sync_buffer_ok)) MakeBuffer(dest);
  }
  return status;
}

bool LimitingWriterBase::PushSlow(size_t min_length,
                                  size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Writer::PushSlow(): "
         "enough space available, use Push() instead";
  RIEGELI_ASSERT_LE(start_pos(), max_pos_)
      << "Failed invariant of LimitingWriterBase: "
         "position already exceeds its limit";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Writer& dest = *DestWriter();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(dest))) return false;
  const bool push_ok = dest.Push(min_length, recommended_length);
  MakeBuffer(dest);
  return push_ok;
}

bool LimitingWriterBase::WriteSlow(absl::string_view src) {
  RIEGELI_ASSERT_LT(available(), src.size())
      << "Failed precondition of Writer::WriteSlow(string_view): "
         "enough space available, use Write(string_view) instead";
  return WriteInternal(src, [](absl::string_view src, size_t length) {
    src.remove_suffix(length);
    return src;
  });
}

bool LimitingWriterBase::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain): "
         "enough space available, use Write(Chain) instead";
  return WriteInternal(src, [](const Chain& src, size_t length) {
    Chain result = src;
    result.RemoveSuffix(length);
    return result;
  });
}

bool LimitingWriterBase::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain&&): "
         "enough space available, use Write(Chain&&) instead";
  return WriteInternal(std::move(src),
                       [](Chain&& src, size_t length) -> Chain&& {
                         src.RemoveSuffix(length);
                         return std::move(src);
                       });
}

bool LimitingWriterBase::WriteSlow(const absl::Cord& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Cord): "
         "enough space available, use Write(Cord) instead";
  return WriteInternal(src, [](const absl::Cord& src, size_t length) {
    absl::Cord result = src;
    result.RemoveSuffix(length);
    return result;
  });
}

bool LimitingWriterBase::WriteSlow(absl::Cord&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Cord&&): "
         "enough space available, use Write(Cord&&) instead";
  return WriteInternal(std::move(src),
                       [](absl::Cord&& src, size_t length) -> absl::Cord&& {
                         src.RemoveSuffix(length);
                         return std::move(src);
                       });
}

bool LimitingWriterBase::WriteSlow(ExternalRef src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(ExternalRef): "
         "enough space available, use Write(ExternalRef) instead";
  return WriteInternal(std::move(src), [](ExternalRef src, size_t length) {
    Chain result(std::move(src));
    result.RemoveSuffix(length);
    return result;
  });
}

template <typename Src, typename RemoveSuffix>
inline bool LimitingWriterBase::WriteInternal(Src&& src,
                                              RemoveSuffix&& remove_suffix) {
  RIEGELI_ASSERT_LE(start_pos(), max_pos_)
      << "Failed invariant of LimitingWriterBase: "
         "position already exceeds its limit";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Writer& dest = *DestWriter();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(dest))) return false;
  const Position max_length = max_pos_ - pos();
  if (ABSL_PREDICT_TRUE(src.size() <= max_length)) {
    const bool write_ok = dest.Write(std::forward<Src>(src));
    MakeBuffer(dest);
    return write_ok;
  }
  if (ABSL_PREDICT_FALSE(!dest.Write(std::forward<RemoveSuffix>(remove_suffix)(
          std::forward<Src>(src), src.size() - max_length)))) {
    MakeBuffer(dest);
    return false;
  }
  return FailLimitExceeded(dest);
}

bool LimitingWriterBase::WriteSlow(ByteFill src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(ByteFill): "
         "enough space available, use Write(ByteFill) instead";
  RIEGELI_ASSERT_LE(start_pos(), max_pos_)
      << "Failed invariant of LimitingWriterBase: "
         "position already exceeds its limit";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Writer& dest = *DestWriter();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(dest))) return false;
  const Position max_length = max_pos_ - pos();
  if (ABSL_PREDICT_TRUE(src.size() <= max_length)) {
    const bool write_ok = dest.Write(src);
    MakeBuffer(dest);
    return write_ok;
  }
  if (ABSL_PREDICT_FALSE(!dest.Write(src.Extract(max_length)))) {
    MakeBuffer(dest);
    return false;
  }
  return FailLimitExceeded(dest);
}

bool LimitingWriterBase::SupportsRandomAccess() {
  Writer* const dest = DestWriter();
  return dest != nullptr && dest->SupportsRandomAccess();
}

bool LimitingWriterBase::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT_NE(new_pos, pos())
      << "Failed precondition of Writer::SeekSlow(): "
         "position unchanged, use Seek() instead";
  RIEGELI_ASSERT_LE(start_pos(), max_pos_)
      << "Failed invariant of LimitingWriterBase: "
         "position already exceeds its limit";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Writer& dest = *DestWriter();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(dest))) return false;
  const Position pos_to_seek = UnsignedMin(new_pos, max_pos_);
  const bool seek_ok = dest.Seek(pos_to_seek);
  MakeBuffer(dest);
  return seek_ok && pos_to_seek == new_pos;
}

absl::optional<Position> LimitingWriterBase::SizeImpl() {
  RIEGELI_ASSERT_LE(start_pos(), max_pos_)
      << "Failed invariant of LimitingWriterBase: "
         "position already exceeds its limit";
  if (ABSL_PREDICT_FALSE(!ok())) return absl::nullopt;
  Writer& dest = *DestWriter();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(dest))) return absl::nullopt;
  const absl::optional<Position> size = dest.Size();
  MakeBuffer(dest);
  return size;
}

bool LimitingWriterBase::SupportsTruncate() {
  Writer* const dest = DestWriter();
  return dest != nullptr && dest->SupportsTruncate();
}

bool LimitingWriterBase::TruncateImpl(Position new_size) {
  RIEGELI_ASSERT_LE(start_pos(), max_pos_)
      << "Failed invariant of LimitingWriterBase: "
         "position already exceeds its limit";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Writer& dest = *DestWriter();
  if (ABSL_PREDICT_FALSE(pos() > max_pos_) && new_size <= max_pos_) {
    set_cursor(cursor() - IntCast<size_t>(pos() - max_pos_));
  }
  if (ABSL_PREDICT_FALSE(!SyncBuffer(dest))) return false;
  const bool truncate_ok = dest.Truncate(new_size);
  MakeBuffer(dest);
  return truncate_ok;
}

bool LimitingWriterBase::SupportsReadMode() {
  Writer* const dest = DestWriter();
  return dest != nullptr && dest->SupportsReadMode();
}

Reader* LimitingWriterBase::ReadModeImpl(Position initial_pos) {
  RIEGELI_ASSERT_LE(start_pos(), max_pos_)
      << "Failed invariant of LimitingWriterBase: "
         "position already exceeds its limit";
  if (ABSL_PREDICT_FALSE(!ok())) return nullptr;
  Writer& dest = *DestWriter();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(dest))) return nullptr;
  Reader* const reader = dest.ReadMode(initial_pos);
  MakeBuffer(dest);
  return reader;
}

}  // namespace riegeli
