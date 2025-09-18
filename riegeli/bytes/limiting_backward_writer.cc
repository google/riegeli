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

#include "riegeli/bytes/limiting_backward_writer.h"

#include <stddef.h>

#include <limits>
#include <optional>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/byte_fill.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/external_ref.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/backward_writer.h"

namespace riegeli {

void LimitingBackwardWriterBase::Initialize(BackwardWriter* dest,
                                            const Options& options,
                                            bool is_owning) {
  RIEGELI_ASSERT_NE(dest, nullptr)
      << "Failed precondition of LimitingBackwardWriter: "
         "null BackwardWriter pointer";
  if (is_owning && exact()) {
    if (options.max_pos() != std::nullopt) {
      dest->SetWriteSizeHint(SaturatingSub(*options.max_pos(), dest->pos()));
    } else if (options.max_length() != std::nullopt) {
      dest->SetWriteSizeHint(*options.max_length());
    }
  }
  set_buffer(dest->limit(), dest->start_to_limit(), dest->start_to_cursor());
  set_start_pos(dest->start_pos());
  if (ABSL_PREDICT_FALSE(!dest->ok())) FailWithoutAnnotation(dest->status());
  if (options.max_pos() != std::nullopt) {
    set_max_pos(*options.max_pos());
  } else if (options.max_length() != std::nullopt) {
    set_max_length(*options.max_length());
  }
}

void LimitingBackwardWriterBase::set_max_pos(Position max_pos) {
  max_pos_ = max_pos;
  if (ABSL_PREDICT_FALSE(start_pos() > max_pos_)) {
    set_buffer(cursor());
    set_start_pos(max_pos_);
    FailLimitExceeded();
  }
}

void LimitingBackwardWriterBase::set_max_length(Position max_length) {
  if (ABSL_PREDICT_FALSE(max_length >
                         std::numeric_limits<Position>::max() - pos())) {
    if (exact_) FailLengthOverflow(max_length);
    max_pos_ = std::numeric_limits<Position>::max();
    return;
  }
  set_max_pos(pos() + max_length);
}

void LimitingBackwardWriterBase::Done() {
  BackwardWriter& dest = *DestWriter();
  if (ABSL_PREDICT_TRUE(ok())) SyncBuffer(dest);
  if (exact_ && ABSL_PREDICT_FALSE(pos() < max_pos_)) {
    // Do not call `Fail()` because `AnnotateStatusImpl()` synchronizes the
    // buffer again.
    FailWithoutAnnotation(dest.AnnotateStatus(absl::InvalidArgumentError(
        absl::StrCat("Not enough data: expected ", max_pos_))));
  }
  BackwardWriter::Done();
}

bool LimitingBackwardWriterBase::FailLimitExceeded() {
  BackwardWriter& dest = *DestWriter();
  return FailLimitExceeded(dest);
}

bool LimitingBackwardWriterBase::FailLimitExceeded(BackwardWriter& dest) {
  set_start_pos(max_pos_);
  set_buffer();
  // Do not call `Fail()` because `AnnotateStatusImpl()` synchronizes the buffer
  // again.
  return FailWithoutAnnotation(dest.AnnotateStatus(
      absl::ResourceExhaustedError("Position limit exceeded")));
}

inline void LimitingBackwardWriterBase::FailLengthOverflow(
    Position max_length) {
  Fail(absl::InvalidArgumentError(
      absl::StrCat("Not enough data: expected ", pos(), " + ", max_length,
                   " which overflows the BackwardWriter position")));
}

absl::Status LimitingBackwardWriterBase::AnnotateStatusImpl(
    absl::Status status) {
  // Fully delegate annotations to `*DestWriter()`.
  if (is_open()) {
    BackwardWriter& dest = *DestWriter();
    const bool sync_buffer_ok = SyncBuffer(dest);
    status = dest.AnnotateStatus(std::move(status));
    if (ABSL_PREDICT_TRUE(sync_buffer_ok)) MakeBuffer(dest);
  }
  return status;
}

bool LimitingBackwardWriterBase::PushSlow(size_t min_length,
                                          size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of BackwardWriter::PushSlow(): "
         "enough space available, use Push() instead";
  RIEGELI_ASSERT_LE(start_pos(), max_pos_)
      << "Failed invariant of LimitingBackwardWriterBase: "
         "position already exceeds its limit";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  BackwardWriter& dest = *DestWriter();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(dest))) return false;
  const bool push_ok = dest.Push(min_length, recommended_length);
  MakeBuffer(dest);
  return push_ok;
}

bool LimitingBackwardWriterBase::WriteSlow(absl::string_view src) {
  RIEGELI_ASSERT_LT(available(), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(string_view): "
         "enough space available, use Write(string_view) instead";
  return WriteInternal(src, [](absl::string_view src, size_t length) {
    src.remove_prefix(length);
    return src;
  });
}

bool LimitingBackwardWriterBase::WriteSlow(ExternalRef src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(ExternalRef): "
         "enough space available, use Write(ExternalRef) instead";
  return WriteInternal(std::move(src), [](ExternalRef src, size_t length) {
    Chain result(std::move(src));
    result.RemovePrefix(length);
    return result;
  });
}

bool LimitingBackwardWriterBase::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Chain): "
         "enough space available, use Write(Chain) instead";
  return WriteInternal(src, [](const Chain& src, size_t length) {
    Chain result = src;
    result.RemovePrefix(length);
    return result;
  });
}

bool LimitingBackwardWriterBase::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Chain&&): "
         "enough space available, use Write(Chain&&) instead";
  return WriteInternal(std::move(src),
                       [](Chain&& src, size_t length) -> Chain&& {
                         src.RemovePrefix(length);
                         return std::move(src);
                       });
}

bool LimitingBackwardWriterBase::WriteSlow(const absl::Cord& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Cord): "
         "enough space available, use Write(Cord) instead";
  return WriteInternal(src, [](const absl::Cord& src, size_t length) {
    absl::Cord result = src;
    result.RemovePrefix(length);
    return result;
  });
}

bool LimitingBackwardWriterBase::WriteSlow(absl::Cord&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Cord&&): "
         "enough space available, use Write(Cord&&) instead";
  return WriteInternal(std::move(src),
                       [](absl::Cord&& src, size_t length) -> absl::Cord&& {
                         src.RemovePrefix(length);
                         return std::move(src);
                       });
}

template <typename Src, typename RemovePrefix>
inline bool LimitingBackwardWriterBase::WriteInternal(
    Src&& src, RemovePrefix&& remove_prefix) {
  RIEGELI_ASSERT_LE(start_pos(), max_pos_)
      << "Failed invariant of LimitingBackwardWriterBase: "
         "position already exceeds its limit";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  BackwardWriter& dest = *DestWriter();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(dest))) return false;
  const Position max_length = max_pos_ - pos();
  if (ABSL_PREDICT_TRUE(src.size() <= max_pos_ - pos())) {
    const bool write_ok = dest.Write(std::forward<Src>(src));
    MakeBuffer(dest);
    return write_ok;
  }
  if (ABSL_PREDICT_FALSE(!dest.Write(std::forward<RemovePrefix>(remove_prefix)(
          std::forward<Src>(src), src.size() - max_length)))) {
    MakeBuffer(dest);
    return false;
  }
  return FailLimitExceeded(dest);
}

bool LimitingBackwardWriterBase::WriteSlow(ByteFill src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(ByteFill): "
         "enough space available, use Write(ByteFill) instead";
  RIEGELI_ASSERT_LE(start_pos(), max_pos_)
      << "Failed invariant of LimitingBackwardWriterBase: "
         "position already exceeds its limit";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  BackwardWriter& dest = *DestWriter();
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

bool LimitingBackwardWriterBase::SupportsTruncate() {
  BackwardWriter* const dest = DestWriter();
  return dest != nullptr && dest->SupportsTruncate();
}

bool LimitingBackwardWriterBase::TruncateImpl(Position new_size) {
  RIEGELI_ASSERT_LE(start_pos(), max_pos_)
      << "Failed invariant of LimitingBackwardWriterBase: "
         "position already exceeds its limit";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  BackwardWriter& dest = *DestWriter();
  if (ABSL_PREDICT_FALSE(pos() > max_pos_) && new_size <= max_pos_) {
    set_cursor(cursor() + IntCast<size_t>(pos() - max_pos_));
  }
  if (ABSL_PREDICT_FALSE(!SyncBuffer(dest))) return false;
  const bool truncate_ok = dest.Truncate(new_size);
  MakeBuffer(dest);
  return truncate_ok;
}

}  // namespace riegeli
