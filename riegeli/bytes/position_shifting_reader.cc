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

#include "riegeli/bytes/position_shifting_reader.h"

#include <stddef.h>

#include <limits>
#include <memory>
#include <optional>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void PositionShiftingReaderBase::Done() {
  if (ABSL_PREDICT_TRUE(ok())) {
    Reader& src = *SrcReader();
    SyncBuffer(src);
  }
  Reader::Done();
}

bool PositionShiftingReaderBase::FailUnderflow(Position new_pos,
                                               Object& object) {
  return object.Fail(absl::InvalidArgumentError(
      absl::StrCat("PositionShiftingReader does not support "
                   "seeking before the base position: ",
                   new_pos, " < ", base_pos_)));
}

absl::Status PositionShiftingReaderBase::AnnotateStatusImpl(
    absl::Status status) {
  if (is_open()) {
    Reader& src = *SrcReader();
    SyncBuffer(src);
    status = src.AnnotateStatus(std::move(status));
    MakeBuffer(src);
  }
  // The status might have been annotated by `src` with the original position.
  // Clarify that the current position is the relative position instead of
  // delegating to `Reader::AnnotateStatusImpl()`.
  return AnnotateOverSrc(std::move(status));
}

absl::Status PositionShiftingReaderBase::AnnotateOverSrc(absl::Status status) {
  if (is_open() && base_pos_ > 0) {
    return Annotate(status,
                    absl::StrCat("with relative position at byte ", pos()));
  }
  return status;
}

bool PositionShiftingReaderBase::PullSlow(size_t min_length,
                                          size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Reader::PullSlow(): "
         "enough data available, use Pull() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader& src = *SrcReader();
  SyncBuffer(src);
  const bool pull_ok = src.Pull(min_length, recommended_length);
  return MakeBuffer(src, min_length) && pull_ok;
}

bool PositionShiftingReaderBase::ReadSlow(size_t length, char* dest) {
  RIEGELI_ASSERT_LT(available(), length)
      << "Failed precondition of Reader::ReadSlow(char*): "
         "enough data available, use Read(char*) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader& src = *SrcReader();
  SyncBuffer(src);
  const bool read_ok = src.Read(length, dest);
  return MakeBuffer(src) && read_ok;
}

bool PositionShiftingReaderBase::ReadSlow(size_t length, Chain& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::ReadSlow(Chain&): "
         "enough data available, use Read(Chain&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadSlow(Chain&): "
         "Chain size overflow";
  return ReadInternal(length, dest);
}

bool PositionShiftingReaderBase::ReadSlow(size_t length, absl::Cord& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::ReadSlow(Cord&): "
         "enough data available, use Read(Cord&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadSlow(Cord&): "
         "Cord size overflow";
  return ReadInternal(length, dest);
}

template <typename Dest>
inline bool PositionShiftingReaderBase::ReadInternal(size_t length,
                                                     Dest& dest) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader& src = *SrcReader();
  SyncBuffer(src);
  const bool read_ok = src.ReadAndAppend(length, dest);
  return MakeBuffer(src) && read_ok;
}

bool PositionShiftingReaderBase::CopySlow(Position length, Writer& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::CopySlow(Writer&): "
         "enough data available, use Copy(Writer&) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader& src = *SrcReader();
  SyncBuffer(src);
  const bool copy_ok = src.Copy(length, dest);
  return MakeBuffer(src) && copy_ok;
}

bool PositionShiftingReaderBase::CopySlow(size_t length, BackwardWriter& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::CopySlow(BackwardWriter&): "
         "enough data available, use Copy(BackwardWriter&) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader& src = *SrcReader();
  SyncBuffer(src);
  const bool copy_ok = src.Copy(length, dest);
  return MakeBuffer(src) && copy_ok;
}

bool PositionShiftingReaderBase::ReadSomeSlow(size_t max_length, char* dest) {
  RIEGELI_ASSERT_GT(max_length, 0u)
      << "Failed precondition of Reader::ReadSomeSlow(char*): "
         "nothing to read, use ReadSome(char*) instead";
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of Reader::ReadSomeSlow(char*): "
         "some data available, use ReadSome(char*) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader& src = *SrcReader();
  SyncBuffer(src);
  const bool read_ok = src.ReadSome(max_length, dest);
  return MakeBuffer(src) && read_ok;
}

bool PositionShiftingReaderBase::CopySomeSlow(size_t max_length, Writer& dest) {
  RIEGELI_ASSERT_GT(max_length, 0u)
      << "Failed precondition of Reader::CopySomeSlow(Writer&): "
         "nothing to read, use CopySome(Writer&) instead";
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of Reader::CopySomeSlow(Writer&): "
         "some data available, use CopySome(Writer&) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader& src = *SrcReader();
  SyncBuffer(src);
  const bool copy_ok = src.CopySome(max_length, dest);
  return MakeBuffer(src) && copy_ok;
}

void PositionShiftingReaderBase::ReadHintSlow(size_t min_length,
                                              size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Reader::ReadHintSlow(): "
         "enough data available, use ReadHint() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return;
  Reader& src = *SrcReader();
  SyncBuffer(src);
  src.ReadHint(min_length, recommended_length);
  MakeBuffer(src);
}

bool PositionShiftingReaderBase::ToleratesReadingAhead() {
  Reader* const src = SrcReader();
  return src != nullptr && src->ToleratesReadingAhead();
}

bool PositionShiftingReaderBase::SupportsRandomAccess() {
  Reader* const src = SrcReader();
  return src != nullptr && src->SupportsRandomAccess();
}

bool PositionShiftingReaderBase::SupportsRewind() {
  Reader* const src = SrcReader();
  return src != nullptr && src->SupportsRewind();
}

bool PositionShiftingReaderBase::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of Reader::SeekSlow(): "
         "position in the buffer, use Seek() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (ABSL_PREDICT_FALSE(new_pos < base_pos_)) {
    return FailUnderflow(new_pos, *this);
  }
  Reader& src = *SrcReader();
  SyncBuffer(src);
  const bool seek_ok = src.Seek(new_pos - base_pos_);
  return MakeBuffer(src) && seek_ok;
}

bool PositionShiftingReaderBase::SupportsSize() {
  Reader* const src = SrcReader();
  return src != nullptr && src->SupportsSize();
}

std::optional<Position> PositionShiftingReaderBase::SizeImpl() {
  if (ABSL_PREDICT_FALSE(!ok())) return std::nullopt;
  Reader& src = *SrcReader();
  SyncBuffer(src);
  const std::optional<Position> size = src.Size();
  if (ABSL_PREDICT_FALSE(!MakeBuffer(src) || size == std::nullopt)) {
    return std::nullopt;
  }
  if (ABSL_PREDICT_FALSE(*size >
                         std::numeric_limits<Position>::max() - base_pos_)) {
    FailOverflow();
    return std::nullopt;
  }
  return *size + base_pos_;
}

bool PositionShiftingReaderBase::SupportsNewReader() {
  Reader* const src = SrcReader();
  return src != nullptr && src->SupportsNewReader();
}

std::unique_ptr<Reader> PositionShiftingReaderBase::NewReaderImpl(
    Position initial_pos) {
  if (ABSL_PREDICT_FALSE(!ok())) return nullptr;
  // `NewReaderImpl()` is thread-safe from this point
  // if `SrcReader()->SupportsNewReader()`.
  Reader& src = *SrcReader();
  std::unique_ptr<Reader> base_reader =
      src.NewReader(SaturatingSub(initial_pos, base_pos_));
  if (ABSL_PREDICT_FALSE(base_reader == nullptr)) {
    FailWithoutAnnotation(AnnotateOverSrc(src.status()));
    return nullptr;
  }
  std::unique_ptr<Reader> reader =
      std::make_unique<PositionShiftingReader<std::unique_ptr<Reader>>>(
          std::move(base_reader),
          PositionShiftingReaderBase::Options().set_base_pos(base_pos_));
  if (ABSL_PREDICT_FALSE(initial_pos < base_pos_)) {
    FailUnderflow(initial_pos, *reader);
  }
  return reader;
}

}  // namespace riegeli
