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

#include "riegeli/bytes/limiting_reader.h"

#include <stddef.h>

#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void LimitingReaderBase::Done() {
  if (ABSL_PREDICT_TRUE(ok())) {
    Reader& src = *src_reader();
    SyncBuffer(src);
  }
  Reader::Done();
}

inline bool LimitingReaderBase::CheckEnough(Reader& src) {
  if (ABSL_PREDICT_FALSE(exact_)) {
    return Fail(absl::InvalidArgumentError(
        absl::StrCat("Not enough data: expected at least ", max_pos_)));
  }
  return false;
}

void LimitingReaderBase::FailLengthOverflow(Position max_length) {
  Fail(absl::InvalidArgumentError(
      absl::StrCat("Not enough data: expected at least ", pos(), " + ",
                   max_length, " which overflows the Reader position")));
}

void LimitingReaderBase::FailNotEnoughEarly(Position expected) {
  Fail(absl::InvalidArgumentError(
      absl::StrCat("Not enough data: expected at least ", expected,
                   ", will have at most ", max_pos_)));
}

absl::Status LimitingReaderBase::AnnotateStatusImpl(absl::Status status) {
  // Fully delegate annotations to `*src_reader()`.
  if (is_open()) {
    Reader& src = *src_reader();
    SyncBuffer(src);
    status = src.AnnotateStatus(std::move(status));
    MakeBuffer(src);
  }
  return status;
}

bool LimitingReaderBase::PullSlow(size_t min_length,
                                  size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Reader::PullSlow(): "
         "enough data available, use Pull() instead";
  RIEGELI_ASSERT_LE(pos(), max_pos_)
      << "Failed invariant of LimitingReaderBase: position exceeds the limit";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader& src = *src_reader();
  SyncBuffer(src);
  const size_t min_length_to_pull = UnsignedMin(min_length, max_pos_ - pos());
  const bool pull_ok = src.Pull(min_length_to_pull, recommended_length);
  MakeBuffer(src);
  if (ABSL_PREDICT_FALSE(!pull_ok)) return CheckEnough(src);
  return min_length_to_pull == min_length;
}

bool LimitingReaderBase::ReadSlow(size_t length, char* dest) {
  RIEGELI_ASSERT_LT(available(), length)
      << "Failed precondition of Reader::ReadSlow(char*): "
         "enough data available, use Read(char&) instead";
  RIEGELI_ASSERT_LE(pos(), max_pos_)
      << "Failed invariant of LimitingReaderBase: "
         "position already exceeds its limit";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader& src = *src_reader();
  SyncBuffer(src);
  const size_t length_to_read = UnsignedMin(length, max_pos_ - pos());
  const bool read_ok = src.Read(length_to_read, dest);
  MakeBuffer(src);
  if (ABSL_PREDICT_FALSE(!read_ok)) return CheckEnough(src);
  return length_to_read == length;
}

bool LimitingReaderBase::ReadSlow(size_t length, Chain& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::ReadSlow(Chain&): "
         "enough data available, use Read(Chain&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadSlow(Chain&): "
         "Chain size overflow";
  return ReadInternal(length, dest);
}

bool LimitingReaderBase::ReadSlow(size_t length, absl::Cord& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::ReadSlow(Cord&): "
         "enough data available, use Read(Cord&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadSlow(Cord&): "
         "Cord size overflow";
  return ReadInternal(length, dest);
}

template <typename Dest>
inline bool LimitingReaderBase::ReadInternal(size_t length, Dest& dest) {
  RIEGELI_ASSERT_LE(pos(), max_pos_)
      << "Failed invariant of LimitingReaderBase: "
         "position already exceeds its limit";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader& src = *src_reader();
  SyncBuffer(src);
  const size_t length_to_read = UnsignedMin(length, max_pos_ - pos());
  const bool read_ok = src.ReadAndAppend(length_to_read, dest);
  MakeBuffer(src);
  if (ABSL_PREDICT_FALSE(!read_ok)) return CheckEnough(src);
  return length_to_read == length;
}

bool LimitingReaderBase::CopySlow(Position length, Writer& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::CopySlow(Writer&): "
         "enough data available, use Copy(Writer&) instead";
  RIEGELI_ASSERT_LE(pos(), max_pos_)
      << "Failed invariant of LimitingReaderBase: "
         "position already exceeds its limit";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader& src = *src_reader();
  SyncBuffer(src);
  const Position length_to_copy = UnsignedMin(length, max_pos_ - pos());
  const bool copy_ok = src.Copy(length_to_copy, dest);
  MakeBuffer(src);
  if (ABSL_PREDICT_FALSE(!copy_ok)) return CheckEnough(src);
  return length_to_copy == length;
}

bool LimitingReaderBase::CopySlow(size_t length, BackwardWriter& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::CopySlow(BackwardWriter&): "
         "enough data available, use Copy(BackwardWriter&) instead";
  RIEGELI_ASSERT_LE(pos(), max_pos_)
      << "Failed invariant of LimitingReaderBase: "
         "position already exceeds its limit";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader& src = *src_reader();
  SyncBuffer(src);
  if (ABSL_PREDICT_FALSE(length > max_pos_ - pos())) {
    const bool seek_ok = src.Seek(max_pos_);
    MakeBuffer(src);
    if (ABSL_PREDICT_FALSE(!seek_ok)) return CheckEnough(src);
    return false;
  }
  const bool copy_ok = src.Copy(length, dest);
  MakeBuffer(src);
  if (ABSL_PREDICT_FALSE(!copy_ok)) return CheckEnough(src);
  return true;
}

void LimitingReaderBase::ReadHintSlow(size_t min_length,
                                      size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Reader::ReadHintSlow(): "
         "enough data available, use ReadHint() instead";
  RIEGELI_ASSERT_LE(pos(), max_pos_)
      << "Failed invariant of LimitingReaderBase: "
         "position already exceeds its limit";
  if (ABSL_PREDICT_FALSE(!ok())) return;
  Reader& src = *src_reader();
  SyncBuffer(src);
  const Position remaining = max_pos_ - pos();
  src.ReadHint(UnsignedMin(min_length, remaining),
               UnsignedMin(recommended_length, remaining));
  MakeBuffer(src);
}

bool LimitingReaderBase::ToleratesReadingAhead() {
  Reader* const src = src_reader();
  return src != nullptr && src->ToleratesReadingAhead();
}

bool LimitingReaderBase::SupportsRandomAccess() {
  Reader* const src = src_reader();
  return src != nullptr && src->SupportsRandomAccess();
}

bool LimitingReaderBase::SupportsRewind() {
  Reader* const src = src_reader();
  return src != nullptr && src->SupportsRewind();
}

bool LimitingReaderBase::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of Reader::SeekSlow(): "
         "position in the buffer, use Seek() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader& src = *src_reader();
  SyncBuffer(src);
  const Position pos_to_seek = UnsignedMin(new_pos, max_pos_);
  const bool seek_ok = src.Seek(pos_to_seek);
  MakeBuffer(src);
  if (ABSL_PREDICT_FALSE(!seek_ok)) return CheckEnough(src);
  return pos_to_seek == new_pos;
}

bool LimitingReaderBase::SupportsSize() {
  if (exact_) return true;
  Reader* const src = src_reader();
  return src != nullptr && src->SupportsSize();
}

absl::optional<Position> LimitingReaderBase::SizeImpl() {
  if (ABSL_PREDICT_FALSE(!ok())) return absl::nullopt;
  if (exact_) return max_pos_;
  Reader& src = *src_reader();
  SyncBuffer(src);
  const absl::optional<Position> size = src.Size();
  MakeBuffer(src);
  if (ABSL_PREDICT_FALSE(size == absl::nullopt)) return absl::nullopt;
  return UnsignedMin(*size, max_pos_);
}

bool LimitingReaderBase::SupportsNewReader() {
  Reader* const src = src_reader();
  return src != nullptr && src->SupportsNewReader();
}

std::unique_ptr<Reader> LimitingReaderBase::NewReaderImpl(
    Position initial_pos) {
  if (ABSL_PREDICT_FALSE(!ok())) return nullptr;
  // `NewReaderImpl()` is thread-safe from this point
  // if `src_reader()->SupportsNewReader()`.
  Reader& src = *src_reader();
  std::unique_ptr<Reader> reader =
      src.NewReader(UnsignedMin(initial_pos, max_pos_));
  if (ABSL_PREDICT_FALSE(reader == nullptr)) {
    FailWithoutAnnotation(src.status());
    return nullptr;
  }
  return std::make_unique<LimitingReader<std::unique_ptr<Reader>>>(
      std::move(reader),
      LimitingReaderBase::Options().set_max_pos(max_pos_).set_exact(exact_));
}

}  // namespace riegeli
