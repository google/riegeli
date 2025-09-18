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
#include <optional>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void LimitingReaderBase::Initialize(Reader* src, const Options& options) {
  RIEGELI_ASSERT_NE(src, nullptr)
      << "Failed precondition of LimitingReader: null Reader pointer";
  set_buffer(src->start(), src->start_to_limit(), src->start_to_cursor());
  set_limit_pos(src->limit_pos());
  if (ABSL_PREDICT_FALSE(!src->ok())) FailWithoutAnnotation(src->status());
  if (options.max_pos() != std::nullopt) {
    set_max_pos(*options.max_pos());
  } else if (options.max_length() != std::nullopt) {
    set_max_length(*options.max_length());
  }
}

void LimitingReaderBase::set_max_pos(Position max_pos) {
  max_pos_ = max_pos;
  if (ABSL_PREDICT_FALSE(limit_pos() > max_pos_)) {
    if (pos() > max_pos_) {
      set_buffer(cursor());
    } else {
      set_buffer(start(),
                 start_to_limit() - IntCast<size_t>(limit_pos() - max_pos_),
                 start_to_cursor());
    }
    set_limit_pos(max_pos_);
  }
}

void LimitingReaderBase::set_max_length(Position max_length) {
  if (ABSL_PREDICT_FALSE(max_length >
                         std::numeric_limits<Position>::max() - pos())) {
    if (exact_) FailLengthOverflow(max_length);
    max_pos_ = std::numeric_limits<Position>::max();
    return;
  }
  max_pos_ = pos() + max_length;
  if (ABSL_PREDICT_FALSE(limit_pos() > max_pos_)) {
    set_buffer(start(),
               start_to_limit() - IntCast<size_t>(limit_pos() - max_pos_),
               start_to_cursor());
    set_limit_pos(max_pos_);
  }
}

void LimitingReaderBase::Done() {
  if (ABSL_PREDICT_TRUE(ok())) {
    Reader& src = *SrcReader();
    SyncBuffer(src);
    if (fail_if_longer_ && pos() == max_pos_ &&
        ABSL_PREDICT_FALSE(src.Pull())) {
      // Do not call `Fail()` because `AnnotateStatusImpl()` synchronizes the
      // buffer again.
      FailWithoutAnnotation(src.AnnotateStatus(
          absl::ResourceExhaustedError("Position limit exceeded")));
    }
  }
  Reader::Done();
}

inline bool LimitingReaderBase::CheckEnough() {
  if (ABSL_PREDICT_FALSE(exact_)) return FailNotEnough();
  return false;
}

inline bool LimitingReaderBase::FailNotEnough() {
  return Fail(absl::InvalidArgumentError(
      absl::StrCat("Not enough data: expected at least ", max_pos_)));
}

inline void LimitingReaderBase::FailNotEnoughEarly(Position expected) {
  Fail(absl::InvalidArgumentError(
      absl::StrCat("Not enough data: expected at least ", expected,
                   ", will have at most ", max_pos_)));
}

inline void LimitingReaderBase::FailLengthOverflow(Position max_length) {
  Fail(absl::InvalidArgumentError(
      absl::StrCat("Not enough data: expected at least ", pos(), " + ",
                   max_length, " which overflows the Reader position")));
}

inline void LimitingReaderBase::FailPositionLimitExceeded() {
  Fail(absl::ResourceExhaustedError("Position limit exceeded"));
}

absl::Status LimitingReaderBase::AnnotateStatusImpl(absl::Status status) {
  // Fully delegate annotations to `*SrcReader()`.
  if (is_open()) {
    Reader& src = *SrcReader();
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
  Reader& src = *SrcReader();
  SyncBuffer(src);
  const size_t min_length_to_pull = UnsignedMin(min_length, max_pos_ - pos());
  const bool pull_ok = src.Pull(min_length_to_pull, recommended_length);
  MakeBuffer(src);
  if (ABSL_PREDICT_FALSE(!pull_ok)) return CheckEnough();
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
  Reader& src = *SrcReader();
  SyncBuffer(src);
  const size_t length_to_read = UnsignedMin(length, max_pos_ - pos());
  const bool read_ok = src.Read(length_to_read, dest);
  MakeBuffer(src);
  if (ABSL_PREDICT_FALSE(!read_ok)) return CheckEnough();
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
  Reader& src = *SrcReader();
  SyncBuffer(src);
  const size_t length_to_read = UnsignedMin(length, max_pos_ - pos());
  const bool read_ok = src.ReadAndAppend(length_to_read, dest);
  MakeBuffer(src);
  if (ABSL_PREDICT_FALSE(!read_ok)) return CheckEnough();
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
  Reader& src = *SrcReader();
  SyncBuffer(src);
  const Position length_to_copy = UnsignedMin(length, max_pos_ - pos());
  const bool copy_ok = src.Copy(length_to_copy, dest);
  MakeBuffer(src);
  if (ABSL_PREDICT_FALSE(!copy_ok)) {
    if (dest.ok()) return CheckEnough();
    return false;
  }
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
  Reader& src = *SrcReader();
  SyncBuffer(src);
  if (ABSL_PREDICT_FALSE(length > max_pos_ - pos())) {
    const bool seek_ok = src.Seek(max_pos_);
    MakeBuffer(src);
    if (ABSL_PREDICT_FALSE(!seek_ok)) return CheckEnough();
    return false;
  }
  const bool copy_ok = src.Copy(length, dest);
  MakeBuffer(src);
  if (ABSL_PREDICT_FALSE(!copy_ok)) {
    if (dest.ok()) return CheckEnough();
    return false;
  }
  return true;
}

bool LimitingReaderBase::ReadOrPullSomeSlow(
    size_t max_length, absl::FunctionRef<char*(size_t&)> get_dest) {
  RIEGELI_ASSERT_GT(max_length, 0u)
      << "Failed precondition of Reader::ReadOrPullSomeSlow(): "
         "nothing to read, use ReadOrPullSome() instead";
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of Reader::ReadOrPullSomeSlow(): "
         "some data available, use ReadOrPullSome() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader& src = *SrcReader();
  SyncBuffer(src);
  const Position max_length_to_copy = UnsignedMin(max_length, max_pos_ - pos());
  bool write_ok = true;
  const bool read_ok = src.ReadOrPullSome(
      max_length_to_copy, [get_dest, &write_ok](size_t& length) {
        char* const dest = get_dest(length);
        if (ABSL_PREDICT_FALSE(length == 0)) write_ok = false;
        return dest;
      });
  MakeBuffer(src);
  if (ABSL_PREDICT_FALSE(!read_ok)) {
    if (write_ok) return CheckEnough();
    return false;
  }
  return max_length_to_copy > 0;
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
  Reader& src = *SrcReader();
  SyncBuffer(src);
  const Position remaining = max_pos_ - pos();
  src.ReadHint(UnsignedMin(min_length, remaining),
               UnsignedMin(recommended_length, remaining));
  MakeBuffer(src);
}

bool LimitingReaderBase::ToleratesReadingAhead() {
  Reader* const src = SrcReader();
  return src != nullptr && src->ToleratesReadingAhead();
}

bool LimitingReaderBase::SupportsRandomAccess() {
  Reader* const src = SrcReader();
  return src != nullptr && src->SupportsRandomAccess();
}

bool LimitingReaderBase::SupportsRewind() {
  Reader* const src = SrcReader();
  return src != nullptr && src->SupportsRewind();
}

bool LimitingReaderBase::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of Reader::SeekSlow(): "
         "position in the buffer, use Seek() instead";
  RIEGELI_ASSERT_LE(pos(), max_pos_)
      << "Failed invariant of LimitingReaderBase: "
         "position already exceeds its limit";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader& src = *SrcReader();
  SyncBuffer(src);
  const Position pos_to_seek = UnsignedMin(new_pos, max_pos_);
  const bool seek_ok = src.Seek(pos_to_seek);
  MakeBuffer(src);
  if (ABSL_PREDICT_FALSE(!seek_ok)) return CheckEnough();
  return pos_to_seek == new_pos;
}

bool LimitingReaderBase::SupportsSize() {
  if (exact_) return true;
  Reader* const src = SrcReader();
  return src != nullptr && src->SupportsSize();
}

std::optional<Position> LimitingReaderBase::SizeImpl() {
  RIEGELI_ASSERT_LE(pos(), max_pos_)
      << "Failed invariant of LimitingReaderBase: "
         "position already exceeds its limit";
  if (ABSL_PREDICT_FALSE(!ok())) return std::nullopt;
  if (exact_) return max_pos_;
  Reader& src = *SrcReader();
  SyncBuffer(src);
  const std::optional<Position> size = src.Size();
  MakeBuffer(src);
  if (ABSL_PREDICT_FALSE(size == std::nullopt)) return std::nullopt;
  return UnsignedMin(*size, max_pos_);
}

bool LimitingReaderBase::SupportsNewReader() {
  Reader* const src = SrcReader();
  return src != nullptr && src->SupportsNewReader();
}

std::unique_ptr<Reader> LimitingReaderBase::NewReaderImpl(
    Position initial_pos) {
  RIEGELI_ASSERT_LE(pos(), max_pos_)
      << "Failed invariant of LimitingReaderBase: "
         "position already exceeds its limit";
  if (ABSL_PREDICT_FALSE(!ok())) return nullptr;
  // `NewReaderImpl()` is thread-safe from this point
  // if `SrcReader()->SupportsNewReader()`.
  Reader& src = *SrcReader();
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

ScopedLimiter::ScopedLimiter(
    LimitingReaderBase* reader ABSL_ATTRIBUTE_LIFETIME_BOUND, Options options)
    : reader_(RIEGELI_EVAL_ASSERT_NOTNULL(reader)),
      old_max_pos_(reader_->max_pos()),
      old_exact_(reader_->exact()),
      fail_if_longer_(options.fail_if_longer()) {
  if (options.max_pos() != std::nullopt) {
    if (ABSL_PREDICT_FALSE(*options.max_pos() > reader_->max_pos())) {
      if (options.exact()) reader_->FailNotEnoughEarly(*options.max_pos());
    } else {
      reader_->set_max_pos(*options.max_pos());
    }
  } else if (options.max_length() != std::nullopt) {
    if (ABSL_PREDICT_FALSE(*options.max_length() >
                           std::numeric_limits<Position>::max() -
                               reader_->pos())) {
      if (options.exact()) reader_->FailLengthOverflow(*options.max_length());
    } else {
      const Position max_pos = reader_->pos() + *options.max_length();
      if (ABSL_PREDICT_FALSE(max_pos > reader_->max_pos())) {
        if (options.exact()) reader_->FailNotEnoughEarly(max_pos);
      } else {
        reader_->set_max_pos(max_pos);
      }
    }
  } else if (ABSL_PREDICT_FALSE(reader_->max_pos() <
                                    std::numeric_limits<Position>::max() &&
                                options.exact())) {
    reader_->FailNotEnoughEarly(std::numeric_limits<Position>::max());
  }
  reader_->exact_ |= options.exact();
}

ScopedLimiter::~ScopedLimiter() {
  RIEGELI_ASSERT_GE(old_max_pos_, reader_->max_pos())
      << "Failed precondtion of ~ScopedLimiter: "
         "The underlying LimitingReader increased its limit "
         "while the ScopedLimiter was active";
  const Position inner_max_pos = reader_->max_pos();
  reader_->set_max_pos(old_max_pos_);
  reader_->set_exact(old_exact_);
  if (fail_if_longer_ && reader_->pos() == inner_max_pos &&
      ABSL_PREDICT_FALSE(!reader_->Pull())) {
    reader_->FailPositionLimitExceeded();
  }
}

}  // namespace riegeli
