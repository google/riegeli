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

#include "riegeli/bytes/prefix_limiting_reader.h"

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
#include "riegeli/base/status.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void PrefixLimitingReaderBase::Done() {
  if (ABSL_PREDICT_TRUE(healthy())) {
    Reader& src = *src_reader();
    SyncBuffer(src);
  }
  Reader::Done();
}

absl::Status PrefixLimitingReaderBase::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) {
    Reader& src = *src_reader();
    status = src.AnnotateStatus(std::move(status));
  }
  // The status might have been annotated by `*src->reader()` with the original
  // position. Clarify that the current position is the relative position
  // instead of delegating to `Reader::AnnotateStatusImpl()`.
  return AnnotateOverSrc(std::move(status));
}

absl::Status PrefixLimitingReaderBase::AnnotateOverSrc(absl::Status status) {
  if (is_open()) {
    return Annotate(status,
                    absl::StrCat("with relative position at byte ", pos()));
  }
  return status;
}

bool PrefixLimitingReaderBase::PullSlow(size_t min_length,
                                        size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Reader::PullSlow(): "
         "enough data available, use Pull() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader& src = *src_reader();
  SyncBuffer(src);
  const bool ok = src.Pull(min_length, recommended_length);
  MakeBuffer(src);
  return ok;
}

bool PrefixLimitingReaderBase::ReadSlow(size_t length, char* dest) {
  RIEGELI_ASSERT_LT(available(), length)
      << "Failed precondition of Reader::ReadSlow(char*): "
         "enough data available, use Read(char*) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader& src = *src_reader();
  SyncBuffer(src);
  const bool ok = src.Read(length, dest);
  MakeBuffer(src);
  return ok;
}

bool PrefixLimitingReaderBase::ReadSlow(size_t length, Chain& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::ReadSlow(Chain&): "
         "enough data available, use Read(Chain&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadSlow(Chain&): "
         "Chain size overflow";
  return ReadInternal(length, dest);
}

bool PrefixLimitingReaderBase::ReadSlow(size_t length, absl::Cord& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::ReadSlow(Cord&): "
         "enough data available, use Read(Cord&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadSlow(Cord&): "
         "Cord size overflow";
  return ReadInternal(length, dest);
}

template <typename Dest>
inline bool PrefixLimitingReaderBase::ReadInternal(size_t length, Dest& dest) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader& src = *src_reader();
  SyncBuffer(src);
  const bool ok = src.ReadAndAppend(length, dest);
  MakeBuffer(src);
  return ok;
}

bool PrefixLimitingReaderBase::CopySlow(Position length, Writer& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::CopySlow(Writer&): "
         "enough data available, use Copy(Writer&) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader& src = *src_reader();
  SyncBuffer(src);
  const bool ok = src.Copy(length, dest);
  MakeBuffer(src);
  return ok;
}

bool PrefixLimitingReaderBase::CopySlow(size_t length, BackwardWriter& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::CopySlow(BackwardWriter&): "
         "enough data available, use Copy(BackwardWriter&) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader& src = *src_reader();
  SyncBuffer(src);
  const bool ok = src.Copy(length, dest);
  MakeBuffer(src);
  return ok;
}

void PrefixLimitingReaderBase::ReadHintSlow(size_t length) {
  RIEGELI_ASSERT_LT(available(), length)
      << "Failed precondition of Reader::ReadHintSlow(): "
         "enough data available, use ReadHint() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return;
  Reader& src = *src_reader();
  SyncBuffer(src);
  src.ReadHint(length);
  MakeBuffer(src);
}

bool PrefixLimitingReaderBase::SupportsRandomAccess() {
  Reader* const src = src_reader();
  return src != nullptr && src->SupportsRandomAccess();
}

bool PrefixLimitingReaderBase::SupportsRewind() {
  Reader* const src = src_reader();
  return src != nullptr && src->SupportsRewind();
}

bool PrefixLimitingReaderBase::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of Reader::SeekSlow(): "
         "position in the buffer, use Seek() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader& src = *src_reader();
  SyncBuffer(src);
  bool ok;
  if (ABSL_PREDICT_FALSE(new_pos >
                         std::numeric_limits<Position>::max() - base_pos_)) {
    src.Seek(std::numeric_limits<Position>::max());
    ok = false;
  } else {
    ok = src.Seek(base_pos_ + new_pos);
  }
  MakeBuffer(src);
  return ok;
}

bool PrefixLimitingReaderBase::SupportsSize() {
  Reader* const src = src_reader();
  return src != nullptr && src->SupportsSize();
}

absl::optional<Position> PrefixLimitingReaderBase::SizeImpl() {
  if (ABSL_PREDICT_FALSE(!healthy())) return absl::nullopt;
  Reader& src = *src_reader();
  SyncBuffer(src);
  const absl::optional<Position> size = src.Size();
  MakeBuffer(src);
  if (ABSL_PREDICT_FALSE(size == absl::nullopt)) return absl::nullopt;
  return SaturatingSub(*size, base_pos_);
}

bool PrefixLimitingReaderBase::SupportsNewReader() {
  Reader* const src = src_reader();
  return src != nullptr && src->SupportsNewReader();
}

std::unique_ptr<Reader> PrefixLimitingReaderBase::NewReaderImpl(
    Position initial_pos) {
  if (ABSL_PREDICT_FALSE(!healthy())) return nullptr;
  Reader& src = *src_reader();
  SyncBuffer(src);
  std::unique_ptr<Reader> reader =
      src.NewReader(SaturatingAdd(base_pos_, initial_pos));
  MakeBuffer(src);
  if (ABSL_PREDICT_FALSE(reader == nullptr)) return nullptr;
  return std::make_unique<PrefixLimitingReader<std::unique_ptr<Reader>>>(
      std::move(reader),
      PrefixLimitingReaderBase::Options().set_base_pos(base_pos_));
}

}  // namespace riegeli
