// Copyright 2020 Google LLC
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

#include "riegeli/bytes/wrapped_reader.h"

#include <stddef.h>

#include <limits>
#include <memory>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void WrappedReaderBase::Done() {
  if (ABSL_PREDICT_TRUE(ok())) {
    Reader& src = *SrcReader();
    SyncBuffer(src);
  }
  Reader::Done();
}

absl::Status WrappedReaderBase::AnnotateStatusImpl(absl::Status status) {
  // Fully delegate annotations to `*SrcReader()`.
  if (is_open()) {
    Reader& src = *SrcReader();
    SyncBuffer(src);
    status = src.AnnotateStatus(std::move(status));
    MakeBuffer(src);
  }
  return status;
}

bool WrappedReaderBase::PullSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Reader::PullSlow(): "
         "enough data available, use Pull() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader& src = *SrcReader();
  SyncBuffer(src);
  const bool pull_ok = src.Pull(min_length, recommended_length);
  MakeBuffer(src);
  return pull_ok;
}

bool WrappedReaderBase::ReadSlow(size_t length, char* dest) {
  RIEGELI_ASSERT_LT(available(), length)
      << "Failed precondition of Reader::ReadSlow(char*): "
         "enough data available, use Read(char*) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader& src = *SrcReader();
  SyncBuffer(src);
  const bool read_ok = src.Read(length, dest);
  MakeBuffer(src);
  return read_ok;
}

bool WrappedReaderBase::ReadSlow(size_t length, Chain& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::ReadSlow(Chain&): "
         "enough data available, use Read(Chain&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadSlow(Chain&): "
         "Chain size overflow";
  return ReadInternal(length, dest);
}

bool WrappedReaderBase::ReadSlow(size_t length, absl::Cord& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::ReadSlow(Cord&): "
         "enough data available, use Read(Cord&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadSlow(Cord&): "
         "Cord size overflow";
  return ReadInternal(length, dest);
}

template <typename Dest>
inline bool WrappedReaderBase::ReadInternal(size_t length, Dest& dest) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader& src = *SrcReader();
  SyncBuffer(src);
  const bool read_ok = src.ReadAndAppend(length, dest);
  MakeBuffer(src);
  return read_ok;
}

bool WrappedReaderBase::CopySlow(Position length, Writer& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::CopySlow(Writer&): "
         "enough data available, use Copy(Writer&) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader& src = *SrcReader();
  SyncBuffer(src);
  const bool copy_ok = src.Copy(length, dest);
  MakeBuffer(src);
  return copy_ok;
}

bool WrappedReaderBase::CopySlow(size_t length, BackwardWriter& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::CopySlow(BackwardWriter&): "
         "enough data available, use Copy(BackwardWriter&) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader& src = *SrcReader();
  SyncBuffer(src);
  const bool copy_ok = src.Copy(length, dest);
  MakeBuffer(src);
  return copy_ok;
}

void WrappedReaderBase::ReadHintSlow(size_t min_length,
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

bool WrappedReaderBase::ToleratesReadingAhead() {
  Reader* const src = SrcReader();
  return src != nullptr && src->ToleratesReadingAhead();
}

bool WrappedReaderBase::SupportsRandomAccess() {
  Reader* const src = SrcReader();
  return src != nullptr && src->SupportsRandomAccess();
}

bool WrappedReaderBase::SupportsRewind() {
  Reader* const src = SrcReader();
  return src != nullptr && src->SupportsRewind();
}

bool WrappedReaderBase::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of Reader::SeekSlow(): "
         "position in the buffer, use Seek() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader& src = *SrcReader();
  SyncBuffer(src);
  const bool seek_ok = src.Seek(new_pos);
  MakeBuffer(src);
  return seek_ok;
}

bool WrappedReaderBase::SupportsSize() {
  Reader* const src = SrcReader();
  return src != nullptr && src->SupportsSize();
}

absl::optional<Position> WrappedReaderBase::SizeImpl() {
  if (ABSL_PREDICT_FALSE(!ok())) return absl::nullopt;
  Reader& src = *SrcReader();
  SyncBuffer(src);
  const absl::optional<Position> size = src.Size();
  MakeBuffer(src);
  return size;
}

bool WrappedReaderBase::SupportsNewReader() {
  Reader* const src = SrcReader();
  return src != nullptr && src->SupportsNewReader();
}

std::unique_ptr<Reader> WrappedReaderBase::NewReaderImpl(Position initial_pos) {
  if (ABSL_PREDICT_FALSE(!ok())) return nullptr;
  // `NewReaderImpl()` is thread-safe from this point
  // if `SrcReader()->SupportsNewReader()`.
  Reader& src = *SrcReader();
  std::unique_ptr<Reader> reader = src.NewReader(initial_pos);
  if (ABSL_PREDICT_FALSE(reader == nullptr)) {
    FailWithoutAnnotation(src.status());
  }
  return reader;
}

}  // namespace riegeli
