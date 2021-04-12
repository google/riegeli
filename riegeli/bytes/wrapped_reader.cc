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

#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void WrappedReaderBase::Done() {
  if (ABSL_PREDICT_TRUE(healthy())) {
    Reader& src = *src_reader();
    SyncBuffer(src);
  }
  Reader::Done();
}

bool WrappedReaderBase::PullSlow(size_t min_length, size_t recommended_length) {
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

bool WrappedReaderBase::ReadSlow(size_t length, char* dest) {
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
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader& src = *src_reader();
  SyncBuffer(src);
  const bool ok = src.ReadAndAppend(length, dest);
  MakeBuffer(src);
  return ok;
}

bool WrappedReaderBase::CopyToSlow(Position length, Writer& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::CopyToSlow(Writer&): "
         "enough data available, use CopyTo(Writer&) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader& src = *src_reader();
  SyncBuffer(src);
  const bool ok = src.CopyTo(length, dest);
  MakeBuffer(src);
  return ok;
}

bool WrappedReaderBase::CopyToSlow(size_t length, BackwardWriter& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::CopyToSlow(BackwardWriter&): "
         "enough data available, use CopyTo(BackwardWriter&) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader& src = *src_reader();
  SyncBuffer(src);
  const bool ok = src.CopyTo(length, dest);
  MakeBuffer(src);
  return ok;
}

void WrappedReaderBase::ReadHintSlow(size_t length) {
  RIEGELI_ASSERT_LT(available(), length)
      << "Failed precondition of Reader::ReadHintSlow(): "
         "enough data available, use ReadHint() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return;
  Reader& src = *src_reader();
  SyncBuffer(src);
  src.ReadHint(length);
  MakeBuffer(src);
}

bool WrappedReaderBase::Sync() {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader& src = *src_reader();
  SyncBuffer(src);
  const bool ok = src.Sync();
  MakeBuffer(src);
  return ok;
}

bool WrappedReaderBase::SupportsRandomAccess() {
  Reader* const src = src_reader();
  return src != nullptr && src->SupportsRandomAccess();
}

bool WrappedReaderBase::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of Reader::SeekSlow(): "
         "position in the buffer, use Seek() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader& src = *src_reader();
  SyncBuffer(src);
  const bool ok = src.Seek(new_pos);
  MakeBuffer(src);
  return ok;
}

bool WrappedReaderBase::SupportsSize() {
  Reader* const src = src_reader();
  return src != nullptr && src->SupportsSize();
}

absl::optional<Position> WrappedReaderBase::Size() {
  if (ABSL_PREDICT_FALSE(!healthy())) return absl::nullopt;
  Reader& src = *src_reader();
  SyncBuffer(src);
  const absl::optional<Position> size = src.Size();
  MakeBuffer(src);
  return size;
}

}  // namespace riegeli
