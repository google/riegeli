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
    Reader* const src = src_reader();
    SyncBuffer(src);
  }
  Reader::Done();
}

bool WrappedReaderBase::PullSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_GT(min_length, available())
      << "Failed precondition of Reader::PullSlow(): "
         "length too small, use Pull() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader* const src = src_reader();
  SyncBuffer(src);
  const bool ok = src->Pull(min_length, recommended_length);
  MakeBuffer(src);
  return ok;
}

bool WrappedReaderBase::ReadSlow(char* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, available())
      << "Failed precondition of Reader::ReadSlow(char*): "
         "length too small, use Read(char*) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader* const src = src_reader();
  SyncBuffer(src);
  const bool ok = src->Read(dest, length);
  MakeBuffer(src);
  return ok;
}

bool WrappedReaderBase::ReadSlow(Chain* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::ReadSlow(Chain*): "
         "length too small, use Read(Chain*) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Reader::ReadSlow(Chain*): "
         "Chain size overflow";
  return ReadInternal(dest, length);
}

bool WrappedReaderBase::ReadSlow(absl::Cord* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::ReadSlow(Cord*): "
         "length too small, use Read(Cord*) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Reader::ReadSlow(Cord*): "
         "Cord size overflow";
  return ReadInternal(dest, length);
}

template <typename Dest>
inline bool WrappedReaderBase::ReadInternal(Dest* dest, size_t length) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader* const src = src_reader();
  SyncBuffer(src);
  const bool ok = src->ReadAndAppend(dest, length);
  MakeBuffer(src);
  return ok;
}

bool WrappedReaderBase::CopyToSlow(Writer* dest, Position length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::CopyToSlow(Writer*): "
         "length too small, use CopyTo(Writer*) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader* const src = src_reader();
  SyncBuffer(src);
  const bool ok = src->CopyTo(dest, length);
  MakeBuffer(src);
  return ok;
}

bool WrappedReaderBase::CopyToSlow(BackwardWriter* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::CopyToSlow(BackwardWriter*): "
         "length too small, use CopyTo(BackwardWriter*) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader* const src = src_reader();
  SyncBuffer(src);
  const bool ok = src->CopyTo(dest, length);
  MakeBuffer(src);
  return ok;
}

void WrappedReaderBase::ReadHintSlow(size_t length) {
  RIEGELI_ASSERT_GT(length, available())
      << "Failed precondition of Reader::ReadHintSlow(): "
         "length too small, use ReadHint() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return;
  Reader* const src = src_reader();
  SyncBuffer(src);
  src->ReadHint(length);
  MakeBuffer(src);
}

bool WrappedReaderBase::Sync() {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader* const src = src_reader();
  SyncBuffer(src);
  const bool ok = src->Sync();
  MakeBuffer(src);
  return ok;
}

bool WrappedReaderBase::SupportsRandomAccess() const {
  const Reader* const src = src_reader();
  return src != nullptr && src->SupportsRandomAccess();
}

bool WrappedReaderBase::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT(new_pos < start_pos() || new_pos > limit_pos())
      << "Failed precondition of Reader::SeekSlow(): "
         "position in the buffer, use Seek() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader* const src = src_reader();
  SyncBuffer(src);
  const bool ok = src->Seek(new_pos);
  MakeBuffer(src);
  return ok;
}

bool WrappedReaderBase::SupportsSize() const {
  const Reader* const src = src_reader();
  return src != nullptr && src->SupportsSize();
}

absl::optional<Position> WrappedReaderBase::Size() {
  if (ABSL_PREDICT_FALSE(!healthy())) return absl::nullopt;
  Reader* const src = src_reader();
  SyncBuffer(src);
  const absl::optional<Position> size = src->Size();
  MakeBuffer(src);
  return size;
}

}  // namespace riegeli
