// Copyright 2019 Google LLC
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

#include "riegeli/bytes/tee_reader.h"

#include <stddef.h>

#include <limits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void TeeReaderBase::Done() {
  if (ABSL_PREDICT_TRUE(healthy())) {
    Reader* const src = src_reader();
    Writer* const side_dest = side_dest_writer();
    SyncBuffer(src, side_dest);
  }
  Reader::Done();
}

bool TeeReaderBase::PullSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_GT(min_length, available())
      << "Failed precondition of Reader::PullSlow(): "
         "length too small, use Pull() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader* const src = src_reader();
  Writer* const side_dest = side_dest_writer();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(src, side_dest))) return false;
  const bool ok = src->Pull(min_length, recommended_length);
  MakeBuffer(src);
  return ok;
}

bool TeeReaderBase::ReadSlow(char* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, available())
      << "Failed precondition of Reader::ReadSlow(char*): "
         "length too small, use Read(char*) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader* const src = src_reader();
  Writer* const side_dest = side_dest_writer();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(src, side_dest))) return false;
  const Position pos_before = src->pos();
  bool ok = src->Read(dest, length);
  if (ABSL_PREDICT_FALSE(!ok)) {
    RIEGELI_ASSERT_GE(src->pos(), pos_before)
        << "Reader::ReadSlow(char*) decreased pos()";
    const Position length_read = src->pos() - pos_before;
    RIEGELI_ASSERT_LE(length_read, length)
        << "Reader::ReadSlow(char*) read more than requested";
    length = IntCast<size_t>(length_read);
  }
  if (ABSL_PREDICT_FALSE(!side_dest->Write(absl::string_view(dest, length)))) {
    Fail(*side_dest);
    ok = false;
  }
  MakeBuffer(src);
  return ok;
}

bool TeeReaderBase::ReadSlow(Chain* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::ReadSlow(Chain*): "
         "length too small, use Read(Chain*) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Reader::ReadSlow(Chain*): "
         "Chain size overflow";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader* const src = src_reader();
  Writer* const side_dest = side_dest_writer();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(src, side_dest))) return false;
  Chain data;
  bool ok = src->Read(&data, length);
  if (ABSL_PREDICT_FALSE(!side_dest->Write(data))) {
    Fail(*side_dest);
    ok = false;
  }
  dest->Append(std::move(data));
  MakeBuffer(src);
  return ok;
}

bool TeeReaderBase::ReadSlow(absl::Cord* dest, size_t length) {
  RIEGELI_ASSERT_GT(length, UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Reader::ReadSlow(Cord*): "
         "length too small, use Read(Cord*) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Reader::ReadSlow(Cord*): "
         "Cord size overflow";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader* const src = src_reader();
  Writer* const side_dest = side_dest_writer();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(src, side_dest))) return false;
  absl::Cord data;
  bool ok = src->Read(&data, length);
  if (ABSL_PREDICT_FALSE(!side_dest->Write(data))) {
    Fail(*side_dest);
    ok = false;
  }
  dest->Append(std::move(data));
  MakeBuffer(src);
  return ok;
}

void TeeReaderBase::ReadHintSlow(size_t length) {
  RIEGELI_ASSERT_GT(length, available())
      << "Failed precondition of Reader::ReadHintSlow(): "
         "length too small, use ReadHint() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return;
  Reader* const src = src_reader();
  Writer* const side_dest = side_dest_writer();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(src, side_dest))) return;
  src->ReadHint(length);
  MakeBuffer(src);
}

bool TeeReaderBase::Sync() {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader* const src = src_reader();
  Writer* const side_dest = side_dest_writer();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(src, side_dest))) return false;
  MakeBuffer(src);
  return true;
}

}  // namespace riegeli
