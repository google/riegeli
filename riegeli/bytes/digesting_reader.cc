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

#include "riegeli/bytes/digesting_reader.h"

#include <stddef.h>

#include <limits>
#include <memory>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

void DigestingReaderBase::Done() {
  if (ABSL_PREDICT_TRUE(healthy())) {
    Reader& src = *src_reader();
    SyncBuffer(src);
  }
  Reader::Done();
}

bool DigestingReaderBase::PullSlow(size_t min_length,
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

bool DigestingReaderBase::ReadSlow(size_t length, char* dest) {
  RIEGELI_ASSERT_LT(available(), length)
      << "Failed precondition of Reader::ReadSlow(char*): "
         "enough data available, use Read(char*) instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader& src = *src_reader();
  SyncBuffer(src);
  const Position pos_before = src.pos();
  const bool ok = src.Read(length, dest);
  if (ABSL_PREDICT_FALSE(!ok)) {
    RIEGELI_ASSERT_GE(src.pos(), pos_before)
        << "Reader::ReadSlow(char*) decreased pos()";
    const Position length_read = src.pos() - pos_before;
    RIEGELI_ASSERT_LE(length_read, length)
        << "Reader::ReadSlow(char*) read more than requested";
    length = IntCast<size_t>(length_read);
  }
  if (length > 0) DigesterWrite(absl::string_view(dest, length));
  MakeBuffer(src);
  return ok;
}

bool DigestingReaderBase::ReadSlow(size_t length, Chain& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::ReadSlow(Chain&): "
         "enough data available, use Read(Chain&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadSlow(Chain&): "
         "Chain size overflow";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader& src = *src_reader();
  SyncBuffer(src);
  Chain data;
  const bool ok = src.Read(length, data);
  DigesterWrite(data);
  dest.Append(std::move(data));
  MakeBuffer(src);
  return ok;
}

bool DigestingReaderBase::ReadSlow(size_t length, absl::Cord& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::ReadSlow(Cord&): "
         "enough data available, use Read(Cord&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadSlow(Cord&): "
         "Cord size overflow";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Reader& src = *src_reader();
  SyncBuffer(src);
  absl::Cord data;
  const bool ok = src.Read(length, data);
  DigesterWrite(data);
  dest.Append(std::move(data));
  MakeBuffer(src);
  return ok;
}

void DigestingReaderBase::ReadHintSlow(size_t length) {
  RIEGELI_ASSERT_LT(available(), length)
      << "Failed precondition of Reader::ReadHintSlow(): "
         "enough data available, use ReadHint() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return;
  Reader& src = *src_reader();
  SyncBuffer(src);
  src.ReadHint(length);
  MakeBuffer(src);
}

bool DigestingReaderBase::SupportsSize() {
  Reader* const src = src_reader();
  return src != nullptr && src->SupportsSize();
}

absl::optional<Position> DigestingReaderBase::SizeImpl() {
  if (ABSL_PREDICT_FALSE(!healthy())) return absl::nullopt;
  Reader& src = *src_reader();
  SyncBuffer(src);
  const absl::optional<Position> size = src.Size();
  MakeBuffer(src);
  return size;
}

bool DigestingReaderBase::SupportsNewReader() {
  Reader* const src = src_reader();
  return src != nullptr && src->SupportsNewReader();
}

std::unique_ptr<Reader> DigestingReaderBase::NewReaderImpl(
    Position initial_pos) {
  if (ABSL_PREDICT_FALSE(!healthy())) return nullptr;
  Reader& src = *src_reader();
  SyncBuffer(src);
  std::unique_ptr<Reader> reader = src.NewReader(initial_pos);
  MakeBuffer(src);
  return reader;
}

inline void DigestingReaderBase::DigesterWrite(const Chain& src) {
  for (const absl::string_view fragment : src.blocks()) {
    DigesterWrite(fragment);
  }
}

inline void DigestingReaderBase::DigesterWrite(const absl::Cord& src) {
  for (const absl::string_view fragment : src.Chunks()) {
    DigesterWrite(fragment);
  }
}

}  // namespace riegeli
