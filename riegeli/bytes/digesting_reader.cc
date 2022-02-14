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
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

void DigestingReaderBase::Done() {
  if (ABSL_PREDICT_TRUE(ok())) {
    Reader& src = *src_reader();
    SyncBuffer(src);
  }
  Reader::Done();
}

absl::Status DigestingReaderBase::AnnotateStatusImpl(absl::Status status) {
  // Fully delegate annotations to `*src_reader()`.
  if (is_open()) {
    Reader& src = *src_reader();
    SyncBuffer(src);
    status = src.AnnotateStatus(std::move(status));
    MakeBuffer(src);
  }
  return status;
}

bool DigestingReaderBase::PullSlow(size_t min_length,
                                   size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Reader::PullSlow(): "
         "enough data available, use Pull() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader& src = *src_reader();
  SyncBuffer(src);
  const bool pull_ok = src.Pull(min_length, recommended_length);
  MakeBuffer(src);
  return pull_ok;
}

bool DigestingReaderBase::ReadSlow(size_t length, char* dest) {
  RIEGELI_ASSERT_LT(available(), length)
      << "Failed precondition of Reader::ReadSlow(char*): "
         "enough data available, use Read(char*) instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader& src = *src_reader();
  SyncBuffer(src);
  const Position pos_before = src.pos();
  const bool read_ok = src.Read(length, dest);
  if (ABSL_PREDICT_FALSE(!read_ok)) {
    RIEGELI_ASSERT_GE(src.pos(), pos_before)
        << "Reader::ReadSlow(char*) decreased pos()";
    const Position length_read = src.pos() - pos_before;
    RIEGELI_ASSERT_LE(length_read, length)
        << "Reader::ReadSlow(char*) read more than requested";
    length = IntCast<size_t>(length_read);
  }
  if (length > 0) DigesterWrite(absl::string_view(dest, length));
  MakeBuffer(src);
  return read_ok;
}

bool DigestingReaderBase::ReadSlow(size_t length, Chain& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::ReadSlow(Chain&): "
         "enough data available, use Read(Chain&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadSlow(Chain&): "
         "Chain size overflow";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader& src = *src_reader();
  SyncBuffer(src);
  Chain data;
  const bool read_ok = src.Read(length, data);
  DigesterWrite(data);
  dest.Append(std::move(data));
  MakeBuffer(src);
  return read_ok;
}

bool DigestingReaderBase::ReadSlow(size_t length, absl::Cord& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Reader::ReadSlow(Cord&): "
         "enough data available, use Read(Cord&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadSlow(Cord&): "
         "Cord size overflow";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader& src = *src_reader();
  SyncBuffer(src);
  absl::Cord data;
  const bool read_ok = src.Read(length, data);
  DigesterWrite(data);
  dest.Append(std::move(data));
  MakeBuffer(src);
  return read_ok;
}

void DigestingReaderBase::ReadHintSlow(size_t min_length,
                                       size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Reader::ReadHintSlow(): "
         "enough data available, use ReadHint() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return;
  Reader& src = *src_reader();
  SyncBuffer(src);
  src.ReadHint(min_length, recommended_length);
  MakeBuffer(src);
}

bool DigestingReaderBase::SupportsSize() {
  Reader* const src = src_reader();
  return src != nullptr && src->SupportsSize();
}

absl::optional<Position> DigestingReaderBase::SizeImpl() {
  if (ABSL_PREDICT_FALSE(!ok())) return absl::nullopt;
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
  if (ABSL_PREDICT_FALSE(!ok())) return nullptr;
  // `NewReaderImpl()` is thread-safe from this point
  // if `src_reader()->SupportsNewReader()`.
  Reader& src = *src_reader();
  std::unique_ptr<Reader> reader = src.NewReader(initial_pos);
  if (ABSL_PREDICT_FALSE(reader == nullptr)) {
    FailWithoutAnnotation(src.status());
  }
  return reader;
}

inline void DigestingReaderBase::DigesterWrite(const Chain& src) {
  for (const absl::string_view fragment : src.blocks()) {
    DigesterWrite(fragment);
  }
}

inline void DigestingReaderBase::DigesterWrite(const absl::Cord& src) {
  if (const absl::optional<absl::string_view> flat = src.TryFlat()) {
    DigesterWrite(*flat);
    return;
  }
  for (const absl::string_view fragment : src.Chunks()) {
    DigesterWrite(fragment);
  }
}

}  // namespace riegeli
