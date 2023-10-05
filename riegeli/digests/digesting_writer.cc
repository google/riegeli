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

#include "riegeli/digests/digesting_writer.h"

#include <stddef.h>

#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/digests/digester.h"

namespace riegeli {

void DigestingWriterBase::Done() {
  if (ABSL_PREDICT_TRUE(ok())) {
    Writer& dest = *DestWriter();
    SyncBuffer(dest);
  }
  Writer::Done();
}

absl::Status DigestingWriterBase::AnnotateStatusImpl(absl::Status status) {
  // Fully delegate annotations to `*DestWriter()`.
  if (is_open()) {
    Writer& dest = *DestWriter();
    SyncBuffer(dest);
    status = dest.AnnotateStatus(std::move(status));
    MakeBuffer(dest);
  }
  return status;
}

bool DigestingWriterBase::PushSlow(size_t min_length,
                                   size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of Writer::PushSlow(): "
         "enough space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Writer& dest = *DestWriter();
  SyncBuffer(dest);
  const bool push_ok = dest.Push(min_length, recommended_length);
  MakeBuffer(dest);
  return push_ok;
}

bool DigestingWriterBase::WriteSlow(absl::string_view src) {
  RIEGELI_ASSERT_LT(available(), src.size())
      << "Failed precondition of Writer::WriteSlow(string_view): "
         "enough space available, use Write(string_view) instead";
  return WriteInternal(src);
}

bool DigestingWriterBase::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain): "
         "enough space available, use Write(Chain) instead";
  return WriteInternal(src);
}

bool DigestingWriterBase::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain&&): "
         "enough space available, use Write(Chain&&) instead";
  return WriteInternal(std::move(src));
}

bool DigestingWriterBase::WriteSlow(const absl::Cord& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Cord): "
         "enough space available, use Write(Cord) instead";
  return WriteInternal(src);
}

bool DigestingWriterBase::WriteSlow(absl::Cord&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Cord&&): "
         "enough space available, use Write(Cord&&) instead";
  return WriteInternal(std::move(src));
}

template <typename Src>
inline bool DigestingWriterBase::WriteInternal(Src&& src) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Writer& dest = *DestWriter();
  SyncBuffer(dest);
  DigesterBase* const digester = GetDigester();
  digester->Write(src);
  const bool write_ok = dest.Write(std::forward<Src>(src));
  MakeBuffer(dest);
  return write_ok;
}

bool DigestingWriterBase::WriteZerosSlow(Position length) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Writer::WriteZerosSlow(): "
         "enough space available, use WriteZeros() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Writer& dest = *DestWriter();
  SyncBuffer(dest);
  DigesterBase* const digester = GetDigester();
  digester->WriteZeros(length);
  const bool write_ok = dest.WriteZeros(length);
  MakeBuffer(dest);
  return write_ok;
}

bool DigestingWriterBase::PrefersCopying() const {
  const Writer* const dest = DestWriter();
  return dest != nullptr && dest->PrefersCopying();
}

bool DigestingWriterBase::SupportsReadMode() {
  Writer* const dest = DestWriter();
  return dest != nullptr && dest->SupportsReadMode();
}

Reader* DigestingWriterBase::ReadModeImpl(Position initial_pos) {
  if (ABSL_PREDICT_FALSE(!ok())) return nullptr;
  Writer& dest = *DestWriter();
  SyncBuffer(dest);
  Reader* const reader = dest.ReadMode(initial_pos);
  MakeBuffer(dest);
  return reader;
}

}  // namespace riegeli
