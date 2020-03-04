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

#include "riegeli/bytes/tee_writer.h"

#include <stddef.h>

#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void TeeWriterBase::Done() {
  if (ABSL_PREDICT_TRUE(healthy())) {
    Writer* const dest = dest_writer();
    Writer* const side_dest = side_dest_writer();
    SyncBuffer(dest, side_dest);
  }
  Writer::Done();
}

bool TeeWriterBase::PushSlow(size_t min_length, size_t recommended_length) {
  RIEGELI_ASSERT_GT(min_length, available())
      << "Failed precondition of Writer::PushSlow(): "
         "length too small, use Push() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Writer* const dest = dest_writer();
  Writer* const side_dest = side_dest_writer();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(dest, side_dest))) return false;
  const bool ok = dest->Push(min_length, recommended_length);
  MakeBuffer(dest);
  return ok;
}

bool TeeWriterBase::WriteSlow(absl::string_view src) {
  RIEGELI_ASSERT_GT(src.size(), available())
      << "Failed precondition of Writer::WriteSlow(string_view): "
         "length too small, use Write(string_view) instead";
  return WriteInternal(src);
}

bool TeeWriterBase::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_GT(src.size(), UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Writer::WriteSlow(Chain): "
         "length too small, use Write(Chain) instead";
  return WriteInternal(src);
}

bool TeeWriterBase::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_GT(src.size(), UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Writer::WriteSlow(Chain&&): "
         "length too small, use Write(Chain&&) instead";
  return WriteInternal(std::move(src));
}

bool TeeWriterBase::WriteSlow(const absl::Cord& src) {
  RIEGELI_ASSERT_GT(src.size(), UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of Writer::WriteSlow(Cord): "
         "length too small, use Write(Cord) instead";
  return WriteInternal(src);
}

template <typename Src>
inline bool TeeWriterBase::WriteInternal(Src&& src) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Writer* const dest = dest_writer();
  Writer* const side_dest = side_dest_writer();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(dest, side_dest))) return false;
  if (ABSL_PREDICT_FALSE(!side_dest->Write(src))) {
    return FailWithoutAnnotation(*side_dest);
  }
  const bool ok = dest->Write(std::forward<Src>(src));
  MakeBuffer(dest);
  return ok;
}

void TeeWriterBase::WriteHintSlow(size_t length) {
  RIEGELI_ASSERT_GT(length, available())
      << "Failed precondition of Writer::WriteHintSlow(): "
         "length too small, use WriteHint() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return;
  Writer* const dest = dest_writer();
  Writer* const side_dest = side_dest_writer();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(dest, side_dest))) return;
  dest->WriteHint(length);
  MakeBuffer(dest);
}

bool TeeWriterBase::Flush(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Writer* const dest = dest_writer();
  Writer* const side_dest = side_dest_writer();
  if (ABSL_PREDICT_FALSE(!SyncBuffer(dest, side_dest))) return false;
  if (ABSL_PREDICT_FALSE(!side_dest->Flush(flush_type))) {
    return FailWithoutAnnotation(*side_dest);
  }
  const bool ok = dest->Flush(flush_type);
  MakeBuffer(dest);
  return ok;
}

}  // namespace riegeli
