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

#include "riegeli/bytes/wrapped_backward_writer.h"

#include <stddef.h>

#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/backward_writer.h"

namespace riegeli {

void WrappedBackwardWriterBase::Done() {
  if (ABSL_PREDICT_TRUE(ok())) {
    BackwardWriter& dest = *dest_writer();
    SyncBuffer(dest);
  }
  BackwardWriter::Done();
}

absl::Status WrappedBackwardWriterBase::AnnotateStatusImpl(
    absl::Status status) {
  // Fully delegate annotations to `*dest_writer()`.
  if (is_open()) {
    BackwardWriter& dest = *dest_writer();
    SyncBuffer(dest);
    status = dest.AnnotateStatus(std::move(status));
    MakeBuffer(dest);
  }
  return status;
}

bool WrappedBackwardWriterBase::PushSlow(size_t min_length,
                                         size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of BackwardWriter::PushSlow(): "
         "enough space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  BackwardWriter& dest = *dest_writer();
  SyncBuffer(dest);
  const bool push_ok = dest.Push(min_length, recommended_length);
  MakeBuffer(dest);
  return push_ok;
}

bool WrappedBackwardWriterBase::WriteSlow(absl::string_view src) {
  RIEGELI_ASSERT_LT(available(), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(string_view): "
         "enough space available, use Write(string_view) instead";
  return WriteInternal(src);
}

bool WrappedBackwardWriterBase::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Chain): "
         "enough space available, use Write(Chain) instead";
  return WriteInternal(src);
}

bool WrappedBackwardWriterBase::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Chain&&): "
         "enough space available, use Write(Chain&&) instead";
  return WriteInternal(std::move(src));
}

bool WrappedBackwardWriterBase::WriteSlow(const absl::Cord& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Cord): "
         "enough space available, use Write(Cord) instead";
  return WriteInternal(src);
}

bool WrappedBackwardWriterBase::WriteSlow(absl::Cord&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Cord&&): "
         "enough space available, use Write(Cord&&) instead";
  return WriteInternal(std::move(src));
}

template <typename Src>
inline bool WrappedBackwardWriterBase::WriteInternal(Src&& src) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  BackwardWriter& dest = *dest_writer();
  SyncBuffer(dest);
  const bool write_ok = dest.Write(std::forward<Src>(src));
  MakeBuffer(dest);
  return write_ok;
}

bool WrappedBackwardWriterBase::WriteZerosSlow(Position length) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of BackwardWriter::WriteZerosSlow(): "
         "enough space available, use WriteZeros() instead";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  BackwardWriter& dest = *dest_writer();
  SyncBuffer(dest);
  const bool write_ok = dest.WriteZeros(length);
  MakeBuffer(dest);
  return write_ok;
}

bool WrappedBackwardWriterBase::PrefersCopying() const {
  const BackwardWriter* const dest = dest_writer();
  return dest != nullptr && dest->PrefersCopying();
}

bool WrappedBackwardWriterBase::SupportsTruncate() {
  BackwardWriter* const dest = dest_writer();
  return dest != nullptr && dest->SupportsTruncate();
}

bool WrappedBackwardWriterBase::TruncateImpl(Position new_size) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  BackwardWriter& dest = *dest_writer();
  SyncBuffer(dest);
  const bool truncate_ok = dest.Truncate(new_size);
  MakeBuffer(dest);
  return truncate_ok;
}

}  // namespace riegeli
