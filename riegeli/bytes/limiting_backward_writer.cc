// Copyright 2018 Google LLC
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

#include "riegeli/bytes/limiting_backward_writer.h"

#include <stddef.h>
#include <memory>
#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/backward_writer.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if __cplusplus < 201703
constexpr Position LimitingBackwardWriterBase::kNoSizeLimit;
#endif

void LimitingBackwardWriterBase::Done() {
  if (ABSL_PREDICT_TRUE(healthy())) {
    BackwardWriter* const dest = dest_writer();
    SyncBuffer(dest);
  }
  BackwardWriter::Done();
}

bool LimitingBackwardWriterBase::PushSlow() {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of BackwardWriter::PushSlow(): "
         "space available, use Push() instead";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  BackwardWriter* const dest = dest_writer();
  if (ABSL_PREDICT_FALSE(pos() == size_limit_)) return FailOverflow();
  SyncBuffer(dest);
  const bool ok = dest->Push();
  MakeBuffer(dest);
  return ok;
}

bool LimitingBackwardWriterBase::WriteSlow(absl::string_view src) {
  RIEGELI_ASSERT_GT(src.size(), available())
      << "Failed precondition of BackwardWriter::WriteSlow(string_view): "
         "length too small, use Write(string_view) instead";
  return WriteInternal(src);
}

bool LimitingBackwardWriterBase::WriteSlow(std::string&& src) {
  RIEGELI_ASSERT_GT(src.size(), available())
      << "Failed precondition of BackwardWriter::WriteSlow(string&&): "
         "length too small, use Write(string&&) instead";
  return WriteInternal(std::move(src));
}

bool LimitingBackwardWriterBase::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_GT(src.size(), UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of BackwardWriter::WriteSlow(Chain): "
         "length too small, use Write(Chain) instead";
  return WriteInternal(src);
}

bool LimitingBackwardWriterBase::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_GT(src.size(), UnsignedMin(available(), kMaxBytesToCopy))
      << "Failed precondition of BackwardWriter::WriteSlow(Chain&&): "
         "length too small, use Write(Chain&&) instead";
  return WriteInternal(std::move(src));
}

template <typename Src>
inline bool LimitingBackwardWriterBase::WriteInternal(Src&& src) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  BackwardWriter* const dest = dest_writer();
  RIEGELI_ASSERT_LE(pos(), size_limit_)
      << "Failed invariant of LimitingBackwardWriter: "
         "position exceeds size limit";
  if (ABSL_PREDICT_FALSE(src.size() > size_limit_ - pos())) {
    return FailOverflow();
  }
  SyncBuffer(dest);
  const bool ok = dest->Write(std::forward<Src>(src));
  MakeBuffer(dest);
  return ok;
}

bool LimitingBackwardWriterBase::SupportsTruncate() const {
  const BackwardWriter* const dest = dest_writer();
  return dest != nullptr && dest->SupportsTruncate();
}

bool LimitingBackwardWriterBase::Truncate(Position new_size) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  BackwardWriter* const dest = dest_writer();
  SyncBuffer(dest);
  const bool ok = dest->Truncate(new_size);
  MakeBuffer(dest);
  return ok;
}

template class LimitingBackwardWriter<BackwardWriter*>;
template class LimitingBackwardWriter<std::unique_ptr<BackwardWriter>>;

}  // namespace riegeli
