// Copyright 2017 Google LLC
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

#include "riegeli/bytes/backward_writer.h"

#include <stddef.h>

#include <cstring>
#include <iterator>
#include <string>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"

namespace riegeli {

void BackwardWriter::OnFail() { set_buffer(); }

void BackwardWriter::DefaultAnnotateStatus() {
  RIEGELI_ASSERT(!not_failed())
      << "Failed precondition of Object::DefaultAnnotateStatus(): "
         "Object not failed";
  if (is_open()) AnnotateStatus(absl::StrCat("at byte ", start_pos()));
}

bool BackwardWriter::FailOverflow() {
  return Fail(absl::ResourceExhaustedError("BackwardWriter position overflow"));
}

bool BackwardWriter::WriteSlow(absl::string_view src) {
  RIEGELI_ASSERT_LT(available(), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(string_view): "
         "enough space available, use Write(string_view) instead";
  do {
    const size_t available_length = available();
    if (
        // `std::memcpy(nullptr, _, 0)` is undefined.
        available_length > 0) {
      move_cursor(available_length);
      std::memcpy(cursor(), src.data() + src.size() - available_length,
                  available_length);
      src.remove_suffix(available_length);
    }
    if (ABSL_PREDICT_FALSE(!PushSlow(1, src.size()))) return false;
  } while (src.size() > available());
  move_cursor(src.size());
  std::memcpy(cursor(), src.data(), src.size());
  return true;
}

bool BackwardWriter::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Chain): "
         "enough space available, use Write(Chain) instead";
  for (Chain::Blocks::const_reverse_iterator iter = src.blocks().crbegin();
       iter != src.blocks().crend(); ++iter) {
    if (ABSL_PREDICT_FALSE(!Write(*iter))) return false;
  }
  return true;
}

bool BackwardWriter::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Chain&&): "
         "enough space available, use Write(Chain&&) instead";
  // Not `std::move(src)`: forward to `WriteSlow(const Chain&)`.
  return WriteSlow(src);
}

bool BackwardWriter::WriteSlow(const absl::Cord& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Cord): "
         "enough space available, use Write(Cord) instead";
  if (src.size() <= available()) {
    move_cursor(src.size());
    char* dest = cursor();
    for (absl::string_view fragment : src.Chunks()) {
      std::memcpy(dest, fragment.data(), fragment.size());
      dest += fragment.size();
    }
    return true;
  }
  std::vector<absl::string_view> fragments(src.chunk_begin(), src.chunk_end());
  for (std::vector<absl::string_view>::const_reverse_iterator iter =
           fragments.crbegin();
       iter != fragments.crend(); ++iter) {
    if (ABSL_PREDICT_FALSE(!Write(*iter))) return false;
  }
  return true;
}

bool BackwardWriter::WriteSlow(absl::Cord&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Cord&&): "
         "enough space available, use Write(Cord&&) instead";
  // Not `std::move(src)`: forward to `WriteSlow(const absl::Cord&)`.
  return WriteSlow(src);
}

bool BackwardWriter::WriteZerosSlow(Position length) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of BackwardWriter::WriteZerosSlow(): "
         "enough space available, use WriteZeros() instead";
  while (length > available()) {
    const size_t available_length = available();
    if (
        // `std::memset(nullptr, _, 0)` is undefined.
        available_length > 0) {
      move_cursor(available_length);
      std::memset(cursor(), 0, available_length);
      length -= available_length;
    }
    if (ABSL_PREDICT_FALSE(!Push(1, SaturatingIntCast<size_t>(length)))) {
      return false;
    }
  }
  move_cursor(IntCast<size_t>(length));
  std::memset(cursor(), 0, IntCast<size_t>(length));
  return true;
}

bool BackwardWriter::FlushImpl(FlushType flush_type) { return healthy(); }

bool BackwardWriter::TruncateImpl(Position new_size) {
  return Fail(
      absl::UnimplementedError("BackwardWriter::Truncate() not supported"));
}

}  // namespace riegeli
