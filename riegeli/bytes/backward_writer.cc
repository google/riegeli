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
#include <string>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/cord_utils.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"

namespace riegeli {

void BackwardWriter::OnFail() { set_buffer(); }

absl::Status BackwardWriter::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) return Annotate(status, absl::StrCat("at byte ", pos()));
  return status;
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
    // `std::memcpy(nullptr, _, 0)` is undefined.
    if (available_length > 0) {
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

bool BackwardWriter::WriteStringSlow(std::string&& src) {
  RIEGELI_ASSERT_GT(src.size(), kMaxBytesToCopy)
      << "Failed precondition of BackwardWriter::WriteStringSlow(): "
         "string too short, use Write() instead";
  if (PrefersCopying() || Wasteful(src.capacity(), src.size())) {
    return Write(absl::string_view(src));
  }
  AssertInitialized(src.data(), src.size());
  AssertInitialized(cursor(), start_to_cursor());
  return WriteSlow(Chain(std::move(src)));
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
  {
    const absl::optional<absl::string_view> flat = src.TryFlat();
    if (flat != absl::nullopt) {
      return Write(*flat);
    }
  }
  if (src.size() <= available()) {
    move_cursor(src.size());
    CopyCordToArray(src, cursor());
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
    // `std::memset(nullptr, _, 0)` is undefined.
    if (available_length > 0) {
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

bool BackwardWriter::WriteCharsSlow(Position length, char src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of BackwardWriter::WriteCharsSlow(): "
         "enough space available, use WriteChars() instead";
  if (src == '\0') return WriteZerosSlow(length);
  while (length > available()) {
    const size_t available_length = available();
    // `std::memset(nullptr, _, 0)` is undefined.
    if (available_length > 0) {
      move_cursor(available_length);
      std::memset(cursor(), src, available_length);
      length -= available_length;
    }
    if (ABSL_PREDICT_FALSE(!Push(1, SaturatingIntCast<size_t>(length)))) {
      return false;
    }
  }
  move_cursor(IntCast<size_t>(length));
  std::memset(cursor(), src, IntCast<size_t>(length));
  return true;
}

bool BackwardWriter::FlushImpl(FlushType flush_type) { return ok(); }

bool BackwardWriter::TruncateImpl(Position new_size) {
  return Fail(
      absl::UnimplementedError("BackwardWriter::Truncate() not supported"));
}

}  // namespace riegeli
