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

#include <cmath>
#include <cstring>
#include <limits>
#include <optional>
#include <utility>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/numeric/int128.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/byte_fill.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/cord_utils.h"
#include "riegeli/base/external_ref.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/write_int_internal.h"

namespace riegeli {

namespace {

template <typename T>
inline bool WriteUnsigned(T src, BackwardWriter& dest) {
  // `digits10` is rounded down, `kMaxNumDigits` is rounded up, hence `+ 1`.
  constexpr size_t kMaxNumDigits = std::numeric_limits<T>::digits10 + 1;
  if (ABSL_PREDICT_FALSE(!dest.Push(kMaxNumDigits))) return false;
  dest.set_cursor(
      write_int_internal::WriteDecUnsignedBackward(src, dest.cursor()));
  return true;
}

template <typename T>
inline bool WriteSigned(T src, BackwardWriter& dest) {
  // `digits10` is rounded down, `kMaxNumDigits` is rounded up, hence `+ 1`.
  constexpr size_t kMaxNumDigits = std::numeric_limits<T>::digits10 + 1;
  // `+ 1` for the minus sign.
  if (ABSL_PREDICT_FALSE(!dest.Push(kMaxNumDigits + 1))) return false;
  dest.set_cursor(
      write_int_internal::WriteDecSignedBackward(src, dest.cursor()));
  return true;
}

}  // namespace

void BackwardWriter::OnFail() { set_buffer(start()); }

absl::Status BackwardWriter::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) return Annotate(status, absl::StrCat("at byte ", pos()));
  return status;
}

bool BackwardWriter::FailOverflow() {
  return Fail(absl::ResourceExhaustedError("BackwardWriter position overflow"));
}

bool BackwardWriter::Write(const Chain& src) {
#ifdef MEMORY_SANITIZER
  for (const absl::string_view fragment : src.blocks()) {
    AssertInitialized(fragment.data(), fragment.size());
  }
#endif
  if (ABSL_PREDICT_TRUE(available() >= src.size() &&
                        src.size() <= kMaxBytesToCopy)) {
    move_cursor(src.size());
    src.CopyTo(cursor());
    return true;
  }
  AssertInitialized(cursor(), start_to_cursor());
  return WriteSlow(src);
}

bool BackwardWriter::Write(Chain&& src) {
#ifdef MEMORY_SANITIZER
  for (const absl::string_view fragment : src.blocks()) {
    AssertInitialized(fragment.data(), fragment.size());
  }
#endif
  if (ABSL_PREDICT_TRUE(available() >= src.size() &&
                        src.size() <= kMaxBytesToCopy)) {
    move_cursor(src.size());
    src.CopyTo(cursor());
    return true;
  }
  AssertInitialized(cursor(), start_to_cursor());
  return WriteSlow(std::move(src));
}

bool BackwardWriter::Write(const absl::Cord& src) {
#ifdef MEMORY_SANITIZER
  for (const absl::string_view fragment : src.Chunks()) {
    AssertInitialized(fragment.data(), fragment.size());
  }
#endif
  if (ABSL_PREDICT_TRUE(available() >= src.size() &&
                        src.size() <= kMaxBytesToCopy)) {
    move_cursor(src.size());
    cord_internal::CopyCordToArray(src, cursor());
    return true;
  }
  AssertInitialized(cursor(), start_to_cursor());
  return WriteSlow(src);
}

bool BackwardWriter::Write(absl::Cord&& src) {
#ifdef MEMORY_SANITIZER
  for (const absl::string_view fragment : src.Chunks()) {
    AssertInitialized(fragment.data(), fragment.size());
  }
#endif
  if (ABSL_PREDICT_TRUE(available() >= src.size() &&
                        src.size() <= kMaxBytesToCopy)) {
    move_cursor(src.size());
    cord_internal::CopyCordToArray(src, cursor());
    return true;
  }
  AssertInitialized(cursor(), start_to_cursor());
  return WriteSlow(std::move(src));
}

bool BackwardWriter::Write(signed char src) { return WriteSigned(src, *this); }

bool BackwardWriter::Write(unsigned char src) {
  return WriteUnsigned(src, *this);
}

bool BackwardWriter::Write(short src) { return WriteSigned(src, *this); }

bool BackwardWriter::Write(unsigned short src) {
  return WriteUnsigned(src, *this);
}

bool BackwardWriter::Write(int src) { return WriteSigned(src, *this); }

bool BackwardWriter::Write(unsigned src) { return WriteUnsigned(src, *this); }

bool BackwardWriter::Write(long src) { return WriteSigned(src, *this); }

bool BackwardWriter::Write(unsigned long src) {
  return WriteUnsigned(src, *this);
}

bool BackwardWriter::Write(long long src) { return WriteSigned(src, *this); }

bool BackwardWriter::Write(unsigned long long src) {
  return WriteUnsigned(src, *this);
}

bool BackwardWriter::Write(absl::int128 src) { return WriteSigned(src, *this); }

bool BackwardWriter::Write(absl::uint128 src) {
  return WriteUnsigned(src, *this);
}

// TODO: Optimize implementations below.
bool BackwardWriter::Write(float src) { return Write(absl::StrCat(src)); }

bool BackwardWriter::Write(double src) { return Write(absl::StrCat(src)); }

bool BackwardWriter::Write(long double src) {
  return Write(
      absl::StrFormat("%g",
                      // Consistently use "nan", never "-nan".
                      ABSL_PREDICT_FALSE(std::isnan(src))
                          ? std::numeric_limits<long double>::quiet_NaN()
                          : src));
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

bool BackwardWriter::WriteSlow(ExternalRef src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(ExternalRef): "
         "enough space available, use Write(ExternalRef) instead";
  return Write(absl::string_view(src));
}

bool BackwardWriter::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(Chain): "
         "enough space available, use Write(Chain) instead";
  for (Chain::Blocks::const_reverse_iterator iter = src.blocks().crbegin();
       iter != src.blocks().crend(); ++iter) {
    if (ABSL_PREDICT_FALSE(!Write(absl::string_view(*iter)))) return false;
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
  if (const std::optional<absl::string_view> flat = src.TryFlat();
      flat != std::nullopt) {
    return Write(*flat);
  }
  if (src.size() <= available()) {
    move_cursor(src.size());
    cord_internal::CopyCordToArray(src, cursor());
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

bool BackwardWriter::WriteSlow(ByteFill src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of BackwardWriter::WriteSlow(ByteFill): "
         "enough space available, use Write(ByteFill) instead";
  while (src.size() > available()) {
    const size_t available_length = available();
    // `std::memset(nullptr, _, 0)` is undefined.
    if (available_length > 0) {
      move_cursor(available_length);
      std::memset(cursor(), src.fill(), available_length);
      src.Extract(available_length);
    }
    if (ABSL_PREDICT_FALSE(!Push(1, SaturatingIntCast<size_t>(src.size())))) {
      return false;
    }
  }
  move_cursor(IntCast<size_t>(src.size()));
  std::memset(cursor(), src.fill(), IntCast<size_t>(src.size()));
  return true;
}

bool BackwardWriter::FlushImpl(FlushType flush_type) { return ok(); }

bool BackwardWriter::TruncateImpl(Position new_size) {
  return Fail(
      absl::UnimplementedError("BackwardWriter::Truncate() not supported"));
}

}  // namespace riegeli
