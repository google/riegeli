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

#include "riegeli/bytes/writer.h"

#include <stddef.h>

#include <cmath>
#include <cstring>
#include <limits>
#include <optional>
#include <utility>

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
#include "riegeli/base/null_safe_memcpy.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

namespace {

template <typename T>
inline bool WriteUnsigned(T src, Writer& dest) {
  // `digits10` is rounded down, `kMaxNumDigits` is rounded up, hence `+ 1`.
  constexpr size_t kMaxNumDigits = std::numeric_limits<T>::digits10 + 1;
  if (ABSL_PREDICT_FALSE(!dest.Push(kMaxNumDigits))) return false;
  dest.set_cursor(write_int_internal::WriteDecUnsigned(src, dest.cursor()));
  return true;
}

template <typename T>
inline bool WriteSigned(T src, Writer& dest) {
  // `digits10` is rounded down, `kMaxNumDigits` is rounded up, hence `+ 1`.
  constexpr size_t kMaxNumDigits = std::numeric_limits<T>::digits10 + 1;
  // `+ 1` for the minus sign.
  if (ABSL_PREDICT_FALSE(!dest.Push(kMaxNumDigits + 1))) return false;
  dest.set_cursor(write_int_internal::WriteDecSigned(src, dest.cursor()));
  return true;
}

}  // namespace

void Writer::OnFail() { set_buffer(start()); }

absl::Status Writer::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) return Annotate(status, absl::StrCat("at byte ", pos()));
  return status;
}

bool Writer::FailOverflow() {
  return Fail(absl::ResourceExhaustedError("Writer position overflow"));
}

bool Writer::Write(const Chain& src) {
#ifdef MEMORY_SANITIZER
  for (const absl::string_view fragment : src.blocks()) {
    AssertInitialized(fragment.data(), fragment.size());
  }
#endif
  if (ABSL_PREDICT_TRUE(available() >= src.size() &&
                        src.size() <= kMaxBytesToCopy)) {
    src.CopyTo(cursor());
    move_cursor(src.size());
    return true;
  }
  AssertInitialized(start(), start_to_cursor());
  return WriteSlow(src);
}

bool Writer::Write(Chain&& src) {
#ifdef MEMORY_SANITIZER
  for (const absl::string_view fragment : src.blocks()) {
    AssertInitialized(fragment.data(), fragment.size());
  }
#endif
  if (ABSL_PREDICT_TRUE(available() >= src.size() &&
                        src.size() <= kMaxBytesToCopy)) {
    src.CopyTo(cursor());
    move_cursor(src.size());
    return true;
  }
  AssertInitialized(start(), start_to_cursor());
  return WriteSlow(std::move(src));
}

bool Writer::Write(const absl::Cord& src) {
#ifdef MEMORY_SANITIZER
  for (const absl::string_view fragment : src.Chunks()) {
    AssertInitialized(fragment.data(), fragment.size());
  }
#endif
  if (ABSL_PREDICT_TRUE(available() >= src.size() &&
                        src.size() <= kMaxBytesToCopy)) {
    cord_internal::CopyCordToArray(src, cursor());
    move_cursor(src.size());
    return true;
  }
  AssertInitialized(start(), start_to_cursor());
  return WriteSlow(src);
}

bool Writer::Write(absl::Cord&& src) {
#ifdef MEMORY_SANITIZER
  for (const absl::string_view fragment : src.Chunks()) {
    AssertInitialized(fragment.data(), fragment.size());
  }
#endif
  if (ABSL_PREDICT_TRUE(available() >= src.size() &&
                        src.size() <= kMaxBytesToCopy)) {
    cord_internal::CopyCordToArray(src, cursor());
    move_cursor(src.size());
    return true;
  }
  AssertInitialized(start(), start_to_cursor());
  return WriteSlow(std::move(src));
}

bool Writer::Write(signed char src) { return WriteSigned(src, *this); }

bool Writer::Write(unsigned char src) { return WriteUnsigned(src, *this); }

bool Writer::Write(short src) { return WriteSigned(src, *this); }

bool Writer::Write(unsigned short src) { return WriteUnsigned(src, *this); }

bool Writer::Write(int src) { return WriteSigned(src, *this); }

bool Writer::Write(unsigned src) { return WriteUnsigned(src, *this); }

bool Writer::Write(long src) { return WriteSigned(src, *this); }

bool Writer::Write(unsigned long src) { return WriteUnsigned(src, *this); }

bool Writer::Write(long long src) { return WriteSigned(src, *this); }

bool Writer::Write(unsigned long long src) { return WriteUnsigned(src, *this); }

bool Writer::Write(absl::int128 src) { return WriteSigned(src, *this); }

bool Writer::Write(absl::uint128 src) { return WriteUnsigned(src, *this); }

// TODO: Optimize implementations below.
bool Writer::Write(float src) { return Write(absl::StrCat(src)); }

bool Writer::Write(double src) { return Write(absl::StrCat(src)); }

bool Writer::Write(long double src) {
  absl::Format(this, "%g",
               // Consistently use "nan", never "-nan".
               ABSL_PREDICT_FALSE(std::isnan(src))
                   ? std::numeric_limits<long double>::quiet_NaN()
                   : src);
  return ok();
}

bool Writer::WriteSlow(absl::string_view src) {
  RIEGELI_ASSERT_LT(available(), src.size())
      << "Failed precondition of Writer::WriteSlow(string_view): "
         "enough space available, use Write(string_view) instead";
  do {
    const size_t available_length = available();
    riegeli::null_safe_memcpy(cursor(), src.data(), available_length);
    move_cursor(available_length);
    src.remove_prefix(available_length);
    if (ABSL_PREDICT_FALSE(!PushSlow(1, src.size()))) return false;
  } while (src.size() > available());
  std::memcpy(cursor(), src.data(), src.size());
  move_cursor(src.size());
  return true;
}

bool Writer::WriteSlow(ExternalRef src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(ExternalRef): "
         "enough space available, use Write(ExternalRef) instead";
  return Write(absl::string_view(src));
}

bool Writer::WriteSlow(const Chain& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain): "
         "enough space available, use Write(Chain) instead";
  for (const absl::string_view fragment : src.blocks()) {
    if (ABSL_PREDICT_FALSE(!Write(fragment))) return false;
  }
  return true;
}

bool Writer::WriteSlow(Chain&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Chain&&): "
         "enough space available, use Write(Chain&&) instead";
  // Not `std::move(src)`: forward to `WriteSlow(const Chain&)`.
  return WriteSlow(src);
}

bool Writer::WriteSlow(const absl::Cord& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Cord): "
         "enough space available, use Write(Cord) instead";
  if (const std::optional<absl::string_view> flat = src.TryFlat();
      flat != std::nullopt) {
    return Write(*flat);
  }
  for (const absl::string_view fragment : src.Chunks()) {
    if (ABSL_PREDICT_FALSE(!Write(fragment))) return false;
  }
  return true;
}

bool Writer::WriteSlow(absl::Cord&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(Cord&&): "
         "enough space available, use Write(Cord&&) instead";
  // Not `std::move(src)`: forward to `WriteSlow(const absl::Cord&)`.
  return WriteSlow(src);
}

bool Writer::WriteSlow(ByteFill src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of Writer::WriteSlow(ByteFill): "
         "enough space available, use Write(ByteFill) instead";
  while (src.size() > available()) {
    const size_t available_length = available();
    riegeli::null_safe_memset(cursor(), src.fill(), available_length);
    move_cursor(available_length);
    src.Extract(available_length);
    if (ABSL_PREDICT_FALSE(!Push(1, SaturatingIntCast<size_t>(src.size())))) {
      return false;
    }
  }
  std::memset(cursor(), src.fill(), IntCast<size_t>(src.size()));
  move_cursor(IntCast<size_t>(src.size()));
  return true;
}

bool Writer::FlushImpl(FlushType flush_type) { return ok(); }

bool Writer::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT_NE(new_pos, pos())
      << "Failed precondition of Writer::SeekSlow(): "
         "position unchanged, use Seek() instead";
  return Fail(absl::UnimplementedError("Writer::Seek() not supported"));
}

std::optional<Position> Writer::SizeImpl() {
  Fail(absl::UnimplementedError("Writer::Size() not supported"));
  return std::nullopt;
}

bool Writer::TruncateImpl(Position new_size) {
  return Fail(absl::UnimplementedError("Writer::Truncate() not supported"));
}

Reader* Writer::ReadModeImpl(Position initial_pos) {
  Fail(absl::UnimplementedError("Writer::ReadMode() not supported"));
  return nullptr;
}

namespace writer_internal {

void DeleteReader(Reader* reader) { delete reader; }

}  // namespace writer_internal

}  // namespace riegeli
