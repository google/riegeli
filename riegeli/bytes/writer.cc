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
#include <stdint.h>

#include <cmath>
#include <cstring>
#include <limits>
#include <string>
#include <type_traits>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

namespace {

inline char* WriteOneDigit(uint32_t src, char* dest) {
  RIEGELI_ASSERT_LE(src, 10u)
      << "Failed precondition of WriteOneDigit(): value too large";
  *dest = '0' + static_cast<char>(src);
  return dest + 1;
}

inline char* WriteTwoDigits(uint32_t src, char* dest) {
  RIEGELI_ASSERT_LE(src, 100u)
      << "Failed precondition of WriteTwoDigits(): value too large";
  static constexpr char kTwoDigits[100][2] = {
      {'0', '0'}, {'0', '1'}, {'0', '2'}, {'0', '3'}, {'0', '4'}, {'0', '5'},
      {'0', '6'}, {'0', '7'}, {'0', '8'}, {'0', '9'}, {'1', '0'}, {'1', '1'},
      {'1', '2'}, {'1', '3'}, {'1', '4'}, {'1', '5'}, {'1', '6'}, {'1', '7'},
      {'1', '8'}, {'1', '9'}, {'2', '0'}, {'2', '1'}, {'2', '2'}, {'2', '3'},
      {'2', '4'}, {'2', '5'}, {'2', '6'}, {'2', '7'}, {'2', '8'}, {'2', '9'},
      {'3', '0'}, {'3', '1'}, {'3', '2'}, {'3', '3'}, {'3', '4'}, {'3', '5'},
      {'3', '6'}, {'3', '7'}, {'3', '8'}, {'3', '9'}, {'4', '0'}, {'4', '1'},
      {'4', '2'}, {'4', '3'}, {'4', '4'}, {'4', '5'}, {'4', '6'}, {'4', '7'},
      {'4', '8'}, {'4', '9'}, {'5', '0'}, {'5', '1'}, {'5', '2'}, {'5', '3'},
      {'5', '4'}, {'5', '5'}, {'5', '6'}, {'5', '7'}, {'5', '8'}, {'5', '9'},
      {'6', '0'}, {'6', '1'}, {'6', '2'}, {'6', '3'}, {'6', '4'}, {'6', '5'},
      {'6', '6'}, {'6', '7'}, {'6', '8'}, {'6', '9'}, {'7', '0'}, {'7', '1'},
      {'7', '2'}, {'7', '3'}, {'7', '4'}, {'7', '5'}, {'7', '6'}, {'7', '7'},
      {'7', '8'}, {'7', '9'}, {'8', '0'}, {'8', '1'}, {'8', '2'}, {'8', '3'},
      {'8', '4'}, {'8', '5'}, {'8', '6'}, {'8', '7'}, {'8', '8'}, {'8', '9'},
      {'9', '0'}, {'9', '1'}, {'9', '2'}, {'9', '3'}, {'9', '4'}, {'9', '5'},
      {'9', '6'}, {'9', '7'}, {'9', '8'}, {'9', '9'}};
  std::memcpy(dest, kTwoDigits[src], 2);
  return dest + 2;
}

char* WriteUnsignedImpl(uint32_t src, char* dest) {
  uint32_t digits;

  if (src < 100) {
    if (src >= 10) goto lt100;
    return WriteOneDigit(src, dest);
  }
  if (src < 10'000) {
    if (src >= 1'000) goto lt10_000;
    digits = src / 100;
    src %= 100;
    dest = WriteOneDigit(digits, dest);
    goto lt100;
  }
  if (src < 1'000'000) {
    if (src >= 100'000) goto lt1_000_000;
    digits = src / 10'000;
    src %= 10'000;
    dest = WriteOneDigit(digits, dest);
    goto lt10_000;
  }
  if (src < 100'000'000) {
    if (src >= 10'000'000) goto lt100_000_000;
    digits = src / 1'000'000;
    src %= 1'000'000;
    dest = WriteOneDigit(digits, dest);
    goto lt1_000_000;
  }

  if (src >= 1'000'000'000) {
    digits = src / 100'000'000;
    src %= 100'000'000;
    dest = WriteTwoDigits(digits, dest);
    goto lt100_000_000;
  }

  digits = src / 100'000'000;
  src %= 100'000'000;
  dest = WriteOneDigit(digits, dest);
lt100_000_000:
  digits = src / 1'000'000;
  src %= 1'000'000;
  dest = WriteTwoDigits(digits, dest);
lt1_000_000:
  digits = src / 10'000;
  src %= 10'000;
  dest = WriteTwoDigits(digits, dest);
lt10_000:
  digits = src / 100;
  src %= 100;
  dest = WriteTwoDigits(digits, dest);
lt100:
  return WriteTwoDigits(src, dest);
}

char* WriteUnsignedImpl(uint64_t src, char* dest) {
  if (src <= std::numeric_limits<uint32_t>::max()) {
    return WriteUnsignedImpl(IntCast<uint32_t>(src), dest);
  }
  if (src >= 10'000'000'000) {
    const uint64_t over_10_digits = src / 10'000'000'000;
    src %= 10'000'000'000;
    dest = WriteUnsignedImpl(IntCast<uint32_t>(over_10_digits), dest);
  }

  uint32_t digits = IntCast<uint32_t>(src / 100'000'000);
  uint32_t src32 = IntCast<uint32_t>(src % 100'000'000);
  dest = WriteTwoDigits(digits, dest);
  digits = src32 / 1'000'000;
  src32 %= 1'000'000;
  dest = WriteTwoDigits(digits, dest);
  digits = src32 / 10'000;
  src32 %= 10'000;
  dest = WriteTwoDigits(digits, dest);
  digits = src32 / 100;
  src32 %= 100;
  dest = WriteTwoDigits(digits, dest);
  return WriteTwoDigits(src32, dest);
}

template <typename T, std::enable_if_t<(std::numeric_limits<T>::max() <=
                                        std::numeric_limits<uint32_t>::max()),
                                       int> = 0>
inline char* WriteUnsignedDispatch(T src, char* dest) {
  return WriteUnsignedImpl(IntCast<uint32_t>(src), dest);
}

template <
    typename T,
    std::enable_if_t<
        (std::numeric_limits<T>::max() > std::numeric_limits<uint32_t>::max() &&
         std::numeric_limits<T>::max() <= std::numeric_limits<uint64_t>::max()),
        int> = 0>
inline char* WriteUnsignedDispatch(T src, char* dest) {
  return WriteUnsignedImpl(IntCast<uint64_t>(src), dest);
}

template <typename T>
inline bool WriteUnsigned(T src, Writer& dest) {
  // `digits10` is rounded down: the maximal number of decimal digits such that
  // all numbers with that many digits can be represented in `T`. The maximal
  // number of decimal digits of representations of values of type `T` is
  // rounded up.
  if (ABSL_PREDICT_FALSE(!dest.Push(std::numeric_limits<T>::digits10 + 1))) {
    return false;
  }
  dest.set_cursor(WriteUnsignedDispatch(src, dest.cursor()));
  return true;
}

template <typename T>
inline bool WriteSigned(T src, Writer& dest) {
  if (src < 0) {
    // See `digits10` comment in `WriteUnsigned()`. One extra character for the
    // minus sign.
    if (ABSL_PREDICT_FALSE(!dest.Push(std::numeric_limits<T>::digits10 + 2))) {
      return false;
    }
    char* ptr = dest.cursor();
    *ptr++ = '-';
    // Negate in the unsigned space to handle `std::numeric_limits<T>::min()`
    // correctly.
    dest.set_cursor(WriteUnsignedDispatch(
        static_cast<std::make_unsigned_t<T>>(
            0 - static_cast<std::make_unsigned_t<T>>(src)),
        ptr));
  } else {
    // See `digits10` comment in `WriteUnsigned()`.
    if (ABSL_PREDICT_FALSE(!dest.Push(std::numeric_limits<T>::digits10 + 1))) {
      return false;
    }
    dest.set_cursor(WriteUnsignedDispatch(IntCast<std::make_unsigned_t<T>>(src),
                                          dest.cursor()));
  }
  return true;
}

}  // namespace

void Writer::OnFail() { set_buffer(); }

absl::Status Writer::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) return Annotate(status, absl::StrCat("at byte ", pos()));
  return status;
}

bool Writer::FailOverflow() {
  return Fail(absl::ResourceExhaustedError("Writer position overflow"));
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
    if (
        // `std::memcpy(nullptr, _, 0)` is undefined.
        available_length > 0) {
      std::memcpy(cursor(), src.data(), available_length);
      move_cursor(available_length);
      src.remove_prefix(available_length);
    }
    if (ABSL_PREDICT_FALSE(!PushSlow(1, src.size()))) return false;
  } while (src.size() > available());
  std::memcpy(cursor(), src.data(), src.size());
  move_cursor(src.size());
  return true;
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
  if (const absl::optional<absl::string_view> flat = src.TryFlat()) {
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

bool Writer::WriteZerosSlow(Position length) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Writer::WriteZerosSlow(): "
         "enough space available, use WriteZeros() instead";
  while (length > available()) {
    const size_t available_length = available();
    if (
        // `std::memset(nullptr, _, 0)` is undefined.
        available_length > 0) {
      std::memset(cursor(), 0, available_length);
      move_cursor(available_length);
      length -= available_length;
    }
    if (ABSL_PREDICT_FALSE(!Push(1, SaturatingIntCast<size_t>(length)))) {
      return false;
    }
  }
  std::memset(cursor(), 0, IntCast<size_t>(length));
  move_cursor(IntCast<size_t>(length));
  return true;
}

bool Writer::WriteCharsSlow(Position length, char src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of Writer::WriteCharsSlow(): "
         "enough space available, use WriteChars() instead";
  if (src == '\0') return WriteZerosSlow(length);
  while (length > available()) {
    const size_t available_length = available();
    if (
        // `std::memset(nullptr, _, 0)` is undefined.
        available_length > 0) {
      std::memset(cursor(), src, available_length);
      move_cursor(available_length);
      length -= available_length;
    }
    if (ABSL_PREDICT_FALSE(!Push(1, SaturatingIntCast<size_t>(length)))) {
      return false;
    }
  }
  std::memset(cursor(), src, IntCast<size_t>(length));
  move_cursor(IntCast<size_t>(length));
  return true;
}

bool Writer::FlushImpl(FlushType flush_type) { return ok(); }

bool Writer::SeekSlow(Position new_pos) {
  RIEGELI_ASSERT_NE(new_pos, pos())
      << "Failed precondition of Writer::SeekSlow(): "
         "position unchanged, use Seek() instead";
  return Fail(absl::UnimplementedError("Writer::Seek() not supported"));
}

absl::optional<Position> Writer::SizeImpl() {
  Fail(absl::UnimplementedError("Writer::Size() not supported"));
  return absl::nullopt;
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
