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

#ifndef RIEGELI_BYTES_BACKWARD_WRITER_H_
#define RIEGELI_BYTES_BACKWARD_WRITER_H_

#include <stddef.h>
#include <stdint.h>

#include <cstring>
#include <limits>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/meta/type_traits.h"
#include "absl/numeric/int128.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/cord_utils.h"
#include "riegeli/base/object.h"
#include "riegeli/base/type_traits.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/restricted_chain_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Abstract class `BackwardWriter` writes sequences of bytes to a destination,
// like `Writer`, but back to front.
//
// Sequential writing is supported, random access is not supported, truncation
// is optionally supported.
class BackwardWriter : public Object {
 public:
  // The same as `Object::Close()`.
  //
  // The implementation in this class adds an assertion.
  bool Close();

  // If `write_size_hint` is not `absl::nullopt`, hints that this amount of data
  // will be written sequentially from the current position, then `Close()` will
  // be called.
  //
  // This may improve performance and memory usage:
  //  * Larger buffer sizes may be used before reaching the size hint, and
  //    a smaller buffer size may be used when reaching the size hint.
  //  * This hint may be propagated to owned destinations.
  //  * Other consequences are possible.
  //
  // If the hint turns out to not match reality, nothing breaks. It is better if
  // `write_size_hint` is slightly too large than slightly too small.
  //
  // `SetWriteSizeHint()` is usually be called from the same abstraction layer
  // which later calls `Close()`.
  void SetWriteSizeHint(absl::optional<Position> write_size_hint);

  // Ensures that enough space is available in the buffer: if less than
  // `min_length` of space is available, pushes previously written data to the
  // destination, and points `cursor()` and `limit()` to space following the
  // current position with length at least `min_length`, preferably
  // `recommended_length`.
  //
  // The current position does not change with `Push()`. It changes with e.g.
  // `move_cursor()` and `Write()`.
  //
  // If `recommended_length < min_length`, `recommended_length` is assumed to be
  // `min_length`.
  //
  // Return values:
  //  * `true`  - success (`available() >= min_length`)
  //  * `false` - failure (`available() < min_length`, `!ok()`)
  bool Push(size_t min_length = 1, size_t recommended_length = 0);

  // Buffer pointers. Space between `start()` (exclusive upper bound) and
  // `limit()` (inclusive lower bound) is available for immediate writing data
  // to it, with `cursor()` pointing to the current position going downwards
  // (past the next byte to write).
  //
  // Memory before the address to which `cursor()` is eventually moved must not
  // be clobbered.
  //
  // Non-const member functions may change buffer pointers, including changing
  // how much data around the current position are buffered.
  //
  // Invariants:
  //   `start() >= cursor() >= limit()` (possibly all `nullptr`)
  //   if `!ok()` then `start() == cursor() == limit() == nullptr`
  char* start() const { return start_; }
  char* cursor() const { return cursor_; }
  char* limit() const { return limit_; }

  // Decrements the value of `cursor()`. Does not change `start()` nor
  // `limit()`. Call this during writing data under `cursor()` to indicate how
  // much was written.
  //
  // Precondition: `length <= available()`
  void move_cursor(size_t length);

  // Sets the value of `cursor()`. Does not change `start()` nor `limit()`. Call
  // this during writing data under `cursor()` to indicate how much was written.
  //
  // Precondition: `start() >= cursor >= limit()`
  void set_cursor(char* cursor);

  // Returns the amount of space available in the buffer, between `cursor()` and
  // `limit()`.
  //
  // Invariant: if `!ok()` then `available() == 0`
  size_t available() const { return PtrDistance(limit_, cursor_); }

  // Returns the buffer size, between `start()` and `limit()`.
  //
  // Invariant: if `!ok()` then `start_to_limit() == 0`
  size_t start_to_limit() const { return PtrDistance(limit_, start_); }

  // Returns the amount of data written to the buffer, between `start()` and
  // `cursor()`.
  //
  // Invariant: if `!ok()` then `start_to_cursor() == 0`
  size_t start_to_cursor() const { return PtrDistance(cursor_, start_); }

  // Writes a single byte to the buffer or the destination.
  //
  // Return values:
  //  * `true`  - success (`ok()`)
  //  * `false` - failure (`!ok()`)
  bool WriteByte(uint8_t src) { return Write(static_cast<char>(src)); }

  // Writes a fixed number of bytes from `src` to the buffer and/or the
  // destination. The whole `src` is prepended, bytes are not reversed.
  //
  // `std::string&&` is accepted with a template to avoid implicit conversions
  // to `std::string` which can be ambiguous against `absl::string_view`
  // (e.g. `const char*`).
  //
  // `absl::Span<const char>` is accepted with a template to avoid ambiguity
  // with `absl::string_view` (e.g. `std::string`).
  //
  // Return values:
  //  * `true`  - success (`src.size()` bytes written)
  //  * `false` - failure (a suffix of less than `src.size()` bytes written,
  //                       `!ok()`)
  bool Write(char src);
#if __cpp_char8_t
  bool Write(char8_t src) { return Write(static_cast<char>(src)); }
#endif
  bool Write(absl::string_view src);
  ABSL_ATTRIBUTE_ALWAYS_INLINE
  bool Write(const char* src) { return Write(absl::string_view(src)); }
  template <typename Src,
            std::enable_if_t<std::is_same<Src, std::string>::value, int> = 0>
  bool Write(Src&& src);
  template <
      typename Src,
      std::enable_if_t<
          absl::conjunction<std::is_convertible<Src&&, absl::Span<const char>>,
                            absl::negation<std::is_convertible<
                                Src&&, absl::string_view>>>::value,
          int> = 0>
  bool Write(Src&& src);
  bool Write(const Chain& src);
  bool Write(Chain&& src);
  bool Write(const absl::Cord& src);
  bool Write(absl::Cord&& src);

  // Writes a stringified value to the buffer and/or the destination.
  //
  // Return values:
  //  * `true`  - success
  //  * `false` - failure (`!ok()`)
  bool Write(signed char src);
  bool Write(unsigned char src);
  bool Write(short src);
  bool Write(unsigned short src);
  bool Write(int src);
  bool Write(unsigned src);
  bool Write(long src);
  bool Write(unsigned long src);
  bool Write(long long src);
  bool Write(unsigned long long src);
  bool Write(absl::int128 src);
  bool Write(absl::uint128 src);
  bool Write(float src);
  bool Write(double src);
  bool Write(long double src);
  template <
      typename Src,
      std::enable_if_t<
          absl::conjunction<
              HasAbslStringify<Src>,
              absl::negation<std::is_convertible<Src&&, absl::string_view>>,
              absl::negation<
                  std::is_convertible<Src&&, absl::Span<const char>>>,
              absl::negation<std::is_convertible<Src&&, const Chain&>>,
              absl::negation<std::is_convertible<Src&&, const absl::Cord&>>>::
              value,
          int> = 0>
  bool Write(Src&& src);

  // Other integer types are is not supported. Delete overloads to avoid
  // implicit conversions.
  bool Write(bool src) = delete;
  bool Write(wchar_t src) = delete;
  bool Write(char16_t src) = delete;
  bool Write(char32_t src) = delete;

  // Writes stringified values to the buffer and/or the destination.
  //
  // `srcs` are prepended in the reverse order, so that they appear in the
  // destination in the same order as arguments of `Write()`.
  //
  // Return values:
  //  * `true`  - success
  //  * `false` - failure (`!ok()`)
  template <
      typename... Srcs,
      std::enable_if_t<
          absl::conjunction<std::integral_constant<bool, sizeof...(Srcs) != 1>,
                            IsStringifiable<Srcs>...>::value,
          int> = 0>
  bool Write(Srcs&&... srcs);

  // Writes stringified elements of the tuple to the buffer and/or the
  // destination.
  //
  // `srcs` are prepended in the reverse order, so that they appear in the
  // destination in the same order as arguments of the tuple.
  //
  // Return values:
  //  * `true`  - success
  //  * `false` - failure (`!ok()`)
  template <typename... Srcs,
            std::enable_if_t<absl::conjunction<IsStringifiable<Srcs>...>::value,
                             int> = 0>
  bool WriteTuple(const std::tuple<Srcs...>& srcs);
  template <typename... Srcs,
            std::enable_if_t<absl::conjunction<IsStringifiable<Srcs>...>::value,
                             int> = 0>
  bool WriteTuple(std::tuple<Srcs...>&& srcs);

  // Writes the given number of zero bytes to the buffer and/or the destination.
  //
  // Return values:
  //  * `true`  - success (`length` bytes written)
  //  * `false` - failure (less than `length` bytes written, `!ok()`)
  bool WriteZeros(Position length);

  // Writes the given number of copies of the given character or byte to the
  // buffer and/or the destination.
  //
  // Return values:
  //  * `true`  - success (`length` bytes written)
  //  * `false` - failure (less than `length` bytes written, `!ok()`)
  bool WriteChars(Position length, char src);
  bool WriteBytes(Position length, uint8_t src);

  // If `true`, a hint that there is no benefit in preparing a `Chain` or
  // `absl::Cord` for writing instead of `absl::string_view`, e.g. because
  // `WriteSlow(const Chain&)` and `WriteSlow(const absl::Cord&)` are
  // implemented in terms of `WriteSlow(absl::string_view)` anyway.
  virtual bool PrefersCopying() const { return false; }

  // Pushes buffered data to the destination.
  //
  // This makes data written so far visible, but in contrast to `Close()`,
  // keeps the possibility to write more data later. What exactly does it mean
  // for data to be visible depends on the destination. If this is not
  // applicable or not feasible, does nothing.
  //
  // The scope of objects to flush and the intended data durability (without a
  // guarantee) are specified by `flush_type`:
  //  * `FlushType::kFromObject`  - Makes data written so far visible in other
  //                                objects, propagating flushing through owned
  //                                dependencies of the given writer.
  //  * `FlushType::kFromProcess` - Makes data written so far visible outside
  //                                the process, propagating flushing through
  //                                dependencies of the given writer.
  //                                This is the default.
  //  * `FlushType::kFromMachine` - Makes data written so far visible outside
  //                                the process and durable in case of operating
  //                                system crash, propagating flushing through
  //                                dependencies of the given writer.
  //
  // Return values:
  //  * `true ` - success (`ok()`)
  //  * `false` - failure (`!ok()`)
  bool Flush(FlushType flush_type = FlushType::kFromProcess);

  // Returns the current position (increasing as data are prepended).
  //
  // This is not necessarily 0 after creating the `BackwardWriter` if it
  // prepends to a destination with existing contents, or if the
  // `BackwardWriter` wraps another writer or output stream propagating its
  // position.
  //
  // This may decrease when the `BackwardWriter` becomes not OK (due to
  // buffering, previously written but unflushed data may be lost).
  //
  // `pos()` is unchanged by a successful `Close()`.
  Position pos() const;

  // Returns the position corresponding to `start()`,
  // i.e. `pos() - start_to_cursor()`.
  Position start_pos() const { return start_pos_; }

  // Returns the position corresponding to `limit()`,
  // i.e. `pos() + available()`.
  Position limit_pos() const;

  // Returns `true` if this `BackwardWriter` supports `Truncate()`.
  virtual bool SupportsTruncate() { return false; }

  // Discards the part of the destination after the given position. Sets the
  // current position to the new end.
  //
  // Return values:
  //  * `true`                 - success (destination truncated, `ok()`)
  //  * `false` (when `ok()`)  - destination is smaller than `new_size`
  //                             (position is set to end)
  //  * `false` (when `!ok()`) - failure
  //
  // `Truncate()` is supported if `SupportsTruncate()` is `true`.
  bool Truncate(Position new_size);

 protected:
  using Object::Object;

  // Moves the part of the object defined in this class.
  //
  // Buffer pointers do not need to satisfy their invariants during this part of
  // the move, here they are merely exchanged with `nullptr` and copied.
  BackwardWriter(BackwardWriter&& that) noexcept;
  BackwardWriter& operator=(BackwardWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `BackwardWriter`. This
  // avoids constructing a temporary `BackwardWriter` and moving from it.
  // Derived classes which redefine `Reset()` should include a call to
  // `BackwardWriter::Reset()`.
  void Reset(Closed);
  void Reset();

  // `BackwardWriter` overrides `Object::Done()` to set buffer pointers to
  // `nullptr`. Derived classes which override it further should include a call
  // to `BackwardWriter::Done()`.
  void Done() override;

  // `BackwardWriter` overrides `Object::OnFail()` to set buffer pointers to
  // `nullptr`. Derived classes which override it further should include a call
  // to `BackwardWriter::OnFail()`.
  //
  // `pos()` decreases by `start_to_cursor()` to indicate that any buffered
  // data have been lost.
  ABSL_ATTRIBUTE_COLD void OnFail() override;

  // `BackwardWriter` overrides `Object::AnnotateStatusImpl()` to annotate the
  // status with the current position.
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;

  // Marks the `BackwardWriter` as failed with message
  // "BackwardWriter position overflow". Always returns `false`.
  //
  // This can be called if the destination would exceed its maximum possible
  // size or if `start_pos()` would overflow.
  ABSL_ATTRIBUTE_COLD bool FailOverflow();

  // Implementation of `SetWriteSizeHint()`.
  virtual void SetWriteSizeHintImpl(
      ABSL_ATTRIBUTE_UNUSED absl::optional<Position> write_size_hint) {}

  // Implementation of the slow part of `Push()`.
  //
  // Precondition: `available() < min_length`
  virtual bool PushSlow(size_t min_length, size_t recommended_length) = 0;

  // Sets the values of:
  //  * `start()`  - to `limit + start_to_limit`
  //  * `cursor()` - to `start() - start_to_cursor`
  //  * `limit()`  - to `limit`
  //
  // Preconditions:
  //   [`limit`..`limit + start_to_limit`) is a valid byte range
  //   `start_to_cursor <= start_to_limit`
  void set_buffer(char* limit = nullptr, size_t start_to_limit = 0,
                  size_t start_to_cursor = 0);

  // Implementation of the slow part of `Write()`.
  //
  // By default:
  //  * `WriteSlow(absl::string_view)` is implemented in terms of `PushSlow()`
  //  * `WriteSlow(const Chain&)` and `WriteSlow(const absl::Cord&)` are
  //    implemented in terms of `WriteSlow(absl::string_view)`
  //  * `WriteSlow(Chain&&)` is implemented in terms of
  //    `WriteSlow(const Chain&)`
  //  * `WriteSlow(absl::Cord&&)` is implemented in terms of
  //    `WriteSlow(const absl::Cord&)`
  //
  // Precondition for `WriteSlow(absl::string_view)`:
  //   `available() < src.size()`
  //
  // Precondition for `WriteStringSlow()`:
  //   `src.size() >= kMaxBytesToCopy`
  //
  // Precondition for `WriteSlow(const Chain&)`, `WriteSlow(Chain&&)`,
  // `WriteSlow(const absl::Cord&)`, and `WriteSlow(absl::Cord&&):
  //   `UnsignedMin(available(), kMaxBytesToCopy) < src.size()`
  virtual bool WriteSlow(absl::string_view src);
  bool WriteStringSlow(std::string&& src);
  virtual bool WriteSlow(const Chain& src);
  virtual bool WriteSlow(Chain&& src);
  virtual bool WriteSlow(const absl::Cord& src);
  virtual bool WriteSlow(absl::Cord&& src);

  // Implementation of the slow part of `WriteZeros()`.
  //
  // By default implemented in terms of `Push()`.
  //
  // Precondition:
  //   `UnsignedMin(available(), kMaxBytesToCopy) < length`
  virtual bool WriteZerosSlow(Position length);

  // Implementation of the slow part of `WriteChars()`.
  //
  // Precondition:
  //   `UnsignedMin(available(), kMaxBytesToCopy) < length`
  bool WriteCharsSlow(Position length, char src);

  // Implementation of `Flush()`, except that the parameter is not defaulted,
  // which is problematic for virtual functions.
  //
  // By default does nothing and returns `ok()`.
  virtual bool FlushImpl(FlushType flush_type);

  // Increments the value of `start_pos()`.
  void move_start_pos(Position length);

  // Sets the value of `start_pos()`.
  void set_start_pos(Position start_pos);

  // Implementation of `Truncate()`.
  //
  // By default fails.
  virtual bool TruncateImpl(Position new_size);

 private:
  template <size_t index, typename... Srcs,
            std::enable_if_t<(index > 0), int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE bool WriteInternal(
      const std::tuple<Srcs...>& srcs) {
    return Write(std::get<index - 1>(srcs)) && WriteInternal<index - 1>(srcs);
  }
  template <size_t index, typename... Srcs,
            std::enable_if_t<(index == 0), int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE bool WriteInternal(
      ABSL_ATTRIBUTE_UNUSED const std::tuple<Srcs...>& srcs) {
    return true;
  }

  template <size_t index, typename... Srcs,
            std::enable_if_t<(index > 0), int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE bool WriteInternal(std::tuple<Srcs...>&& srcs) {
    return Write(std::forward<
                 std::tuple_element_t<index - 1, std::tuple<Srcs...>>>(
               std::get<index - 1>(srcs))) &&
           WriteInternal<index - 1>(std::move(srcs));
  }
  template <size_t index, typename... Srcs,
            std::enable_if_t<(index == 0), int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE bool WriteInternal(
      ABSL_ATTRIBUTE_UNUSED std::tuple<Srcs...>&& srcs) {
    return true;
  }

  char* start_ = nullptr;
  char* cursor_ = nullptr;
  char* limit_ = nullptr;

  // Destination position corresponding to `start_`.
  //
  // Invariant:
  //   `start_pos_ <= std::numeric_limits<Position>::max() - start_to_limit()`
  Position start_pos_ = 0;
};

// Implementation details follow.

namespace write_int_internal {

template <typename T>
inline bool WriteUnsigned(T src, BackwardWriter& dest) {
  // `digits10` is rounded down, `kMaxNumDigits` is rounded up, hence `+ 1`.
  constexpr size_t kMaxNumDigits = std::numeric_limits<T>::digits10 + 1;
  if (ABSL_PREDICT_FALSE(!dest.Push(kMaxNumDigits))) return false;
  dest.set_cursor(WriteDecUnsignedBackward(src, dest.cursor()));
  return true;
}

template <typename T>
inline bool WriteSigned(T src, BackwardWriter& dest) {
  // `digits10` is rounded down, `kMaxNumDigits` is rounded up, hence `+ 1`.
  constexpr size_t kMaxNumDigits = std::numeric_limits<T>::digits10 + 1;
  // `+ 1` for the minus sign.
  if (ABSL_PREDICT_FALSE(!dest.Push(kMaxNumDigits + 1))) return false;
  dest.set_cursor(WriteDecSignedBackward(src, dest.cursor()));
  return true;
}

}  // namespace write_int_internal

inline BackwardWriter::BackwardWriter(BackwardWriter&& that) noexcept
    : Object(static_cast<Object&&>(that)),
      start_(std::exchange(that.start_, nullptr)),
      cursor_(std::exchange(that.cursor_, nullptr)),
      limit_(std::exchange(that.limit_, nullptr)),
      start_pos_(std::exchange(that.start_pos_, 0)) {}

inline BackwardWriter& BackwardWriter::operator=(
    BackwardWriter&& that) noexcept {
  Object::operator=(static_cast<Object&&>(that));
  start_ = std::exchange(that.start_, nullptr);
  cursor_ = std::exchange(that.cursor_, nullptr);
  limit_ = std::exchange(that.limit_, nullptr);
  start_pos_ = std::exchange(that.start_pos_, 0);
  return *this;
}

inline void BackwardWriter::Reset(Closed) {
  Object::Reset(kClosed);
  start_ = nullptr;
  cursor_ = nullptr;
  limit_ = nullptr;
  start_pos_ = 0;
}

inline void BackwardWriter::Reset() {
  Object::Reset();
  start_ = nullptr;
  cursor_ = nullptr;
  limit_ = nullptr;
  start_pos_ = 0;
}

inline bool BackwardWriter::Close() {
  AssertInitialized(cursor(), start_to_cursor());
  return Object::Close();
}

inline void BackwardWriter::Done() {
  start_pos_ = pos();
  set_buffer();
}

inline void BackwardWriter::SetWriteSizeHint(
    absl::optional<Position> write_size_hint) {
  AssertInitialized(cursor(), start_to_cursor());
  SetWriteSizeHintImpl(write_size_hint);
}

inline bool BackwardWriter::Push(size_t min_length, size_t recommended_length) {
  if (ABSL_PREDICT_TRUE(available() >= min_length)) return true;
  AssertInitialized(cursor(), start_to_cursor());
  if (ABSL_PREDICT_FALSE(!PushSlow(min_length, recommended_length))) {
    return false;
  }
  RIEGELI_ASSERT_GE(available(), min_length)
      << "Failed postcondition of BackwardWriter::PushSlow(): "
         "not enough space available";
  return true;
}

inline void BackwardWriter::move_cursor(size_t length) {
  RIEGELI_ASSERT_LE(length, available())
      << "Failed precondition of BackwardWriter::move_cursor(): "
         "length out of range";
  cursor_ -= length;
}

inline void BackwardWriter::set_cursor(char* cursor) {
  RIEGELI_ASSERT(cursor <= start())
      << "Failed precondition of BackwardWriter::set_cursor(): "
         "pointer out of range";
  RIEGELI_ASSERT(cursor >= limit())
      << "Failed precondition of BackwardWriter::set_cursor(): "
         "pointer out of range";
  cursor_ = cursor;
}

inline void BackwardWriter::set_buffer(char* limit, size_t start_to_limit,
                                       size_t start_to_cursor) {
  RIEGELI_ASSERT_LE(start_to_cursor, start_to_limit)
      << "Failed precondition of BackwardWriter::set_buffer(): "
         "length out of range";
  start_ = limit + start_to_limit;
  cursor_ = start_ - start_to_cursor;
  limit_ = limit;
}

inline bool BackwardWriter::Write(char src) {
  if (ABSL_PREDICT_FALSE(!Push())) return false;
  move_cursor(1);
  *cursor() = src;
  return true;
}

inline bool BackwardWriter::Write(absl::string_view src) {
  AssertInitialized(src.data(), src.size());
  if (ABSL_PREDICT_TRUE(available() >= src.size())) {
    if (ABSL_PREDICT_TRUE(
            // `std::memcpy(nullptr, _, 0)` and `std::memcpy(_, nullptr, 0)`
            // are undefined.
            !src.empty())) {
      move_cursor(src.size());
      std::memcpy(cursor(), src.data(), src.size());
    }
    return true;
  }
  AssertInitialized(cursor(), start_to_cursor());
  return WriteSlow(src);
}

template <typename Src,
          std::enable_if_t<std::is_same<Src, std::string>::value, int>>
inline bool BackwardWriter::Write(Src&& src) {
  if (ABSL_PREDICT_TRUE(src.size() <= kMaxBytesToCopy)) {
    return Write(absl::string_view(src));
  }
  // `std::move(src)` is correct and `std::forward<Src>(src)` is not necessary:
  // `Src` is always `std::string`, never an lvalue reference.
  return WriteStringSlow(std::move(src));
}

template <
    typename Src,
    std::enable_if_t<
        absl::conjunction<std::is_convertible<Src&&, absl::Span<const char>>,
                          absl::negation<std::is_convertible<
                              Src&&, absl::string_view>>>::value,
        int>>
inline bool BackwardWriter::Write(Src&& src) {
  const absl::Span<const char> span = std::forward<Src>(src);
  return Write(absl::string_view(span.data(), span.size()));
}

inline bool BackwardWriter::Write(const Chain& src) {
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

inline bool BackwardWriter::Write(Chain&& src) {
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

inline bool BackwardWriter::Write(const absl::Cord& src) {
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

inline bool BackwardWriter::Write(absl::Cord&& src) {
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

inline bool BackwardWriter::Write(signed char src) {
  return write_int_internal::WriteSigned(src, *this);
}

inline bool BackwardWriter::Write(unsigned char src) {
  return write_int_internal::WriteUnsigned(src, *this);
}

inline bool BackwardWriter::Write(short src) {
  return write_int_internal::WriteSigned(src, *this);
}

inline bool BackwardWriter::Write(unsigned short src) {
  return write_int_internal::WriteUnsigned(src, *this);
}

inline bool BackwardWriter::Write(int src) {
  return write_int_internal::WriteSigned(src, *this);
}

inline bool BackwardWriter::Write(unsigned src) {
  return write_int_internal::WriteUnsigned(src, *this);
}

inline bool BackwardWriter::Write(long src) {
  return write_int_internal::WriteSigned(src, *this);
}

inline bool BackwardWriter::Write(unsigned long src) {
  return write_int_internal::WriteUnsigned(src, *this);
}

inline bool BackwardWriter::Write(long long src) {
  return write_int_internal::WriteSigned(src, *this);
}

inline bool BackwardWriter::Write(unsigned long long src) {
  return write_int_internal::WriteUnsigned(src, *this);
}

inline bool BackwardWriter::Write(absl::int128 src) {
  return write_int_internal::WriteSigned(src, *this);
}

inline bool BackwardWriter::Write(absl::uint128 src) {
  return write_int_internal::WriteUnsigned(src, *this);
}

template <
    typename Src,
    std::enable_if_t<
        absl::conjunction<
            HasAbslStringify<Src>,
            absl::negation<std::is_convertible<Src&&, absl::string_view>>,
            absl::negation<std::is_convertible<Src&&, absl::Span<const char>>>,
            absl::negation<std::is_convertible<Src&&, const Chain&>>,
            absl::negation<std::is_convertible<Src&&, const absl::Cord&>>>::
            value,
        int>>
inline bool BackwardWriter::Write(Src&& src) {
  RestrictedChainWriter chain_writer;
  WriterAbslStringifySink sink(&chain_writer);
  AbslStringify(sink, std::forward<Src>(src));
  if (ABSL_PREDICT_FALSE(!chain_writer.Close())) {
    return Fail(chain_writer.status());
  }
  return Write(std::move(chain_writer.dest()));
}

template <
    typename... Srcs,
    std::enable_if_t<
        absl::conjunction<std::integral_constant<bool, sizeof...(Srcs) != 1>,
                          IsStringifiable<Srcs>...>::value,
        int>>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool BackwardWriter::Write(Srcs&&... srcs) {
  return WriteTuple(std::forward_as_tuple(std::forward<Srcs>(srcs)...));
}

template <
    typename... Srcs,
    std::enable_if_t<absl::conjunction<IsStringifiable<Srcs>...>::value, int>>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool BackwardWriter::WriteTuple(
    const std::tuple<Srcs...>& srcs) {
  return WriteInternal<sizeof...(Srcs)>(srcs);
}

template <
    typename... Srcs,
    std::enable_if_t<absl::conjunction<IsStringifiable<Srcs>...>::value, int>>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool BackwardWriter::WriteTuple(
    std::tuple<Srcs...>&& srcs) {
  return WriteInternal<sizeof...(Srcs)>(std::move(srcs));
}

inline bool BackwardWriter::WriteZeros(Position length) {
  if (ABSL_PREDICT_TRUE(available() >= length && length <= kMaxBytesToCopy)) {
    // `std::memset(nullptr, _, 0)` is undefined.
    if (ABSL_PREDICT_TRUE(length > 0)) {
      move_cursor(IntCast<size_t>(length));
      std::memset(cursor(), 0, IntCast<size_t>(length));
    }
    return true;
  }
  AssertInitialized(cursor(), start_to_cursor());
  return WriteZerosSlow(length);
}

inline bool BackwardWriter::WriteChars(Position length, char src) {
  if (ABSL_PREDICT_TRUE(available() >= length && length <= kMaxBytesToCopy)) {
    // `std::memset(nullptr, _, 0)` is undefined.
    if (ABSL_PREDICT_TRUE(length > 0)) {
      move_cursor(IntCast<size_t>(length));
      std::memset(cursor(), src, IntCast<size_t>(length));
    }
    return true;
  }
  AssertInitialized(cursor(), start_to_cursor());
  return WriteCharsSlow(length, src);
}

inline bool BackwardWriter::WriteBytes(Position length, uint8_t src) {
  return WriteChars(length, static_cast<char>(src));
}

inline bool BackwardWriter::Flush(FlushType flush_type) {
  AssertInitialized(cursor(), start_to_cursor());
  return FlushImpl(flush_type);
}

inline Position BackwardWriter::pos() const {
  RIEGELI_ASSERT_LE(start_pos_,
                    std::numeric_limits<Position>::max() - start_to_limit())
      << "Failed invariant of BackwardWriter: "
         "position of buffer limit overflow";
  return start_pos_ + start_to_cursor();
}

inline Position BackwardWriter::limit_pos() const {
  RIEGELI_ASSERT_LE(start_pos_,
                    std::numeric_limits<Position>::max() - start_to_limit())
      << "Failed invariant of BackwardWriter: "
         "position of buffer limit overflow";
  return start_pos_ + start_to_limit();
}

inline void BackwardWriter::move_start_pos(Position length) {
  RIEGELI_ASSERT_LE(length, std::numeric_limits<Position>::max() - start_pos_)
      << "Failed precondition of BackwardWriter::move_start_pos(): "
         "position out of range";
  start_pos_ += length;
}

inline void BackwardWriter::set_start_pos(Position start_pos) {
  start_pos_ = start_pos;
}

inline bool BackwardWriter::Truncate(Position new_size) {
  AssertInitialized(cursor(), start_to_cursor());
  return TruncateImpl(new_size);
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_BACKWARD_WRITER_H_
