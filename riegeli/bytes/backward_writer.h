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
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"

namespace riegeli {

// Abstract class `BackwardWriter` writes sequences of bytes to a destination,
// like `Writer`, but back to front.
//
// Sequential writing is supported, random access is not supported, truncation
// is optionally supported.
class BackwardWriter : public Object {
 public:
  // `BackwardWriter` overrides `Object::Fail()` to set buffer pointers to
  // `nullptr` and annotate the status with the current position. Derived
  // classes which override it further should include a call to
  // `BackwardWriter::Fail()`.
  //
  // `pos()` decreases by `written_to_buffer()` to indicate that any buffered
  // data have been lost.
  using Object::Fail;
  ABSL_ATTRIBUTE_COLD bool Fail(absl::Status status) override;

  // Ensures that enough space is available for writing: pushes previously
  // written data to the destination, and points `cursor()` and `limit()` to
  // space with length at least `min_length`, preferably `recommended_length`.
  // If enough space was already available, does nothing.
  //
  // If `recommended_length < min_length`, `recommended_length` is assumed to be
  // `min_length`.
  //
  // Return values:
  //  * `true`  - success (`available() >= min_length`)
  //  * `false` - failure (`available() < min_length`, `!healthy()`)
  bool Push(size_t min_length = 1, size_t recommended_length = 0);

  // Buffer pointers. Space between `start()` (exclusive upper bound) and
  // `limit()` (inclusive lower bound) is available for writing data to it, with
  // `cursor()` pointing to the current position going downwards (past the next
  // byte to write).
  //
  // Invariants:
  //   `start() >= cursor() >= limit()` (possibly all `nullptr`)
  //   if `!healthy()` then `start() == cursor() == limit() == nullptr`
  char* start() const { return start_; }
  char* cursor() const { return cursor_; }
  char* limit() const { return limit_; }

  // Decrements the value of `cursor()`. Call this during writing data under
  // `cursor()` to indicate how much was written.
  //
  // Precondition: `length <= available()`
  void move_cursor(size_t length);

  // Updates the value of `cursor()`. Call this during writing data under
  // `cursor()` to indicate how much was written, or to seek within the buffer.
  //
  // Precondition: `start() >= cursor >= limit()`
  void set_cursor(char* cursor);

  // Returns the amount of space available in the buffer, between `cursor()` and
  // `limit()`.
  //
  // Invariant: if `!healthy()` then `available() == 0`
  size_t available() const { return PtrDistance(limit_, cursor_); }

  // Returns the buffer size, between `start()` and `limit()`.
  //
  // Invariant: if `!healthy()` then `buffer_size() == 0`
  size_t buffer_size() const { return PtrDistance(limit_, start_); }

  // Returns the amount of data written to the buffer, between `start()` and
  // `cursor()`.
  //
  // Invariant: if `!healthy()` then `written_to_buffer() == 0`
  size_t written_to_buffer() const { return PtrDistance(cursor_, start_); }

  // Writes a single byte.
  //
  // Return values:
  //  * `true`  - success (`healthy()`)
  //  * `false` - failure (`!healthy()`)
  bool WriteChar(char data);
  bool WriteByte(uint8_t data);

  // Prepends a fixed number of bytes from `src` to the buffer, pushing data to
  // the destination as needed.
  //
  // `std::string&&` is accepted with a template to avoid implicit conversions
  // to `std::string` which can be ambiguous against `absl::string_view`
  // (e.g. `const char*`).
  //
  // Return values:
  //  * `true`  - success (`src.size()` bytes written)
  //  * `false` - failure (a suffix of less than `src.size()` bytes written,
  //                       `!healthy()`)
  bool Write(absl::string_view src);
  template <typename Src,
            std::enable_if_t<std::is_same<Src, std::string>::value, int> = 0>
  bool Write(Src&& src);
  bool Write(const Chain& src);
  bool Write(Chain&& src);
  bool Write(const absl::Cord& src);
  bool Write(absl::Cord&& src);

  // Writes the given number of zero bytes.
  //
  // Return values:
  //  * `true`  - success (`length` bytes written)
  //  * `false` - failure (less than `length` bytes written, `!healthy()`)
  bool WriteZeros(Position length);

  // If `true`, a hint that there is no benefit in preparing a `Chain` or
  // `absl::Cord` for writing instead of `absl::string_view`, e.g. because
  // `WriteSlow(const Chain&)` and `WriteSlow(const absl::Cord&)` are
  // implemented in terms of `WriteSlow(absl::string_view)` anyway.
  virtual bool PrefersCopying() const { return false; }

  // Hints that several consecutive `Push()` or `Write()` calls will follow,
  // writing this amount of data in total.
  //
  // This can make these calls faster by preparing a larger internal buffer for
  // all the data, and then pushing the data at once to the destination.
  //
  // Many classes derived from `BackwardWriter` accept a `size_hint` in their
  // `Options`, which serves a similar purpose, but `WriteHint()` can be used
  // later and can be applied to any `BackwardWriter`.
  void WriteHint(size_t length);

  // Pushes data written between `start()` and `cursor()` to the destination.
  //
  // Additionally, attempts to ensure the following, depending on `flush_type`
  // (without a guarantee though):
  //  * `FlushType::kFromObject`  - flushes the destination too if it is owned
  //  * `FlushType::kFromProcess` - data survives process crash
  //  * `FlushType::kFromMachine` - data survives operating system crash
  //
  // The precise meaning of `Flush()` depends on the particular
  // `BackwardWriter`. The intent is to make data written so far visible, but in
  // contrast to `Close()`, keeping the possibility to write more data later.
  //
  // Return values:
  //  * `true ` - success (`healthy()`)
  //  * `false` - failure (`!healthy()`)
  virtual bool Flush(FlushType flush_type) = 0;

  // Returns the current position (increasing as data are prepended).
  //
  // This is not necessarily 0 after creating the `BackwardWriter` if it
  // prepends to a destination with existing contents, or if the
  // `BackwardWriter` wraps another writer or output stream propagating its
  // position.
  //
  // This may decrease when the `BackwardWriter` becomes unhealthy (due to
  // buffering, previously written but unflushed data may be lost).
  //
  // `pos()` is unchanged by a successful `Close()`.
  Position pos() const;

  // Returns `true` if this `BackwardWriter` supports `Truncate()`.
  virtual bool SupportsTruncate() { return false; }

  // Discards the part of the destination after the given position. Sets the
  // current position to the new end.
  //
  // Return values:
  //  * `true`                      - success
  //                                  (destination truncated, `healthy()`)
  //  * `false` (when `healthy()`)  - destination is smaller than `new_size`
  //                                  (position is set to end)
  //  * `false` (when `!healthy()`) - failure
  virtual bool Truncate(Position new_size);

 protected:
  // Creates a `BackwardWriter` with the given initial state.
  explicit BackwardWriter(InitiallyClosed) noexcept
      : Object(kInitiallyClosed) {}
  explicit BackwardWriter(InitiallyOpen) noexcept : Object(kInitiallyOpen) {}

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
  void Reset(InitiallyClosed);
  void Reset(InitiallyOpen);

  // `BackwardWriter` overrides `Object::Done()` to set buffer pointers to
  // `nullptr`. Derived classes which override it further should include a call
  // to `BackwardWriter::Done()`.
  void Done() override;

  // Exposes a `Fail()` override which does not annotate the status with the
  // current position, unlike the public `BackwardWriter::Fail()`.
  ABSL_ATTRIBUTE_COLD bool FailWithoutAnnotation(absl::Status status);
  ABSL_ATTRIBUTE_COLD bool FailWithoutAnnotation(const Object& dependency);

  // Marks the `BackwardWriter` as failed with message
  // "BackwardWriter position overflow". Always returns `false`.
  //
  // This can be called if the destination would exceed its maximum possible
  // size or if `start_pos()` would overflow.
  ABSL_ATTRIBUTE_COLD bool FailOverflow();

  // Implementation of the slow part of `Push()`.
  //
  // Precondition: `available() < min_length`
  virtual bool PushSlow(size_t min_length, size_t recommended_length) = 0;

  // Sets the values of:
  //  * `start()` - to `limit + buffer_size`
  //  * `cursor()` - to `start() - written_to_buffer`
  //  * `limit()` - to `limit`
  //
  // Preconditions:
  //   [`limit`, `limit + buffer_size`) is a valid byte range
  //   `written_to_buffer <= buffer_size`
  void set_buffer(char* limit = nullptr, size_t buffer_size = 0,
                  size_t written_to_buffer = 0);

  // Implementation of the slow part of `Write()`.
  //
  // By default `WriteSlow(absl::string_view)` is implemented in terms of
  // `Push()`; `WriteSlow(const Chain&)` and `WriteSlow(const absl::Cord&)` are
  // implemented in terms of `WriteSlow(absl::string_view)`;
  // `WriteSlow(Chain&&)` is implemented in terms of `WriteSlow(const Chain&)`;
  // and `WriteSlow(absl::Cord&&)` is implemented in terms of
  // `WriteSlow(const absl::Cord&)`;
  //
  // Precondition for `WriteSlow(absl::string_view)`:
  //   `available() < src.size()`
  //
  // Precondition for `WriteSlow(const Chain&)`, `WriteSlow(Chain&&)`,
  // `WriteSlow(const absl::Cord&)`, and `WriteSlow(absl::Cord&&):
  //   `UnsignedMin(available(), kMaxBytesToCopy) < src.size()`
  virtual bool WriteSlow(absl::string_view src);
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

  // Implementation of the slow part of `WriteHint()`.
  //
  // By default does nothing.
  //
  // Precondition: `available() < length`
  virtual void WriteHintSlow(size_t length);

  // Destination position corresponding to `start()`.
  Position start_pos() const { return start_pos_; }

  // Destination position corresponding to `limit()`.
  Position limit_pos() const;

  // Increments the value of `start_pos()`.
  void move_start_pos(Position length);

  // Sets the value of `start_pos()`.
  void set_start_pos(Position start_pos);

 private:
  char* start_ = nullptr;
  char* cursor_ = nullptr;
  char* limit_ = nullptr;

  // Destination position corresponding to `start_`.
  //
  // Invariant:
  //   `start_pos_ <= std::numeric_limits<Position>::max() - buffer_size()`
  Position start_pos_ = 0;
};

// Implementation details follow.

inline BackwardWriter::BackwardWriter(BackwardWriter&& that) noexcept
    : Object(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      start_(std::exchange(that.start_, nullptr)),
      cursor_(std::exchange(that.cursor_, nullptr)),
      limit_(std::exchange(that.limit_, nullptr)),
      start_pos_(std::exchange(that.start_pos_, 0)) {}

inline BackwardWriter& BackwardWriter::operator=(
    BackwardWriter&& that) noexcept {
  Object::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  start_ = std::exchange(that.start_, nullptr);
  cursor_ = std::exchange(that.cursor_, nullptr);
  limit_ = std::exchange(that.limit_, nullptr);
  start_pos_ = std::exchange(that.start_pos_, 0);
  return *this;
}

inline void BackwardWriter::Reset(InitiallyClosed) {
  Object::Reset(kInitiallyClosed);
  start_ = nullptr;
  cursor_ = nullptr;
  limit_ = nullptr;
  start_pos_ = 0;
}

inline void BackwardWriter::Reset(InitiallyOpen) {
  Object::Reset(kInitiallyOpen);
  start_ = nullptr;
  cursor_ = nullptr;
  limit_ = nullptr;
  start_pos_ = 0;
}

inline void BackwardWriter::Done() {
  start_pos_ = pos();
  set_buffer();
}

inline bool BackwardWriter::Push(size_t min_length, size_t recommended_length) {
  if (ABSL_PREDICT_TRUE(available() >= min_length)) return true;
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

inline void BackwardWriter::set_buffer(char* limit, size_t buffer_size,
                                       size_t written_to_buffer) {
  RIEGELI_ASSERT_LE(written_to_buffer, buffer_size)
      << "Failed precondition of BackwardWriter::set_buffer(): "
         "length out of range";
  start_ = limit + buffer_size;
  cursor_ = start_ - written_to_buffer;
  limit_ = limit;
}

inline bool BackwardWriter::WriteChar(char data) {
  if (ABSL_PREDICT_FALSE(!Push())) return false;
  move_cursor(1);
  *cursor() = data;
  return true;
}

inline bool BackwardWriter::WriteByte(uint8_t data) {
  return WriteChar(static_cast<char>(data));
}

inline bool BackwardWriter::Write(absl::string_view src) {
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
  return WriteSlow(src);
}

template <typename Src,
          std::enable_if_t<std::is_same<Src, std::string>::value, int>>
inline bool BackwardWriter::Write(Src&& src) {
  if (ABSL_PREDICT_TRUE(src.size() <= kMaxBytesToCopy)) {
    return Write(absl::string_view(src));
  } else {
    // `std::move(src)` is correct and `std::forward<Src>(src)` is not
    // necessary: `Src` is always `std::string`, never an lvalue reference.
    return WriteSlow(Chain(std::move(src)));
  }
}

inline bool BackwardWriter::Write(const Chain& src) {
  if (ABSL_PREDICT_TRUE(available() >= src.size() &&
                        src.size() <= kMaxBytesToCopy)) {
    move_cursor(src.size());
    src.CopyTo(cursor());
    return true;
  }
  return WriteSlow(src);
}

inline bool BackwardWriter::Write(Chain&& src) {
  if (ABSL_PREDICT_TRUE(available() >= src.size() &&
                        src.size() <= kMaxBytesToCopy)) {
    move_cursor(src.size());
    src.CopyTo(cursor());
    return true;
  }
  return WriteSlow(std::move(src));
}

inline bool BackwardWriter::Write(const absl::Cord& src) {
  if (ABSL_PREDICT_TRUE(available() >= src.size() &&
                        src.size() <= kMaxBytesToCopy)) {
    move_cursor(src.size());
    char* dest = cursor();
    for (absl::string_view fragment : src.Chunks()) {
      std::memcpy(dest, fragment.data(), fragment.size());
      dest += fragment.size();
    }
    return true;
  }
  return WriteSlow(src);
}

inline bool BackwardWriter::WriteZeros(Position length) {
  if (ABSL_PREDICT_TRUE(available() >= length && length <= kMaxBytesToCopy)) {
    if (ABSL_PREDICT_TRUE(
            // `std::memset(nullptr, _, 0)` is undefined.
            length > 0)) {
      move_cursor(IntCast<size_t>(length));
      std::memset(cursor(), 0, IntCast<size_t>(length));
    }
    return true;
  }
  return WriteZerosSlow(length);
}

inline bool BackwardWriter::Write(absl::Cord&& src) {
  if (ABSL_PREDICT_TRUE(available() >= src.size() &&
                        src.size() <= kMaxBytesToCopy)) {
    move_cursor(src.size());
    char* dest = cursor();
    for (absl::string_view fragment : src.Chunks()) {
      std::memcpy(dest, fragment.data(), fragment.size());
      dest += fragment.size();
    }
    return true;
  }
  return WriteSlow(std::move(src));
}

inline void BackwardWriter::WriteHint(size_t length) {
  if (ABSL_PREDICT_TRUE(available() >= length)) return;
  WriteHintSlow(length);
}

inline Position BackwardWriter::pos() const {
  RIEGELI_ASSERT_LE(start_pos_,
                    std::numeric_limits<Position>::max() - buffer_size())
      << "Failed invariant of BackwardWriter: "
         "position of buffer limit overflow";
  return start_pos_ + written_to_buffer();
}

inline Position BackwardWriter::limit_pos() const {
  RIEGELI_ASSERT_LE(start_pos_,
                    std::numeric_limits<Position>::max() - buffer_size())
      << "Failed invariant of BackwardWriter: "
         "position of buffer limit overflow";
  return start_pos_ + buffer_size();
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

}  // namespace riegeli

#endif  // RIEGELI_BYTES_BACKWARD_WRITER_H_
