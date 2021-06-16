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

#ifndef RIEGELI_BYTES_WRITER_H_
#define RIEGELI_BYTES_WRITER_H_

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
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"

namespace riegeli {

// Abstract class `Writer` writes sequences of bytes to a destination. The
// nature of the destination depends on the particular class derived from
// `Writer`.
//
// A `Writer` object manages a buffer of data to be pushed to the destination,
// which amortizes the overhead of pushing data over multiple writes. Data can
// be written directly into the buffer, and classes derived from `Writer` can
// avoid copying by allocating the buffer in a way which fits the destination,
// e.g. pointing it to a fragment of the destination itself.
//
// All `Writer`s support writing data sequentially and querying for the current
// position. Some `Writer`s also support random access: changing the position
// for subsequent operations and querying for the total size of the destination.
// Some `Writer`s also support truncation; this is implied by supporting random
// access.
//
// A `Writer` must be explicitly closed, and `Close()` must succeed, in order
// for its output to be available in the destination.
class Writer : public Object {
 public:
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
  //  * `false` - failure (`available() < min_length`, `!healthy()`)
  bool Push(size_t min_length = 1, size_t recommended_length = 0);

  // Buffer pointers. Space between `start()` and `limit()` is available for
  // immediate writing data to it, with `cursor()` pointing to the current
  // position.
  //
  // Invariants:
  //   `start() <= cursor() <= limit()` (possibly all `nullptr`)
  //   if `!healthy()` then `start() == cursor() == limit() == nullptr`
  char* start() const { return start_; }
  char* cursor() const { return cursor_; }
  char* limit() const { return limit_; }

  // Increments the value of `cursor()`. Call this during writing data under
  // `cursor()` to indicate how much was written.
  //
  // Precondition: `length <= available()`
  void move_cursor(size_t length);

  // Sets the value of `cursor()`. Call this during writing data under
  // `cursor()` to indicate how much was written, or to seek within the buffer.
  //
  // Preconditions: `start() <= cursor <= limit()`
  void set_cursor(char* cursor);

  // Returns the amount of space available in the buffer, between `cursor()` and
  // `limit()`.
  //
  // Invariant: if `!healthy()` then `available() == 0`
  size_t available() const { return PtrDistance(cursor_, limit_); }

  // Returns the buffer size, between `start()` and `limit()`.
  //
  // Invariant: if `!healthy()` then `buffer_size() == 0`
  size_t buffer_size() const { return PtrDistance(start_, limit_); }

  // Returns the amount of data written to the buffer, between `start()` and
  // `cursor()`.
  //
  // Invariant: if `!healthy()` then `written_to_buffer() == 0`
  size_t written_to_buffer() const { return PtrDistance(start_, cursor_); }

  // Writes a single byte to the buffer or the destination.
  //
  // Return values:
  //  * `true`  - success (`healthy()`)
  //  * `false` - failure (`!healthy()`)
  bool WriteChar(char data);
  bool WriteByte(uint8_t data);

  // Writes a fixed number of bytes from `src` to the buffer and/or the
  // destination.
  //
  // `std::string&&` is accepted with a template to avoid implicit conversions
  // to `std::string` which can be ambiguous against `absl::string_view`
  // (e.g. `const char*`).
  //
  // Return values:
  //  * `true`  - success (`src.size()` bytes written)
  //  * `false` - failure (less than `src.size()` bytes written, `!healthy()`)
  bool Write(absl::string_view src);
  template <typename Src,
            std::enable_if_t<std::is_same<Src, std::string>::value, int> = 0>
  bool Write(Src&& src);
  bool Write(const Chain& src);
  bool Write(Chain&& src);
  bool Write(const absl::Cord& src);
  bool Write(absl::Cord&& src);

  // Writes the given number of zero bytes to the buffer and/or the destination.
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
  // Many classes derived from `Writer` accept a `size_hint` in their `Options`,
  // which serves a similar purpose, but `WriteHint()` can be used later and can
  // be applied to any `Writer`.
  void WriteHint(size_t length);

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
  //  * `true`  - success (`healthy()`)
  //  * `false` - failure (`!healthy()`)
  bool Flush(FlushType flush_type = FlushType::kFromProcess);

  // Returns the current position.
  //
  // This is not necessarily 0 after creating the `Writer` if it appends to a
  // destination with existing contents, or if the `Writer` wraps another writer
  // or output stream propagating its position.
  //
  // This may decrease when the `Writer` becomes unhealthy (due to buffering,
  // previously written but unflushed data may be lost).
  //
  // `pos()` is unchanged by a successful `Close()`.
  Position pos() const;

  // Returns true if this Writer supports `Seek()`, `Size()`, and `Truncate()`.
  //
  // Invariant: if `SupportsRandomAccess()` then `SupportsTruncate()`
  virtual bool SupportsRandomAccess() { return false; }

  // Sets the current position for subsequent operations.
  //
  // Return values:
  //  * `true`                      - success (position is set to `new_pos`)
  //  * `false` (when `healthy()`)  - destination ends before `new_pos`
  //                                  (position is set to end)
  //  * `false` (when `!healthy()`) - failure
  bool Seek(Position new_pos);

  // Returns the size of the destination, i.e. the position corresponding to its
  // end.
  //
  // Returns `absl::nullopt` on failure (`!healthy()`).
  virtual absl::optional<Position> Size();

  // Returns `true` if this `Writer` supports `Truncate()`.
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
  // Creates a `Writer` with the given initial state.
  explicit Writer(InitiallyClosed) noexcept : Object(kInitiallyClosed) {}
  explicit Writer(InitiallyOpen) noexcept : Object(kInitiallyOpen) {}

  // Moves the part of the object defined in this class.
  //
  // Buffer pointers do not need to satisfy their invariants during this part of
  // the move, here they are merely exchanged with `nullptr` and copied.
  Writer(Writer&& that) noexcept;
  Writer& operator=(Writer&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `Writer`. This avoids
  // constructing a temporary `Writer` and moving from it. Derived classes which
  // redefine `Reset()` should include a call to `Write::Reset()`.
  void Reset(InitiallyClosed);
  void Reset(InitiallyOpen);

  // `Writer` overrides `Object::Done()` to set buffer pointers to `nullptr`.
  // Derived classes which override it further should include a call to
  // `Writer::Done()`.
  void Done() override;

  // `Writer` overrides `Object::AnnotateFailure()` to annotate the status with
  // the current position.
  ABSL_ATTRIBUTE_COLD void AnnotateFailure(absl::Status& status) override;

  // `Writer` overrides `Object::OnFail()` to set buffer pointers to `nullptr`.
  // Derived classes which override it further should include a call to
  // `Writer::OnFail()`.
  //
  // `pos()` decreases by `written_to_buffer()` to indicate that any buffered
  // data have been lost.
  ABSL_ATTRIBUTE_COLD void OnFail() override;

  // Marks the `Writer` as failed with message "Writer position overflow".
  // Always returns `false`.
  //
  // This can be called if the destination would exceed its maximum possible
  // size or if `start_pos()` would overflow.
  ABSL_ATTRIBUTE_COLD bool FailOverflow();

  // Implementation of the slow part of `Push()`.
  //
  // Precondition: `available() < min_length`
  virtual bool PushSlow(size_t min_length, size_t recommended_length) = 0;

  // Sets the values of:
  //  * `start()` - to `start`
  //  * `cursor()` - to `start + written_to_buffer`
  //  * `limit()` - to `start + buffer_size`
  //
  // Preconditions:
  //   [`start`, `start + buffer_size`) is a valid byte range
  //   `written_to_buffer <= buffer_size`
  void set_buffer(char* start = nullptr, size_t buffer_size = 0,
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

  // Implementation of `Flush()`, except that the parameter is not defaulted,
  // which is problematic for virtual functions.
  //
  // By default does nothing and returns `healthy()`.
  virtual bool FlushImpl(FlushType flush_type);

  // Destination position corresponding to `start()`.
  Position start_pos() const { return start_pos_; }

  // Destination position corresponding to `limit()`.
  Position limit_pos() const;

  // Increments the value of `start_pos()`.
  void move_start_pos(Position length);

  // Sets the value of `start_pos()`.
  void set_start_pos(Position start_pos);

  // Implementation of `Seek()`.
  virtual bool SeekImpl(Position new_pos);

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

inline Writer::Writer(Writer&& that) noexcept
    : Object(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      start_(std::exchange(that.start_, nullptr)),
      cursor_(std::exchange(that.cursor_, nullptr)),
      limit_(std::exchange(that.limit_, nullptr)),
      start_pos_(std::exchange(that.start_pos_, 0)) {}

inline Writer& Writer::operator=(Writer&& that) noexcept {
  Object::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  start_ = std::exchange(that.start_, nullptr);
  cursor_ = std::exchange(that.cursor_, nullptr);
  limit_ = std::exchange(that.limit_, nullptr);
  start_pos_ = std::exchange(that.start_pos_, 0);
  return *this;
}

inline void Writer::Reset(InitiallyClosed) {
  Object::Reset(kInitiallyClosed);
  start_ = nullptr;
  cursor_ = nullptr;
  limit_ = nullptr;
  start_pos_ = 0;
}

inline void Writer::Reset(InitiallyOpen) {
  Object::Reset(kInitiallyOpen);
  start_ = nullptr;
  cursor_ = nullptr;
  limit_ = nullptr;
  start_pos_ = 0;
}

inline void Writer::Done() {
  start_pos_ = pos();
  set_buffer();
}

inline bool Writer::Push(size_t min_length, size_t recommended_length) {
  if (ABSL_PREDICT_TRUE(available() >= min_length)) return true;
  if (ABSL_PREDICT_FALSE(!PushSlow(min_length, recommended_length))) {
    return false;
  }
  RIEGELI_ASSERT_GE(available(), min_length)
      << "Failed postcondition of Writer::PushSlow(): "
         "not enough space available";
  return true;
}

inline void Writer::move_cursor(size_t length) {
  RIEGELI_ASSERT_LE(length, available())
      << "Failed precondition of Writer::move_cursor(): length out of range";
  cursor_ += length;
}

inline void Writer::set_cursor(char* cursor) {
  RIEGELI_ASSERT(cursor >= start())
      << "Failed precondition of Writer::set_cursor(): pointer out of range";
  RIEGELI_ASSERT(cursor <= limit())
      << "Failed precondition of Writer::set_cursor(): pointer out of range";
  cursor_ = cursor;
}

inline void Writer::set_buffer(char* start, size_t buffer_size,
                               size_t written_to_buffer) {
  RIEGELI_ASSERT_LE(written_to_buffer, buffer_size)
      << "Failed precondition of Writer::set_buffer(): length out of range";
  start_ = start;
  cursor_ = start + written_to_buffer;
  limit_ = start + buffer_size;
}

inline bool Writer::WriteChar(char data) {
  if (ABSL_PREDICT_FALSE(!Push())) return false;
  *cursor() = data;
  move_cursor(1);
  return true;
}

inline bool Writer::WriteByte(uint8_t data) {
  return WriteChar(static_cast<char>(data));
}

inline bool Writer::Write(absl::string_view src) {
  if (ABSL_PREDICT_TRUE(available() >= src.size())) {
    if (ABSL_PREDICT_TRUE(
            // `std::memcpy(nullptr, _, 0)` and `std::memcpy(_, nullptr, 0)`
            // are undefined.
            !src.empty())) {
      std::memcpy(cursor(), src.data(), src.size());
      move_cursor(src.size());
    }
    return true;
  }
  return WriteSlow(src);
}

template <typename Src,
          std::enable_if_t<std::is_same<Src, std::string>::value, int>>
inline bool Writer::Write(Src&& src) {
  if (ABSL_PREDICT_TRUE(src.size() <= kMaxBytesToCopy)) {
    return Write(absl::string_view(src));
  } else {
    // `std::move(src)` is correct and `std::forward<Src>(src)` is not
    // necessary: `Src` is always `std::string`, never an lvalue reference.
    return WriteSlow(Chain(std::move(src)));
  }
}

inline bool Writer::Write(const Chain& src) {
  if (ABSL_PREDICT_TRUE(available() >= src.size() &&
                        src.size() <= kMaxBytesToCopy)) {
    src.CopyTo(cursor());
    move_cursor(src.size());
    return true;
  }
  return WriteSlow(src);
}

inline bool Writer::Write(Chain&& src) {
  if (ABSL_PREDICT_TRUE(available() >= src.size() &&
                        src.size() <= kMaxBytesToCopy)) {
    src.CopyTo(cursor());
    move_cursor(src.size());
    return true;
  }
  return WriteSlow(std::move(src));
}

inline bool Writer::Write(const absl::Cord& src) {
  if (ABSL_PREDICT_TRUE(available() >= src.size() &&
                        src.size() <= kMaxBytesToCopy)) {
    char* dest = cursor();
    for (absl::string_view fragment : src.Chunks()) {
      std::memcpy(dest, fragment.data(), fragment.size());
      dest += fragment.size();
    }
    set_cursor(dest);
    return true;
  }
  return WriteSlow(src);
}

inline bool Writer::Write(absl::Cord&& src) {
  if (ABSL_PREDICT_TRUE(available() >= src.size() &&
                        src.size() <= kMaxBytesToCopy)) {
    char* dest = cursor();
    for (absl::string_view fragment : src.Chunks()) {
      std::memcpy(dest, fragment.data(), fragment.size());
      dest += fragment.size();
    }
    set_cursor(dest);
    return true;
  }
  return WriteSlow(std::move(src));
}

inline bool Writer::WriteZeros(Position length) {
  if (ABSL_PREDICT_TRUE(available() >= length && length <= kMaxBytesToCopy)) {
    if (ABSL_PREDICT_TRUE(
            // `std::memset(nullptr, _, 0)` is undefined.
            length > 0)) {
      std::memset(cursor(), 0, IntCast<size_t>(length));
      move_cursor(IntCast<size_t>(length));
    }
    return true;
  }
  return WriteZerosSlow(length);
}

inline void Writer::WriteHint(size_t length) {
  if (ABSL_PREDICT_TRUE(available() >= length)) return;
  WriteHintSlow(length);
}

inline bool Writer::Flush(FlushType flush_type) {
  return FlushImpl(flush_type);
}

inline Position Writer::pos() const {
  RIEGELI_ASSERT_LE(start_pos_,
                    std::numeric_limits<Position>::max() - buffer_size())
      << "Failed invariant of Writer: position of buffer limit overflow";
  return start_pos_ + written_to_buffer();
}

inline Position Writer::limit_pos() const {
  RIEGELI_ASSERT_LE(start_pos_,
                    std::numeric_limits<Position>::max() - buffer_size())
      << "Failed invariant of Writer: position of buffer limit overflow";
  return start_pos_ + buffer_size();
}

inline void Writer::move_start_pos(Position length) {
  RIEGELI_ASSERT_LE(length, std::numeric_limits<Position>::max() - start_pos_)
      << "Failed precondition of Writer::move_start_pos(): "
         "position out of range";
  start_pos_ += length;
}

inline void Writer::set_start_pos(Position start_pos) {
  start_pos_ = start_pos;
}

inline bool Writer::Seek(Position new_pos) { return SeekImpl(new_pos); }

}  // namespace riegeli

#endif  // RIEGELI_BYTES_WRITER_H_
