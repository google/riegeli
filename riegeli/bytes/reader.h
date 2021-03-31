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

#ifndef RIEGELI_BYTES_READER_H_
#define RIEGELI_BYTES_READER_H_

#include <stddef.h>
#include <stdint.h>

#include <cstring>
#include <limits>
#include <string>
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
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Abstract class `Reader` reads sequences of bytes from a source. The nature of
// the source depends on the particular class derived from `Reader`.
//
// A `Reader` object manages a buffer of data pulled from the source, which
// amortizes the overhead of pulling data over multiple reads. Data can be read
// directly from the buffer, and classes derived from `Reader` can avoid copying
// by allocating the buffer in a way which fits the source, e.g. pointing it to
// a fragment of the source itself.
//
// All `Reader`s support reading data sequentially and querying for the current
// position. Some `Reader`s also support random access: changing the position
// backwards for subsequent operations and querying for the total size of the
// source.
class Reader : public Object {
 public:
  // Verifies that the source ends at the current position, failing the `Reader`
  // with an `absl::DataLossError()` if not. Closes the `Reader`.
  //
  // Return values:
  //  * `true`  - success (the source ends at the former current position)
  //  * `false` - failure (the source does not end at the former current
  //                       position or the `Reader` was not healthy before
  //                       closing)
  bool VerifyEndAndClose();

  // Verifies that the source ends at the current position, failing the `Reader`
  // with an `absl::DataLossError()` if not.
  virtual void VerifyEnd();

  // `Reader` overrides `Object::Fail()` to annotate the status with the current
  // position. Derived classes which override it further should include a call
  // to `Reader::Fail()`.
  using Object::Fail;
  ABSL_ATTRIBUTE_COLD bool Fail(absl::Status status) override;

  // Ensures that enough data are available for reading: pulls more data from
  // the source, and points `cursor()` and `limit()` to data with length at
  // least `min_length`, preferably `recommended_length`. If enough data were
  // already available, does nothing.
  //
  // If `recommended_length < min_length`, `recommended_length` is assumed to be
  // `min_length`.
  //
  // Return values:
  //  * `true`                      - success (`available() >= min_length`)
  //  * `false` (when `healthy()`)  - source ends (`available() < min_length`)
  //  * `false` (when `!healthy()`) - failure (`available() < min_length`)
  bool Pull(size_t min_length = 1, size_t recommended_length = 0);

  // Buffer pointers. Data between `start()` and `limit()` are available for
  // reading, with `cursor()` pointing to the current position.
  //
  // Invariants:
  //   `start() <= cursor() <= limit()` (possibly all `nullptr`)
  //   if `!is_open()` then `start() == cursor() == limit() == nullptr`
  const char* start() const { return start_; }
  const char* cursor() const { return cursor_; }
  const char* limit() const { return limit_; }

  // Increments the value of `cursor()`. Call this during reading data under
  // `cursor()` to indicate how much was read.
  //
  // Precondition: `length <= available()`
  void move_cursor(size_t length);

  // Sets the value of `cursor()`. Call this during reading data under
  // `cursor()` to indicate how much was read, or to seek within the buffer.
  //
  // Precondition: `start() <= cursor <= limit()`
  void set_cursor(const char* cursor);

  // Returns the amount of data available in the buffer, between `cursor()` and
  // `limit()`.
  //
  // It is possible that a `Reader` has a looming failure:
  // `!healthy() && available() > 0`. This means that the source failed but some
  // data are already buffered and can be read before experiencing the failure.
  //
  // Invariant: if `!is_open()` then `available() == 0`
  size_t available() const { return PtrDistance(cursor_, limit_); }

  // Returns the buffer size, between `start()` and `limit()`.
  size_t buffer_size() const { return PtrDistance(start_, limit_); }

  // Returns the amount of data read from the buffer, between `start()` and
  // `cursor()`.
  size_t read_from_buffer() const { return PtrDistance(start_, cursor_); }

  // Reads a single byte.
  //
  // Return values:
  //  * not `absl::nullopt`                 - success
  //  * `absl::nullopt` (when `healthy()`)  - source ends
  //  * `absl::nullopt` (when `!healthy()`) - failure
  absl::optional<char> ReadChar();
  absl::optional<uint8_t> ReadByte();

  // Reads a fixed number of bytes from the buffer to `dest`, pulling data from
  // the source as needed, clearing any existing data in `dest`.
  //
  // `Read(absl::string_view&)` points `dest` to an array holding the data. The
  // array is valid until the next non-const operation on the `Reader`.
  //
  // Precondition for `Read(std::string&)`:
  //   `length <= dest.max_size()`
  //
  // Return values:
  //  * `true`                      - success (`length` bytes read)
  //  * `false` (when `healthy()`)  - source ends
  //                                  (less than `length` bytes read)
  //  * `false` (when `!healthy()`) - failure (less than `length` bytes read)
  bool Read(size_t length, absl::string_view& dest);
  bool Read(size_t length, char* dest);
  bool Read(size_t length, std::string& dest);
  bool Read(size_t length, Chain& dest);
  bool Read(size_t length, absl::Cord& dest);

  // Reads a fixed number of bytes from the buffer to `dest`, pulling data from
  // the source as needed, appending to any existing data in `dest`.
  //
  // Precondition for `ReadAndAppend(std::string&)`:
  //   `length <= dest->max_size() - dest->size()`
  //
  // Precondition for `ReadAndAppend(Chain&)` and `ReadAndAppend(absl::Cord&)`:
  //   `length <= std::numeric_limits<size_t>::max() - dest->size()`
  //
  // Return values:
  //  * `true`                      - success (`length` bytes read)
  //  * `false` (when `healthy()`)  - source ends
  //                                  (less than `length` bytes read)
  //  * `false` (when `!healthy()`) - failure (less than `length` bytes read)
  bool ReadAndAppend(size_t length, std::string& dest);
  bool ReadAndAppend(size_t length, Chain& dest);
  bool ReadAndAppend(size_t length, absl::Cord& dest);

  // Reads a fixed number of bytes from the buffer to `*dest`, pulling data from
  // the source as needed.
  //
  // `CopyTo(Writer&)` writes as much as could be read if reading failed, and
  // reads an unspecified length (between what could be written and the
  // requested length) if writing failed.
  //
  // `CopyTo(BackwardWriter&)` writes nothing if reading failed, and reads the
  // full requested length even if writing failed.
  //
  // Return values:
  //  * `true`                                         - success (`length`
  //                                                     bytes copied)
  //  * `false` (when `dest.healthy() && healthy()`)   - source ends (less than
  //                                                     `length` bytes copied)
  //  * `false` (when `!dest.healthy() || !healthy()`) - failure (less than
  //                                                     `length` bytes copied)
  bool CopyTo(Position length, Writer& dest);
  bool CopyTo(size_t length, BackwardWriter& dest);

  // Hints that several consecutive `Pull()`, `Read()`, or `CopyTo()` calls will
  // follow, reading this amount of data in total.
  //
  // This can make these calls faster by by prefetching all the data at once
  // into an internal buffer.
  void ReadHint(size_t length);

  // Informs the source that data between `start()` and `cursor()` have been
  // read.
  //
  // The precise meaning of `Sync()` depends on the particular `Reader`. The
  // intent is to propagate the current position to the source if the source
  // tracks the current position, but in contrast to `Close()`, keeping the
  // possibility to read more data later.
  //
  // Return values:
  //  * `true`  - success (`healthy()`)
  //  * `false` - failure (`!healthy()`)
  virtual bool Sync() { return healthy(); }

  // Returns the current position.
  //
  // This is often 0 after creating the `Reader`, but not necessarily if the
  // `Reader` wraps another reader or input stream propagating its position.
  //
  // `pos()` is unchanged by `Close()`.
  Position pos() const;

  // Returns `true` if this `Reader` supports `Seek()` backwards (`Seek()`
  // forwards is always supported) and `Size()`.
  //
  // Invariant: if `SupportsRandomAccess()` then `SupportsSize()`
  virtual bool SupportsRandomAccess() const { return false; }

  // Sets the current position for subsequent operations.
  //
  // Seeking to `new_pos >= pos() - read_from_buffer()` is always supported,
  // although if `SupportsRandomAccess()` is `false` then it is not expected
  // to be more efficient than reading and discarding the intervening data.
  // Seeking to `new_pos < pos() - read_from_buffer()` is supported only when
  // `SupportsRandomAccess()` is `true`.
  //
  // Return values:
  //  * `true`                      - success (position is set to `new_pos`)
  //  * `false` (when `healthy()`)  - source ends before `new_pos`
  //                                  (position is set to the end)
  //  * `false` (when `!healthy()`) - failure
  bool Seek(Position new_pos);

  // Seeks to `pos() + length`.
  //
  // Return values:
  //  * `true`                      - success (`length` bytes skipped)
  //  * `false` (when `healthy()`)  - source ends before skipping `length` bytes
  //                                  (position is set to the end)
  //  * `false` (when `!healthy()`) - failure
  bool Skip(Position length);

  // Returns `true` if this `Reader` supports `Size()`.
  virtual bool SupportsSize() const { return false; }

  // Returns the size of the source, i.e. the position corresponding to its end.
  //
  // Returns `absl::nullopt` on failure (`!healthy()`).
  virtual absl::optional<Position> Size();

 protected:
  // Creates a `Reader` with the given initial state.
  explicit Reader(InitiallyClosed) noexcept : Object(kInitiallyClosed) {}
  explicit Reader(InitiallyOpen) noexcept : Object(kInitiallyOpen) {}

  // Moves the part of the object defined in this class.
  //
  // Buffer pointers do not need to satisfy their invariants during this part of
  // the move, here they are merely exchanged with `nullptr` and copied.
  Reader(Reader&& that) noexcept;
  Reader& operator=(Reader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `Reader`. This avoids
  // constructing a temporary `Reader` and moving from it. Derived classes which
  // redefine `Reset()` should include a call to `Reader::Reset()`.
  void Reset(InitiallyClosed);
  void Reset(InitiallyOpen);

  // `Reader` overrides `Object::Done()` to set buffer pointers to `nullptr`.
  // Derived classes which override it further should include a call to
  // `Reader::Done()`.
  void Done() override;

  // Exposes a `Fail()` override which does not annotate the status with the
  // current position, unlike the public `Reader::Fail()`.
  ABSL_ATTRIBUTE_COLD bool FailWithoutAnnotation(absl::Status status);
  ABSL_ATTRIBUTE_COLD bool FailWithoutAnnotation(const Object& dependency);

  // Marks the `Reader` as failed with message "Reader position overflow".
  // Always returns `false`.
  //
  // This can be called if `limit_pos()` would overflow.
  ABSL_ATTRIBUTE_COLD bool FailOverflow();

  // Implementation of the slow part of `Pull()`.
  //
  // Precondition: `available() < min_length`
  virtual bool PullSlow(size_t min_length, size_t recommended_length) = 0;

  // Sets the values of:
  //  * `start()` - to `start`
  //  * `cursor()` - to `start + read_from_buffer`
  //  * `limit()` - to `start + buffer_size`
  //
  // Preconditions:
  //   [`start`, `start + buffer_size`) is a valid byte range
  //   `read_from_buffer <= buffer_size`
  void set_buffer(const char* start = nullptr, size_t buffer_size = 0,
                  size_t read_from_buffer = 0);

  // Implementations of the slow part of `Read()`, `ReadAndAppend()`, and
  // `CopyTo()`.
  //
  // `ReadSlow(std::string&)`, `ReadSlow(Chain&)` and `ReadSlow(absl::Cord&)`
  // append to any existing data in `*dest`.
  //
  // By default `ReadSlow(char*)` and `CopyToSlow(Writer&)` are implemented in
  // terms of `PullSlow()`; `ReadSlow(Chain&)` and `ReadSlow(absl::Cord&)` are
  // implemented in terms of `ReadSlow(char*)`; and
  // `CopyToSlow(BackwardWriter&)` is implemented in terms of `ReadSlow(char*)`
  // and `ReadSlow(Chain&)`.
  //
  // Precondition for `ReadSlow(char*)` and `ReadSlow(std::string&)`:
  //   `available() < length`
  //
  // Precondition for `ReadSlow(Chain&)`, `ReadSlow(absl::Cord&)`, and
  // `CopyToSlow()`:
  //   `UnsignedMin(available(), kMaxBytesToCopy) < length`
  //
  // Precondition for `ReadSlow(std::string&)`:
  //   `length <= dest->max_size() - dest->size()`
  //
  // Precondition for `ReadSlow(Chain&)` and `ReadSlow(absl::Cord&)`:
  //   `length <= std::numeric_limits<size_t>::max() - dest->size()`
  virtual bool ReadSlow(size_t length, char* dest);
  bool ReadSlow(size_t length, std::string& dest);
  virtual bool ReadSlow(size_t length, Chain& dest);
  virtual bool ReadSlow(size_t length, absl::Cord& dest);
  virtual bool CopyToSlow(Position length, Writer& dest);
  virtual bool CopyToSlow(size_t length, BackwardWriter& dest);

  // Implementation of the slow part of `ReadHint()`.
  //
  // By default does nothing.
  //
  // Precondition: `length > available()`
  virtual void ReadHintSlow(size_t length);

  // Source position corresponding to `start()`.
  Position start_pos() const;

  // Source position corresponding to `limit()`.
  Position limit_pos() const { return limit_pos_; }

  // Increments the value of `limit_pos()`.
  void move_limit_pos(Position length);

  // Sets the value of `limit_pos()`.
  void set_limit_pos(Position limit_pos);

  // Implementation of the slow part of `Seek()` and `Skip()`.
  //
  // Precondition: `new_pos < start_pos() || new_pos > limit_pos()`
  virtual bool SeekSlow(Position new_pos);

 private:
  const char* start_ = nullptr;
  const char* cursor_ = nullptr;
  const char* limit_ = nullptr;

  // Source position corresponding to `limit_`.
  //
  // Invariant: `limit_pos_ >= buffer_size()`
  Position limit_pos_ = 0;
};

// Implementation details follow.

inline Reader::Reader(Reader&& that) noexcept
    : Object(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      start_(std::exchange(that.start_, nullptr)),
      cursor_(std::exchange(that.cursor_, nullptr)),
      limit_(std::exchange(that.limit_, nullptr)),
      limit_pos_(std::exchange(that.limit_pos_, 0)) {}

inline Reader& Reader::operator=(Reader&& that) noexcept {
  Object::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  start_ = std::exchange(that.start_, nullptr);
  cursor_ = std::exchange(that.cursor_, nullptr);
  limit_ = std::exchange(that.limit_, nullptr);
  limit_pos_ = std::exchange(that.limit_pos_, 0);
  return *this;
}

inline void Reader::Reset(InitiallyClosed) {
  Object::Reset(kInitiallyClosed);
  start_ = nullptr;
  cursor_ = nullptr;
  limit_ = nullptr;
  limit_pos_ = 0;
}

inline void Reader::Reset(InitiallyOpen) {
  Object::Reset(kInitiallyOpen);
  start_ = nullptr;
  cursor_ = nullptr;
  limit_ = nullptr;
  limit_pos_ = 0;
}

inline void Reader::Done() {
  limit_pos_ = pos();
  set_buffer();
}

inline bool Reader::VerifyEndAndClose() {
  VerifyEnd();
  return Close();
}

inline bool Reader::Pull(size_t min_length, size_t recommended_length) {
  if (ABSL_PREDICT_TRUE(available() >= min_length)) return true;
  if (ABSL_PREDICT_FALSE(!PullSlow(min_length, recommended_length))) {
    return false;
  }
  RIEGELI_ASSERT_GE(available(), min_length)
      << "Failed postcondition of Reader::PullSlow(): "
         "not enough data available";
  return true;
}

inline void Reader::move_cursor(size_t length) {
  RIEGELI_ASSERT_LE(length, available())
      << "Failed precondition of Reader::move_cursor(): length out of range";
  cursor_ += length;
}

inline void Reader::set_cursor(const char* cursor) {
  RIEGELI_ASSERT(cursor >= start())
      << "Failed precondition of Reader::set_cursor(): pointer out of range";
  RIEGELI_ASSERT(cursor <= limit())
      << "Failed precondition of Reader::set_cursor(): pointer out of range";
  cursor_ = cursor;
}

inline void Reader::set_buffer(const char* start, size_t buffer_size,
                               size_t read_from_buffer) {
  RIEGELI_ASSERT_LE(read_from_buffer, buffer_size)
      << "Failed precondition of Reader::set_buffer(): length out of range";
  start_ = start;
  cursor_ = start + read_from_buffer;
  limit_ = start + buffer_size;
}

inline absl::optional<char> Reader::ReadChar() {
  if (ABSL_PREDICT_FALSE(!Pull())) return absl::nullopt;
  const char data = *cursor();
  move_cursor(1);
  return data;
}

inline absl::optional<uint8_t> Reader::ReadByte() {
  const absl::optional<char> data = ReadChar();
  if (ABSL_PREDICT_FALSE(data == absl::nullopt)) return absl::nullopt;
  return static_cast<uint8_t>(*data);
}

inline bool Reader::Read(size_t length, absl::string_view& dest) {
  const bool ok = Pull(length);
  if (ABSL_PREDICT_FALSE(!ok)) length = available();
  dest = absl::string_view(cursor(), length);
  move_cursor(length);
  return ok;
}

inline bool Reader::Read(size_t length, char* dest) {
  if (ABSL_PREDICT_TRUE(available() >= length)) {
    // `std::memcpy(nullptr, _, 0)` and `std::memcpy(_, nullptr, 0)` are
    // undefined.
    if (ABSL_PREDICT_TRUE(length > 0)) {
      std::memcpy(dest, cursor(), length);
      move_cursor(length);
    }
    return true;
  }
  return ReadSlow(length, dest);
}

inline bool Reader::Read(size_t length, std::string& dest) {
  RIEGELI_CHECK_LE(length, dest.max_size())
      << "Failed precondition of Reader::Read(string*): "
         "string size overflow";
  if (ABSL_PREDICT_TRUE(available() >= length)) {
    dest.assign(cursor(), length);
    move_cursor(length);
    return true;
  }
  dest.clear();
  return ReadSlow(length, dest);
}

inline bool Reader::Read(size_t length, Chain& dest) {
  dest.Clear();
  if (ABSL_PREDICT_TRUE(available() >= length && length <= kMaxBytesToCopy)) {
    dest.Append(absl::string_view(cursor(), length));
    move_cursor(length);
    return true;
  }
  return ReadSlow(length, dest);
}

inline bool Reader::Read(size_t length, absl::Cord& dest) {
  if (ABSL_PREDICT_TRUE(available() >= length && length <= kMaxBytesToCopy)) {
    dest = absl::string_view(cursor(), length);
    move_cursor(length);
    return true;
  }
  dest.Clear();
  return ReadSlow(length, dest);
}

inline bool Reader::ReadAndAppend(size_t length, std::string& dest) {
  RIEGELI_CHECK_LE(length, dest.max_size() - dest.size())
      << "Failed precondition of Reader::ReadAndAppend(string*): "
         "string size overflow";
  if (ABSL_PREDICT_TRUE(available() >= length)) {
    dest.append(cursor(), length);
    move_cursor(length);
    return true;
  }
  return ReadSlow(length, dest);
}

inline bool Reader::ReadAndAppend(size_t length, Chain& dest) {
  RIEGELI_CHECK_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadAndAppend(Chain*): "
         "Chain size overflow";
  if (ABSL_PREDICT_TRUE(available() >= length && length <= kMaxBytesToCopy)) {
    dest.Append(absl::string_view(cursor(), length));
    move_cursor(length);
    return true;
  }
  return ReadSlow(length, dest);
}

inline bool Reader::ReadAndAppend(size_t length, absl::Cord& dest) {
  RIEGELI_CHECK_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadAndAppend(Cord*): "
         "Cord size overflow";
  if (ABSL_PREDICT_TRUE(available() >= length && length <= kMaxBytesToCopy)) {
    dest.Append(absl::string_view(cursor(), length));
    move_cursor(length);
    return true;
  }
  return ReadSlow(length, dest);
}

inline bool Reader::CopyTo(Position length, Writer& dest) {
  if (ABSL_PREDICT_TRUE(available() >= length && length <= kMaxBytesToCopy)) {
    const absl::string_view data(cursor(), IntCast<size_t>(length));
    move_cursor(IntCast<size_t>(length));
    return dest.Write(data);
  }
  return CopyToSlow(length, dest);
}

inline bool Reader::CopyTo(size_t length, BackwardWriter& dest) {
  if (ABSL_PREDICT_TRUE(available() >= length && length <= kMaxBytesToCopy)) {
    const absl::string_view data(cursor(), length);
    move_cursor(length);
    return dest.Write(data);
  }
  return CopyToSlow(length, dest);
}

inline void Reader::ReadHint(size_t length) {
  if (ABSL_PREDICT_TRUE(available() >= length)) return;
  ReadHintSlow(length);
}

inline Position Reader::pos() const {
  RIEGELI_ASSERT_GE(limit_pos_, buffer_size())
      << "Failed invariant of Reader: negative position of buffer start";
  return limit_pos_ - available();
}

inline Position Reader::start_pos() const {
  RIEGELI_ASSERT_GE(limit_pos_, buffer_size())
      << "Failed invariant of Reader: negative position of buffer start";
  return limit_pos_ - buffer_size();
}

inline void Reader::move_limit_pos(Position length) {
  RIEGELI_ASSERT_LE(length, std::numeric_limits<Position>::max() - limit_pos_)
      << "Failed precondition of Reader::move_limit_pos(): "
         "position out of range";
  limit_pos_ += length;
}

inline void Reader::set_limit_pos(Position limit_pos) {
  limit_pos_ = limit_pos;
}

inline bool Reader::Seek(Position new_pos) {
  if (ABSL_PREDICT_TRUE(new_pos >= start_pos() && new_pos <= limit_pos())) {
    set_cursor(limit() - (limit_pos() - new_pos));
    return true;
  }
  return SeekSlow(new_pos);
}

inline bool Reader::Skip(Position length) {
  if (ABSL_PREDICT_TRUE(available() >= length)) {
    move_cursor(IntCast<size_t>(length));
    return true;
  }
  if (ABSL_PREDICT_FALSE(length >
                         std::numeric_limits<Position>::max() - pos())) {
    return FailOverflow();
  }
  return SeekSlow(pos() + length);
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_READER_H_
