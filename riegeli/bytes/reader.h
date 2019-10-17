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

#include <cstring>
#include <limits>
#include <string>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
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
  // if not. Closes the `Reader`.
  //
  // Return values:
  //  * `true`  - success (the source ends at the former current position)
  //  * `false` - failure (the source does not end at the former current
  //                       position or the `Reader` was not healthy before
  //                       closing)
  bool VerifyEndAndClose();

  // Verifies that the source ends at the current position, failing the `Reader`
  // if not.
  virtual void VerifyEnd();

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
  //   if `closed()` then `start() == cursor() == limit() == nullptr`
  const char* start() const { return start_; }
  const char* cursor() const { return cursor_; }
  const char* limit() const { return limit_; }

  // Updates the value of `cursor()`. Call this during reading data under
  // `cursor()` to indicate how much was read, or to seek within the buffer.
  //
  // Precondition: `start() <= cursor <= limit()`
  void set_cursor(const char* cursor);

  // Returns the amount of data available in the buffer, between `cursor()` and
  // `limit()`.
  //
  // Invariant: if `closed()` then `available() == 0`
  size_t available() const { return PtrDistance(cursor_, limit_); }

  // Returns the buffer size, between `start()` and `limit()`.
  size_t buffer_size() const { return PtrDistance(start_, limit_); }

  // Returns the amount of data read from the buffer, between `start()` and
  // `cursor()`.
  size_t read_from_buffer() const { return PtrDistance(start_, cursor_); }

  // Reads a fixed number of bytes from the buffer to `*dest`, pulling data from
  // the source as needed.
  //
  // `Read(std::string*)` and `Read(Chain*)` append to any existing data in
  // `*dest`.
  //
  // `Read(absl::string_view*)` points `*dest` to an array holding the data. The
  // array is valid until the next non-const operation on the `Reader`.
  //
  // `CopyTo(Writer*)` writes as much as could be read if reading failed, and
  // reads an unspecified length (between what could be written and the
  // requested length) if writing failed.
  //
  // `CopyTo(BackwardWriter*)` writes nothing if reading failed, and reads the
  // full requested length even if writing failed.
  //
  // Precondition for `Read(std::string*)`:
  //   `length <= dest->max_size() - dest->size()`
  //
  // Precondition for `Read(Chain*)`:
  //   `length <= std::numeric_limits<size_t>::max() - dest->size()`
  //
  // Return values for `Read()`:
  //  * `true`                      - success (`length` bytes read)
  //  * `false` (when `healthy()`)  - source ends
  //                                  (less than `length` bytes read)
  //  * `false` (when `!healthy()`) - failure (less than `length` bytes read)
  //
  // Return values for `CopyTo()`:
  //  * `true`                                          - success (`length`
  //                                                      bytes copied)
  //  * `false` (when `dest->healthy() && healthy()`)   - source ends (less than
  //                                                      `length` bytes copied)
  //  * `false` (when `!dest->healthy() || !healthy()`) - failure (less than
  //                                                      `length` bytes copied)
  bool Read(char* dest, size_t length);
  bool Read(std::string* dest, size_t length);
  bool Read(absl::string_view* dest, size_t length);
  bool Read(Chain* dest, size_t length);
  bool CopyTo(Writer* dest, Position length);
  bool CopyTo(BackwardWriter* dest, size_t length);

  // Reads all remaining bytes from the buffer and the source to `*dest`.
  //
  // `ReadAll(std::string*)` and `ReadAll(Chain*)` append to any existing data
  // in `*dest`.
  //
  // `CopyAllTo(Writer*)` writes as much as could be read if reading failed, and
  // reads an unspecified length (between what could be written and the
  // requested length) if writing failed.
  //
  // `CopyAllTo(BackwardWriter*)` writes nothing if reading failed, and reads
  // the full requested length even if writing failed.
  //
  // Return values for `ReadAll()`:
  //  * `true` (`healthy()`)   - success
  //  * `false` (`!healthy()`) - failure
  //
  // Return values for `CopyAllTo()`:
  //  * `true` (`dest->healthy() && healthy()`)    - success
  //  * `false` (`!dest->healthy() || !healthy()`) - failure
  bool ReadAll(std::string* dest);
  bool ReadAll(Chain* dest);
  bool CopyAllTo(Writer* dest);
  bool CopyAllTo(BackwardWriter* dest);

  // Returns the current position.
  //
  // This is often 0 after creating the `Reader`, but not necessarily if the
  // `Reader` wraps another reader or input stream propagating its position.
  //
  // `pos()` is unchanged by `Close()`.
  Position pos() const;

  // Returns `true` if this `Reader` supports `Seek()` backwards (`Seek()`
  // forwards is always supported) and `Size()`.
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

  // Returns the size of the source, i.e. the position corresponding to its end.
  //
  // Return values:
  //  * `true`  - success (`*size` is set, `healthy()`)
  //  * `false` - failure (`!healthy()`)
  virtual bool Size(Position* size);

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
  // override `Reset()` should include a call to `Reader::Reset()`.
  void Reset(InitiallyClosed);
  void Reset(InitiallyOpen);

  // `Reader` overrides `Object::Done()` to set buffer pointers to `nullptr`.
  // Derived classes which override it further should include a call to
  // `Reader::Done()`.
  void Done() override;

  // Marks the `Reader` as failed with message "Reader position overflow".
  // Always returns `false`.
  //
  // This can be called if `limit_pos_` would overflow.
  //
  // Precondition: `!closed()`
  ABSL_ATTRIBUTE_COLD bool FailOverflow();

  // Implementation of the slow part of `Pull()`.
  //
  // Precondition: `min_length > available()`
  virtual bool PullSlow(size_t min_length, size_t recommended_length) = 0;

  // Implementations of the slow part of `Read()` and `CopyTo()`.
  //
  // By default `ReadSlow(char*)` and `CopyToSlow(Writer*)` are implemented in
  // terms of `PullSlow()`; `ReadSlow(Chain*)` is implemented in terms of
  // `ReadSlow(char*)`; and `CopyToSlow(BackwardWriter*)` is implemented in
  // terms of `ReadSlow(char*)` and `ReadSlow(Chain*)`.
  //
  // Precondition for `ReadSlow(char*)` and `ReadSlow(std::string*)`:
  //   `length > available()`
  //
  // Precondition for `ReadSlow(Chain*)` and `CopyToSlow()`:
  //   `length > UnsignedMin(available(), kMaxBytesToCopy)`
  //
  // Precondition for `ReadSlow(std::string*)`:
  //   `length <= dest->max_size() - dest->size()`
  //
  // Precondition for `ReadSlow(Chain*)`:
  //   `length <= std::numeric_limits<size_t>::max() - dest->size()`
  virtual bool ReadSlow(char* dest, size_t length);
  bool ReadSlow(std::string* dest, size_t length);
  virtual bool ReadSlow(Chain* dest, size_t length);
  virtual bool CopyToSlow(Writer* dest, Position length);
  virtual bool CopyToSlow(BackwardWriter* dest, size_t length);

  // Implementation of the slow part of `Seek()` and `Skip()`.
  //
  // Precondition: `new_pos < start_pos() || new_pos > limit_pos_`
  virtual bool SeekSlow(Position new_pos);

  // Source position corresponding to `start_`.
  Position start_pos() const;

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
      start_(std::exchange(that.start_, nullptr)),
      cursor_(std::exchange(that.cursor_, nullptr)),
      limit_(std::exchange(that.limit_, nullptr)),
      limit_pos_(std::exchange(that.limit_pos_, 0)) {}

inline Reader& Reader::operator=(Reader&& that) noexcept {
  Object::operator=(std::move(that));
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
  start_ = nullptr;
  cursor_ = nullptr;
  limit_ = nullptr;
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

inline void Reader::set_cursor(const char* cursor) {
  RIEGELI_ASSERT(cursor >= start())
      << "Failed precondition of Reader::set_cursor(): pointer out of range";
  RIEGELI_ASSERT(cursor <= limit())
      << "Failed precondition of Reader::set_cursor(): pointer out of range";
  cursor_ = cursor;
}

inline bool Reader::Read(char* dest, size_t length) {
  if (ABSL_PREDICT_TRUE(length <= available())) {
    // `std::memcpy(nullptr, _, 0)` and `std::memcpy(_, nullptr, 0)` are
    // undefined.
    if (ABSL_PREDICT_TRUE(length > 0)) {
      std::memcpy(dest, cursor_, length);
      cursor_ += length;
    }
    return true;
  }
  return ReadSlow(dest, length);
}

inline bool Reader::Read(std::string* dest, size_t length) {
  RIEGELI_CHECK_LE(length, dest->max_size() - dest->size())
      << "Failed precondition of Reader::Read(string*): string size overflow";
  if (ABSL_PREDICT_TRUE(length <= available())) {
    dest->append(cursor_, length);
    cursor_ += length;
    return true;
  }
  return ReadSlow(dest, length);
}

inline bool Reader::Read(absl::string_view* dest, size_t length) {
  if (ABSL_PREDICT_FALSE(!Pull(length))) return false;
  *dest = absl::string_view(cursor_, length);
  cursor_ += length;
  return true;
}

inline bool Reader::Read(Chain* dest, size_t length) {
  RIEGELI_CHECK_LE(length, std::numeric_limits<size_t>::max() - dest->size())
      << "Failed precondition of Reader::Read(Chain*): Chain size overflow";
  if (ABSL_PREDICT_TRUE(length <= available() && length <= kMaxBytesToCopy)) {
    dest->Append(absl::string_view(cursor_, length));
    cursor_ += length;
    return true;
  }
  return ReadSlow(dest, length);
}

inline bool Reader::CopyTo(Writer* dest, Position length) {
  if (ABSL_PREDICT_TRUE(length <= available() && length <= kMaxBytesToCopy)) {
    const absl::string_view data(cursor_, IntCast<size_t>(length));
    cursor_ += length;
    return dest->Write(data);
  }
  return CopyToSlow(dest, length);
}

inline bool Reader::CopyTo(BackwardWriter* dest, size_t length) {
  if (ABSL_PREDICT_TRUE(length <= available() && length <= kMaxBytesToCopy)) {
    const absl::string_view data(cursor_, length);
    cursor_ += length;
    return dest->Write(data);
  }
  return CopyToSlow(dest, length);
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

inline bool Reader::Seek(Position new_pos) {
  if (ABSL_PREDICT_TRUE(new_pos >= start_pos() && new_pos <= limit_pos_)) {
    cursor_ = limit_ - (limit_pos_ - new_pos);
    return true;
  }
  return SeekSlow(new_pos);
}

inline bool Reader::Skip(Position length) {
  if (ABSL_PREDICT_TRUE(length <= available())) {
    cursor_ += length;
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
