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
#include <string>
#include <utility>

#include "riegeli/base/assert.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/base/string_view.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// A Reader reads sequences of bytes from a source. The nature of the source
// depends on the particular class derived from Reader.
//
// A Reader object manages a buffer of data pulled from the source, which
// amortizes the overhead of pulling data over multiple reads. Data can be read
// directly from the buffer, and classes derived from Reader can avoid copying
// by allocating the buffer in a way which fits the source, e.g. pointing it to
// a fragment of the source itself.
//
// All Readers support reading data sequentially and querying for the current
// position. Some Readers also support random access: changing the position
// backwards for subsequent operations and querying for the total size of the
// source.
class Reader : public Object {
 public:
  // Verifies that the source ends at the current position, failing the Reader
  // if not. Closes the Reader.
  //
  // Return values:
  //  * true  - success (the source ends at the former current position)
  //  * false - failure (the source does not end at the former current position
  //                     or the Reader was not healthy before closing)
  bool VerifyEndAndClose();

  // Ensures that some data are available for reading: pulls more data from the
  // source, and points cursor() and limit() to non-empty data. If some data
  // were already buffered, does nothing.
  //
  // Return values:
  //  * true                    - success (available() > 0)
  //  * false (when healthy())  - source ends (available() == 0)
  //  * false (when !healthy()) - failure (available() == 0)
  bool Pull();

  // Buffer pointers. Data between start() and limit() are available for
  // reading, with cursor() pointing to the current position.
  //
  // Invariants:
  //   start() <= cursor() <= limit() (possibly all nullptr)
  //   if closed() then start() == cursor() == limit() == nullptr
  const char* start() const { return start_; }
  const char* cursor() const { return cursor_; }
  const char* limit() const { return limit_; }

  // Updates the value of cursor(). Call this during reading data under cursor()
  // to indicate how much was read, or to seek within the buffer.
  //
  // Precondition: start() <= cursor <= limit()
  void set_cursor(const char* cursor);

  // Returns the amount of data available between cursor() and limit().
  //
  // Invariant: if closed() then available() == 0
  size_t available() const { return limit_ - cursor_; }

  // Reads a fixed number of bytes from the buffer to dest, pulling data from
  // the source as needed.
  //
  // Read(string*) and Read(Chain*) append to any existing data in
  // dest.
  //
  // Read(string_view*) points dest to an array holding the data, residing
  // either in internal buffers or in scratch (resized) if internal buffers are
  // too small.
  //
  // CopyTo(Writer*) writes as much as could be read if reading failed, and
  // reads an unspecified length (between what could be written and the
  // requested length) if writing failed.
  //
  // CopyTo(BackwardWriter*) writes nothing if reading failed, and reads the
  // full requested length even if writing failed.
  //
  // Return values:
  //  * true                    - success (length bytes read)
  //  * false (when healthy())  - source ends (less than length bytes read)
  //  * false (when !healthy()) - failure (less than length bytes read)
  //
  // If CopyTo() returns false and !dest->healthy(), the problem was at dest.
  bool Read(char* dest, size_t length);
  bool Read(std::string* dest, size_t length);
  bool Read(string_view* dest, std::string* scratch, size_t length);
  bool Read(Chain* dest, size_t length);
  bool CopyTo(Writer* dest, Position length);
  bool CopyTo(BackwardWriter* dest, size_t length);

  // Returns true if reading from the current position might succeed, possibly
  // after some data is appended to the source. Returns false if reading from
  // the current position will always return false.
  //
  // Invariants:
  //   if available() > 0 then HopeForMore()
  //   if available() == 0 && !healthy() then !HopeForMore()
  bool HopeForMore() const;

  // Returns the current position.
  //
  // This is often 0 after creating the Reader, but not necessarily if the
  // Reader wraps another reader or input stream propagating its position.
  //
  // Invariant: if closed() then pos() == 0
  Position pos() const { return limit_pos_ - available(); }

  // Returns true if this Reader supports Seek() backwards (Seek() forwards is
  // always supported) and Size().
  virtual bool SupportsRandomAccess() const { return false; }

  // Sets the current position for subsequent operations.
  //
  // Seeking to new_pos >= pos() - (cursor() - start()) is always supported,
  // although if SupportsRandomAccess() is false then it is not expected
  // to be more efficient than reading and discarding the intervening data.
  // Seeking to new_pos < pos() - (cursor() - start()) is supported only when
  // SupportsRandomAccess() is true.
  //
  // Return values:
  //  * true                    - success (position is set to new_pos)
  //  * false (when healthy())  - source ends before new_pos (position is set to
  //                              the end) or seeking backwards is not supported
  //                              (position is unchanged)
  //  * false (when !healthy()) - failure
  bool Seek(Position new_pos);

  // Seeks to pos() + length.
  //
  // Return values:
  //  * true                    - success (length bytes skipped)
  //  * false (when healthy())  - source ends before skipping length bytes
  //                              (position is set to the end)
  //  * false (when !healthy()) - failure
  bool Skip(Position length);

  // Returns the size of the source, i.e. the position corresponding to its end.
  //
  // Return values:
  //  * true  - success (*size is set, healthy())
  //  * false - failure (healthy() is unchanged)
  virtual bool Size(Position* size) const { return false; }

 protected:
  // Creates a Reader with the given initial state.
  explicit Reader(State state) noexcept : Object(state) {}

  // Moves the part of the object defined in this class.
  //
  // Buffer pointers do not need to satisfy their invariants during this part of
  // the move, here they are merely exchanged with nullptr and copied.
  Reader(Reader&& src) noexcept;
  Reader& operator=(Reader&& src) noexcept;

  // Reader provides a partial override of Object::Done(). Derived classes must
  // override it further and include a call to Reader::Done().
  virtual void Done() override = 0;

  // Implementation of the slow part of Pull().
  //
  // Precondition: available() == 0
  virtual bool PullSlow() = 0;

  // Returns the amount of data read from the buffer, between start() and
  // cursor().
  size_t read_from_buffer() const { return cursor_ - start_; }

  // Implementations of the slow part of Read() and CopyTo().
  //
  // By default ReadSlow(char*) and CopyToSlow(Writer*) are implemented in terms
  // of PullSlow(); ReadSlow(Chain*) is implemented in
  // terms of ReadSlow(char*); and CopyToSlow(BackwardWriter*) is implemented in
  // terms of ReadSlow(char*) and ReadSlow(Chain*).
  //
  // Precondition for ReadSlow(char*), ReadSlow(string*), and
  // ReadSlow(string_view*):
  //   length > available()
  //
  // Precondition for ReadSlow(Chain*) and CopyToSlow():
  //   length > UnsignedMin(available(), kMaxBytesToCopy())
  virtual bool ReadSlow(char* dest, size_t length);
  bool ReadSlow(std::string* dest, size_t length);
  bool ReadSlow(string_view* dest, std::string* scratch, size_t length);
  virtual bool ReadSlow(Chain* dest, size_t length);
  virtual bool CopyToSlow(Writer* dest, Position length);
  virtual bool CopyToSlow(BackwardWriter* dest, size_t length);

  // Implementation of the slow part of HopeForMore().
  //
  // Precondition: available() == 0
  virtual bool HopeForMoreSlow() const;

  // Implementation of the slow part of Seek() and Skip().
  //
  // Precondition: new_pos < start_pos() || new_pos > limit_pos_
  virtual bool SeekSlow(Position new_pos);

  // Source position corresponding to start_.
  Position start_pos() const { return limit_pos_ - (limit_ - start_); }

  const char* start_ = nullptr;
  const char* cursor_ = nullptr;
  const char* limit_ = nullptr;

  // Source position corresponding to limit_.
  Position limit_pos_ = 0;
};

// Implementation details follow.

inline Reader::Reader(Reader&& src) noexcept
    : Object(std::move(src)),
      start_(riegeli::exchange(src.start_, nullptr)),
      cursor_(riegeli::exchange(src.cursor_, nullptr)),
      limit_(riegeli::exchange(src.limit_, nullptr)),
      limit_pos_(riegeli::exchange(src.limit_pos_, 0)) {}

inline Reader& Reader::operator=(Reader&& src) noexcept {
  Object::operator=(std::move(src));
  start_ = riegeli::exchange(src.start_, nullptr);
  cursor_ = riegeli::exchange(src.cursor_, nullptr);
  limit_ = riegeli::exchange(src.limit_, nullptr);
  limit_pos_ = riegeli::exchange(src.limit_pos_, 0);
  return *this;
}

inline void Reader::Done() {
  start_ = nullptr;
  cursor_ = nullptr;
  limit_ = nullptr;
  limit_pos_ = 0;
}

inline bool Reader::Pull() {
  if (RIEGELI_LIKELY(available() > 0)) return true;
  return PullSlow();
}

inline void Reader::set_cursor(const char* cursor) {
  RIEGELI_ASSERT(cursor >= start());
  RIEGELI_ASSERT(cursor <= limit());
  cursor_ = cursor;
}

inline bool Reader::Read(char* dest, size_t length) {
  if (RIEGELI_LIKELY(length <= available())) {
    if (length > 0) {  // memcpy(nullptr, _, 0) and
                       // memcpy(_, nullptr, 0) are undefined.
      std::memcpy(dest, cursor_, length);
      cursor_ += length;
    }
    return true;
  }
  return ReadSlow(dest, length);
}

inline bool Reader::Read(std::string* dest, size_t length) {
  if (RIEGELI_LIKELY(length <= available())) {
    if (length > 0) {  // Avoid std::string::append(nullptr, 0) just in case.
      dest->append(cursor_, length);
      cursor_ += length;
    }
    return true;
  }
  return ReadSlow(dest, length);
}

inline bool Reader::Read(string_view* dest, std::string* scratch, size_t length) {
  if (RIEGELI_LIKELY(length <= available())) {
    *dest = string_view(cursor_, length);
    cursor_ += length;
    return true;
  }
  return ReadSlow(dest, scratch, length);
}

inline bool Reader::Read(Chain* dest, size_t length) {
  if (RIEGELI_LIKELY(length <= available() && length <= kMaxBytesToCopy())) {
    dest->Append(string_view(cursor_, length), dest->size() + length);
    cursor_ += length;
    return true;
  }
  return ReadSlow(dest, length);
}

inline bool Reader::CopyTo(Writer* dest, Position length) {
  if (RIEGELI_LIKELY(length <= available() && length <= kMaxBytesToCopy())) {
    const string_view data(cursor_, length);
    cursor_ += length;
    return dest->Write(data);
  }
  return CopyToSlow(dest, length);
}

inline bool Reader::CopyTo(BackwardWriter* dest, size_t length) {
  if (RIEGELI_LIKELY(length <= available() && length <= kMaxBytesToCopy())) {
    const string_view data(cursor_, length);
    cursor_ += length;
    return dest->Write(data);
  }
  return CopyToSlow(dest, length);
}

inline bool Reader::HopeForMore() const {
  return available() > 0 || HopeForMoreSlow();
}

inline bool Reader::Seek(Position new_pos) {
  if (RIEGELI_LIKELY(new_pos >= start_pos() && new_pos <= limit_pos_)) {
    cursor_ = limit_ - (limit_pos_ - new_pos);
    return true;
  }
  return SeekSlow(new_pos);
}

inline bool Reader::Skip(Position length) {
  if (RIEGELI_LIKELY(length <= available())) {
    cursor_ += length;
    return true;
  }
  return SeekSlow(pos() + length);
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_READER_H_
