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
#include "riegeli/base/status.h"

namespace riegeli {

// A BackwardWriter writes sequences of bytes to a destination, like Writer,
// but back to front.
//
// Sequential writing is supported, random access is not supported, truncation
// is optionally supported.
class BackwardWriter : public Object {
 public:
  // Ensures that enough space is available for writing: pushes previously
  // written data to the destination, and points cursor() and limit() to space
  // with length at least min_length, preferably recommended_length. If enough
  // space was already available, does nothing.
  //
  // If recommended_length < min_length, recommended_length is assumed to be
  // min_length.
  //
  // Return values:
  //  * true  - success (available() >= min_length)
  //  * false - failure (available() < min_length, !healthy())
  bool Push(size_t min_length = 1, size_t recommended_length = 0);

  // Buffer pointers. Space between start() (exclusive upper bound) and limit()
  // (inclusive lower bound) is available for writing data to it, with cursor()
  // pointing to the current position going downwards (past the next byte to
  // write).
  //
  // Invariants:
  //   start() >= cursor() >= limit() (possibly all nullptr)
  //   if !healthy() then start() == cursor() == limit()
  //   if closed() then start() == cursor() == limit() == nullptr
  char* start() const { return start_; }
  char* cursor() const { return cursor_; }
  char* limit() const { return limit_; }

  // Updates the value of cursor(). Call this during writing data under cursor()
  // to indicate how much was written, or to seek within the buffer.
  //
  // Precondition: start() >= cursor >= limit()
  void set_cursor(char* cursor);

  // Returns the amount of space available in the buffer, between cursor() and
  // limit().
  //
  // Invariant: if !healthy() then available() == 0
  size_t available() const { return PtrDistance(limit_, cursor_); }

  // Returns the buffer size, between start() and limit().
  //
  // Invariant: if !healthy() then buffer_size() == 0
  size_t buffer_size() const { return PtrDistance(limit_, start_); }

  // Returns the amount of data written to the buffer, between start() and
  // cursor().
  //
  // Invariant: if !healthy() then written_to_buffer() == 0
  size_t written_to_buffer() const { return PtrDistance(cursor_, start_); }

  // Prepends a fixed number of bytes from src to the buffer, pushing data to
  // the destination as needed.
  //
  // Return values:
  //  * true  - success (src.size() bytes written)
  //  * false - failure (a suffix of less than src.size() bytes written,
  //                    !healthy())
  bool Write(absl::string_view src);
  bool Write(std::string&& src);
  bool Write(const char* src);
  bool Write(const Chain& src);
  bool Write(Chain&& src);

  // Pushes data written between start() and cursor() to the destination.
  //
  // Additionally, attempts to ensure the following, depending on flush_type
  // (without a guarantee though):
  //  * FlushType::kFromObject  - nothing
  //  * FlushType::kFromProcess - data survives process crash
  //  * FlushType::kFromMachine - data survives operating system crash
  //
  // The precise meaning of Flush() depends on the particular BackwardWriter.
  // The intent is to make data written so far visible, but in contrast to
  // Close(), keeping the possibility to write more data later.
  //
  // Return values:
  //  * true  - success (healthy())
  //  * false - failure (!healthy())
  virtual bool Flush(FlushType flush_type) = 0;

  // Returns the current position (increasing as data are prepended).
  //
  // This is not necessarily 0 after creating the BackwardWriter if it prepends
  // to a destination with existing contents, or if the BackwardWriter wraps
  // another writer or output stream propagating its position.
  //
  // This may decrease when the BackwardWriter becomes unhealthy (due to
  // buffering, previously written but unflushed data may be lost).
  //
  // pos() is unchanged by a successful Close().
  Position pos() const;

  // Returns true if this BackwardWriter supports Truncate().
  virtual bool SupportsTruncate() const { return false; }

  // Discards the part of the destination after the given position. Sets the
  // current position to the new end.
  //
  // Return values:
  //  * true                    - success (destination truncated, healthy())
  //  * false (when healthy())  - destination is smaller than new_size
  //                              (position is set to end)
  //  * false (when !healthy()) - failure
  virtual bool Truncate(Position new_size);

 protected:
  // Creates a BackwardWriter with the given initial state.
  explicit BackwardWriter(InitiallyClosed) noexcept
      : Object(kInitiallyClosed) {}
  explicit BackwardWriter(InitiallyOpen) noexcept : Object(kInitiallyOpen) {}

  // Moves the part of the object defined in this class.
  //
  // Buffer pointers do not need to satisfy their invariants during this part of
  // the move, here they are merely exchanged with nullptr and copied.
  BackwardWriter(BackwardWriter&& that) noexcept;
  BackwardWriter& operator=(BackwardWriter&& that) noexcept;

  // Makes *this equivalent to a newly constructed BackwardWriter. This avoids
  // constructing a temporary BackwardWriter and moving from it. Derived classes
  // which override Reset() should include a call to BackwardWriter::Reset().
  void Reset(InitiallyClosed);
  void Reset(InitiallyOpen);

  // BackwardWriter overrides Object::Done() to set buffer pointers to nullptr.
  // Derived classes which override it further should include a call to
  // BackwardWriter::Done().
  void Done() override;

  // BackwardWriter overrides Object::Fail() to set buffer pointers to nullptr.
  // Derived classes which override it further should include a call to
  // BackwardWriter::Fail().
  using Object::Fail;
  ABSL_ATTRIBUTE_COLD bool Fail(Status status) override;

  // Marks the BackwardWriter as failed with message "BackwardWriter position
  // overflow". Always returns false.
  //
  // This can be called if the destination would exceed its maximum possible
  // size or if start_pos_ would overflow.
  //
  // Precondition: !closed()
  ABSL_ATTRIBUTE_COLD bool FailOverflow();

  // Implementation of the slow part of Push().
  //
  // Precondition: available() == 0
  virtual bool PushSlow(size_t min_length, size_t recommended_length) = 0;

  // Implementation of the slow part of Write().
  //
  // By default WriteSlow(string_view) is implemented in terms of Push();
  // WriteSlow(const Chain&) is implemented in terms of
  // WriteSlow(string_view); and WriteSlow(Chain&&) is implemented in terms of
  // WriteSlow(const Chain&).
  //
  // Precondition for WriteSlow(string_view):
  //   src.size() > available()
  //
  // Precondition for WriteSlow(Chain&&):
  //   src.size() > UnsignedMin(available(), kMaxBytesToCopy)
  virtual bool WriteSlow(absl::string_view src);
  virtual bool WriteSlow(const Chain& src);
  virtual bool WriteSlow(Chain&& src);

  // Destination position corresponding to limit_.
  Position limit_pos() const;

  char* start_ = nullptr;
  char* cursor_ = nullptr;
  char* limit_ = nullptr;

  // Destination position corresponding to start_.
  //
  // Invariant: start_pos_ <= numeric_limits<Position>::max() - buffer_size()
  Position start_pos_ = 0;
};

// Implementation details follow.

inline BackwardWriter::BackwardWriter(BackwardWriter&& that) noexcept
    : Object(std::move(that)),
      start_(std::exchange(that.start_, nullptr)),
      cursor_(std::exchange(that.cursor_, nullptr)),
      limit_(std::exchange(that.limit_, nullptr)),
      start_pos_(std::exchange(that.start_pos_, 0)) {}

inline BackwardWriter& BackwardWriter::operator=(
    BackwardWriter&& that) noexcept {
  Object::operator=(std::move(that));
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
  start_ = nullptr;
  cursor_ = nullptr;
  limit_ = nullptr;
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

inline void BackwardWriter::set_cursor(char* cursor) {
  RIEGELI_ASSERT(cursor <= start())
      << "Failed precondition of BackwardWriter::set_cursor(): "
         "pointer out of range";
  RIEGELI_ASSERT(cursor >= limit())
      << "Failed precondition of BackwardWriter::set_cursor(): "
         "pointer out of range";
  cursor_ = cursor;
}

inline bool BackwardWriter::Write(absl::string_view src) {
  if (ABSL_PREDICT_TRUE(src.size() <= available())) {
    if (ABSL_PREDICT_TRUE(!src.empty())) {  // memcpy(nullptr, _, 0) and
                                            // memcpy(_, nullptr, 0)
                                            // are undefined.
      cursor_ -= src.size();
      std::memcpy(cursor_, src.data(), src.size());
    }
    return true;
  }
  return WriteSlow(src);
}

inline bool BackwardWriter::Write(std::string&& src) {
  if (ABSL_PREDICT_TRUE(src.size() <= available() &&
                        src.size() <= kMaxBytesToCopy)) {
    if (ABSL_PREDICT_TRUE(!src.empty())) {  // memcpy(nullptr, _, 0)
                                            // is undefined.
      cursor_ -= src.size();
      std::memcpy(cursor_, src.data(), src.size());
    }
    return true;
  }
  return WriteSlow(Chain(std::move(src)));
}

inline bool BackwardWriter::Write(const char* src) {
  return Write(absl::string_view(src));
}

inline bool BackwardWriter::Write(const Chain& src) {
  if (ABSL_PREDICT_TRUE(src.size() <= available() &&
                        src.size() <= kMaxBytesToCopy)) {
    cursor_ -= src.size();
    src.CopyTo(cursor_);
    return true;
  }
  return WriteSlow(src);
}

inline bool BackwardWriter::Write(Chain&& src) {
  if (ABSL_PREDICT_TRUE(src.size() <= available() &&
                        src.size() <= kMaxBytesToCopy)) {
    cursor_ -= src.size();
    src.CopyTo(cursor_);
    return true;
  }
  return WriteSlow(std::move(src));
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

}  // namespace riegeli

#endif  // RIEGELI_BYTES_BACKWARD_WRITER_H_
