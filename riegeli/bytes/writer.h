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
#include <cstring>
#include <limits>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/meta/type_traits.h"
#include "absl/strings/string_view.h"
#include "absl/utility/utility.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"

namespace riegeli {

// A Writer writes sequences of bytes to a destination. The nature of the
// destination depends on the particular class derived from Writer.
//
// A Writer object manages a buffer of data to be pushed to the destination,
// which amortizes the overhead of pushing data over multiple writes. Data can
// be written directly into the buffer, and classes derived from Writer can
// avoid copying by allocating the buffer in a way which fits the destination,
// e.g. pointing it to a fragment of the destination itself.
//
// All Writers support writing data sequentially and querying for the current
// position. Some Writers also support random access: changing the position for
// subsequent operations, querying for the total size of the destination, and
// truncation.
//
// A Writer must be explicitly closed, and Close() must succeed, in order for
// its output to be available in the destination.
class Writer : public Object {
 public:
  // Ensures that some space is available for writing: pushes previously written
  // data to the destination, and points cursor() and limit() to non-empty
  // space. If some space was already available, does nothing.
  //
  // Return values:
  //  * true  - success (available() > 0, healthy())
  //  * false - failure (available() == 0, !healthy())
  bool Push();

  // Buffer pointers. Space between start() and limit() is available for writing
  // data to it, with cursor() pointing to the current position.
  //
  // Invariants:
  //   start() <= cursor() <= limit() (possibly all nullptr)
  //   if !healthy() then start() == cursor() == limit()
  //   if closed() then start() == cursor() == limit() == nullptr
  char* start() const { return start_; }
  char* cursor() const { return cursor_; }
  char* limit() const { return limit_; }

  // Updates the value of cursor(). Call this during writing data under cursor()
  // to indicate how much was written, or to seek within the buffer.
  //
  // Preconditions: start() <= cursor <= limit()
  void set_cursor(char* cursor);

  // Returns the amount of space available in the buffer, between cursor() and
  // limit().
  //
  // Invariant: if !healthy() then available() == 0
  size_t available() const { return PtrDistance(cursor_, limit_); }

  // Returns the buffer size, between start() and limit().
  //
  // Invariant: if !healthy() then buffer_size() == 0
  size_t buffer_size() const { return PtrDistance(start_, limit_); }

  // Returns the amount of data written to the buffer, between start() and
  // cursor().
  //
  // Invariant: if !healthy() then written_to_buffer() == 0
  size_t written_to_buffer() const { return PtrDistance(start_, cursor_); }

  // Writes a fixed number of bytes from src to the buffer, pushing data to the
  // destination as needed.
  //
  // Return values:
  //  * true  - success (src.size() bytes written)
  //  * false - failure (less than src.size() bytes written, !healthy())
  bool Write(absl::string_view src);
  bool Write(std::string&& src);
  template <typename Src>
  absl::enable_if_t<std::is_convertible<Src, absl::string_view>::value, bool>
  Write(const Src& src) {
    return Write(absl::string_view(src));
  }
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
  // The precise meaning of Flush() depends on the particular Writer. The intent
  // is to make data written so far visible, but in contrast to Close(), keeping
  // the possibility to write more data later.
  //
  // Return values:
  //  * true  - success (healthy())
  //  * false - failure (!healthy())
  virtual bool Flush(FlushType flush_type) = 0;

  // Returns the current position.
  //
  // This is not necessarily 0 after creating the Writer if it appends to a
  // destination with existing contents, or if the Writer wraps another writer
  // or output stream propagating its position.
  //
  // This may decrease when the Writer becomes unhealthy (due to buffering,
  // previously written but unflushed data may be lost).
  //
  // pos() is unchanged by Close().
  Position pos() const;

  // Returns true if this Writer supports Seek(), Size(), and Truncate().
  virtual bool SupportsRandomAccess() const { return false; }

  // Sets the current position for subsequent operations.
  //
  // Return values:
  //  * true                    - success (position is set to new_pos)
  //  * false (when healthy())  - destination ends before new_pos (position is
  //                              set to end) or seeking backwards is not
  //                              supported (position is unchanged)
  //  * false (when !healthy()) - failure
  bool Seek(Position new_pos);

  // Returns the size of the destination, i.e. the position corresponding to its
  // end.
  //
  // Return values:
  //  * true  - success (*size is set, healthy())
  //  * false - failure (!healthy())
  virtual bool Size(Position* size);

  // Returns true if this Writer supports Truncate().
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
  // Creates a Writer with the given initial state.
  explicit Writer(State state) noexcept : Object(state) {}

  // Moves the part of the object defined in this class.
  //
  // Buffer pointers do not need to satisfy their invariants during this part of
  // the move, here they are merely exchanged with nullptr and copied.
  Writer(Writer&& that) noexcept;
  Writer& operator=(Writer&& that) noexcept;

  // Writer overrides Object::Done(). Derived classes which override it further
  // should include a call to Writer::Done().
  void Done() override;

  // Marks the Writer as failed with message "Writer position overflow".
  // Always returns false.
  //
  // This can be called if the destination would exceed its maximum possible
  // size or if start_pos_ would overflow.
  //
  // Precondition: healthy()
  ABSL_ATTRIBUTE_COLD bool FailOverflow();

  // Implementation of the slow part of Push().
  //
  // Precondition: available() == 0
  virtual bool PushSlow() = 0;

  // Implementation of the slow part of Write().
  //
  // By default WriteSlow(string_view) is implemented in terms of Push();
  // WriteSlow(string&&) and WriteSlow(const Chain&) are
  // implemented in terms of WriteSlow(string_view); and WriteSlow(Chain&&) is
  // implemented in terms of WriteSlow(const Chain&).
  //
  // Precondition for WriteSlow(string_view):
  //   src.size() > available()
  //
  // Precondition for WriteSlow(string&&), WriteSlow(const Chain&), and
  // WriteSlow(Chain&&):
  //   src.size() > UnsignedMin(available(), kMaxBytesToCopy())
  virtual bool WriteSlow(absl::string_view src);
  virtual bool WriteSlow(std::string&& src);
  virtual bool WriteSlow(const Chain& src);
  virtual bool WriteSlow(Chain&& src);

  // Implementation of the slow part of Seek().
  //
  // Precondition: new_pos < start_pos_ || new_pos > pos()
  virtual bool SeekSlow(Position new_pos);

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

inline Writer::Writer(Writer&& that) noexcept
    : Object(std::move(that)),
      start_(absl::exchange(that.start_, nullptr)),
      cursor_(absl::exchange(that.cursor_, nullptr)),
      limit_(absl::exchange(that.limit_, nullptr)),
      start_pos_(absl::exchange(that.start_pos_, 0)) {}

inline Writer& Writer::operator=(Writer&& that) noexcept {
  Object::operator=(std::move(that));
  start_ = absl::exchange(that.start_, nullptr);
  cursor_ = absl::exchange(that.cursor_, nullptr);
  limit_ = absl::exchange(that.limit_, nullptr);
  start_pos_ = absl::exchange(that.start_pos_, 0);
  return *this;
}

inline void Writer::Done() {
  start_pos_ = pos();
  start_ = nullptr;
  cursor_ = nullptr;
  limit_ = nullptr;
}

inline bool Writer::Push() {
  if (ABSL_PREDICT_TRUE(available() > 0)) return true;
  return PushSlow();
}

inline void Writer::set_cursor(char* cursor) {
  RIEGELI_ASSERT(cursor >= start())
      << "Failed precondition of Writer::set_cursor(): pointer out of range";
  RIEGELI_ASSERT(cursor <= limit())
      << "Failed precondition of Writer::set_cursor(): pointer out of range";
  cursor_ = cursor;
}

inline bool Writer::Write(absl::string_view src) {
  if (ABSL_PREDICT_TRUE(src.size() <= available())) {
    if (ABSL_PREDICT_TRUE(!src.empty())) {  // memcpy(nullptr, _, 0) and
                                            // memcpy(_, nullptr, 0)
                                            // are undefined.
      std::memcpy(cursor_, src.data(), src.size());
      cursor_ += src.size();
    }
    return true;
  }
  return WriteSlow(src);
}

inline bool Writer::Write(std::string&& src) {
  if (ABSL_PREDICT_TRUE(src.size() <= available() &&
                        src.size() <= kMaxBytesToCopy())) {
    if (ABSL_PREDICT_TRUE(!src.empty())) {  // memcpy(nullptr, _, 0)
                                            // is undefined.
      std::memcpy(cursor_, src.data(), src.size());
      cursor_ += src.size();
    }
    return true;
  }
  return WriteSlow(std::move(src));
}

inline bool Writer::Write(const Chain& src) {
  if (ABSL_PREDICT_TRUE(src.size() <= available() &&
                        src.size() <= kMaxBytesToCopy())) {
    src.CopyTo(cursor_);
    cursor_ += src.size();
    return true;
  }
  return WriteSlow(src);
}

inline bool Writer::Write(Chain&& src) {
  if (ABSL_PREDICT_TRUE(src.size() <= available() &&
                        src.size() <= kMaxBytesToCopy())) {
    src.CopyTo(cursor_);
    cursor_ += src.size();
    return true;
  }
  return WriteSlow(std::move(src));
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

inline bool Writer::Seek(Position new_pos) {
  if (ABSL_PREDICT_TRUE(new_pos >= start_pos_ && new_pos <= pos())) {
    cursor_ = start_ + (new_pos - start_pos_);
    return true;
  }
  return SeekSlow(new_pos);
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_WRITER_H_
