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
#include <string>
#include <utility>

#include "riegeli/base/assert.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/object.h"
#include "riegeli/base/string_view.h"

namespace riegeli {

// Specifies how durable data should be.
enum class FlushType {
  // Just write the data to the corresponding destination, e.g. file.
  kFromObject = 0,
  // Attempt to ensure that data survives process crash.
  kFromProcess = 1,
  // Attempt to ensure that data survives operating system crash.
  kFromMachine = 2,
};

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
// its output to be available in the destination. Otherwise the destination is
// cancelled, which means clearing it if the destination supports that.
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

  // Space between start() and limit() is available for writing data to it, with
  // cursor() pointing to the current position.
  //
  // Invariants:
  //   start() <= cursor() <= limit() (possibly all nullptr)
  //   if !healthy() then start() == cursor() == limit()
  char* start() const { return start_; }
  char* cursor() const { return cursor_; }
  char* limit() const { return limit_; }

  // Updates the value of cursor(). Call this during writing data under cursor()
  // to indicate how much was written, or to seek within the buffer.
  //
  // Preconditions: start() <= cursor <= limit()
  void set_cursor(char* cursor);

  // Returns the amount of space available between cursor() and limit().
  //
  // Invariant: if !healthy() then available() == 0
  size_t available() const { return limit_ - cursor_; }

  // Writes a fixed number of bytes from src to the buffer, pushing data to the
  // destination as needed.
  //
  // Return values:
  //  * true  - success (src.size() bytes written)
  //  * false - failure (less than src.size() bytes written, !healthy())
  bool Write(string_view src);
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
  // The precise meaning of Flush() depends on the particular Writer. The intent
  // is to make data written so far visible, but in contrast to Close(), keeping
  // the possibility to write more data later.
  //
  // Return values:
  //  * true                    - success (pushed and synced, healthy())
  //  * false (when healthy())  - failure to sync
  //  * false (when !healthy()) - failure to push
  virtual bool Flush(FlushType flush_type) { return false; }

  // Returns the current position.
  //
  // This is not necessarily 0 after creating the Writer if it appends to a
  // destination with existing contents, or if the Writer wraps another writer
  // or output stream propagating its position.
  //
  // This may decrease when the Writer becomes unhealthy (due to buffering,
  // previously written but unflushed data may be lost).
  //
  // This is always 0 when the Writer is closed.
  Position pos() const { return start_pos_ + written_to_buffer(); }

  // Returns true if this Writer supports Seek(), Size(), and Truncate().
  virtual bool SupportsRandomAccess() const { return false; }

  // Sets the current position for subsequent operations.
  //
  // Return values:
  //  * true                    - success (seeked to new_pos)
  //  * false (when healthy())  - destination ends before (seeked to end)
  //  * false (when !healthy()) - failure
  bool Seek(Position new_pos);

  // Returns the size of the destination, i.e. the position corresponding to its
  // end.
  //
  // Return values:
  //  * true  - success (*size is set, healthy())
  //  * false - failure (healthy() is unchanged)
  virtual bool Size(Position* size) const { return false; }

  // Discards the part of the destination after the current position.
  //
  // Return values:
  //  * true  - success (destination truncated, healthy())
  //  * false - failure (!healthy())
  virtual bool Truncate() { return false; }

 protected:
  Writer() = default;

  // Moves the part of the object defined in this class.
  //
  // Precondition: &src != this
  Writer(Writer&& src) noexcept;
  void operator=(Writer&& src) noexcept;

  // Writer provides a partial override of Object::Done(). Derived classes must
  // override it further and include a call to Writer::Done().
  virtual void Done() override = 0;

  // Resets cursor_ and limit_ to start_. Marks the Writer as failed with the
  // specified message. Always returns false.
  //
  // Precondition: healthy()
  RIEGELI_ATTRIBUTE_COLD bool Fail(string_view message);

  // Implementation of the slow part of Push().
  //
  // Precondition: available() == 0
  virtual bool PushSlow() = 0;

  // Returns the amount of data written to the buffer, between start() and
  // cursor().
  size_t written_to_buffer() const { return cursor_ - start_; }

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
  virtual bool WriteSlow(string_view src);
  virtual bool WriteSlow(std::string&& src);
  virtual bool WriteSlow(const Chain& src);
  virtual bool WriteSlow(Chain&& src);

  // Implementation of the slow part of Seek().
  //
  // Precondition: new_pos < start_pos_ || new_pos > pos()
  virtual bool SeekSlow(Position new_pos) { return false; }

  // Destination position corresponding to limit_.
  Position limit_pos() const { return start_pos_ + (limit_ - start_); }

  char* start_ = nullptr;
  char* cursor_ = nullptr;
  char* limit_ = nullptr;

  // Destination position corresponding to start_.
  Position start_pos_ = 0;
};

// Implementation details follow.

inline Writer::Writer(Writer&& src) noexcept
    : start_(riegeli::exchange(src.start_, nullptr)),
      cursor_(riegeli::exchange(src.cursor_, nullptr)),
      limit_(riegeli::exchange(src.limit_, nullptr)),
      start_pos_(riegeli::exchange(src.start_pos_, 0)) {}

inline void Writer::operator=(Writer&& src) noexcept {
  RIEGELI_ASSERT(&src != this);
  Object::operator=(std::move(src));
  start_ = riegeli::exchange(src.start_, nullptr);
  cursor_ = riegeli::exchange(src.cursor_, nullptr);
  limit_ = riegeli::exchange(src.limit_, nullptr);
  start_pos_ = riegeli::exchange(src.start_pos_, 0);
}

inline void Writer::Done() {
  start_ = nullptr;
  cursor_ = nullptr;
  limit_ = nullptr;
  start_pos_ = 0;
}

inline bool Writer::Push() {
  if (RIEGELI_LIKELY(available() > 0)) return true;
  return PushSlow();
}

inline void Writer::set_cursor(char* cursor) {
  RIEGELI_ASSERT(cursor >= start());
  RIEGELI_ASSERT(cursor <= limit());
  cursor_ = cursor;
}

inline bool Writer::Write(string_view src) {
  if (RIEGELI_LIKELY(src.size() <= available())) {
    if (!src.empty()) {  // memcpy(nullptr, _, 0) and
                         // memcpy(_, nullptr, 0) are undefined.
      std::memcpy(cursor_, src.data(), src.size());
      cursor_ += src.size();
    }
    return true;
  }
  return WriteSlow(src);
}

inline bool Writer::Write(std::string&& src) {
  if (RIEGELI_LIKELY(src.size() <= available() &&
                     src.size() <= kMaxBytesToCopy())) {
    if (!src.empty()) {  // memcpy(nullptr, _, 0) is undefined.
      std::memcpy(cursor_, src.data(), src.size());
      cursor_ += src.size();
    }
    return true;
  }
  return WriteSlow(std::move(src));
}

inline bool Writer::Write(const char* src) { return Write(string_view(src)); }

inline bool Writer::Write(const Chain& src) {
  if (RIEGELI_LIKELY(src.size() <= available() &&
                     src.size() <= kMaxBytesToCopy())) {
    src.CopyTo(cursor_);
    cursor_ += src.size();
    return true;
  }
  return WriteSlow(src);
}

inline bool Writer::Write(Chain&& src) {
  if (RIEGELI_LIKELY(src.size() <= available() &&
                     src.size() <= kMaxBytesToCopy())) {
    src.CopyTo(cursor_);
    cursor_ += src.size();
    return true;
  }
  return WriteSlow(std::move(src));
}

inline bool Writer::Seek(Position new_pos) {
  if (RIEGELI_LIKELY(new_pos >= start_pos_ && new_pos <= pos())) {
    cursor_ = start_ + (new_pos - start_pos_);
    return true;
  }
  return SeekSlow(new_pos);
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_WRITER_H_
