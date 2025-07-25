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

#include <limits>
#include <optional>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/numeric/int128.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/has_absl_stringify.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/byte_fill.h"
#include "riegeli/base/bytes_ref.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/external_ref.h"
#include "riegeli/base/null_safe_memcpy.h"
#include "riegeli/base/object.h"
#include "riegeli/base/reset.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/stringify.h"

namespace riegeli {

class Reader;
class Writer;

// An sink for `AbslStringify()` which appends to a `Writer`.
class WriterStringifySink {
 public:
  // Creates a dummy `WriterStringifySink`. It must not be used.
  WriterStringifySink() = default;

  // Will write to `*dest`.
  explicit WriterStringifySink(Writer* dest ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : dest_(RIEGELI_EVAL_ASSERT_NOTNULL(dest)) {}

  WriterStringifySink(WriterStringifySink&& that) = default;
  WriterStringifySink& operator=(WriterStringifySink&& that) = default;

  // Returns the `Writer` being written to.
  Writer* dest() const { return dest_; }

  void Append(size_t length, char fill);
  template <
      typename Src,
      std::enable_if_t<std::is_convertible_v<Src, absl::string_view>, int> = 0>
  void Append(Src&& src);
  friend void AbslFormatFlush(WriterStringifySink* dest,
                              absl::string_view src) {
    dest->Append(src);
  }

 private:
  Writer* dest_ = nullptr;
};

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
// for its output to be guaranteed to be available in the destination.
class Writer : public Object {
 public:
  // The same as `Object::Close()`.
  //
  // The implementation in this class adds an assertion.
  bool Close();

  // If `write_size_hint` is not `std::nullopt`, hints that this amount of data
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
  void SetWriteSizeHint(std::optional<Position> write_size_hint);

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

  // Buffer pointers. Space between `start()` and `limit()` is available for
  // immediate writing data to it, with `cursor()` pointing to the current
  // position.
  //
  // Memory after the address to which `cursor()` is eventually moved must not
  // be clobbered.
  //
  // Non-const member functions may change buffer pointers, including changing
  // how much data around the current position are buffered.
  //
  // Invariants:
  //   `start() <= cursor() <= limit()` (possibly all `nullptr`)
  //   if `!ok()` then `start() == cursor() == limit() == nullptr`
  char* start() const ABSL_ATTRIBUTE_LIFETIME_BOUND { return start_; }
  char* cursor() const ABSL_ATTRIBUTE_LIFETIME_BOUND { return cursor_; }
  char* limit() const ABSL_ATTRIBUTE_LIFETIME_BOUND { return limit_; }

  // Increments the value of `cursor()`. Does not change `start()` nor
  // `limit()`. Call this during writing data under `cursor()` to indicate how
  // much was written.
  //
  // Precondition: `length <= available()`
  void move_cursor(size_t length);

  // Sets the value of `cursor()`. Does not change `start()` nor `limit()`. Call
  // this during writing data under `cursor()` to indicate how much was written.
  //
  // Preconditions: `start() <= cursor <= limit()`
  void set_cursor(char* cursor);

  // Returns the amount of space available in the buffer, between `cursor()` and
  // `limit()`.
  //
  // Invariant: if `!ok()` then `available() == 0`
  size_t available() const { return PtrDistance(cursor_, limit_); }

  // Returns the buffer size, between `start()` and `limit()`.
  //
  // Invariant: if `!ok()` then `start_to_limit() == 0`
  size_t start_to_limit() const { return PtrDistance(start_, limit_); }

  // Returns the amount of data written to the buffer, between `start()` and
  // `cursor()`.
  //
  // Invariant: if `!ok()` then `start_to_cursor() == 0`
  size_t start_to_cursor() const { return PtrDistance(start_, cursor_); }

  // Writes a single byte to the buffer or the destination.
  //
  // Return values:
  //  * `true`  - success (`ok()`)
  //  * `false` - failure (`!ok()`)
  bool WriteByte(uint8_t src) { return Write(static_cast<char>(src)); }

  // Writes a fixed number of bytes from `src` to the buffer and/or the
  // destination.
  //
  // Return values:
  //  * `true`  - success (`src.size()` bytes written)
  //  * `false` - failure (less than `src.size()` bytes written, `!ok()`)
  bool Write(char src);
#if __cpp_char8_t
  bool Write(char8_t src) { return Write(static_cast<char>(src)); }
#endif
  bool Write(BytesRef src);
  ABSL_ATTRIBUTE_ALWAYS_INLINE
  bool Write(const char* src) { return Write(absl::string_view(src)); }
  bool Write(ExternalRef src);
  template <typename Src,
            std::enable_if_t<SupportsExternalRefWhole<Src>::value, int> = 0>
  bool Write(Src&& src);
  bool Write(const Chain& src);
  bool Write(Chain&& src);
  bool Write(const absl::Cord& src);
  bool Write(absl::Cord&& src);
  bool Write(ByteFill src);

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
          std::conjunction_v<
              absl::HasAbslStringify<Src>,
              std::negation<std::is_convertible<Src&&, BytesRef>>,
              std::negation<std::is_convertible<Src&&, const Chain&>>,
              std::negation<std::is_convertible<Src&&, const absl::Cord&>>,
              std::negation<std::is_convertible<Src&&, ByteFill>>>,
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
  // Return values:
  //  * `true`  - success
  //  * `false` - failure (`!ok()`)
  template <typename... Srcs,
            std::enable_if_t<
                std::conjunction_v<std::bool_constant<sizeof...(Srcs) != 1>,
                                   IsStringifiable<Srcs...>>,
                int> = 0>
  bool Write(Srcs&&... srcs);

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
  //  * `true`  - success (`ok()`)
  //  * `false` - failure (`!ok()`)
  bool Flush(FlushType flush_type = FlushType::kFromProcess);

  // Returns the current position.
  //
  // This is not necessarily 0 after creating the `Writer` if it appends to a
  // destination with existing contents, or if the `Writer` wraps another writer
  // or output stream propagating its position.
  //
  // This may decrease when the `Writer` becomes not OK (due to buffering,
  // previously written but unflushed data may be lost).
  //
  // `pos()` is unchanged by a successful `Close()`.
  Position pos() const;

  // Returns the position corresponding to `start()`,
  // i.e. `pos() - start_to_cursor()`.
  Position start_pos() const { return start_pos_; }

  // Returns the position corresponding to `limit()`,
  // i.e. `pos() + available()`.
  Position limit_pos() const;

  // Returns `true` if this Writer supports `Seek()` to other positions
  // (`Seek()` to the current position is always supported) and `Size()`.
  virtual bool SupportsRandomAccess() { return false; }

  // Sets the current position for subsequent operations.
  //
  // Return values:
  //  * `true`                 - success (position is set to `new_pos`)
  //  * `false` (when `ok()`)  - destination ends before `new_pos`
  //                             (position is set to end)
  //  * `false` (when `!ok()`) - failure
  //
  // `Seek()` to the current position is always supported.
  //
  // `Seek()` to other positions is supported if `SupportsRandomAccess()` is
  // `true`.
  bool Seek(Position new_pos);

  // Returns the size of the destination, i.e. the position corresponding to its
  // end.
  //
  // Returns `std::nullopt` on failure (`!ok()`).
  //
  // `Size()` is supported if `SupportsRandomAccess()` is `true`.
  std::optional<Position> Size();

  // Returns `true` if this `Writer` supports `Truncate()`.
  virtual bool SupportsTruncate() { return SupportsRandomAccess(); }

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

  // Returns `true` if this `Writer` supports `ReadMode()`.
  virtual bool SupportsReadMode() { return false; }

  // Switches from writing to reading.
  //
  // Returns a `Reader` which reads from the current contents of the destination
  // of this `Writer`, starting from `initial_pos`.
  //
  // If the source ends before `initial_pos`, the position of the new `Reader`
  // is set to the end. The resulting `Reader` `SupportsRewind()`. If this
  // `Writer` `SupportsRandomAccess()`, the resulting `Reader` also
  // `SupportsRandomAccess()`.
  //
  // When this `Writer` is used again, its position is the same as before
  // `ReadMode()` was called, and the `Reader` becomes invalid.
  //
  // The returned `Reader` is owned by this `Writer`. The `Reader` does not own
  // its source, even if this `Writer` owns its destination.
  //
  // Returns `nullptr` on failure (`!ok()`).
  //
  // `ReadMode()` is supported if `SupportsReadMode()` is `true`.
  Reader* ReadMode(Position initial_pos) ABSL_ATTRIBUTE_LIFETIME_BOUND;

  // Supports `absl::Format(&writer, format, args...)`.
  friend void AbslFormatFlush(Writer* dest, absl::string_view src) {
    dest->Write(src);
  }

 protected:
  using Object::Object;

  // Moves the part of the object defined in this class.
  //
  // Buffer pointers do not need to satisfy their invariants during this part of
  // the move, here they are merely exchanged with `nullptr` and copied.
  Writer(Writer&& that) noexcept;
  Writer& operator=(Writer&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `Writer`. This avoids
  // constructing a temporary `Writer` and moving from it. Derived classes which
  // redefine `Reset()` should include a call to `Writer::Reset()`.
  void Reset(Closed);
  void Reset();

  // `Writer` overrides `Object::Done()` to set buffer pointers to `nullptr`.
  // Derived classes which override it further should include a call to
  // `Writer::Done()`.
  void Done() override;

  // `Writer` overrides `Object::OnFail()` to set buffer pointers to `nullptr`.
  // Derived classes which override it further should include a call to
  // `Writer::OnFail()`.
  //
  // `pos()` decreases by `start_to_cursor()` to indicate that any buffered
  // data have been lost.
  ABSL_ATTRIBUTE_COLD void OnFail() override;

  // `Writer` overrides `Object::AnnotateStatusImpl()` to annotate the status
  // with the current position.
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;

  // Marks the `Writer` as failed with message "Writer position overflow".
  // Always returns `false`.
  //
  // This can be called if the destination would exceed its maximum possible
  // size or if `start_pos()` would overflow.
  ABSL_ATTRIBUTE_COLD bool FailOverflow();

  // Implementation of `SetWriteSizeHint()`.
  virtual void SetWriteSizeHintImpl(
      ABSL_ATTRIBUTE_UNUSED std::optional<Position> write_size_hint) {}

  // Implementation of the slow part of `Push()`.
  //
  // Precondition: `available() < min_length`
  virtual bool PushSlow(size_t min_length, size_t recommended_length) = 0;

  // Sets the values of:
  //  * `start()`  - to `start`
  //  * `cursor()` - to `start + start_to_cursor`
  //  * `limit()`  - to `start + start_to_limit`
  //
  // Preconditions:
  //   [`start`..`start + start_to_limit`) is a valid byte range
  //   `start_to_cursor <= start_to_limit`
  void set_buffer(char* start = nullptr, size_t start_to_limit = 0,
                  size_t start_to_cursor = 0);

  // Implementation of the slow part of `Write()`.
  //
  // By default:
  //  * `WriteSlow(absl::string_view)` and `WriteSlow(ByteFill)` are
  //     implemented in terms of `PushSlow()`
  //  * `WriteSlow(ExternalRef)`, `WriteSlow(const Chain&)`, and
  //    `WriteSlow(const absl::Cord&)` are implemented in terms of
  //    `WriteSlow(absl::string_view)`
  //  * `WriteSlow(Chain&&)` is implemented in terms of
  //    `WriteSlow(const Chain&)`
  //  * `WriteSlow(absl::Cord&&)` is implemented in terms of
  //    `WriteSlow(const absl::Cord&)`
  //
  // Precondition for `WriteSlow(absl::string_view)`:
  //   `available() < src.size()`
  //
  // Precondition for `WriteSlow(ExternalRef)`, `WriteSlow(const Chain&)`,
  // `WriteSlow(Chain&&)`, `WriteSlow(const absl::Cord&)`,
  // `WriteSlow(absl::Cord&&), and `WriteSlow(ByteFill)`:
  //   `UnsignedMin(available(), kMaxBytesToCopy) < src.size()`
  virtual bool WriteSlow(absl::string_view src);
  virtual bool WriteSlow(ExternalRef src);
  virtual bool WriteSlow(const Chain& src);
  virtual bool WriteSlow(Chain&& src);
  virtual bool WriteSlow(const absl::Cord& src);
  virtual bool WriteSlow(absl::Cord&& src);
  virtual bool WriteSlow(ByteFill src);

  // Implementation of `Flush()`, except that the parameter is not defaulted,
  // which is problematic for virtual functions.
  //
  // By default does nothing and returns `ok()`.
  virtual bool FlushImpl(FlushType flush_type);

  // Increments the value of `start_pos()`.
  void move_start_pos(Position length);

  // Sets the value of `start_pos()`.
  void set_start_pos(Position start_pos);

  // Implementation of the slow part of `Seek()`.
  //
  // By default fails.
  //
  // Precondition: `new_pos != pos()`
  virtual bool SeekSlow(Position new_pos);

  // Implementation of `Size()`.
  //
  // By default fails.
  virtual std::optional<Position> SizeImpl();

  // Implementation of `Truncate()`.
  //
  // By default fails.
  virtual bool TruncateImpl(Position new_size);

  // Implementation of `ReadMode()`.
  //
  // By default fails.
  virtual Reader* ReadModeImpl(Position initial_pos);

 private:
  char* start_ = nullptr;
  char* cursor_ = nullptr;
  char* limit_ = nullptr;

  // Destination position corresponding to `start_`.
  //
  // Invariant:
  //   `start_pos_ <= std::numeric_limits<Position>::max() - start_to_limit()`
  Position start_pos_ = 0;
};

// Helps to implement `ReadMode()`. Stores a lazily created `Reader` of the
// given concrete type.
//
// The `ReaderClass` type does not have to be complete, except in a context when
// `ResetReader()` or `reader()` is called. This allows to have only a forward
// declaration of `ReaderClass` in the header which defines the containing
// `Writer` class.
template <typename ReaderClass>
class AssociatedReader {
 public:
  AssociatedReader() = default;

  AssociatedReader(AssociatedReader&& that) noexcept;
  AssociatedReader& operator=(AssociatedReader&& that) noexcept;

  ~AssociatedReader();

  // Destroys the contained `Reader`.
  void Reset();

  // Creates or resets the `ReaderClass` with the given arguments of the
  // constructor or `Reset()`. `ReaderClass` must be complete.
  template <typename... Args>
  ReaderClass* ResetReader(Args&&... args);

  // Returns the `ReaderClass` pointer, or `nullptr` if it was not created yet.
  // `ReaderClass` must be complete.
  ReaderClass* reader() const;

 private:
  static void Delete(Reader* reader);

  Reader* reader_ = nullptr;
};

// Implementation details follow.

inline void WriterStringifySink::Append(size_t length, char fill) {
  dest_->Write(ByteFill(length, fill));
}

template <typename Src,
          std::enable_if_t<std::is_convertible_v<Src, absl::string_view>, int>>
inline void WriterStringifySink::Append(Src&& src) {
  dest_->Write(std::forward<Src>(src));
}

inline Writer::Writer(Writer&& that) noexcept
    : Object(static_cast<Object&&>(that)),
      start_(std::exchange(that.start_, nullptr)),
      cursor_(std::exchange(that.cursor_, nullptr)),
      limit_(std::exchange(that.limit_, nullptr)),
      start_pos_(std::exchange(that.start_pos_, 0)) {}

inline Writer& Writer::operator=(Writer&& that) noexcept {
  Object::operator=(static_cast<Object&&>(that));
  start_ = std::exchange(that.start_, nullptr);
  cursor_ = std::exchange(that.cursor_, nullptr);
  limit_ = std::exchange(that.limit_, nullptr);
  start_pos_ = std::exchange(that.start_pos_, 0);
  return *this;
}

inline void Writer::Reset(Closed) {
  Object::Reset(kClosed);
  start_ = nullptr;
  cursor_ = nullptr;
  limit_ = nullptr;
  start_pos_ = 0;
}

inline void Writer::Reset() {
  Object::Reset();
  start_ = nullptr;
  cursor_ = nullptr;
  limit_ = nullptr;
  start_pos_ = 0;
}

inline bool Writer::Close() {
  AssertInitialized(start(), start_to_cursor());
  return Object::Close();
}

inline void Writer::Done() {
  start_pos_ = pos();
  set_buffer();
}

inline void Writer::SetWriteSizeHint(std::optional<Position> write_size_hint) {
  AssertInitialized(start(), start_to_cursor());
  SetWriteSizeHintImpl(write_size_hint);
}

inline bool Writer::Push(size_t min_length, size_t recommended_length) {
  if (ABSL_PREDICT_TRUE(available() >= min_length)) return true;
  AssertInitialized(start(), start_to_cursor());
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
  RIEGELI_ASSERT_GE(cursor, start())
      << "Failed precondition of Writer::set_cursor(): pointer out of range";
  RIEGELI_ASSERT_LE(cursor, limit())
      << "Failed precondition of Writer::set_cursor(): pointer out of range";
  cursor_ = cursor;
}

inline void Writer::set_buffer(char* start, size_t start_to_limit,
                               size_t start_to_cursor) {
  RIEGELI_ASSERT_LE(start_to_cursor, start_to_limit)
      << "Failed precondition of Writer::set_buffer(): length out of range";
  start_ = start;
  cursor_ = start + start_to_cursor;
  limit_ = start + start_to_limit;
}

inline bool Writer::Write(char src) {
  if (ABSL_PREDICT_FALSE(!Push())) return false;
  *cursor() = src;
  move_cursor(1);
  return true;
}

inline bool Writer::Write(BytesRef src) {
  AssertInitialized(src.data(), src.size());
  if (ABSL_PREDICT_TRUE(available() >= src.size())) {
    riegeli::null_safe_memcpy(cursor(), src.data(), src.size());
    move_cursor(src.size());
    return true;
  }
  AssertInitialized(start(), start_to_cursor());
  return WriteSlow(src);
}

inline bool Writer::Write(ExternalRef src) {
  if (ABSL_PREDICT_TRUE(available() >= src.size() &&
                        src.size() <= kMaxBytesToCopy)) {
    riegeli::null_safe_memcpy(cursor(), src.data(), src.size());
    move_cursor(src.size());
    return true;
  }
  AssertInitialized(start(), start_to_cursor());
  return WriteSlow(std::move(src));
}

template <typename Src,
          std::enable_if_t<SupportsExternalRefWhole<Src>::value, int>>
inline bool Writer::Write(Src&& src) {
  return Write(ExternalRef(std::forward<Src>(src)));
}

inline bool Writer::Write(ByteFill src) {
  if (ABSL_PREDICT_TRUE(available() >= src.size() &&
                        src.size() <= kMaxBytesToCopy)) {
    riegeli::null_safe_memset(cursor(), src.fill(),
                              IntCast<size_t>(src.size()));
    move_cursor(IntCast<size_t>(src.size()));
    return true;
  }
  AssertInitialized(start(), start_to_cursor());
  return WriteSlow(src);
}

template <typename Src,
          std::enable_if_t<
              std::conjunction_v<
                  absl::HasAbslStringify<Src>,
                  std::negation<std::is_convertible<Src&&, BytesRef>>,
                  std::negation<std::is_convertible<Src&&, const Chain&>>,
                  std::negation<std::is_convertible<Src&&, const absl::Cord&>>,
                  std::negation<std::is_convertible<Src&&, ByteFill>>>,
              int>>
bool Writer::Write(Src&& src) {
  WriterStringifySink sink(this);
  AbslStringify(sink, std::forward<Src>(src));
  return ok();
}

template <typename... Srcs,
          std::enable_if_t<
              std::conjunction_v<std::bool_constant<sizeof...(Srcs) != 1>,
                                 IsStringifiable<Srcs...>>,
              int>>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool Writer::Write(Srcs&&... srcs) {
  return (Write(std::forward<Srcs>(srcs)) && ...);
}

inline bool Writer::Flush(FlushType flush_type) {
  AssertInitialized(start(), start_to_cursor());
  return FlushImpl(flush_type);
}

inline Position Writer::pos() const {
  RIEGELI_ASSERT_LE(start_pos_,
                    std::numeric_limits<Position>::max() - start_to_limit())
      << "Failed invariant of Writer: position of buffer limit overflow";
  return start_pos_ + start_to_cursor();
}

inline Position Writer::limit_pos() const {
  RIEGELI_ASSERT_LE(start_pos_,
                    std::numeric_limits<Position>::max() - start_to_limit())
      << "Failed invariant of Writer: position of buffer limit overflow";
  return start_pos_ + start_to_limit();
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

inline bool Writer::Seek(Position new_pos) {
  if (ABSL_PREDICT_TRUE(new_pos == pos())) return true;
  AssertInitialized(start(), start_to_cursor());
  return SeekSlow(new_pos);
}

inline std::optional<Position> Writer::Size() {
  AssertInitialized(start(), start_to_cursor());
  return SizeImpl();
}

inline bool Writer::Truncate(Position new_size) {
  AssertInitialized(start(), start_to_cursor());
  return TruncateImpl(new_size);
}

inline Reader* Writer::ReadMode(Position initial_pos)
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  AssertInitialized(start(), start_to_cursor());
  return ReadModeImpl(initial_pos);
}

namespace writer_internal {

// Does `delete reader`. This is defined in a separate file because `Reader`
// might be incomplete here.
void DeleteReader(Reader* reader);

}  // namespace writer_internal

template <typename ReaderClass>
inline AssociatedReader<ReaderClass>::AssociatedReader(
    AssociatedReader&& that) noexcept
    : reader_(std::exchange(that.reader_, nullptr)) {}

template <typename ReaderClass>
inline AssociatedReader<ReaderClass>& AssociatedReader<ReaderClass>::operator=(
    AssociatedReader&& that) noexcept {
  Delete(std::exchange(reader_, std::exchange(that.reader_, nullptr)));
  return *this;
}

template <typename ReaderClass>
inline AssociatedReader<ReaderClass>::~AssociatedReader() {
  Delete(reader_);
}

template <typename ReaderClass>
inline void AssociatedReader<ReaderClass>::Reset() {
  Delete(std::exchange(reader_, nullptr));
}

template <typename ReaderClass>
template <typename... Args>
inline ReaderClass* AssociatedReader<ReaderClass>::ResetReader(Args&&... args) {
  if (ABSL_PREDICT_FALSE(reader_ == nullptr)) {
    reader_ = new ReaderClass(std::forward<Args>(args)...);
  } else {
    riegeli::Reset(*reader(), std::forward<Args>(args)...);
  }
  return reader();
}

template <typename ReaderClass>
ReaderClass* AssociatedReader<ReaderClass>::reader() const {
  return static_cast<ReaderClass*>(reader_);
}

template <typename ReaderClass>
void AssociatedReader<ReaderClass>::Delete(Reader* reader) {
  if (ABSL_PREDICT_FALSE(reader != nullptr)) {
    writer_internal::DeleteReader(reader);
  }
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_WRITER_H_
