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
#include <memory>
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
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"

namespace riegeli {

class BackwardWriter;
class Writer;

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
  // with an `absl::InvalidArgumentError()` if not. Closes the `Reader`.
  //
  // This is an alternative to `Close()` if the presence of unread data at the
  // current position should be treated as an error.
  //
  // If `*this` reads data from an owned source, such as a decompressor reading
  // compressed data, then generally the source is verified too.
  //
  // Return values:
  //  * `true`  - success (the source ends at the former current position)
  //  * `false` - failure (the source does not end at the former current
  //                       position or the `Reader` was not OK before closing)
  bool VerifyEndAndClose();

  // Verifies that the source ends at the current position, failing the `Reader`
  // with an `absl::InvalidArgumentError()` if not.
  //
  // If `*this` reads data from an owned source, such as a decompressor reading
  // compressed data, then generally the source is verified too.
  void VerifyEnd();

  // Ensures that enough data are available in the buffer: if less than
  // `min_length` of data is available, pulls more data from the source, and
  // points `cursor()` and `limit()` to data following the current position
  // with length at least `min_length`, preferably `recommended_length`.
  //
  // The current position does not change with `Pull()`. It changes with e.g.
  // `move_cursor()` and `Read()`.
  //
  // If `recommended_length < min_length`, `recommended_length` is assumed to be
  // `min_length`.
  //
  // Return values:
  //  * `true`                 - success (`available() >= min_length`)
  //  * `false` (when `ok()`)  - source ends (`available() < min_length`)
  //  * `false` (when `!ok()`) - failure (`available() < min_length`)
  bool Pull(size_t min_length = 1, size_t recommended_length = 0);

  // Buffer pointers. Data between `start()` and `limit()` are available for
  // immediate reading, with `cursor()` pointing to the current position.
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
  // `!ok() && available() > 0`. This means that the source failed but some data
  // are already buffered and can be read before experiencing the failure.
  //
  // Invariant: if `!is_open()` then `available() == 0`
  size_t available() const { return PtrDistance(cursor_, limit_); }

  // Returns the buffer size, between `start()` and `limit()`.
  size_t start_to_limit() const { return PtrDistance(start_, limit_); }

  // Returns the amount of data read from the buffer, between `start()` and
  // `cursor()`.
  size_t start_to_cursor() const { return PtrDistance(start_, cursor_); }

  // Reads a single byte from the buffer or the source.
  //
  // Return values:
  //  * `true`                 - success (`dest` is set)
  //  * `false` (when `ok()`)  - source ends (`dest` is undefined)
  //  * `false` (when `!ok()`) - failure (`dest` is undefined)
  bool ReadChar(char& dest);
  bool ReadByte(uint8_t& dest);

  // Reads a fixed number of bytes from the buffer and/or the source to `dest`,
  // clearing any existing data in `dest`.
  //
  // `Read(absl::string_view&)` points `dest` to an array holding the data. The
  // array is valid until the next non-const operation on the `Reader`.
  //
  // If `length_read != nullptr` then sets `*length_read` to the length read.
  // This is equal to the difference between `pos()` after and before the call,
  // and is equal to `length` if `Read()` returned `true`. In overloads without
  // a `length_read` parameter it would be just `dest.size()` after the call.
  //
  // Precondition for `Read(std::string&)`:
  //   `length <= dest.max_size()`
  //
  // Return values:
  //  * `true`                 - success (`length` bytes read)
  //  * `false` (when `ok()`)  - source ends (less than `length` bytes read)
  //  * `false` (when `!ok()`) - failure (less than `length` bytes read)
  bool Read(size_t length, absl::string_view& dest);
  bool Read(size_t length, char* dest, size_t* length_read = nullptr);
  bool Read(size_t length, std::string& dest);
  bool Read(size_t length, Chain& dest);
  bool Read(size_t length, absl::Cord& dest);

  // Reads a fixed number of bytes from the buffer and/or the source to `dest`,
  // appending to any existing data in `dest`.
  //
  // If `length_read != nullptr` then sets `*length_read` to the length read.
  // This is equal to the difference between `pos()` or `dest.size()` after and
  // before the call, and is equal to `length` if `ReadAndAppend()` returned
  // `true`.
  //
  // Precondition for `ReadAndAppend(std::string&)`:
  //   `length <= dest->max_size() - dest->size()`
  //
  // Precondition for `ReadAndAppend(Chain&)` and `ReadAndAppend(absl::Cord&)`:
  //   `length <= std::numeric_limits<size_t>::max() - dest->size()`
  //
  // Return values:
  //  * `true`                 - success (`length` bytes read)
  //  * `false` (when `ok()`)  - source ends (less than `length` bytes read)
  //  * `false` (when `!ok()`) - failure (less than `length` bytes read)
  bool ReadAndAppend(size_t length, std::string& dest,
                     size_t* length_read = nullptr);
  bool ReadAndAppend(size_t length, Chain& dest, size_t* length_read = nullptr);
  bool ReadAndAppend(size_t length, absl::Cord& dest,
                     size_t* length_read = nullptr);

  // Reads a fixed number of bytes from the buffer and/or the source to `dest`.
  //
  // `Copy(Writer&)` writes as much as could be read if reading failed, and
  // reads an unspecified length (between what could be written and the
  // requested length) if writing failed.
  //
  // `Copy(BackwardWriter&)` writes nothing if reading failed, and reads an
  // unspecified length (between what could be written and the requested length)
  // if writing failed.
  //
  // If `length_read != nullptr` then sets `*length_read` to the length read.
  // This is equal to the difference between `pos()` after and before the call,
  // and is equal to `length` if `Copy()` returned `true`.
  //
  // Return values:
  //  * `true`                               - success (`length` bytes copied)
  //  * `false` (when `dest.ok() && ok()`)   - source ends
  //                                           (less than `length` bytes copied)
  //  * `false` (when `!dest.ok() || !ok()`) - failure
  //                                           (less than `length` bytes copied)
  bool Copy(Position length, Writer& dest, Position* length_read = nullptr);
  bool Copy(size_t length, BackwardWriter& dest);

  // Hints that several consecutive `Pull()`, `Read()`, or `Copy()` calls will
  // follow, reading this amount of data in total.
  //
  // This can make these calls faster by prefetching all the data at once into
  // memory. In contrast to `Pull()`, the data are not necessarily flattened
  // into a single array.
  //
  // `ReadHint()` ensures that at least `min_length`, preferably
  // `recommended_length` is available in memory.
  //
  // If `recommended_length < min_length`, `recommended_length` is assumed to be
  // `min_length`.
  void ReadHint(size_t min_length = 1, size_t recommended_length = 0);

  // Reads all remaining bytes from the buffer and/or the source to `dest`,
  // clearing any existing data in `dest`.
  //
  // Fails with `absl::ResourceExhaustedError()` if more than `max_length` bytes
  // would be read.
  //
  // `ReadAll(absl::string_view&)` points `dest` to an array holding the data.
  // The array is valid until the next non-const operation on the `Reader`.
  //
  // Return values:
  //  * `true` (`ok()`)   - success
  //  * `false` (`!ok()`) - failure
  bool ReadAll(absl::string_view& dest,
               size_t max_length = std::numeric_limits<size_t>::max());
  bool ReadAll(std::string& dest,
               size_t max_length = std::numeric_limits<size_t>::max());
  bool ReadAll(Chain& dest,
               size_t max_length = std::numeric_limits<size_t>::max());
  bool ReadAll(absl::Cord& dest,
               size_t max_length = std::numeric_limits<size_t>::max());

  // Reads all remaining bytes from the buffer and/or the source to `dest`,
  // appending to any existing data in `dest`.
  //
  // Fails with `absl::ResourceExhaustedError()` if more than `max_length` bytes
  // would be read.
  //
  // Return values:
  //  * `true` (`ok()`)   - success
  //  * `false` (`!ok()`) - failure
  bool ReadAndAppendAll(std::string& dest,
                        size_t max_length = std::numeric_limits<size_t>::max());
  bool ReadAndAppendAll(Chain& dest,
                        size_t max_length = std::numeric_limits<size_t>::max());
  bool ReadAndAppendAll(absl::Cord& dest,
                        size_t max_length = std::numeric_limits<size_t>::max());

  // Reads all remaining bytes from the buffer and/or the source to `dest`,
  //
  // `CopyAll(Writer&)` writes as much as could be read if reading failed,
  // and reads an unspecified length (between what could be written and the
  // requested length) if writing failed.
  //
  // `CopyAll(BackwardWriter&)` writes nothing if reading failed, and reads
  // the full requested length even if writing failed.
  //
  // Fails with `absl::ResourceExhaustedError()` if more than `max_length` bytes
  // would be read.
  //
  // Return values:
  //  * `true` (`dest.ok() && ok()`)    - success
  //  * `false` (`!dest.ok() || !ok()`) - failure
  bool CopyAll(Writer& dest,
               Position max_length = std::numeric_limits<Position>::max());
  bool CopyAll(BackwardWriter& dest,
               size_t max_length = std::numeric_limits<size_t>::max());

  // Synchronizes the current position to the source and discards buffered data
  // read from the source (if applicable).
  //
  // In contrast to `Close()`, keeps the possibility to read more data later.
  // What exactly does it mean for the position to be synchronized depends on
  // the source. If this is not applicable or not feasible, does nothing.
  //
  // The scope of objects to synchronize is specified by `sync_type`:
  //  * `SyncType::kFromObject`  - Propagates synchronization through owned
  //                               dependencies of the given reader.
  //  * `SyncType::kFromProcess` - Propagates synchronization through all
  //                               dependencies of the given reader.
  //                               This is the default.
  //
  // Return values:
  //  * `true`  - success (`ok()`)
  //  * `false` - failure (`!ok()`)
  bool Sync(SyncType sync_type = SyncType::kFromProcess);

  // Returns the current position.
  //
  // This is often 0 after creating the `Reader`, but not necessarily if the
  // `Reader` wraps another reader or input stream propagating its position.
  //
  // `pos()` is unchanged by `Close()`.
  Position pos() const;

  // Returns the position corresponding to `start()`,
  // i.e. `pos() - start_to_cursor()`.
  Position start_pos() const;

  // Returns the position corresponding to `limit()`,
  // i.e. `pos() + available()`.
  Position limit_pos() const { return limit_pos_; }

  // Returns `true` if this `Reader` supports efficient `Seek()`, `Skip()`, and
  // `Size()`.
  //
  // Invariant: if `SupportsRandomAccess()` then `SupportsRewind()`
  //                                         and `SupportsSize()`
  virtual bool SupportsRandomAccess() { return false; }

  // Returns `true` if this `Reader` supports `Seek()` backwards (`Seek()`
  // forwards is always supported).
  //
  // Even if `SupportsRewind()` is `true`, `Seek()` can be inefficient if
  // `SupportsRandomAccess()` is `false`.
  //
  // Invariant: if `SupportsRandomAccess()` then `SupportsRewind()`
  virtual bool SupportsRewind() { return SupportsRandomAccess(); }

  // Sets the current position for subsequent operations.
  //
  // Return values:
  //  * `true`                 - success (position is set to `new_pos`)
  //  * `false` (when `ok()`)  - source ends before `new_pos`
  //                             (position is set to the end)
  //  * `false` (when `!ok()`) - failure
  //
  // `Seek()` forwards (or backwards but within the buffer) is always supported,
  // although if `SupportsRandomAccess()` is `false`, then it is as inefficient
  // as reading and discarding the intervening data.
  //
  // `Seek()` backwards is supported and efficient if `SupportsRandomAccess()`
  // is `true`. Otherwise, if `SupportsRewind()` is `true`, `Seek()` backwards
  // is as inefficient as seeking to 0, and then reading and discarding the
  // intervening data. If `SupportsRewind()` is `false`, `Seek()` backwards is
  // not supported.
  bool Seek(Position new_pos);

  // Increments the current position. Same as `Seek(pos() + length)` if there is
  // no overflow.
  //
  // The current position might decrease if the source size decreased.
  //
  // If `length_skipped != nullptr` then sets `*length_skipped` to the length
  // skipped. This is equal to the difference between `pos()` after and before
  // the call (saturated to 0), and is equal to `length` if `Skip()` returned
  // `true`.
  //
  // Return values:
  //  * `true`                 - success (`length` bytes skipped)
  //  * `false` (when `ok()`)  - source ends before skipping `length` bytes
  //                             (position is set to the end)
  //  * `false` (when `!ok()`) - failure
  //
  // `Skip()` is always supported, although if `SupportsRandomAccess()` is
  // `false`, then it is as inefficient as reading and discarding the
  // intervening data.
  bool Skip(Position length, Position* length_skipped = nullptr);

  // Returns `true` if this `Reader` supports `Size()`.
  //
  // Invariant: if `SupportsRandomAccess()` then `SupportsSize()`.
  virtual bool SupportsSize() { return SupportsRandomAccess(); }

  // Returns the size of the source, i.e. the position corresponding to its end.
  //
  // Returns `absl::nullopt` on failure (`!ok()`).
  //
  // `Size()` is supported if `SupportsRandomAccess()` or `SupportsSize()` is
  // `true`.
  absl::optional<Position> Size();

  // Returns `true` if this `Reader` supports `NewReader()`.
  virtual bool SupportsNewReader() { return false; }

  // Returns a `Reader` which reads from the same source, but has an independent
  // current position, starting from `initial_pos`. The returned `Reader` can be
  // used concurrently with this `Reader` and other siblings.
  //
  // If the source ends before `initial_pos`, the position of the new `Reader`
  // is set to the end. The resulting `Reader` supports `Seek()` and
  // `NewReader()`.
  //
  // The new `Reader` does not own the source, even if the this `Reader` does.
  // The source of this `Reader` must not be changed until the new `Reader` is
  // closed or no longer used.
  //
  // Returns `nullptr` on failure (`!ok()`).
  //
  // `NewReader()` is supported if `SupportsNewReader()` is `true`.
  //
  // If `SupportsNewReader()` returned `true`, then `NewReader()` may be called
  // concurrently. If also `ok()` is `true`, then `NewReader()` does not return
  // `nullptr`.
  std::unique_ptr<Reader> NewReader(Position initial_pos);

 protected:
  using Object::Object;

  // Moves the part of the object defined in this class.
  //
  // Buffer pointers do not need to satisfy their invariants during this part of
  // the move, here they are merely exchanged with `nullptr` and copied.
  Reader(Reader&& that) noexcept;
  Reader& operator=(Reader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `Reader`. This avoids
  // constructing a temporary `Reader` and moving from it. Derived classes which
  // redefine `Reset()` should include a call to `Reader::Reset()`.
  void Reset(Closed);
  void Reset();

  // `Reader` overrides `Object::Done()` to set buffer pointers to `nullptr`.
  // Derived classes which override it further should include a call to
  // `Reader::Done()`.
  void Done() override;

  // `Reader` overrides `Object::AnnotateStatusImpl()` to annotate the status
  // with the current position.
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;

  // Marks the `Reader` as failed with message "Reader position overflow".
  // Always returns `false`.
  //
  // This can be called if `limit_pos()` would overflow.
  ABSL_ATTRIBUTE_COLD bool FailOverflow();

  // Implementation of `VerifyEnd()`.
  //
  // By default is implemented in terms of `Pull()`.
  virtual void VerifyEndImpl();

  // Implementation of the slow part of `Pull()`.
  //
  // Precondition: `available() < min_length`
  virtual bool PullSlow(size_t min_length, size_t recommended_length) = 0;

  // Sets the values of:
  //  * `start()`  - to `start`
  //  * `cursor()` - to `start + start_to_cursor`
  //  * `limit()`  - to `start + start_to_limit`
  //
  // Preconditions:
  //   [`start`..`start + start_to_limit`) is a valid byte range
  //   `start_to_cursor <= start_to_limit`
  void set_buffer(const char* start = nullptr, size_t start_to_limit = 0,
                  size_t start_to_cursor = 0);

  // Implementations of the slow part of `Read()`, `ReadAndAppend()`, and
  // `Copy()`.
  //
  // `ReadSlow(std::string&)`, `ReadSlow(Chain&)` and `ReadSlow(absl::Cord&)`
  // append to any existing data in `dest`.
  //
  // By default `ReadSlow(char*)` and `CopySlow(Writer&)` are implemented in
  // terms of `PullSlow()`; `ReadSlow(Chain&)` and `ReadSlow(absl::Cord&)` are
  // implemented in terms of `ReadSlow(char*)`; and `CopySlow(BackwardWriter&)`
  // is implemented in terms of `ReadSlow(char*)` and `ReadSlow(Chain&)`.
  //
  // Precondition for `ReadSlow(char*)` and `ReadSlow(std::string&)`:
  //   `available() < length`
  //
  // Precondition for `ReadSlow(Chain&)`, `ReadSlow(absl::Cord&)`,
  // `CopySlow(Writer&)`, and `CopySlow(BackwardWriter&)`:
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
  virtual bool CopySlow(Position length, Writer& dest);
  virtual bool CopySlow(size_t length, BackwardWriter& dest);

  // Implementation of the slow part of `ReadHint()`.
  //
  // By default does nothing.
  //
  // Precondition: `available() < min_length`
  virtual void ReadHintSlow(size_t min_length, size_t recommended_length);

  // Implementation of `Sync()`, except that the parameter is not defaulted,
  // which is problematic for virtual functions.
  //
  // By default does nothing and returns `ok()`.
  virtual bool SyncImpl(SyncType sync_type);

  // Increments the value of `limit_pos()`.
  void move_limit_pos(Position length);

  // Sets the value of `limit_pos()`.
  void set_limit_pos(Position limit_pos);

  // Implementation of the slow part of `Seek()` and `Skip()`.
  //
  // By default seeking forwards is implemented in terms of `Pull()`, and
  // seeking backwards fails.
  //
  // Precondition: `new_pos < start_pos() || new_pos > limit_pos()`
  virtual bool SeekSlow(Position new_pos);

  // Implementation of `Size()`.
  //
  // By default fails.
  virtual absl::optional<Position> SizeImpl();

  virtual std::unique_ptr<Reader> NewReaderImpl(Position initial_pos);

 private:
  ABSL_ATTRIBUTE_COLD bool FailMaxLengthExceeded(Position max_length);

  const char* start_ = nullptr;
  const char* cursor_ = nullptr;
  const char* limit_ = nullptr;

  // Source position corresponding to `limit_`.
  //
  // Invariant: `limit_pos_ >= start_to_limit()`
  Position limit_pos_ = 0;
};

// Combines creating a `Reader`, calling `ReadAll()`, and `VerifyEndAndClose()`
// (if the `Reader` is owned).
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the `Reader`. `Src` must support
// `Dependency<Reader*, Src&&>`, e.g. `Reader&` (not owned),
// `ChainReader<>` (owned), `std::unique_ptr<Reader>` (owned).
template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int> = 0>
absl::Status ReadAll(Src&& src, std::string& dest,
                     size_t max_length = std::numeric_limits<size_t>::max());
template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int> = 0>
absl::Status ReadAll(Src&& src, Chain& dest,
                     size_t max_length = std::numeric_limits<size_t>::max());
template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int> = 0>
absl::Status ReadAll(Src&& src, absl::Cord& dest,
                     size_t max_length = std::numeric_limits<size_t>::max());

// Combines creating a `Reader`, calling `ReadAndAppendAll()`, and
// `VerifyEndAndClose()` (if the `Reader` is owned).
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the `Reader`. `Src` must support
// `Dependency<Reader*, Src&&>`, e.g. `Reader&` (not owned),
// `ChainReader<>` (owned), `std::unique_ptr<Reader>` (owned).
template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int> = 0>
absl::Status ReadAndAppendAll(
    Src&& src, std::string& dest,
    size_t max_length = std::numeric_limits<size_t>::max());
template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int> = 0>
absl::Status ReadAndAppendAll(
    Src&& src, Chain& dest,
    size_t max_length = std::numeric_limits<size_t>::max());
template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int> = 0>
absl::Status ReadAndAppendAll(
    Src&& src, absl::Cord& dest,
    size_t max_length = std::numeric_limits<size_t>::max());

// Combines creating a `Reader` and/or `Writer` / `BackwardWriter`, calling
// `CopyAll()`, and `VerifyEndAndClose()` and/or `Close()` (if the `Reader`
// and/or `Writer` / `BackwardWriter` is owned).
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the `Reader`. `Src` must support
// `Dependency<Reader*, Src&&>`, e.g. `Reader&` (not owned),
// `ChainReader<>` (owned), `std::unique_ptr<Reader>` (owned).
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the `Writer` / `BackwardWriter`. `Dest` must support
// `Dependency<Writer*, Dest&&>`, e.g. `Writer&` (not owned),
// `ChainWriter<>` (owned). `std::unique_ptr<Writer>` (owned).
// Analogously for `BackwardWriter`.
template <typename Src, typename Dest,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value &&
                               IsValidDependency<Writer*, Dest&&>::value,
                           int> = 0>
absl::Status CopyAll(
    Src&& src, Dest&& dest,
    Position max_length = std::numeric_limits<Position>::max());
template <
    typename Src, typename Dest,
    std::enable_if_t<IsValidDependency<Reader*, Src&&>::value &&
                         IsValidDependency<BackwardWriter*, Dest&&>::value,
                     int> = 0>
absl::Status CopyAll(Src&& src, Dest&& dest,
                     size_t max_length = std::numeric_limits<size_t>::max());

// Implementation details follow.

inline Reader::Reader(Reader&& that) noexcept
    : Object(static_cast<Object&&>(that)),
      start_(std::exchange(that.start_, nullptr)),
      cursor_(std::exchange(that.cursor_, nullptr)),
      limit_(std::exchange(that.limit_, nullptr)),
      limit_pos_(std::exchange(that.limit_pos_, 0)) {}

inline Reader& Reader::operator=(Reader&& that) noexcept {
  Object::operator=(static_cast<Object&&>(that));
  start_ = std::exchange(that.start_, nullptr);
  cursor_ = std::exchange(that.cursor_, nullptr);
  limit_ = std::exchange(that.limit_, nullptr);
  limit_pos_ = std::exchange(that.limit_pos_, 0);
  return *this;
}

inline void Reader::Reset(Closed) {
  Object::Reset(kClosed);
  start_ = nullptr;
  cursor_ = nullptr;
  limit_ = nullptr;
  limit_pos_ = 0;
}

inline void Reader::Reset() {
  Object::Reset();
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

inline void Reader::VerifyEnd() { VerifyEndImpl(); }

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

inline void Reader::set_buffer(const char* start, size_t start_to_limit,
                               size_t start_to_cursor) {
  RIEGELI_ASSERT_LE(start_to_cursor, start_to_limit)
      << "Failed precondition of Reader::set_buffer(): length out of range";
  start_ = start;
  cursor_ = start + start_to_cursor;
  limit_ = start + start_to_limit;
}

inline bool Reader::ReadChar(char& dest) {
  if (ABSL_PREDICT_FALSE(!Pull())) return false;
  dest = *cursor();
  move_cursor(1);
  return true;
}

inline bool Reader::ReadByte(uint8_t& dest) {
  if (ABSL_PREDICT_FALSE(!Pull())) return false;
  dest = static_cast<uint8_t>(*cursor());
  move_cursor(1);
  return true;
}

inline bool Reader::Read(size_t length, absl::string_view& dest) {
  const bool pull_ok = Pull(length);
  if (ABSL_PREDICT_FALSE(!pull_ok)) length = available();
  dest = absl::string_view(cursor(), length);
  move_cursor(length);
  return pull_ok;
}

inline bool Reader::Read(size_t length, char* dest, size_t* length_read) {
  if (ABSL_PREDICT_TRUE(available() >= length)) {
    // `std::memcpy(nullptr, _, 0)` and `std::memcpy(_, nullptr, 0)` are
    // undefined.
    if (ABSL_PREDICT_TRUE(length > 0)) {
      std::memcpy(dest, cursor(), length);
      move_cursor(length);
    }
    if (length_read != nullptr) *length_read = length;
    return true;
  }
  if (length_read == nullptr) {
    return ReadSlow(length, dest);
  } else {
    const Position pos_before = pos();
    const bool read_ok = ReadSlow(length, dest);
    RIEGELI_ASSERT_GE(pos(), pos_before)
        << "Reader::ReadSlow(char*) decreased pos()";
    RIEGELI_ASSERT_LE(pos() - pos_before, length)
        << "Reader::ReadSlow(char*) read more than requested";
    if (ABSL_PREDICT_FALSE(!read_ok)) {
      *length_read = IntCast<size_t>(pos() - pos_before);
      return false;
    }
    RIEGELI_ASSERT_EQ(pos() - pos_before, length)
        << "Reader::ReadSlow(char*) read less than requested but returned true";
    *length_read = length;
    return true;
  }
}

inline bool Reader::Read(size_t length, std::string& dest) {
  RIEGELI_CHECK_LE(length, dest.max_size())
      << "Failed precondition of Reader::Read(string&): "
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

inline bool Reader::ReadAndAppend(size_t length, std::string& dest,
                                  size_t* length_read) {
  RIEGELI_CHECK_LE(length, dest.max_size() - dest.size())
      << "Failed precondition of Reader::ReadAndAppend(string&): "
         "string size overflow";
  if (ABSL_PREDICT_TRUE(available() >= length)) {
    dest.append(cursor(), length);
    move_cursor(length);
    if (length_read != nullptr) *length_read = length;
    return true;
  }
  if (length_read == nullptr) {
    return ReadSlow(length, dest);
  } else {
    const size_t dest_size_before = dest.size();
    const bool read_ok = ReadSlow(length, dest);
    RIEGELI_ASSERT_GE(dest.size(), dest_size_before)
        << "Reader::ReadSlow(string&) decreased dest.size()";
    RIEGELI_ASSERT_LE(dest.size() - dest_size_before, length)
        << "Reader::ReadSlow(string&) read more than requested";
    if (ABSL_PREDICT_FALSE(!read_ok)) {
      *length_read = dest.size() - dest_size_before;
      return false;
    }
    RIEGELI_ASSERT_EQ(dest.size() - dest_size_before, length)
        << "Reader::ReadSlow(string&) read less than requested "
           "but returned true";
    *length_read = length;
    return true;
  }
}

inline bool Reader::ReadAndAppend(size_t length, Chain& dest,
                                  size_t* length_read) {
  RIEGELI_CHECK_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadAndAppend(Chain&): "
         "Chain size overflow";
  if (ABSL_PREDICT_TRUE(available() >= length && length <= kMaxBytesToCopy)) {
    dest.Append(absl::string_view(cursor(), length));
    move_cursor(length);
    if (length_read != nullptr) *length_read = length;
    return true;
  }
  if (length_read == nullptr) {
    return ReadSlow(length, dest);
  } else {
    const size_t dest_size_before = dest.size();
    const bool read_ok = ReadSlow(length, dest);
    RIEGELI_ASSERT_GE(dest.size(), dest_size_before)
        << "Reader::ReadSlow(Chain&) decreased dest.size()";
    RIEGELI_ASSERT_LE(dest.size() - dest_size_before, length)
        << "Reader::ReadSlow(Chain&) read more than requested";
    if (ABSL_PREDICT_FALSE(!read_ok)) {
      *length_read = dest.size() - dest_size_before;
      return false;
    }
    RIEGELI_ASSERT_EQ(dest.size() - dest_size_before, length)
        << "Reader::ReadSlow(Chain&) read less than requested "
           "but returned true";
    *length_read = length;
    return true;
  }
}

inline bool Reader::ReadAndAppend(size_t length, absl::Cord& dest,
                                  size_t* length_read) {
  RIEGELI_CHECK_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of Reader::ReadAndAppend(Cord&): "
         "Cord size overflow";
  if (ABSL_PREDICT_TRUE(available() >= length && length <= kMaxBytesToCopy)) {
    dest.Append(absl::string_view(cursor(), length));
    move_cursor(length);
    if (length_read != nullptr) *length_read = length;
    return true;
  }
  if (length_read == nullptr) {
    return ReadSlow(length, dest);
  } else {
    const size_t dest_size_before = dest.size();
    const bool read_ok = ReadSlow(length, dest);
    RIEGELI_ASSERT_GE(dest.size(), dest_size_before)
        << "Reader::ReadSlow(Cord&) decreased dest.size()";
    RIEGELI_ASSERT_LE(dest.size() - dest_size_before, length)
        << "Reader::ReadSlow(Cord&) read more than requested";
    if (ABSL_PREDICT_FALSE(!read_ok)) {
      *length_read = dest.size() - dest_size_before;
      return false;
    }
    RIEGELI_ASSERT_EQ(dest.size() - dest_size_before, length)
        << "Reader::ReadSlow(Cord&) read less than requested but returned true";
    *length_read = length;
    return true;
  }
}

inline void Reader::ReadHint(size_t min_length, size_t recommended_length) {
  if (ABSL_PREDICT_TRUE(available() >= min_length)) return;
  ReadHintSlow(min_length, recommended_length);
}

inline bool Reader::Sync(SyncType sync_type) { return SyncImpl(sync_type); }

inline Position Reader::pos() const {
  RIEGELI_ASSERT_GE(limit_pos_, start_to_limit())
      << "Failed invariant of Reader: negative position of buffer start";
  return limit_pos_ - available();
}

inline Position Reader::start_pos() const {
  RIEGELI_ASSERT_GE(limit_pos_, start_to_limit())
      << "Failed invariant of Reader: negative position of buffer start";
  return limit_pos_ - start_to_limit();
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

inline bool Reader::Skip(Position length, Position* length_skipped) {
  if (ABSL_PREDICT_TRUE(available() >= length)) {
    move_cursor(IntCast<size_t>(length));
    if (length_skipped != nullptr) *length_skipped = length;
    return true;
  }
  if (ABSL_PREDICT_FALSE(length >
                         std::numeric_limits<Position>::max() - pos())) {
    if (length_skipped != nullptr) *length_skipped = 0;
    return FailOverflow();
  }
  if (length_skipped == nullptr) {
    return SeekSlow(pos() + length);
  } else {
    const Position pos_before = pos();
    const bool seek_ok = SeekSlow(pos_before + length);
    // `SeekSlow()` could have decreased `pos()` if the source decreased its
    // size.
    RIEGELI_ASSERT_LE(pos(), pos_before + length)
        << "Reader::SeekSlow() skipped more than requested";
    if (ABSL_PREDICT_FALSE(!seek_ok)) {
      *length_skipped = SaturatingSub(pos(), pos_before);
      return false;
    }
    RIEGELI_ASSERT_EQ(pos(), pos_before + length)
        << "Reader::SeekSlow() skipped less than requested but returned true";
    *length_skipped = length;
    return true;
  }
}

inline absl::optional<Position> Reader::Size() { return SizeImpl(); }

namespace reader_internal {

template <typename Src, typename Dest>
inline absl::Status ReadAllImpl(Src&& src, Dest& dest, size_t max_length) {
  Dependency<Reader*, Src&&> src_ref(std::forward<Src>(src));
  absl::Status status;
  if (ABSL_PREDICT_FALSE(!src_ref->ReadAll(dest, max_length))) {
    status = src_ref->status();
  }
  if (src_ref.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_ref->VerifyEndAndClose())) {
      status.Update(src_ref->status());
    }
  }
  return status;
}

template <typename Src, typename Dest>
inline absl::Status ReadAndAppendAllImpl(Src&& src, Dest& dest,
                                         size_t max_length) {
  Dependency<Reader*, Src&&> src_ref(std::forward<Src>(src));
  absl::Status status;
  if (ABSL_PREDICT_FALSE(!src_ref->ReadAndAppendAll(dest, max_length))) {
    status = src_ref->status();
  }
  if (src_ref.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_ref->VerifyEndAndClose())) {
      status.Update(src_ref->status());
    }
  }
  return status;
}

template <typename WriterType, typename LengthType, typename Src, typename Dest>
inline absl::Status CopyAllImpl(Src&& src, Dest&& dest, LengthType max_length) {
  Dependency<Reader*, Src&&> src_ref(std::forward<Src>(src));
  Dependency<WriterType*, Dest&&> dest_ref(std::forward<Dest>(dest));
  absl::Status status;
  if (ABSL_PREDICT_FALSE(!src_ref->CopyAll(*dest_ref, max_length))) {
    status = !dest_ref->ok() ? dest_ref->status() : src_ref->status();
  }
  if (dest_ref.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_ref->Close())) {
      status.Update(dest_ref->status());
    }
  }
  if (src_ref.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_ref->VerifyEndAndClose())) {
      status.Update(src_ref->status());
    }
  }
  return status;
}

}  // namespace reader_internal

template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int>>
inline absl::Status ReadAll(Src&& src, std::string& dest, size_t max_length) {
  return reader_internal::ReadAllImpl(std::forward<Src>(src), dest, max_length);
}

template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int>>
inline absl::Status ReadAll(Src&& src, Chain& dest, size_t max_length) {
  return reader_internal::ReadAllImpl(std::forward<Src>(src), dest, max_length);
}

template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int>>
inline absl::Status ReadAll(Src&& src, absl::Cord& dest, size_t max_length) {
  return reader_internal::ReadAllImpl(std::forward<Src>(src), dest, max_length);
}

template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int>>
inline absl::Status ReadAndAppendAll(Src&& src, std::string& dest,
                                     size_t max_length) {
  return reader_internal::ReadAndAppendAllImpl(std::forward<Src>(src), dest,
                                               max_length);
}

template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int>>
inline absl::Status ReadAndAppendAll(Src&& src, Chain& dest,
                                     size_t max_length) {
  return reader_internal::ReadAndAppendAllImpl(std::forward<Src>(src), dest,
                                               max_length);
}

template <typename Src,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value, int>>
inline absl::Status ReadAndAppendAll(Src&& src, absl::Cord& dest,
                                     size_t max_length) {
  return reader_internal::ReadAndAppendAllImpl(std::forward<Src>(src), dest,
                                               max_length);
}

template <typename Src, typename Dest,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value &&
                               IsValidDependency<Writer*, Dest&&>::value,
                           int>>
absl::Status CopyAll(Src&& src, Dest&& dest, Position max_length) {
  return reader_internal::CopyAllImpl<Writer>(
      std::forward<Src>(src), std::forward<Dest>(dest), max_length);
}

template <
    typename Src, typename Dest,
    std::enable_if_t<IsValidDependency<Reader*, Src&&>::value &&
                         IsValidDependency<BackwardWriter*, Dest&&>::value,
                     int>>
absl::Status CopyAll(Src&& src, Dest&& dest, size_t max_length) {
  return reader_internal::CopyAllImpl<BackwardWriter>(
      std::forward<Src>(src), std::forward<Dest>(dest), max_length);
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_READER_H_
