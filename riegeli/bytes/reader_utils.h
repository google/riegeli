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

#ifndef RIEGELI_BYTES_READER_UTILS_H_
#define RIEGELI_BYTES_READER_UTILS_H_

#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <string>

#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/backward_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/varint.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Reads all remaining bytes from `*src` to `*dest`.
//
// `ReadAll(std::string*)`, `ReadAll(Chain*)`, and `ReadAll(absl::Cord*)`
// append to any existing data in `*dest`.
//
// `CopyAll(Writer*)` writes as much as could be read if reading failed, and
// reads an unspecified length (between what could be written and the
// requested length) if writing failed.
//
// `CopyAll(BackwardWriter*)` writes nothing if reading failed, and reads
// the full requested length even if writing failed.
//
// Fails `*src` with `ResourceExhaustedError(_)` if `max_size` would be
// exceeded.
//
// Return values for `ReadAll()`:
//  * `true` (`src->healthy()`)   - success
//  * `false` (`!src->healthy()`) - failure
//
// Return values for `CopyAllTo()`:
//  * `true` (`dest->healthy() && src->healthy()`)    - success
//  * `false` (`!dest->healthy() || !src->healthy()`) - failure
bool ReadAll(Reader* src, absl::string_view* dest,
             size_t max_size = std::numeric_limits<size_t>::max());
bool ReadAll(Reader* src, std::string* dest,
             size_t max_size = std::numeric_limits<size_t>::max());
bool ReadAll(Reader* src, Chain* dest,
             size_t max_size = std::numeric_limits<size_t>::max());
bool ReadAll(Reader* src, absl::Cord* dest,
             size_t max_size = std::numeric_limits<size_t>::max());
bool CopyAll(Reader* src, Writer* dest,
             Position max_size = std::numeric_limits<Position>::max());
bool CopyAll(Reader* src, BackwardWriter* dest,
             size_t max_size = std::numeric_limits<size_t>::max());

// Options for `ReadLine()`.
class ReadLineOptions {
 public:
  ReadLineOptions() noexcept {}

  // If `false`, recognized line terminator is LF ("\n").
  //
  // If `true`, recognized line terminators are LF, CR, or CRLF ("\n", "\r", or
  // "\r\n").
  //
  // Default: `false`
  ReadLineOptions& set_recognize_cr(bool recognize_cr) & {
    recognize_cr_ = recognize_cr;
    return *this;
  }
  ReadLineOptions&& set_recognize_cr(bool recognize_cr) && {
    return std::move(set_recognize_cr(recognize_cr));
  }
  bool recognize_cr() const { return recognize_cr_; }

  // If `false`, line terminators will be stripped.
  //
  // If `true`, each returned line will include its terminator if it was present
  // (it can be absent in the last line).
  //
  // Default: `false`
  ReadLineOptions& set_keep_newline(bool keep_newline) & {
    keep_newline_ = keep_newline;
    return *this;
  }
  ReadLineOptions&& set_keep_newline(bool keep_newline) && {
    return std::move(set_keep_newline(keep_newline));
  }
  bool keep_newline() const { return keep_newline_; }

  // Expected maximal line length.
  //
  // If this length is exceeded, reading fails with `ResourceExhaustedError(_)`.
  //
  // Default: `std::numeric_limits<size_t>::max()`
  ReadLineOptions& set_max_length(size_t max_length) & {
    max_length_ = max_length;
    return *this;
  }
  ReadLineOptions&& set_max_length(size_t max_length) && {
    return std::move(set_max_length(max_length));
  }
  size_t max_length() const { return max_length_; }

 private:
  bool recognize_cr_ = false;
  bool keep_newline_ = false;
  size_t max_length_ = std::numeric_limits<size_t>::max();
};

// Reads a line.
//
// Warning: if `options.recognize_cr()` is `true`, for lines terminated with CR
// `ReadLine()` reads ahead one character after the CR. If reading ahead only as
// much as needed is required, e.g. when communicating with another process,
// another implementation would be required (which would keep state between
// calls).
//
// Return values:
//  * `true`                           - success
//  * `false` (when `src->healthy()`)  - source ends (`dest->empty()`)
//  * `false` (when `!src->healthy()`) - failure (`dest->empty()`)
bool ReadLine(Reader* src, absl::string_view* dest,
              ReadLineOptions options = ReadLineOptions());
bool ReadLine(Reader* src, std::string* dest,
              ReadLineOptions options = ReadLineOptions());

// Reads a single byte.
bool ReadByte(Reader* src, uint8_t* data);

// Reads a varint.
//
// If no valid varint representation follows the cursor, the cursor is
// unchanged.
//
// `{Read,Copy}Varint{32,64}()` tolerate representations which are not the
// shortest. They reject representations longer than `kMaxLengthVarint{32,64}`
// bytes or with bits set outside the range of possible values.
//
// Warning: the proto library writes values of type `int32` (not `sint32`) by
// casting them to `uint64`, not `uint32` (negative values take 10 bytes, not
// 5), hence they must be read with `ReadVarint64()`, not `ReadVarint32()`, if
// negative values are possible.
//
// Warning: `{Read,Copy}Varint{32,64}()` may read ahead more than eventually
// needed. If reading ahead only as much as needed is required, e.g. when
// communicating with another process, use `StreamingReadVarint{32,64}()`
// instead. It is slower.

bool ReadVarint32(Reader* src, uint32_t* data);
bool ReadVarint64(Reader* src, uint64_t* data);

// Reads a varint.
//
// Accepts only the canonical representation, i.e. the shortest: rejecting a
// trailing zero byte, except for 0 itself.
bool ReadCanonicalVarint32(Reader* src, uint32_t* data);
bool ReadCanonicalVarint64(Reader* src, uint64_t* data);

// Reads a varint.
//
// Reads ahead only as much as needed, which can be required e.g. when
// communicating with another process. This is slower than
// `ReadVarint{32,64}()`.

bool StreamingReadVarint32(Reader* src, uint32_t* data);
bool StreamingReadVarint64(Reader* src, uint64_t* data);

// Copies a varint.
//
// Returns the updated `dest` after the copied value, or `nullptr` on failure.
// At least `kMaxLengthVarint{32,64}` bytes of space at `dest[]` must be
// available.
char* CopyVarint32(Reader* src, char* dest);
char* CopyVarint64(Reader* src, char* dest);

// Reads a varint from an array.
bool ReadVarint32(const char** src, const char* limit, uint32_t* data);
bool ReadVarint64(const char** src, const char* limit, uint64_t* data);

// Copies a varint from an array to an array.
char* CopyVarint32(const char** src, const char* limit, char* dest);
char* CopyVarint64(const char** src, const char* limit, char* dest);

// Implementation details follow.

inline bool ReadByte(Reader* src, uint8_t* data) {
  if (ABSL_PREDICT_FALSE(!src->Pull())) return false;
  *data = static_cast<uint8_t>(*src->cursor());
  src->move_cursor(1);
  return true;
}

inline bool ReadVarint32(Reader* src, uint32_t* data) {
  src->Pull(kMaxLengthVarint32);
  const char* cursor = src->cursor();
  if (ABSL_PREDICT_FALSE(!ReadVarint32(&cursor, src->limit(), data))) {
    return false;
  }
  src->set_cursor(cursor);
  return true;
}

inline bool ReadVarint64(Reader* src, uint64_t* data) {
  src->Pull(kMaxLengthVarint64);
  const char* cursor = src->cursor();
  if (ABSL_PREDICT_FALSE(!ReadVarint64(&cursor, src->limit(), data))) {
    return false;
  }
  src->set_cursor(cursor);
  return true;
}

inline bool ReadCanonicalVarint32(Reader* src, uint32_t* data) {
  src->Pull(kMaxLengthVarint32);
  const char* cursor = src->cursor();
  if (ABSL_PREDICT_FALSE(cursor == src->limit())) return false;
  const uint8_t first_byte = static_cast<uint8_t>(*cursor);
  if ((first_byte & 0x80) == 0) {
    // Any byte with the highest bit clear is accepted as the only byte,
    // including 0 itself.
    *data = first_byte;
    src->move_cursor(size_t{1});
    return true;
  }
  if (ABSL_PREDICT_FALSE(!ReadVarint32(&cursor, src->limit(), data))) {
    return false;
  }
  if (ABSL_PREDICT_FALSE(cursor[-1] == 0)) return false;
  src->set_cursor(cursor);
  return true;
}

inline bool ReadCanonicalVarint64(Reader* src, uint64_t* data) {
  src->Pull(kMaxLengthVarint64);
  const char* cursor = src->cursor();
  if (ABSL_PREDICT_FALSE(cursor == src->limit())) return false;
  const uint8_t first_byte = static_cast<uint8_t>(*cursor);
  if ((first_byte & 0x80) == 0) {
    // Any byte with the highest bit clear is accepted as the only byte,
    // including 0 itself.
    *data = first_byte;
    src->move_cursor(size_t{1});
    return true;
  }
  if (ABSL_PREDICT_FALSE(!ReadVarint64(&cursor, src->limit(), data))) {
    return false;
  }
  if (ABSL_PREDICT_FALSE(cursor[-1] == 0)) return false;
  src->set_cursor(cursor);
  return true;
}

namespace internal {

bool StreamingReadVarint32Slow(Reader* src, uint32_t* data);
bool StreamingReadVarint64Slow(Reader* src, uint64_t* data);

}  // namespace internal

inline bool StreamingReadVarint32(Reader* src, uint32_t* data) {
  if (ABSL_PREDICT_FALSE(!src->Pull(1, kMaxLengthVarint32))) return false;
  if (ABSL_PREDICT_TRUE(src->available() >= kMaxLengthVarint32)) {
    const char* cursor = src->cursor();
    if (ABSL_PREDICT_FALSE(!ReadVarint32(&cursor, src->limit(), data))) {
      return false;
    }
    src->set_cursor(cursor);
    return true;
  }
  return internal::StreamingReadVarint32Slow(src, data);
}

inline bool StreamingReadVarint64(Reader* src, uint64_t* data) {
  if (ABSL_PREDICT_FALSE(!src->Pull(1, kMaxLengthVarint64))) return false;
  if (ABSL_PREDICT_TRUE(src->available() >= kMaxLengthVarint64)) {
    const char* cursor = src->cursor();
    if (ABSL_PREDICT_FALSE(!ReadVarint64(&cursor, src->limit(), data))) {
      return false;
    }
    src->set_cursor(cursor);
    return true;
  }
  return internal::StreamingReadVarint64Slow(src, data);
}

inline char* CopyVarint32(Reader* src, char* dest) {
  src->Pull(kMaxLengthVarint32);
  const char* cursor = src->cursor();
  dest = CopyVarint32(&cursor, src->limit(), dest);
  src->set_cursor(cursor);
  return dest;
}

inline char* CopyVarint64(Reader* src, char* dest) {
  src->Pull(kMaxLengthVarint64);
  const char* cursor = src->cursor();
  dest = CopyVarint64(&cursor, src->limit(), dest);
  src->set_cursor(cursor);
  return dest;
}

inline bool ReadVarint32(const char** src, const char* limit, uint32_t* data) {
  const char* cursor = *src;
  uint32_t acc = 0;
  size_t shift = 0;
  uint8_t byte;
  do {
    if (ABSL_PREDICT_FALSE(cursor == limit)) return false;
    byte = static_cast<uint8_t>(*cursor++);
    acc |= (uint32_t{byte} & 0x7f) << shift;
    if (ABSL_PREDICT_FALSE(shift == (kMaxLengthVarint32 - 1) * 7)) {
      // Last possible byte.
      if (ABSL_PREDICT_FALSE(
              byte >= uint8_t{1} << (32 - (kMaxLengthVarint32 - 1) * 7))) {
        // The representation is longer than `kMaxLengthVarint32`
        // or the represented value does not fit in `uint32_t`.
        return false;
      }
      break;
    }
    shift += 7;
  } while ((byte & 0x80) != 0);
  *src = cursor;
  *data = acc;
  return true;
}

inline bool ReadVarint64(const char** src, const char* limit, uint64_t* data) {
  const char* cursor = *src;
  uint64_t acc = 0;
  size_t shift = 0;
  uint8_t byte;
  do {
    if (ABSL_PREDICT_FALSE(cursor == limit)) return false;
    byte = static_cast<uint8_t>(*cursor++);
    acc |= (uint64_t{byte} & 0x7f) << shift;
    if (ABSL_PREDICT_FALSE(shift == (kMaxLengthVarint64 - 1) * 7)) {
      // Last possible byte.
      if (ABSL_PREDICT_FALSE(
              byte >= uint8_t{1} << (64 - (kMaxLengthVarint64 - 1) * 7))) {
        // The representation is longer than `kMaxLengthVarint64`
        // or the represented value does not fit in `uint64_t`.
        return false;
      }
      break;
    }
    shift += 7;
  } while ((byte & 0x80) != 0);
  *src = cursor;
  *data = acc;
  return true;
}

inline char* CopyVarint32(const char** src, const char* limit, char* dest) {
  const char* cursor = *src;
  int remaining = kMaxLengthVarint32;
  uint8_t byte;
  do {
    if (ABSL_PREDICT_FALSE(cursor == limit)) return nullptr;
    byte = static_cast<uint8_t>(*cursor++);
    *dest++ = static_cast<char>(byte);
    if (ABSL_PREDICT_FALSE(--remaining == 0)) {
      // Last possible byte.
      if (ABSL_PREDICT_FALSE(
              byte >= uint8_t{1} << (32 - (kMaxLengthVarint32 - 1) * 7))) {
        // The representation is longer than `kMaxLengthVarint32`
        // or the represented value does not fit in `uint32_t`.
        return nullptr;
      }
      break;
    }
  } while ((byte & 0x80) != 0);
  *src = cursor;
  return dest;
}

inline char* CopyVarint64(const char** src, const char* limit, char* dest) {
  const char* cursor = *src;
  int remaining = kMaxLengthVarint64;
  uint8_t byte;
  do {
    if (ABSL_PREDICT_FALSE(cursor == limit)) return nullptr;
    byte = static_cast<uint8_t>(*cursor++);
    *dest++ = static_cast<char>(byte);
    if (ABSL_PREDICT_FALSE(--remaining == 0)) {
      // Last possible byte.
      if (ABSL_PREDICT_FALSE(
              byte >= uint8_t{1} << (64 - (kMaxLengthVarint64 - 1) * 7))) {
        // The representation is longer than `kMaxLengthVarint64`
        // or the represented value does not fit in `uint64_t`.
        return nullptr;
      }
      break;
    }
  } while ((byte & 0x80) != 0);
  *src = cursor;
  return dest;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_READER_UTILS_H_
