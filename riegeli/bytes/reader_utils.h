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
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
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
// Fails `*src` with `absl::ResourceExhaustedError()` if `max_size` would be
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
  // If this length is exceeded, reading fails with
  // `absl::ResourceExhaustedError()`.
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
absl::optional<uint8_t> ReadByte(Reader* src);

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

absl::optional<uint32_t> ReadVarint32(Reader* src);
absl::optional<uint64_t> ReadVarint64(Reader* src);

// Reads a varint.
//
// Accepts only the canonical representation, i.e. the shortest: rejecting a
// trailing zero byte, except for 0 itself.
absl::optional<uint32_t> ReadCanonicalVarint32(Reader* src);
absl::optional<uint64_t> ReadCanonicalVarint64(Reader* src);

// Reads a varint.
//
// Reads ahead only as much as needed, which can be required e.g. when
// communicating with another process. This is slower than
// `ReadVarint{32,64}()`.

absl::optional<uint32_t> StreamingReadVarint32(Reader* src);
absl::optional<uint64_t> StreamingReadVarint64(Reader* src);

// Copies a varint.
//
// Returns the updated `dest` after the copied value, or `nullptr` on failure.
// At least `kMaxLengthVarint{32,64}` bytes of space at `dest[]` must be
// available.
absl::optional<char*> CopyVarint32(Reader* src, char* dest);
absl::optional<char*> CopyVarint64(Reader* src, char* dest);

// Reads a varint from an array.

template <typename T>
struct ReadFromStringResult {
  T value;             // Parsed value.
  const char* cursor;  // Pointer to remaining data.
};

absl::optional<ReadFromStringResult<uint32_t>> ReadVarint32(const char* src,
                                                            const char* limit);
absl::optional<ReadFromStringResult<uint64_t>> ReadVarint64(const char* src,
                                                            const char* limit);

// Copies a varint from an array to an array.
absl::optional<ReadFromStringResult<char*>> CopyVarint32(const char* src,
                                                         const char* limit,
                                                         char* dest);
absl::optional<ReadFromStringResult<char*>> CopyVarint64(const char* src,
                                                         const char* limit,
                                                         char* dest);

// Implementation details follow.

inline absl::optional<uint8_t> ReadByte(Reader* src) {
  if (ABSL_PREDICT_FALSE(!src->Pull())) return absl::nullopt;
  const uint8_t data = static_cast<uint8_t>(*src->cursor());
  src->move_cursor(1);
  return data;
}

inline absl::optional<uint32_t> ReadVarint32(Reader* src) {
  src->Pull(kMaxLengthVarint32);
  const absl::optional<ReadFromStringResult<uint32_t>> result =
      ReadVarint32(src->cursor(), src->limit());
  if (ABSL_PREDICT_FALSE(result == absl::nullopt)) return absl::nullopt;
  src->set_cursor(result->cursor);
  return result->value;
}

inline absl::optional<uint64_t> ReadVarint64(Reader* src) {
  src->Pull(kMaxLengthVarint64);
  const absl::optional<ReadFromStringResult<uint64_t>> result =
      ReadVarint64(src->cursor(), src->limit());
  if (ABSL_PREDICT_FALSE(result == absl::nullopt)) return absl::nullopt;
  src->set_cursor(result->cursor);
  return result->value;
}

inline absl::optional<uint32_t> ReadCanonicalVarint32(Reader* src) {
  src->Pull(kMaxLengthVarint32);
  if (ABSL_PREDICT_FALSE(src->cursor() == src->limit())) return absl::nullopt;
  const uint8_t first_byte = static_cast<uint8_t>(*src->cursor());
  if ((first_byte & 0x80) == 0) {
    // Any byte with the highest bit clear is accepted as the only byte,
    // including 0 itself.
    src->move_cursor(size_t{1});
    return first_byte;
  }
  const absl::optional<ReadFromStringResult<uint32_t>> result =
      ReadVarint32(src->cursor(), src->limit());
  if (ABSL_PREDICT_FALSE(result == absl::nullopt)) return absl::nullopt;
  if (ABSL_PREDICT_FALSE(result->cursor[-1] == 0)) return absl::nullopt;
  src->set_cursor(result->cursor);
  return result->value;
}

inline absl::optional<uint64_t> ReadCanonicalVarint64(Reader* src) {
  src->Pull(kMaxLengthVarint64);
  if (ABSL_PREDICT_FALSE(src->cursor() == src->limit())) return absl::nullopt;
  const uint8_t first_byte = static_cast<uint8_t>(*src->cursor());
  if ((first_byte & 0x80) == 0) {
    // Any byte with the highest bit clear is accepted as the only byte,
    // including 0 itself.
    src->move_cursor(size_t{1});
    return first_byte;
  }
  const absl::optional<ReadFromStringResult<uint64_t>> result =
      ReadVarint64(src->cursor(), src->limit());
  if (ABSL_PREDICT_FALSE(result == absl::nullopt)) return absl::nullopt;
  if (ABSL_PREDICT_FALSE(result->cursor[-1] == 0)) return absl::nullopt;
  src->set_cursor(result->cursor);
  return result->value;
}

namespace internal {

absl::optional<uint32_t> StreamingReadVarint32Slow(Reader* src);
absl::optional<uint64_t> StreamingReadVarint64Slow(Reader* src);

}  // namespace internal

inline absl::optional<uint32_t> StreamingReadVarint32(Reader* src) {
  if (ABSL_PREDICT_FALSE(!src->Pull(1, kMaxLengthVarint32))) {
    return absl::nullopt;
  }
  if (ABSL_PREDICT_TRUE(src->available() >= kMaxLengthVarint32)) {
    const absl::optional<ReadFromStringResult<uint32_t>> result =
        ReadVarint32(src->cursor(), src->limit());
    if (ABSL_PREDICT_FALSE(result == absl::nullopt)) return absl::nullopt;
    src->set_cursor(result->cursor);
    return result->value;
  }
  return internal::StreamingReadVarint32Slow(src);
}

inline absl::optional<uint64_t> StreamingReadVarint64(Reader* src) {
  if (ABSL_PREDICT_FALSE(!src->Pull(1, kMaxLengthVarint64))) {
    return absl::nullopt;
  }
  if (ABSL_PREDICT_TRUE(src->available() >= kMaxLengthVarint64)) {
    const absl::optional<ReadFromStringResult<uint64_t>> result =
        ReadVarint64(src->cursor(), src->limit());
    if (ABSL_PREDICT_FALSE(result == absl::nullopt)) return absl::nullopt;
    src->set_cursor(result->cursor);
    return result->value;
  }
  return internal::StreamingReadVarint64Slow(src);
}

inline absl::optional<char*> CopyVarint32(Reader* src, char* dest) {
  src->Pull(kMaxLengthVarint32);
  const absl::optional<ReadFromStringResult<char*>> result =
      CopyVarint32(src->cursor(), src->limit(), dest);
  if (ABSL_PREDICT_FALSE(result == absl::nullopt)) return absl::nullopt;
  src->set_cursor(result->cursor);
  return result->value;
}

inline absl::optional<char*> CopyVarint64(Reader* src, char* dest) {
  src->Pull(kMaxLengthVarint64);
  const absl::optional<ReadFromStringResult<char*>> result =
      CopyVarint64(src->cursor(), src->limit(), dest);
  if (ABSL_PREDICT_FALSE(result == absl::nullopt)) return absl::nullopt;
  src->set_cursor(result->cursor);
  return result->value;
}

inline absl::optional<ReadFromStringResult<uint32_t>> ReadVarint32(
    const char* src, const char* limit) {
  uint32_t result = 0;
  size_t shift = 0;
  uint8_t byte;
  do {
    if (ABSL_PREDICT_FALSE(src == limit)) return absl::nullopt;
    byte = static_cast<uint8_t>(*src++);
    result |= (uint32_t{byte} & 0x7f) << shift;
    if (ABSL_PREDICT_FALSE(shift == (kMaxLengthVarint32 - 1) * 7)) {
      // Last possible byte.
      if (ABSL_PREDICT_FALSE(
              byte >= uint8_t{1} << (32 - (kMaxLengthVarint32 - 1) * 7))) {
        // The representation is longer than `kMaxLengthVarint32`
        // or the represented value does not fit in `uint32_t`.
        return absl::nullopt;
      }
      break;
    }
    shift += 7;
  } while ((byte & 0x80) != 0);
  return ReadFromStringResult<uint32_t>{result, src};
}

inline absl::optional<ReadFromStringResult<uint64_t>> ReadVarint64(
    const char* src, const char* limit) {
  uint64_t result = 0;
  size_t shift = 0;
  uint8_t byte;
  do {
    if (ABSL_PREDICT_FALSE(src == limit)) return absl::nullopt;
    byte = static_cast<uint8_t>(*src++);
    result |= (uint64_t{byte} & 0x7f) << shift;
    if (ABSL_PREDICT_FALSE(shift == (kMaxLengthVarint64 - 1) * 7)) {
      // Last possible byte.
      if (ABSL_PREDICT_FALSE(
              byte >= uint8_t{1} << (64 - (kMaxLengthVarint64 - 1) * 7))) {
        // The representation is longer than `kMaxLengthVarint64`
        // or the represented value does not fit in `uint64_t`.
        return absl::nullopt;
      }
      break;
    }
    shift += 7;
  } while ((byte & 0x80) != 0);
  return ReadFromStringResult<uint64_t>{result, src};
}

inline absl::optional<ReadFromStringResult<char*>> CopyVarint32(
    const char* src, const char* limit, char* dest) {
  int remaining = kMaxLengthVarint32;
  uint8_t byte;
  do {
    if (ABSL_PREDICT_FALSE(src == limit)) return absl::nullopt;
    byte = static_cast<uint8_t>(*src++);
    *dest++ = static_cast<char>(byte);
    if (ABSL_PREDICT_FALSE(--remaining == 0)) {
      // Last possible byte.
      if (ABSL_PREDICT_FALSE(
              byte >= uint8_t{1} << (32 - (kMaxLengthVarint32 - 1) * 7))) {
        // The representation is longer than `kMaxLengthVarint32`
        // or the represented value does not fit in `uint32_t`.
        return absl::nullopt;
      }
      break;
    }
  } while ((byte & 0x80) != 0);
  return ReadFromStringResult<char*>{dest, src};
}

inline absl::optional<ReadFromStringResult<char*>> CopyVarint64(
    const char* src, const char* limit, char* dest) {
  int remaining = kMaxLengthVarint64;
  uint8_t byte;
  do {
    if (ABSL_PREDICT_FALSE(src == limit)) return absl::nullopt;
    byte = static_cast<uint8_t>(*src++);
    *dest++ = static_cast<char>(byte);
    if (ABSL_PREDICT_FALSE(--remaining == 0)) {
      // Last possible byte.
      if (ABSL_PREDICT_FALSE(
              byte >= uint8_t{1} << (64 - (kMaxLengthVarint64 - 1) * 7))) {
        // The representation is longer than `kMaxLengthVarint64`
        // or the represented value does not fit in `uint64_t`.
        return absl::nullopt;
      }
      break;
    }
  } while ((byte & 0x80) != 0);
  return ReadFromStringResult<char*>{dest, src};
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_READER_UTILS_H_
