// Copyright 2019 Google LLC
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

#include "riegeli/lines/line_reading.h"

#include <stddef.h>

#include <cstring>
#include <string>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/lines/newline.h"

namespace riegeli {

namespace {

// Reads `length_to_read` bytes from `src`, writes their prefix of
// `length_to_write` bytes to `dest`, appending to existing contents
// (unless `Dest` is `absl::string_view`).
//
// The data to read must be already available in the buffer.

template <typename Dest>
inline void ReadFlatAndSkip(Reader& src, size_t length_to_read,
                            size_t length_to_write, Dest& dest) {
  RIEGELI_ASSERT_LE(length_to_read, src.available())
      << "Failed precondition of ReadFlatAndSkip(): "
         "reading more than buffered";
  RIEGELI_ASSERT_LE(length_to_write, length_to_read)
      << "Failed precondition of ReadFlatAndSkip(): "
         "writing more than reading";
  src.ReadAndAppend(length_to_write, dest);
  src.Skip(length_to_read - length_to_write);
}

inline void ReadFlatAndSkip(Reader& src, size_t length_to_read,
                            size_t length_to_write, absl::string_view& dest) {
  RIEGELI_ASSERT_LE(length_to_read, src.available())
      << "Failed precondition of ReadFlatAndSkip(): "
         "reading more than buffered";
  RIEGELI_ASSERT_LE(length_to_write, length_to_read)
      << "Failed precondition of ReadFlatAndSkip(): "
         "writing more than reading";
  dest = absl::string_view(src.cursor(), length_to_write);
  src.move_cursor(length_to_read);
}

inline void ReadFlatAndSkip(Reader& src, size_t length_to_read,
                            size_t length_to_write, std::string& dest) {
  RIEGELI_ASSERT_LE(length_to_read, src.available())
      << "Failed precondition of ReadFlatAndSkip(): "
         "reading more than buffered";
  RIEGELI_ASSERT_LE(length_to_write, length_to_read)
      << "Failed precondition of ReadFlatAndSkip(): "
         "writing more than reading";
  dest.append(src.cursor(), length_to_write);
  src.move_cursor(length_to_read);
}

template <typename Dest>
inline void ReadFlat(Reader& src, size_t length, Dest& dest) {
  return ReadFlatAndSkip(src, length, length, dest);
}

template <typename Dest>
ABSL_ATTRIBUTE_COLD bool MaxLineLengthExceeded(Reader& src, Dest& dest,
                                               size_t max_length) {
  ReadFlat(src, max_length, dest);
  return src.Fail(absl::ResourceExhaustedError(
      absl::StrCat("Maximum line length exceeded: ", max_length)));
}

template <typename Dest>
inline bool FoundNewline(Reader& src, Dest& dest, ReadLineOptions options,
                         size_t length, size_t newline_length) {
  const size_t length_with_newline = length + newline_length;
  if (options.keep_newline()) length = length_with_newline;
  if (ABSL_PREDICT_FALSE(length > options.max_length())) {
    return MaxLineLengthExceeded(src, dest, options.max_length());
  }
  ReadFlatAndSkip(src, length_with_newline, length, dest);
  return true;
}

template <typename Dest>
inline bool ReadLineInternal(Reader& src, Dest& dest, ReadLineOptions options) {
  if (ABSL_PREDICT_FALSE(!src.Pull())) return false;
  size_t length;
  do {
    switch (options.newline()) {
      case ReadNewline::kLf: {
        const char* const newline = static_cast<const char*>(
            std::memchr(src.cursor(), '\n', src.available()));
        if (ABSL_PREDICT_TRUE(newline != nullptr)) {
          return FoundNewline(src, dest, options,
                              PtrDistance(src.cursor(), newline), 1);
        }
        length = src.available();
        goto continue_reading;
      }
      case ReadNewline::kCrLfOrLf: {
        const char* newline = static_cast<const char*>(
            std::memchr(src.cursor(), '\n', src.available()));
        for (;;) {
          if (ABSL_PREDICT_TRUE(newline != nullptr)) {
            length = PtrDistance(src.cursor(), newline);
            if (length > 0 && newline[-1] == '\r') {
              return FoundNewline(src, dest, options, length - 1, 2);
            }
            return FoundNewline(src, dest, options, length, 1);
          }
          if (ABSL_PREDICT_TRUE(src.limit()[-1] != '\r')) {
            length = src.available();
            goto continue_reading;
          }
          // The buffer ends with CR.
          length = src.available() - 1;
          if (ABSL_PREDICT_TRUE(length > 0)) {
            // The CR is not first in the buffer. Move line read so far to
            // `dest` to avoid copying that part during flattening of the CR
            // together with the next buffer.
            goto continue_reading;
          }
          // The buffer contains only CR.
          if (ABSL_PREDICT_FALSE(!src.Pull(2))) {
            // The CR is the final character and is not a part of a line
            // terminator.
            if (ABSL_PREDICT_FALSE(options.max_length() < 1)) {
              return MaxLineLengthExceeded(src, dest, options.max_length());
            }
            ReadFlat(src, 1, dest);
            return src.ok();
          }
          // The buffer begins with CR.
          if (ABSL_PREDICT_TRUE(src.cursor()[1] == '\n')) {
            return FoundNewline(src, dest, options, 0, 2);
          }
          // The CR is not a part of a line terminator. Search for LF again.
          newline = static_cast<const char*>(
              std::memchr(src.cursor() + 2, '\n', src.available() - 2));
        }
      }
      case ReadNewline::kAny:
        for (const char* newline = src.cursor(); newline < src.limit();
             ++newline) {
          if (ABSL_PREDICT_FALSE(*newline == '\n')) {
            return FoundNewline(src, dest, options,
                                PtrDistance(src.cursor(), newline), 1);
          }
          if (ABSL_PREDICT_FALSE(*newline == '\r')) {
            length = PtrDistance(src.cursor(), newline);
            if (ABSL_PREDICT_FALSE(newline + 1 == src.limit())) {
              // The CR is last in the buffer.
              if (ABSL_PREDICT_TRUE(length > 0)) {
                // The CR is not first in the buffer. Move line read so far to
                // `dest` to avoid copying that part during flattening of the CR
                // together with the next buffer.
                goto continue_reading;
              }
              // The buffer contains only CR.
              return FoundNewline(
                  src, dest, options, 0,
                  ABSL_PREDICT_TRUE(src.Pull(2) && src.cursor()[1] == '\n')
                      ? size_t{2}
                      : size_t{1});
            }
            return FoundNewline(
                src, dest, options, length,
                ABSL_PREDICT_TRUE(newline[1] == '\n') ? size_t{2} : size_t{1});
          }
        }
        length = src.available();
        goto continue_reading;
    }
    RIEGELI_ASSERT_UNREACHABLE()
        << "Unknown newline: " << static_cast<int>(options.newline());
  continue_reading:
    if (ABSL_PREDICT_FALSE(length > options.max_length())) {
      return MaxLineLengthExceeded(src, dest, options.max_length());
    }
    options.set_max_length(options.max_length() - length);
    ReadFlat(src, length, dest);
  } while (src.Pull());
  return src.ok();
}

}  // namespace

bool ReadLine(Reader& src, absl::string_view& dest, ReadLineOptions options) {
  options.set_max_length(UnsignedMin(options.max_length(), dest.max_size()));
  size_t length = 0;
  if (ABSL_PREDICT_FALSE(!src.Pull())) {
    dest = absl::string_view();
    return false;
  }
  do {
    switch (options.newline()) {
      case ReadNewline::kLf: {
        const char* const newline = static_cast<const char*>(
            std::memchr(src.cursor() + length, '\n', src.available() - length));
        if (ABSL_PREDICT_TRUE(newline != nullptr)) {
          return FoundNewline(src, dest, options,
                              PtrDistance(src.cursor(), newline), 1);
        }
        goto continue_reading;
      }
      case ReadNewline::kCrLfOrLf:
        for (;;) {
          const char* const newline = static_cast<const char*>(std::memchr(
              src.cursor() + length, '\n', src.available() - length));
          if (ABSL_PREDICT_TRUE(newline != nullptr)) {
            length = PtrDistance(src.cursor(), newline);
            if (length > 0 && newline[-1] == '\r') {
              return FoundNewline(src, dest, options, length - 1, 2);
            }
            return FoundNewline(src, dest, options, length, 1);
          }
          if (ABSL_PREDICT_TRUE(src.limit()[-1] != '\r')) goto continue_reading;
          // The buffer ends with CR.
          length = src.available() - 1;
          if (ABSL_PREDICT_FALSE(length > options.max_length())) {
            return MaxLineLengthExceeded(src, dest, options.max_length());
          }
          if (ABSL_PREDICT_FALSE(!src.Pull(length + 2))) {
            // The CR is the final character and is not a part of a line
            // terminator.
            if (ABSL_PREDICT_FALSE(src.available() > options.max_length())) {
              return MaxLineLengthExceeded(src, dest, options.max_length());
            }
            dest = absl::string_view(src.cursor(), src.available());
            src.move_cursor(src.available());
            return src.ok();
          }
          if (ABSL_PREDICT_TRUE(src.cursor()[length + 1] == '\n')) {
            return FoundNewline(src, dest, options, length, 2);
          }
          // The CR at `src.cursor()[length]` is not a part of a line
          // terminator. Search for LF again.
          length += 2;
        }
      case ReadNewline::kAny:
        for (const char* newline = src.cursor() + length; newline < src.limit();
             ++newline) {
          if (ABSL_PREDICT_FALSE(*newline == '\n')) {
            return FoundNewline(src, dest, options,
                                PtrDistance(src.cursor(), newline), 1);
          }
          if (ABSL_PREDICT_FALSE(*newline == '\r')) {
            length = PtrDistance(src.cursor(), newline);
            return FoundNewline(
                src, dest, options, length,
                ABSL_PREDICT_TRUE(src.Pull(length + 2) &&
                                  src.cursor()[length + 1] == '\n')
                    ? size_t{2}
                    : size_t{1});
          }
        }
        goto continue_reading;
    }
    RIEGELI_ASSERT_UNREACHABLE()
        << "Unknown newline: " << static_cast<int>(options.newline());
  continue_reading:
    length = src.available();
    if (ABSL_PREDICT_FALSE(length > options.max_length())) {
      return MaxLineLengthExceeded(src, dest, options.max_length());
    }
  } while (src.Pull(length + 1));
  dest = absl::string_view(src.cursor(), src.available());
  src.move_cursor(src.available());
  return src.ok();
}

bool ReadLine(Reader& src, std::string& dest, ReadLineOptions options) {
  dest.clear();
  options.set_max_length(UnsignedMin(options.max_length(), dest.max_size()));
  return ReadLineInternal(src, dest, options);
}

bool ReadLine(Reader& src, Chain& dest, ReadLineOptions options) {
  dest.Clear();
  return ReadLineInternal(src, dest, options);
}

bool ReadLine(Reader& src, absl::Cord& dest, ReadLineOptions options) {
  dest.Clear();
  return ReadLineInternal(src, dest, options);
}

void SkipUtf8Bom(Reader& src) {
  src.Pull(kUtf8Bom.size());
  if (absl::StartsWith(absl::string_view(src.cursor(), src.available()),
                       kUtf8Bom)) {
    src.move_cursor(kUtf8Bom.size());
  }
}

}  // namespace riegeli
