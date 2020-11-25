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
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

namespace {

ABSL_ATTRIBUTE_COLD bool MaxLineLengthExceeded(Reader& src,
                                               absl::string_view& dest,
                                               size_t max_length) {
  dest = absl::string_view(src.cursor(), max_length);
  src.move_cursor(max_length);
  src.Fail(absl::ResourceExhaustedError(
      absl::StrCat("Maximum line length exceeded: ", max_length)));
  return true;
}

ABSL_ATTRIBUTE_COLD bool MaxLineLengthExceeded(Reader& src, std::string& dest,
                                               size_t max_length) {
  dest.append(src.cursor(), max_length);
  src.move_cursor(max_length);
  src.Fail(absl::ResourceExhaustedError(
      absl::StrCat("Maximum length exceeded: ", max_length)));
  return true;
}

template <typename Dest>
ABSL_ATTRIBUTE_COLD bool MaxLineLengthExceeded(Reader& src, Dest& dest,
                                               size_t max_length) {
  src.ReadAndAppend(max_length, dest);
  src.Fail(absl::ResourceExhaustedError(
      absl::StrCat("Maximum length exceeded: ", max_length)));
  return true;
}

ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool FoundNewline(Reader& src,
                                                      absl::string_view& dest,
                                                      ReadLineOptions options,
                                                      size_t length,
                                                      size_t newline_length) {
  const size_t length_with_newline = length + newline_length;
  if (options.keep_newline()) length = length_with_newline;
  if (ABSL_PREDICT_FALSE(length > options.max_length())) {
    return MaxLineLengthExceeded(src, dest, options.max_length());
  }
  dest = absl::string_view(src.cursor(), length);
  src.move_cursor(length_with_newline);
  return true;
}

ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool FoundNewline(Reader& src,
                                                      std::string& dest,
                                                      ReadLineOptions options,
                                                      size_t length,
                                                      size_t newline_length) {
  const size_t length_with_newline = length + newline_length;
  if (options.keep_newline()) length = length_with_newline;
  if (ABSL_PREDICT_FALSE(length > options.max_length())) {
    return MaxLineLengthExceeded(src, dest, options.max_length());
  }
  dest.append(src.cursor(), length);
  src.move_cursor(length_with_newline);
  return true;
}

template <typename Dest>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool FoundNewline(Reader& src, Dest& dest,
                                                      ReadLineOptions options,
                                                      size_t length,
                                                      size_t newline_length) {
  if (options.keep_newline()) {
    length += newline_length;
    newline_length = 0;
  }
  if (ABSL_PREDICT_FALSE(length > options.max_length())) {
    return MaxLineLengthExceeded(src, dest, options.max_length());
  }
  src.ReadAndAppend(length, dest);
  src.Skip(newline_length);
  return true;
}

template <typename Dest>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool ReadLineAndAppend(
    Reader& src, Dest& dest, ReadLineOptions options) {
  if (ABSL_PREDICT_FALSE(!src.Pull())) return false;
  do {
    switch (options.newline()) {
      case ReadLineOptions::Newline::kLf: {
        const char* const newline = static_cast<const char*>(
            std::memchr(src.cursor(), '\n', src.available()));
        if (ABSL_PREDICT_TRUE(newline != nullptr)) {
          return FoundNewline(src, dest, options,
                              PtrDistance(src.cursor(), newline), 1);
        }
        goto continue_reading;
      }
      case ReadLineOptions::Newline::kAny:
        for (const char* newline = src.cursor(); newline < src.limit();
             ++newline) {
          if (ABSL_PREDICT_FALSE(*newline == '\n')) {
            return FoundNewline(src, dest, options,
                                PtrDistance(src.cursor(), newline), 1);
          }
          if (ABSL_PREDICT_FALSE(*newline == '\r')) {
            const size_t length = PtrDistance(src.cursor(), newline);
            return FoundNewline(src, dest, options, length,
                                ABSL_PREDICT_TRUE(src.Pull(length + 2)) &&
                                        src.cursor()[length + 1] == '\n'
                                    ? size_t{2}
                                    : size_t{1});
          }
        }
        goto continue_reading;
    }
    RIEGELI_ASSERT_UNREACHABLE()
        << "Unknown newline: " << static_cast<int>(options.newline());
  continue_reading:
    if (ABSL_PREDICT_FALSE(src.available() > options.max_length())) {
      return MaxLineLengthExceeded(src, dest, options.max_length());
    }
    options.set_max_length(options.max_length() - src.available());
    src.ReadAndAppend(src.available(), dest);
  } while (src.Pull());
  return true;
}

}  // namespace

bool ReadLine(Reader& src, absl::string_view& dest, ReadLineOptions options) {
  options.set_max_length(UnsignedMin(options.max_length(), dest.max_size()));
  size_t length = 0;
  if (ABSL_PREDICT_FALSE(!src.Pull())) return false;
  do {
    switch (options.newline()) {
      case ReadLineOptions::Newline::kLf: {
        const char* const newline = static_cast<const char*>(
            std::memchr(src.cursor() + length, '\n', src.available() - length));
        if (ABSL_PREDICT_TRUE(newline != nullptr)) {
          return FoundNewline(src, dest, options,
                              PtrDistance(src.cursor(), newline), 1);
        }
        goto continue_reading;
      }
      case ReadLineOptions::Newline::kAny:
        for (const char* newline = src.cursor() + length; newline < src.limit();
             ++newline) {
          if (ABSL_PREDICT_FALSE(*newline == '\n')) {
            return FoundNewline(src, dest, options,
                                PtrDistance(src.cursor(), newline), 1);
          }
          if (ABSL_PREDICT_FALSE(*newline == '\r')) {
            length = PtrDistance(src.cursor(), newline);
            return FoundNewline(src, dest, options, length,
                                ABSL_PREDICT_TRUE(src.Pull(length + 2)) &&
                                        src.cursor()[length + 1] == '\n'
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
  } while (src.Pull(length + 1, SaturatingAdd(length, length)));
  dest = absl::string_view(src.cursor(), src.available());
  src.move_cursor(src.available());
  return true;
}

bool ReadLine(Reader& src, std::string& dest, ReadLineOptions options) {
  dest.clear();
  options.set_max_length(UnsignedMin(options.max_length(), dest.max_size()));
  if (ABSL_PREDICT_FALSE(!src.Pull())) return false;
  do {
    switch (options.newline()) {
      case ReadLineOptions::Newline::kLf: {
        const char* const newline = static_cast<const char*>(
            std::memchr(src.cursor(), '\n', src.available()));
        if (ABSL_PREDICT_TRUE(newline != nullptr)) {
          return FoundNewline(src, dest, options,
                              PtrDistance(src.cursor(), newline), 1);
        }
        goto continue_reading;
      }
      case ReadLineOptions::Newline::kAny:
        for (const char* newline = src.cursor(); newline < src.limit();
             ++newline) {
          if (ABSL_PREDICT_FALSE(*newline == '\n')) {
            return FoundNewline(src, dest, options,
                                PtrDistance(src.cursor(), newline), 1);
          }
          if (ABSL_PREDICT_FALSE(*newline == '\r')) {
            const size_t length = PtrDistance(src.cursor(), newline);
            return FoundNewline(src, dest, options, length,
                                ABSL_PREDICT_TRUE(src.Pull(length + 2)) &&
                                        src.cursor()[length + 1] == '\n'
                                    ? size_t{2}
                                    : size_t{1});
          }
        }
        goto continue_reading;
    }
    RIEGELI_ASSERT_UNREACHABLE()
        << "Unknown newline: " << static_cast<int>(options.newline());
  continue_reading:
    if (ABSL_PREDICT_FALSE(src.available() > options.max_length())) {
      return MaxLineLengthExceeded(src, dest, options.max_length());
    }
    options.set_max_length(options.max_length() - src.available());
    dest.append(src.cursor(), src.available());
    src.move_cursor(src.available());
  } while (src.Pull());
  return true;
}

bool ReadLine(Reader& src, Chain& dest, ReadLineOptions options) {
  dest.Clear();
  return ReadLineAndAppend(src, dest, options);
}

bool ReadLine(Reader& src, absl::Cord& dest, ReadLineOptions options) {
  dest.Clear();
  return ReadLineAndAppend(src, dest, options);
}

void SkipBOM(riegeli::Reader& src) {
  if (src.pos() != 0) return;
  src.Pull(3);
  if (src.available() >= 3 && src.cursor()[0] == static_cast<char>(0xef) &&
      src.cursor()[1] == static_cast<char>(0xbb) &&
      src.cursor()[2] == static_cast<char>(0xbf)) {
    src.move_cursor(3);
  }
}

}  // namespace riegeli
