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

#ifndef RIEGELI_LINES_LINE_READING_H_
#define RIEGELI_LINES_LINE_READING_H_

#include <stddef.h>

#include <limits>
#include <string>
#include <utility>

#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

// Options for `ReadLine()`.
class ReadLineOptions {
 public:
  // Line terminator representations to recognize.
  enum class Newline {
    kLf,        // LF ("\n")
    kLfOrCrLf,  // LF |      CR LF ("\n" |        "\r\n")
    kAny,       // LF | CR | CR LF ("\n" | "\r" | "\r\n")
  };

  ReadLineOptions() noexcept {}

  // Line terminator representations to recognize.
  //
  // Default: `Newline::kLf`.
  ReadLineOptions& set_newline(Newline newline) & {
    newline_ = newline;
    return *this;
  }
  ReadLineOptions&& set_newline(Newline newline) && {
    return std::move(set_newline(newline));
  }
  Newline newline() const { return newline_; }

  // If `false`, line terminators are stripped.
  //
  // If `true`, each returned line includes its terminator if it was present
  // (it can be absent in the last line).
  //
  // Default: `false`.
  ReadLineOptions& set_keep_newline(bool keep_newline) & {
    keep_newline_ = keep_newline;
    return *this;
  }
  ReadLineOptions&& set_keep_newline(bool keep_newline) && {
    return std::move(set_keep_newline(keep_newline));
  }
  bool keep_newline() const { return keep_newline_; }

  // Expected maximum line length.
  //
  // If this length is exceeded, reading fails with
  // `absl::ResourceExhaustedError()`.
  //
  // Default: `std::numeric_limits<size_t>::max()`.
  ReadLineOptions& set_max_length(size_t max_length) & {
    max_length_ = max_length;
    return *this;
  }
  ReadLineOptions&& set_max_length(size_t max_length) && {
    return std::move(set_max_length(max_length));
  }
  size_t max_length() const { return max_length_; }

 private:
  Newline newline_ = Newline::kLf;
  bool keep_newline_ = false;
  size_t max_length_ = std::numeric_limits<size_t>::max();
};

// Reads a line.
//
// Line terminator after the last line is optional.
//
// Warning: if `options.newline()` is `Newline::kAny`, for lines terminated with
// CR, `ReadLine()` reads ahead one character after the CR. If reading ahead
// only as much as needed is required, e.g. when reading from an interactive
// stream, another implementation would be required (which would keep state
// between calls).
//
// Return values:
//  * `true`                     - success (`dest` is set)
//  * `false` (when `src.ok()`)  - source ends (`dest` is empty)
//  * `false` (when `!src.ok()`) - failure (`dest` is set to the partial line
//                                 read before the failure)
bool ReadLine(Reader& src, absl::string_view& dest,
              ReadLineOptions options = ReadLineOptions());
bool ReadLine(Reader& src, std::string& dest,
              ReadLineOptions options = ReadLineOptions());
bool ReadLine(Reader& src, Chain& dest,
              ReadLineOptions options = ReadLineOptions());
bool ReadLine(Reader& src, absl::Cord& dest,
              ReadLineOptions options = ReadLineOptions());

// Skips an initial UTF-8 BOM if it is present.
//
// Does nothing unless `src.pos() == 0`.
void SkipBOM(Reader& src);

}  // namespace riegeli

#endif  // RIEGELI_LINES_LINE_READING_H_
