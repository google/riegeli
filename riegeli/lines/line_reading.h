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

#include "absl/base/attributes.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/lines/newline.h"

namespace riegeli {

// Options for `ReadLine()`.
class ReadLineOptions {
 public:
  ReadLineOptions() noexcept {}

  // Options can also be specified by the line terminator alone.
  /*implicit*/ ReadLineOptions(ReadNewline newline) : newline_(newline) {}

  // Line terminator representations to recognize.
  //
  // Default: `ReadNewline::kCrLfOrLf`.
  ReadLineOptions& set_newline(ReadNewline newline) &
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    newline_ = newline;
    return *this;
  }
  ReadLineOptions&& set_newline(ReadNewline newline) &&
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return std::move(set_newline(newline));
  }
  ReadNewline newline() const { return newline_; }

  // If `false`, line terminators are stripped.
  //
  // If `true`, each returned line includes its terminator if it was present
  // (it can be absent in the last line).
  //
  // Default: `false`.
  ReadLineOptions& set_keep_newline(bool keep_newline) &
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    keep_newline_ = keep_newline;
    return *this;
  }
  ReadLineOptions&& set_keep_newline(bool keep_newline) &&
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return std::move(set_keep_newline(keep_newline));
  }
  bool keep_newline() const { return keep_newline_; }

  // Expected maximum line length.
  //
  // If this length is exceeded, reading fails with
  // `absl::ResourceExhaustedError()`.
  //
  // Default: `std::numeric_limits<size_t>::max()`.
  ReadLineOptions& set_max_length(size_t max_length) &
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    max_length_ = max_length;
    return *this;
  }
  ReadLineOptions&& set_max_length(size_t max_length) &&
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return std::move(set_max_length(max_length));
  }
  size_t max_length() const { return max_length_; }

 private:
  ReadNewline newline_ = ReadNewline::kCrLfOrLf;
  bool keep_newline_ = false;
  size_t max_length_ = std::numeric_limits<size_t>::max();
};

// Reads a line.
//
// Line terminator after the last line is optional.
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

// Skips UTF-8 BOM if it is present.
void SkipUtf8Bom(Reader& src);

}  // namespace riegeli

#endif  // RIEGELI_LINES_LINE_READING_H_
