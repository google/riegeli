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

#ifndef RIEGELI_LINES_NEWLINE_H_
#define RIEGELI_LINES_NEWLINE_H_

#include "absl/strings/string_view.h"

namespace riegeli {

// Line terminator representations to recognize.
enum class ReadNewline {
  kLf,        // LF              ("\n")
  kCrLfOrLf,  // LF |      CR-LF ("\n" |        "\r\n")
  kAny,       // LF | CR | CR-LF ("\n" | "\r" | "\r\n")
};

// Line terminator representation to write.
enum class WriteNewline {
  kLf,    // LF    ("\n")
  kCr,    // CR    ("\r")
  kCrLf,  // CR-LF ("\r\n")

#ifndef _WIN32
  kNative = kLf,
#else
  kNative = kCrLf,
#endif
};

// Native line representation as a string.
#ifndef _WIN32
inline constexpr absl::string_view kNewline = "\n";
#else
inline constexpr absl::string_view kNewline = "\r\n";
#endif

// UTF-8 BOM representation as a string.
inline constexpr absl::string_view kUtf8Bom = "\xef\xbb\xbf";

}  // namespace riegeli

#endif  // RIEGELI_LINES_NEWLINE_H_
