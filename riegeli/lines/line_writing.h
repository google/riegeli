// Copyright 2020 Google LLC
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

#ifndef RIEGELI_LINES_LINE_WRITING_H_
#define RIEGELI_LINES_LINE_WRITING_H_

#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Options for `WriteLine()`.
class WriteLineOptions {
 public:
  // Line terminator representation to write.
  enum class Newline {
    kLf,    // LF ("\n")
    kCr,    // CR ("\r")
    kCrLf,  // CR LF ("\r\n")
  };

  WriteLineOptions() noexcept {}

  // Line terminator representation to write.
  //
  // Default: `Newline::kLf`.
  WriteLineOptions& set_newline(Newline newline) & {
    newline_ = newline;
    return *this;
  }
  WriteLineOptions&& set_newline(Newline newline) && {
    return std::move(set_newline(newline));
  }
  Newline newline() const { return newline_; }

 private:
  Newline newline_ = Newline::kLf;
};

// Writes an optional string, then a line terminator.
//
// `std::string&&` is accepted with a template to avoid implicit conversions
// to `std::string` which can be ambiguous against `absl::string_view`
// (e.g. `const char*`).
//
// Return values:
//  * `true`  - success
//  * `false` - failure (`!ok()`)
bool WriteLine(absl::string_view src, Writer& dest,
               WriteLineOptions options = WriteLineOptions());
template <typename Src,
          std::enable_if_t<std::is_same<Src, std::string>::value, int> = 0>
bool WriteLine(Src&& src, Writer& dest,
               WriteLineOptions options = WriteLineOptions());
bool WriteLine(const Chain& src, Writer& dest,
               WriteLineOptions options = WriteLineOptions());
bool WriteLine(Chain&& src, Writer& dest,
               WriteLineOptions options = WriteLineOptions());
bool WriteLine(const absl::Cord& src, Writer& dest,
               WriteLineOptions options = WriteLineOptions());
bool WriteLine(absl::Cord&& src, Writer& dest,
               WriteLineOptions options = WriteLineOptions());
bool WriteLine(Writer& dest, WriteLineOptions options = WriteLineOptions());

// Implementation details follow.

inline bool WriteLine(absl::string_view src, Writer& dest,
                      WriteLineOptions options) {
  if (ABSL_PREDICT_FALSE(!dest.Write(src))) return false;
  return WriteLine(dest, options);
}

template <typename Src,
          std::enable_if_t<std::is_same<Src, std::string>::value, int>>
inline bool WriteLine(Src&& src, Writer& dest, WriteLineOptions options) {
  // `std::move(src)` is correct and `std::forward<Src>(src)` is not necessary:
  // `Src` is always `std::string`, never an lvalue reference.
  if (ABSL_PREDICT_FALSE(!dest.Write(std::move(src)))) return false;
  return WriteLine(dest, options);
}

inline bool WriteLine(const Chain& src, Writer& dest,
                      WriteLineOptions options) {
  if (ABSL_PREDICT_FALSE(!dest.Write(src))) return false;
  return WriteLine(dest, options);
}

inline bool WriteLine(Chain&& src, Writer& dest, WriteLineOptions options) {
  if (ABSL_PREDICT_FALSE(!dest.Write(std::move(src)))) return false;
  return WriteLine(dest, options);
}

inline bool WriteLine(const absl::Cord& src, Writer& dest,
                      WriteLineOptions options) {
  if (ABSL_PREDICT_FALSE(!dest.Write(src))) return false;
  return WriteLine(dest, options);
}

inline bool WriteLine(absl::Cord&& src, Writer& dest,
                      WriteLineOptions options) {
  if (ABSL_PREDICT_FALSE(!dest.Write(std::move(src)))) return false;
  return WriteLine(dest, options);
}

inline bool WriteLine(Writer& dest, WriteLineOptions options) {
  switch (options.newline()) {
    case WriteLineOptions::Newline::kLf:
      return dest.WriteChar('\n');
    case WriteLineOptions::Newline::kCr:
      return dest.WriteChar('\r');
    case WriteLineOptions::Newline::kCrLf:
      return dest.Write("\r\n");
  }
  RIEGELI_ASSERT_UNREACHABLE()
      << "Unknown newline: " << static_cast<int>(options.newline());
}

}  // namespace riegeli

#endif  // RIEGELI_LINES_LINE_WRITING_H_
