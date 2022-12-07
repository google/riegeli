// Copyright 2022 Google LLC
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

#ifndef RIEGELI_BYTES_FILE_MODE_STRING_H_
#define RIEGELI_BYTES_FILE_MODE_STRING_H_

#include <string>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"

namespace riegeli {
namespace file_internal {

// If `false`, the file will be created if it does not exist, or it will be
// truncated to empty if it exists. This implies `SetRead(false)` and
// `set_append(false)` unless overwritten later.
//
// If `true`, the file must already exist, and its contents will not be
// truncated. Writing will start from the beginning, with random access
// supported. This implies `SetRead(true)`.
void SetExisting(bool existing, std::string& mode);
bool GetExisting(absl::string_view mode);

// If `false`, the file will be open for writing, except that `SetRead(false)`
// has no effect after `SetExisting(true)`.
//
// If `true`, the file will be open for writing and reading (using
// `ReadMode()`).
void SetRead(bool read, std::string& mode);
bool GetRead(absl::string_view mode);

// If `false`, the file will be truncated to empty if it exists.
//
// If `true`, the file will not be truncated if it exists, and writing will
// always happen at its end.
//
// Calling `SetAppend()` with any argument after `SetExisting(true)` undoes the
// effect if the file does not exist: it will be created.
void SetAppend(bool append, std::string& mode);
bool GetAppend(absl::string_view mode);

// If `false`, data will be read/written directly from/to the file. This is
// called the binary mode.
//
// If `true`, text mode translation will be applied on Windows: for reading
// CR-LF character pairs are translated to LF, and a ^Z character is interpreted
// as end of file; for writing LF characters are translated to CR-LF.
void SetTextReading(bool text, std::string& mode);
void SetTextWriting(bool text, std::string& mode);

#ifdef _WIN32

// Translates the text mode marker from 'b' / 't' / nothing to `_O_BINARY` /
// `_O_TEXT` / 0.
int GetTextAsFlags(absl::string_view mode);

#endif

// Implementation details follow.

inline bool GetExisting(absl::string_view mode) {
  return ABSL_PREDICT_TRUE(!mode.empty()) && mode[0] == 'r';
}

inline void SetAppend(bool append, std::string& mode) {
  if (ABSL_PREDICT_FALSE(mode.empty())) mode = "w";
  mode[0] = append ? 'a' : 'w';
}

inline bool GetAppend(absl::string_view mode) {
  return ABSL_PREDICT_TRUE(!mode.empty()) && mode[0] == 'a';
}

}  // namespace file_internal
}  // namespace riegeli

#endif  // RIEGELI_BYTES_FILE_MODE_STRING_H_
