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

// Make `O_CLOEXEC` available on Darwin.
#if !defined(_XOPEN_SOURCE) || _XOPEN_SOURCE < 700
#undef _XOPEN_SOURCE
#define _XOPEN_SOURCE 700
#endif

#include "riegeli/bytes/cfile_handle.h"

#ifdef __APPLE__
#include <fcntl.h>
#endif
#include <stdio.h>

#include <cerrno>
#ifdef _WIN32
#include <string>
#endif

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/status.h"
#ifdef _WIN32
#include "riegeli/base/unicode.h"
#endif

namespace riegeli {

#ifndef _WIN32
absl::Status OwnedCFile::Open(const char* filename, const char* mode) {
  Reset();
#ifndef __APPLE__
  FILE* const file = fopen(filename, mode);
  if (ABSL_PREDICT_FALSE(file == nullptr)) {
    const int error_number = errno;
    return Annotate(absl::ErrnoToStatus(error_number, "fopen() failed"),
                    absl::StrCat("opening ", filename));
  }
#else   // __APPLE__
  // Emulate `fopen()` with `open()` + `fdopen()`, adding support for 'e'
  // (`O_CLOEXEC`).
  const char* fdopen_mode = mode;
  mode_t open_mode;
  switch (mode[0]) {
    case 'r':
      open_mode = O_RDONLY;
      break;
    case 'w':
      open_mode = O_WRONLY | O_CREAT | O_TRUNC;
      break;
    case 'a':
      open_mode = O_WRONLY | O_CREAT | O_APPEND;
      break;
    default:
      return absl::InvalidArgumentError(
          absl::StrCat("Mode must begin with 'r', 'w', or 'a': ", mode));
  }
  for (++mode; *mode != '\0' && *mode != ','; ++mode) {
    switch (*mode) {
      case '+':
        open_mode = (open_mode & ~O_ACCMODE) | O_RDWR;
        break;
      case 'b':
        break;
      case 'x':
        open_mode |= O_EXCL;
        break;
      case 'e':
        open_mode |= O_CLOEXEC;
        break;
      default:
        break;
    }
  }
  const int fd = open(filename, open_mode, 0666);
  if (ABSL_PREDICT_FALSE(fd < 0)) {
    const int error_number = errno;
    return Annotate(absl::ErrnoToStatus(error_number, "open() failed"),
                    absl::StrCat("opening ", filename));
  }
  FILE* const file = fdopen(fd, fdopen_mode);
  if (ABSL_PREDICT_FALSE(file == nullptr)) {
    const int error_number = errno;
    close(fd);
    return Annotate(absl::ErrnoToStatus(error_number, "fdopen() failed"),
                    absl::StrCat("opening ", filename));
  }
#endif  // __APPLE__
  Reset(file);
  return absl::OkStatus();
}
#else   // _WIN32
absl::Status OwnedCFile::Open(absl::string_view filename,
                              absl::string_view mode) {
  Reset();
  std::wstring filename_wide;
  if (ABSL_PREDICT_FALSE(!Utf8ToWide(filename, filename_wide))) {
    return absl::InvalidArgumentError(
        absl::StrCat("Filename not valid UTF-8: ", filename));
  }
  std::wstring mode_wide;
  if (ABSL_PREDICT_FALSE(!Utf8ToWide(mode, mode_wide))) {
    return absl::InvalidArgumentError(
        absl::StrCat("Mode not valid UTF-8: ", mode));
  }
  FILE* const file = _wfopen(filename_wide.c_str(), mode_wide.c_str());
  if (ABSL_PREDICT_FALSE(file == nullptr)) {
    const int error_number = errno;
    return Annotate(absl::ErrnoToStatus(error_number, "_wfopen() failed"),
                    absl::StrCat("opening ", filename));
  }
  Reset(file);
  return absl::OkStatus();
}
#endif  // _WIN32

absl::Status OwnedCFile::Close() {
  if (is_open()) {
    if (ABSL_PREDICT_FALSE(fclose(Release()) != 0)) {
      const int error_number = errno;
      return absl::ErrnoToStatus(error_number, "fclose() failed");
    }
  }
  return absl::OkStatus();
}

}  // namespace riegeli
