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
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/c_string_ref.h"
#include "riegeli/base/compact_string.h"
#include "riegeli/base/status.h"
#include "riegeli/base/type_erased_ref.h"
#ifdef _WIN32
#include "riegeli/base/unicode.h"
#endif
#include "riegeli/bytes/path_ref.h"

namespace riegeli {

namespace cfile_internal {

template class CFileBase<UnownedCFileDeleter>;
template class CFileBase<OwnedCFileDeleter>;

}  // namespace cfile_internal

FILE* CFileHandle::GetMethodDefault(
    ABSL_ATTRIBUTE_UNUSED TypeErasedRef target) {
  return nullptr;
}

bool CFileHandle::IsOwningMethodDefault(
    ABSL_ATTRIBUTE_UNUSED TypeErasedRef target) {
  return false;
}

absl::string_view CFileHandle::FilenameMethodDefault(
    ABSL_ATTRIBUTE_UNUSED TypeErasedRef target) {
  return kDefaultFilename;
}

absl::Status CFileHandle::CloseMethodDefault(
    ABSL_ATTRIBUTE_UNUSED TypeErasedRef target) {
  return absl::OkStatus();
}

absl::Status OwnedCFile::Open(CompactString filename, CStringRef mode) {
  Reset(nullptr, std::move(filename));
#ifndef _WIN32
#ifndef __APPLE__
  FILE* const file = fopen(c_filename(), mode.c_str());
  if (ABSL_PREDICT_FALSE(file == nullptr)) {
    const int error_number = errno;
    return Annotate(absl::ErrnoToStatus(error_number, "fopen() failed"),
                    absl::StrCat("opening ", this->filename()));
  }
#else   // __APPLE__
  // Emulate `fopen()` with `open()` + `fdopen()`, adding support for 'e'
  // (`O_CLOEXEC`).
  mode_t open_mode;
  const char* mode_ptr = mode.c_str();
  switch (mode_ptr[0]) {
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
      return absl::InvalidArgumentError(absl::StrCat(
          "Mode must begin with 'r', 'w', or 'a': ", mode.c_str()));
  }
  for (++mode_ptr; *mode_ptr != '\0' && *mode_ptr != ','; ++mode_ptr) {
    switch (*mode_ptr) {
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
again:
  const int fd = open(c_filename(), open_mode, 0666);
  if (ABSL_PREDICT_FALSE(fd < 0)) {
    const int error_number = errno;
    if (error_number == EINTR) goto again;
    return Annotate(absl::ErrnoToStatus(error_number, "open() failed"),
                    absl::StrCat("opening ", this->filename()));
  }
  FILE* const file = fdopen(fd, mode.c_str());
  if (ABSL_PREDICT_FALSE(file == nullptr)) {
    const int error_number = errno;
    close(fd);
    return Annotate(absl::ErrnoToStatus(error_number, "fdopen() failed"),
                    absl::StrCat("opening ", this->filename()));
  }
#endif  // __APPLE__
#else   // _WIN32
  std::wstring filename_wide;
  if (ABSL_PREDICT_FALSE(!Utf8ToWide(this->filename(), filename_wide))) {
    return absl::InvalidArgumentError(
        absl::StrCat("Filename not valid UTF-8: ", this->filename()));
  }
  std::wstring mode_wide;
  if (ABSL_PREDICT_FALSE(!Utf8ToWide(mode.c_str(), mode_wide))) {
    return absl::InvalidArgumentError(
        absl::StrCat("Mode not valid UTF-8: ", mode.c_str()));
  }
  FILE* const file = _wfopen(filename_wide.c_str(), mode_wide.c_str());
  if (ABSL_PREDICT_FALSE(file == nullptr)) {
    const int error_number = errno;
    return Annotate(absl::ErrnoToStatus(error_number, "_wfopen() failed"),
                    absl::StrCat("opening ", this->filename()));
  }
#endif  // _WIN32
  SetFileKeepFilename(file);
  return absl::OkStatus();
}

absl::Status OwnedCFile::Close() {
  FILE* const file = Release();
  if (file == nullptr) return absl::OkStatus();
  if (ABSL_PREDICT_FALSE(fclose(file) != 0)) {
    const int error_number = errno;
    return Annotate(absl::ErrnoToStatus(error_number, "fclose() failed"),
                    absl::StrCat("closing ", filename()));
  }
  return absl::OkStatus();
}

}  // namespace riegeli
