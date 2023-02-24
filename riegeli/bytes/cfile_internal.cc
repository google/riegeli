// Copyright 2023 Google LLC
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

#include "riegeli/bytes/cfile_internal.h"

#ifndef _WIN32
#ifdef __APPLE__

#include <fcntl.h>
#include <stdio.h>

#include <cerrno>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"

namespace riegeli {
namespace cfile_internal {

FILE* FOpen(const char* filename, const char* mode,
            absl::string_view& failed_function_name) {
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
      errno = EINVAL;
      return nullptr;
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
    failed_function_name = "open()";
    return nullptr;
  }
  FILE* const file = fdopen(fd, fdopen_mode);
  if (ABSL_PREDICT_FALSE(file == nullptr)) {
    close(fd);
    failed_function_name = "fdopen()";
  }
  return file;
}

}  // namespace cfile_internal
}  // namespace riegeli

#endif
#endif
