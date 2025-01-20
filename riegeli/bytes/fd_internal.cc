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

#ifndef _WIN32

// Make `readlink()` available, and make `O_CLOEXEC` available on Darwin.
#if !defined(_XOPEN_SOURCE) || _XOPEN_SOURCE < 700
#undef _XOPEN_SOURCE
#define _XOPEN_SOURCE 700
#endif

#endif

#include "riegeli/bytes/fd_internal.h"

#ifdef __APPLE__
#include <fcntl.h>
#endif
#ifndef _WIN32
#include <stddef.h>
#include <unistd.h>
#endif

#include <string>

#ifndef _WIN32
#include "absl/base/optimization.h"
#endif
#include "absl/strings/str_cat.h"
#ifndef _WIN32
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/buffer.h"
#endif

namespace riegeli {
namespace fd_internal {

void FilenameForFd(int fd, std::string& filename) {
  filename.clear();
#ifndef _WIN32
  absl::StrAppend(&filename, "/proc/self/fd/", fd);
  Buffer buffer(PATH_MAX);
  const ssize_t length = readlink(filename.c_str(), buffer.data(), PATH_MAX);
  if (ABSL_PREDICT_FALSE(length < 0)) return;
  filename.assign(buffer.data(), IntCast<size_t>(length));
#else   // _WIN32
  absl::StrAppend(&filename, "<fd ", fd, ">");
#endif  // _WIN32
}

#ifdef __APPLE__
// On Darwin `O_CLOEXEC` is available conditionally, so `kCloseOnExec` is
// defined out of line.
extern const int kCloseOnExec = O_CLOEXEC;
#endif  // __APPLE__

}  // namespace fd_internal
}  // namespace riegeli
