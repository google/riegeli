// Copyright 2021 Google LLC
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

#include "riegeli/bytes/fd_handle.h"

#include <fcntl.h>

#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
#endif

#include <cerrno>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "riegeli/base/status.h"
#ifdef _WIN32
#include "riegeli/base/unicode.h"
#endif

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if !__cpp_inline_variables
constexpr OwnedFd::Permissions OwnedFd::kDefaultPermissions;
#endif

#ifndef _WIN32
absl::Status OwnedFd::Open(const char* filename, int mode,
                           Permissions permissions) {
  Reset();
again:
  const int fd = open(filename, mode, permissions);
  if (ABSL_PREDICT_FALSE(fd < 0)) {
    const int error_number = errno;
    if (error_number == EINTR) goto again;
    return Annotate(absl::ErrnoToStatus(error_number, "open() failed"),
                    absl::StrCat("opening ", filename));
  }
  Reset(fd);
  return absl::OkStatus();
}
#else   // _WIN32
absl::Status OwnedFd::Open(absl::string_view filename, int mode,
                           Permissions permissions) {
  Reset();
  std::wstring filename_wide;
  if (ABSL_PREDICT_FALSE(!Utf8ToWide(filename, filename_wide))) {
    return absl::InvalidArgumentError(
        absl::StrCat("Filename not valid UTF-8: ", filename));
  }
  int fd;
  if (ABSL_PREDICT_FALSE(_wsopen_s(&fd, filename_wide.c_str(), mode, _SH_DENYNO,
                                   permissions) != 0)) {
    const int error_number = errno;
    return Annotate(absl::ErrnoToStatus(error_number, "_wsopen_s() failed"),
                    absl::StrCat("opening ", filename));
  }
  Reset(fd);
  return absl::OkStatus();
}
#endif  // _WIN32

absl::Status OwnedFd::Close() {
  if (is_open()) {
#ifndef _WIN32
    // http://austingroupbugs.net/view.php?id=529 explains this mess.
#ifdef POSIX_CLOSE_RESTART
    // Avoid `EINTR` by using `posix_close(_, 0)` if available.
    if (ABSL_PREDICT_FALSE(posix_close(Release(), 0) < 0)) {
      const int error_number = errno;
      if (error_number != EINPROGRESS) {
        return absl::ErrnoToStatus(error_number, "posix_close() failed");
      }
    }
#else   // !POSIX_CLOSE_RESTART
    if (ABSL_PREDICT_FALSE(close(Release()) < 0)) {
      const int error_number = errno;
      // After `EINTR` it is unspecified whether `fd` has been closed or not.
      // Assume that it is closed, which is the case e.g. on Linux.
      if (error_number != EINPROGRESS && error_number != EINTR) {
        return absl::ErrnoToStatus(error_number, "close() failed");
      }
    }
#endif  // !POSIX_CLOSE_RESTART
#else   // _WIN32
    if (ABSL_PREDICT_FALSE(_close(Release()) < 0)) {
      const int error_number = errno;
      return absl::ErrnoToStatus(error_number, "_close() failed");
    }
#endif  // _WIN32
  }
  return absl::OkStatus();
}

}  // namespace riegeli
