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

// Make `openat()` available.
#if !defined(_POSIX_C_SOURCE) || _POSIX_C_SOURCE < 200809
#undef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809
#endif

#include "riegeli/bytes/fd_handle.h"

#include <fcntl.h>
#ifdef _WIN32
#include <io.h>
#include <share.h>
#else
#include <stddef.h>
#include <unistd.h>
#endif

#include <cerrno>
#ifndef _WIN32
#include <utility>
#endif

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#ifndef _WIN32
#include "riegeli/base/compact_string.h"
#endif
#include "riegeli/base/status.h"
#ifdef _WIN32
#include "riegeli/base/unicode.h"
#endif
#include "riegeli/bytes/path_ref.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if !__cpp_inline_variables
constexpr FdHandle::Methods FdHandle::kMethodsDefault;
constexpr OwnedFd::Permissions OwnedFd::kDefaultPermissions;
#endif

namespace fd_internal {

template class FdBase<UnownedFdDeleter>;
template class FdBase<OwnedFdDeleter>;

}  // namespace fd_internal

absl::Status OwnedFd::Open(PathRef filename, int mode,
                           Permissions permissions) {
#ifndef _WIN32
  ResetCFilename(filename);
again:
  const int fd = open(c_filename(), mode, permissions);
  if (ABSL_PREDICT_FALSE(fd < 0)) {
    const int error_number = errno;
    if (error_number == EINTR) goto again;
    return Annotate(absl::ErrnoToStatus(error_number, "open() failed"),
                    absl::StrCat("opening ", absl::string_view(filename)));
  }
#else   // _WIN32
  Reset(-1, filename);
  std::wstring filename_wide;
  if (ABSL_PREDICT_FALSE(!Utf8ToWide(filename, filename_wide))) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Filename not valid UTF-8: ", absl::string_view(filename)));
  }
  int fd;
  if (ABSL_PREDICT_FALSE(_wsopen_s(&fd, filename_wide.c_str(), mode, _SH_DENYNO,
                                   permissions) != 0)) {
    const int error_number = errno;
    return Annotate(absl::ErrnoToStatus(error_number, "_wsopen_s() failed"),
                    absl::StrCat("opening ", absl::string_view(filename)));
  }
#endif  // _WIN32
  SetFdKeepFilename(fd);
  return absl::OkStatus();
}

#ifndef _WIN32
absl::Status OwnedFd::OpenAt(UnownedFd dir_fd, PathRef filename, int mode,
                             Permissions permissions) {
  absl::string_view dir_filename;
  bool needs_slash = false;
  if (dir_fd != AT_FDCWD && (filename.empty() || filename.front() != '/')) {
    dir_filename = dir_fd.filename();
    needs_slash = !dir_filename.empty() && dir_filename.back() != '/';
  }
  CompactString full_filename;
  const size_t relative_filename_pos =
      dir_filename.size() + (needs_slash ? 1 : 0);
  // Reserve 1 extra char so that `c_str()` does not need reallocation.
  full_filename.reserve(relative_filename_pos + filename.size() + 1);
  full_filename = dir_filename;
  if (needs_slash) *full_filename.append(1) = '/';
  full_filename.append(filename);
  ResetFilename(std::move(full_filename));

again:
  const int fd = openat(dir_fd.get(), c_filename() + relative_filename_pos,
                        mode, permissions);
  if (ABSL_PREDICT_FALSE(fd < 0)) {
    const int error_number = errno;
    if (error_number == EINTR) goto again;
    return Annotate(absl::ErrnoToStatus(error_number, "openat() failed"),
                    absl::StrCat("opening ", this->filename()));
  }
  SetFdKeepFilename(fd);
  return absl::OkStatus();
}
#endif  // !_WIN32

absl::Status OwnedFd::Close() {
  const int fd = Release();
  if (fd < 0) return absl::OkStatus();
#ifndef _WIN32
  // http://austingroupbugs.net/view.php?id=529 explains this mess.
#ifdef POSIX_CLOSE_RESTART
  // Avoid `EINTR` by using `posix_close(_, 0)` if available.
  if (ABSL_PREDICT_FALSE(posix_close(fd, 0) < 0)) {
    const int error_number = errno;
    if (error_number != EINPROGRESS) {
      return Annotate(absl::ErrnoToStatus(error_number, "posix_close() failed"),
                      absl::StrCat("closing ", filename()));
    }
  }
#else   // !POSIX_CLOSE_RESTART
  if (ABSL_PREDICT_FALSE(close(fd) < 0)) {
    const int error_number = errno;
    // After `EINTR` it is unspecified whether `fd` has been closed or not.
    // Assume that it is closed, which is the case e.g. on Linux.
    if (error_number != EINPROGRESS && error_number != EINTR) {
      return Annotate(absl::ErrnoToStatus(error_number, "close() failed"),
                      absl::StrCat("closing ", filename()));
    }
  }
#endif  // !POSIX_CLOSE_RESTART
#else   // _WIN32
  if (ABSL_PREDICT_FALSE(_close(fd) < 0)) {
    const int error_number = errno;
    return Annotate(absl::ErrnoToStatus(error_number, "_close() failed"),
                    absl::StrCat("closing ", filename()));
  }
#endif  // _WIN32
  return absl::OkStatus();
}

}  // namespace riegeli
