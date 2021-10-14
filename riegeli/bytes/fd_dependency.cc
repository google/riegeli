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

#include "riegeli/bytes/fd_dependency.h"

#include <unistd.h>

#include <cerrno>
#include <string>

#include "absl/base/optimization.h"
#include "absl/strings/str_cat.h"
#include "absl/types/optional.h"

namespace riegeli {
namespace internal {

std::string ResolveFilename(int fd,
                            absl::optional<std::string>&& assumed_filename) {
  if (assumed_filename != absl::nullopt) {
    return *std::move(assumed_filename);
  }
  switch (fd) {
    case 0:
      return "/dev/stdin";
    case 1:
      return "/dev/stdout";
    case 2:
      return "/dev/stderr";
    default:
      return absl::StrCat("/proc/self/fd/", fd);
  }
}

int CloseFd(int fd) {
  // http://austingroupbugs.net/view.php?id=529 explains this mess.
#ifdef POSIX_CLOSE_RESTART
  // Avoid EINTR by using posix_close(_, 0) if available.
  if (ABSL_PREDICT_FALSE(posix_close(fd, 0) < 0)) {
    if (errno == EINPROGRESS) return 0;
    return -1;
  }
#else
  if (ABSL_PREDICT_FALSE(close(fd) < 0)) {
    // After `EINTR` it is unspecified whether `fd` has been closed or not.
    // Assume that it is closed, which is the case e.g. on Linux.
    if (errno == EINPROGRESS || errno == EINTR) return 0;
    return -1;
  }
#endif
  return 0;
}

}  // namespace internal
}  // namespace riegeli
