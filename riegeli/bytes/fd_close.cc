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

#include "riegeli/bytes/fd_close.h"

#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
#endif

#include <cerrno>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"

namespace riegeli {
namespace fd_internal {

int Close(int fd) {
#ifndef _WIN32
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
#else
  if (ABSL_PREDICT_FALSE(_close(fd) < 0)) return -1;
#endif
  return 0;
}

#ifndef _WIN32
#ifdef POSIX_CLOSE_RESTART
extern const absl::string_view kCloseFunctionName = "posix_close()";
#else
extern const absl::string_view kCloseFunctionName = "close()";
#endif
#else
extern const absl::string_view kCloseFunctionName = "_close()";
#endif

}  // namespace fd_internal
}  // namespace riegeli
