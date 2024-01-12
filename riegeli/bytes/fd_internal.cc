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

#include "riegeli/bytes/fd_internal.h"

#include <string>

#include "absl/strings/str_cat.h"

namespace riegeli {
namespace fd_internal {

void FilenameForFd(int fd, std::string& filename) {
  switch (fd) {
#ifndef _WIN32
    case 0:
      filename = "/dev/stdin";
      break;
    case 1:
      filename = "/dev/stdout";
      break;
    case 2:
      filename = "/dev/stderr";
      break;
    default:
      filename.clear();
      absl::StrAppend(&filename, "/proc/self/fd/", fd);
      break;
#else
    case 0:
      filename = "CONIN$";
      break;
    case 1:
      filename = "CONOUT$";
      break;
    case 2:
      filename = "CONERR$";
      break;
    default:
      filename.clear();
      absl::StrAppend(&filename, "<fd ", fd, ">");
      break;
#endif
  }
}

}  // namespace fd_internal
}  // namespace riegeli
