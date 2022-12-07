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
#include <utility>

#include "absl/strings/str_cat.h"
#include "absl/types/optional.h"

namespace riegeli {
namespace fd_internal {

std::string ResolveFilename(int fd,
                            absl::optional<std::string>&& assumed_filename) {
  if (assumed_filename != absl::nullopt) return *std::move(assumed_filename);
  switch (fd) {
#ifndef _WIN32
    case 0:
      return "/dev/stdin";
    case 1:
      return "/dev/stdout";
    case 2:
      return "/dev/stderr";
    default:
      return absl::StrCat("/proc/self/fd/", fd);
#else
    case 0:
      return "CONIN$";
    case 1:
      return "CONOUT$";
    case 2:
      return "CONERR$";
    default:
      return absl::StrCat("<fd ", fd, ">");
#endif
  }
}

}  // namespace fd_internal
}  // namespace riegeli
