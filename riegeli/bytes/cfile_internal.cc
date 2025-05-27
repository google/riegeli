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

#include "riegeli/bytes/cfile_internal.h"

#include <stdio.h>

#include "absl/base/optimization.h"
#include "riegeli/base/compact_string.h"
#include "riegeli/bytes/fd_internal.h"

namespace riegeli::cfile_internal {

CompactString FilenameForCFile(FILE* file) {
  const int fd = fileno(file);
  if (ABSL_PREDICT_FALSE(fd < 0)) {
    return CompactString("<unknown>");
  } else {
    return fd_internal::FilenameForFd(fd);
  }
}

}  // namespace riegeli::cfile_internal
