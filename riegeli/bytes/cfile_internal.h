// Copyright 2024 Google LLC
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

#ifndef RIEGELI_BYTES_CFILE_INTERNAL_H_
#define RIEGELI_BYTES_CFILE_INTERNAL_H_

#include <stdio.h>

#include <string>

namespace riegeli::cfile_internal {

// Infers a filename from the fd corresponding to the `FILE` by reading the
// symlink target for `absl::StrCat("/proc/self/fd/", fd)` (on Windows returns
// a `absl::StrCat("<fd ", fd, ">")` placeholder instead), or returning
// "<unknown>" if there is no corresponding fd.
std::string FilenameForCFile(FILE* file);

}  // namespace riegeli::cfile_internal

#endif  // RIEGELI_BYTES_CFILE_INTERNAL_H_
