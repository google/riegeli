// Copyright 2017 Google LLC
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

#ifndef RIEGELI_BYTES_FD_INTERNAL_H_
#define RIEGELI_BYTES_FD_INTERNAL_H_

// Warning: Do not include this header in other headers, because the definition
// of `off_t` depends on `_FILE_OFFSET_BITS` which can reliably be set only
// in a standalone compilation unit.

#include <string>

#ifdef _WIN32
#include <io.h>
#endif
#include <sys/stat.h>
#include <sys/types.h>
#ifndef _WIN32
#include <unistd.h>
#endif

#include "absl/strings/string_view.h"
#include "riegeli/base/constexpr.h"

namespace riegeli {
namespace fd_internal {

// Infers a filename from fd: "/dev/stdin", "/dev/stdout", "/dev/stderr", or
// `absl::StrCat("/proc/self/fd/", fd)` (on Windows: "CONIN$", "CONOUT$",
// "CONERR$", or `absl::StrCat("<fd ", fd, ">")`).
void FilenameForFd(int fd, std::string& filename);

#ifndef _WIN32

using Offset = off_t;

inline Offset LSeek(int fd, Offset offset, int whence) {
  return lseek(fd, offset, whence);
}

RIEGELI_INLINE_CONSTEXPR(absl::string_view, kLSeekFunctionName, "lseek()");

using StatInfo = struct stat;

inline int FStat(int fd, StatInfo* stat_info) { return fstat(fd, stat_info); }

RIEGELI_INLINE_CONSTEXPR(absl::string_view, kFStatFunctionName, "fstat()");

#else  // _WIN32

using Offset = __int64;

inline Offset LSeek(int fd, Offset offset, int whence) {
  return _lseeki64(fd, offset, whence);
}

RIEGELI_INLINE_CONSTEXPR(absl::string_view, kLSeekFunctionName, "_lseeki64()");

// `struct __stat64` in a namespace does not work in MSVC due to a bug regarding
// https://en.cppreference.com/w/cpp/language/elaborated_type_specifier.
using StatInfo = struct ::__stat64;

inline int FStat(int fd, StatInfo* stat_info) {
  return _fstat64(fd, stat_info);
}

RIEGELI_INLINE_CONSTEXPR(absl::string_view, kFStatFunctionName, "_fstat64()");

#endif  // _WIN32

}  // namespace fd_internal
}  // namespace riegeli

#endif  // RIEGELI_BYTES_FD_INTERNAL_H_
