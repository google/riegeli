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

// Make `O_CLOEXEC` available on Darwin.
#if !defined(_XOPEN_SOURCE) || _XOPEN_SOURCE < 700
#undef _XOPEN_SOURCE
#define _XOPEN_SOURCE 700
#endif

#include "riegeli/bytes/fd_internal_for_headers.h"  // IWYU pragma: keep

#ifdef __APPLE__
#include <fcntl.h>
#endif

namespace riegeli {
namespace fd_internal {

#ifdef __APPLE__
// On Darwin `O_CLOEXEC` is available conditionally, so `kCloseOnExec` is
// defined out of line.
extern const int kCloseOnExec = O_CLOEXEC;
#endif  // __APPLE__

}  // namespace fd_internal
}  // namespace riegeli
