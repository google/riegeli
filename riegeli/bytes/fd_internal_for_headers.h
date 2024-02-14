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

#ifndef RIEGELI_BYTES_FD_INTERNAL_FOR_HEADERS_H_
#define RIEGELI_BYTES_FD_INTERNAL_FOR_HEADERS_H_

#ifndef __APPLE__
#include <fcntl.h>
#endif

#include "riegeli/base/constexpr.h"  // IWYU pragma: keep

namespace riegeli {
namespace fd_internal {

#ifndef _WIN32
#ifndef __APPLE__
RIEGELI_INLINE_CONSTEXPR(int, kCloseOnExec, O_CLOEXEC);
#else   // __APPLE__
// On Darwin `O_CLOEXEC` is available conditionally, so `kCloseOnExec` is
// defined out of line.
extern const int kCloseOnExec;
#endif  // __APPLE__
#else   // _WIN32
RIEGELI_INLINE_CONSTEXPR(int, kCloseOnExec, _O_NOINHERIT);
#endif  // _WIN32

}  // namespace fd_internal
}  // namespace riegeli

#endif  // RIEGELI_BYTES_FD_INTERNAL_FOR_HEADERS_H_
