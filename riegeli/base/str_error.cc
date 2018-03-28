// Copyright 2018 Google LLC
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

// Make strerror_r() available.
#if !defined(_XOPEN_SOURCE) || _XOPEN_SOURCE < 600
#undef _XOPEN_SOURCE
#define _XOPEN_SOURCE 600
#endif

#include "riegeli/base/str_error.h"

#include <string.h>
#include <string>

#include "absl/strings/str_cat.h"
#include "riegeli/base/base.h"

namespace riegeli {

namespace {

// POSIX strerror_r returns int.
std::string StrErrorResult(int result, const char* buffer, int error_code) {
  if (RIEGELI_UNLIKELY(result != 0)) {
    return absl::StrCat("Unknown error ", error_code);
  }
  return buffer;
}

// GNU strerror_r returns char*.
std::string StrErrorResult(char* result, const char* buffer, int error_code) {
  return result;
}

}  // namespace

std::string StrError(int error_code) {
  char message[256];
  return StrErrorResult(strerror_r(error_code, message, sizeof(message)),
                        message, error_code);
}

}  // namespace riegeli
