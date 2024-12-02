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

#include "riegeli/zlib/zlib_error.h"

#include <string>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/assert.h"
#include "zlib.h"

namespace riegeli {
namespace zlib_internal {

absl::Status ZlibErrorToStatus(absl::string_view operation, int zlib_code,
                               const char* details) {
  absl::StatusCode code;
  switch (zlib_code) {
    case Z_OK:
      return absl::OkStatus();
    case Z_NEED_DICT:
    case Z_DATA_ERROR:
      code = absl::StatusCode::kInvalidArgument;
      break;
    case Z_MEM_ERROR:
      code = absl::StatusCode::kResourceExhausted;
      break;
    default:
      // Should not happen.
      code = absl::StatusCode::kInternal;
      break;
  }
  std::string message = absl::StrCat(operation, " failed");
  if (details == nullptr) {
    switch (zlib_code) {
      case Z_OK:
        RIEGELI_ASSUME_UNREACHABLE() << "Handled before switch";
      case Z_STREAM_END:
        details = "stream end";
        break;
      case Z_NEED_DICT:
        details = "need dictionary";
        break;
      case Z_ERRNO:
        details = "file error";
        break;
      case Z_STREAM_ERROR:
        details = "stream error";
        break;
      case Z_DATA_ERROR:
        details = "data error";
        break;
      case Z_MEM_ERROR:
        details = "insufficient memory";
        break;
      case Z_BUF_ERROR:
        details = "buffer error";
        break;
      case Z_VERSION_ERROR:
        details = "incompatible version";
        break;
      default:
        absl::StrAppend(&message, ": unknown zlib error code: ", zlib_code);
        break;
    }
  }
  if (details != nullptr) absl::StrAppend(&message, ": ", details);
  return absl::Status(code, message);
}

}  // namespace zlib_internal
}  // namespace riegeli
