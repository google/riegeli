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

#include "riegeli/xz/xz_error.h"

#include <string>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "lzma.h"
#include "riegeli/base/assert.h"

namespace riegeli {
namespace xz_internal {

absl::Status XzErrorToStatus(absl::string_view operation,
                             lzma_ret liblzma_code) {
  absl::StatusCode code;
  switch (liblzma_code) {
    case LZMA_OK:
      return absl::OkStatus();
    case LZMA_NO_CHECK:
    case LZMA_UNSUPPORTED_CHECK:
    case LZMA_DATA_ERROR:
      code = absl::StatusCode::kInvalidArgument;
      break;
    case LZMA_MEM_ERROR:
    case LZMA_MEMLIMIT_ERROR:
      code = absl::StatusCode::kResourceExhausted;
      break;
    default:
      // Should not happen.
      code = absl::StatusCode::kInternal;
      break;
  }
  std::string message = absl::StrCat(operation, " failed");
  absl::string_view details;
  switch (liblzma_code) {
    case LZMA_OK:
      RIEGELI_ASSERT_UNREACHABLE() << "Handled before switch";
    case LZMA_STREAM_END:
      details = "End of stream was reached";
      break;
    case LZMA_NO_CHECK:
      details = "Input stream has no integrity check";
      break;
    case LZMA_UNSUPPORTED_CHECK:
      details = "Cannot calculate the integrity check";
      break;
    case LZMA_GET_CHECK:
      details = "Integrity check type is now available";
      break;
    case LZMA_MEM_ERROR:
      details = "Cannot allocate memory";
      break;
    case LZMA_MEMLIMIT_ERROR:
      details = "Memory usage limit was reached";
      break;
    case LZMA_FORMAT_ERROR:
      details = "File format not recognized";
      break;
    case LZMA_OPTIONS_ERROR:
      details = "Invalid or unsupported options";
      break;
    case LZMA_DATA_ERROR:
      details = "Data is corrupt";
      break;
    case LZMA_BUF_ERROR:
      details = "No progress is possible";
      break;
    case LZMA_PROG_ERROR:
      details = "Programming error";
      break;
    default:
      absl::StrAppend(&message, ": unknown liblzma error code: ", liblzma_code);
      break;
  }
  if (!details.empty()) absl::StrAppend(&message, ": ", details);
  return absl::Status(code, message);
}

}  // namespace xz_internal
}  // namespace riegeli
