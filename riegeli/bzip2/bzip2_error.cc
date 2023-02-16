// Copyright 2022 Google LLC
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

#include "riegeli/bzip2/bzip2_error.h"

#include <string>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "bzlib.h"
#include "riegeli/base/assert.h"

namespace riegeli {
namespace bzip2_internal {

absl::Status Bzip2ErrorToStatus(absl::string_view operation, int bzlib_code) {
  absl::StatusCode code;
  switch (bzlib_code) {
    case BZ_OK:
    case BZ_RUN_OK:
    case BZ_FLUSH_OK:
    case BZ_FINISH_OK:
      return absl::OkStatus();
    case BZ_DATA_ERROR:
    case BZ_DATA_ERROR_MAGIC:
      code = absl::StatusCode::kInvalidArgument;
      break;
    case BZ_MEM_ERROR:
      code = absl::StatusCode::kResourceExhausted;
      break;
    default:
      // Should not happen.
      code = absl::StatusCode::kInternal;
      break;
  }
  std::string message = absl::StrCat(operation, " failed");
  absl::string_view details;
  switch (bzlib_code) {
    case BZ_OK:
    case BZ_RUN_OK:
    case BZ_FLUSH_OK:
    case BZ_FINISH_OK:
      RIEGELI_ASSERT_UNREACHABLE() << "Handled before switch";
    case BZ_STREAM_END:
      details = "stream end";
      break;
    case BZ_SEQUENCE_ERROR:
      details = "sequence error";
      break;
    case BZ_PARAM_ERROR:
      details = "parameter error";
      break;
    case BZ_MEM_ERROR:
      details = "memory error";
      break;
    case BZ_DATA_ERROR:
      details = "data error";
      break;
    case BZ_DATA_ERROR_MAGIC:
      details = "data error (magic)";
      break;
    case BZ_IO_ERROR:
      details = "I/O error";
      break;
    case BZ_UNEXPECTED_EOF:
      details = "unexpected EOF";
      break;
    case BZ_OUTBUFF_FULL:
      details = "output buffer full";
      break;
    case BZ_CONFIG_ERROR:
      details = "config error";
      break;
    default:
      absl::StrAppend(&message, ": unknown bzlib error code: ", bzlib_code);
      break;
  }
  if (!details.empty()) absl::StrAppend(&message, ": ", details);
  return absl::Status(code, message);
}

}  // namespace bzip2_internal
}  // namespace riegeli
