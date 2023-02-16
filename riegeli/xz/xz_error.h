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

#ifndef RIEGELI_XZ_XZ_ERROR_H_
#define RIEGELI_XZ_XZ_ERROR_H_

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "lzma.h"

namespace riegeli {
namespace xz_internal {

absl::Status XzErrorToStatus(absl::string_view operation,
                             lzma_ret liblzma_code);

}  // namespace xz_internal
}  // namespace riegeli

#endif  // RIEGELI_XZ_XZ_ERROR_H_
