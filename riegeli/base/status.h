// Copyright 2019 Google LLC
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

#ifndef RIEGELI_BASE_STATUS_H_
#define RIEGELI_BASE_STATUS_H_

#include "absl/status/status.h"
#include "absl/strings/string_view.h"

namespace riegeli {

// Returns an `absl::Status` that is identical to `status` except that the
// `message()` has been replaced with `message`.
//
// OK status values have no message and therefore if `status` is OK, the result
// is unchanged.
absl::Status SetMessage(const absl::Status& status, absl::string_view message);

// Returns an `absl::Status` that is identical to `status` except that the
// `message()` has been augmented by adding `message` to the end of the original
// message.
//
// `Annotate()` adds the appropriate separators, so callers should not include a
// separator in `message`. The exact formatting is subject to change, so you
// should not depend on it in your tests.
//
// OK status values have no message and therefore if `status` is OK, the result
// is unchanged.
absl::Status Annotate(const absl::Status& status, absl::string_view message);

}  // namespace riegeli

#endif  // RIEGELI_BASE_STATUS_H_
