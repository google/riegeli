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

#include "riegeli/base/status.h"

#include <string>

#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"

namespace riegeli {

absl::Status SetMessage(const absl::Status& status, absl::string_view message) {
  absl::Status result(status.code(), message);
  status.ForEachPayload(
      [&](absl::string_view type_url, const absl::Cord& payload) {
        result.SetPayload(type_url, payload);
      });
  return result;
}

absl::Status Annotate(const absl::Status& status, absl::string_view message) {
  if (status.ok() || message.empty()) return status;
  return SetMessage(status,
                    status.message().empty()
                        ? message
                        : absl::StrCat(status.message(), "; ", message));
}

}  // namespace riegeli
