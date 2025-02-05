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

#include "riegeli/records/skipped_region.h"

#include <ostream>
#include <string>

#include "absl/strings/str_cat.h"

namespace riegeli {

std::string SkippedRegion::ToString() const {
  return absl::StrCat("[", begin_, "..", end_, "): ", message_);
}

void SkippedRegion::Output(std::ostream& dest) const { dest << ToString(); }

}  // namespace riegeli
