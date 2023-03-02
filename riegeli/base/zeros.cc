// Copyright 2020 Google LLC
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

#include "riegeli/base/zeros.h"

#include <stddef.h>

#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/cord_utils.h"
#include "riegeli/base/no_destructor.h"

namespace riegeli {

absl::Cord CordOfZeros(size_t length) {
  const absl::string_view kArrayOfZeros = ArrayOfZeros();
  absl::Cord result;
  while (length >= kArrayOfZeros.size()) {
    static const NoDestructor<absl::Cord> kCordOfZeros(
        absl::MakeCordFromExternal(kArrayOfZeros, [] {}));
    result.Append(*kCordOfZeros);
    length -= kArrayOfZeros.size();
  }
  if (length > 0) {
    const absl::string_view zeros = kArrayOfZeros.substr(0, length);
    if (length <= MaxBytesToCopyToCord(result)) {
      result.Append(zeros);
    } else {
      result.Append(absl::MakeCordFromExternal(zeros, [] {}));
    }
  }
  return result;
}

}  // namespace riegeli
