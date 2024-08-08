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

#include "riegeli/base/shared_buffer.h"

#include <stddef.h>

#include <ostream>

#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"

namespace riegeli {

void SharedBuffer::DumpStructure(absl::string_view substr,
                                 std::ostream& out) const {
  out << "[shared_buffer] {";
  const size_t ref_count = GetRefCount();
  if (ref_count != 1) out << " ref_count: " << ref_count;
  if (!substr.empty()) {
    if (substr.data() != data()) {
      out << " space_before: " << PtrDistance(data(), substr.data());
    }
    out << " space_after: "
        << PtrDistance(substr.data() + substr.size(), data() + capacity());
  }
  out << " }";
}

}  // namespace riegeli
