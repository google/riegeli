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

#include "riegeli/digests/crc32_digester.h"

#include <stdint.h>

#include <limits>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "zconf.h"
#include "zlib.h"

namespace riegeli {

Crc32Digester::Crc32Digester(uint32_t seed) : crc_(seed) {
  // This checks CPU features.
  crc32(0, nullptr, 0);
}

void Crc32Digester::WriteImpl(absl::string_view src) {
  if (ABSL_PREDICT_FALSE(src.empty())) {
    // `crc32(state, nullptr, 0)` exceptionally returns 0, not `state`.
    return;
  }
  while (src.size() > std::numeric_limits<uInt>::max()) {
    crc_ = IntCast<uint32_t>(crc32(IntCast<uLong>(crc_),
                                   reinterpret_cast<const Bytef*>(src.data()),
                                   std::numeric_limits<uInt>::max()));
    src.remove_prefix(std::numeric_limits<uInt>::max());
  }
  crc_ = IntCast<uint32_t>(crc32(IntCast<uLong>(crc_),
                                 reinterpret_cast<const Bytef*>(src.data()),
                                 IntCast<uInt>(src.size())));
}

}  // namespace riegeli
