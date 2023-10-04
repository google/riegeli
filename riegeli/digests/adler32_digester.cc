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

#include "riegeli/digests/adler32_digester.h"

#include <stdint.h>

#include <limits>

#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "src/zconf.h"
#include "zlib.h"

namespace riegeli {

Adler32Digester::Adler32Digester(uint32_t seed) : adler_(seed) {
  // This checks CPU features.
  adler32(0, nullptr, 0);
}

void Adler32Digester::WriteImpl(absl::string_view src) {
  while (src.size() > std::numeric_limits<uInt>::max()) {
    adler_ = IntCast<uint32_t>(adler32(
        IntCast<uLong>(adler_), reinterpret_cast<const Bytef*>(src.data()),
        std::numeric_limits<uInt>::max()));
    src.remove_prefix(std::numeric_limits<uInt>::max());
  }
  adler_ = IntCast<uint32_t>(adler32(IntCast<uLong>(adler_),
                                     reinterpret_cast<const Bytef*>(src.data()),
                                     IntCast<uInt>(src.size())));
}

}  // namespace riegeli
