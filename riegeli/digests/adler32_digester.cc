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

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "zconf.h"
#include "zlib.h"

namespace riegeli {

Adler32Digester::Adler32Digester(uint32_t seed) : adler_(seed) {
  // This checks CPU features.
  adler32_z(0, nullptr, 0);
}

void Adler32Digester::Write(absl::string_view src) {
  if (ABSL_PREDICT_FALSE(src.empty())) {
    // `adler32_z(state, nullptr, 0)` exceptionally returns 1, not `state`.
    return;
  }
  adler_ = IntCast<uint32_t>(adler32_z(
      IntCast<uLong>(adler_), reinterpret_cast<const Bytef*>(src.data()),
      IntCast<z_size_t>(src.size())));
}

}  // namespace riegeli
