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

#ifndef RIEGELI_DIGESTS_CRC32C_DIGESTER_H_
#define RIEGELI_DIGESTS_CRC32C_DIGESTER_H_

#include <stdint.h>

#include "absl/strings/string_view.h"
#include "crc32c/crc32c.h"
#include "riegeli/base/arithmetic.h"

namespace riegeli {

// A Digester computing CRC32C checksums, for `DigestingReader` and
// `DigestingWriter`.
class Crc32cDigester {
 public:
  Crc32cDigester() = default;

  Crc32cDigester(const Crc32cDigester& that) = default;
  Crc32cDigester& operator=(const Crc32cDigester& that) = default;

  void Write(absl::string_view src);

  uint32_t Digest() const { return crc_; }

 private:
  uint32_t crc_ = 0;
};

// Implementation details follow.

inline void Crc32cDigester::Write(absl::string_view src) {
  crc_ = crc32c::Extend(crc_, reinterpret_cast<const uint8_t*>(src.data()),
                        src.size());
}

}  // namespace riegeli

#endif  // RIEGELI_DIGESTS_CRC32C_DIGESTER_H_
