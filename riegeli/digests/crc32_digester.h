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

#ifndef RIEGELI_DIGESTS_CRC32_DIGESTER_H_
#define RIEGELI_DIGESTS_CRC32_DIGESTER_H_

#include <stdint.h>

#include "absl/strings/string_view.h"

namespace riegeli {

// A digester computing CRC32 checksums, for `DigestingReader` and
// `DigestingWriter`.
//
// This uses the polynomial x^32 + x^26 + x^23 + x^22 + x^16 + x^12 + x^11 +
// x^10 + x^8 + x^7 + x^5 + x^4 + x^2 + x + 1 (0x104c11db7).
//
// This polynomial is used e.g. by gzip, zip, and png:
// https://en.wikipedia.org/wiki/Cyclic_redundancy_check#Polynomial_representations_of_cyclic_redundancy_checks
class Crc32Digester {
 public:
  Crc32Digester() : Crc32Digester(0) {}

  explicit Crc32Digester(uint32_t seed);

  Crc32Digester(const Crc32Digester& that) = default;
  Crc32Digester& operator=(const Crc32Digester& that) = default;

  void Write(absl::string_view src);
  uint32_t Digest() { return crc_; }

 private:
  uint32_t crc_;
};

}  // namespace riegeli

#endif  // RIEGELI_DIGESTS_CRC32_DIGESTER_H_
