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

#include <stddef.h>
#include <stdint.h>

#include <cstring>
#include <limits>

#include "absl/base/optimization.h"
#include "absl/crc/crc32c.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/byte_fill.h"

namespace riegeli {

// A digester computing CRC32C checksums, for `DigestingReader` and
// `DigestingWriter`.
//
// This uses the polynomial x^32 + x^28 + x^27 + x^26 + x^25 + x^23 + x^22 +
// x^20 + x^19 + x^18 + x^14 + x^13 + x^11 + x^10 + x^9 + x^8 + x^6 + 1
// (0x11edc6f41).
//
// This polynomial is used e.g. by SSE4.2:
// https://en.wikipedia.org/wiki/Cyclic_redundancy_check#Polynomial_representations_of_cyclic_redundancy_checks
class Crc32cDigester {
 public:
  Crc32cDigester() : Crc32cDigester(0) {}

  explicit Crc32cDigester(uint32_t seed) : crc_(seed) {}

  Crc32cDigester(const Crc32cDigester& that) = default;
  Crc32cDigester& operator=(const Crc32cDigester& that) = default;

  void Write(absl::string_view src);
  void Write(const absl::Cord& src);
  void Write(ByteFill src);
  uint32_t Digest() { return crc_; }

 private:
  uint32_t crc_;
};

// A common way to mask CRC32C values for storage along with the data.
// These constants are used e.g. by Framed Snappy and TFRecord.

template <uint32_t delta = 0xa282ead8, int ror_bits = 15>
constexpr uint32_t MaskCrc32c(uint32_t unmasked) {
  const uint32_t rotated =
      (unmasked << (32 - ror_bits)) | (unmasked >> ror_bits);
  return rotated + delta;
}

template <uint32_t delta = 0xa282ead8, int ror_bits = 15>
constexpr uint32_t UnmaskCrc32c(uint32_t masked) {
  const uint32_t rotated = masked - delta;
  return (rotated << ror_bits) | (rotated >> (32 - ror_bits));
}

// Implementation details follow.

inline void Crc32cDigester::Write(absl::string_view src) {
  crc_ = static_cast<uint32_t>(absl::ExtendCrc32c(absl::crc32c_t{crc_}, src));
}

inline void Crc32cDigester::Write(const absl::Cord& src) {
  if (const absl::optional<uint32_t> src_crc = src.ExpectedChecksum();
      src_crc != absl::nullopt) {
    crc_ = static_cast<uint32_t>(absl::ConcatCrc32c(
        absl::crc32c_t{crc_}, absl::crc32c_t{*src_crc}, src.size()));
    return;
  }
  if (const absl::optional<absl::string_view> flat = src.TryFlat();
      flat != absl::nullopt) {
    crc_ =
        static_cast<uint32_t>(absl::ExtendCrc32c(absl::crc32c_t{crc_}, *flat));
    return;
  }
  for (const absl::string_view fragment : src.Chunks()) {
    crc_ = static_cast<uint32_t>(
        absl::ExtendCrc32c(absl::crc32c_t{crc_}, fragment));
  }
}

inline void Crc32cDigester::Write(ByteFill src) {
  if (src.fill() == '\0') {
    while (
        ABSL_PREDICT_FALSE(src.size() > std::numeric_limits<size_t>::max())) {
      crc_ = static_cast<uint32_t>(absl::ExtendCrc32cByZeroes(
          absl::crc32c_t{crc_}, std::numeric_limits<size_t>::max()));
      src.Extract(std::numeric_limits<size_t>::max());
    }
    crc_ = static_cast<uint32_t>(absl::ExtendCrc32cByZeroes(
        absl::crc32c_t{crc_}, IntCast<size_t>(src.size())));
    return;
  }
  for (const absl::string_view fragment : src.blocks()) {
    crc_ = static_cast<uint32_t>(
        absl::ExtendCrc32c(absl::crc32c_t{crc_}, fragment));
  }
}

}  // namespace riegeli

#endif  // RIEGELI_DIGESTS_CRC32C_DIGESTER_H_
