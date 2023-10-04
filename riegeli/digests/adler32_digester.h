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

#ifndef RIEGELI_DIGESTS_ADLER32_DIGESTER_H_
#define RIEGELI_DIGESTS_ADLER32_DIGESTER_H_

#include <stdint.h>

#include "absl/strings/string_view.h"
#include "riegeli/digests/digester.h"

namespace riegeli {

// A Digester computing Adler32 checksums, for `DigestingReader` and
// `DigestingWriter`.
class Adler32Digester : public Digester<uint32_t> {
 public:
  Adler32Digester() : Adler32Digester(1) {}

  explicit Adler32Digester(uint32_t seed);

  Adler32Digester(const Adler32Digester& that) = default;
  Adler32Digester& operator=(const Adler32Digester& that) = default;

  Adler32Digester(Adler32Digester&& that) = default;
  Adler32Digester& operator=(Adler32Digester&& that) = default;

 protected:
  void WriteImpl(absl::string_view src) override;
  uint32_t DigestImpl() override { return adler_; }

 private:
  uint32_t adler_;
};

}  // namespace riegeli

#endif  // RIEGELI_DIGESTS_ADLER32_DIGESTER_H_
