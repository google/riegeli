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

#ifndef RIEGELI_DIGESTS_SHA512_DIGESTER_H_
#define RIEGELI_DIGESTS_SHA512_DIGESTER_H_

#include "openssl/base.h"
#include "openssl/sha.h"
#include "riegeli/digests/openssl_digester.h"

namespace riegeli {

// A digester computing SHA-512 checksums, for `DigestingReader` and
// `DigestingWriter`.
using Sha512Digester = OpenSslDigester<SHA512_CTX, SHA512_Init, SHA512_Update,
                                       SHA512_Final, SHA512_DIGEST_LENGTH>;

}  // namespace riegeli

#endif  // RIEGELI_DIGESTS_SHA512_DIGESTER_H_
