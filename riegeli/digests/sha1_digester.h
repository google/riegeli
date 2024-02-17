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

#ifndef RIEGELI_DIGESTS_SHA1_DIGESTER_H_
#define RIEGELI_DIGESTS_SHA1_DIGESTER_H_

#include "openssl/base.h"
#include "openssl/sha.h"
#include "riegeli/digests/openssl_digester.h"

namespace riegeli {

// A digester computing SHA-1 checksums, for `DigestingReader` and
// `DigestingWriter`.
//
// Warning: SHA-1 as a cryptographic hash function is broken.
// Use this only if a preexisting format has already decided to use SHA-1.
// Please contact ise-team@ in case of doubt.
using Sha1Digester = OpenSslDigester<SHA_CTX, SHA1_Init, SHA1_Update,
                                     SHA1_Final, SHA_DIGEST_LENGTH>;

}  // namespace riegeli

#endif  // RIEGELI_DIGESTS_SHA1_DIGESTER_H_
