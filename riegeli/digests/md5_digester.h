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

#ifndef RIEGELI_DIGESTS_MD5_DIGESTER_H_
#define RIEGELI_DIGESTS_MD5_DIGESTER_H_

#include "openssl/base.h"
#include "openssl/md5.h"
#include "riegeli/digests/openssl_digester.h"

namespace riegeli {

// A digester computing MD5 checksums, for `DigestingReader` and
// `DigestingWriter`.
//
// Warning: MD5 as a cryptographic hash function is broken.
// Use this only if a preexisting format has already decided to use MD5.
// Please contact ise-team@ in case of doubt.
using Md5Digester = OpenSslDigester<MD5_CTX, MD5_Init, MD5_Update, MD5_Final,
                                    MD5_DIGEST_LENGTH>;

}  // namespace riegeli

#endif  // RIEGELI_DIGESTS_MD5_DIGESTER_H_
