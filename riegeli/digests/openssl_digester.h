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

#ifndef RIEGELI_DIGESTS_OPENSSL_DIGESTER_H_
#define RIEGELI_DIGESTS_OPENSSL_DIGESTER_H_

#include <stddef.h>
#include <stdint.h>

#include <array>

#include "absl/strings/string_view.h"

namespace riegeli {

// A digester template computing checksums implemented by OpenSSL, for
// `DigestingReader` and `DigestingWriter`.
template <typename H, int (*init)(H*), int (*update)(H*, const void*, size_t),
          int (*final)(uint8_t*, H*), int digest_size>
class OpenSslDigester {
 public:
  OpenSslDigester() { init(&ctx_); }

  OpenSslDigester(const OpenSslDigester& that) = default;
  OpenSslDigester& operator=(const OpenSslDigester& that) = default;

  void Reset() {
    init(&ctx_);
    is_open_ = true;
  }

  void Write(absl::string_view src) { update(&ctx_, src.data(), src.size()); }

  void Close() {
    if (is_open_) {
      final(reinterpret_cast<uint8_t*>(digest_.data()), &ctx_);
      is_open_ = false;
    }
  }

  std::array<char, digest_size> Digest() {
    if (is_open_) {
      H copy = ctx_;
      final(reinterpret_cast<uint8_t*>(digest_.data()), &copy);
    }
    return digest_;
  }

 private:
  H ctx_;
  std::array<char, digest_size> digest_;
  bool is_open_ = true;
};

}  // namespace riegeli

#endif  // RIEGELI_DIGESTS_OPENSSL_DIGESTER_H_
