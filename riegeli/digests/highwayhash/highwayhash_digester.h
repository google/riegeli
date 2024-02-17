// Copyright 2024 Google LLC
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

#ifndef RIEGELI_DIGESTS_HIGHWAYHASH_HIGHWAYHASH_DIGESTER_H_
#define RIEGELI_DIGESTS_HIGHWAYHASH_HIGHWAYHASH_DIGESTER_H_

#include <stddef.h>

#include <array>
#include <type_traits>

#include "absl/meta/type_traits.h"
#include "absl/strings/string_view.h"
#include "highwayhash/arch_specific.h"
#include "highwayhash/hh_types.h"
#include "highwayhash/highwayhash.h"

namespace riegeli {

// `uint64_t[4]`; prefer `alignas(32)` for constants of this type.
using HighwayHashKey = highwayhash::HHKey;

template <typename ResultType>
class HighwayHashDigester {
 private:
  using DigestType =
      std::conditional_t<std::is_array<ResultType>::value,
                         const std::array<std::remove_extent_t<ResultType>,
                                          std::extent<ResultType>::value>&,
                         ResultType>;

 public:
  explicit HighwayHashDigester(const HighwayHashKey& key) : cat_(key) {}

  void Write(absl::string_view chunk) {
    cat_.Append(chunk.data(), chunk.size());
  }

  void Close() {
    if (is_open_) {
      cat_.Finalize(reinterpret_cast<ResultType*>(&digest_));
      is_open_ = false;
    }
  }

  DigestType Digest() {
    if (is_open_) {
      highwayhash::HighwayHashCatT<HH_TARGET>(cat_).Finalize(
          reinterpret_cast<ResultType*>(&digest_));
    }
    return digest_;
  }

 private:
  highwayhash::HighwayHashCatT<HH_TARGET> cat_;
  absl::remove_cvref_t<DigestType> digest_;
  bool is_open_ = true;
};

using HighwayHash64Digester = HighwayHashDigester<highwayhash::HHResult64>;
using HighwayHash128Digester = HighwayHashDigester<highwayhash::HHResult128>;
using HighwayHash256Digester = HighwayHashDigester<highwayhash::HHResult256>;

}  // namespace riegeli

#endif  // RIEGELI_DIGESTS_HIGHWAYHASH_HIGHWAYHASH_DIGESTER_H_
