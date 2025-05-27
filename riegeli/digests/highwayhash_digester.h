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

#ifndef RIEGELI_DIGESTS_HIGHWAYHASH_DIGESTER_H_
#define RIEGELI_DIGESTS_HIGHWAYHASH_DIGESTER_H_

#include <stddef.h>

#include <array>
#include <type_traits>

#include "absl/base/attributes.h"
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
      std::conditional_t<std::is_array_v<ResultType>,
                         const std::array<std::remove_extent_t<ResultType>,
                                          std::extent_v<ResultType>>&,
                         ResultType>;

 public:
  // The default keys were chosen once with `openssl rand`.
  alignas(32) static const HighwayHashKey kDefaultKey;

  explicit HighwayHashDigester(const HighwayHashKey& key = kDefaultKey)
      : cat_(key) {}

  HighwayHashDigester(const HighwayHashDigester& that) = default;
  HighwayHashDigester& operator=(const HighwayHashDigester& that) = default;

  ABSL_ATTRIBUTE_REINITIALIZES
  void Reset(const HighwayHashKey& key = kDefaultKey) {
    cat_.Reset(key);
    is_open_ = true;
  }

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

// Implementation details follow.

template <>
alignas(32) const HighwayHashKey
    HighwayHashDigester<highwayhash::HHResult64>::kDefaultKey;

template <>
alignas(32) const HighwayHashKey
    HighwayHashDigester<highwayhash::HHResult128>::kDefaultKey;

template <>
alignas(32) const HighwayHashKey
    HighwayHashDigester<highwayhash::HHResult256>::kDefaultKey;

}  // namespace riegeli

#endif  // RIEGELI_DIGESTS_HIGHWAYHASH_DIGESTER_H_
