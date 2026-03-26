// Copyright 2026 Google LLC
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

#ifndef RIEGELI_BASE_HYBRID_DIRECT_COMMON_H_
#define RIEGELI_BASE_HYBRID_DIRECT_COMMON_H_

// IWYU pragma: private, include "riegeli/base/hybrid_direct_map.h"

#include <stddef.h>

#include <type_traits>

#include "absl/base/nullability.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

// The default `Traits` parameter for `HybridDirectMap`.
//
// `expected_min_key` is the expected lower bound of keys. Keys smaller than
// that are never put in the array.
template <typename Key, Key expected_min_key = Key(), typename Enable = void>
struct HybridDirectTraits;

template <typename Key, Key expected_min_key>
struct HybridDirectTraits<Key, expected_min_key,
                          std::enable_if_t<std::is_integral_v<Key>>> {
  using RawKey = std::make_unsigned_t<Key>;

  static RawKey ToRawKey(Key key) {
    // Wrap-around is not an error.
    return static_cast<RawKey>(static_cast<RawKey>(key) -
                               static_cast<RawKey>(expected_min_key));
  }
};

template <typename Key, Key expected_min_key>
struct HybridDirectTraits<Key, expected_min_key,
                          std::enable_if_t<std::is_enum_v<Key>>> {
  using RawKey = std::make_unsigned_t<std::underlying_type_t<Key>>;

  static RawKey ToRawKey(Key key) {
    // Wrap-around is not an error.
    return static_cast<RawKey>(static_cast<RawKey>(key) -
                               static_cast<RawKey>(expected_min_key));
  }
};

// The default `direct_capacity` parameter for `HybridDirectMap` building.
constexpr size_t kHybridDirectDefaultDirectCapacity = 128;

}  // namespace riegeli

#endif  // RIEGELI_BASE_HYBRID_DIRECT_COMMON_H_
