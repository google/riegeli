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
// IWYU pragma: private, include "riegeli/base/hybrid_direct_set.h"

#include <stddef.h>

#include <type_traits>

#include "absl/base/nullability.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

namespace hybrid_direct_internal {

template <typename Key, typename Enable = void>
struct HasRiegeliHybridDirectToRawKey : std::false_type {};

template <typename Key>
struct HasRiegeliHybridDirectToRawKey<
    Key, std::enable_if_t<std::is_unsigned_v<
             decltype(RiegeliHybridDirectToRawKey(std::declval<Key>()))>>>
    : std::true_type {};

template <typename Key, typename Enable = void>
struct HasRiegeliHybridDirectFromRawKey : std::false_type {};

template <typename Key>
struct HasRiegeliHybridDirectFromRawKey<
    Key, std::enable_if_t<std::is_convertible_v<
             decltype(RiegeliHybridDirectFromRawKey(
                 RiegeliHybridDirectToRawKey(std::declval<Key>()),
                 static_cast<Key*>(nullptr))),
             Key>>> : std::true_type {};

}  // namespace hybrid_direct_internal

// The default `Traits` parameter for `HybridDirectMap` and `HybridDirectSet`,
// which specifies a mapping of keys to an unsigned integer type.
//
// Key types supported by default are integral types, enum types, and types
// supporting `RiegeliHybridDirectToRawKey()` as below. The latter takes
// precedence.
//
// To override `HybridDirectTraits` for a type `Key`, define a free function
// `friend RawKey RiegeliHybridDirectToRawKey(Key key)` as a friend of `Key`
// inside class definition or in the same namespace as `Key`, so that it can be
// found via ADL. Different `Key` values must yield different `RawKey` values.
//
// Optionally, define also a free function
// `friend Key RiegeliHybridDirectFromRawKey(RawKey raw_key, Key*)`.
// This is needed only for iterators.
//
// The second argument of `RiegeliHybridDirectFromRawKey()` is always a null
// pointer, used to choose the right overload based on the type.
//
// `expected_min_key` is the expected lower bound of keys. Keys smaller than
// that are never put in the array. `expected_min_key` has a type which supports
// `static_cast<RawKey>(expected_min_key)`.
template <typename Key, auto expected_min_key = 0, typename Enable = void>
struct HybridDirectTraits;

template <typename Key, auto expected_min_key>
struct HybridDirectTraits<
    Key, expected_min_key,
    std::enable_if_t<
        hybrid_direct_internal::HasRiegeliHybridDirectToRawKey<Key>::value>> {
 private:
  using RawKey = decltype(RiegeliHybridDirectToRawKey(std::declval<Key>()));

 public:
  static RawKey ToRawKey(Key key) {
    // Wrap-around is not an error.
    return static_cast<RawKey>(RiegeliHybridDirectToRawKey(key) -
                               static_cast<RawKey>(expected_min_key));
  }

  template <
      typename DependentKey = Key,
      std::enable_if_t<hybrid_direct_internal::HasRiegeliHybridDirectFromRawKey<
                           DependentKey>::value,
                       int> = 0>
  static Key FromRawKey(RawKey raw_key) {
    // Wrap-around is not an error.
    return RiegeliHybridDirectFromRawKey(
        static_cast<RawKey>(raw_key + static_cast<RawKey>(expected_min_key)),
        static_cast<Key*>(nullptr));
  }
};

template <typename Key, auto expected_min_key>
struct HybridDirectTraits<
    Key, expected_min_key,
    std::enable_if_t<std::conjunction_v<
        std::negation<
            hybrid_direct_internal::HasRiegeliHybridDirectToRawKey<Key>>,
        std::is_integral<Key>>>> {
 private:
  using RawKey = std::make_unsigned_t<Key>;

 public:
  static RawKey ToRawKey(Key key) {
    // Wrap-around is not an error.
    return static_cast<RawKey>(static_cast<RawKey>(key) -
                               static_cast<RawKey>(expected_min_key));
  }

  static Key FromRawKey(RawKey raw_key) {
    // Wrap-around is not an error.
    return static_cast<Key>(
        static_cast<RawKey>(raw_key + static_cast<RawKey>(expected_min_key)));
  }
};

template <typename Key, auto expected_min_key>
struct HybridDirectTraits<
    Key, expected_min_key,
    std::enable_if_t<std::conjunction_v<
        std::negation<
            hybrid_direct_internal::HasRiegeliHybridDirectToRawKey<Key>>,
        std::is_enum<Key>>>> {
 private:
  using RawKey = std::make_unsigned_t<std::underlying_type_t<Key>>;

 public:
  static RawKey ToRawKey(Key key) {
    // Wrap-around is not an error.
    return static_cast<RawKey>(static_cast<RawKey>(key) -
                               static_cast<RawKey>(expected_min_key));
  }

  static Key FromRawKey(RawKey raw_key) {
    // Wrap-around is not an error.
    return static_cast<Key>(
        static_cast<RawKey>(raw_key + static_cast<RawKey>(expected_min_key)));
  }
};

// The default `direct_capacity` parameter for `HybridDirectMap` and
// `HybridDirectSet` building.
constexpr size_t kHybridDirectDefaultDirectCapacity = 128;

}  // namespace riegeli

#endif  // RIEGELI_BASE_HYBRID_DIRECT_COMMON_H_
