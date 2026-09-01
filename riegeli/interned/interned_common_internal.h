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

#ifndef RIEGELI_INTERNED_INTERNED_COMMON_INTERNAL_H_
#define RIEGELI_INTERNED_INTERNED_COMMON_INTERNAL_H_

#include <stddef.h>
#include <stdint.h>

#include <type_traits>
#include <utility>

#include "absl/base/nullability.h"
#include "absl/numeric/bits.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli::interned_internal {

// Default template parameter `num_shards` for global interners.
// Also, a default template parameter for `Concurrent` nested types.
constexpr size_t kDefaultInternerNumShards = 64;

template <typename Arg, typename T, typename Hash, typename Eq,
          typename Enable = void>
struct SupportedByHashAndEq : std::is_same<std::decay_t<Arg>, T> {};

template <typename Arg, typename T, typename Hash, typename Eq>
struct SupportedByHashAndEq<
    Arg, T, Hash, Eq,
    std::void_t<typename Hash::is_transparent, typename Eq::is_transparent,
                decltype(std::declval<const Hash&>()(
                    std::declval<const Arg&>())),
                decltype(std::declval<const Eq&>()(
                    std::declval<const T&>(), std::declval<const Arg&>()))>>
    : std::true_type {};

template <size_t num_shards>
inline size_t ShardIndex(size_t hash) {
  static_assert(absl::has_single_bit(num_shards));
  if constexpr (num_shards == 1) {
    return 0;
  } else {
    static constexpr int kBits = sizeof(size_t) * 8;
    // Same as `absl::hash_internal::kMul`.
    static constexpr size_t kMul =
        static_cast<size_t>(0x79d5'f9e0'de1e'8cf5 >> (64 - kBits));
    return (hash * kMul) >> (kBits - absl::bit_width(num_shards - 1));
  }
}

// `HasTransparentNullptrHash<Hash>::value` is `true` if `Hash` is transparent
// and supports hashing `nullptr`.
//
// In that case, `Hash` will be used to hash not only `T` but also `nullptr`,
// so that `Interned<T>::ValueHash` is consistent with `Hash` for `nullptr`.
template <typename Hash, typename Enable = void>
struct HasTransparentNullptrHash : std::false_type {};

template <typename Hash>
struct HasTransparentNullptrHash<
    Hash, std::void_t<typename Hash::is_transparent,
                      decltype(std::declval<const Hash&>()(nullptr))>>
    : std::true_type {};

}  // namespace riegeli::interned_internal

#endif  // RIEGELI_INTERNED_INTERNED_COMMON_INTERNAL_H_
