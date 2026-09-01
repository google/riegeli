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

#include <cstring>
#include <type_traits>
#include <utility>

#include "absl/base/config.h"
#include "absl/base/nullability.h"
#include "absl/container/hash_container_defaults.h"
#include "absl/numeric/bits.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/cord_utils.h"
#include "riegeli/base/null_safe_memcpy.h"

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

template <typename Arg, typename Encoder, typename Enable = void>
struct SupportedByEncoderForIntern : std::false_type {};

template <typename Arg, typename Encoder>
struct SupportedByEncoderForIntern<
    Arg, Encoder,
    std::void_t<decltype(Encoder::EncodedEmpty(std::declval<const Arg&>())),
                decltype(Encoder::EncodedSize(std::declval<const Arg&>())),
                decltype(Encoder::Encode(std::declval<const Arg&>(),
                                         std::declval<char*>()))>>
    : SupportedByHashAndEq<Arg, absl::string_view, typename Encoder::Hash,
                           typename Encoder::Eq> {};

struct DefaultStringEncoder {
  using Hash = absl::DefaultHashContainerHash<absl::string_view>;
  using Eq = absl::DefaultHashContainerEq<absl::string_view>;

  static bool EncodedEmpty(absl::string_view src) { return src.empty(); }
  static bool EncodedEmpty(const absl::Cord& src) { return src.empty(); }

  static size_t EncodedSize(absl::string_view src) { return src.size(); }
  static size_t EncodedSize(const absl::Cord& src) { return src.size(); }

  static void Encode(absl::string_view src, char* dest) {
    riegeli::null_safe_memcpy(dest, src.data(), src.size());
  }
  static void Encode(const absl::Cord& src, char* dest) {
    cord_internal::CopyCordToArray(src, dest);
  }
};

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

inline uint16_t ReadLittleEndian16(const char* src) {
#if ABSL_IS_LITTLE_ENDIAN
  uint16_t dest;
  std::memcpy(&dest, src, sizeof(uint16_t));
  return dest;
#else
  return static_cast<uint16_t>(static_cast<uint8_t>(src[0])) |
         (static_cast<uint16_t>(static_cast<uint8_t>(src[1])) << 8);
#endif
}

inline void WriteLittleEndian16(uint16_t data, char* dest) {
#if ABSL_IS_LITTLE_ENDIAN
  std::memcpy(dest, &data, sizeof(uint16_t));
#else
  dest[0] = static_cast<char>(data);
  dest[1] = static_cast<char>(data >> 8);
#endif
}

inline size_t ReadLittleEndianSize(const char* src) {
#if ABSL_IS_LITTLE_ENDIAN
  size_t dest;
  std::memcpy(&dest, src, sizeof(size_t));
  return dest;
#else
  size_t dest = 0;
  for (size_t i = 0; i < sizeof(size_t); ++i) {
    dest |= size_t{static_cast<uint8_t>(src[i])} << (i * 8);
  }
  return dest;
#endif
}

inline void WriteLittleEndianSize(size_t data, char* dest) {
#if ABSL_IS_LITTLE_ENDIAN
  std::memcpy(dest, &data, sizeof(size_t));
#else
  for (size_t i = 0; i < sizeof(size_t); ++i) {
    dest[i] = static_cast<char>(data >> (i * 8));
  }
#endif
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
