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

#ifndef RIEGELI_DIGESTS_DIGEST_CONVERTER_H_
#define RIEGELI_DIGESTS_DIGEST_CONVERTER_H_

#include <stddef.h>
#include <stdint.h>

#include <array>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/meta/type_traits.h"
#include "absl/numeric/int128.h"
#include "riegeli/endian/endian_reading.h"
#include "riegeli/endian/endian_writing.h"

namespace riegeli {

// `DigestConverterImpl<From, To>::Convert(From)` specifies how to convert a
// digest to another supported type.
//
// This template is specialized but does not have a primary definition.
template <typename From, typename To, typename Enable = void>
struct DigestConverterImpl;

// `HasDigestConverterImpl<From, To>::value` is `true` when
// `DigestConverterImpl<From, To>` is defined.

template <typename From, typename To, typename Enable = void>
struct HasDigestConverterImpl : std::false_type {};

template <typename From, typename To>
struct HasDigestConverterImpl<
    From, To,
    std::void_t<decltype(DigestConverterImpl<From, To>::Convert(
        std::declval<From>()))>> : std::true_type {};

// `DigestConverterImpl<From, To>` tries to transform `From` from types with
// more information to types with less information, or from integers or arrays
// of integers of larger sizes to arrays of integers of smaller sizes, and then
// considers further transformations, until reaching a type which can be
// natively explicitly converted to `To`.
//
// To prevent infinite recursion, in conversions from arrays of integers of
// smaller sizes to integers or arrays of integers of larger sizes, further
// conversions using `DigestConverterImpl` are not considered, only types which
// can be natively explicitly converted.

// A digest can be converted if its type can be natively explicitly converted.
// This includes the case when `To` is the same as `From`.
template <typename From, typename To>
struct DigestConverterImpl<
    From, To, std::enable_if_t<std::is_constructible_v<To, From>>> {
  template <
      typename DependentFrom = From,
      std::enable_if_t<!std::is_rvalue_reference_v<DependentFrom>, int> = 0>
  static To Convert(const From& digest) {
    return To(digest);
  }
  template <
      typename DependentFrom = From,
      std::enable_if_t<!std::is_lvalue_reference_v<DependentFrom>, int> = 0>
  static To Convert(From&& digest) {
    return To(std::forward<From>(digest));
  }
};

// `std::array<char, size>` can be converted to `std::string`.
template <size_t size, typename To>
struct DigestConverterImpl<
    std::array<char, size>, To,
    std::enable_if_t<std::conjunction_v<
        std::negation<std::is_constructible<To, std::array<char, size>>>,
        HasDigestConverterImpl<std::string, To>>>> {
  static To Convert(const std::array<char, size>& digest) {
    return DigestConverterImpl<std::string, To>::Convert(
        std::string(digest.data(), digest.size()));
  }
};

// `uint32_t`, `uint64_t`, and `absl::uint128` can be converted to
// `std::array<char, sizeof(T)>`, using Big Endian.

template <typename To>
struct DigestConverterImpl<
    uint32_t, To,
    std::enable_if_t<std::conjunction_v<
        std::negation<std::is_constructible<To, uint32_t>>,
        HasDigestConverterImpl<std::array<char, sizeof(uint32_t)>, To>>>> {
  static To Convert(uint32_t digest) {
    std::array<char, sizeof(uint32_t)> result;
    riegeli::WriteBigEndian32(digest, result.data());
    return DigestConverterImpl<std::array<char, sizeof(uint32_t)>, To>::Convert(
        result);
  }
};

template <typename To>
struct DigestConverterImpl<
    uint64_t, To,
    std::enable_if_t<std::conjunction_v<
        std::negation<std::is_constructible<To, uint64_t>>,
        HasDigestConverterImpl<std::array<char, sizeof(uint64_t)>, To>>>> {
  static To Convert(uint64_t digest) {
    std::array<char, sizeof(uint64_t)> result;
    riegeli::WriteBigEndian64(digest, result.data());
    return DigestConverterImpl<std::array<char, sizeof(uint64_t)>, To>::Convert(
        result);
  }
};

template <typename To>
struct DigestConverterImpl<
    absl::uint128, To,
    std::enable_if_t<std::conjunction_v<
        std::negation<std::is_constructible<To, absl::uint128>>,
        HasDigestConverterImpl<std::array<char, sizeof(absl::uint128)>, To>>>> {
  static To Convert(absl::uint128 digest) {
    std::array<char, sizeof(absl::uint128)> result;
    riegeli::WriteBigEndian128(digest, result.data());
    return DigestConverterImpl<std::array<char, sizeof(absl::uint128)>,
                               To>::Convert(result);
  }
};

// `std::array<char, sizeof(T)>` can be converted to `uint32_t`, `uint64_t`,
// and `absl::uint128`, using Big Endian.
//
// To prevent infinite recursion, further conversions using
// `DigestConverterImpl` are not considered, only types which can be natively
// explicitly converted.

template <typename To>
struct DigestConverterImpl<std::array<char, sizeof(uint32_t)>, To,
                           std::enable_if_t<std::conjunction_v<
                               std::negation<std::is_constructible<
                                   To, std::array<char, sizeof(uint32_t)>>>,
                               std::is_constructible<To, uint32_t>>>> {
  static To Convert(std::array<char, sizeof(uint32_t)> digest) {
    return To(riegeli::ReadBigEndian32(digest.data()));
  }
};

template <typename To>
struct DigestConverterImpl<std::array<char, sizeof(uint64_t)>, To,
                           std::enable_if_t<std::conjunction_v<
                               std::negation<std::is_constructible<
                                   To, std::array<char, sizeof(uint64_t)>>>,
                               std::is_constructible<To, uint64_t>>>> {
  static To Convert(std::array<char, sizeof(uint64_t)> digest) {
    return To(riegeli::ReadBigEndian64(digest.data()));
  }
};

template <typename To>
struct DigestConverterImpl<
    std::array<char, sizeof(absl::uint128)>, To,
    std::enable_if_t<
        std::conjunction_v<std::negation<std::is_constructible<
                               To, std::array<char, sizeof(absl::uint128)>>>,
                           std::is_constructible<To, absl::uint128>>>> {
  static To Convert(std::array<char, sizeof(absl::uint128)> digest) {
    return To(riegeli::ReadBigEndian128(digest.data()));
  }
};

// `std::array<uint64_t, size>` can be converted to
// `std::array<char, size * sizeof(uint64_t)>`, using Big Endian.
template <size_t size, typename To>
struct DigestConverterImpl<
    std::array<uint64_t, size>, To,
    std::enable_if_t<std::conjunction_v<
        std::negation<std::is_constructible<To, std::array<uint64_t, size>>>,
        HasDigestConverterImpl<std::array<char, size * sizeof(uint64_t)>,
                               To>>>> {
  static To Convert(const std::array<uint64_t, size>& digest) {
    std::array<char, size * sizeof(uint64_t)> result;
    riegeli::WriteBigEndian64s(absl::MakeConstSpan(digest.data(), size),
                               result.data());
    return DigestConverterImpl<std::array<char, size * sizeof(uint64_t)>,
                               To>::Convert(result);
  }
};

// `std::array<char, size * sizeof(uint64_t)>` can be converted to
// `std::array<uint64_t, size>`, using Big Endian.
//
// To prevent infinite recursion, further conversions using
// `DigestConverterImpl` are not considered, only types which can be natively
// explicitly converted.
template <size_t size, typename To>
struct DigestConverterImpl<
    std::array<char, size>, To,
    std::enable_if_t<std::conjunction_v<
        std::bool_constant<size % sizeof(uint64_t) == 0>,
        std::negation<std::is_constructible<To, std::array<char, size>>>,
        std::is_constructible<
            To, std::array<uint64_t, size / sizeof(uint64_t)>>>>> {
  static To Convert(const std::array<char, size>& digest) {
    std::array<uint64_t, size / sizeof(uint64_t)> result;
    riegeli::ReadBigEndian64s(
        digest.data(), absl::MakeSpan(result.data(), size / sizeof(uint64_t)));
    return To(result);
  }
};

// `DigestConverter<From, To>` extends `DigestConverterImpl<From, To>` with the
// case of `To` being a reference.

template <typename From, typename To, typename Enable = void>
struct DigestConverter;

template <typename From, typename To>
struct DigestConverter<From&, To&,
                       std::enable_if_t<std::is_convertible_v<From*, To*>>> {
  static To& Convert(From& digest) { return digest; }
};

template <typename From, typename To>
struct DigestConverter<
    From, To,
    std::enable_if_t<std::conjunction_v<
        std::negation<std::is_reference<To>>,
        HasDigestConverterImpl<absl::remove_cvref_t<From>, To>>>>
    : DigestConverterImpl<absl::remove_cvref_t<From>, To> {
  static_assert(
      std::is_convertible_v<
          decltype(DigestConverterImpl<absl::remove_cvref_t<From>, To>::Convert(
              std::declval<From>())),
          To>,
      "DigestConverterImpl<From, To>::Convert() must return To");
};

// `HasDigestConverter<From, To>::value` is `true` when
// `DigestConverter<From, To>` is defined or when `To` is `void`.
template <typename From, typename To>
struct HasDigestConverter
    : std::disjunction<
          std::is_void<To>,
          std::conjunction<std::is_lvalue_reference<To>,
                           std::is_lvalue_reference<From>,
                           std::is_convertible<std::remove_reference_t<From>*,
                                               std::remove_reference_t<To>*>>,
          std::conjunction<
              std::negation<std::is_reference<To>>,
              HasDigestConverterImpl<absl::remove_cvref_t<From>, To>>> {};

// Converts a digest returned by `digest_function` to another supported type.
//
// The digest is passed as `digest_function` to support `void`.

template <
    typename To, typename DigestFunction,
    std::enable_if_t<
        std::conjunction_v<
            std::negation<std::is_void<To>>,
            HasDigestConverter<decltype(std::declval<DigestFunction>()()), To>>,
        int> = 0>
inline To ConvertDigest(DigestFunction&& digest_function) {
  return DigestConverter<decltype(std::declval<DigestFunction>()()), To>::
      Convert(std::forward<DigestFunction>(digest_function)());
}

template <typename To, typename DigestFunction,
          std::enable_if_t<std::is_void_v<To>, int> = 0>
inline void ConvertDigest(DigestFunction&& digest_function) {
  std::forward<DigestFunction>(digest_function)();
}

namespace digest_converter_internal {

// A placeholder type to support an optional template parameter specifying the
// desired digest type, which precedes other template parameters to be deduced
// from function arguments.
//
// The optional template parameter should default to the digest type deduced
// from later template parameters, but this cannot be written directly because
// they are not in scope yet. Instead, the optional template parameter defaults
// to `NoConversion` and is resolved later.
class NoConversion;

template <typename From, typename To>
struct ResolveNoConversionImpl {
  using type = To;
};

template <typename From>
struct ResolveNoConversionImpl<From, NoConversion> {
  using type = From;
};

template <typename From, typename To>
using ResolveNoConversion = typename ResolveNoConversionImpl<From, To>::type;

template <typename From, typename To>
struct HasDigestConverterOrNoConversion : HasDigestConverter<From, To> {};

template <typename From>
struct HasDigestConverterOrNoConversion<From, NoConversion> : std::true_type {};

}  // namespace digest_converter_internal

}  // namespace riegeli

#endif  // RIEGELI_DIGESTS_DIGEST_CONVERTER_H_
