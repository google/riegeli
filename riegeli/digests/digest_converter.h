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
    absl::void_t<decltype(DigestConverterImpl<From, To>::Convert(
        std::declval<From>()))>> : std::true_type {};

// A digest can be converted if its type can be natively explicitly converted.
// This includes the case when `To` is the same as `From`.
template <typename From, typename To>
struct DigestConverterImpl<
    From, To, std::enable_if_t<std::is_constructible<To, From>::value>> {
  template <typename DependentFrom = From,
            std::enable_if_t<!std::is_rvalue_reference<DependentFrom>::value,
                             int> = 0>
  static To Convert(const From& digest) {
    return To(digest);
  }
  template <typename DependentFrom = From,
            std::enable_if_t<!std::is_lvalue_reference<DependentFrom>::value,
                             int> = 0>
  static To Convert(From&& digest) {
    return To(std::move(digest));
  }
};

// `std::array<char, size>` can be converted to `std::string`.
template <size_t size, typename To>
struct DigestConverterImpl<
    std::array<char, size>, To,
    std::enable_if_t<HasDigestConverterImpl<std::string, To>::value>> {
  static To Convert(const std::array<char, size>& digest) {
    return DigestConverterImpl<std::string, To>::Convert(
        std::string(digest.data(), digest.size()));
  }
};

// `uint32_t`, `uint64_t`, and `absl::uint128` can be converted to
// `std::array<char, sizeof(T)>`.

template <typename To>
struct DigestConverterImpl<
    uint32_t, To,
    std::enable_if_t<HasDigestConverterImpl<std::array<char, sizeof(uint32_t)>,
                                            To>::value>> {
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
    std::enable_if_t<HasDigestConverterImpl<std::array<char, sizeof(uint64_t)>,
                                            To>::value>> {
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
    std::enable_if_t<HasDigestConverterImpl<
        std::array<char, sizeof(absl::uint128)>, To>::value>> {
  static To Convert(absl::uint128 digest) {
    std::array<char, sizeof(absl::uint128)> result;
    riegeli::WriteBigEndian64(absl::Uint128High64(digest), result.data());
    riegeli::WriteBigEndian64(absl::Uint128Low64(digest),
                              result.data() + sizeof(uint64_t));
    return DigestConverterImpl<std::array<char, sizeof(absl::uint128)>,
                               To>::Convert(result);
  }
};

// `std::array<uint64_t, size>` can be converted to
// `std::array<char, size * sizeof(uint64_t)>`.
template <size_t size, typename To>
struct DigestConverterImpl<
    std::array<uint64_t, size>, To,
    std::enable_if_t<HasDigestConverterImpl<
        std::array<char, size * sizeof(uint64_t)>, To>::value>> {
  static To Convert(const std::array<uint64_t, size>& digest) {
    std::array<char, size * sizeof(uint64_t)> result;
    riegeli::WriteBigEndian64s(absl::MakeConstSpan(digest.data(), size),
                               result.data());
    return DigestConverterImpl<std::array<char, size * sizeof(uint64_t)>,
                               To>::Convert(result);
  }
};

// `DigestConverter<From, To>` extends `DigestConverterImpl<From, To>` with the
// case of `To` being a reference.

template <typename From, typename To, typename Enable = void>
struct DigestConverter;

template <typename From, typename To>
struct DigestConverter<
    From&, To&, std::enable_if_t<std::is_convertible<From*, To*>::value>> {
  static To& Convert(From& digest) { return digest; }
};

template <typename From, typename To>
struct DigestConverter<
    From, To,
    std::enable_if_t<absl::conjunction<
        absl::negation<std::is_reference<To>>,
        HasDigestConverterImpl<absl::remove_cvref_t<From>, To>>::value>>
    : DigestConverterImpl<absl::remove_cvref_t<From>, To> {
  static_assert(
      std::is_convertible<
          decltype(DigestConverterImpl<absl::remove_cvref_t<From>, To>::Convert(
              std::declval<From>())),
          To>::value,
      "DigestConverterImpl<From, To>::Convert() must return To");
};

// `HasDigestConverter<From, To>::value` is `true` when
// `DigestConverter<From, To>` is defined or when `To` is `void`.
template <typename From, typename To>
struct HasDigestConverter
    : absl::disjunction<
          std::is_void<To>,
          absl::conjunction<std::is_lvalue_reference<To>,
                            std::is_lvalue_reference<From>,
                            std::is_convertible<std::remove_reference_t<From>*,
                                                std::remove_reference_t<To>*>>,
          absl::conjunction<
              absl::negation<std::is_reference<To>>,
              HasDigestConverterImpl<absl::remove_cvref_t<From>, To>>> {};

// Converts a digest returned by `digest_function` to another supported type.
//
// The digest is passed as `digest_function` to support `void`.

template <typename To, typename DigestFunction,
          std::enable_if_t<
              absl::conjunction<
                  absl::negation<std::is_void<To>>,
                  HasDigestConverter<decltype(std::declval<DigestFunction>()()),
                                     To>>::value,
              int> = 0>
inline To ConvertDigest(DigestFunction&& digest_function) {
  return DigestConverter<decltype(std::declval<DigestFunction>()()), To>::
      Convert(std::forward<DigestFunction>(digest_function)());
}

template <typename To, typename DigestFunction,
          std::enable_if_t<std::is_void<To>::value, int> = 0>
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
