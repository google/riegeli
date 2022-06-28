// Copyright 2021 Google LLC
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

#ifndef RIEGELI_DIGESTS_DIGESTING_INTERNAL_H_
#define RIEGELI_DIGESTS_DIGESTING_INTERNAL_H_

#include <stddef.h>

#include <array>
#include <type_traits>
#include <utility>

#include "absl/meta/type_traits.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/memory.h"

namespace riegeli {
namespace digesting_internal {

// `digester_internal::WriteZeros()` calls `Digester::WriteZeros()`, or uses
// `Digester::Write()` if that is not defined.

template <typename Digester, typename Enable = void>
struct HasWriteZeros : std::false_type {};

template <typename Digester>
struct HasWriteZeros<Digester,
                     absl::void_t<decltype(std::declval<Digester>().WriteZeros(
                         std::declval<Position>()))>> : std::true_type {};

template <typename Digester,
          absl::enable_if_t<HasWriteZeros<Digester>::value, int> = 0>
inline void WriteZeros(Digester& digester, Position length) {
  digester.WriteZeros(length);
}

template <typename Digester,
          absl::enable_if_t<!HasWriteZeros<Digester>::value, int> = 0>
inline void WriteZeros(Digester& digester, Position length) {
  while (length > kArrayOfZeros.size()) {
    digester.Write(
        absl::string_view(kArrayOfZeros.data(), kArrayOfZeros.size()));
    length -= kArrayOfZeros.size();
  }
  digester.Write(
      absl::string_view(kArrayOfZeros.data(), IntCast<size_t>(length)));
}

// `digester_internal::Close()` calls `Digester::Close()`, or does nothing if
// that is not defined.

template <typename Digester, typename Enable = void>
struct HasClose : std::false_type {};

template <typename Digester>
struct HasClose<Digester,
                absl::void_t<decltype(std::declval<Digester>().Close())>>
    : std::true_type {};

template <typename Digester,
          absl::enable_if_t<HasClose<Digester>::value, int> = 0>
inline void Close(Digester& digester) {
  digester.Close();
}

template <typename Digester,
          absl::enable_if_t<!HasClose<Digester>::value, int> = 0>
inline void Close(Digester& digester) {}

// `digester_internal::DigesterType` is the result of `Digester::Digest()`, or
// `void` if that is not defined.
//
// `digester_internal::Digest()` calls `Digester::Digest()`, or does nothing if
// that is not defined.

template <typename Digester, typename Enable = void>
struct HasDigest : std::false_type {};

template <typename Digester>
struct HasDigest<Digester,
                 absl::void_t<decltype(std::declval<Digester>().Digest())>>
    : std::true_type {};

template <typename Digester, typename Enable = void>
struct DigestTypeImpl {
  using type = void;
};

template <typename Digester>
struct DigestTypeImpl<Digester, std::enable_if_t<HasDigest<Digester>::value>> {
  using type = decltype(std::declval<Digester>().Digest());
};

template <typename Digester>
using DigestType = typename DigestTypeImpl<Digester>::type;

template <typename Digester,
          absl::enable_if_t<HasDigest<Digester>::value, int> = 0>
inline DigestType<Digester> Digest(Digester& digester) {
  return digester.Digest();
}

template <typename Digester,
          absl::enable_if_t<!HasDigest<Digester>::value, int> = 0>
inline DigestType<Digester> Digest(Digester& digester) {}

}  // namespace digesting_internal
}  // namespace riegeli

#endif  // RIEGELI_DIGESTS_DIGESTING_INTERNAL_H_
