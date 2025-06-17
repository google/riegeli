// Copyright 2022 Google LLC
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

#ifndef RIEGELI_BYTES_STRINGIFY_H_
#define RIEGELI_BYTES_STRINGIFY_H_

#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/numeric/int128.h"
#include "absl/strings/cord.h"
#include "absl/strings/has_absl_stringify.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/byte_fill.h"
#include "riegeli/base/bytes_ref.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/types.h"

namespace riegeli {

// `riegeli::StringifiedSize()` of a stringifiable value returns the size of its
// stringification as `Position` if easily known, otherwise it is only declared
// as returning `void`.
//
// It has the same overloads as `Writer::Write()`, assuming that the parameter
// is passed by const reference.
inline Position StringifiedSize(ABSL_ATTRIBUTE_UNUSED char src) { return 1; }
#if __cpp_char8_t
inline Position StringifiedSize(ABSL_ATTRIBUTE_UNUSED char8_t src) { return 1; }
#endif
inline Position StringifiedSize(BytesRef src) { return src.size(); }
ABSL_ATTRIBUTE_ALWAYS_INLINE inline Position StringifiedSize(const char* src) {
  return absl::string_view(src).size();
}
inline Position StringifiedSize(const Chain& src) { return src.size(); }
inline Position StringifiedSize(const absl::Cord& src) { return src.size(); }
inline Position StringifiedSize(ByteFill src) { return src.size(); }
void StringifiedSize(signed char);
void StringifiedSize(unsigned char);
void StringifiedSize(short src);
void StringifiedSize(unsigned short src);
void StringifiedSize(int src);
void StringifiedSize(unsigned src);
void StringifiedSize(long src);
void StringifiedSize(unsigned long src);
void StringifiedSize(long long src);
void StringifiedSize(unsigned long long src);
void StringifiedSize(absl::int128 src);
void StringifiedSize(absl::uint128 src);
void StringifiedSize(float);
void StringifiedSize(double);
void StringifiedSize(long double);
template <typename Src,
          std::enable_if_t<
              std::conjunction_v<
                  absl::HasAbslStringify<Src>,
                  std::negation<std::is_convertible<Src&&, BytesRef>>,
                  std::negation<std::is_convertible<Src&&, const Chain&>>,
                  std::negation<std::is_convertible<Src&&, const absl::Cord&>>,
                  std::negation<std::is_convertible<Src&&, ByteFill>>>,
              int> = 0>
void StringifiedSize(Src&&);
void StringifiedSize(bool) = delete;
void StringifiedSize(wchar_t) = delete;
void StringifiedSize(char16_t) = delete;
void StringifiedSize(char32_t) = delete;

// `IsStringifiable` checks if the type is an appropriate argument for
// `Writer::Write()`, `BackwardWriter::Write()`, `riegeli::Write()`, and e.g.
// `riegeli::WriteLine()`.
template <typename T, typename Enable = void>
struct IsStringifiable : std::false_type {};
template <typename T>
struct IsStringifiable<T, std::void_t<decltype(riegeli::StringifiedSize(
                              std::declval<const T&>()))>> : std::true_type {};

// `HasStringifiedSize` checks if the type has `riegeli::StringifiedSize()`
// defined returning the size.
template <typename T, typename Enable = void>
struct HasStringifiedSize : std::false_type {};
template <typename T>
struct HasStringifiedSize<
    T, std::enable_if_t<std::is_convertible_v<decltype(riegeli::StringifiedSize(
                                                  std::declval<const T&>())),
                                              Position>>> : std::true_type {};

}  // namespace riegeli

#endif  // RIEGELI_BYTES_STRINGIFY_H_
