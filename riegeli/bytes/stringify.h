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
#include "riegeli/bytes/write_int_internal.h"

namespace riegeli {

namespace stringify_internal {

template <typename Src, typename Enable = void>
struct HasRiegeliStringifiedSize : std::false_type {};

template <typename Src>
struct HasRiegeliStringifiedSize<
    Src, std::enable_if_t<std::is_convertible_v<
             decltype(RiegeliStringifiedSize(std::declval<const Src&>())),
             Position>>> : std::true_type {};

}  // namespace stringify_internal

// `riegeli::StringifiedSize()` of a stringifiable value returns the size of its
// stringification as `Position` if easily known, otherwise it is only declared
// as returning `void`.
//
// It has the same overloads as `Writer::Write()`, assuming that the parameter
// is passed by const reference.
//
// To customize `riegeli::StringifiedSize()` for a class `Src` supporting
// `AbslStringify()`, define a free function
// `friend Position RiegeliStringifiedSize(const Src& src)` as a friend of `Src`
// inside class definition or in the same namespace as `Src`, so that it can be
// found via ADL. A function returning `void` is treated as absent.
//
// There is no need to define `RiegeliStringifiedSize()` for types convertible
// to `BytesRef`, even if they support `AbslStringify()`.
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
inline Position StringifiedSize(signed char src) {
  return write_int_internal::StringifiedSizeSigned(src);
}
inline Position StringifiedSize(unsigned char src) {
  return write_int_internal::StringifiedSizeUnsigned(src);
}
inline Position StringifiedSize(short src) {
  return write_int_internal::StringifiedSizeSigned(src);
}
inline Position StringifiedSize(unsigned short src) {
  return write_int_internal::StringifiedSizeUnsigned(src);
}
inline Position StringifiedSize(int src) {
  return write_int_internal::StringifiedSizeSigned(src);
}
inline Position StringifiedSize(unsigned src) {
  return write_int_internal::StringifiedSizeUnsigned(src);
}
inline Position StringifiedSize(long src) {
  return write_int_internal::StringifiedSizeSigned(src);
}
inline Position StringifiedSize(unsigned long src) {
  return write_int_internal::StringifiedSizeUnsigned(src);
}
inline Position StringifiedSize(long long src) {
  return write_int_internal::StringifiedSizeSigned(src);
}
inline Position StringifiedSize(unsigned long long src) {
  return write_int_internal::StringifiedSizeUnsigned(src);
}
inline Position StringifiedSize(absl::int128 src) {
  return write_int_internal::StringifiedSizeSigned(src);
}
inline Position StringifiedSize(absl::uint128 src) {
  return write_int_internal::StringifiedSizeUnsigned(src);
}
inline void StringifiedSize(float);
inline void StringifiedSize(double);
inline void StringifiedSize(long double);
template <
    typename Src,
    std::enable_if_t<
        std::conjunction_v<
            absl::HasAbslStringify<Src>,
            std::negation<std::is_convertible<const Src&, BytesRef>>,
            std::negation<std::is_convertible<const Src&, const Chain&>>,
            std::negation<std::is_convertible<const Src&, const absl::Cord&>>,
            std::negation<std::is_convertible<const Src&, ByteFill>>>,
        int> = 0>
inline auto StringifiedSize(const Src& src) {
  if constexpr (stringify_internal::HasRiegeliStringifiedSize<Src>::value) {
    return RiegeliStringifiedSize(src);
  }
}
void StringifiedSize(bool) = delete;
void StringifiedSize(wchar_t) = delete;
void StringifiedSize(char16_t) = delete;
void StringifiedSize(char32_t) = delete;

namespace stringify_internal {

template <typename T, typename Enable = void>
struct IsStringifiableImpl : std::false_type {};

template <typename T>
struct IsStringifiableImpl<T, std::void_t<decltype(riegeli::StringifiedSize(
                                  std::declval<const T&>()))>>
    : std::true_type {};

template <typename T, typename Enable = void>
struct HasStringifiedSizeImpl : std::false_type {};

template <typename T>
struct HasStringifiedSizeImpl<
    T, std::enable_if_t<std::is_convertible_v<decltype(riegeli::StringifiedSize(
                                                  std::declval<const T&>())),
                                              Position>>> : std::true_type {};

}  // namespace stringify_internal

// `IsStringifiable` checks if the type is an appropriate argument for
// `Writer::Write()`, `BackwardWriter::Write()`, `riegeli::Write()`, and e.g.
// `riegeli::WriteLine()`.
template <typename... T>
struct IsStringifiable
    : std::conjunction<stringify_internal::IsStringifiableImpl<T>...> {};

// `HasStringifiedSize` checks if the type has `riegeli::StringifiedSize()`
// defined returning the size.
template <typename... T>
struct HasStringifiedSize
    : std::conjunction<stringify_internal::HasStringifiedSizeImpl<T>...> {};

// `riegeli::StringifiedSize()` of multiple stringifiable values returns the
// total size of their stringifications, interpreted as for
// `riegeli::StringifiedSize()` with a single parameter.
template <typename... Srcs,
          std::enable_if_t<
              std::conjunction_v<std::bool_constant<sizeof...(Srcs) != 1>,
                                 IsStringifiable<Srcs...>>,
              int> = 0>
inline auto StringifiedSize(const Srcs&... srcs) {
  if constexpr (HasStringifiedSize<Srcs...>::value) {
    return (Position{0} + ... + riegeli::StringifiedSize(srcs));
  }
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_STRINGIFY_H_
