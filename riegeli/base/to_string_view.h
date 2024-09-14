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

#ifndef RIEGELI_BASE_TO_STRING_VIEW_H_
#define RIEGELI_BASE_TO_STRING_VIEW_H_

#include <type_traits>

#include "absl/base/attributes.h"
#include "absl/meta/type_traits.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"

namespace riegeli {

namespace to_string_view_internal {

template <typename T, typename Enable = void>
struct HasRiegeliToStringView : std::false_type {};

template <typename T>
struct HasRiegeliToStringView<
    T, std::enable_if_t<std::is_convertible<decltype(RiegeliToStringView(
                                                std::declval<const T*>())),
                                            absl::string_view>::value>>
    : std::true_type {};

}  // namespace to_string_view_internal

template <typename T>
struct SupportsToStringView
    : absl::disjunction<
          to_string_view_internal::HasRiegeliToStringView<T>,
          std::is_constructible<absl::string_view, const T&>,
          std::is_constructible<absl::Span<const char>, const T&>> {};

template <
    typename T,
    std::enable_if_t<to_string_view_internal::HasRiegeliToStringView<T>::value,
                     int> = 0>
inline absl::string_view ToStringView(
    const T& value ABSL_ATTRIBUTE_LIFETIME_BOUND) {
  return RiegeliToStringView(&value);
}

template <
    typename T,
    std::enable_if_t<
        absl::conjunction<
            absl::negation<to_string_view_internal::HasRiegeliToStringView<T>>,
            std::is_constructible<absl::string_view, const T&>>::value,
        int> = 0>
inline absl::string_view ToStringView(
    const T& value ABSL_ATTRIBUTE_LIFETIME_BOUND) {
  return absl::string_view(value);
}

template <
    typename T,
    std::enable_if_t<
        absl::conjunction<
            absl::negation<to_string_view_internal::HasRiegeliToStringView<T>>,
            absl::negation<std::is_constructible<absl::string_view, const T&>>,
            std::is_constructible<absl::Span<const char>, const T&>>::value,
        int> = 0>
inline absl::string_view ToStringView(
    const T& value ABSL_ATTRIBUTE_LIFETIME_BOUND) {
  const absl::Span<const char> span(value);
  return absl::string_view(span.data(), span.size());
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_TO_STRING_VIEW_H_
