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

#ifndef RIEGELI_BASE_IN_PLACE_TEMPLATE_H_
#define RIEGELI_BASE_IN_PLACE_TEMPLATE_H_

// IWYU pragma: private, include "riegeli/base/any_dependency.h"

#include <stddef.h>

#include <utility>

#include "absl/utility/utility.h"

namespace riegeli {

#if __cpp_deduction_guides

// Tag type used for in-place construction when the type to construct needs to
// be specified.
//
// Like `absl::in_place_type_t`, but only the class template is specified, with
// the exact type being deduced using CTAD.
//
// Only templates with solely type template parameters are supported.

template <template <typename...> class T>
struct in_place_template_t {};

template <template <typename...> class T>
constexpr in_place_template_t<T> in_place_template = {};

#endif

// `TypeFromInPlaceTag<T, Args...>::type` and `TypeFromInPlaceTagT<T, Args...>`
// extract the type from `absl::in_place_type` or `in_place_template`, given
// types of arguments passed to the constructor.

template <typename T, typename... Args>
struct TypeFromInPlaceTag;

template <typename T, typename... Args>
struct TypeFromInPlaceTag<T&, Args...> : TypeFromInPlaceTag<T, Args...> {};

template <typename T, typename... Args>
struct TypeFromInPlaceTag<T&&, Args...> : TypeFromInPlaceTag<T, Args...> {};

template <typename T, typename... Args>
struct TypeFromInPlaceTag<const T, Args...> : TypeFromInPlaceTag<T, Args...> {};

template <typename T, typename... Args>
struct TypeFromInPlaceTag<absl::in_place_type_t<T>, Args...> {
  using type = T;
};

#if __cpp_deduction_guides
template <template <typename...> class Template, typename... Args>
struct TypeFromInPlaceTag<in_place_template_t<Template>, Args...> {
  using type = decltype(Template(std::declval<Args>()...));
};
#endif

template <typename T, typename... Args>
using TypeFromInPlaceTagT = typename TypeFromInPlaceTag<T, Args...>::type;

}  // namespace riegeli

#endif  // RIEGELI_BASE_IN_PLACE_TEMPLATE_H_
