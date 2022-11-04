// Copyright 2017 Google LLC
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

#ifndef RIEGELI_BASE_TYPE_TRAITS_H_
#define RIEGELI_BASE_TYPE_TRAITS_H_

#include <type_traits>
#include <utility>

namespace riegeli {

// `type_identity<T>::type` and `type_identity_t<T>` are `T`, but do not deduce
// the `T` in templates.

template <typename T>
struct type_identity {
  using type = T;
};

template <typename T>
using type_identity_t = typename type_identity<T>::type;

#if __cpp_deduction_guides

// `DeduceClassTemplateArguments<Template, Args...>::type` and
// `DeduceClassTemplateArgumentsT<Template, Args...>` deduce class template
// arguments using CTAD from constructor arguments.
//
// Only templates with solely type template parameters are supported.

template <template <typename...> class Template, typename... Args>
struct DeduceClassTemplateArguments {
  using type = decltype(Template(std::declval<Args>()...));
};

template <template <typename...> class Template, typename... Args>
using DeduceClassTemplateArgumentsT =
    typename DeduceClassTemplateArguments<Template, Args...>::type;

#endif

// `IntersectionType<Ts...>::type` and `IntersectionTypeT<Ts...>` compute the
// smallest of unsigned integer types.

namespace type_traits_internal {

template <typename A, typename B, typename Common>
struct IntersectionTypeImpl;

template <typename A, typename B>
struct IntersectionTypeImpl<A, B, A> {
  using type = B;
};

template <typename A, typename B>
struct IntersectionTypeImpl<A, B, B> {
  using type = A;
};

template <typename A>
struct IntersectionTypeImpl<A, A, A> {
  using type = A;
};

}  // namespace type_traits_internal

template <typename... T>
struct IntersectionType;

template <typename... T>
using IntersectionTypeT = typename IntersectionType<T...>::type;

template <typename A>
struct IntersectionType<A> {
  using type = A;
};

template <typename A, typename B>
struct IntersectionType<A, B>
    : type_traits_internal::IntersectionTypeImpl<A, B,
                                                 std::common_type_t<A, B>> {};

template <typename A, typename B, typename... Rest>
struct IntersectionType<A, B, Rest...>
    : IntersectionType<IntersectionTypeT<A, B>, Rest...> {};

}  // namespace riegeli

#endif  // RIEGELI_BASE_TYPE_TRAITS_H_
