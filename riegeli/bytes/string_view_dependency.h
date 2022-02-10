// Copyright 2018 Google LLC
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

#ifndef RIEGELI_BYTES_STRING_VIEW_DEPENDENCY_H_
#define RIEGELI_BYTES_STRING_VIEW_DEPENDENCY_H_

#include <memory>
#include <type_traits>

#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/any_dependency.h"
#include "riegeli/base/dependency.h"

namespace riegeli {

// Specializations of `DependencyImpl<absl::string_view, Manager>`.

namespace string_view_internal {

inline absl::string_view ToStringView(absl::string_view value) { return value; }

// `absl::Span<const char>` is accepted with a template to avoid implicit
// conversions which can be ambiguous against `absl::string_view`
// (e.g. `std::string`).
template <typename T,
          std::enable_if_t<
              std::is_convertible<T, absl::Span<const char>>::value, int> = 0>
inline absl::string_view ToStringView(const T& value) {
  const absl::Span<const char> span = value;
  return absl::string_view(span.data(), span.size());
}

}  // namespace string_view_internal

// Specializations for `absl::string_view`, `absl::Span<const char>`,
// `absl::Span<char>`, `const char*`, and `char*` are defined separately for
// `kIsStable` to be `true`.

template <>
class DependencyImpl<absl::string_view, absl::string_view>
    : public DependencyBase<absl::string_view> {
 public:
  using DependencyBase<absl::string_view>::DependencyBase;

  absl::string_view get() const { return this->manager(); }

  static constexpr bool kIsStable = true;
};

template <>
class DependencyImpl<absl::string_view, absl::Span<const char>>
    : public DependencyBase<absl::Span<const char>> {
 public:
  using DependencyBase<absl::Span<const char>>::DependencyBase;

  absl::string_view get() const {
    return string_view_internal::ToStringView(this->manager());
  }

  static constexpr bool kIsStable = true;
};

template <>
class DependencyImpl<absl::string_view, absl::Span<char>>
    : public DependencyBase<absl::Span<char>> {
 public:
  using DependencyBase<absl::Span<char>>::DependencyBase;

  absl::string_view get() const {
    return string_view_internal::ToStringView(this->manager());
  }

  static constexpr bool kIsStable = true;
};

template <>
class DependencyImpl<absl::string_view, const char*>
    : public DependencyBase<const char*> {
 public:
  using DependencyBase<const char*>::DependencyBase;

  absl::string_view get() const {
    return string_view_internal::ToStringView(this->manager());
  }

  static constexpr bool kIsStable = true;
};

template <>
class DependencyImpl<absl::string_view, char*> : public DependencyBase<char*> {
 public:
  using DependencyBase<char*>::DependencyBase;

  absl::string_view get() const {
    return string_view_internal::ToStringView(this->manager());
  }

  static constexpr bool kIsStable = true;
};

template <typename M>
class DependencyImpl<
    absl::string_view, M*,
    std::enable_if_t<std::is_convertible<M, absl::string_view>::value ||
                     std::is_convertible<M, absl::Span<const char>>::value>>
    : public DependencyBase<M*> {
 public:
  using DependencyBase<M*>::DependencyBase;

  absl::string_view get() const {
    return string_view_internal::ToStringView(*this->manager());
  }

  static constexpr bool kIsStable = true;
};

template <typename M>
class DependencyImpl<
    absl::string_view, M,
    std::enable_if_t<std::is_convertible<M, absl::string_view>::value ||
                     std::is_convertible<M, absl::Span<const char>>::value>>
    : public DependencyBase<M> {
 public:
  using DependencyBase<M>::DependencyBase;

  absl::string_view get() const {
    return string_view_internal::ToStringView(this->manager());
  }

  static constexpr bool kIsStable = false;
};

template <typename M, typename Deleter>
class DependencyImpl<
    absl::string_view, std::unique_ptr<M, Deleter>,
    std::enable_if_t<std::is_convertible<M, absl::string_view>::value ||
                     std::is_convertible<M, absl::Span<const char>>::value>>
    : public DependencyBase<std::unique_ptr<M, Deleter>> {
 public:
  using DependencyBase<std::unique_ptr<M, Deleter>>::DependencyBase;

  absl::string_view get() const {
    return string_view_internal::ToStringView(*this->manager());
  }

  static constexpr bool kIsStable = true;
};

}  // namespace riegeli

#endif  // RIEGELI_BYTES_STRING_VIEW_DEPENDENCY_H_
