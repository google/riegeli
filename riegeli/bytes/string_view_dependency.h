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
#include "riegeli/base/dependency.h"

namespace riegeli {

// Specializations of Dependency<string_view, Manager>.

template <>
class Dependency<absl::string_view, absl::string_view>
    : public DependencyBase<absl::string_view> {
 public:
  using DependencyBase<absl::string_view>::DependencyBase;

  absl::string_view get() const { return this->manager(); }

  static constexpr bool kIsStable() { return true; }
};

template <typename M>
class Dependency<
    absl::string_view, M*,
    std::enable_if_t<std::is_convertible<M, absl::string_view>::value>>
    : public DependencyBase<M*> {
 public:
  using DependencyBase<M*>::DependencyBase;

  absl::string_view get() const { return *this->manager(); }

  static constexpr bool kIsStable() { return true; }
};

template <typename M>
class Dependency<
    absl::string_view, M,
    std::enable_if_t<std::is_convertible<M, absl::string_view>::value>>
    : public DependencyBase<M> {
 public:
  using DependencyBase<M>::DependencyBase;

  absl::string_view get() const { return this->manager(); }

  static constexpr bool kIsStable() { return false; }
};

template <typename M, typename Deleter>
class Dependency<
    absl::string_view, std::unique_ptr<M, Deleter>,
    std::enable_if_t<std::is_convertible<M, absl::string_view>::value>>
    : public DependencyBase<std::unique_ptr<M, Deleter>> {
 public:
  using DependencyBase<std::unique_ptr<M, Deleter>>::DependencyBase;

  absl::string_view get() const { return *this->manager(); }

  static constexpr bool kIsStable() { return true; }
};

}  // namespace riegeli

#endif  // RIEGELI_BYTES_STRING_VIEW_DEPENDENCY_H_
