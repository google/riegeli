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

#ifndef RIEGELI_BYTES_SPAN_DEPENDENCY_H_
#define RIEGELI_BYTES_SPAN_DEPENDENCY_H_

#include <memory>
#include <type_traits>

#include "absl/types/span.h"
#include "riegeli/base/dependency.h"

namespace riegeli {

// Specializations of `DependencyImpl<absl::Span<char>, Manager>`.

// Specialization for `absl::Span<char>` itself is defined separately for
// `kIsStable` to be `true`.
template <>
class DependencyImpl<absl::Span<char>, absl::Span<char>>
    : public DependencyBase<absl::Span<char>> {
 public:
  using DependencyBase<absl::Span<char>>::DependencyBase;

  absl::Span<char> get() const { return this->manager(); }

  static constexpr bool kIsStable = true;
};

template <typename M>
class DependencyImpl<
    absl::Span<char>, M*,
    std::enable_if_t<std::is_constructible<absl::Span<char>, M&>::value &&
                     !std::is_pointer<M>::value>> : public DependencyBase<M*> {
 public:
  using DependencyBase<M*>::DependencyBase;

  absl::Span<char> get() const { return absl::Span<char>(*this->manager()); }

  static constexpr bool kIsStable = true;
};

template <typename M>
class DependencyImpl<
    absl::Span<char>, M,
    std::enable_if_t<std::is_constructible<absl::Span<char>, M&>::value &&
                     !std::is_pointer<M>::value>> : public DependencyBase<M> {
 public:
  using DependencyBase<M>::DependencyBase;

  absl::Span<char> get() { return absl::Span<char>(this->manager()); }
  absl::Span<const char> get() const {
    return absl::Span<const char>(this->manager());
  }

  static constexpr bool kIsStable = false;
};

template <typename M, typename Deleter>
class DependencyImpl<
    absl::Span<char>, std::unique_ptr<M, Deleter>,
    std::enable_if_t<std::is_constructible<absl::Span<char>, M&>::value &&
                     !std::is_pointer<M>::value>>
    : public DependencyBase<std::unique_ptr<M, Deleter>> {
 public:
  using DependencyBase<std::unique_ptr<M, Deleter>>::DependencyBase;

  absl::Span<char> get() const { return absl::Span<char>(*this->manager()); }

  static constexpr bool kIsStable = true;
};

}  // namespace riegeli

#endif  // RIEGELI_BYTES_SPAN_DEPENDENCY_H_
