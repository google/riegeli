// Copyright 2020 Google LLC
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

#ifndef RIEGELI_BASE_FUNCTION_DEPENDENCY_H_
#define RIEGELI_BASE_FUNCTION_DEPENDENCY_H_

#include <tuple>
#include <type_traits>
#include <utility>

#include "riegeli/base/dependency.h"

namespace riegeli {

// `FunctionDependency<Ptr, ManagerRef>` returns the appropriate
// `Dependency<Ptr, Manager>` to be constructed locally in a function, where
// `ManagerRef` is either specified explicitly or deduced from a function
// argument.
//
// Such a function can be called with a reference instead of the corresponding
// pointer as the `ManagerRef`. Taking an argument by reference instead of by
// pointer is appropriate when the object does not have to be valid after the
// function returns, which is the case when the dependency is constructed
// locally in the function. Hence `FunctionDependency<P, M&>` returns
// `Dependency<P, M*>` if possible, i.e. if `M*` is convertible to `P`.
//
// Such a function can also be called with a reference when decaying to its
// value is intended, but `ManagerRef` is deduced from a function argument as a
// reference type. Hence `FunctionDependency<P, M&>` returns `Dependency<P, M>`
// otherwise, i.e. if `M*` is not convertible to `P`.

namespace internal {

// By default `FunctionDependency<P, M>` returns `Dependency<P, M>`.
template <typename P, typename M, typename Enable = void>
struct FunctionDependencyResult {
  using type = Dependency<P, M>;

  static Dependency<P, M> Construct(const M& manager) {
    return Dependency<P, M>(manager);
  }
  static Dependency<P, M> Construct(M&& manager) {
    return Dependency<P, M>(std::move(manager));
  }
};

// `FunctionDependency<P, M&>` returns `Dependency<P, M*>` if possible.
template <typename P, typename M>
struct FunctionDependencyResult<
    P, M&, std::enable_if_t<std::is_convertible<M*, P>::value>> {
  using type = Dependency<P, M*>;

  static Dependency<P, M*> Construct(M& manager) {
    return Dependency<P, M*>(&manager);
  }
};

// `FunctionDependency<P, M&>` returns `Dependency<P, M>` otherwise.
template <typename P, typename M>
struct FunctionDependencyResult<
    P, M&, std::enable_if_t<!std::is_convertible<M*, P>::value>> {
  using type = Dependency<P, M>;

  static Dependency<P, M> Construct(M& manager) {
    return Dependency<P, M>(manager);
  }
};

}  // namespace internal

template <typename Ptr, typename Manager>
inline typename internal::FunctionDependencyResult<Ptr, Manager>::type
FunctionDependency(const Manager& manager) {
  return internal::FunctionDependencyResult<Ptr, Manager>::Construct(manager);
}

template <typename Ptr, typename Manager>
inline typename internal::FunctionDependencyResult<Ptr, Manager>::type
FunctionDependency(Manager&& manager) {
  return internal::FunctionDependencyResult<Ptr, Manager>::Construct(
      std::forward<Manager>(manager));
}

template <typename Ptr, typename Manager, typename... ManagerArgs>
inline Dependency<Ptr, Manager> FunctionDependency(
    std::tuple<ManagerArgs...> manager_args) {
  return Dependency<Ptr, Manager>(std::move(manager_args));
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_FUNCTION_DEPENDENCY_H_
