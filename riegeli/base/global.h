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

#ifndef RIEGELI_BASE_GLOBAL_H_
#define RIEGELI_BASE_GLOBAL_H_

#include <functional>
#include <new>
#include <type_traits>

namespace riegeli {

// `Global<T>()` returns a const reference to a default-constructed object of
// type `T`.
//
// All calls with the given `T` type return a reference to the same object.
//
// The object is created when `Global` is first called with the given `T` type,
// and is never destroyed.
template <typename T,
          std::enable_if_t<std::is_default_constructible_v<T>, int> = 0>
const T& Global();

// `Global(construct)` returns a reference to an object returned by `construct`.
//
// The object is created when `Global` is first called with the given
// `construct` type, and is never destroyed.
//
// If `T` is not const-qualified, this is recommended only when the object is
// thread-safe, or when it will be accessed only in a thread-safe way despite
// its non-const type.
//
// The `construct` type should be a lambda with no captures. This restriction is
// a safeguard against making the object dependent on local state, which would
// be misleadingly ignored for subsequent calls. Since distinct lambdas have
// distinct types, distinct call sites with lambdas return references to
// distinct objects.
template <typename Construct,
          std::enable_if_t<std::conjunction_v<std::is_empty<Construct>,
                                              std::is_invocable<Construct>>,
                           int> = 0>
std::decay_t<std::invoke_result_t<Construct>>& Global(Construct construct);

// `Global(construct, initialize)` returns a reference to an object returned by
// `construct`. After construction, `initialize` is called on the reference.
//
// The object is created when `Global` is first called with the given
// `construct` and `initialize` types, and is never destroyed.
//
// If `T` is not const-qualified, this is recommended only when the object is
// thread-safe, or when it will be accessed only in a thread-safe way despite
// its non-const type.
//
// The `construct` and `initialize` types should be lambdas with no captures.
// This restriction is a safeguard against making the object dependent on local
// state, which would be misleadingly ignored for subsequent calls. Since
// distinct lambdas have distinct types, distinct call sites with lambdas return
// references to distinct objects.
template <
    typename Construct, typename Initialize,
    std::enable_if_t<
        std::conjunction_v<
            std::is_empty<Construct>, std::is_empty<Initialize>,
            std::is_invocable<Initialize,
                              std::decay_t<std::invoke_result_t<Construct>>&>>,
        int> = 0>
std::decay_t<std::invoke_result_t<Construct>>& Global(Construct construct,
                                                      Initialize initialize);

// Implementation details follow.

namespace global_internal {

template <typename T>
class NoDestructor {
 public:
  NoDestructor() { new (storage_) T(); }

  template <typename Construct>
  explicit NoDestructor(Construct construct) {
    new (storage_) T(std::invoke(construct));
  }

  template <typename Construct, typename Initialize>
  explicit NoDestructor(Construct construct, Initialize initialize) {
    new (storage_) T(std::invoke(construct));
    std::invoke(initialize, object());
  }

  NoDestructor(const NoDestructor&) = delete;
  NoDestructor& operator=(const NoDestructor&) = delete;

  T& object() { return *std::launder(reinterpret_cast<T*>(storage_)); }

 private:
  alignas(T) char storage_[sizeof(T)];
};

}  // namespace global_internal

template <typename T, std::enable_if_t<std::is_default_constructible_v<T>, int>>
inline const T& Global() {
  static global_internal::NoDestructor<const T> kStorage;
  return kStorage.object();
}

template <typename Construct,
          std::enable_if_t<std::conjunction_v<std::is_empty<Construct>,
                                              std::is_invocable<Construct>>,
                           int>>
inline std::decay_t<std::invoke_result_t<Construct>>& Global(
    Construct construct) {
  static global_internal::NoDestructor<
      std::decay_t<std::invoke_result_t<Construct>>>
      kStorage(construct);
  return kStorage.object();
}

template <
    typename Construct, typename Initialize,
    std::enable_if_t<
        std::conjunction_v<
            std::is_empty<Construct>, std::is_empty<Initialize>,
            std::is_invocable<Initialize,
                              std::decay_t<std::invoke_result_t<Construct>>&>>,
        int>>
inline std::decay_t<std::invoke_result_t<Construct>>& Global(
    Construct construct, Initialize initialize) {
  static global_internal::NoDestructor<
      std::decay_t<std::invoke_result_t<Construct>>>
      kStorage(construct, initialize);
  return kStorage.object();
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_GLOBAL_H_
