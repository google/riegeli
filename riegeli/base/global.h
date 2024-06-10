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

#include <new>
#include <type_traits>
#include <utility>

#include "absl/meta/type_traits.h"

namespace riegeli {

namespace global_internal {

template <typename Init>
struct InitResult {
  using type = decltype(std::declval<Init>()());
};

template <typename Init>
using InitResultT = typename InitResult<Init>::type;

template <typename T, typename Init>
struct IsInitFor
    : absl::disjunction<
          absl::conjunction<std::is_void<global_internal::InitResultT<Init>>,
                            std::is_default_constructible<T>>,
#if __cpp_guaranteed_copy_elision
          std::is_same<std::remove_cv_t<T>,
                       std::remove_cv_t<global_internal::InitResultT<Init>>>,
#endif
          std::is_constructible<T, global_internal::InitResultT<Init>>> {
};

}  // namespace global_internal

// `Global<T>()` returns a reference to a default-constructed object of type `T`
// which must be const-qualified.
//
// All calls with the given `T` type return a reference to the same object.
//
// The object is created when `Global` is first called with the given `T` type,
// and is never destroyed.
template <
    typename T,
    std::enable_if_t<absl::conjunction<std::is_const<T>,
                                       std::is_default_constructible<T>>::value,
                     int> = 0>
T& Global();

// `Global(init)` returns a const reference to an object returned by `init()`.
//
// The object is created when `Global` is first called with the given `Init`
// type, and is never destroyed.
//
// The `Init` type should be a lambda with no captures. This restriction is a
// safeguard against making the object dependent on local state, which would be
// misleadingly ignored for subsequent calls. Since distinct lambdas have
// distinct types, distinct call sites with lambdas return references to
// distinct objects.
//
// The `deduced` template parameter lets `Global<T>()` with an explicit
// template argument unambiguously call another overload of `Global()`.
template <
    int deduced = 0, typename Init,
    std::enable_if_t<
        absl::conjunction<
            std::is_empty<Init>,
            global_internal::IsInitFor<
                std::decay_t<global_internal::InitResultT<Init>>, Init>>::value,
        int> = 0>
const std::decay_t<global_internal::InitResultT<Init>>& Global(Init init);

// `Global<T>(init)` returns a reference to an object of type `T` initialized by
// `init()`, which returns `void`, `T`, or a constructor argument for `T`.
//
// Returning `void` makes the object default-constructed. Returning `void` or a
// constructor argument allows to construct immovable objects before C++17 which
// guarantees copy elision when returning an rvalue.
//
// If `T` is not const-qualified, this is recommended only when the object is
// thread-safe, or when it will be accessed only in a thread-safe way despite
// the non-const type.
//
// The object is created when `Global` is first called with the given `T` and
// `Init` types, and is never destroyed.
//
// The `Init` type should be a lambda with no captures. This restriction is a
// safeguard against making the object dependent on local state, which would be
// misleadingly ignored for subsequent calls. Since distinct lambdas have
// distinct types, distinct call sites with lambdas return references to
// distinct objects.
template <typename T, typename Init,
          std::enable_if_t<
              absl::conjunction<
                  std::is_empty<Init>,
                  absl::disjunction<
                      absl::conjunction<
                          std::is_void<global_internal::InitResultT<Init>>,
                          std::is_default_constructible<T>>,
                      global_internal::IsInitFor<T, Init>>>::value,
              int> = 0>
T& Global(Init init);

// Implementation details follow.

namespace global_internal {

template <typename T>
class NoDestructor {
 public:
  NoDestructor() { new (storage_) T(); }

  template <
      typename Init,
      std::enable_if_t<std::is_void<global_internal::InitResultT<Init>>::value,
                       int> = 0>
  explicit NoDestructor(Init init) {
    init();
    new (storage_) T();
  }

  template <
      typename Init,
      std::enable_if_t<!std::is_void<global_internal::InitResultT<Init>>::value,
                       int> = 0>
  explicit NoDestructor(Init init) {
    new (storage_) T(init());
  }

  NoDestructor(const NoDestructor&) = delete;
  NoDestructor& operator=(const NoDestructor&) = delete;

  T& value() {
    return *
#if __cpp_lib_launder >= 201606
        std::launder
#endif
        (reinterpret_cast<T*>(storage_));
  }

 private:
  alignas(T) char storage_[sizeof(T)];
};

}  // namespace global_internal

template <
    typename T,
    std::enable_if_t<absl::conjunction<std::is_const<T>,
                                       std::is_default_constructible<T>>::value,
                     int>>
inline T& Global() {
  static global_internal::NoDestructor<T> kStorage;
  return kStorage.value();
}

template <
    int deduced, typename Init,
    std::enable_if_t<
        absl::conjunction<
            std::is_empty<Init>,
            global_internal::IsInitFor<
                std::decay_t<global_internal::InitResultT<Init>>, Init>>::value,
        int>>
inline const std::decay_t<global_internal::InitResultT<Init>>& Global(
    Init init) {
  static global_internal::NoDestructor<
      const std::decay_t<global_internal::InitResultT<Init>>>
      kStorage(init);
  return kStorage.value();
}

template <typename T, typename Init,
          std::enable_if_t<
              absl::conjunction<
                  std::is_empty<Init>,
                  absl::disjunction<
                      absl::conjunction<
                          std::is_void<global_internal::InitResultT<Init>>,
                          std::is_default_constructible<T>>,
                      global_internal::IsInitFor<T, Init>>>::value,
              int>>
inline T& Global(Init init) {
  static global_internal::NoDestructor<T> kStorage(init);
  return kStorage.value();
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_GLOBAL_H_
