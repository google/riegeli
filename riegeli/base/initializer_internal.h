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

#ifndef RIEGELI_BASE_INITIALIZER_INTERNAL_H_
#define RIEGELI_BASE_INITIALIZER_INTERNAL_H_

#include <new>
#include <type_traits>
#include <utility>

#include "riegeli/base/assert.h"

namespace riegeli {
namespace initializer_internal {

// Internal storage which is conditionally needed for storing the object that
// `MakerType<Args...>::Reference<T>()`,
// `MakerTypeFor<T, Args...>::Reference()`, and `Initializer<T>::Reference()`
// refer to.
//
// The public name of this type is `MakerType<Args...>::ReferenceStorage<T>`,
// `MakerTypeFor<T, Args...>::ReferenceStorage`, and
// `Initializer<T>::RefereneStorage`.
//
// `ReferenceStorage<T>()` is passed as the default value of a parameter of
// these functions with type `ReferenceStorage<T>&&`, so that
// it is allocated as a temporary by the caller.
//
// It can also be passed explicitly if a call to these functions happens in a
// context which needs the returned reference to be valid longer than the full
// expression containing the call. This passes the responsibility for passing a
// `ReferenceStorage<T>` with a suitable lifetime to the caller of that context.
template <typename T, typename Enable = void>
class ReferenceStorage {
 public:
  ReferenceStorage() noexcept {}

  ReferenceStorage(const ReferenceStorage&) = delete;
  ReferenceStorage& operator=(const ReferenceStorage&) = delete;

  ~ReferenceStorage() {
    if (initialized_) value_.~T();
  }

  template <typename... Args>
  void emplace(Args&&... args) {
    RIEGELI_ASSERT(!initialized_)
        << "Failed precondition of ReferenceStorage::emplace(): "
           "already initialized";
    new (&value_) T(std::forward<Args>(args)...);
    initialized_ = true;
  }

  T&& operator*() && {
    RIEGELI_ASSERT(initialized_)
        << "Failed precondition of ReferenceStorage::operator*: "
           "not initialized";
    return std::move(value_);
  }

 private:
  union {
    std::remove_cv_t<T> value_;
  };
  bool initialized_ = false;
};

// Specialization of `ReferenceStorage<T>` for trivially destructible types.
// There is no need to track whether the object was initialized.
template <typename T>
class ReferenceStorage<
    T, std::enable_if_t<std::is_trivially_destructible<T>::value>> {
 public:
  ReferenceStorage() noexcept {}

  ReferenceStorage(const ReferenceStorage&) = delete;
  ReferenceStorage& operator=(const ReferenceStorage&) = delete;

  template <typename... Args>
  void emplace(Args&&... args) {
    new (&value_) T(std::forward<Args>(args)...);
  }

  T&& operator*() && { return std::move(value_); }

 private:
  union {
    std::remove_cv_t<T> value_;
  };
};

// `CanBindTo<T&&, Args&&...>::value` is `true` if constructing `T(args...)`
// with `args...` of type `Args&&...` can be elided, with `T&&` binding directly
// to the only element of `args...` instead.

template <typename T, typename... Args>
struct CanBindTo : std::false_type {};

template <typename T, typename Arg>
struct CanBindTo<T&, Arg&> : std::is_convertible<Arg*, T*> {};

template <typename T, typename Arg>
struct CanBindTo<T&, Arg&&> : std::false_type {};

template <typename T, typename Arg>
struct CanBindTo<const T&, Arg&&> : std::is_convertible<Arg*, const T*> {};

template <typename T, typename Arg>
struct CanBindTo<T&&, Arg&> : std::false_type {};

template <typename T, typename Arg>
struct CanBindTo<T&&, Arg&&> : std::is_convertible<Arg*, T*> {};

}  // namespace initializer_internal
}  // namespace riegeli

#endif  // RIEGELI_BASE_INITIALIZER_INTERNAL_H_
