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

#ifndef RIEGELI_BASE_TEMPORARY_STORAGE_H_
#define RIEGELI_BASE_TEMPORARY_STORAGE_H_

#include <stdint.h>

#include <new>  // IWYU pragma: keep
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "riegeli/base/assert.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

// Internal storage used by functions which return a reference to either an
// existing object or a newly constructed one.
//
// Such functions take a parameter
//   `TemporaryStorage<T>&& storage ABSL_ATTRIBUTE_LIFETIME_BOUND = {}`
// so that the default value is allocated as a temporary by the caller.
//
// The parameter can also be passed explicitly if a call to these functions
// happens in a context which needs the returned reference to be valid longer
// than the full expression containing the call. This passes the responsibility
// for passing a `TemporaryStorage<T>` with a suitable lifetime to the caller of
// that context.
template <typename T, typename Enable = void>
class TemporaryStorage {
 public:
  TemporaryStorage() noexcept {}

  TemporaryStorage(const TemporaryStorage&) = delete;
  TemporaryStorage& operator=(const TemporaryStorage&) = delete;

  ~TemporaryStorage() {
    if (state_ != State::kUninitialized) value_.~T();
  }

  template <typename... Args,
            std::enable_if_t<std::is_constructible_v<T, Args&&...>, int> = 0>
      T& emplace(Args&&... args) & ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT_EQ(state_, State::kUninitialized)
        << "Failed precondition of TemporaryStorage::emplace()";
    new (&value_) T(std::forward<Args>(args)...);
    state_ = State::kValid;
    return value_;
  }
  template <typename... Args,
            std::enable_if_t<std::is_constructible_v<T, Args&&...>, int> = 0>
      T&& emplace(Args&&... args) && ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT_EQ(state_, State::kUninitialized)
        << "Failed precondition of TemporaryStorage::emplace()";
    new (&value_) T(std::forward<Args>(args)...);
    state_ = State::kMovedFrom;
    return std::move(value_);
  }

  T& operator*() & ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT_EQ(state_, State::kValid)
        << "Failed precondition of TemporaryStorage::operator*";
    return value_;
  }
  const T& operator*() const& ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT_EQ(state_, State::kValid)
        << "Failed precondition of TemporaryStorage::operator*";
    return value_;
  }
  T&& operator*() && ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT_EQ(state_, State::kValid)
        << "Failed precondition of TemporaryStorage::operator*";
    state_ = State::kMovedFrom;
    return std::move(value_);
  }
  const T&& operator*() const&& ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT_EQ(state_, State::kValid)
        << "Failed precondition of TemporaryStorage::operator*";
    state_ = State::kMovedFrom;
    return std::move(value_);
  }

 private:
  enum class State : uint8_t {
    kUninitialized = 0,
    kValid = 1,
    kMovedFrom = 2,
  };

  union {
    std::remove_cv_t<T> value_;
  };
  mutable State state_ = State::kUninitialized;
};

// Specialization of `TemporaryStorage<T>` for non-reference trivially
// destructible but not trivially default constructible types. There is no need
// to track whether the object was initialized.
template <typename T>
class TemporaryStorage<
    T,
    std::enable_if_t<std::conjunction_v<
        std::negation<std::is_reference<T>>, std::is_trivially_destructible<T>,
        std::negation<std::is_trivially_default_constructible<T>>>>> {
 public:
  TemporaryStorage() noexcept {}

  TemporaryStorage(const TemporaryStorage&) = delete;
  TemporaryStorage& operator=(const TemporaryStorage&) = delete;

  template <typename... Args,
            std::enable_if_t<std::is_constructible_v<T, Args&&...>, int> = 0>
      T& emplace(Args&&... args) & ABSL_ATTRIBUTE_LIFETIME_BOUND {
    new (&value_) T(std::forward<Args>(args)...);
    return value_;
  }
  template <typename... Args,
            std::enable_if_t<std::is_constructible_v<T, Args&&...>, int> = 0>
      T&& emplace(Args&&... args) && ABSL_ATTRIBUTE_LIFETIME_BOUND {
    new (&value_) T(std::forward<Args>(args)...);
    return std::move(value_);
  }

  T& operator*() & ABSL_ATTRIBUTE_LIFETIME_BOUND { return value_; }
  const T& operator*() const& ABSL_ATTRIBUTE_LIFETIME_BOUND { return value_; }
  T&& operator*() && ABSL_ATTRIBUTE_LIFETIME_BOUND { return std::move(value_); }
  const T&& operator*() const&& ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return std::move(value_);
  }

 private:
  union {
    std::remove_cv_t<T> value_;
  };
};

// Specialization of `TemporaryStorage<T>` for non-reference trivially
// destructible and trivially default constructible types. There is no need to
// track whether the object was initialized, and
// `ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS` can be applied.
template <typename T>
class TemporaryStorage<
    T,
    std::enable_if_t<std::conjunction_v<
        std::negation<std::is_reference<T>>, std::is_trivially_destructible<T>,
        std::is_trivially_default_constructible<T>>>> {
 public:
  TemporaryStorage() = default;

  TemporaryStorage(const TemporaryStorage&) = delete;
  TemporaryStorage& operator=(const TemporaryStorage&) = delete;

  template <typename... Args,
            std::enable_if_t<std::is_constructible_v<T, Args&&...>, int> = 0>
      T& emplace(Args&&... args) & ABSL_ATTRIBUTE_LIFETIME_BOUND {
    new (&value_) T(std::forward<Args>(args)...);
    return value_;
  }
  template <typename... Args,
            std::enable_if_t<std::is_constructible_v<T, Args&&...>, int> = 0>
      T&& emplace(Args&&... args) && ABSL_ATTRIBUTE_LIFETIME_BOUND {
    new (&value_) T(std::forward<Args>(args)...);
    return std::move(value_);
  }

  T& operator*() & ABSL_ATTRIBUTE_LIFETIME_BOUND { return value_; }
  const T& operator*() const& ABSL_ATTRIBUTE_LIFETIME_BOUND { return value_; }
  T&& operator*() && ABSL_ATTRIBUTE_LIFETIME_BOUND { return std::move(value_); }
  const T&& operator*() const&& ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return std::move(value_);
  }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS std::remove_cv_t<T> value_;
};

// Specialization of `TemporaryStorage<T>` for reference types.
template <typename T>
class TemporaryStorage<T, std::enable_if_t<std::is_reference_v<T>>> {
 public:
  TemporaryStorage() = default;

  TemporaryStorage(const TemporaryStorage&) = delete;
  TemporaryStorage& operator=(const TemporaryStorage&) = delete;

  template <typename Arg,
            std::enable_if_t<std::is_convertible_v<Arg&&, T>, int> = 0>
      T& emplace(Arg&& arg) & ABSL_ATTRIBUTE_LIFETIME_BOUND {
    value_ = &arg;
    return *value_;
  }
  template <typename Arg,
            std::enable_if_t<std::is_convertible_v<Arg&&, T>, int> = 0>
      T&& emplace(Arg&& arg) && ABSL_ATTRIBUTE_LIFETIME_BOUND {
    value_ = &arg;
    return std::forward<T>(*value_);
  }

  T& operator*() & ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT(value_ != nullptr)
        << "Failed precondition of TemporaryStorage::operator*";
    return *value_;
  }
  const T& operator*() const& ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT(value_ != nullptr)
        << "Failed precondition of TemporaryStorage::operator*";
    return *value_;
  }
  T&& operator*() && ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT(value_ != nullptr)
        << "Failed precondition of TemporaryStorage::operator*";
    return std::forward<T>(*value_);
  }
  const T&& operator*() const&& ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT(value_ != nullptr)
        << "Failed precondition of TemporaryStorage::operator*";
    return std::forward<T>(*value_);
  }

 private:
  std::remove_reference_t<T>* absl_nullable value_;
};

}  // namespace riegeli

#endif  // RIEGELI_BASE_TEMPORARY_STORAGE_H_
