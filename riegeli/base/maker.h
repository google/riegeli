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

#ifndef RIEGELI_BASE_MAKER_H_
#define RIEGELI_BASE_MAKER_H_

#include <stddef.h>

#include <new>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/casts.h"
#include "absl/meta/type_traits.h"
#include "absl/utility/utility.h"
#include "riegeli/base/initializer_internal.h"
#include "riegeli/base/reset.h"
#include "riegeli/base/temporary_storage.h"
#include "riegeli/base/type_traits.h"

namespace riegeli {

// `MakerType<Args...>`, usually made with `riegeli::Maker(args...)`, packs
// constructor arguments for a yet unspecified type, which will be specified by
// the caller. `MakerType<Args...>` is convertible to `Initializer<T>` for any
// `T` which can be constructed from `Args...`.
//
// This allows the function taking `Initializer<T>` to construct the object
// in-place, avoiding constructing a temporary and moving from it.
//
// In contrast to `MakerTypeFor<T, Args...>`, `MakerType<Args...>` requires the
// caller to know `T`.
//
// `InvokerType` complements `MakerType` by extending constructors with factory
// functions.
template <typename... Args>
class MakerType : public ConditionallyAssignable<absl::conjunction<
                      absl::negation<std::is_reference<Args>>...>::value> {
 public:
  // Constructs `MakerType` from `args...` convertible to `Args...`.
  template <
      typename... SrcArgs,
      std::enable_if_t<
          absl::conjunction<std::is_convertible<SrcArgs&&, Args>...>::value,
          int> = 0>
  /*implicit*/ MakerType(SrcArgs&&... args)
      : args_(std::forward<SrcArgs>(args)...) {}

  MakerType(MakerType&& that) = default;
  MakerType& operator=(MakerType&& that) = default;

  MakerType(const MakerType& that) = default;
  MakerType& operator=(const MakerType& that) = default;

  // Constructs the `T`.
  template <
      typename T,
      std::enable_if_t<std::is_constructible<T, Args&&...>::value, int> = 0>
  T Construct() && {
    return absl::make_from_tuple<T>(std::move(args_));
  }
  template <typename T,
            std::enable_if_t<std::is_constructible<T, const Args&...>::value,
                             int> = 0>
  T Construct() const& {
    return absl::make_from_tuple<T>(args_);
  }

  // Constructs the `T` at `ptr` using placement `new`.
  template <
      typename T,
      std::enable_if_t<std::is_constructible<T, Args&&...>::value, int> = 0>
  void ConstructAt(void* ptr) && {
    std::move(*this).template ConstructAtImpl<T>(
        ptr, std::index_sequence_for<Args...>());
  }
  template <typename T,
            std::enable_if_t<std::is_constructible<T, const Args&...>::value,
                             int> = 0>
  void ConstructAt(void* ptr) const& {
    ConstructAtImpl<T>(ptr, std::index_sequence_for<Args...>());
  }

  // Constructs the `T`, or returns a reference to an already constructed object
  // if that was passed to the `MakerType`.
  //
  // `Reference()` instead of `Construct()` can avoid moving the object if the
  // caller does not need to store the object, or if it will be moved later
  // because the target location for the object is not ready yet.
  //
  // `storage` must outlive usages of the returned reference.
  template <
      typename T,
      std::enable_if_t<std::is_constructible<T, Args&&...>::value, int> = 0>
  T&& Reference(TemporaryStorage<T>&& storage ABSL_ATTRIBUTE_LIFETIME_BOUND =
                    TemporaryStorage<T>()) && {
    return absl::apply(
        [&](Args&&... args) -> T&& {
          return std::move(storage).emplace(std::forward<Args>(args)...);
        },
        std::move(args_));
  }
  template <typename T,
            std::enable_if_t<std::is_constructible<T, const Args&...>::value,
                             int> = 0>
  T&& Reference(TemporaryStorage<T>&& storage ABSL_ATTRIBUTE_LIFETIME_BOUND =
                    TemporaryStorage<T>()) const& {
    return absl::apply(
        [&](const Args&... args) -> T&& {
          return std::move(storage).emplace(args...);
        },
        args_);
  }

  // Constructs the `T`, or returns a const reference to an already constructed
  // object if that was passed to the `MakerType`.
  //
  // `ConstReference()` can avoid moving the object in more cases than
  // `Reference()` if the caller does not need to store the object.
  //
  // `storage` must outlive usages of the returned reference.
  template <
      typename T,
      std::enable_if_t<std::is_constructible<T, Args&&...>::value, int> = 0>
  const T& ConstReference(TemporaryStorage<T>&& storage
                              ABSL_ATTRIBUTE_LIFETIME_BOUND =
                                  TemporaryStorage<T>()) && {
    return absl::apply(
        [&](Args&&... args) -> const T& {
          return storage.emplace(std::forward<Args>(args)...);
        },
        std::move(args_));
  }
  template <typename T,
            std::enable_if_t<std::is_constructible<T, const Args&...>::value,
                             int> = 0>
  const T& ConstReference(TemporaryStorage<T>&& storage
                              ABSL_ATTRIBUTE_LIFETIME_BOUND =
                                  TemporaryStorage<T>()) const& {
    return absl::apply(
        [&](const Args&... args) -> const T& {
          return storage.emplace(args...);
        },
        args_);
  }

  // `riegeli::Reset(dest, MakerType)` makes `dest` equivalent to the
  // constructed `T`. This avoids constructing a temporary `T` and moving from
  // it.
  template <typename T,
            std::enable_if_t<
                absl::conjunction<absl::negation<std::is_reference<T>>,
                                  std::is_move_assignable<T>,
                                  std::is_constructible<T, Args&&...>>::value,
                int> = 0>
  friend void RiegeliReset(T& dest, MakerType&& src) {
    absl::apply(
        [&](Args&&... args) {
          riegeli::Reset(dest, std::forward<Args>(args)...);
        },
        std::move(src.args_));
  }
  template <
      typename T,
      std::enable_if_t<
          absl::conjunction<absl::negation<std::is_reference<T>>,
                            std::is_move_assignable<T>,
                            std::is_constructible<T, const Args&...>>::value,
          int> = 0>
  friend void RiegeliReset(T& dest, const MakerType& src) {
    absl::apply([&](const Args&... args) { riegeli::Reset(dest, args...); },
                src.args_);
  }

 private:
  template <typename T, size_t... indices>
  void ConstructAtImpl(void* ptr, std::index_sequence<indices...>) && {
    new (ptr) T(std::forward<Args>(std::get<indices>(args_))...);
  }
  template <typename T, size_t... indices>
  void ConstructAtImpl(void* ptr, std::index_sequence<indices...>) const& {
    new (ptr) T(std::get<indices>(args_)...);
  }

  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS std::tuple<Args...> args_;
};

// Specializations of `MakerType` for 0 to 4 arguments to apply `CanBindTo`
// optimization for the 1 argument case, to make it trivially copy
// constructible when possible (for `ReferenceOrCheapValue` optimization),
// and to apply `ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS`.

template <>
class MakerType<> {
 public:
  MakerType() = default;

  MakerType(MakerType&& that) = default;
  MakerType& operator=(MakerType&& that) = default;

  MakerType(const MakerType& that) = default;
  MakerType& operator=(const MakerType& that) = default;

  template <typename T,
            std::enable_if_t<std::is_default_constructible<T>::value, int> = 0>
  T Construct() const {
    return T();
  }

  template <typename T,
            std::enable_if_t<std::is_default_constructible<T>::value, int> = 0>
  void ConstructAt(void* ptr) const {
    new (ptr) T();
  }

  template <typename T,
            std::enable_if_t<std::is_default_constructible<T>::value, int> = 0>
  T&& Reference(TemporaryStorage<T>&& storage ABSL_ATTRIBUTE_LIFETIME_BOUND =
                    TemporaryStorage<T>()) const {
    return std::move(storage).emplace();
  }

  template <typename T,
            std::enable_if_t<std::is_default_constructible<T>::value, int> = 0>
  const T& ConstReference(TemporaryStorage<T>&& storage
                              ABSL_ATTRIBUTE_LIFETIME_BOUND =
                                  TemporaryStorage<T>()) const {
    return storage.emplace();
  }

  template <typename T,
            std::enable_if_t<
                absl::conjunction<absl::negation<std::is_reference<T>>,
                                  std::is_move_assignable<T>,
                                  std::is_default_constructible<T>>::value,
                int> = 0>
  friend void RiegeliReset(T& dest, ABSL_ATTRIBUTE_UNUSED MakerType src) {
    riegeli::Reset(dest);
  }
};

template <typename Arg0>
class MakerType<Arg0> {
 public:
  template <
      typename SrcArg0,
      std::enable_if_t<
          absl::conjunction<
              absl::negation<std::is_same<std::decay_t<SrcArg0>, MakerType>>,
              std::is_convertible<SrcArg0&&, Arg0>>::value,
          int> = 0>
  /*implicit*/ MakerType(SrcArg0&& arg0) : arg0_(std::forward<SrcArg0>(arg0)) {}

  MakerType(MakerType&& that) = default;
  MakerType& operator=(MakerType&& that) = default;

  MakerType(const MakerType& that) = default;
  MakerType& operator=(const MakerType& that) = default;

  template <typename T,
            std::enable_if_t<std::is_constructible<T, Arg0&&>::value, int> = 0>
  T Construct() && {
    return T(std::forward<Arg0>(arg0_));
  }
  template <
      typename T,
      std::enable_if_t<std::is_constructible<T, const Arg0&>::value, int> = 0>
  T Construct() const& {
    return T(arg0_);
  }

  template <typename T,
            std::enable_if_t<std::is_constructible<T, Arg0&&>::value, int> = 0>
  void ConstructAt(void* ptr) && {
    new (ptr) T(std::forward<Arg0>(arg0_));
  }
  template <
      typename T,
      std::enable_if_t<std::is_constructible<T, const Arg0&>::value, int> = 0>
  void ConstructAt(void* ptr) const& {
    new (ptr) T(arg0_);
  }

  template <typename T,
            std::enable_if_t<std::is_constructible<T, Arg0&&>::value, int> = 0>
  T&& Reference(TemporaryStorage<T>&& storage ABSL_ATTRIBUTE_LIFETIME_BOUND =
                    TemporaryStorage<T>()) && {
    return std::move(*this).template ReferenceImpl<T>(std::move(storage));
  }
  template <
      typename T,
      std::enable_if_t<std::is_constructible<T, const Arg0&>::value, int> = 0>
  T&& Reference(TemporaryStorage<T>&& storage ABSL_ATTRIBUTE_LIFETIME_BOUND =
                    TemporaryStorage<T>()) const& {
    return this->template ReferenceImpl<T>(std::move(storage));
  }

  template <typename T,
            std::enable_if_t<std::is_constructible<T, Arg0&&>::value, int> = 0>
  const T& ConstReference(TemporaryStorage<T>&& storage
                              ABSL_ATTRIBUTE_LIFETIME_BOUND =
                                  TemporaryStorage<T>()) && {
    return std::move(*this).template ConstReferenceImpl<T>(std::move(storage));
  }
  template <
      typename T,
      std::enable_if_t<std::is_constructible<T, const Arg0&>::value, int> = 0>
  const T& ConstReference(TemporaryStorage<T>&& storage
                              ABSL_ATTRIBUTE_LIFETIME_BOUND =
                                  TemporaryStorage<T>()) const& {
    return this->template ConstReferenceImpl<T>(std::move(storage));
  }

  template <typename T,
            std::enable_if_t<
                absl::conjunction<absl::negation<std::is_reference<T>>,
                                  std::is_move_assignable<T>,
                                  std::is_constructible<T, Arg0&&>>::value,
                int> = 0>
  friend void RiegeliReset(T& dest, MakerType&& src) {
    riegeli::Reset(dest, std::forward<Arg0>(src.arg0_));
  }
  template <typename T,
            std::enable_if_t<
                absl::conjunction<absl::negation<std::is_reference<T>>,
                                  std::is_move_assignable<T>,
                                  std::is_constructible<T, const Arg0&>>::value,
                int> = 0>
  friend void RiegeliReset(T& dest, const MakerType& src) {
    riegeli::Reset(dest, src.arg0_);
  }

 private:
  template <typename T,
            std::enable_if_t<
                initializer_internal::CanBindTo<T&&, Arg0&&>::value, int> = 0>
  T&& ReferenceImpl(ABSL_ATTRIBUTE_UNUSED TemporaryStorage<T>&& storage =
                        TemporaryStorage<T>()) && {
    auto&& reference = std::forward<Arg0>(arg0_);
    return std::forward<T>(
        *absl::implicit_cast<std::remove_reference_t<T>*>(&reference));
  }
  template <typename T,
            std::enable_if_t<
                !initializer_internal::CanBindTo<T&&, Arg0&&>::value, int> = 0>
  T&& ReferenceImpl(TemporaryStorage<T>&& storage
                        ABSL_ATTRIBUTE_LIFETIME_BOUND =
                            TemporaryStorage<T>()) && {
    return std::move(storage).emplace(std::forward<Arg0>(arg0_));
  }

  template <
      typename T,
      std::enable_if_t<initializer_internal::CanBindTo<T&&, const Arg0&>::value,
                       int> = 0>
  T&& ReferenceImpl(ABSL_ATTRIBUTE_UNUSED TemporaryStorage<T>&& storage =
                        TemporaryStorage<T>()) const& {
    return std::forward<T>(
        *absl::implicit_cast<std::remove_reference_t<T>*>(&arg0_));
  }
  template <
      typename T,
      std::enable_if_t<
          !initializer_internal::CanBindTo<T&&, const Arg0&>::value, int> = 0>
  T&& ReferenceImpl(TemporaryStorage<T>&& storage
                        ABSL_ATTRIBUTE_LIFETIME_BOUND =
                            TemporaryStorage<T>()) const& {
    return std::move(storage).emplace(arg0_);
  }

  template <
      typename T,
      std::enable_if_t<initializer_internal::CanBindTo<const T&, Arg0&&>::value,
                       int> = 0>
  const T& ConstReferenceImpl(ABSL_ATTRIBUTE_UNUSED TemporaryStorage<T>&&
                                  storage = TemporaryStorage<T>()) && {
    auto&& reference = std::forward<Arg0>(arg0_);
    return *absl::implicit_cast<std::remove_reference_t<const T>*>(&reference);
  }
  template <
      typename T,
      std::enable_if_t<
          !initializer_internal::CanBindTo<const T&, Arg0&&>::value, int> = 0>
  const T& ConstReferenceImpl(TemporaryStorage<T>&& storage
                                  ABSL_ATTRIBUTE_LIFETIME_BOUND =
                                      TemporaryStorage<T>()) && {
    return storage.emplace(std::forward<Arg0>(arg0_));
  }

  template <typename T, std::enable_if_t<initializer_internal::CanBindTo<
                                             const T&, const Arg0&>::value,
                                         int> = 0>
  const T& ConstReferenceImpl(ABSL_ATTRIBUTE_UNUSED TemporaryStorage<T>&&
                                  storage = TemporaryStorage<T>()) const& {
    return *absl::implicit_cast<std::remove_reference_t<const T>*>(&arg0_);
  }
  template <typename T, std::enable_if_t<!initializer_internal::CanBindTo<
                                             const T&, const Arg0&>::value,
                                         int> = 0>
  const T& ConstReferenceImpl(TemporaryStorage<T>&& storage
                                  ABSL_ATTRIBUTE_LIFETIME_BOUND =
                                      TemporaryStorage<T>()) const& {
    return storage.emplace(arg0_);
  }

  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Arg0 arg0_;
};

template <typename Arg0, typename Arg1>
class MakerType<Arg0, Arg1> {
 public:
  template <typename SrcArg0, typename SrcArg1,
            std::enable_if_t<
                absl::conjunction<std::is_convertible<SrcArg0&&, Arg0>,
                                  std::is_convertible<SrcArg1&&, Arg1>>::value,
                int> = 0>
  /*implicit*/ MakerType(SrcArg0&& arg0, SrcArg1&& arg1)
      : arg0_(std::forward<SrcArg0>(arg0)),
        arg1_(std::forward<SrcArg1>(arg1)) {}

  MakerType(MakerType&& that) = default;
  MakerType& operator=(MakerType&& that) = default;

  MakerType(const MakerType& that) = default;
  MakerType& operator=(const MakerType& that) = default;

  template <typename T,
            std::enable_if_t<std::is_constructible<T, Arg0&&, Arg1&&>::value,
                             int> = 0>
  T Construct() && {
    return T(std::forward<Arg0>(arg0_), std::forward<Arg1>(arg1_));
  }
  template <
      typename T,
      std::enable_if_t<
          std::is_constructible<T, const Arg0&, const Arg1&>::value, int> = 0>
  T Construct() const& {
    return T(arg0_, arg1_);
  }

  template <typename T,
            std::enable_if_t<std::is_constructible<T, Arg0&&, Arg1&&>::value,
                             int> = 0>
  void ConstructAt(void* ptr) && {
    new (ptr) T(std::forward<Arg0>(arg0_), std::forward<Arg1>(arg1_));
  }
  template <
      typename T,
      std::enable_if_t<
          std::is_constructible<T, const Arg0&, const Arg1&>::value, int> = 0>
  void ConstructAt(void* ptr) const& {
    new (ptr) T(arg0_, arg1_);
  }

  template <typename T,
            std::enable_if_t<std::is_constructible<T, Arg0&&, Arg1&&>::value,
                             int> = 0>
  T&& Reference(TemporaryStorage<T>&& storage ABSL_ATTRIBUTE_LIFETIME_BOUND =
                    TemporaryStorage<T>()) && {
    return std::move(storage).emplace(std::forward<Arg0>(arg0_),
                                      std::forward<Arg1>(arg1_));
  }
  template <
      typename T,
      std::enable_if_t<
          std::is_constructible<T, const Arg0&, const Arg1&>::value, int> = 0>
  T&& Reference(TemporaryStorage<T>&& storage ABSL_ATTRIBUTE_LIFETIME_BOUND =
                    TemporaryStorage<T>()) const& {
    return std::move(storage).emplace(arg0_, arg1_);
  }

  template <typename T,
            std::enable_if_t<std::is_constructible<T, Arg0&&, Arg1&&>::value,
                             int> = 0>
  const T& ConstReference(TemporaryStorage<T>&& storage
                              ABSL_ATTRIBUTE_LIFETIME_BOUND =
                                  TemporaryStorage<T>()) && {
    return storage.emplace(std::forward<Arg0>(arg0_),
                           std::forward<Arg1>(arg1_));
  }
  template <
      typename T,
      std::enable_if_t<
          std::is_constructible<T, const Arg0&, const Arg1&>::value, int> = 0>
  const T& ConstReference(TemporaryStorage<T>&& storage
                              ABSL_ATTRIBUTE_LIFETIME_BOUND =
                                  TemporaryStorage<T>()) const& {
    return storage.emplace(arg0_, arg1_);
  }

  template <
      typename T,
      std::enable_if_t<
          absl::conjunction<absl::negation<std::is_reference<T>>,
                            std::is_move_assignable<T>,
                            std::is_constructible<T, Arg0&&, Arg1&&>>::value,
          int> = 0>
  friend void RiegeliReset(T& dest, MakerType&& src) {
    riegeli::Reset(dest, std::forward<Arg0>(src.arg0_),
                   std::forward<Arg1>(src.arg1_));
  }
  template <typename T,
            std::enable_if_t<
                absl::conjunction<
                    std::is_move_assignable<T>,
                    std::is_constructible<T, const Arg0&, const Arg1&>>::value,
                int> = 0>
  friend void RiegeliReset(T& dest, const MakerType& src) {
    riegeli::Reset(dest, src.arg0_, src.arg1_);
  }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Arg0 arg0_;
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Arg1 arg1_;
};

template <typename Arg0, typename Arg1, typename Arg2>
class MakerType<Arg0, Arg1, Arg2> {
 public:
  template <typename SrcArg0, typename SrcArg1, typename SrcArg2,
            std::enable_if_t<
                absl::conjunction<std::is_convertible<SrcArg0&&, Arg0>,
                                  std::is_convertible<SrcArg1&&, Arg1>,
                                  std::is_convertible<SrcArg2&&, Arg2>>::value,
                int> = 0>
  /*implicit*/ MakerType(SrcArg0&& arg0, SrcArg1&& arg1, SrcArg2&& arg2)
      : arg0_(std::forward<SrcArg0>(arg0)),
        arg1_(std::forward<SrcArg1>(arg1)),
        arg2_(std::forward<SrcArg2>(arg2)) {}

  MakerType(MakerType&& that) = default;
  MakerType& operator=(MakerType&& that) = default;

  MakerType(const MakerType& that) = default;
  MakerType& operator=(const MakerType& that) = default;

  template <
      typename T,
      std::enable_if_t<std::is_constructible<T, Arg0&&, Arg1&&, Arg2&&>::value,
                       int> = 0>
  T Construct() && {
    return T(std::forward<Arg0>(arg0_), std::forward<Arg1>(arg1_),
             std::forward<Arg2>(arg2_));
  }
  template <typename T,
            std::enable_if_t<std::is_constructible<T, const Arg0&, const Arg1&,
                                                   const Arg2&>::value,
                             int> = 0>
  T Construct() const& {
    return T(arg0_, arg1_, arg2_);
  }

  template <
      typename T,
      std::enable_if_t<std::is_constructible<T, Arg0&&, Arg1&&, Arg2&&>::value,
                       int> = 0>
  void ConstructAt(void* ptr) && {
    new (ptr) T(std::forward<Arg0>(arg0_), std::forward<Arg1>(arg1_),
                std::forward<Arg2>(arg2_));
  }
  template <typename T,
            std::enable_if_t<std::is_constructible<T, const Arg0&, const Arg1&,
                                                   const Arg2&>::value,
                             int> = 0>
  void ConstructAt(void* ptr) const& {
    new (ptr) T(arg0_, arg1_, arg2_);
  }

  template <
      typename T,
      std::enable_if_t<std::is_constructible<T, Arg0&&, Arg1&&, Arg2&&>::value,
                       int> = 0>
  T&& Reference(TemporaryStorage<T>&& storage ABSL_ATTRIBUTE_LIFETIME_BOUND =
                    TemporaryStorage<T>()) && {
    return std::move(storage).emplace(std::forward<Arg0>(arg0_),
                                      std::forward<Arg1>(arg1_),
                                      std::forward<Arg2>(arg2_));
  }
  template <typename T,
            std::enable_if_t<std::is_constructible<T, const Arg0&, const Arg1&,
                                                   const Arg2&>::value,
                             int> = 0>
  T&& Reference(TemporaryStorage<T>&& storage ABSL_ATTRIBUTE_LIFETIME_BOUND =
                    TemporaryStorage<T>()) const& {
    return std::move(storage).emplace(arg0_, arg1_, arg2_);
  }

  template <
      typename T,
      std::enable_if_t<std::is_constructible<T, Arg0&&, Arg1&&, Arg2&&>::value,
                       int> = 0>
  const T& ConstReference(TemporaryStorage<T>&& storage
                              ABSL_ATTRIBUTE_LIFETIME_BOUND =
                                  TemporaryStorage<T>()) && {
    return storage.emplace(std::forward<Arg0>(arg0_), std::forward<Arg1>(arg1_),
                           std::forward<Arg2>(arg2_));
  }
  template <typename T,
            std::enable_if_t<std::is_constructible<T, const Arg0&, const Arg1&,
                                                   const Arg2&>::value,
                             int> = 0>
  const T& ConstReference(TemporaryStorage<T>&& storage
                              ABSL_ATTRIBUTE_LIFETIME_BOUND =
                                  TemporaryStorage<T>()) const& {
    return storage.emplace(arg0_, arg1_, arg2_);
  }

  template <
      typename T,
      std::enable_if_t<
          absl::conjunction<
              absl::negation<std::is_reference<T>>, std::is_move_assignable<T>,
              std::is_constructible<T, Arg0&&, Arg1&&, Arg2&&>>::value,
          int> = 0>
  friend void RiegeliReset(T& dest, MakerType&& src) {
    riegeli::Reset(dest, std::forward<Arg0>(src.arg0_),
                   std::forward<Arg1>(src.arg1_),
                   std::forward<Arg2>(src.arg2_));
  }
  template <
      typename T,
      std::enable_if_t<
          absl::conjunction<absl::negation<std::is_reference<T>>,
                            std::is_move_assignable<T>,
                            std::is_constructible<T, const Arg0&, const Arg1&,
                                                  const Arg2&>>::value,
          int> = 0>
  friend void RiegeliReset(T& dest, const MakerType& src) {
    riegeli::Reset(dest, src.arg0_, src.arg1_, src.arg2_);
  }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Arg0 arg0_;
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Arg1 arg1_;
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Arg2 arg2_;
};

template <typename Arg0, typename Arg1, typename Arg2, typename Arg3>
class MakerType<Arg0, Arg1, Arg2, Arg3> {
 public:
  template <typename SrcArg0, typename SrcArg1, typename SrcArg2,
            typename SrcArg3,
            std::enable_if_t<
                absl::conjunction<std::is_convertible<SrcArg0&&, Arg0>,
                                  std::is_convertible<SrcArg1&&, Arg1>,
                                  std::is_convertible<SrcArg2&&, Arg2>,
                                  std::is_convertible<SrcArg3&&, Arg3>>::value,
                int> = 0>
  /*implicit*/ MakerType(SrcArg0&& arg0, SrcArg1&& arg1, SrcArg2&& arg2,
                         SrcArg3&& arg3)
      : arg0_(std::forward<SrcArg0>(arg0)),
        arg1_(std::forward<SrcArg1>(arg1)),
        arg2_(std::forward<SrcArg2>(arg2)),
        arg3_(std::forward<SrcArg3>(arg3)) {}

  MakerType(MakerType&& that) = default;
  MakerType& operator=(MakerType&& that) = default;

  MakerType(const MakerType& that) = default;
  MakerType& operator=(const MakerType& that) = default;

  template <typename T,
            std::enable_if_t<
                std::is_constructible<T, Arg0&&, Arg1&&, Arg2&&, Arg3&&>::value,
                int> = 0>
  T Construct() && {
    return T(std::forward<Arg0>(arg0_), std::forward<Arg1>(arg1_),
             std::forward<Arg2>(arg2_), std::forward<Arg3>(arg3_));
  }
  template <
      typename T,
      std::enable_if_t<std::is_constructible<T, const Arg0&, const Arg1&,
                                             const Arg2&, const Arg3&>::value,
                       int> = 0>
  T Construct() const& {
    return T(arg0_, arg1_, arg2_, arg3_);
  }

  template <typename T,
            std::enable_if_t<
                std::is_constructible<T, Arg0&&, Arg1&&, Arg2&&, Arg3&&>::value,
                int> = 0>
  void ConstructAt(void* ptr) && {
    new (ptr) T(std::forward<Arg0>(arg0_), std::forward<Arg1>(arg1_),
                std::forward<Arg2>(arg2_), std::forward<Arg3>(arg3_));
  }
  template <
      typename T,
      std::enable_if_t<std::is_constructible<T, const Arg0&, const Arg1&,
                                             const Arg2&, const Arg3&>::value,
                       int> = 0>
  void ConstructAt(void* ptr) const& {
    new (ptr) T(arg0_, arg1_, arg2_, arg3_);
  }

  template <typename T,
            std::enable_if_t<
                std::is_constructible<T, Arg0&&, Arg1&&, Arg2&&, Arg3&&>::value,
                int> = 0>
  T&& Reference(TemporaryStorage<T>&& storage ABSL_ATTRIBUTE_LIFETIME_BOUND =
                    TemporaryStorage<T>()) && {
    return std::move(storage).emplace(
        std::forward<Arg0>(arg0_), std::forward<Arg1>(arg1_),
        std::forward<Arg2>(arg2_), std::forward<Arg3>(arg3_));
  }
  template <
      typename T,
      std::enable_if_t<std::is_constructible<T, const Arg0&, const Arg1&,
                                             const Arg2&, const Arg3&>::value,
                       int> = 0>
  T&& Reference(TemporaryStorage<T>&& storage ABSL_ATTRIBUTE_LIFETIME_BOUND =
                    TemporaryStorage<T>()) const& {
    return std::move(storage).emplace(arg0_, arg1_, arg2_, arg3_);
  }

  template <typename T,
            std::enable_if_t<
                std::is_constructible<T, Arg0&&, Arg1&&, Arg2&&, Arg3&&>::value,
                int> = 0>
  const T& ConstReference(TemporaryStorage<T>&& storage
                              ABSL_ATTRIBUTE_LIFETIME_BOUND =
                                  TemporaryStorage<T>()) && {
    return storage.emplace(std::forward<Arg0>(arg0_), std::forward<Arg1>(arg1_),
                           std::forward<Arg2>(arg2_),
                           std::forward<Arg3>(arg3_));
  }
  template <
      typename T,
      std::enable_if_t<std::is_constructible<T, const Arg0&, const Arg1&,
                                             const Arg2&, const Arg3&>::value,
                       int> = 0>
  const T& ConstReference(TemporaryStorage<T>&& storage
                              ABSL_ATTRIBUTE_LIFETIME_BOUND =
                                  TemporaryStorage<T>()) const& {
    return storage.emplace(arg0_, arg1_, arg2_, arg3_);
  }

  template <
      typename T,
      std::enable_if_t<
          absl::conjunction<
              absl::negation<std::is_reference<T>>, std::is_move_assignable<T>,
              std::is_constructible<T, Arg0&&, Arg1&&, Arg2&&, Arg3&&>>::value,
          int> = 0>
  friend void RiegeliReset(T& dest, MakerType&& src) {
    riegeli::Reset(dest, std::forward<Arg0>(src.arg0_),
                   std::forward<Arg1>(src.arg1_), std::forward<Arg2>(src.arg2_),
                   std::forward<Arg3>(src.arg3_));
  }
  template <
      typename T,
      std::enable_if_t<
          absl::conjunction<
              absl::negation<std::is_reference<T>>, std::is_move_assignable<T>,
              std::is_constructible<T, const Arg0&, const Arg1&, const Arg2&,
                                    const Arg3&>>::value,
          int> = 0>
  friend void RiegeliReset(T& dest, const MakerType& src) {
    riegeli::Reset(dest, src.arg0_, src.arg1_, src.arg2_, src.arg3_);
  }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Arg0 arg0_;
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Arg1 arg1_;
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Arg2 arg2_;
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Arg3 arg3_;
};

// Support CTAD.
#if __cpp_deduction_guides
template <typename... Args>
/*implicit*/ MakerType(Args&&...) -> MakerType<std::decay_t<Args>...>;
#endif

template <typename T, typename... Args>
class MakerTypeFor;

namespace maker_internal {

template <typename T, typename... Args>
class MakerTypeForBase
    : public ConditionallyAssignable<absl::conjunction<
          absl::negation<std::is_reference<Args>>...>::value> {
 public:
  // Make `MakerTypeFor<T>` trivially default constructible (for
  // `TemporaryStorage` optimization).
  MakerTypeForBase() = default;

  // Constructs `MakerTypeFor` from `args...` convertible to `Args...`.
  template <
      typename... SrcArgs,
      std::enable_if_t<
          absl::conjunction<std::is_constructible<T, Args&&...>,
                            absl::negation<std::is_same<
                                std::tuple<std::decay_t<SrcArgs>...>,
                                std::tuple<MakerTypeFor<T, Args...>>>>,
                            std::is_convertible<SrcArgs&&, Args>...>::value,
          int> = 0>
  /*implicit*/ MakerTypeForBase(SrcArgs&&... args)
      : maker_(std::forward<SrcArgs>(args)...) {}

  MakerTypeForBase(MakerTypeForBase&& that) = default;
  MakerTypeForBase& operator=(MakerTypeForBase&& that) = default;

  MakerTypeForBase(const MakerTypeForBase& that) = default;
  MakerTypeForBase& operator=(const MakerTypeForBase& that) = default;

  // Constructs the `T`.
  /*implicit*/ operator T() && {
    return std::move(*this).maker().template Construct<T>();
  }

  // Constructs the `T` at `ptr` using placement `new`.
  void ConstructAt(void* ptr) && {
    std::move(*this).maker().template ConstructAt<T>(ptr);
  }

  // Constructs the `T`, or returns a reference to an already constructed object
  // if that was passed to the `MakerTypeFor`.
  //
  // `Reference()` instead of conversion to `T` can avoid moving the object if
  // the caller does not need to store the object, or if it will be moved later
  // because the target location for the object is not ready yet.
  //
  // `storage` must outlive usages of the returned reference.
  T&& Reference(TemporaryStorage<T>&& storage ABSL_ATTRIBUTE_LIFETIME_BOUND =
                    TemporaryStorage<T>()) && {
    return std::move(*this).maker().template Reference<T>(std::move(storage));
  }

  // Constructs the `T`, or returns a const reference to an already constructed
  // object if that was passed to the `MakerTypeFor`.
  //
  // `ConstReference()` can avoid moving the object in more cases than
  // `Reference()` if the caller does not need to store the object.
  //
  // `storage` must outlive usages of the returned reference.
  const T& ConstReference(TemporaryStorage<T>&& storage
                              ABSL_ATTRIBUTE_LIFETIME_BOUND =
                                  TemporaryStorage<T>()) && {
    return std::move(*this).maker().template ConstReference<T>(
        std::move(storage));
  }

  // `riegeli::Reset(dest, MakerTypeFor)` makes `dest` equivalent to the
  // constructed `T`. This avoids constructing a temporary `T` and moving from
  // it.
  template <typename DependentT = T,
            std::enable_if_t<
                absl::conjunction<absl::negation<std::is_reference<DependentT>>,
                                  std::is_move_assignable<DependentT>>::value,
                int> = 0>
  friend void RiegeliReset(T& dest, MakerTypeForBase&& src) {
    riegeli::Reset(dest, std::move(src.maker()));
  }

  // Returns the corresponding `MakerType` which does not specify `T`.
  //
  // This is useful for handling `MakerType` and `MakerTypeFor` generically.
  MakerType<Args...>&& maker() && { return std::move(maker_); }
  const MakerType<Args...>& maker() const& { return maker_; }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS MakerType<Args...> maker_;
};

template <bool is_const_makable, typename T, typename... Args>
class MakerTypeForByConstBase;

template <typename T, typename... Args>
class MakerTypeForByConstBase</*is_const_makable=*/false, T, Args...>
    : public MakerTypeForBase<T, Args...> {
 public:
  using MakerTypeForByConstBase::MakerTypeForBase::MakerTypeForBase;

  MakerTypeForByConstBase(MakerTypeForByConstBase&& that) = default;
  MakerTypeForByConstBase& operator=(MakerTypeForByConstBase&& that) = default;

  MakerTypeForByConstBase(const MakerTypeForByConstBase& that) = default;
  MakerTypeForByConstBase& operator=(const MakerTypeForByConstBase& that) =
      default;
};

template <typename T, typename... Args>
class MakerTypeForByConstBase</*is_const_makable=*/true, T, Args...>
    : public MakerTypeForBase<T, Args...> {
 public:
  using MakerTypeForByConstBase::MakerTypeForBase::MakerTypeForBase;

  MakerTypeForByConstBase(MakerTypeForByConstBase&& that) = default;
  MakerTypeForByConstBase& operator=(MakerTypeForByConstBase&& that) = default;

  MakerTypeForByConstBase(const MakerTypeForByConstBase& that) = default;
  MakerTypeForByConstBase& operator=(const MakerTypeForByConstBase& that) =
      default;

  using MakerTypeForByConstBase::MakerTypeForBase::operator T;
  /*implicit*/ operator T() const& {
    return this->maker().template Construct<T>();
  }

  using MakerTypeForByConstBase::MakerTypeForBase::ConstructAt;
  void ConstructAt(void* ptr) const& {
    this->maker().template ConstructAt<T>(ptr);
  }

  using MakerTypeForByConstBase::MakerTypeForBase::Reference;
  T&& Reference(TemporaryStorage<T>&& storage ABSL_ATTRIBUTE_LIFETIME_BOUND =
                    TemporaryStorage<T>()) const& {
    return this->maker().template Reference<T>(std::move(storage));
  }

  using MakerTypeForByConstBase::MakerTypeForBase::ConstReference;
  const T& ConstReference(TemporaryStorage<T>&& storage
                              ABSL_ATTRIBUTE_LIFETIME_BOUND =
                                  TemporaryStorage<T>()) const& {
    return this->maker().template ConstReference<T>(std::move(storage));
  }

  template <typename DependentT = T,
            std::enable_if_t<
                absl::conjunction<absl::negation<std::is_reference<DependentT>>,
                                  std::is_move_assignable<DependentT>>::value,
                int> = 0>
  friend void RiegeliReset(T& dest, const MakerTypeForByConstBase& src) {
    riegeli::Reset(dest, src.maker());
  }
};

}  // namespace maker_internal

// `MakerTypeFor<T, Args...>, usually made with `riegeli::Maker<T>(args...)`,
// packs constructor arguments for `T`. `MakerTypeFor<T, Args...>` is
// convertible to `Initializer<T>`.
//
// This allows the function taking `Initializer<T>` to construct the object
// in-place, avoiding constructing a temporary and moving from it.
//
// In contrast to `MakerType<Args...>`, `MakerTypeFor<T, Args...>` allows the
// caller to deduce `T`, e.g. using `InitializerTargetT`.
template <typename T, typename... Args>
class MakerTypeFor
    : public maker_internal::MakerTypeForByConstBase<
          std::is_constructible<T, const Args&...>::value, T, Args...> {
 public:
  using MakerTypeFor::MakerTypeForByConstBase::MakerTypeForByConstBase;

  MakerTypeFor(const MakerTypeFor& that) = default;
  MakerTypeFor& operator=(const MakerTypeFor& that) = default;

  MakerTypeFor(MakerTypeFor&& that) = default;
  MakerTypeFor& operator=(MakerTypeFor&& that) = default;
};

// `riegeli::Maker(args...)` returns `MakerType<Args&&...>` which packs
// constructor arguments for a yet unspecified type, which will be specified by
// the caller. `riegeli::Maker(args...)` is convertible to `Initializer<T>` for
// any `T` which can be constructed from `Args...`.
//
// This allows the function taking `Initializer<T>` to construct the object
// in-place, avoiding constructing a temporary and moving from it.
//
// In contrast to `riegeli::Maker<T>(args...)`, `riegeli::Maker(args...)`
// requires the caller to know `T`. Prefer
// `Template(riegeli::Maker<T>(args...))` over
// `Template<T>(riegeli::Maker(args...))` if CTAD of `Template` can be used.
//
// `riegeli::Invoker()` complements `riegeli::Maker()` by extending constructors
// with factory functions.
//
// `riegeli::Maker(args...)` does not generally own `args`, even if they
// involve temporaries, hence it should be used only as a parameter of a
// function or constructor, so that the temporaries outlive its usage.
// For storing a `MakerType` in a variable or returning it from a function,
// use `riegeli::OwningMaker(args...)` or construct `MakerType` directly.
//
// Some arguments can be stored by value instead of by reference as an
// optimization: some of `Args&&...` in the result type can be `Args...`.
//
// The `generic` template parameter lets `riegeli::Maker<T>()` with an explicit
// template argument unambiguously call another overload of `riegeli::Maker()`.
template <int generic = 0, typename... Args>
MakerType<ReferenceOrCheapValueT<Args>...> Maker(
    Args&&... args ABSL_ATTRIBUTE_LIFETIME_BOUND) {
  return {std::forward<Args>(args)...};
}

// `riegeli::Maker<T>(args...)` returns `MakerTypeFor<T, Args&&...>` which packs
// constructor arguments for `T`. `riegeli::Maker<T>(args...)` is convertible to
// `Initializer<T>`.
//
// This allows the function taking `Initializer<T>` to construct the object
// in-place, avoiding constructing a temporary and moving from it.
//
// `riegeli::Invoker()` complements `riegeli::Maker<T>()` by extending
// constructors with factory functions.
//
// In contrast to `riegeli::Maker(args...)`, `riegeli::Maker<T>(args...)` allows
// the caller to deduce `T`, e.g. using `InitializerTargetT`.
//
// `riegeli::Maker<T>(args...)` does not generally own `args`, even if they
// involve temporaries, hence it should be used only as a parameter of a
// function or constructor, so that the temporaries outlive its usage.
// For storing a `MakerTypeFor` in a variable or returning it from a function,
// use `riegeli::OwningMaker<T>(args...)` or construct `MakerTypeFor` directly.
//
// Some arguments can be stored by value instead of by reference as an
// optimization: some of `Args&&...` in the result type can be `Args...`.
template <typename T, typename... Args,
          std::enable_if_t<std::is_constructible<T, Args&&...>::value, int> = 0>
MakerTypeFor<T, ReferenceOrCheapValueT<Args>...> Maker(
    Args&&... args ABSL_ATTRIBUTE_LIFETIME_BOUND) {
  return {std::forward<Args>(args)...};
}

#if __cpp_deduction_guides
// `riegeli::Maker<Template>()` is like `riegeli::Maker<T>()`, but the exact
// target type is deduced using CTAD from the class template and the constructor
// arguments.
//
// Only class templates with solely type template parameters are supported.
template <template <typename...> class Template, typename... Args,
          std::enable_if_t<std::is_constructible<
                               DeduceClassTemplateArgumentsT<Template, Args...>,
                               Args&&...>::value,
                           int> = 0>
MakerTypeFor<DeduceClassTemplateArgumentsT<Template, Args...>,
             ReferenceOrCheapValueT<Args>...>
Maker(Args&&... args ABSL_ATTRIBUTE_LIFETIME_BOUND) {
  return {std::forward<Args>(args)...};
}
#endif

// `riegeli::OwningMaker()` is like `riegeli::Maker()`, but the arguments are
// stored by value instead of by reference. This is useful for storing the
// `MakerType` in a variable or returning it from a function.
template <int generic = 0, typename... Args>
MakerType<std::decay_t<Args>...> OwningMaker(Args&&... args) {
  return {std::forward<Args>(args)...};
}

// `riegeli::OwningMaker<T>()` is like `riegeli::Maker<T>()`, but the arguments
// are stored by value instead of by reference. This is useful for storing the
// `MakerTypeFor` in a variable or returning it from a function.
template <
    typename T, typename... Args,
    std::enable_if_t<std::is_constructible<T, std::decay_t<Args>&&...>::value,
                     int> = 0>
MakerTypeFor<T, std::decay_t<Args>...> OwningMaker(Args&&... args) {
  return {std::forward<Args>(args)...};
}

#if __cpp_deduction_guides
// `riegeli::OwningMaker<Template>()` is like `riegeli::OwningMaker<T>()`, but
// the exact target type is deduced using CTAD from the class template and the
// constructor arguments.
//
// Only class templates with solely type template parameters are supported.
template <
    template <typename...> class Template, typename... Args,
    std::enable_if_t<std::is_constructible<DeduceClassTemplateArgumentsT<
                                               Template, std::decay_t<Args>...>,
                                           std::decay_t<Args>...>::value,
                     int> = 0>
MakerTypeFor<DeduceClassTemplateArgumentsT<Template, std::decay_t<Args>&&...>,
             std::decay_t<Args>...>
OwningMaker(Args&&... args) {
  return {std::forward<Args>(args)...};
}
#endif

}  // namespace riegeli

#endif  // RIEGELI_BASE_MAKER_H_
