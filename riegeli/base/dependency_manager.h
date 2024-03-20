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

#ifndef RIEGELI_BASE_DEPENDENCY_MANAGER_H_
#define RIEGELI_BASE_DEPENDENCY_MANAGER_H_

#include <cstddef>
#include <memory>
#include <type_traits>
#include <utility>

#include "absl/meta/type_traits.h"
#include "riegeli/base/dependency_base.h"

namespace riegeli {

// `DependencyManager<Manager>` provides a preliminary interpretation of
// `Manager` as a pointer or pointer-like type, in the form of a protected
// member function `ptr()`. It is used by `DependencyImpl` specializations to
// infer `get()`, which often returns `*ptr()` or `ptr()`, depending on which
// of them is convertible to `Handle`.
//
// Examples:
//  * `T* DependencyManager<T>::ptr()`
//  * `T* DependencyManager<T&>::ptr()`
//  * `T* DependencyManager<T&&>::ptr()`
//  * `T* DependencyManager<T*>::ptr()`
//  * `std::nullptr_t DependencyManager<std::nullptr_t>::ptr()`
//  * `T* DependencyManager<std::unique_ptr<T, Deleter>>::ptr()`
//  * `Handle DependencyManager<AnyDependency<Handle>>::ptr()`
//
// `DependencyManager<Manager>` derives from
// `DependencyManagerImpl<Manager, ManagerStorage>` (where `ManagerStorage` is
// `Manager`, `Manager&`, or `Manager&&`) which has specializations for various
// `Manager` types.
//
// `DependencyManagerImpl<Manager, ManagerStorage>` specializations often derive
// from `DependencyBase<ManagerStorage>` (or from `DependencyBase<Manager>` if
// `Manager` is cheap to move).
//
// `DependencyManager` provides what `DependencyBase` provides (constructors,
// `Reset()`, `manager()`, and `kIsStable`), and also `ptr()`, `IsOwning()`,
// `kIsOwning`, and optionally `GetIf()`.

// This template is specialized but does not have a primary definition.
template <typename Manager, typename ManagerStorage, typename Enable = void>
class DependencyManagerImpl;

// Specialization of `DependencyManagerImpl<T*, ManagerStorage>`: an unowned
// dependency stored by pointer.
template <typename T, typename ManagerStorage>
class DependencyManagerImpl<T*, ManagerStorage> : public DependencyBase<T*> {
 public:
  using DependencyManagerImpl::DependencyBase::DependencyBase;

  static constexpr bool kIsOwning = false;

  static constexpr bool kIsStable = true;

 protected:
  DependencyManagerImpl(const DependencyManagerImpl& that) = default;
  DependencyManagerImpl& operator=(const DependencyManagerImpl& that) = default;

  DependencyManagerImpl(DependencyManagerImpl&& that) = default;
  DependencyManagerImpl& operator=(DependencyManagerImpl&& that) = default;

  ~DependencyManagerImpl() = default;

  T* ptr() const { return this->manager(); }
};

// Specialization of `DependencyManagerImpl<std::nullptr_t, ManagerStorage>`:
// an unowned dependency stored by pointer, always missing. This is useful for
// `AnyDependency` and `AnyDependencyRef`.
template <typename ManagerStorage>
class DependencyManagerImpl<std::nullptr_t, ManagerStorage>
    : public DependencyBase<std::nullptr_t> {
 public:
  using DependencyManagerImpl::DependencyBase::DependencyBase;

  static constexpr bool kIsOwning = false;

  static constexpr bool kIsStable = true;

 protected:
  DependencyManagerImpl(const DependencyManagerImpl& that) = default;
  DependencyManagerImpl& operator=(const DependencyManagerImpl& that) = default;

  DependencyManagerImpl(DependencyManagerImpl&& that) = default;
  DependencyManagerImpl& operator=(DependencyManagerImpl&& that) = default;

  ~DependencyManagerImpl() = default;

  std::nullptr_t ptr() const { return nullptr; }
};

// Specialization of
// `DependencyManagerImpl<std::unique_ptr<T, Deleter>, ManagerStorage>`:
// an owned dependency stored by `std::unique_ptr`.
template <typename T, typename Deleter, typename ManagerStorage>
class DependencyManagerImpl<std::unique_ptr<T, Deleter>, ManagerStorage>
    : public DependencyBase<
          std::conditional_t<std::is_empty<Deleter>::value,
                             std::unique_ptr<T, Deleter>, ManagerStorage>> {
 public:
  using DependencyManagerImpl::DependencyBase::DependencyBase;

  bool IsOwning() const { return this->manager() != nullptr; }

  static constexpr bool kIsOwning = true;

  static constexpr bool kIsStable = true;

 protected:
  DependencyManagerImpl(const DependencyManagerImpl& that) = default;
  DependencyManagerImpl& operator=(const DependencyManagerImpl& that) = default;

  DependencyManagerImpl(DependencyManagerImpl&& that) = default;
  DependencyManagerImpl& operator=(DependencyManagerImpl&& that) = default;

  ~DependencyManagerImpl() = default;

  T* ptr() const { return this->manager().get(); }
};

namespace dependency_manager_internal {

// `IsValidDependencyManagerImpl<Manager>::value` is `true` when
// `DependencyManagerImpl<Manager, Manager>` is defined.

template <typename Manager, typename Enable = void>
struct IsValidDependencyManagerImpl : std::false_type {};

template <typename Manager>
struct IsValidDependencyManagerImpl<
    Manager,
    absl::void_t<
        decltype(std::declval<const DependencyManagerImpl<Manager, Manager>&>()
                     .manager())>> : std::true_type {};

}  // namespace dependency_manager_internal

// `DependencyManager<Manager>` extends
// `DependencyManagerImpl<Manager, ManagerStorage>` with the basic case when
// `Manager` is an owned dependency stored by value, and with specializations
// when `Manager` is `T&` or `T&&`.

template <typename Manager, typename Enable = void>
class DependencyManager;

// Specialization of `DependencyManager<Manager>` when
// `DependencyManagerImpl<Manager>` is defined: delegate to it.
template <typename Manager>
class DependencyManager<
    Manager, std::enable_if_t<absl::conjunction<
                 absl::negation<std::is_reference<Manager>>,
                 dependency_manager_internal::IsValidDependencyManagerImpl<
                     Manager>>::value>>
    : public DependencyManagerImpl<Manager, Manager> {
 public:
  using DependencyManager::DependencyManagerImpl::DependencyManagerImpl;

  static_assert(
      std::is_convertible<
          decltype(std::declval<DependencyManagerImpl<Manager, Manager>&>()
                       .manager()),
          Manager&>::value,
      "DependencyManagerImpl<Manager, Manager>::manager() "
      "must return Manager&");

 protected:
  DependencyManager(const DependencyManager& that) = default;
  DependencyManager& operator=(const DependencyManager& that) = default;

  DependencyManager(DependencyManager&& that) = default;
  DependencyManager& operator=(DependencyManager&& that) = default;

  ~DependencyManager() = default;
};

// Specialization of `DependencyManager<Manager>` when
// `DependencyManagerImpl<Manager>` is not defined: an owned dependency stored
// by value.
template <typename Manager>
class DependencyManager<
    Manager,
    std::enable_if_t<absl::conjunction<
        absl::negation<std::is_reference<Manager>>,
        absl::negation<dependency_manager_internal::
                           IsValidDependencyManagerImpl<Manager>>>::value>>
    : public DependencyBase<Manager> {
 public:
  using DependencyManager::DependencyBase::DependencyBase;

  static constexpr bool kIsOwning = true;

 protected:
  DependencyManager(const DependencyManager& that) = default;
  DependencyManager& operator=(const DependencyManager& that) = default;

  DependencyManager(DependencyManager&& that) = default;
  DependencyManager& operator=(DependencyManager&& that) = default;

  ~DependencyManager() = default;

  Manager* ptr() const { return &this->mutable_manager(); }
};

// Specialization of `DependencyManager<Manager&>` when
// `DependencyManagerImpl<absl::remove_cvref_t<Manager>>` is defined:
// delegate to it, but store `absl::remove_cvref_t<Manager>` by reference
// to avoid moving it.
//
// This handles cases where `Manager` is deduced from a function parameter
// as a reference type, but the type under the reference determines the
// interpretation, e.g. `T*&`.
template <typename Manager>
class DependencyManager<
    Manager&,
    std::enable_if_t<dependency_manager_internal::IsValidDependencyManagerImpl<
        absl::remove_cvref_t<Manager>>::value>>
    : public DependencyManagerImpl<absl::remove_cvref_t<Manager>,
                                   absl::remove_cvref_t<Manager>&> {
 public:
  using DependencyManager::DependencyManagerImpl::DependencyManagerImpl;

  static_assert(
      std::is_convertible<decltype(std::declval<DependencyManagerImpl<
                                       absl::remove_cvref_t<Manager>,
                                       absl::remove_cvref_t<Manager>&>&>()
                                       .manager()),
                          Manager&>::value,
      "DependencyManagerImpl<Manager, Manager&>::manager() "
      "must return Manager&");

 protected:
  DependencyManager(const DependencyManager& that) = default;
  DependencyManager& operator=(const DependencyManager& that) = default;

  DependencyManager(DependencyManager&& that) = default;
  DependencyManager& operator=(DependencyManager&& that) = default;

  ~DependencyManager() = default;
};

// Specialization of `DependencyManager<Manager&>` when
// `DependencyManagerImpl<absl::remove_cvref_t<Manager>>` is not defined:
// an unowned dependency stored by lvalue reference.
template <typename Manager>
class DependencyManager<
    Manager&,
    std::enable_if_t<!dependency_manager_internal::IsValidDependencyManagerImpl<
        absl::remove_cvref_t<Manager>>::value>>
    : public DependencyBase<Manager&> {
 public:
  using DependencyManager::DependencyBase::DependencyBase;

  static constexpr bool kIsOwning = false;

 protected:
  DependencyManager(const DependencyManager& that) = default;
  DependencyManager& operator=(const DependencyManager&) = delete;

  ~DependencyManager() = default;

  Manager* ptr() const { return &this->manager(); }
};

// Specialization of `DependencyManager<Manager&&>` when
// `DependencyManagerImpl<absl::remove_cvref_t<Manager>>` is defined:
// delegate to it, but store `absl::remove_cvref_t<Manager>` by reference
// to avoid moving it.
//
// This handles cases where `Manager` is deduced from a function parameter
// as a reference type, but the type under the reference determines the
// interpretation, e.g. `std::unique_ptr<T>&&`.
template <typename Manager>
class DependencyManager<
    Manager&&,
    std::enable_if_t<dependency_manager_internal::IsValidDependencyManagerImpl<
        absl::remove_cvref_t<Manager>>::value>>
    : public DependencyManagerImpl<absl::remove_cvref_t<Manager>,
                                   absl::remove_cvref_t<Manager>&&> {
 public:
  using DependencyManager::DependencyManagerImpl::DependencyManagerImpl;

  static_assert(
      std::is_convertible<decltype(std::declval<DependencyManagerImpl<
                                       absl::remove_cvref_t<Manager>,
                                       absl::remove_cvref_t<Manager>&&>&>()
                                       .manager()),
                          Manager&>::value,
      "DependencyManagerImpl<Manager, Manager&&>::manager() "
      "must return Manager&");

 protected:
  DependencyManager(const DependencyManager& that) = default;
  DependencyManager& operator=(const DependencyManager& that) = default;

  DependencyManager(DependencyManager&& that) = default;
  DependencyManager& operator=(DependencyManager&& that) = default;

  ~DependencyManager() = default;
};

// Specialization of `DependencyManager<Manager&&>` when
// `DependencyManagerImpl<absl::remove_cvref_t<Manager>>` is not defined: an
// owned dependency stored by rvalue reference.
template <typename Manager>
class DependencyManager<
    Manager&&,
    std::enable_if_t<!dependency_manager_internal::IsValidDependencyManagerImpl<
        absl::remove_cvref_t<Manager>>::value>>
    : public DependencyBase<Manager&&> {
 public:
  using DependencyManager::DependencyBase::DependencyBase;

  static constexpr bool kIsOwning = true;

 protected:
  DependencyManager(DependencyManager&& that) = default;
  DependencyManager& operator=(DependencyManager&&) = delete;

  ~DependencyManager() = default;

  Manager* ptr() const { return &this->manager(); }
};

namespace dependency_manager_internal {

// Expose protected `DependencyManager::ptr()` for `DependencyManagerPtr`.
template <typename Manager>
struct DependencyManagerAccess : DependencyManager<Manager> {
  using DependencyManagerAccess::DependencyManager::ptr;
};

// `DependencyManagerPtrImpl<Manager>::type` is the type returned by
// `DependencyManager<Manager>::ptr()`.
template <typename Manager, typename Enable = void>
struct DependencyManagerPtrImpl {
  using type =
      decltype(std::declval<const DependencyManagerAccess<Manager>&>().ptr());
};

// In `DependencyManagerPtrImpl<Manager>` for `Manager` stored by value, avoid
// instantiating `DependencyManager<Manager>` just to see what its `ptr()` would
// return. This could lead to subtle compile errors, causing the following chain
// of template instantiations:
//
//  * IsValidDependency<BackwardWriter*, Writer&>
//  * IsValidDependencyDefault<BackwardWriter*, Writer>
//  * DependencyManagerPtr<Writer>
//  * DependencyManager<Writer>
//  * DependencyBase<Writer>
//
// which contains a member variable of an incomplete type.
template <typename Manager>
struct DependencyManagerPtrImpl<
    Manager,
    std::enable_if_t<absl::conjunction<
        absl::negation<std::is_reference<Manager>>,
        absl::negation<IsValidDependencyManagerImpl<Manager>>>::value>> {
  using type = Manager*;
};

}  // namespace dependency_manager_internal

// `DependencyManagerPtr<Manager>` is the type returned by
// `DependencyManager<Manager>::ptr()`.
template <typename Manager>
using DependencyManagerPtr =
    typename dependency_manager_internal::DependencyManagerPtrImpl<
        Manager>::type;

// `DependencyManagerRef<Manager>` is
// `std::remove_pointer_t<DependencyManagerPtr<Manager>>`.
template <typename Manager>
using DependencyManagerRef =
    std::remove_pointer_t<DependencyManagerPtr<Manager>>;

}  // namespace riegeli

#endif  // RIEGELI_BASE_DEPENDENCY_MANAGER_H_
