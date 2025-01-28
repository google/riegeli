// Copyright 2017 Google LLC
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

#ifndef RIEGELI_BYTES_FD_HANDLE_H_
#define RIEGELI_BYTES_FD_HANDLE_H_

#include "riegeli/base/type_traits.h"
#ifdef _WIN32
#include <sys/stat.h>
#else
#include <sys/types.h>
#endif

#include <cstddef>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/meta/type_traits.h"
#include "absl/status/status.h"
#include "riegeli/base/any.h"
#include "riegeli/base/c_string_ref.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/type_erased_ref.h"

namespace riegeli {

namespace fd_internal {

#ifndef _WIN32
using Permissions = mode_t;
#else
using Permissions = int;
#endif

}  // namespace fd_internal

// `SupportsFdHandle<T>::value` is `true` if `T&` is a valid constructor
// argument for `FdHandle`.

template <typename T, typename Enable = void>
struct SupportsFdHandle : std::false_type {};

template <typename T>
struct SupportsFdHandle<
    T, std::enable_if_t<absl::conjunction<
           absl::negation<std::is_const<T>>,
           std::is_convertible<decltype(std::declval<const T&>().get()),
                               int>>::value>> : std::true_type {};

// `FdSupportsOpen<T>::value` is `true` if `T` supports `Open()` with the
// signature like in `OwnedFd` (with `permissions` present), except that
// `const char* filename` is sufficient.

template <typename T, typename Enable = void>
struct FdSupportsOpen : std::false_type {};

template <typename T>
struct FdSupportsOpen<T,
                      std::enable_if_t<std::is_convertible<
                          decltype(std::declval<T&>().Open(
                              std::declval<const char*>(), std::declval<int>(),
                              std::declval<fd_internal::Permissions>())),
                          absl::Status>::value>> : std::true_type {};

// `FdSupportsOpenAt<T>::value` is `true` if `T` supports `OpenAt()` with the
// signature like in `OwnedFd` (with `permissions` present), except that
// `const char* filename` is sufficient.

template <typename T, typename Enable = void>
struct FdSupportsOpenAt : std::false_type {};

template <typename T>
struct FdSupportsOpenAt<
    T, std::enable_if_t<std::is_convertible<
           decltype(std::declval<T&>().OpenAt(
               std::declval<int>(), std::declval<const char*>(),
               std::declval<int>(), std::declval<fd_internal::Permissions>())),
           absl::Status>::value>> : std::true_type {};

// Type-erased pointer to a target object like `UnownedFd` or `OwnedFd` which
// stores and possibly owns a fd.
//
// The target should support:
//
// ```
//   // Returns the fd.
//   int get() const;
//
//   // Returns `true` if the target owns the fd, i.e. is responsible for
//   // closing it and the fd is present.
//   //
//   // Optional. If absent, the presence of `Close()` determines whether the
//   // target is considered to own the fd.
//   bool IsOwning() const;
//
//   // Opens a new fd, like with `open()` but returning `absl::Status`.
//   //
//   // Optional. Used by `FdReader` and `FdWriter` constructors from the
//   // filename.
//   absl::Status Open(const char* filename, int mode,
//                     OwnedFd::Permissions permissions);
//
//   // Closes the fd if `IsOwning()`.
//   //
//   // Returns `absl::OkStatus()` if `!IsOwning()`.
//   //
//   // Optional. If absent, `absl::OkStatus()` is assumed.
//   absl::Status Close();
// ```
class
#ifdef ABSL_NULLABILITY_COMPATIBLE
    ABSL_NULLABILITY_COMPATIBLE
#endif
        FdHandle : public WithEqual<FdHandle> {
 public:
  // Creates an `FdHandle` which does not refer to a target.
  FdHandle() = default;
  /*implicit*/ FdHandle(std::nullptr_t) {}

  // Creates an `FdHandle` which refers to `target`.
  template <typename T,
            std::enable_if_t<absl::conjunction<NotSelfCopy<FdHandle, T&>,
                                               SupportsFdHandle<T>>::value,
                             int> = 0>
  /*implicit*/ FdHandle(T& target ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : methods_(&kMethods<T>), target_(target) {}

  FdHandle(const FdHandle& that) = default;
  FdHandle& operator=(const FdHandle& that) = default;

  // Returns `true` if the fd is present.
  bool is_open() const { return *this != nullptr; }

  // Returns the fd.
  int get() const { return methods_->get(target_); }

  // Returns `true` if the `FdHandle` owns the fd, i.e. is responsible for
  // closing it and the fd is present.
  bool IsOwning() const { return methods_->is_owning(target_); }

  // Closes the fd if `IsOwning()`.
  //
  // Returns `absl::OkStatus()` if `!IsOwning()`.
  absl::Status Close() { return methods_->close(target_); }

  friend bool operator==(FdHandle a, FdHandle b) { return a.get() == b.get(); }
  friend bool operator==(FdHandle a, int b) { return a.get() == b; }
  friend bool operator==(FdHandle a, std::nullptr_t) {
    return a.target_ == nullptr || a.get() < 0;
  }

 private:
  struct Methods {
    int (*get)(TypeErasedRef target);
    bool (*is_owning)(TypeErasedRef target);
    absl::Status (*close)(TypeErasedRef target);
  };

  template <typename T, typename Enable = void>
  struct HasIsOwning : std::false_type {};
  template <typename T>
  struct HasIsOwning<
      T, std::enable_if_t<std::is_convertible<
             decltype(std::declval<const T&>().IsOwning()), bool>::value>>
      : std::true_type {};

  template <typename T, typename Enable = void>
  struct HasClose : std::false_type {};
  template <typename T>
  struct HasClose<
      T, std::enable_if_t<std::is_convertible<
             decltype(std::declval<T&>().Close()), absl::Status>::value>>
      : std::true_type {};

  static int GetMethodDefault(ABSL_ATTRIBUTE_UNUSED TypeErasedRef target) {
    return -1;
  }

  static bool IsOwningMethodDefault(
      ABSL_ATTRIBUTE_UNUSED TypeErasedRef target) {
    return false;
  }

  static absl::Status CloseMethodDefault(
      ABSL_ATTRIBUTE_UNUSED TypeErasedRef target) {
    return absl::OkStatus();
  }

  static constexpr Methods kMethodsDefault = {
      GetMethodDefault, IsOwningMethodDefault, CloseMethodDefault};

  template <typename T>
  static int GetMethod(TypeErasedRef target) {
    return target.Cast<const T&>().get();
  }

  template <typename T, std::enable_if_t<HasIsOwning<T>::value, int> = 0>
  static bool IsOwningMethod(TypeErasedRef target) {
    return target.Cast<const T&>().IsOwning();
  }
  template <typename T, std::enable_if_t<!HasIsOwning<T>::value, int> = 0>
  static bool IsOwningMethod(TypeErasedRef target) {
    return HasClose<T>::value && target.Cast<const T&>().get() >= 0;
  }

  template <typename T, std::enable_if_t<HasClose<T>::value, int> = 0>
  static absl::Status CloseMethod(TypeErasedRef target) {
    return target.Cast<T&>().Close();
  }
  template <typename T, std::enable_if_t<!HasClose<T>::value, int> = 0>
  static absl::Status CloseMethod(ABSL_ATTRIBUTE_UNUSED TypeErasedRef target) {
    return absl::OkStatus();
  }

  template <typename T>
  static constexpr Methods kMethods = {GetMethod<T>, IsOwningMethod<T>,
                                       CloseMethod<T>};

  const Methods* methods_ = &kMethodsDefault;
  TypeErasedRef target_;
};

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if !__cpp_inline_variables
template <typename T>
constexpr FdHandle::Methods FdHandle::kMethods;
#endif

// Stores a file descriptor but does not own it, i.e. is not responsible for
// closing it.
//
// The fd can be negative which means absent.
class
#ifdef ABSL_NULLABILITY_COMPATIBLE
    ABSL_NULLABILITY_COMPATIBLE
#endif
        UnownedFd : public WithEqual<UnownedFd> {
 public:
  // Creates an `UnownedFd` which does not store a fd.
  UnownedFd() = default;
  /*implicit*/ UnownedFd(std::nullptr_t) {}

  // Creates an `UnownedFd` which stores `fd`.
  explicit UnownedFd(int fd ABSL_ATTRIBUTE_LIFETIME_BOUND) : fd_(fd) {}

  UnownedFd(const UnownedFd& that) = default;
  UnownedFd& operator=(const UnownedFd& that) = default;

  ABSL_ATTRIBUTE_REINITIALIZES void Reset(std::nullptr_t = nullptr) {
    fd_ = -1;
  }
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(int fd) { fd_ = fd; }

  // Returns `true` if the fd is present.
  bool is_open() const { return *this != nullptr; }

  // Returns the fd.
  int get() const { return fd_; }

  friend bool operator==(UnownedFd a, UnownedFd b) {
    return a.get() == b.get();
  }
  friend bool operator==(UnownedFd a, int b) { return a.get() == b; }
  friend bool operator==(UnownedFd a, std::nullptr_t) { return a.get() < 0; }

 private:
  int fd_ = -1;
};

// Owns a file descriptor, i.e. stores it and is responsible for closing it.
//
// The fd can be negative which means absent.
class
#ifdef ABSL_NULLABILITY_COMPATIBLE
    ABSL_NULLABILITY_COMPATIBLE
#endif
#ifdef ABSL_ATTRIBUTE_TRIVIAL_ABI
        ABSL_ATTRIBUTE_TRIVIAL_ABI
#endif
            OwnedFd : public WithEqual<OwnedFd> {
 public:
  using Permissions = fd_internal::Permissions;
#ifndef _WIN32
  static constexpr Permissions kDefaultPermissions = 0666;
#else
  static constexpr Permissions kDefaultPermissions = _S_IREAD | _S_IWRITE;
#endif

  // Creates an `OwnedFd` which does not own a fd.
  OwnedFd() = default;
  /*implicit*/ OwnedFd(std::nullptr_t) {}

  // Creates an `OwnedFd` which owns `fd`.
  explicit OwnedFd(int fd ABSL_ATTRIBUTE_LIFETIME_BOUND) : fd_(fd) {}

  // The moved-from fd is left absent.
  OwnedFd(OwnedFd&& that) noexcept : fd_(that.Release()) {}
  OwnedFd& operator=(OwnedFd&& that) noexcept {
    Reset(that.Release());
    return *this;
  }

  ~OwnedFd() { Close().IgnoreError(); }

  ABSL_ATTRIBUTE_REINITIALIZES void Reset(std::nullptr_t = nullptr) {
    Close().IgnoreError();
    fd_ = -1;
  }
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(int fd) {
    Close().IgnoreError();
    fd_ = fd;
  }

  // Returns `true` if the fd is present.
  bool is_open() const { return *this != nullptr; }

  // Returns the fd.
  int get() const ABSL_ATTRIBUTE_LIFETIME_BOUND { return fd_; }

  // Releases and returns the fd without closing it.
  int Release() { return std::exchange(fd_, -1); }

  friend bool operator==(const OwnedFd& a, int b) { return a.get() == b; }
  friend bool operator==(const OwnedFd& a, std::nullptr_t) {
    return a.get() < 0;
  }

  // Opens a new fd, like with `open()` but taking `CStringRef filename`
  // and returning `absl::Status`.
  ABSL_ATTRIBUTE_REINITIALIZES absl::Status Open(
      CStringRef filename, int mode,
      Permissions permissions = kDefaultPermissions);

#ifndef _WIN32
  // Opens a new fd with the filename interpreted relatively to the directory
  // specified by an existing fd, like with `openat()` but taking
  // `absl::string_view filename` and returning `absl::Status`.
  ABSL_ATTRIBUTE_REINITIALIZES absl::Status OpenAt(
      int dir_fd, CStringRef filename, int mode,
      Permissions permissions = kDefaultPermissions);
#endif  // !_WIN32

  // Closes the fd if present.
  //
  // Returns `absl::OkStatus()` if absent.
  absl::Status Close();

 private:
  int fd_ = -1;
};

// Type-erased object like `UnownedFd` or `OownedFd` which stores and possibly
// owns a fd.
using AnyFd = Any<FdHandle>::Inlining<UnownedFd, OwnedFd>;

}  // namespace riegeli

#endif  // RIEGELI_BYTES_FD_HANDLE_H_
