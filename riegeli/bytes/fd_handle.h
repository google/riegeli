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

#ifndef _WIN32
#include <sys/types.h>
#endif

#include <cstddef>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/meta/type_traits.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/compare.h"

namespace riegeli {

// Owns a file descriptor, i.e. stores it and is responsible for closing it.
//
// The fd can be negative which means absent.
class
#ifdef ABSL_ATTRIBUTE_TRIVIAL_ABI
    ABSL_ATTRIBUTE_TRIVIAL_ABI
#endif
        OwnedFd : public WithEqual<OwnedFd> {
 public:
#ifndef _WIN32
  using Permissions = mode_t;
  static constexpr Permissions kDefaultPermissions = 0666;
#else
  using Permissions = int;
  static constexpr Permissions kDefaultPermissions = _S_IREAD | _S_IWRITE;
#endif

  // Creates an `OwnedFd` which does not own a fd.
  OwnedFd() = default;

  // Creates an `OwnedFd` which owns `fd`.
  explicit OwnedFd(int fd) noexcept : fd_(fd) {}

  // The moved-from fd is left absent.
  OwnedFd(OwnedFd&& that) noexcept : fd_(that.Release()) {}
  OwnedFd& operator=(OwnedFd&& that) {
    Reset(that.Release());
    return *this;
  }

  ~OwnedFd() { Close().IgnoreError(); }

  ABSL_ATTRIBUTE_REINITIALIZES void Reset(int fd = -1) {
    Close().IgnoreError();
    fd_ = fd;
  }

  // Returns `true` if the fd is present.
  bool is_open() const { return fd_ >= 0; }

  // Returns the fd.
  int get() const { return fd_; }

  // Releases and returns the fd without closing it.
  int Release() { return std::exchange(fd_, -1); }

  friend bool operator==(const OwnedFd& a, int b) { return a.get() == b; }

  // Opens a new fd, like with `open()` but taking `absl::string_view filename`
  // and returning `absl::Status`.
#ifndef _WIN32
  ABSL_ATTRIBUTE_REINITIALIZES absl::Status Open(
      absl::string_view filename, int mode,
      Permissions permissions = kDefaultPermissions) {
    return Open(std::string(filename).c_str(), mode, permissions);
  }
  ABSL_ATTRIBUTE_REINITIALIZES absl::Status Open(
      const std::string& filename, int mode,
      Permissions permissions = kDefaultPermissions) {
    return Open(filename.c_str(), mode, permissions);
  }
  ABSL_ATTRIBUTE_REINITIALIZES absl::Status Open(
      const char* filename, int mode,
      Permissions permissions = kDefaultPermissions);
#else
  ABSL_ATTRIBUTE_REINITIALIZES absl::Status Open(
      absl::string_view filename, int mode,
      Permissions permissions = kDefaultPermissions);
#endif

#ifndef _WIN32
  // Opens a new fd with the filename interpreted relatively to the directory
  // specified by an existing fd, like with `openat()` but taking
  // `absl::string_view filename` and returning `absl::Status`.
  ABSL_ATTRIBUTE_REINITIALIZES absl::Status OpenAt(
      int dir_fd, absl::string_view filename, int mode,
      Permissions permissions = kDefaultPermissions) {
    return OpenAt(dir_fd, std::string(filename).c_str(), mode, permissions);
  }
  ABSL_ATTRIBUTE_REINITIALIZES absl::Status OpenAt(
      int dir_fd, const std::string& filename, int mode,
      Permissions permissions = kDefaultPermissions) {
    return OpenAt(dir_fd, filename.c_str(), mode, permissions);
  }
  ABSL_ATTRIBUTE_REINITIALIZES absl::Status OpenAt(
      int dir_fd, const char* filename, int mode,
      Permissions permissions = kDefaultPermissions);
#endif  // !_WIN32

  // Closes the fd if present.
  //
  // Returns `absl::OkStatus()` if absent.
  absl::Status Close();

 private:
  int fd_ = -1;
};

// Stores a file descriptor but does not own it, i.e. is not responsible for
// closing it.
//
// The fd can be negative which means absent.
class UnownedFd : public WithEqual<UnownedFd> {
 public:
  // Creates an `UnownedFd` which does not store a fd.
  UnownedFd() = default;

  // Creates an `UnownedFd` which stores `fd`.
  explicit UnownedFd(int fd) noexcept : fd_(fd) {}

  UnownedFd(const UnownedFd& that) = default;
  UnownedFd& operator=(const UnownedFd& that) = default;

  ABSL_ATTRIBUTE_REINITIALIZES void Reset(int fd = -1) { fd_ = fd; }

  // Returns `true` if the fd is present.
  bool is_open() const { return fd_ >= 0; }

  // Returns the fd.
  int get() const { return fd_; }

  friend bool operator==(UnownedFd a, UnownedFd b) {
    return a.get() == b.get();
  }
  friend bool operator==(UnownedFd a, int b) { return a.get() == b; }

 private:
  int fd_ = -1;
};

// `IsValidFdTarget<T>::value` is `true` if `T*` is a valid constructor
// argument for `FdHandle`.

template <typename T, typename Enable = void>
struct IsValidFdTarget : std::false_type {};

template <typename T>
struct IsValidFdTarget<
    T, std::enable_if_t<absl::conjunction<
           absl::negation<std::is_const<T>>,
           std::is_convertible<decltype(std::declval<const T&>().get()),
                               int>>::value>> : std::true_type {};

// `FdTargetHasOpen<T>::value` is `true` if `T` supports `Open()` with the
// signature like in `OwnedFd` (with `permissions` present).

template <typename T, typename Enable = void>
struct FdTargetHasOpen : std::false_type {};

template <typename T>
struct FdTargetHasOpen<
    T, std::enable_if_t<std::is_convertible<
           decltype(std::declval<T&>().Open(
               std::declval<absl::string_view>(), std::declval<int>(),
               std::declval<OwnedFd::Permissions>())),
           absl::Status>::value>> : std::true_type {};

// `FdTargetHasOpenAt<T>::value` is `true` if `T` supports `OpenAt()` with the
// signature like in `OwnedFd` (with `permissions` present).

template <typename T, typename Enable = void>
struct FdTargetHasOpenAt : std::false_type {};

template <typename T>
struct FdTargetHasOpenAt<
    T, std::enable_if_t<std::is_convertible<
           decltype(std::declval<T&>().OpenAt(
               std::declval<int>(), std::declval<absl::string_view>(),
               std::declval<int>(), std::declval<OwnedFd::Permissions>())),
           absl::Status>::value>> : std::true_type {};

// Type-erased pointer to a target object like `OwnedFd` or `UnownedFd` which
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
class FdHandle : public WithEqual<FdHandle> {
 public:
  // Creates an `FdHandle` which does not point to a target.
  FdHandle() = default;
  /*implicit*/ FdHandle(std::nullptr_t) noexcept {}

  // Creates an `FdHandle` which points to `target`.
  template <typename T, std::enable_if_t<IsValidFdTarget<T>::value, int> = 0>
  explicit FdHandle(T* target) : methods_(&kMethods<T>), target_(target) {}

  FdHandle(const FdHandle& that) = default;
  FdHandle& operator=(const FdHandle& that) = default;

  // Returns `true` if the fd is present.
  bool is_open() const { return **this >= 0; }

  // Returns the fd.
  int operator*() const { return methods_->get(target_); }

  friend bool operator==(FdHandle a, FdHandle b) {
    return a.target_ == b.target_;
  }

  // Allow Nullability annotations on `FdHandle`.
  using absl_nullability_compatible = void;

  // Returns `true` if the `FdHandle` owns the fd, i.e. is responsible for
  // closing it and the fd is present.
  bool IsOwning() const { return methods_->is_owning(target_); }

  // Closes the fd if `IsOwning()`.
  //
  // Returns `absl::OkStatus()` if `!IsOwning()`.
  absl::Status Close() { return methods_->close(target_); }

 private:
  template <typename T, typename Enable = void>
  struct FdTargetHasClose : std::false_type {};

  template <typename T>
  struct FdTargetHasClose<
      T, std::enable_if_t<std::is_convertible<
             decltype(std::declval<T&>().Close()), absl::Status>::value>>
      : std::true_type {};

  template <typename T, typename Enable = void>
  struct FdTargetHasIsOwning : std::false_type {};

  template <typename T>
  struct FdTargetHasIsOwning<
      T, std::enable_if_t<std::is_convertible<
             decltype(std::declval<const T&>().IsOwning()), bool>::value>>
      : std::true_type {};

  template <typename T>
  static int GetMethod(const void* target) {
    return static_cast<const T*>(target)->get();
  }

  template <typename T,
            std::enable_if_t<FdTargetHasIsOwning<T>::value, int> = 0>
  static bool IsOwningMethod(const void* target) {
    return static_cast<const T*>(target)->IsOwning();
  }
  template <typename T,
            std::enable_if_t<!FdTargetHasIsOwning<T>::value, int> = 0>
  static bool IsOwningMethod(const void* target) {
    return FdTargetHasClose<T>::value &&
           static_cast<const T*>(target)->get() >= 0;
  }

  template <typename T, std::enable_if_t<FdTargetHasClose<T>::value, int> = 0>
  static absl::Status CloseMethod(void* target) {
    return static_cast<T*>(target)->Close();
  }
  template <typename T, std::enable_if_t<!FdTargetHasClose<T>::value, int> = 0>
  static absl::Status CloseMethod(ABSL_ATTRIBUTE_UNUSED void* target) {
    return absl::OkStatus();
  }

  struct Methods {
    int (*get)(const void* target);
    bool (*is_owning)(const void* target);
    absl::Status (*close)(void* target);
  };

  template <typename T>
  static constexpr Methods kMethods = {GetMethod<T>, IsOwningMethod<T>,
                                       CloseMethod<T>};

  const Methods* methods_ = nullptr;
  void* target_ = nullptr;
};

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if !__cpp_inline_variables
template <typename T>
constexpr FdHandle::Methods FdHandle::kMethods;
#endif

}  // namespace riegeli

#endif  // RIEGELI_BYTES_FD_HANDLE_H_
