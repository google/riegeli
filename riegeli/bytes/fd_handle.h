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

#ifdef _WIN32
#include <sys/stat.h>
#else
#include <sys/types.h>
#endif

#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
#endif

#include <cstddef>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/any.h"
#include "riegeli/base/compact_string.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/type_erased_ref.h"
#include "riegeli/base/type_traits.h"
#include "riegeli/bytes/fd_internal.h"
#include "riegeli/bytes/path_ref.h"

namespace riegeli {

namespace fd_internal {

#ifndef _WIN32
using Permissions = mode_t;
#else
using Permissions = int;
#endif

}  // namespace fd_internal

class UnownedFd;

// `SupportsFdHandle<T>::value` is `true` if `T&` is a valid constructor
// argument for `FdHandle`.

template <typename T, typename Enable = void>
struct SupportsFdHandle : std::false_type {};

template <typename T>
struct SupportsFdHandle<
    T, std::enable_if_t<std::conjunction_v<
           std::negation<std::is_const<T>>,
           std::is_convertible<decltype(std::declval<const T&>().get()), int>>>>
    : std::true_type {};

// `FdSupportsOpen<T>::value` is `true` if `T` supports `Open()` with the
// signature like in `OwnedFd`, but taking `absl::string_view filename`
// is sufficient and `permissions` can be required.

template <typename T, typename Enable = void>
struct FdSupportsOpen : std::false_type {};

template <typename T>
struct FdSupportsOpen<
    T, std::enable_if_t<std::is_convertible_v<
           decltype(std::declval<T&>().Open(
               std::declval<absl::string_view>(), std::declval<int>(),
               std::declval<fd_internal::Permissions>())),
           absl::Status>>> : std::true_type {};

// `FdSupportsOpenAt<T>::value` is `true` if `T` supports `OpenAt()` with the
// signature like in `OwnedFd` (with `permissions` present).

template <typename T, typename Enable = void>
struct FdSupportsOpenAt : std::false_type {};

template <typename T>
struct FdSupportsOpenAt<
    T, std::enable_if_t<std::is_convertible_v<
           decltype(std::declval<T&>().OpenAt(
               std::declval<UnownedFd>(), std::declval<PathRef>(),
               std::declval<int>(), std::declval<fd_internal::Permissions>())),
           absl::Status>>> : std::true_type {};

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
//   // Opens a new fd, like with `open()`, but taking
//   // `absl::string_view filename` and returning `absl::Status`.
//   //
//   // Optional. Not used by `FdHandle` itself. Used by `FdReader` and
//   // `FdWriter` constructors from the filename.
//   absl::Status Open(absl::string_view filename, int mode,
//                     OwnedFd::Permissions permissions);
//
//   // Returns the filename of the fd, or "<none>" for
//   // default-constructed or moved-from target. Unchanged by `Close()`.
//   //
//   // If `Open()` was used, this is the filename passed to `Open()`, otherwise
//   // a filename is inferred from the fd. This can be a placeholder instead of
//   // a real filename if the fd does not refer to a named file or inferring
//   // the filename is not supported.
//   //
//   // Optional. If absent, "<unsupported>" is assumed.
//   absl::string_view filename() const ABSL_ATTRIBUTE_LIFETIME_BOUND;
//
//   // If `IsOwning()`, closes the fd.
//   //
//   // If `!IsOwning()`, does nothing and returns `absl::OkStatus()`.
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
            std::enable_if_t<std::conjunction_v<NotSameRef<FdHandle, T&>,
                                                SupportsFdHandle<T>>,
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

  // Returns the filename of the fd, or "<none>" for default-constructed or
  // moved-from target. Unchanged by `Close()`.
  //
  // If `Open()` was used, this is the filename passed to `Open()`, otherwise a
  // filename is inferred from the fd. This can be a placeholder instead of a
  // real filename if the fd does not refer to a named file or inferring the
  // filename is not supported.
  //
  // If the target does not support `filename()`, returns "<unsupported>".
  absl::string_view filename() const { return methods_->filename(target_); }

  // If `IsOwning()`, closes the fd.
  //
  // If `!IsOwning()`, does nothing and returns `absl::OkStatus()`.
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
    absl::string_view (*filename)(TypeErasedRef target);
    absl::Status (*close)(TypeErasedRef target);
  };

  template <typename T, typename Enable = void>
  struct HasIsOwning : std::false_type {};
  template <typename T>
  struct HasIsOwning<T,
                     std::enable_if_t<std::is_convertible_v<
                         decltype(std::declval<const T&>().IsOwning()), bool>>>
      : std::true_type {};

  template <typename T, typename Enable = void>
  struct HasFilename : std::false_type {};
  template <typename T>
  struct HasFilename<
      T, std::enable_if_t<std::is_convertible_v<
             decltype(std::declval<const T&>().filename()), absl::string_view>>>
      : std::true_type {};

  template <typename T, typename Enable = void>
  struct HasClose : std::false_type {};
  template <typename T>
  struct HasClose<T, std::enable_if_t<std::is_convertible_v<
                         decltype(std::declval<T&>().Close()), absl::Status>>>
      : std::true_type {};

  static int GetMethodDefault(ABSL_ATTRIBUTE_UNUSED TypeErasedRef target) {
    return -1;
  }

  static bool IsOwningMethodDefault(
      ABSL_ATTRIBUTE_UNUSED TypeErasedRef target) {
    return false;
  }

  static absl::string_view FilenameMethodDefault(
      ABSL_ATTRIBUTE_UNUSED TypeErasedRef target) {
    return kDefaultFilename;
  }

  static absl::Status CloseMethodDefault(
      ABSL_ATTRIBUTE_UNUSED TypeErasedRef target) {
    return absl::OkStatus();
  }

  static constexpr Methods kMethodsDefault = {
      GetMethodDefault, IsOwningMethodDefault, FilenameMethodDefault,
      CloseMethodDefault};

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

  template <typename T, std::enable_if_t<HasFilename<T>::value, int> = 0>
  static absl::string_view FilenameMethod(TypeErasedRef target) {
    return target.Cast<const T&>().filename();
  }
  template <typename T, std::enable_if_t<!HasFilename<T>::value, int> = 0>
  static absl::string_view FilenameMethod(
      ABSL_ATTRIBUTE_UNUSED TypeErasedRef target) {
    return "<unsupported>";
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
                                       FilenameMethod<T>, CloseMethod<T>};

  const Methods* methods_ = &kMethodsDefault;
  TypeErasedRef target_;
};

namespace fd_internal {

class UnownedFdDeleter;

// Common parts of `UnownedFdDeleter` and `OwnedFdDeleter`.
class
#ifdef ABSL_ATTRIBUTE_TRIVIAL_ABI
    ABSL_ATTRIBUTE_TRIVIAL_ABI
#endif
    FdDeleterBase {
 public:
  FdDeleterBase() = default;

  explicit FdDeleterBase(CompactString filename)
      : filename_(std::move(filename)) {}

  // Supports creating a `FdBase` converted from `UnownedFd`.
  explicit FdDeleterBase(const UnownedFdDeleter& that);
  explicit FdDeleterBase(UnownedFdDeleter&& that);

  // Supports creating a `FdBase` converted from `UnownedFd`.
  void Reset(const UnownedFdDeleter& that);

  // Supports creating a `FdBase` converted from `UnownedFd`, and resetting
  // `FdBase` from the same `FdBase`.
  void Reset(FdDeleterBase&& that);

  absl::string_view filename() const { return filename_; }

  CompactString&& ReleaseFilename() { return std::move(filename_); }

  void set_filename(CompactString filename) { filename_ = std::move(filename); }

  const char* c_filename() { return filename_.c_str(); }

 protected:
  FdDeleterBase(const FdDeleterBase& that) = default;
  FdDeleterBase& operator=(const FdDeleterBase& that) = default;

  FdDeleterBase(FdDeleterBase&& that) noexcept
      : filename_(
            std::exchange(that.filename_, CompactString(kDefaultFilename))) {}
  FdDeleterBase& operator=(FdDeleterBase&& that) noexcept {
    filename_ = std::exchange(that.filename_, CompactString(kDefaultFilename));
    return *this;
  }

 private:
  CompactString filename_{kDefaultFilename};
};

class UnownedFdDeleter : public FdDeleterBase {
 public:
  using FdDeleterBase::FdDeleterBase;

  // Supports creating an `UnownedFd` converted from any `FdBase`.
  explicit UnownedFdDeleter(const FdDeleterBase& that) : FdDeleterBase(that) {}

  UnownedFdDeleter(const UnownedFdDeleter& that) = default;
  UnownedFdDeleter& operator=(const UnownedFdDeleter& that) = default;

  UnownedFdDeleter(UnownedFdDeleter&& that) = default;
  UnownedFdDeleter& operator=(UnownedFdDeleter&& that) = default;

  // Supports creating an `UnownedFd` converted from any `FdBase`.
  void Reset(const FdDeleterBase& that) { FdDeleterBase::operator=(that); }

  static void Destroy(ABSL_ATTRIBUTE_UNUSED int fd) {}
};

class OwnedFdDeleter : public FdDeleterBase {
 public:
  using FdDeleterBase::FdDeleterBase;

  OwnedFdDeleter(OwnedFdDeleter&& that) = default;
  OwnedFdDeleter& operator=(OwnedFdDeleter&& that) = default;

  static void Destroy(int fd) {
#ifndef _WIN32
    // http://austingroupbugs.net/view.php?id=529 explains this mess.
#ifdef POSIX_CLOSE_RESTART
    // Avoid `EINTR` by using `posix_close(_, 0)` if available.
    posix_close(fd, 0);
#else   // !POSIX_CLOSE_RESTART
    close(fd);
#endif  // !POSIX_CLOSE_RESTART
#else   // _WIN32
    _close(fd);
#endif  // _WIN32
  }
};

inline FdDeleterBase::FdDeleterBase(const UnownedFdDeleter& that)
    : filename_(that.filename_) {}

inline FdDeleterBase::FdDeleterBase(UnownedFdDeleter&& that)
    : filename_(
          std::exchange(that.filename_, CompactString(kDefaultFilename))) {}

inline void FdDeleterBase::Reset(const UnownedFdDeleter& that) {
  filename_ = that.filename_;
}

inline void FdDeleterBase::Reset(FdDeleterBase&& that) {
  filename_ = std::exchange(that.filename_, CompactString(kDefaultFilename));
}

// Common parts of `UnownedFd` and `OwnedFd`.
template <typename Deleter>
class
#ifdef ABSL_ATTRIBUTE_TRIVIAL_ABI
    ABSL_ATTRIBUTE_TRIVIAL_ABI
#endif
    FdBase {
 public:
  // Creates an `FdBase` which does not store a fd and stores "<none>" as the
  // filename.
  FdBase() = default;
  /*implicit*/ FdBase(std::nullptr_t) {}

  // Creates an `FdBase` which stores `fd` with the filename inferred from the
  // fd (or "<none>" if `fd < 0`).
  explicit FdBase(int fd ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : fd_(fd),
        deleter_(fd_ < 0 ? CompactString(kDefaultFilename)
                         : FilenameForFd(fd_)) {}

  // Creates an `FdBase` which stores `fd` with `filename`.
  explicit FdBase(int fd ABSL_ATTRIBUTE_LIFETIME_BOUND, PathRef filename)
      : FdBase(fd, CompactString::ForCStr(filename)) {}
  explicit FdBase(int fd ABSL_ATTRIBUTE_LIFETIME_BOUND, CompactString filename)
      : fd_(fd), deleter_(std::move(filename)) {}

  // Creates a `FdBase` converted from `UnownedFd`.
  template <typename DependentDeleter = Deleter,
            std::enable_if_t<
                !std::is_same_v<DependentDeleter, UnownedFdDeleter>, int> = 0>
  explicit FdBase(const FdBase<UnownedFdDeleter>& that)
      : fd_(that.fd_), deleter_(that.deleter_) {}
  template <typename DependentDeleter = Deleter,
            std::enable_if_t<
                !std::is_same_v<DependentDeleter, UnownedFdDeleter>, int> = 0>
  explicit FdBase(FdBase<UnownedFdDeleter>&& that)
      : fd_(that.Release()), deleter_(std::move(that.deleter_)) {}

  // Creates an `UnownedFd` converted from any `FdBase`.
  template <
      typename OtherDeleter,
      std::enable_if_t<
          std::conjunction_v<
              std::is_same<Deleter, UnownedFdDeleter>,
              std::negation<std::is_same<OtherDeleter, UnownedFdDeleter>>>,
          int> = 0>
  /*implicit*/ FdBase(
      const FdBase<OtherDeleter>& that ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : fd_(that.fd_), deleter_(that.deleter_) {}

  // Makes `*this` equivalent to a newly constructed `FdBase`.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(std::nullptr_t = nullptr) {
    SetFdKeepFilename();
    deleter_.set_filename(CompactString(kDefaultFilename));
  }
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(int fd) {
    SetFdKeepFilename(fd);
    deleter_.set_filename(fd < 0 ? CompactString(kDefaultFilename)
                                 : FilenameForFd(fd));
  }
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(int fd, PathRef filename) {
    Reset(fd, CompactString::ForCStr(filename));
  }
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(int fd, CompactString filename) {
    SetFdKeepFilename(fd);
    deleter_.set_filename(std::move(filename));
  }
  template <typename OtherDeleter,
            std::enable_if_t<std::disjunction_v<
                                 std::is_same<Deleter, UnownedFdDeleter>,
                                 std::is_same<OtherDeleter, UnownedFdDeleter>>,
                             int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(const FdBase<OtherDeleter>& that) {
    SetFdKeepFilename(that.fd_);
    deleter_.Reset(that.deleter_);
  }
  template <typename OtherDeleter,
            std::enable_if_t<
                std::disjunction_v<std::is_same<OtherDeleter, UnownedFdDeleter>,
                                   std::is_same<OtherDeleter, Deleter>>,
                int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(FdBase<OtherDeleter>&& that) {
    SetFdKeepFilename(that.Release());
    deleter_.Reset(std::move(that.deleter_));
  }

  // Sets the fd, keeping `filename()` unchanged.
  void SetFdKeepFilename(int fd = -1) {
    Destroy();
    fd_ = fd;
  }

  // Returns `true` if the fd is present.
  bool is_open() const { return fd_ >= 0; }

  // Returns the fd.
  int get() const { return fd_; }

  // Returns the filename of the fd, or "<none>" for default-constructed or
  // moved-from `FdBase`. Unchanged by `Close()` and `Release()`.
  //
  // If `Open()` was used, this is the filename passed to `Open()`, otherwise
  // a filename is inferred from the fd. This can be a placeholder instead of
  // a real filename if the fd does not refer to a named file or inferring the
  // filename is not supported.
  absl::string_view filename() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return deleter_.filename();
  }

  // Returns `filename()` as a NUL-terminated string.
  const char* c_filename() ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return deleter_.c_filename();
  }

 protected:
  FdBase(const FdBase& that) = default;
  FdBase& operator=(const FdBase& that) = default;

  FdBase(FdBase&& that) noexcept
      : fd_(that.Release()), deleter_(std::move(that.deleter_)) {}
  FdBase& operator=(FdBase&& that) noexcept {
    const int fd = that.Release();
    Destroy();
    fd_ = fd;
    deleter_ = std::move(that.deleter_);
    return *this;
  }

  ~FdBase() { Destroy(); }

  // Returns the fd. The stored fd is left absent, without modifying
  // `filename()`.
  int Release() { return std::exchange(fd_, -1); }

 private:
  template <typename OtherDeleter>
  friend class FdBase;  // For conversions.

  void Destroy() {
    if (is_open()) deleter_.Destroy(fd_);
  }

  int fd_ = -1;
  Deleter deleter_;
};

extern template class FdBase<UnownedFdDeleter>;
extern template class FdBase<OwnedFdDeleter>;

}  // namespace fd_internal

// Stores a file descriptor but does not own it, i.e. is not responsible for
// closing it.
//
// The fd can be negative which means absent.
class
#ifdef ABSL_NULLABILITY_COMPATIBLE
    ABSL_NULLABILITY_COMPATIBLE
#endif
    UnownedFd : public fd_internal::FdBase<fd_internal::UnownedFdDeleter>,
                public WithEqual<UnownedFd> {
 public:
  using FdBase::FdBase;

  // Overridden to make implicit.
  /*implicit*/ UnownedFd(int fd ABSL_ATTRIBUTE_LIFETIME_BOUND) : FdBase(fd) {}

  // Creates an `UnownedFd` which stores `fd.get()` with `fd.filename()`.
  explicit UnownedFd(FdHandle fd) : FdBase(fd.get(), fd.filename()) {}

  UnownedFd(const UnownedFd& that) = default;
  UnownedFd& operator=(const UnownedFd& that) = default;

  // The moved-from fd is left absent.
  UnownedFd(UnownedFd&& that) = default;
  UnownedFd& operator=(UnownedFd&& that) = default;

  using FdBase::Reset;
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(FdHandle fd) {
    Reset(fd.get(), fd.filename());
  }

  friend bool operator==(const UnownedFd& a, const UnownedFd& b) {
    return a.get() == b.get();
  }
  friend bool operator==(const UnownedFd& a, int b) { return a.get() == b; }
  friend bool operator==(const UnownedFd& a, std::nullptr_t) {
    return a.get() < 0;
  }
};

// Owns a file descriptor, i.e. stores it and is responsible for closing it.
//
// The fd can be negative which means absent.
class
#ifdef ABSL_NULLABILITY_COMPATIBLE
    ABSL_NULLABILITY_COMPATIBLE
#endif
    OwnedFd : public fd_internal::FdBase<fd_internal::OwnedFdDeleter>,
              public WithEqual<OwnedFd> {
 public:
  using Permissions = fd_internal::Permissions;
#ifndef _WIN32
  static constexpr Permissions kDefaultPermissions = 0666;
#else
  static constexpr Permissions kDefaultPermissions = _S_IREAD | _S_IWRITE;
#endif

  using FdBase::FdBase;

  // The moved-from fd is left absent.
  OwnedFd(OwnedFd&& that) = default;
  OwnedFd& operator=(OwnedFd&& that) = default;

  // Overridden to apply `ABSL_ATTRIBUTE_LIFETIME_BOUND`.
  int get() const ABSL_ATTRIBUTE_LIFETIME_BOUND { return FdBase::get(); }

  using FdBase::Release;

  // Opens a new fd, like with `open()`, but taking `PathRef filename` and
  // returning `absl::Status`.
  ABSL_ATTRIBUTE_REINITIALIZES absl::Status Open(
      PathRef filename, int mode,
      Permissions permissions = kDefaultPermissions) {
    return Open(CompactString::ForCStr(filename), mode, permissions);
  }
  ABSL_ATTRIBUTE_REINITIALIZES absl::Status Open(
      CompactString filename, int mode,
      Permissions permissions = kDefaultPermissions);

#ifndef _WIN32
  // Opens a new fd with the filename interpreted relatively to the directory
  // specified by an existing fd, like with `openat()`, but taking
  // `PathRef filename` and returning `absl::Status`.
  ABSL_ATTRIBUTE_REINITIALIZES absl::Status OpenAt(
      UnownedFd dir_fd, PathRef filename, int mode,
      Permissions permissions = kDefaultPermissions);
#endif  // !_WIN32

  // Closes the fd if present.
  //
  // Returns `absl::OkStatus()` if absent.
  absl::Status Close();

  friend bool operator==(const OwnedFd& a, int b) { return a.get() == b; }
  friend bool operator==(const OwnedFd& a, std::nullptr_t) {
    return a.get() < 0;
  }
};

// Type-erased object like `UnownedFd` or `OownedFd` which stores and possibly
// owns a fd.
using AnyFd = Any<FdHandle>::Inlining<UnownedFd, OwnedFd>;

}  // namespace riegeli

#endif  // RIEGELI_BYTES_FD_HANDLE_H_
