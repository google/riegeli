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

#ifndef RIEGELI_BYTES_FD_DEPENDENCY_H_
#define RIEGELI_BYTES_FD_DEPENDENCY_H_

#include <string>
#include <tuple>
#include <utility>

#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/dependency.h"

namespace riegeli {

namespace internal {

// Returns the `assumed_filename`. If `absl::nullopt`, then "/dev/stdin",
// "/dev/stdout", "/dev/stderr", or "/proc/self/fd/<fd>" is inferred from `fd`.
std::string ResolveFilename(int fd,
                            absl::optional<std::string>&& assumed_filename);

// Closes a file descriptor, taking interruption by signals into account.
//
// Return value:
//  * 0  - success
//  * -1 - failure (`errno` is set, `fd` is closed anyway)
int CloseFd(int fd);

#ifdef POSIX_CLOSE_RESTART
RIEGELI_INTERNAL_INLINE_CONSTEXPR(absl::string_view, kCloseFunctionName,
                                  "posix_close()");
#else
RIEGELI_INTERNAL_INLINE_CONSTEXPR(absl::string_view, kCloseFunctionName,
                                  "close()");
#endif

}  // namespace internal

// Owns a file descriptor (-1 means none).
class OwnedFd {
 public:
  // Creates an `OwnedFd` which does not own a fd.
  OwnedFd() noexcept {}

  // Creates an `OwnedFd` which owns `fd` if `fd >= 0`.
  /*implicit*/ OwnedFd(int fd) noexcept : fd_(fd) {}

  OwnedFd(OwnedFd&& that) noexcept;
  OwnedFd& operator=(OwnedFd&& that) noexcept;

  ~OwnedFd();

  // Returns the owned file descriptor, or -1 if none.
  int get() const { return fd_; }

  // Releases ans returns the owned file descriptor without closing it.
  int Release() { return std::exchange(fd_, -1); }

 private:
  int fd_ = -1;
};

// An unowned file descriptor is represented simply by `int`, but it is clearer
// to write `FdReader<UnownedFd>` or `FdWriter<UnownedFd>`.
using UnownedFd = int;

// Specializations of `Dependency<int, Manager>`.

template <>
class Dependency<int, OwnedFd> : public DependencyBase<OwnedFd> {
 public:
  using DependencyBase<OwnedFd>::DependencyBase;

  int get() const { return this->manager().get(); }
  int Release() { return this->manager().Release(); }

  bool is_owning() const { return get() >= 0; }
  static constexpr bool kIsStable() { return true; }
};

template <>
class Dependency<int, UnownedFd> {
 public:
  // `DependencyBase` is not used because the default value is -1, not `int()`.

  Dependency() noexcept {}

  explicit Dependency(int fd) : fd_(fd) {}

  template <typename FdArg>
  explicit Dependency(std::tuple<FdArg> fd_args)
      : fd_(std::forward<FdArg>(std::get<0>(fd_args))) {}

  Dependency(Dependency&& that) noexcept : fd_(that.fd_) {}
  Dependency& operator=(Dependency&& that) noexcept {
    fd_ = that.fd_;
    return *this;
  }

  void Reset() { fd_ = -1; }

  void Reset(int fd) { fd_ = fd; }

  template <typename FdArg>
  void Reset(std::tuple<FdArg> fd_args) {
    fd_ = std::forward<FdArg>(std::get<0>(fd_args));
  }

  int& manager() { return fd_; }
  const int& manager() const { return fd_; }

  int get() const { return fd_; }
  int Release() {
    RIEGELI_ASSERT_UNREACHABLE()
        << "Dependency<int, UnownedFd>::Release() called "
           "but is_owning() is false";
  }

  bool is_owning() const { return false; }
  static constexpr bool kIsStable() { return true; }

 private:
  int fd_ = -1;
};

// Implementation details follow.

inline OwnedFd::OwnedFd(OwnedFd&& that) noexcept
    : fd_(std::exchange(that.fd_, -1)) {}

inline OwnedFd& OwnedFd::operator=(OwnedFd&& that) noexcept {
  // Exchange `that.fd_` early to support self-assignment.
  const int fd = std::exchange(that.fd_, -1);
  if (fd_ >= 0) internal::CloseFd(fd_);
  fd_ = fd;
  return *this;
}

inline OwnedFd::~OwnedFd() {
  if (fd_ >= 0) internal::CloseFd(fd_);
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_FD_DEPENDENCY_H_
