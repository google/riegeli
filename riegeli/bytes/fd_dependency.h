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

#include <unistd.h>
#include <cerrno>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/dependency.h"

namespace riegeli {

namespace internal {

// Return value:
//  * 0     - success
//  * errno - failure (fd is closed anyway)
int CloseFd(int fd);

absl::string_view CloseFunctionName();

}  // namespace internal

// Owns a file descriptor (-1 means none).
class OwnedFd {
 public:
  // Creates an OwnedFd which does not own a fd.
  OwnedFd() noexcept {}

  // Creates an OwnedFd which owns fd if fd >= 0.
  OwnedFd(int fd) noexcept : fd_(fd) {}

  OwnedFd(OwnedFd&& that) noexcept;
  OwnedFd& operator=(OwnedFd&& that) noexcept;

  ~OwnedFd() {
    if (fd_ >= 0) internal::CloseFd(fd_);
  }

  // Returns the owned file descriptor, or -1 if none.
  int fd() const { return fd_; }

  // Releases ans returns the owned file descriptor without closing it.
  int Release() { return riegeli::exchange(fd_, -1); }

 private:
  int fd_ = -1;
};

// Specializations of Dependency<int, Manager>.

template <>
class Dependency<int, OwnedFd> : public DependencyBase<OwnedFd> {
 public:
  using DependencyBase<OwnedFd>::DependencyBase;

  int ptr() const { return this->manager().fd(); }
  int Release() { return this->manager().Release(); }

  static constexpr bool kIsOwning() { return true; }
  static constexpr bool kIsStable() { return true; }
};

template <>
class Dependency<int, int> {
 public:
  // DependencyBase is not used because the default value is -1, not int().

  Dependency() noexcept {}

  explicit Dependency(int fd) : fd_(fd) {}

  Dependency(Dependency&& that) noexcept
      : fd_(riegeli::exchange(that.fd_, -1)) {}
  Dependency& operator=(Dependency&& that) noexcept {
    fd_ = riegeli::exchange(that.fd_, -1);
    return *this;
  }

  int& manager() { return fd_; }
  const int& manager() const { return fd_; }

  int ptr() const { return fd_; }
  int Release() { return riegeli::exchange(fd_, -1); }

  static constexpr bool kIsOwning() { return false; }
  static constexpr bool kIsStable() { return true; }

 private:
  int fd_ = -1;
};

// Implementation details follow.

namespace internal {

inline int CloseFd(int fd) {
  int error_code = 0;
  // http://austingroupbugs.net/view.php?id=529 explains this mess.
#ifdef POSIX_CLOSE_RESTART
  // Avoid EINTR by using posix_close(_, 0) if available.
  if (ABSL_PREDICT_FALSE(posix_close(fd, 0) < 0)) {
    error_code = errno;
    if (error_code == EINPROGRESS) error_code = 0;
  }
#else
  if (ABSL_PREDICT_FALSE(close(fd) < 0)) {
    error_code = errno;
    // After EINTR it is unspecified whether fd has been closed or not.
    // Assume that it is closed, which is the case e.g. on Linux.
    if (error_code == EINPROGRESS || error_code == EINTR) error_code = 0;
  }
#endif
  return error_code;
}

inline absl::string_view CloseFunctionName() {
#ifdef POSIX_CLOSE_RESTART
  return "posix_close()";
#else
  return "close()";
#endif
}

}  // namespace internal

inline OwnedFd::OwnedFd(OwnedFd&& that) noexcept
    : fd_(riegeli::exchange(that.fd_, -1)) {}

inline OwnedFd& OwnedFd::operator=(OwnedFd&& that) noexcept {
  // Exchange that.fd_ early to support self-assignment.
  const int fd = riegeli::exchange(that.fd_, -1);
  if (fd_ >= 0) internal::CloseFd(fd_);
  fd_ = fd;
  return *this;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_FD_DEPENDENCY_H_
