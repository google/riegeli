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
#include <tuple>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "absl/utility/utility.h"
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
  /*implicit*/ OwnedFd(int fd) noexcept : fd_(fd) {}

  OwnedFd(OwnedFd&& that) noexcept;
  OwnedFd& operator=(OwnedFd&& that) noexcept;

  ~OwnedFd() {
    if (fd_ >= 0) internal::CloseFd(fd_);
  }

  // Returns the owned file descriptor, or -1 if none.
  int get() const { return fd_; }

  // Releases ans returns the owned file descriptor without closing it.
  int Release() { return absl::exchange(fd_, -1); }

 private:
  int fd_ = -1;
};

// Specializations of Dependency<int, Manager>.

template <>
class Dependency<int, OwnedFd> : public DependencyBase<OwnedFd> {
 public:
  using DependencyBase<OwnedFd>::DependencyBase;

  int get() const { return this->manager().get(); }
  int Release() { return this->manager().Release(); }

  bool is_owning() const { return true; }
  static constexpr bool kIsStable() { return true; }
};

template <>
class Dependency<int, int> {
 public:
  // DependencyBase is not used because the default value is -1, not int().

  Dependency() noexcept {}

  explicit Dependency(int fd) : fd_(fd) {}

  template <typename FdArg>
  explicit Dependency(std::tuple<FdArg> fd_args)
      : fd_(std::get<0>(std::move(fd_args))) {}

  Dependency(Dependency&& that) noexcept : fd_(absl::exchange(that.fd_, -1)) {}
  Dependency& operator=(Dependency&& that) noexcept {
    fd_ = absl::exchange(that.fd_, -1);
    return *this;
  }

  void Reset() { fd_ = -1; }

  void Reset(int fd) { fd_ = fd; }

  template <typename FdArg>
  void Reset(std::tuple<FdArg> fd_args) {
    fd_ = std::get<0>(std::move(fd_args));
  }

  int& manager() { return fd_; }
  const int& manager() const { return fd_; }

  int get() const { return fd_; }
  int Release() {
    RIEGELI_ASSERT_UNREACHABLE() << "Dependency<int, int>::Release() called "
                                    "but is_owning() is false";
  }

  bool is_owning() const { return false; }
  static constexpr bool kIsStable() { return true; }

 private:
  int fd_ = -1;
};

// Implementation details follow.

namespace internal {

inline int CloseFd(int fd) {
  // http://austingroupbugs.net/view.php?id=529 explains this mess.
#ifdef POSIX_CLOSE_RESTART
  // Avoid EINTR by using posix_close(_, 0) if available.
  if (ABSL_PREDICT_FALSE(posix_close(fd, 0) < 0)) {
    if (errno == EINPROGRESS) return 0;
    return -1;
  }
#else
  if (ABSL_PREDICT_FALSE(close(fd) < 0)) {
    // After EINTR it is unspecified whether fd has been closed or not.
    // Assume that it is closed, which is the case e.g. on Linux.
    if (errno == EINPROGRESS || errno == EINTR) return 0;
    return -1;
  }
#endif
  return 0;
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
    : fd_(absl::exchange(that.fd_, -1)) {}

inline OwnedFd& OwnedFd::operator=(OwnedFd&& that) noexcept {
  // Exchange that.fd_ early to support self-assignment.
  const int fd = absl::exchange(that.fd_, -1);
  if (fd_ >= 0) internal::CloseFd(fd_);
  fd_ = fd;
  return *this;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_FD_DEPENDENCY_H_
