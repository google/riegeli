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

#include <utility>

#include "absl/base/attributes.h"
#include "riegeli/base/dependency.h"
#include "riegeli/bytes/fd_internal.h"

namespace riegeli {

// Owns a file descriptor (-1 means none).
//
// `OwnedFd` is explicitly convertible from `int`.
class
#ifdef ABSL_ATTRIBUTE_TRIVIAL_ABI
    ABSL_ATTRIBUTE_TRIVIAL_ABI
#endif
        OwnedFd {
 public:
  // Creates an `OwnedFd` which does not own a fd.
  OwnedFd() = default;

  // Creates an `OwnedFd` which owns `fd` if `fd >= 0`.
  explicit OwnedFd(int fd) noexcept : fd_(fd) {}

  OwnedFd(OwnedFd&& that) noexcept;
  OwnedFd& operator=(OwnedFd&& that) noexcept;

  ~OwnedFd();

  ABSL_ATTRIBUTE_REINITIALIZES void Reset();
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(int fd);

  // Returns the owned file descriptor, or -1 if none.
  int get() const { return fd_; }

  // Releases ans returns the owned file descriptor without closing it.
  int Release() { return std::exchange(fd_, -1); }

 private:
  int fd_ = -1;
};

// Refers to a file descriptor but does not own it (a negative value means
// none).
//
// `UnownedFd` is explicitly convertible from `int`.
class UnownedFd {
 public:
  // Creates an `UnownedFd` which does not refer to a fd.
  UnownedFd() = default;

  // Creates an `UnownedFd` which refers to `fd` if `fd >= 0`.
  explicit UnownedFd(int fd) noexcept : fd_(fd) {}

  UnownedFd(const UnownedFd& that) = default;
  UnownedFd& operator=(const UnownedFd& that) = default;

  ABSL_ATTRIBUTE_REINITIALIZES void Reset() { fd_ = -1; }
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(int fd) { fd_ = fd; }

  // Returns the owned file descriptor, or a negative value if none.
  int get() const { return fd_; }

 private:
  int fd_ = -1;
};

// Specializations of `DependencyImpl<int, Manager>`.

template <>
class DependencyImpl<int, OwnedFd> : public DependencyBase<OwnedFd> {
 public:
  using DependencyBase<OwnedFd>::DependencyBase;

  int get() const { return this->manager().get(); }
  int Release() { return this->manager().Release(); }

  bool is_owning() const { return get() >= 0; }
  static constexpr bool kIsStable = true;
};

template <>
class DependencyImpl<int, UnownedFd> : public DependencyBase<UnownedFd> {
 public:
  using DependencyBase<UnownedFd>::DependencyBase;

  int get() const { return this->manager().get(); }
  int Release() { return -1; }

  bool is_owning() const { return false; }
  static constexpr bool kIsStable = true;
};

// Implementation details follow.

inline OwnedFd::OwnedFd(OwnedFd&& that) noexcept
    : fd_(std::exchange(that.fd_, -1)) {}

inline OwnedFd& OwnedFd::operator=(OwnedFd&& that) noexcept {
  // Exchange `that.fd_` early to support self-assignment.
  const int fd = std::exchange(that.fd_, -1);
  if (fd_ >= 0) fd_internal::Close(fd_);
  fd_ = fd;
  return *this;
}

inline OwnedFd::~OwnedFd() {
  if (fd_ >= 0) fd_internal::Close(fd_);
}

inline void OwnedFd::Reset() {
  if (fd_ >= 0) fd_internal::Close(fd_);
  fd_ = -1;
}

inline void OwnedFd::Reset(int fd) {
  if (fd_ >= 0) fd_internal::Close(fd_);
  fd_ = fd;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_FD_DEPENDENCY_H_
