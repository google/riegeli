// Copyright 2022 Google LLC
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

#ifndef RIEGELI_BYTES_CFILE_DEPENDENCY_H_
#define RIEGELI_BYTES_CFILE_DEPENDENCY_H_

#include <stdio.h>

#include <memory>
#include <utility>

#include "riegeli/base/dependency.h"

namespace riegeli {

// Owns a `FILE` (`nullptr` means none).
//
// `OwnedCFile` is implicitly convertible from `FILE*`.
class OwnedCFile {
 public:
  // Creates an `OwnedCFile` which does not own a file.
  OwnedCFile() noexcept {}

  // Creates an `OwnedCFile` which owns `file` if `file != nullptr`.
  /*implicit*/ OwnedCFile(FILE* file) noexcept : file_(file) {}

  OwnedCFile(OwnedCFile&& that) noexcept;
  OwnedCFile& operator=(OwnedCFile&& that) noexcept;

  // Returns the owned `FILE`, or `nullptr` if none.
  FILE* get() const { return file_.get(); }

  // Releases the owned `FILE` without closing it.
  FILE* Release() { return file_.release(); }

 private:
  struct CFileDeleter {
    void operator()(FILE* ptr) const { fclose(ptr); }
  };

  std::unique_ptr<FILE, CFileDeleter> file_;
};

// Refers to a `FILE` but does not own it (`nullptr` means none).
//
// `UnownedCFile` is implicitly convertible from `FILE*`.
class UnownedCFile {
 public:
  // Creates an `UnownedCFile` which does not refer to a file.
  UnownedCFile() noexcept {}

  // Creates an `UnownedCFile` which refers to `file` if `file != nullptr`.
  /*implicit*/ UnownedCFile(FILE* file) noexcept : file_(file) {}

  UnownedCFile(const UnownedCFile& that) noexcept = default;
  UnownedCFile& operator=(const UnownedCFile& that) noexcept = default;

  // Returns the referred to `FILE`, or `nullptr` if none.
  FILE* get() const { return file_; }

 private:
  FILE* file_ = nullptr;
};

// Specializations of `Dependency<FILE*, Manager>`.

template <>
class Dependency<FILE*, OwnedCFile> : public DependencyBase<OwnedCFile> {
 public:
  using DependencyBase<OwnedCFile>::DependencyBase;

  FILE* get() const { return this->manager().get(); }
  FILE* Release() { return this->manager().Release(); }

  bool is_owning() const { return get() != nullptr; }
  static constexpr bool kIsStable() { return true; }
};

template <>
class Dependency<FILE*, UnownedCFile> : public DependencyBase<UnownedCFile> {
 public:
  using DependencyBase<UnownedCFile>::DependencyBase;

  FILE* get() const { return this->manager().get(); }
  FILE* Release() { return nullptr; }

  bool is_owning() const { return false; }
  static constexpr bool kIsStable() { return true; }
};

// Implementation details follow.

inline OwnedCFile::OwnedCFile(OwnedCFile&& that) noexcept
    : file_(std::move(that.file_)) {}

inline OwnedCFile& OwnedCFile::operator=(OwnedCFile&& that) noexcept {
  file_ = std::move(that.file_);
  return *this;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_CFILE_DEPENDENCY_H_
