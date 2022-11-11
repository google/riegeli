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

#include "absl/base/attributes.h"
#include "riegeli/base/dependency.h"

namespace riegeli {

// Owns a `FILE` (`nullptr` means none).
//
// `OwnedCFile` is explicitly convertible from `FILE*`.
class OwnedCFile {
 public:
  // Creates an `OwnedCFile` which does not own a file.
  OwnedCFile() = default;

  // Creates an `OwnedCFile` which owns `file` if `file != nullptr`.
  explicit OwnedCFile(FILE* file) noexcept : file_(file) {}

  OwnedCFile(OwnedCFile&& that) = default;
  OwnedCFile& operator=(OwnedCFile&& that) = default;

  ABSL_ATTRIBUTE_REINITIALIZES void Reset() { file_.reset(); }
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(FILE* file) { file_.reset(file); }

  // Returns the owned `FILE`, or `nullptr` if none.
  FILE* get() const { return file_.get(); }

  // Releases the owned `FILE` without closing it.
  FILE* Release() { return file_.release(); }

  friend bool operator==(const OwnedCFile& a, const OwnedCFile& b) {
    return a.get() == b.get();
  }
  friend bool operator!=(const OwnedCFile& a, const OwnedCFile& b) {
    return a.get() != b.get();
  }
  friend bool operator==(const OwnedCFile& a, FILE* b) { return a.get() == b; }
  friend bool operator!=(const OwnedCFile& a, FILE* b) { return a.get() != b; }
  friend bool operator==(FILE* a, const OwnedCFile& b) { return a == b.get(); }
  friend bool operator!=(FILE* a, const OwnedCFile& b) { return a != b.get(); }

 private:
  struct CFileDeleter {
    void operator()(FILE* ptr) const { fclose(ptr); }
  };

  std::unique_ptr<FILE, CFileDeleter> file_;
};

// Refers to a `FILE` but does not own it (`nullptr` means none).
//
// `UnownedCFile` is explicitly convertible from `FILE*`.
class UnownedCFile {
 public:
  // Creates an `UnownedCFile` which does not refer to a file.
  UnownedCFile() = default;

  // Creates an `UnownedCFile` which refers to `file` if `file != nullptr`.
  explicit UnownedCFile(FILE* file) noexcept : file_(file) {}

  UnownedCFile(const UnownedCFile& that) = default;
  UnownedCFile& operator=(const UnownedCFile& that) = default;

  ABSL_ATTRIBUTE_REINITIALIZES void Reset() { file_ = nullptr; }
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(FILE* file) { file_ = file; }

  // Returns the referred to `FILE`, or `nullptr` if none.
  FILE* get() const { return file_; }

  friend bool operator==(UnownedCFile a, UnownedCFile b) {
    return a.get() == b.get();
  }
  friend bool operator!=(UnownedCFile a, UnownedCFile b) {
    return a.get() != b.get();
  }
  friend bool operator==(UnownedCFile a, FILE* b) { return a.get() == b; }
  friend bool operator!=(UnownedCFile a, FILE* b) { return a.get() != b; }
  friend bool operator==(FILE* a, UnownedCFile b) { return a == b.get(); }
  friend bool operator!=(FILE* a, UnownedCFile b) { return a != b.get(); }

 private:
  FILE* file_ = nullptr;
};

// Specializations of `DependencyImpl<FILE*, Manager>`.

template <>
class DependencyImpl<FILE*, OwnedCFile> : public DependencyBase<OwnedCFile> {
 public:
  using DependencyImpl::DependencyBase::DependencyBase;

  FILE* get() const { return this->manager().get(); }
  FILE* Release() { return this->manager().Release(); }

  bool is_owning() const { return this->manager() != nullptr; }
  static constexpr bool kIsStable = true;
};

template <>
class DependencyImpl<FILE*, UnownedCFile>
    : public DependencyBase<UnownedCFile> {
 public:
  using DependencyImpl::DependencyBase::DependencyBase;

  FILE* get() const { return this->manager().get(); }
  FILE* Release() { return nullptr; }

  bool is_owning() const { return false; }
  static constexpr bool kIsStable = true;
};

}  // namespace riegeli

#endif  // RIEGELI_BYTES_CFILE_DEPENDENCY_H_
