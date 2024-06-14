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

#ifndef RIEGELI_BYTES_CFILE_HANDLE_H_
#define RIEGELI_BYTES_CFILE_HANDLE_H_

#include <stdio.h>

#include <cstddef>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/meta/type_traits.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/any_dependency.h"
#include "riegeli/base/compare.h"

namespace riegeli {

// Owns a `FILE*`, i.e. stores it and is responsible for closing it.
//
// The `FILE*` can be `nullptr` which means absent.
class OwnedCFile : public WithEqual<OwnedCFile> {
 public:
  // Creates an `OwnedCFile` which does not own a file.
  OwnedCFile() = default;

  // Creates an `OwnedCFile` which owns `file`.
  explicit OwnedCFile(FILE* file) noexcept : file_(file) {}

  // The moved-from `FILE*` is left absent.
  OwnedCFile(OwnedCFile&& that) = default;
  OwnedCFile& operator=(OwnedCFile&& that) = default;

  ABSL_ATTRIBUTE_REINITIALIZES void Reset(FILE* file = nullptr) {
    file_.reset(file);
  }

  // Returns `true` if the `FILE*` is present.
  bool is_open() const { return file_ != nullptr; }

  // Returns the `FILE*`.
  FILE* get() const { return file_.get(); }

  // Releases and returns the `FILE*` without closing it.
  FILE* Release() { return file_.release(); }

  friend bool operator==(const OwnedCFile& a, FILE* b) { return a.get() == b; }

  // Allow Nullability annotations on `OwnedCFile`.
  using absl_nullability_compatible = void;

  // Opens a new `FILE*`, like with `fopen()` but taking
  // `absl::string_view filename`, `absl::string_view mode`, and returning
  // `absl::Status`.
#ifndef _WIN32
  ABSL_ATTRIBUTE_REINITIALIZES absl::Status Open(absl::string_view filename,
                                                 absl::string_view mode) {
    return Open(std::string(filename).c_str(), std::string(mode).c_str());
  }
  ABSL_ATTRIBUTE_REINITIALIZES absl::Status Open(absl::string_view filename,
                                                 const std::string& mode) {
    return Open(std::string(filename).c_str(), mode.c_str());
  }
  ABSL_ATTRIBUTE_REINITIALIZES absl::Status Open(absl::string_view filename,
                                                 const char* mode) {
    return Open(std::string(filename).c_str(), mode);
  }
  ABSL_ATTRIBUTE_REINITIALIZES absl::Status Open(const std::string& filename,
                                                 absl::string_view mode) {
    return Open(filename.c_str(), std::string(mode).c_str());
  }
  ABSL_ATTRIBUTE_REINITIALIZES absl::Status Open(const std::string& filename,
                                                 const std::string& mode) {
    return Open(filename.c_str(), mode.c_str());
  }
  ABSL_ATTRIBUTE_REINITIALIZES absl::Status Open(const std::string& filename,
                                                 const char* mode) {
    return Open(filename.c_str(), mode);
  }
  ABSL_ATTRIBUTE_REINITIALIZES absl::Status Open(const char* filename,
                                                 absl::string_view mode) {
    return Open(filename, std::string(mode).c_str());
  }
  ABSL_ATTRIBUTE_REINITIALIZES absl::Status Open(const char* filename,
                                                 const std::string& mode) {
    return Open(filename, mode.c_str());
  }
  ABSL_ATTRIBUTE_REINITIALIZES absl::Status Open(const char* filename,
                                                 const char* mode);
#else   // _WIN32
  ABSL_ATTRIBUTE_REINITIALIZES absl::Status Open(absl::string_view filename,
                                                 absl::string_view mode);
#endif  // _WIN32

  // Closes the `FILE*` if present.
  //
  // Returns `absl::OkStatus()` if absent.
  absl::Status Close();

 private:
  struct CFileDeleter {
    void operator()(FILE* ptr) const { fclose(ptr); }
  };

  std::unique_ptr<FILE, CFileDeleter> file_;
};

// Stores a `FILE*` but does not own it, i.e. is not responsible for closing it.
//
// The `FILE*` can be `nullptr` which means absent.
class UnownedCFile : public WithEqual<UnownedCFile> {
 public:
  // Creates an `UnownedCFile` which does not store a file.
  UnownedCFile() = default;

  // Creates an `UnownedCFile` which stores `file`.
  explicit UnownedCFile(FILE* file) noexcept : file_(file) {}

  UnownedCFile(const UnownedCFile& that) = default;
  UnownedCFile& operator=(const UnownedCFile& that) = default;

  ABSL_ATTRIBUTE_REINITIALIZES void Reset(FILE* file = nullptr) {
    file_ = file;
  }

  // Returns `true` if the `FILE*` is present.
  bool is_open() const { return file_ != nullptr; }

  // Returns the `FILE*`.
  FILE* get() const { return file_; }

  friend bool operator==(UnownedCFile a, UnownedCFile b) {
    return a.get() == b.get();
  }
  friend bool operator==(UnownedCFile a, FILE* b) { return a.get() == b; }

  // Allow Nullability annotations on `UnownedCFile`.
  using absl_nullability_compatible = void;

 private:
  FILE* file_ = nullptr;
};

// `IsValidCFileTarget<T>::value` is `true` if `T*` is a valid constructor
// argument for `CFileHandle`.

template <typename T, typename Enable = void>
struct IsValidCFileTarget : std::false_type {};

template <typename T>
struct IsValidCFileTarget<
    T, std::enable_if_t<absl::conjunction<
           absl::negation<std::is_const<T>>,
           std::is_convertible<decltype(std::declval<const T&>().get()),
                               FILE*>>::value>> : std::true_type {};

// `CFileTargetHasOpen<T>::value` is `true` if `T` supports `Open()` with the
// signature like in `OwnedCFile`.

template <typename T, typename Enable = void>
struct CFileTargetHasOpen : std::false_type {};

template <typename T>
struct CFileTargetHasOpen<
    T, std::enable_if_t<std::is_convertible<
           decltype(std::declval<T&>().Open(std::declval<absl::string_view>(),
                                            std::declval<absl::string_view>())),
           absl::Status>::value>> : std::true_type {};

// Type-erased pointer to a target object like `OwnedCFile` or `UnownedCFile`
// which stores and possibly owns a `FILE*`.
//
// The target should support:
//
// ```
//   // Returns the `FILE*`.
//   FILE* get() const;
//
//   // Returns `true` if the target owns the `FILE*`, i.e. is responsible for
//   // closing it and the `FILE*` is present.
//   //
//   // Optional. If absent, the presence of `Close()` determines whether the
//   // target is considered to own the `FILE*`.
//   bool IsOwning() const;
//
//   // Opens a new `FILE*`, like with `fopen()` but returning `absl::Status`.
//   //
//   // Optional. Used by `CFileReader` and `CFileWriter` constructors from the
//   // filename.
//   absl::Status Open(const char* filename, const char* mode);
//
//   // Closes the `FILE*` if `IsOwning()`.
//   //
//   // Returns `absl::OkStatus()` if `!IsOwning()`.
//   //
//   // Optional. If absent, `absl::OkStatus()` is assumed.
//   absl::Status Close();
// ```
class CFileHandle : public WithEqual<CFileHandle> {
 public:
  // Creates a `CFileHandle` which does not point to a target.
  CFileHandle() = default;
  /*implicit*/ CFileHandle(std::nullptr_t) noexcept {}

  // Creates a `CFileHandle` which points to `target`.
  template <typename T, std::enable_if_t<IsValidCFileTarget<T>::value, int> = 0>
  explicit CFileHandle(T* target) : methods_(&kMethods<T>), target_(target) {}

  CFileHandle(const CFileHandle& that) = default;
  CFileHandle& operator=(const CFileHandle& that) = default;

  // Returns `true` if the `FILE*` is present.
  bool is_open() const { return **this != nullptr; }

  // Returns the `FILE*`.
  FILE* operator*() const { return methods_->get(target_); }

  friend bool operator==(CFileHandle a, CFileHandle b) {
    return a.target_ == b.target_;
  }

  // Allow Nullability annotations on `CFileHandle`.
  using absl_nullability_compatible = void;

  // Returns `true` if the `CFileHandle` owns the `FILE*`, i.e. is responsible
  // for closing it and the `FILE*` is present.
  bool IsOwning() const { return methods_->is_owning(target_); }

  // Closes the `FILE*` if `IsOwning()`.
  //
  // Returns `absl::OkStatus()` if `!IsOwning()`.
  absl::Status Close() { return methods_->close(target_); }

 private:
  template <typename T, typename Enable = void>
  struct CFileTargetHasClose : std::false_type {};
  template <typename T>
  struct CFileTargetHasClose<
      T, std::enable_if_t<std::is_convertible<
             decltype(std::declval<T&>().Close()), absl::Status>::value>>
      : std::true_type {};

  template <typename T, typename Enable = void>
  struct CFileTargetHasIsOwning : std::false_type {};
  template <typename T>
  struct CFileTargetHasIsOwning<
      T, std::enable_if_t<std::is_convertible<
             decltype(std::declval<const T&>().IsOwning()), bool>::value>>
      : std::true_type {};

  template <typename T>
  static FILE* GetMethod(const void* target) {
    return static_cast<const T*>(target)->get();
  }

  template <typename T,
            std::enable_if_t<CFileTargetHasIsOwning<T>::value, int> = 0>
  static bool IsOwningMethod(const void* target) {
    return static_cast<const T*>(target)->IsOwning();
  }
  template <typename T,
            std::enable_if_t<!CFileTargetHasIsOwning<T>::value, int> = 0>
  static bool IsOwningMethod(const void* target) {
    return CFileTargetHasClose<T>::value &&
           static_cast<const T*>(target)->get() != nullptr;
  }

  template <typename T,
            std::enable_if_t<CFileTargetHasClose<T>::value, int> = 0>
  static absl::Status CloseMethod(void* target) {
    return static_cast<T*>(target)->Close();
  }
  template <typename T,
            std::enable_if_t<!CFileTargetHasClose<T>::value, int> = 0>
  static absl::Status CloseMethod(ABSL_ATTRIBUTE_UNUSED void* target) {
    return absl::OkStatus();
  }

  struct Methods {
    FILE* (*get)(const void* target);
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
constexpr CFileHandle::Methods CFileHandle::kMethods;
#endif

// Type-erased object like `OwnedCFile` or `UnownedCFile` which stores and
// possibly owns a `FILE*`.
using AnyCFile = AnyDependency<CFileHandle>::Inlining<OwnedCFile, UnownedCFile>;

}  // namespace riegeli

#endif  // RIEGELI_BYTES_CFILE_HANDLE_H_
