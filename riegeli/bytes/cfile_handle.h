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

// `SupportsCFileHandle<T>::value` is `true` if `T&` is a valid constructor
// argument for `CFileHandle`.

template <typename T, typename Enable = void>
struct SupportsCFileHandle : std::false_type {};

template <typename T>
struct SupportsCFileHandle<
    T, std::enable_if_t<absl::conjunction<
           absl::negation<std::is_const<T>>,
           std::is_convertible<decltype(std::declval<const T&>().get()),
                               FILE*>>::value>> : std::true_type {};

// `CFileSupportsOpen<T>::value` is `true` if `T` supports `Open()` with the
// signature like in `OwnedCFile`, except that accepting `const char* filename`
// and `const char* mode` are sufficient.

template <typename T, typename Enable = void>
struct CFileSupportsOpen : std::false_type {};

template <typename T>
struct CFileSupportsOpen<
    T, std::enable_if_t<std::is_convertible<decltype(std::declval<T&>().Open(
                                                std::declval<const char*>(),
                                                std::declval<const char*>())),
                                            absl::Status>::value>>
    : std::true_type {};

// Type-erased pointer to a target object like `UnownedCFile` or `OwnedCFile`
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
class
#ifdef ABSL_NULLABILITY_COMPATIBLE
    ABSL_NULLABILITY_COMPATIBLE
#endif
        CFileHandle : public WithEqual<CFileHandle> {
 public:
  // Creates a `CFileHandle` which does not refer to a target.
  CFileHandle() = default;
  /*implicit*/ CFileHandle(std::nullptr_t) {}

  // Creates a `CFileHandle` which refers to `target`.
  template <typename T,
            std::enable_if_t<
                absl::conjunction<
                    absl::negation<std::is_convertible<T&, const CFileHandle&>>,
                    SupportsCFileHandle<T>>::value,
                int> = 0>
  /*implicit*/ CFileHandle(T& target ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : methods_(&kMethods<T>), target_(target) {}

  CFileHandle(const CFileHandle& that) = default;
  CFileHandle& operator=(const CFileHandle& that) = default;

  // Returns `true` if the `FILE*` is present.
  bool is_open() const { return *this != nullptr; }

  // Returns the `FILE*`.
  FILE* get() const { return methods_->get(target_); }

  // Returns `true` if the `CFileHandle` owns the `FILE*`, i.e. is responsible
  // for closing it and the `FILE*` is present.
  bool IsOwning() const { return methods_->is_owning(target_); }

  // Closes the `FILE*` if `IsOwning()`.
  //
  // Returns `absl::OkStatus()` if `!IsOwning()`.
  absl::Status Close() { return methods_->close(target_); }

  friend bool operator==(CFileHandle a, CFileHandle b) {
    return a.get() == b.get();
  }
  friend bool operator==(CFileHandle a, FILE* b) { return a.get() == b; }
  friend bool operator==(CFileHandle a, std::nullptr_t) {
    return a.target_ == nullptr || a.get() == nullptr;
  }

 private:
  struct Methods {
    FILE* (*get)(TypeErasedRef target);
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

  template <typename T>
  static FILE* GetMethod(TypeErasedRef target) {
    return target.Cast<const T&>().get();
  }

  template <typename T, std::enable_if_t<HasIsOwning<T>::value, int> = 0>
  static bool IsOwningMethod(TypeErasedRef target) {
    return target.Cast<const T&>().IsOwning();
  }
  template <typename T, std::enable_if_t<!HasIsOwning<T>::value, int> = 0>
  static bool IsOwningMethod(TypeErasedRef target) {
    return HasClose<T>::value && target.Cast<const T&>().get() != nullptr;
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

  const Methods* methods_ = nullptr;
  TypeErasedRef target_;
};

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if !__cpp_inline_variables
template <typename T>
constexpr CFileHandle::Methods CFileHandle::kMethods;
#endif

// Stores a `FILE*` but does not own it, i.e. is not responsible for closing it.
//
// The `FILE*` can be `nullptr` which means absent.
class
#ifdef ABSL_NULLABILITY_COMPATIBLE
    ABSL_NULLABILITY_COMPATIBLE
#endif
        UnownedCFile : public WithEqual<UnownedCFile> {
 public:
  // Creates an `UnownedCFile` which does not store a file.
  UnownedCFile() = default;
  /*implicit*/ UnownedCFile(std::nullptr_t) {}

  // Creates an `UnownedCFile` which stores `file`.
  explicit UnownedCFile(FILE* file ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : file_(file) {}

  UnownedCFile(const UnownedCFile& that) = default;
  UnownedCFile& operator=(const UnownedCFile& that) = default;

  ABSL_ATTRIBUTE_REINITIALIZES void Reset(FILE* file = nullptr) {
    file_ = file;
  }

  // Returns `true` if the `FILE*` is present.
  bool is_open() const { return *this != nullptr; }

  // Returns the `FILE*`.
  FILE* get() const { return file_; }

  friend bool operator==(UnownedCFile a, UnownedCFile b) {
    return a.get() == b.get();
  }
  friend bool operator==(UnownedCFile a, FILE* b) { return a.get() == b; }

 private:
  FILE* file_ = nullptr;
};

// Owns a `FILE*`, i.e. stores it and is responsible for closing it.
//
// The `FILE*` can be `nullptr` which means absent.
class
#ifdef ABSL_NULLABILITY_COMPATIBLE
    ABSL_NULLABILITY_COMPATIBLE
#endif
        OwnedCFile : public WithEqual<OwnedCFile> {
 public:
  // Creates an `OwnedCFile` which does not own a file.
  OwnedCFile() = default;
  /*implicit*/ OwnedCFile(std::nullptr_t) {}

  // Creates an `OwnedCFile` which owns `file`.
  explicit OwnedCFile(FILE* file ABSL_ATTRIBUTE_LIFETIME_BOUND) : file_(file) {}

  // The moved-from `FILE*` is left absent.
  OwnedCFile(OwnedCFile&& that) = default;
  OwnedCFile& operator=(OwnedCFile&& that) = default;

  ABSL_ATTRIBUTE_REINITIALIZES void Reset(FILE* file = nullptr) {
    file_.reset(file);
  }

  // Returns `true` if the `FILE*` is present.
  bool is_open() const { return *this != nullptr; }

  // Returns the `FILE*`.
  FILE* get() const ABSL_ATTRIBUTE_LIFETIME_BOUND { return file_.get(); }

  // Releases and returns the `FILE*` without closing it.
  FILE* Release() { return file_.release(); }

  friend bool operator==(const OwnedCFile& a, FILE* b) { return a.get() == b; }

  // Opens a new `FILE*`, like with `fopen()` but taking `CStringRef filename`,
  // `CStringRef mode`, and returning `absl::Status`.
  ABSL_ATTRIBUTE_REINITIALIZES absl::Status Open(CStringRef filename,
                                                 CStringRef mode);

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

// Type-erased object like `UnownedCFile` or `OwnedCFile` which stores and
// possibly owns a `FILE*`.
using AnyCFile = Any<CFileHandle>::Inlining<UnownedCFile, OwnedCFile>;

}  // namespace riegeli

#endif  // RIEGELI_BYTES_CFILE_HANDLE_H_
