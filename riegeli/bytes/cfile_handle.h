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
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/any.h"
#include "riegeli/base/c_string_ref.h"
#include "riegeli/base/compact_string.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/type_erased_ref.h"
#include "riegeli/base/type_traits.h"
#include "riegeli/bytes/cfile_internal.h"
#include "riegeli/bytes/path_ref.h"

namespace riegeli {

// `SupportsCFileHandle<T>::value` is `true` if `T&` is a valid constructor
// argument for `CFileHandle`.

template <typename T, typename Enable = void>
struct SupportsCFileHandle : std::false_type {};

template <typename T>
struct SupportsCFileHandle<
    T,
    std::enable_if_t<std::conjunction_v<
        std::negation<std::is_const<T>>,
        std::is_convertible<decltype(std::declval<const T&>().get()), FILE*>>>>
    : std::true_type {};

// `CFileSupportsOpen<T>::value` is `true` if `T` supports `Open()` with the
// signature like in `OwnedCFile`, but taking `absl::string_view filename` and
// `const char* mode` is sufficient.

template <typename T, typename Enable = void>
struct CFileSupportsOpen : std::false_type {};

template <typename T>
struct CFileSupportsOpen<
    T, std::enable_if_t<std::is_convertible_v<
           decltype(std::declval<T&>().Open(std::declval<absl::string_view>(),
                                            std::declval<const char*>())),
           absl::Status>>> : std::true_type {};

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
//   // Opens a new `FILE*`, like with `fopen()`, but taking
//   // `absl::string_view filename` and returning `absl::Status`.
//   //
//   // Optional. Not used by `CFileHandle` itself. Used by `CFileReader` and
//   // `CFileWriter` constructors from the filename.
//   absl::Status Open(absl::string_view filename, const char* mode);
//
//   // Returns the filename of the `FILE*`, or "<none>" for
//   // default-constructed or moved-from target. Unchanged by `Close()`.
//   //
//   // If `Open()` was used, this is the filename passed to `Open()`, otherwise
//   // a filename is inferred from the `FILE*`. This can be a placeholder
//   // instead of a real filename if the `FILE*` does not refer to a named file
//   // or inferring the filename is not supported.
//   //
//   // Optional. If absent, "<unsupported>" is assumed.
//   absl::string_view filename() const ABSL_ATTRIBUTE_LIFETIME_BOUND;
//
//   // If `IsOwning()`, closes the `FILE*`.
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
    CFileHandle : public WithEqual<CFileHandle> {
 public:
  // Creates a `CFileHandle` which does not refer to a target.
  CFileHandle() = default;
  /*implicit*/ CFileHandle(std::nullptr_t) {}

  // Creates a `CFileHandle` which refers to `target`.
  template <typename T,
            std::enable_if_t<std::conjunction_v<NotSameRef<CFileHandle, T&>,
                                                SupportsCFileHandle<T>>,
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

  // Returns the filename of the `FILE*`, or "<none>" for default-constructed or
  // moved-from target. Unchanged by `Close()`.
  //
  // If `Open()` was used, this is the filename passed to `Open()`, otherwise a
  // filename is inferred from the `FILE*`. This can be a placeholder instead of
  // a real filename if the `FILE*` does not refer to a named file or inferring
  // the filename is not supported.
  //
  // If the target does not support `filename()`, returns "<unsupported>".
  absl::string_view filename() const { return methods_->filename(target_); }

  // If `IsOwning()`, closes the `FILE*`.
  //
  // If `!IsOwning()`, does nothing and returns `absl::OkStatus()`.
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

  static FILE* GetMethodDefault(ABSL_ATTRIBUTE_UNUSED TypeErasedRef target) {
    return nullptr;
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

namespace cfile_internal {

class UnownedCFileDeleter;

// Common parts of `UnownedCFileDeleter` and `OwnedCFileDeleter`.
class
#ifdef ABSL_ATTRIBUTE_TRIVIAL_ABI
    ABSL_ATTRIBUTE_TRIVIAL_ABI
#endif
    CFileDeleterBase {
 public:
  CFileDeleterBase() = default;

  explicit CFileDeleterBase(CompactString filename)
      : filename_(std::move(filename)) {}

  // Supports creating a `CFileBase` converted from `UnownedCFile`.
  explicit CFileDeleterBase(const UnownedCFileDeleter& that);
  explicit CFileDeleterBase(UnownedCFileDeleter&& that);

  // Supports creating a `CFileBase` converted from `UnownedCFile`.
  void Reset(const UnownedCFileDeleter& that);

  // Supports creating a `CFileBase` converted from `UnownedCFile`, and
  // resetting `CFileBase` from the same `CFileBase`.
  void Reset(CFileDeleterBase&& that);

  absl::string_view filename() const { return filename_; }

  CompactString&& ReleaseFilename() { return std::move(filename_); }

  void set_filename(CompactString filename) { filename_ = std::move(filename); }

  const char* c_filename() { return filename_.c_str(); }

 protected:
  CFileDeleterBase(const CFileDeleterBase& that) = default;
  CFileDeleterBase& operator=(const CFileDeleterBase& that) = default;

  CFileDeleterBase(CFileDeleterBase&& that) noexcept
      : filename_(
            std::exchange(that.filename_, CompactString(kDefaultFilename))) {}
  CFileDeleterBase& operator=(CFileDeleterBase&& that) noexcept {
    filename_ = std::exchange(that.filename_, CompactString(kDefaultFilename));
    return *this;
  }

 private:
  CompactString filename_{kDefaultFilename};
};

class UnownedCFileDeleter : public CFileDeleterBase {
 public:
  using CFileDeleterBase::CFileDeleterBase;

  // Supports creating an `UnownedCFile` converted from any `CFileBase`.
  explicit UnownedCFileDeleter(const CFileDeleterBase& that)
      : CFileDeleterBase(that) {}

  UnownedCFileDeleter(const UnownedCFileDeleter& that) = default;
  UnownedCFileDeleter& operator=(const UnownedCFileDeleter& that) = default;

  UnownedCFileDeleter(UnownedCFileDeleter&& that) = default;
  UnownedCFileDeleter& operator=(UnownedCFileDeleter&& that) = default;

  // Supports creating an `UnownedCFile` converted from any `CFileBase`.
  void Reset(const CFileDeleterBase& that) {
    CFileDeleterBase::operator=(that);
  }

  static void Destroy(ABSL_ATTRIBUTE_UNUSED FILE* file) {}
};

class OwnedCFileDeleter : public CFileDeleterBase {
 public:
  using CFileDeleterBase::CFileDeleterBase;

  OwnedCFileDeleter(OwnedCFileDeleter&& that) = default;
  OwnedCFileDeleter& operator=(OwnedCFileDeleter&& that) = default;

  static void Destroy(FILE* file) { fclose(file); }
};

inline CFileDeleterBase::CFileDeleterBase(const UnownedCFileDeleter& that)
    : filename_(that.filename_) {}

inline CFileDeleterBase::CFileDeleterBase(UnownedCFileDeleter&& that)
    : filename_(
          std::exchange(that.filename_, CompactString(kDefaultFilename))) {}

inline void CFileDeleterBase::Reset(const UnownedCFileDeleter& that) {
  filename_ = that.filename_;
}

inline void CFileDeleterBase::Reset(CFileDeleterBase&& that) {
  filename_ = std::exchange(that.filename_, CompactString(kDefaultFilename));
}

// Common parts of `UnownedCFile` and `OwnedCFile`.
template <typename Deleter>
class
#ifdef ABSL_ATTRIBUTE_TRIVIAL_ABI
    ABSL_ATTRIBUTE_TRIVIAL_ABI
#endif
    CFileBase {
 public:
  // Creates a `CFileBase` which does not store a `FILE*` and stores "<none>"
  // as the filename.
  CFileBase() = default;
  /*implicit*/ CFileBase(std::nullptr_t) {}

  // Creates a `CFileBase` which stores `file` with the filename inferred from
  // the `FILE*` (or "<none>" if `file == nullptr`).
  explicit CFileBase(FILE* file ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : file_(file),
        deleter_(file_ == nullptr ? CompactString(kDefaultFilename)
                                  : FilenameForCFile(file_)) {}

  // Creates a `CFileBase` which stores `file` with `filename`.
  explicit CFileBase(FILE* file ABSL_ATTRIBUTE_LIFETIME_BOUND, PathRef filename)
      : CFileBase(file, CompactString::ForCStr(filename)) {}
  // Creates a `CFileBase` which stores `file` with `filename`.
  explicit CFileBase(FILE* file ABSL_ATTRIBUTE_LIFETIME_BOUND,
                     CompactString filename)
      : file_(file), deleter_(std::move(filename)) {}

  // Creates a `CFileBase` converted from `UnownedCFile`.
  template <
      typename DependentDeleter = Deleter,
      std::enable_if_t<!std::is_same_v<DependentDeleter, UnownedCFileDeleter>,
                       int> = 0>
  explicit CFileBase(const CFileBase<UnownedCFileDeleter>& that)
      : file_(that.file_), deleter_(that.deleter_) {}
  template <
      typename DependentDeleter = Deleter,
      std::enable_if_t<!std::is_same_v<DependentDeleter, UnownedCFileDeleter>,
                       int> = 0>
  explicit CFileBase(CFileBase<UnownedCFileDeleter>&& that)
      : file_(that.Release()), deleter_(std::move(that.deleter_)) {}

  // Creates an `UnownedCFile` converted from any `CFileBase`.
  template <
      typename OtherDeleter,
      std::enable_if_t<
          std::conjunction_v<
              std::is_same<Deleter, UnownedCFileDeleter>,
              std::negation<std::is_same<OtherDeleter, UnownedCFileDeleter>>>,
          int> = 0>
  /*implicit*/ CFileBase(
      const CFileBase<OtherDeleter>& that ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : file_(that.file_), deleter_(that.deleter_) {}

  // Makes `*this` equivalent to a newly constructed `CFileBase`.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(std::nullptr_t = nullptr) {
    SetFileKeepFilename();
    deleter_.set_filename(CompactString(kDefaultFilename));
  }
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(FILE* file) {
    SetFileKeepFilename(file);
    deleter_.set_filename(file == nullptr ? CompactString(kDefaultFilename)
                                          : FilenameForCFile(file));
  }
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(FILE* file, PathRef filename) {
    Reset(file, CompactString::ForCStr(filename));
  }
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(FILE* file, CompactString filename) {
    SetFileKeepFilename(file);
    deleter_.set_filename(std::move(filename));
  }
  template <
      typename OtherDeleter,
      std::enable_if_t<
          std::disjunction_v<std::is_same<Deleter, UnownedCFileDeleter>,
                             std::is_same<OtherDeleter, UnownedCFileDeleter>>,
          int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(const CFileBase<OtherDeleter>& that) {
    SetFileKeepFilename(that.file_);
    deleter_.Reset(that.deleter_);
  }
  template <
      typename OtherDeleter,
      std::enable_if_t<
          std::disjunction_v<std::is_same<OtherDeleter, UnownedCFileDeleter>,
                             std::is_same<OtherDeleter, Deleter>>,
          int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(CFileBase<OtherDeleter>&& that) {
    SetFileKeepFilename(that.Release());
    deleter_.Reset(std::move(that.deleter_));
  }

  // Sets the `FILE*`, keeping `filename()` unchanged.
  void SetFileKeepFilename(FILE* file = nullptr) {
    Destroy();
    file_ = file;
  }

  // Returns `true` if the `FILE*` is present.
  bool is_open() const { return file_ != nullptr; }

  // Returns the `FILE*`.
  FILE* get() const { return file_; }

  // Returns the filename of the `FILE*`, or "<none>" for default-constructed or
  // moved-from `CFileBase`. Unchanged by `Close()` and `Release()`.
  //
  // If `Open()` was used, this is the filename passed to `Open()`, otherwise
  // a filename is inferred from the `FILE*`. This can be a placeholder instead
  // of a real filename if the `FILE*` does not refer to a named file or
  // inferring the filename is not supported.
  absl::string_view filename() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return deleter_.filename();
  }

  // Returns `filename()` as a NUL-terminated string.
  const char* c_filename() ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return deleter_.c_filename();
  }

 protected:
  CFileBase(const CFileBase& that) = default;
  CFileBase& operator=(const CFileBase& that) = default;

  CFileBase(CFileBase&& that) noexcept
      : file_(that.Release()), deleter_(std::move(that.deleter_)) {}
  CFileBase& operator=(CFileBase&& that) noexcept {
    FILE* const file = that.Release();
    Destroy();
    file_ = file;
    deleter_ = std::move(that.deleter_);
    return *this;
  }

  ~CFileBase() { Destroy(); }

  // Returns the file. The stored `FILE*` is left absent, without modifying
  // `filename()`.
  FILE* Release() { return std::exchange(file_, nullptr); }

 private:
  template <typename OtherDeleter>
  friend class CFileBase;  // For conversions.

  void Destroy() {
    if (is_open()) deleter_.Destroy(file_);
  }

  FILE* file_ = nullptr;
  Deleter deleter_;
};

extern template class CFileBase<UnownedCFileDeleter>;
extern template class CFileBase<OwnedCFileDeleter>;

}  // namespace cfile_internal

// Stores a `FILE*` but does not own it, i.e. is not responsible for closing it.
//
// The `FILE*` can be `nullptr` which means absent.
class
#ifdef ABSL_NULLABILITY_COMPATIBLE
    ABSL_NULLABILITY_COMPATIBLE
#endif
    UnownedCFile
    : public cfile_internal::CFileBase<cfile_internal::UnownedCFileDeleter>,
      public WithEqual<UnownedCFile> {
 public:
  using CFileBase::CFileBase;

  // Overridden to make implicit.
  /*implicit*/ UnownedCFile(FILE* file ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : CFileBase(file) {}

  // Creates an `UnownedCFile` which stores `file.get()` with `file.filename()`.
  explicit UnownedCFile(CFileHandle file)
      : CFileBase(file.get(), file.filename()) {}

  UnownedCFile(const UnownedCFile& that) = default;
  UnownedCFile& operator=(const UnownedCFile& that) = default;

  // The moved-from `FILE*` is left absent.
  UnownedCFile(UnownedCFile&& that) = default;
  UnownedCFile& operator=(UnownedCFile&& that) = default;

  using CFileBase::Reset;
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(CFileHandle file) {
    Reset(file.get(), file.filename());
  }

  friend bool operator==(const UnownedCFile& a, const UnownedCFile& b) {
    return a.get() == b.get();
  }
  friend bool operator==(const UnownedCFile& a, FILE* b) {
    return a.get() == b;
  }
};

// Owns a `FILE*`, i.e. stores it and is responsible for closing it.
//
// The `FILE*` can be `nullptr` which means absent.
class
#ifdef ABSL_NULLABILITY_COMPATIBLE
    ABSL_NULLABILITY_COMPATIBLE
#endif
    OwnedCFile
    : public cfile_internal::CFileBase<cfile_internal::OwnedCFileDeleter>,
      public WithEqual<OwnedCFile> {
 public:
  using CFileBase::CFileBase;

  // The moved-from `FILE*` is left absent.
  OwnedCFile(OwnedCFile&& that) = default;
  OwnedCFile& operator=(OwnedCFile&& that) = default;

  // Overridden to apply `ABSL_ATTRIBUTE_LIFETIME_BOUND`.
  FILE* get() const ABSL_ATTRIBUTE_LIFETIME_BOUND { return CFileBase::get(); }

  using CFileBase::Release;

  // Opens a new `FILE*`, like with `fopen()`, but taking `PathRef filename`,
  // `CStringRef mode`, and returning `absl::Status`.
  ABSL_ATTRIBUTE_REINITIALIZES absl::Status Open(PathRef filename,
                                                 CStringRef mode) {
    return Open(CompactString::ForCStr(filename), mode);
  }
  ABSL_ATTRIBUTE_REINITIALIZES absl::Status Open(CompactString filename,
                                                 CStringRef mode);

  // Closes the `FILE*` if present.
  //
  // Returns `absl::OkStatus()` if absent.
  absl::Status Close();

  friend bool operator==(const OwnedCFile& a, FILE* b) { return a.get() == b; }
};

// Type-erased object like `UnownedCFile` or `OwnedCFile` which stores and
// possibly owns a `FILE*`.
using AnyCFile = Any<CFileHandle>::Inlining<UnownedCFile, OwnedCFile>;

}  // namespace riegeli

#endif  // RIEGELI_BYTES_CFILE_HANDLE_H_
