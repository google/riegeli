// Copyright 2024 Google LLC
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

#ifndef RIEGELI_BYTES_PATH_REF_H_
#define RIEGELI_BYTES_PATH_REF_H_

#include <stddef.h>

#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/invoker.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/string_ref.h"
#include "riegeli/base/temporary_storage.h"
#include "riegeli/base/type_traits.h"

#if defined(__cpp_lib_filesystem) && __cpp_lib_filesystem >= 201703
#include <filesystem>
#endif

namespace riegeli {

// Filename used for default-constructed or moved-from objects.
constexpr char kDefaultFilenameCStr[] = "<none>";
constexpr absl::string_view kDefaultFilename = kDefaultFilenameCStr;

// `PathRef` stores an `absl::string_view` representing a file path.
//
// It is intended for function parameters when the implementation needs
// an `absl::string_view`, and the caller might have another representation
// of the string.
//
// It is implicitly convertible from:
//  * types convertible to `absl::string_view`
//  * types convertible to `std::string`, e.g. `PathInitializer`
//  * `std::filesystem::path`
//  * `StringRef`
//
// For `std::filesystem::path` with `value_type = char`, it refers to
// `path.native()`.
//
// For `std::filesystem::path` with `value_type = wchar_t`, it refers to
// `path.string()` stored in a storage object passed as a default argument to
// the constructor.
//
// It is explicitly convertible to `absl::string_view`, `std::string`, or
// `StringRef`.
//
// `PathRef` does not own path contents and is efficiently copyable.
class PathRef : public StringRefBase, public WithCompare<PathRef> {
 public:
  // Stores an empty `absl::string_view`.
  PathRef() = default;

  // Stores `str` converted to `absl::string_view`.

  ABSL_ATTRIBUTE_ALWAYS_INLINE
  /*implicit*/ PathRef(const char* str ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : StringRefBase(str) {}

  /*implicit*/ PathRef(absl::string_view str ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : StringRefBase(str) {}

  /*implicit*/ PathRef(StringRef str ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : StringRefBase(absl::string_view(str)) {}

#if __cpp_lib_filesystem >= 201703

  // For `std::filesystem::path` with `value_type = char`, stores a reference to
  // `path.native()`.
  template <
      typename DependentPath = std::filesystem::path,
      std::enable_if_t<std::is_same_v<typename DependentPath::value_type, char>,
                       int> = 0>
  /*implicit*/ PathRef(
      const std::filesystem::path& path ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : StringRefBase(static_cast<const DependentPath&>(path).native()) {}

  template <
      typename DependentPath = std::filesystem::path,
      std::enable_if_t<std::is_same_v<typename DependentPath::value_type, char>,
                       int> = 0>
  /*implicit*/ PathRef(
      const std::filesystem::path& path ABSL_ATTRIBUTE_LIFETIME_BOUND,
      ABSL_ATTRIBUTE_UNUSED TemporaryStorage<std::string>&& storage)
      : PathRef(path) {}

  // For `std::filesystem::path` with `value_type = wchar_t`, stores a reference
  // to `path.string()` stored in a storage object passed as a default argument
  // to this constructor.
  template <
      typename DependentPath = std::filesystem::path,
      std::enable_if_t<
          !std::is_same_v<typename DependentPath::value_type, char>, int> = 0>
  /*implicit*/ PathRef(const std::filesystem::path& path,
                       TemporaryStorage<std::string>&& storage
                           ABSL_ATTRIBUTE_LIFETIME_BOUND = {})
      : StringRefBase(std::move(storage).emplace(
            riegeli::Invoker([&] { return path.string(); }))) {}

#endif

  template <typename T,
            std::enable_if_t<
                std::conjunction_v<NotSameRef<PathRef, T>,
                                   std::is_convertible<T&&, absl::string_view>>,
                int> = 0>
  /*implicit*/ PathRef(T&& str ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : StringRefBase(std::forward<T>(str)) {}

  template <typename T,
            std::enable_if_t<
                std::conjunction_v<
                    NotSameRef<PathRef, T>,
                    std::negation<std::is_convertible<T&&, absl::string_view>>,
                    std::is_convertible<T&&, std::string>>,
                int> = 0>
  /*implicit*/ PathRef(T&& str, TemporaryStorage<std::string>&& storage
                                    ABSL_ATTRIBUTE_LIFETIME_BOUND = {})
      : StringRefBase(std::move(storage).emplace(std::forward<T>(str))) {}

  PathRef(const PathRef& that) = default;
  PathRef& operator=(const PathRef&) = delete;

  explicit operator StringRef() const { return absl::string_view(*this); }

  friend bool operator==(PathRef a, PathRef b) {
    return absl::string_view(a) == absl::string_view(b);
  }
  friend riegeli::StrongOrdering RIEGELI_COMPARE(PathRef a, PathRef b) {
    return riegeli::Compare(absl::string_view(a), absl::string_view(b));
  }
};

// `PathInitializer` is convertible from the same types as `PathRef`,
// but efficiently takes ownership of `std::string`.
//
// `PathInitializer` behaves like `Initializer<std::string>`.
class PathInitializer : public Initializer<std::string> {
 public:
#if __cpp_lib_filesystem >= 201703
  class StringFromPath {
   public:
    explicit StringFromPath(
        const std::filesystem::path& path ABSL_ATTRIBUTE_LIFETIME_BOUND)
        : path_(path) {}

    /*implicit*/ operator std::string() const { return path_.string(); }

   private:
    const std::filesystem::path& path_;
  };
#endif

  PathInitializer() = default;

  // Stores `str` converted to `std::string`.

  ABSL_ATTRIBUTE_ALWAYS_INLINE
  /*implicit*/ PathInitializer(const char* str ABSL_ATTRIBUTE_LIFETIME_BOUND,
                               TemporaryStorage<MakerType<absl::string_view>>&&
                                   storage ABSL_ATTRIBUTE_LIFETIME_BOUND = {})
      : PathInitializer(absl::string_view(str), std::move(storage)) {}

  /*implicit*/ PathInitializer(
      absl::string_view str ABSL_ATTRIBUTE_LIFETIME_BOUND,
      TemporaryStorage<MakerType<absl::string_view>>&& storage
          ABSL_ATTRIBUTE_LIFETIME_BOUND = {})
      : Initializer(std::move(storage).emplace(str)) {}

  /*implicit*/ PathInitializer(StringRef str ABSL_ATTRIBUTE_LIFETIME_BOUND,
                               TemporaryStorage<MakerType<absl::string_view>>&&
                                   storage ABSL_ATTRIBUTE_LIFETIME_BOUND = {})
      : PathInitializer(absl::string_view(str), std::move(storage)) {}

  /*implicit*/ PathInitializer(PathRef str ABSL_ATTRIBUTE_LIFETIME_BOUND,
                               TemporaryStorage<MakerType<absl::string_view>>&&
                                   storage ABSL_ATTRIBUTE_LIFETIME_BOUND = {})
      : PathInitializer(absl::string_view(str), std::move(storage)) {}

#if __cpp_lib_filesystem >= 201703
  // Stores `path.string()`.
  /*implicit*/ PathInitializer(const std::filesystem::path& path
                                   ABSL_ATTRIBUTE_LIFETIME_BOUND,
                               TemporaryStorage<StringFromPath>&& storage
                                   ABSL_ATTRIBUTE_LIFETIME_BOUND = {})
      : Initializer(std::move(storage).emplace(path)) {}
#endif

  template <typename T,
            std::enable_if_t<
                std::conjunction_v<NotSameRef<PathInitializer, T>,
                                   std::is_convertible<T&&, std::string>>,
                int> = 0>
  /*implicit*/ PathInitializer(T&& str ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer(std::forward<T>(str)) {}

  template <
      typename T,
      std::enable_if_t<std::conjunction_v<
                           NotSameRef<PathInitializer, T>,
                           std::negation<std::is_convertible<T&&, std::string>>,
                           std::is_convertible<T&&, absl::string_view>>,
                       int> = 0>
  /*implicit*/ PathInitializer(T&& str ABSL_ATTRIBUTE_LIFETIME_BOUND,
                               TemporaryStorage<MakerType<absl::string_view>>&&
                                   storage ABSL_ATTRIBUTE_LIFETIME_BOUND = {})
      : PathInitializer(absl::string_view(std::forward<T>(str)),
                        std::move(storage)) {}

  PathInitializer(PathInitializer&& that) = default;
  PathInitializer& operator=(PathInitializer&&) = delete;
};

}  // namespace riegeli

#endif  // RIEGELI_BYTES_PATH_REF_H_
