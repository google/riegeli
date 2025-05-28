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
#include "absl/base/config.h"  // IWYU pragma: keep
#include "absl/strings/string_view.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/string_ref.h"
#include "riegeli/base/temporary_storage.h"
#include "riegeli/base/type_traits.h"

#if defined(__cpp_lib_filesystem) && __cpp_lib_filesystem >= 201703
#include <filesystem>  // NOLINT(build/c++17)
#endif

namespace riegeli {

// Filename used for default-constructed or moved-from objects.
constexpr absl::string_view kDefaultFilename = "<none>";

// `PathRef` stores an `absl::string_view` representing a file path.
//
// It is intended for function parameters when the implementation needs
// an `absl::string_view`, and the caller might have another representation
// of the string.
//
// It is convertible from:
//  * types convertible to `absl::string_view`
//  * types convertible to `std::string_view`
//  * `std::filesystem::path`
//
// For `std::filesystem::path` with `value_type = char`, it refers to
// `path.native()`.
//
// For `std::filesystem::path` with `value_type = wchar_t`, it refers to
// a copy of `path.string()`. The string is stored in a storage object passed
// as a default argument to the constructor.
//
// `PathRef` does not own path contents and is efficiently copyable.
class PathRef : public StringRef, public WithCompare<PathRef> {
 public:
  // Stores an empty `absl::string_view`.
  PathRef() = default;

  // Stores `str` converted to `StringRef` and then to `absl::string_view`.
  template <
      typename T,
      std::enable_if_t<std::conjunction_v<NotSameRef<PathRef, T>,
                                          std::is_convertible<T&&, StringRef>>,
                       int> = 0>
  /*implicit*/ PathRef(T&& str ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : StringRef(std::forward<T>(str)) {}

#if __cpp_lib_filesystem >= 201703

  // For `std::filesystem::path` with `value_type = char`, stores a reference to
  // `path.native()`.
  template <
      typename DependentPath = std::filesystem::path,
      std::enable_if_t<std::is_same_v<typename DependentPath::value_type, char>,
                       int> = 0>
  /*implicit*/ PathRef(
      const std::filesystem::path& path ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : StringRef(static_cast<const DependentPath&>(path).native()) {}
  template <
      typename DependentPath = std::filesystem::path,
      std::enable_if_t<std::is_same_v<typename DependentPath::value_type, char>,
                       int> = 0>
  /*implicit*/ PathRef(
      const std::filesystem::path& path ABSL_ATTRIBUTE_LIFETIME_BOUND,
      ABSL_ATTRIBUTE_UNUSED TemporaryStorage<std::string>&& storage)
      : PathRef(path) {}

  // For `std::filesystem::path` with `value_type = wchar_t`, stores a reference
  // to stored `path.string()`.
  //
  // The string is stored in a storage object passed as a default argument to
  // this constructor.
  template <
      typename DependentPath = std::filesystem::path,
      std::enable_if_t<
          !std::is_same_v<typename DependentPath::value_type, char>, int> = 0>
  /*implicit*/ PathRef(const std::filesystem::path& path,
                       TemporaryStorage<std::string>&& storage
                           ABSL_ATTRIBUTE_LIFETIME_BOUND = {})
      : StringRef(std::move(storage).emplace(path.string())) {}

#endif

  PathRef(const PathRef& that) = default;
  PathRef& operator=(const PathRef&) = delete;

  friend bool operator==(PathRef a, PathRef b) {
    return absl::string_view(a) == absl::string_view(b);
  }
  friend riegeli::StrongOrdering RIEGELI_COMPARE(PathRef a, PathRef b) {
    return riegeli::Compare(absl::string_view(a), absl::string_view(b));
  }

  template <
      typename T,
      std::enable_if_t<std::conjunction_v<NotSameRef<PathRef, T>,
                                          std::is_convertible<T&&, StringRef>>,
                       int> = 0>
  friend bool operator==(PathRef a, T&& b) {
    return a == PathRef(std::forward<T>(b));
  }
  template <
      typename T,
      std::enable_if_t<std::conjunction_v<NotSameRef<PathRef, T>,
                                          std::is_convertible<T&&, StringRef>>,
                       int> = 0>
  friend riegeli::StrongOrdering RIEGELI_COMPARE(PathRef a, T&& b) {
    return riegeli::Compare(a, PathRef(std::forward<T>(b)));
  }

#if __cpp_lib_filesystem >= 201703

  friend bool operator==(PathRef a, const std::filesystem::path& b) {
    return a == PathRef(b);
  }
  friend riegeli::StrongOrdering RIEGELI_COMPARE(
      PathRef a, const std::filesystem::path& b) {
    return riegeli::Compare(a, PathRef(b));
  }

#endif
};

}  // namespace riegeli

#endif  // RIEGELI_BYTES_PATH_REF_H_
