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

#ifndef RIEGELI_BASE_C_STRING_REF_H_
#define RIEGELI_BASE_C_STRING_REF_H_

#include <cstddef>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/string_ref.h"
#include "riegeli/base/temporary_storage.h"
#include "riegeli/base/type_traits.h"

namespace riegeli {

// `CStringRef` stores a pointer to a C-style NUL-terminated string
// or `nullptr`, possibly converted from another string representation.
//
// It is intended for function parameters when the implementation needs
// a C-style NUL-terminated string, and the caller might have another
// representation of the string.
//
// It is convertible from:
//  * `std::nullptr_t`
//  * types convertible to `const char*`
//  * types supporting `c_str()`, e.g. `std::string` or mutable `CompactString`
//  * types convertible to `absl::string_view`
//  * types convertible to `std::string_view`
//
// It copies string contents when this is needed for NUL-termination,
// e.g. for types convertible to `absl::string_view` or `std::string_view`
// (excluding `std::string` and mutable `CompactString`). In that case
// the string is stored in a storage object passed as a default argument
// to the constructor.
//
// `CStringRef` does not own string contents and is efficiently copyable.
class ABSL_NULLABILITY_COMPATIBLE CStringRef : WithEqual<CStringRef> {
 private:
  template <typename T, typename Enable = void>
  struct HasCStr : std::false_type {};

  template <typename T>
  struct HasCStr<T, std::enable_if_t<std::is_convertible_v<
                        decltype(std::declval<T>().c_str()), const char*>>>
      : std::true_type {};

 public:
  // Stores `nullptr`.
  CStringRef() = default;
  /*implicit*/ CStringRef(std::nullptr_t) {}

  // Stores `str`.
  /*implicit*/ CStringRef(const char* str ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : c_str_(str) {}

  // Stores `str` converted to `const char*`.
  template <typename T,
            std::enable_if_t<
                std::conjunction_v<NotSameRef<CStringRef, T>,
                                   NotSameRef<std::nullptr_t, T>,
                                   NotSameRef<const char*, T>,
                                   std::is_convertible<T&&, const char*>>,
                int> = 0>
  /*implicit*/ CStringRef(T&& str ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : c_str_(std::forward<T>(str)) {}

  // Stores `str.c_str()`. This applies e.g. to `std::string` and
  // mutable `CompactString`.
  template <typename T,
            std::enable_if_t<
                std::conjunction_v<
                    NotSameRef<CStringRef, T>, NotSameRef<std::nullptr_t, T>,
                    std::negation<std::is_convertible<T&&, const char*>>,
                    HasCStr<T&&>>,
                int> = 0>
  /*implicit*/ CStringRef(T&& str ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : c_str_(std::forward<T>(str).c_str()) {}

  // Stores a pointer to the first character of a NUL-terminated copy of `str`
  // converted to `StringRef` and then to `absl::string_view`.
  //
  // The string is stored in a storage object passed as a default argument to
  // this constructor.
  template <
      typename T,
      std::enable_if_t<
          std::conjunction_v<
              NotSameRef<CStringRef, T>, NotSameRef<std::nullptr_t, T>,
              std::negation<std::is_convertible<T&&, const char*>>,
              std::negation<HasCStr<T&&>>, std::is_convertible<T&&, StringRef>>,
          int> = 0>
  /*implicit*/ CStringRef(T&& str, TemporaryStorage<std::string>&& storage
                                       ABSL_ATTRIBUTE_LIFETIME_BOUND = {})
      : CStringRef(std::move(storage).emplace(
            absl::string_view(StringRef(std::forward<T>(str))))) {}

  CStringRef(const CStringRef& that) = default;
  CStringRef& operator=(const CStringRef&) = delete;

  // Returns the pointer to the C-style NUL-terminated string, or `nullptr`.
  const char* c_str() const { return c_str_; }

  friend bool operator==(CStringRef a, std::nullptr_t) {
    return a.c_str_ == nullptr;
  }

 private:
  using pointer = const char*;  // For `ABSL_NULLABILITY_COMPATIBLE`.

  const char* c_str_ = nullptr;
};

}  // namespace riegeli

#endif  // RIEGELI_BASE_C_STRING_REF_H_
