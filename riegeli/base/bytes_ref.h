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

#ifndef RIEGELI_BASE_BYTES_REF_H_
#define RIEGELI_BASE_BYTES_REF_H_

#include <stddef.h>

#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/config.h"  // IWYU pragma: keep
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/string_ref.h"
#include "riegeli/base/temporary_storage.h"
#include "riegeli/base/type_traits.h"

namespace riegeli {

// `BytesRef` stores an `absl::string_view` representing text or binary data
// (see `StringRef` for text data), possibly converted from `std::string_view`
// or `absl::Span<const char>`.
//
// It is intended for function parameters when the implementation needs
// an `absl::string_view`, and the caller might have another representation
// of the string.
//
// It is convertible from:
//  * types convertible to `absl::string_view`
//  * types convertible to `std::string_view`
//  * types convertible to `std::string`, e.g. `BytesInitializer`
//  * types convertible to `absl::Span<const char>`,
//    e.g. `std::vector<char>` or `std::array<char, length>`.
//
// `BytesRef` does not own string contents and is efficiently copyable.
class BytesRef : public StringRef, public WithCompare<BytesRef> {
 public:
  // Stores an empty `absl::string_view`.
  BytesRef() = default;

  // Stores `str` converted to `absl::string_view`.
  ABSL_ATTRIBUTE_ALWAYS_INLINE
  /*implicit*/ BytesRef(const char* str ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : StringRef(absl::string_view(str)) {}

  // Stores `str` converted to `StringRef` and then to `absl::string_view`.
  template <typename T,
            std::enable_if_t<
                std::conjunction_v<
                    NotSameRef<BytesRef, T>,
                    std::disjunction<std::is_convertible<T&&, absl::string_view>
#if !defined(ABSL_USES_STD_STRING_VIEW)
                                     ,
                                     std::is_convertible<T&&, std::string_view>
#endif
                                     >>,
                int> = 0>
  /*implicit*/ BytesRef(T&& str ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : StringRef(std::forward<T>(str)) {
  }

  // Stores `str` converted to `absl::string_view`.
  /*implicit*/ BytesRef(
      absl::Span<const char> str ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : StringRef(absl::string_view(str.data(), str.size())) {}

  // Stores `str` materialized, then converted to `StringRef` and then to
  // `absl::string_view`.
  template <typename T,
            std::enable_if_t<
                std::conjunction_v<
                    NotSameRef<BytesRef, T>,
                    std::negation<std::is_convertible<T&&, absl::string_view>>,
#if !defined(ABSL_USES_STD_STRING_VIEW)
                    std::negation<std::is_convertible<T&&, std::string_view>>,
#endif
                    std::is_convertible<T&&, std::string>>,
                int> = 0>
  /*implicit*/ BytesRef(T&& str ABSL_ATTRIBUTE_LIFETIME_BOUND,
                        TemporaryStorage<std::string>&& storage
                            ABSL_ATTRIBUTE_LIFETIME_BOUND = {})
      : StringRef(std::forward<T>(str), std::move(storage)) {
  }

  // Stores `str` converted to `absl::Span<const char>` and then to
  // `absl::string_view`.
  template <
      typename T,
      std::enable_if_t<
          std::conjunction_v<NotSameRef<BytesRef, T>,
                             std::negation<std::is_convertible<T&&, StringRef>>,
                             NotSameRef<absl::Span<const char>, T>,
                             std::is_convertible<T&&, absl::Span<const char>>>,
          int> = 0>
  /*implicit*/ BytesRef(T&& str ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : BytesRef(absl::Span<const char>(std::forward<T>(str))) {}

  BytesRef(const BytesRef& that) = default;
  BytesRef& operator=(const BytesRef&) = delete;

  friend bool operator==(BytesRef a, BytesRef b) {
    return absl::string_view(a) == absl::string_view(b);
  }
  friend riegeli::StrongOrdering RIEGELI_COMPARE(BytesRef a, BytesRef b) {
    return riegeli::Compare(absl::string_view(a), absl::string_view(b));
  }

  template <
      typename T,
      std::enable_if_t<std::conjunction_v<NotSameRef<BytesRef, T>,
                                          std::is_convertible<T&&, StringRef>>,
                       int> = 0>
  friend bool operator==(BytesRef a, T&& b) {
    return a == BytesRef(std::forward<T>(b));
  }
  template <
      typename T,
      std::enable_if_t<std::conjunction_v<NotSameRef<BytesRef, T>,
                                          std::is_convertible<T&&, StringRef>>,
                       int> = 0>
  friend riegeli::StrongOrdering RIEGELI_COMPARE(BytesRef a, T&& b) {
    return riegeli::Compare(a, BytesRef(std::forward<T>(b)));
  }

  // `absl::Span<const char>` is already comparable against types convertible to
  // `absl::Span<const char>`, which includes `BytesRef`.
};

// `BytesInitializer` is convertible from the same types as `BytesRef`,
// but efficiently takes ownership of `std::string`.
//
// `BytesInitializer` behaves like `Initializer<std::string>`.
class BytesInitializer : public Initializer<std::string> {
 public:
  BytesInitializer() = default;

  // Stores `str` converted to `absl::string_view` and then to `std::string`.
  ABSL_ATTRIBUTE_ALWAYS_INLINE
  /*implicit*/ BytesInitializer(const char* str ABSL_ATTRIBUTE_LIFETIME_BOUND,
                                TemporaryStorage<MakerType<absl::string_view>>&&
                                    storage ABSL_ATTRIBUTE_LIFETIME_BOUND = {})
      : Initializer(std::move(storage).emplace(absl::string_view(str))) {}

  // Stores `str` converted to `std::string`.
  template <typename T,
            std::enable_if_t<
                std::conjunction_v<NotSameRef<BytesInitializer, T>,
                                   std::is_convertible<T&&, std::string>>,
                int> = 0>
  /*implicit*/ BytesInitializer(T&& str ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer(std::forward<T>(str)) {}

  // Stores `str` converted to `BytesRef`, then to `absl::string_view`, and then
  // to `std::string`.
  template <
      typename T,
      std::enable_if_t<std::conjunction_v<
                           NotSameRef<BytesInitializer, T>,
                           std::negation<std::is_convertible<T&&, std::string>>,
                           std::is_convertible<T&&, BytesRef>>,
                       int> = 0>
  /*implicit*/ BytesInitializer(T&& str ABSL_ATTRIBUTE_LIFETIME_BOUND,
                                TemporaryStorage<MakerType<absl::string_view>>&&
                                    storage ABSL_ATTRIBUTE_LIFETIME_BOUND = {})
      : Initializer(
            std::move(storage).emplace(BytesRef(std::forward<T>(str)))) {}

  BytesInitializer(BytesInitializer&& that) = default;
  BytesInitializer& operator=(BytesInitializer&&) = delete;
};

}  // namespace riegeli

#endif  // RIEGELI_BASE_BYTES_REF_H_
