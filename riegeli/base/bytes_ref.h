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
#include "absl/base/nullability.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/string_ref.h"
#include "riegeli/base/temporary_storage.h"
#include "riegeli/base/type_traits.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

// `BytesRef` stores an `absl::string_view` representing text or binary data
// (see `StringRef` for text data), possibly converted from
// `absl::Span<const char>` or temporary `std::string`.
//
// It is intended for function parameters when the implementation needs
// an `absl::string_view`, and the caller might have another representation
// of the string.
//
// It is implicitly convertible from:
//  * types convertible to `absl::string_view`
//  * types convertible to `std::string`, e.g. `BytesInitializer`
//  * types convertible to `absl::Span<const char>`,
//    e.g. `std::vector<char>` or `std::array<char, length>`.
//  * `StringRef`
//
// It is explicitly convertible to `absl::string_view`, `std::string`, or
// `StringRef`.
//
// `BytesRef` does not own string contents and is efficiently copyable.
class BytesRef : public StringRefBase, public WithCompare<BytesRef> {
 public:
  // Stores an empty `absl::string_view`.
  BytesRef() = default;

  // Stores `str` converted to `absl::string_view`.

  ABSL_ATTRIBUTE_ALWAYS_INLINE
  /*implicit*/ BytesRef(const char* str ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : StringRefBase(str) {}

  /*implicit*/ BytesRef(absl::string_view str ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : StringRefBase(str) {}

  /*implicit*/ BytesRef(StringRef str ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : StringRefBase(absl::string_view(str)) {}

  /*implicit*/ BytesRef(absl::Span<char> str ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : StringRefBase(absl::string_view(str.data(), str.size())) {}

  /*implicit*/ BytesRef(
      absl::Span<const char> str ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : StringRefBase(absl::string_view(str.data(), str.size())) {}

  template <typename T,
            std::enable_if_t<
                std::conjunction_v<NotSameRef<BytesRef, T>,
                                   std::is_convertible<T&&, absl::string_view>>,
                int> = 0>
  /*implicit*/ BytesRef(T&& str ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : StringRefBase(std::forward<T>(str)) {}

  template <typename T,
            std::enable_if_t<
                std::conjunction_v<
                    NotSameRef<BytesRef, T>,
                    std::negation<std::is_convertible<T&&, absl::string_view>>,
                    std::is_convertible<T&&, absl::Span<const char>>>,
                int> = 0>
  /*implicit*/ BytesRef(T&& str ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : BytesRef(absl::Span<const char>(std::forward<T>(str))) {}

  template <
      typename T,
      std::enable_if_t<
          std::conjunction_v<
              NotSameRef<BytesRef, T>,
              std::negation<std::is_convertible<T&&, absl::string_view>>,
              std::negation<std::is_convertible<T&&, absl::Span<const char>>>,
              std::is_convertible<T&&, std::string>>,
          int> = 0>
  /*implicit*/ BytesRef(T&& str, TemporaryStorage<std::string>&& storage
                                     ABSL_ATTRIBUTE_LIFETIME_BOUND = {})
      : StringRefBase(std::move(storage).emplace(std::forward<T>(str))) {}

  BytesRef(const BytesRef& that) = default;
  BytesRef& operator=(const BytesRef&) = delete;

  explicit operator StringRef() const { return absl::string_view(*this); }

  friend bool operator==(BytesRef a, BytesRef b) {
    return absl::string_view(a) == absl::string_view(b);
  }
  friend riegeli::StrongOrdering RIEGELI_COMPARE(BytesRef a, BytesRef b) {
    return riegeli::Compare(absl::string_view(a), absl::string_view(b));
  }
};

// `BytesInitializer` is convertible from the same types as `BytesRef`,
// but efficiently takes ownership of `std::string`.
//
// `BytesInitializer` behaves like `Initializer<std::string>`.
class BytesInitializer : public Initializer<std::string> {
 public:
  BytesInitializer() = default;

  // Stores `str` converted to `std::string`.

  ABSL_ATTRIBUTE_ALWAYS_INLINE
  /*implicit*/ BytesInitializer(const char* str ABSL_ATTRIBUTE_LIFETIME_BOUND,
                                TemporaryStorage<MakerType<absl::string_view>>&&
                                    storage ABSL_ATTRIBUTE_LIFETIME_BOUND = {})
      : BytesInitializer(absl::string_view(str), std::move(storage)) {}

  /*implicit*/ BytesInitializer(
      absl::string_view str ABSL_ATTRIBUTE_LIFETIME_BOUND,
      TemporaryStorage<MakerType<absl::string_view>>&& storage
          ABSL_ATTRIBUTE_LIFETIME_BOUND = {})
      : Initializer(std::move(storage).emplace(str)) {}

  /*implicit*/ BytesInitializer(StringRef str ABSL_ATTRIBUTE_LIFETIME_BOUND,
                                TemporaryStorage<MakerType<absl::string_view>>&&
                                    storage ABSL_ATTRIBUTE_LIFETIME_BOUND = {})
      : BytesInitializer(absl::string_view(str), std::move(storage)) {}

  /*implicit*/ BytesInitializer(
      absl::Span<char> str ABSL_ATTRIBUTE_LIFETIME_BOUND,
      TemporaryStorage<MakerType<absl::string_view>>&& storage
          ABSL_ATTRIBUTE_LIFETIME_BOUND = {})
      : BytesInitializer(absl::string_view(str.data(), str.size()),
                         std::move(storage)) {}

  /*implicit*/ BytesInitializer(
      absl::Span<const char> str ABSL_ATTRIBUTE_LIFETIME_BOUND,
      TemporaryStorage<MakerType<absl::string_view>>&& storage
          ABSL_ATTRIBUTE_LIFETIME_BOUND = {})
      : BytesInitializer(absl::string_view(str.data(), str.size()),
                         std::move(storage)) {}

  template <typename T,
            std::enable_if_t<
                std::conjunction_v<NotSameRef<BytesInitializer, T>,
                                   std::is_convertible<T&&, std::string>>,
                int> = 0>
  /*implicit*/ BytesInitializer(T&& str ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer(std::forward<T>(str)) {}

  template <
      typename T,
      std::enable_if_t<std::conjunction_v<
                           NotSameRef<BytesInitializer, T>,
                           std::negation<std::is_convertible<T&&, std::string>>,
                           std::is_convertible<T&&, absl::string_view>>,
                       int> = 0>
  /*implicit*/ BytesInitializer(T&& str ABSL_ATTRIBUTE_LIFETIME_BOUND,
                                TemporaryStorage<MakerType<absl::string_view>>&&
                                    storage ABSL_ATTRIBUTE_LIFETIME_BOUND = {})
      : BytesInitializer(absl::string_view(std::forward<T>(str)),
                         std::move(storage)) {}

  template <typename T,
            std::enable_if_t<
                std::conjunction_v<
                    NotSameRef<BytesInitializer, T>,
                    std::negation<std::is_convertible<T&&, std::string>>,
                    std::negation<std::is_convertible<T&&, absl::string_view>>,
                    std::is_convertible<T&&, absl::Span<const char>>>,
                int> = 0>
  /*implicit*/ BytesInitializer(T&& str ABSL_ATTRIBUTE_LIFETIME_BOUND,
                                TemporaryStorage<MakerType<absl::string_view>>&&
                                    storage ABSL_ATTRIBUTE_LIFETIME_BOUND = {})
      : BytesInitializer(absl::Span<const char>(std::forward<T>(str)),
                         std::move(storage)) {}

  BytesInitializer(BytesInitializer&& that) = default;
  BytesInitializer& operator=(BytesInitializer&&) = delete;
};

}  // namespace riegeli

#endif  // RIEGELI_BASE_BYTES_REF_H_
