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

#ifndef RIEGELI_BASE_STRING_REF_H_
#define RIEGELI_BASE_STRING_REF_H_

#include <stddef.h>

#include <ostream>
#include <string>
#include <string_view>  // IWYU pragma: keep
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/config.h"  // IWYU pragma: keep
#include "absl/strings/string_view.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/temporary_storage.h"
#include "riegeli/base/type_traits.h"

namespace riegeli {

// `StringRef` stores an `absl::string_view`, usually representing text data
// (see `BytesRef` for binary data), possibly converted from `std::string_view`.
//
// It is intended for function parameters when the implementation needs
// an `absl::string_view`, and the caller might have another representation
// of the string.
//
// It is convertible from:
//  * types convertible to `absl::string_view`
//  * types convertible to `std::string_view`
//  * types convertible to `std::string`, e.g. `StringInitializer`
//
// `StringRef` does not own string contents and is efficiently copyable.
//
// If `absl::string_view` was always `std::string_view`, `StringRef` could be
// replaced by simply `absl::string_view`.
class StringRef : public WithCompare<StringRef> {
 public:
  // Stores an empty `absl::string_view`.
  StringRef() = default;

  // Stores `str` converted to `absl::string_view`.
  ABSL_ATTRIBUTE_ALWAYS_INLINE
  /*implicit*/ StringRef(const char* str ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : str_(str) {}

  // Stores `str`.
  /*implicit*/ StringRef(absl::string_view str ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : str_(str) {}

#if !defined(ABSL_USES_STD_STRING_VIEW)
  // Stores `str` converted to `absl::string_view`.
  /*implicit*/ StringRef(std::string_view str ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : str_(str.data(), str.size()) {}
#endif

  // Stores `str` converted to `absl::string_view`.
  template <typename T,
            std::enable_if_t<
                std::conjunction_v<NotSameRef<StringRef, T>,
                                   NotSameRef<absl::string_view, T>,
#if !defined(ABSL_USES_STD_STRING_VIEW)
                                   NotSameRef<std::string_view, T>,
#endif
                                   std::is_convertible<T&&, absl::string_view>>,
                int> = 0>
  /*implicit*/ StringRef(T&& str ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : str_(std::forward<T>(str)) {
  }

#if !defined(ABSL_USES_STD_STRING_VIEW)
  // Stores `str` converted to `std::string_view` and then to
  // `absl::string_view`.
  template <typename T,
            std::enable_if_t<
                std::conjunction_v<
                    NotSameRef<StringRef, T>,
                    std::negation<std::is_convertible<T&&, absl::string_view>>,
                    NotSameRef<std::string_view, T>,
                    std::is_convertible<T&&, std::string_view>>,
                int> = 0>
  /*implicit*/ StringRef(T&& str ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : StringRef(std::string_view(std::forward<T>(str))) {}
#endif

  // Stores `str` materialized and then converted to `absl::string_view`.
  template <typename T,
            std::enable_if_t<
                std::conjunction_v<
                    NotSameRef<StringRef, T>,
                    std::negation<std::is_convertible<T&&, absl::string_view>>,
#if !defined(ABSL_USES_STD_STRING_VIEW)
                    std::negation<std::is_convertible<T&&, std::string_view>>,
#endif
                    std::is_convertible<T&&, std::string>>,
                int> = 0>
  /*implicit*/ StringRef(T&& str ABSL_ATTRIBUTE_LIFETIME_BOUND,
                         TemporaryStorage<std::string>&& storage
                             ABSL_ATTRIBUTE_LIFETIME_BOUND = {})
      : str_(std::move(storage).emplace(std::forward<T>(str))) {
  }

  StringRef(const StringRef& that) = default;
  StringRef& operator=(const StringRef&) = delete;

  /*implicit*/ operator absl::string_view() const { return str_; }

  bool empty() const { return size() == 0; }
  const char* data() const { return str_.data(); };
  size_t size() const { return str_.size(); }

  const char& operator[](size_t index) const;
  const char& at(size_t index) const;
  const char& front() const;
  const char& back() const;

  void remove_prefix(size_t length);
  void remove_suffix(size_t length);

  friend bool operator==(StringRef a, StringRef b) {
    return absl::string_view(a) == absl::string_view(b);
  }
  friend riegeli::StrongOrdering RIEGELI_COMPARE(StringRef a, StringRef b) {
    return riegeli::Compare(absl::string_view(a), absl::string_view(b));
  }

  template <typename T,
            std::enable_if_t<
                std::conjunction_v<
                    NotSameRef<StringRef, T>,
                    std::disjunction<std::is_convertible<T&&, absl::string_view>
#if !defined(ABSL_USES_STD_STRING_VIEW)
                                     ,
                                     std::is_convertible<T&&, std::string_view>
#endif
                                     >>,
                int> = 0>
  friend bool operator==(StringRef a, T&& b) {
    return a == StringRef(std::forward<T>(b));
  }
  template <typename T,
            std::enable_if_t<
                std::conjunction_v<
                    NotSameRef<StringRef, T>,
                    std::disjunction<std::is_convertible<T&&, absl::string_view>
#if !defined(ABSL_USES_STD_STRING_VIEW)
                                     ,
                                     std::is_convertible<T&&, std::string_view>
#endif
                                     >>,
                int> = 0>
  friend riegeli::StrongOrdering RIEGELI_COMPARE(StringRef a, T&& b) {
    return riegeli::Compare(a, StringRef(std::forward<T>(b)));
  }

  // Default stringification by `absl::StrCat()` etc.
  template <typename Sink>
  friend void AbslStringify(Sink& dest, const StringRef& src) {
    dest.Append(absl::string_view(src));
  }

  friend std::ostream& operator<<(std::ostream& dest, const StringRef& src) {
    return dest << absl::string_view(src);
  }

 private:
  absl::string_view str_;
};

// `StringInitializer` is convertible from the same types as `StringRef`,
// but efficiently takes ownership of `std::string`.
//
// `StringInitializer` behaves like `Initializer<std::string>`.
class StringInitializer : public Initializer<std::string> {
 public:
  StringInitializer() = default;

  // Stores `str` converted to `absl::string_view` and then to `std::string`.
  ABSL_ATTRIBUTE_ALWAYS_INLINE
  /*implicit*/ StringInitializer(
      const char* str ABSL_ATTRIBUTE_LIFETIME_BOUND,
      TemporaryStorage<MakerType<absl::string_view>>&& storage
          ABSL_ATTRIBUTE_LIFETIME_BOUND = {})
      : Initializer(std::move(storage).emplace(absl::string_view(str))) {}

  // Stores `str` converted to `std::string`.
  template <typename T,
            std::enable_if_t<
                std::conjunction_v<NotSameRef<StringInitializer, T>,
                                   std::is_convertible<T&&, std::string>>,
                int> = 0>
  /*implicit*/ StringInitializer(T&& str ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer(std::forward<T>(str)) {}

  // Stores `str` converted to `StringRef`, then to `absl::string_view`, and
  // then to `std::string`.
  template <
      typename T,
      std::enable_if_t<std::conjunction_v<
                           NotSameRef<StringInitializer, T>,
                           std::negation<std::is_convertible<T&&, std::string>>,
                           std::is_convertible<T&&, StringRef>>,
                       int> = 0>
  /*implicit*/ StringInitializer(
      T&& str ABSL_ATTRIBUTE_LIFETIME_BOUND,
      TemporaryStorage<MakerType<absl::string_view>>&& storage
          ABSL_ATTRIBUTE_LIFETIME_BOUND = {})
      : Initializer(
            std::move(storage).emplace(StringRef(std::forward<T>(str)))) {}

  StringInitializer(StringInitializer&& that) = default;
  StringInitializer& operator=(StringInitializer&&) = delete;
};

// Implementation details follow.

inline const char& StringRef::operator[](size_t index) const {
  RIEGELI_ASSERT_LT(index, size())
      << "Failed precondition of StringRef::operator[]: index out of range";
  return str_[index];
}

inline const char& StringRef::at(size_t index) const {
  RIEGELI_ASSERT_LT(index, size())
      << "Failed precondition of StringRef::at(): index out of range";
  return str_[index];
}

inline const char& StringRef::front() const {
  RIEGELI_ASSERT(!empty())
      << "Failed precondition of StringRef::front(): empty string";
  return str_.front();
}

inline const char& StringRef::back() const {
  RIEGELI_ASSERT(!empty())
      << "Failed precondition of StringRef::back(): empty string";
  return str_.back();
}

inline void StringRef::remove_prefix(size_t length) {
  RIEGELI_ASSERT_LE(length, size())
      << "Failed precondition of StringRef::remove_prefix(): "
         "length out of range";
  str_.remove_prefix(length);
}

inline void StringRef::remove_suffix(size_t length) {
  RIEGELI_ASSERT_LE(length, size())
      << "Failed precondition of StringRef::remove_suffix(): "
         "length out of range";
  str_.remove_suffix(length);
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_STRING_REF_H_
