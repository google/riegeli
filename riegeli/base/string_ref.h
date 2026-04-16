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
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/temporary_storage.h"
#include "riegeli/base/type_traits.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

// Common parts of `StringRef`, `BytesRef`, and `PathRef`.
class StringRefBase {
 public:
  explicit operator absl::string_view() const { return str_; }
  explicit operator std::string() const { return std::string(str_); }

  bool empty() const { return str_.empty(); }
  const char* absl_nullable data() const { return str_.data(); };
  size_t size() const {
    RIEGELI_ASSUME_LE(str_.size(), str_.max_size());
    return str_.size();
  }

  const char& operator[](size_t index) const;
  const char& at(size_t index) const;
  const char& front() const;
  const char& back() const;

  void remove_prefix(size_t length);
  void remove_suffix(size_t length);

  // Default stringification by `absl::StrCat()` etc.
  template <typename Sink>
  friend void AbslStringify(Sink& dest, const StringRefBase& src) {
    dest.Append(absl::string_view(src));
  }

  friend std::ostream& operator<<(std::ostream& dest,
                                  const StringRefBase& src) {
    return dest << absl::string_view(src);
  }

 protected:
  StringRefBase() = default;

  explicit StringRefBase(absl::string_view str) : str_(str) {}

  StringRefBase(const StringRefBase& that) = default;
  StringRefBase& operator=(const StringRefBase&) = delete;

  ~StringRefBase() = default;

 private:
  absl::string_view str_;
};

// `StringRef` stores an `absl::string_view`, usually representing text data
// (see `BytesRef` for binary data), possibly converted through temporary
// `std::string`.
//
// It is intended for function parameters when the implementation needs
// an `absl::string_view`, and the caller might have another representation
// of the string.
//
// It is implicitly convertible from:
//  * types convertible to `absl::string_view`
//  * types convertible to `std::string`, e.g. `StringInitializer`
//
// It is explicitly convertible to `absl::string_view` or `std::string`.
//
// `StringRef` does not own string contents and is efficiently copyable.
class StringRef : public StringRefBase, public WithCompare<StringRef> {
 public:
  // Stores an empty `absl::string_view`.
  StringRef() = default;

  // Stores `str` converted to `absl::string_view`.

  ABSL_ATTRIBUTE_ALWAYS_INLINE
  /*implicit*/ StringRef(const char* str ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : StringRefBase(str) {}

  /*implicit*/ StringRef(absl::string_view str ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : StringRefBase(str) {}

  template <typename T,
            std::enable_if_t<
                std::conjunction_v<NotSameRef<StringRef, T>,
                                   std::is_convertible<T&&, absl::string_view>>,
                int> = 0>
  /*implicit*/ StringRef(T&& str ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : StringRefBase(std::forward<T>(str)) {}

  template <typename T,
            std::enable_if_t<
                std::conjunction_v<
                    NotSameRef<StringRef, T>,
                    std::negation<std::is_convertible<T&&, absl::string_view>>,
                    std::is_convertible<T&&, std::string>>,
                int> = 0>
  /*implicit*/ StringRef(T&& str, TemporaryStorage<std::string>&& storage
                                      ABSL_ATTRIBUTE_LIFETIME_BOUND = {})
      : StringRefBase(std::move(storage).emplace(std::forward<T>(str))) {}

  StringRef(const StringRef& that) = default;
  StringRef& operator=(const StringRef&) = delete;

  friend bool operator==(StringRef a, StringRef b) {
    return absl::string_view(a) == absl::string_view(b);
  }
  friend riegeli::StrongOrdering RIEGELI_COMPARE(StringRef a, StringRef b) {
    return riegeli::Compare(absl::string_view(a), absl::string_view(b));
  }
};

// `StringInitializer` is convertible from the same types as `StringRef`,
// but efficiently takes ownership of `std::string`.
//
// `StringInitializer` behaves like `Initializer<std::string>`.
class StringInitializer : public Initializer<std::string> {
 public:
  StringInitializer() = default;

  ABSL_ATTRIBUTE_ALWAYS_INLINE
  /*implicit*/ StringInitializer(
      const char* str ABSL_ATTRIBUTE_LIFETIME_BOUND,
      TemporaryStorage<MakerType<absl::string_view>>&& storage
          ABSL_ATTRIBUTE_LIFETIME_BOUND = {})
      : StringInitializer(absl::string_view(str), std::move(storage)) {}

  /*implicit*/ StringInitializer(
      absl::string_view str ABSL_ATTRIBUTE_LIFETIME_BOUND,
      TemporaryStorage<MakerType<absl::string_view>>&& storage
          ABSL_ATTRIBUTE_LIFETIME_BOUND = {})
      : Initializer(std::move(storage).emplace(str)) {}

  /*implicit*/ StringInitializer(
      StringRef str ABSL_ATTRIBUTE_LIFETIME_BOUND,
      TemporaryStorage<MakerType<absl::string_view>>&& storage
          ABSL_ATTRIBUTE_LIFETIME_BOUND = {})
      : StringInitializer(absl::string_view(str), std::move(storage)) {}

  template <typename T,
            std::enable_if_t<
                std::conjunction_v<NotSameRef<StringInitializer, T>,
                                   std::is_convertible<T&&, std::string>>,
                int> = 0>
  /*implicit*/ StringInitializer(T&& str ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : Initializer(std::forward<T>(str)) {}

  template <
      typename T,
      std::enable_if_t<std::conjunction_v<
                           NotSameRef<StringInitializer, T>,
                           std::negation<std::is_convertible<T&&, std::string>>,
                           std::is_convertible<T&&, absl::string_view>>,
                       int> = 0>
  /*implicit*/ StringInitializer(
      T&& str ABSL_ATTRIBUTE_LIFETIME_BOUND,
      TemporaryStorage<MakerType<absl::string_view>>&& storage
          ABSL_ATTRIBUTE_LIFETIME_BOUND = {})
      : StringInitializer(absl::string_view(std::forward<T>(str)),
                          std::move(storage)) {}

  StringInitializer(StringInitializer&& that) = default;
  StringInitializer& operator=(StringInitializer&&) = delete;
};

// Implementation details follow.

inline const char& StringRefBase::operator[](size_t index) const {
  RIEGELI_ASSERT_LT(index, size())
      << "Failed precondition of StringRefBase::operator[]: index out of range";
  return str_[index];
}

inline const char& StringRefBase::at(size_t index) const {
  RIEGELI_ASSERT_LT(index, size())
      << "Failed precondition of StringRefBase::at(): index out of range";
  return str_[index];
}

inline const char& StringRefBase::front() const {
  RIEGELI_ASSERT(!empty())
      << "Failed precondition of StringRefBase::front(): empty string";
  return str_.front();
}

inline const char& StringRefBase::back() const {
  RIEGELI_ASSERT(!empty())
      << "Failed precondition of StringRefBase::back(): empty string";
  return str_.back();
}

inline void StringRefBase::remove_prefix(size_t length) {
  RIEGELI_ASSERT_LE(length, size())
      << "Failed precondition of StringRefBase::remove_prefix(): "
         "length out of range";
  str_.remove_prefix(length);
}

inline void StringRefBase::remove_suffix(size_t length) {
  RIEGELI_ASSERT_LE(length, size())
      << "Failed precondition of StringRefBase::remove_suffix(): "
         "length out of range";
  str_.remove_suffix(length);
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_STRING_REF_H_
