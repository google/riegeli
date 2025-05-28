// Copyright 2025 Google LLC
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

#ifndef RIEGELI_BASE_OPTIONAL_COMPACT_STRING_H_
#define RIEGELI_BASE_OPTIONAL_COMPACT_STRING_H_

#include <stdint.h>

#include <cstddef>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/meta/type_traits.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/bytes_ref.h"
#include "riegeli/base/compact_string.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/type_traits.h"

namespace riegeli {

// Similar to `std::optional<CompactString>`, but takes up the same amount of
// space as `CompactString`.
//
// `OptionalCompactString` is either null or stores data equivalent to a
// `CompactString`. It allows examining the contents as `absl::string_view` or
// `const char*`, but not as `CompactString`, except by copying or moving from.
class
#ifdef ABSL_NULLABILITY_COMPATIBLE
    ABSL_NULLABILITY_COMPATIBLE
#endif
#ifdef ABSL_ATTRIBUTE_TRIVIAL_ABI
    ABSL_ATTRIBUTE_TRIVIAL_ABI
#endif
    OptionalCompactString : public WithCompare<OptionalCompactString> {
 private:
  class StringViewPointer {
   public:
    const absl::string_view* operator->() const { return &ref_; }

   private:
    friend class OptionalCompactString;
    explicit StringViewPointer(absl::string_view ref) : ref_(ref) {}
    absl::string_view ref_;
  };

 public:
  // Creates a null `OptionalCompactString`.
  OptionalCompactString() = default;
  /*implicit*/ OptionalCompactString(std::nullptr_t) {}
  OptionalCompactString& operator=(std::nullptr_t) {
    if (repr_ != kNullRepr) {
      CompactString::MoveFromRaw(repr_);
      repr_ = kNullRepr;
    }
    return *this;
  }

  // Creates an `OptionalCompactString` which holds a copy of `src`.
  explicit OptionalCompactString(BytesRef src)
      : repr_(CompactString(src).RawMove()) {}
  OptionalCompactString& operator=(BytesRef src) {
    const uintptr_t old_repr =
        std::exchange(repr_, CompactString(src).RawMove());
    if (old_repr != kNullRepr) CompactString::MoveFromRaw(old_repr);
    return *this;
  }

  // Creates an `OptionalCompactString` which holds a copy of `src`.
  explicit OptionalCompactString(const CompactString& src)
      : repr_(CompactString(src).RawMove()) {}
  OptionalCompactString& operator=(const CompactString& src) {
    const uintptr_t old_repr =
        std::exchange(repr_, CompactString(src).RawMove());
    if (old_repr != kNullRepr) CompactString::MoveFromRaw(old_repr);
    return *this;
  }

  // Creates an `OptionalCompactString` which holds `src`. The source
  // `CompactString` is left empty.
  explicit OptionalCompactString(CompactString&& src)
      : repr_(std::move(src).RawMove()) {}
  OptionalCompactString& operator=(CompactString&& src) {
    const uintptr_t old_repr = std::exchange(repr_, std::move(src).RawMove());
    if (old_repr != kNullRepr) CompactString::MoveFromRaw(old_repr);
    return *this;
  }

  OptionalCompactString(const OptionalCompactString& that) noexcept
      : repr_(that.repr_ == kNullRepr ? kNullRepr
                                      : CompactString::CopyRaw(that.repr_)) {}
  OptionalCompactString& operator=(const OptionalCompactString& that) noexcept {
    const uintptr_t old_repr = std::exchange(
        repr_, that.repr_ == kNullRepr ? kNullRepr
                                       : CompactString::CopyRaw(that.repr_));
    if (old_repr != kNullRepr) CompactString::MoveFromRaw(old_repr);
    return *this;
  }

  // The source `OptionalCompactString` is left null.
  OptionalCompactString(OptionalCompactString&& that) noexcept
      : repr_(std::exchange(that.repr_, kNullRepr)) {}
  OptionalCompactString& operator=(OptionalCompactString&& that) noexcept {
    const uintptr_t old_repr =
        std::exchange(repr_, std::exchange(that.repr_, kNullRepr));
    if (old_repr != kNullRepr) CompactString::MoveFromRaw(old_repr);
    return *this;
  }

  ~OptionalCompactString() {
    if (repr_ != kNullRepr) CompactString::MoveFromRaw(repr_);
  }

  // Extracts the value as a `CompactString`.
  //
  // Precondition: `*this != nullptr`.
  CompactString Release() ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT(*this != nullptr)
        << "Failed precondition of OptionalCompactString::Release(): "
           "OptionalCompactString is nullptr";
    return CompactString::MoveFromRaw(std::exchange(repr_, kNullRepr));
  }

  // Views the value as an `absl::string_view`.
  //
  // Precondition: `*this != nullptr`.
  absl::string_view operator*() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT(*this != nullptr)
        << "Failed precondition of OptionalCompactString::operator*: "
           "OptionalCompactString is nullptr";
    return CompactString::ViewFromRaw(&repr_);
  }

  StringViewPointer operator->() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT(*this != nullptr)
        << "Failed precondition of OptionalCompactString::operator->: "
           "OptionalCompactString is nullptr";
    return StringViewPointer(**this);
  }

  // Ensures that the value is NUL-terminated after its size and returns
  // a pointer to it. Returns `nullptr` for a null `OptionalCompactString`.
  //
  // In contrast to `std::string::c_str()`, this is a non-const operation.
  // It may reallocate the string and it writes the NUL each time.
  const char* c_str() ABSL_ATTRIBUTE_LIFETIME_BOUND {
    if (repr_ == kNullRepr) return nullptr;
    return CompactString::CStrFromRaw(&repr_);
  }

  friend bool operator==(const OptionalCompactString& a,
                         const OptionalCompactString& b) {
    if (a.repr_ == b.repr_) return true;
    if (a.repr_ == kNullRepr || b.repr_ == kNullRepr) return false;
    return *a == *b;
  }
  friend StrongOrdering RIEGELI_COMPARE(const OptionalCompactString& a,
                                        const OptionalCompactString& b) {
    if (a.repr_ == b.repr_) return StrongOrdering::equal;
    if (a.repr_ == kNullRepr) return StrongOrdering::less;
    if (b.repr_ == kNullRepr) return StrongOrdering::greater;
    return riegeli::Compare(*a, *b);
  }

  friend bool operator==(const OptionalCompactString& a, std::nullptr_t) {
    return a.repr_ == kNullRepr;
  }
  friend StrongOrdering RIEGELI_COMPARE(const OptionalCompactString& a,
                                        std::nullptr_t) {
    if (a.repr_ == kNullRepr) return StrongOrdering::equal;
    return StrongOrdering::greater;
  }

  template <typename T,
            std::enable_if_t<
                absl::conjunction<NotSameRef<OptionalCompactString, T>,
                                  NotSameRef<std::nullptr_t, T>,
                                  std::is_convertible<T&&, BytesRef>>::value,
                int> = 0>
  friend bool operator==(const OptionalCompactString& a, T&& b) {
    if (a.repr_ == kNullRepr) return false;
    return *a == absl::string_view(b);
  }
  template <typename T,
            std::enable_if_t<
                absl::conjunction<NotSameRef<OptionalCompactString, T>,
                                  NotSameRef<std::nullptr_t, T>,
                                  std::is_convertible<T&&, BytesRef>>::value,
                int> = 0>
  friend StrongOrdering RIEGELI_COMPARE(const OptionalCompactString& a, T&& b) {
    if (a.repr_ == kNullRepr) return StrongOrdering::less;
    return riegeli::Compare(*a, absl::string_view(b));
  }

 private:
  static constexpr uintptr_t kNullRepr = 0;

  uintptr_t repr_ = kNullRepr;
};

}  // namespace riegeli

#endif  // RIEGELI_BASE_OPTIONAL_COMPACT_STRING_H_
