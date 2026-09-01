// Copyright 2026 Google LLC
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

#ifndef RIEGELI_INTERNED_INTERNED_STRING_H_
#define RIEGELI_INTERNED_INTERNED_STRING_H_

#include <stddef.h>

#include <array>
#include <cstddef>
#include <ostream>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/hash/hash.h"
#include "absl/numeric/bits.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/external_data.h"
#include "riegeli/base/global.h"
#include "riegeli/base/iterable.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/shared_ptr.h"
#include "riegeli/base/type_traits.h"
#include "riegeli/interned/interned_common_internal.h"
#include "riegeli/interned/interned_internal.h"
#include "riegeli/interned/interned_string_internal.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

// Default template parameter `Encoder` for `InternedString` and
// `LocallyInternedString`.
using interned_internal::DefaultStringEncoder;

// Default template parameter `num_shards` for `InternedString`.
// Also, a default template parameter for `Concurrent` nested types.
using interned_internal::kDefaultInternerNumShards;

template <typename Encoder, typename Interner>
class BasicInternedString;

namespace interned_internal {

template <typename Encoder, typename Tag, typename Mutex, size_t num_shards,
          size_t alignment>
class GlobalStringInterner;

template <typename Encoder, typename Tag, typename Mutex, size_t num_shards,
          size_t alignment>
class LocalStringInterner;

// The public name of `OptionalInternedString` is `InternedString::Optional`.
//
// `InternedString` refers to a string, ensuring that equal strings are shared
// to minimize memory usage.
//
// In contrast to `InternedString`, `InternedString::Optional` can be null.
// It is more efficient than `std::optional<InternedString>`.
//
// See `InternedString` for details.
template <typename Encoder, typename InternerParam>
class OptionalInternedString
    : public WithCompare<OptionalInternedString<Encoder, InternerParam>,
                         std::nullptr_t> {
 public:
  // Navigates between `InternedString` and `InternedString::Optional`.
  using NotOptional = BasicInternedString<Encoder, InternerParam>;
  using Optional = OptionalInternedString;

  // The interner type. See `InternedString::Interner` for details.
  using Interner = InternerParam;

  // Maximum supported string size.
  static constexpr size_t kMaxSize =
      InternedStringRepr<Encoder, InternerParam>::kMaxSize;

  // Creates a null `InternedString::Optional`.
  //
  // This differs from the default constructor of `InternedString`.
  OptionalInternedString() = default;
  /*implicit*/ OptionalInternedString(std::nullptr_t) {}
  OptionalInternedString& operator=(std::nullptr_t) {
    shared_repr_ = nullptr;
    return *this;
  }

  // Creates an `InternedString::Optional` holding the copied string,
  // or sharing an existing string if an equal string already exists.
  //
  // This constructor is available when the `Interner` is global.
  // For a local interner, `Interner::Intern()` must be used instead.
  template <typename Arg, typename DependentInterner = Interner,
            std::enable_if_t<
                std::conjunction_v<std::is_empty<DependentInterner>,
                                   NotSameRef<OptionalInternedString, Arg>,
                                   NotSameRef<std::nullptr_t, Arg>,
                                   SupportedByEncoderForIntern<Arg, Encoder>>,
                int> = 0>
  explicit OptionalInternedString(Arg&& arg)
      : shared_repr_(Interner::InternInternal(std::forward<Arg>(arg))) {}

  // Convert `const char*` to `absl::string_view` early to compute `strlen()`
  // once and to avoid separate template instantiations for `char[length + 1]`.
  template <typename DependentInterner = Interner,
            typename DependentEncoder = Encoder,
            std::enable_if_t<
                std::conjunction_v<std::is_empty<DependentInterner>,
                                   SupportedByEncoderForIntern<
                                       absl::string_view, DependentEncoder>>,
                int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE explicit OptionalInternedString(const char* arg)
      : shared_repr_(Interner::InternInternal(absl::string_view(arg))) {}
  template <typename DependentInterner = Interner,
            typename DependentEncoder = Encoder,
            std::enable_if_t<
                std::conjunction_v<std::is_empty<DependentInterner>,
                                   SupportedByEncoderForIntern<
                                       absl::string_view, DependentEncoder>>,
                int> = 0>
  OptionalInternedString& operator=(absl::string_view arg) {
    *this = OptionalInternedString(arg);
    return *this;
  }

  OptionalInternedString(const OptionalInternedString& that) = default;
  OptionalInternedString& operator=(const OptionalInternedString& that) =
      default;

  // A moved-from `InternedString::Optional` is left null.
  OptionalInternedString(OptionalInternedString&& that) = default;
  OptionalInternedString& operator=(OptionalInternedString&& that) = default;

  // Returns an immortal `InternedString` with a specific value. This function
  // is available when the `Interner` is global.
  //
  // This avoids finding the string each time, and adjusting its reference count
  // is optimized.
  //
  // The `construct` callable should be a lambda with no captures, returning
  // an argument for some `InternedString` constructor.
  template <typename Construct,
            std::enable_if_t<std::conjunction_v<std::is_empty<Interner>,
                                                std::is_empty<Construct>,
                                                std::is_invocable<Construct>>,
                             int> = 0>
  static const NotOptional& Immortal(Construct construct) {
    return Interner::Immortal(construct);
  }

  // Returns `true` if not null.
  explicit operator bool() const { return repr() != nullptr; }

  // Converts from `InternedString::Optional` to `InternedString`.
  NotOptional not_optional() const& {
    RIEGELI_ASSERT(repr() != nullptr)
        << "Failed precondition of InternedString::Optional::not_optional(): "
           "null pointer";
    return NotOptional(shared_repr_);
  }
  NotOptional not_optional() && {
    RIEGELI_ASSERT(repr() != nullptr)
        << "Failed precondition of InternedString::Optional::not_optional(): "
           "null pointer";
    return NotOptional(std::move(shared_repr_));
  }
  NotOptional NotOptionalOrDie() const& {
    RIEGELI_CHECK(repr() != nullptr)
        << "Failed precondition of "
           "InternedString::Optional::NotOptionalOrDie(): "
           "null pointer";
    return NotOptional(shared_repr_);
  }
  NotOptional NotOptionalOrDie() && {
    RIEGELI_CHECK(repr() != nullptr)
        << "Failed precondition of "
           "InternedString::Optional::NotOptionalOrDie(): "
           "null pointer";
    return NotOptional(std::move(shared_repr_));
  }

  // Dereferences the pointer.
  absl::string_view operator*() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT(repr() != nullptr)
        << "Failed precondition of InternedString::Optional::operator*: "
           "null pointer";
    return shared_repr().value();
  }
  ArrowProxy<absl::string_view> operator->() const
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT(repr() != nullptr)
        << "Failed precondition of InternedString::Optional::operator->: "
           "null pointer";
    return ArrowProxy<absl::string_view>(shared_repr().value());
  }

  // Dereferences the pointer, crashing the process if null.
  absl::string_view value() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_CHECK(repr() != nullptr)
        << "Failed precondition of InternedString::Optional::value(): "
           "null pointer";
    return shared_repr().value();
  }

  // Equality of non-null `InternedString::Optional` objects corresponds to
  // equality of the strings they refer to, but is fast, comparing the pointers.
  //
  // Other comparisons are also consistent with the strings, but only the case
  // of equal strings is optimized.
  //
  // All comparisons are valid only for `InternedString::Optional` objects
  // coming from the same interner.

  friend bool operator==(const Optional& a, const Optional& b) {
    return a.repr() == b.repr();
  }
  friend auto RIEGELI_COMPARE(const Optional& a, const Optional& b) {
    using Ordering = decltype(riegeli::Compare(*a, *b));
    if (a.repr() == b.repr()) return Ordering(StrongOrdering::equal);
    if (a.repr() == nullptr) return Ordering(StrongOrdering::less);
    if (b.repr() == nullptr) return Ordering(StrongOrdering::greater);
    return riegeli::Compare(*a, *b);
  }

  friend bool operator==(const Optional& a, std::nullptr_t) {
    return a.repr() == nullptr;
  }
  friend StrongOrdering RIEGELI_COMPARE(const Optional& a, std::nullptr_t) {
    if (a.repr() == nullptr) return StrongOrdering::equal;
    return StrongOrdering::greater;
  }

  template <
      typename Other,
      std::enable_if_t<
          std::conjunction_v<
              NotSameRef<Optional, Other>,
              std::negation<std::is_base_of<WithEqualMarker<Other>, Other>>,
              HasEqual<absl::string_view, const Other&>>,
          int> = 0>
  friend bool operator==(const Optional& a, const Other& b) {
    if (a.repr() == nullptr) return false;
    return *a == b;
  }
  template <
      typename Other,
      std::enable_if_t<
          std::conjunction_v<
              NotSameRef<Optional, Other>,
              std::negation<std::is_base_of<WithCompareMarker<Other>, Other>>,
              std::disjunction<HasCompare<absl::string_view, const Other&>,
                               HasLessThan<absl::string_view, const Other&>>>,
          int> = 0>
  friend auto RIEGELI_COMPARE(const Optional& a, const Other& b) {
    if constexpr (HasCompare<absl::string_view, const Other&>::value) {
      using Ordering = decltype(riegeli::Compare(*a, b));
      if (a.repr() == nullptr) return Ordering(StrongOrdering::less);
      return riegeli::Compare(*a, b);
    } else {
      if (a.repr() == nullptr) return StrongOrdering::less;
      if (*a == b) return StrongOrdering::equal;
      if (*a < b) return StrongOrdering::less;
      return StrongOrdering::greater;
    }
  }

#if !__cpp_impl_three_way_comparison
  template <
      typename Other,
      std::enable_if_t<
          std::conjunction_v<
              NotSameRef<Optional, Other>,
              std::negation<std::is_base_of<WithEqualMarker<Other>, Other>>,
              HasEqual<const Other&, absl::string_view>>,
          int> = 0>
  friend bool operator==(const Other& a, const Optional& b) {
    if (b.repr() == nullptr) return false;
    return a == *b;
  }
  template <
      typename Other,
      std::enable_if_t<
          std::conjunction_v<
              NotSameRef<Optional, Other>,
              std::negation<std::is_base_of<WithCompareMarker<Other>, Other>>,
              std::disjunction<HasCompare<const Other&, absl::string_view>,
                               HasLessThan<const Other&, absl::string_view>>>,
          int> = 0>
  friend auto RIEGELI_COMPARE(const Other& a, const Optional& b) {
    if constexpr (HasCompare<const Other&, absl::string_view>::value) {
      using Ordering = decltype(riegeli::Compare(a, *b));
      if (b.repr() == nullptr) return Ordering(StrongOrdering::greater);
      return riegeli::Compare(a, *b);
    } else {
      if (b.repr() == nullptr) return StrongOrdering::greater;
      if (a == *b) return StrongOrdering::equal;
      if (a < *b) return StrongOrdering::less;
      return StrongOrdering::greater;
    }
  }
#endif

  // `InternedString::ByAddress` is implicitly convertible from `InternedString`
  // or `InternedString::Optional`, but instances are compared by address. This
  // is more efficient, but the order is arbitrary, consistent within the
  // process.
  //
  // `std::less<ByAddress>` can be used as a comparator for algorithms over
  // `InternedString` or `InternedString::Optional`.
  class ByAddress;

  // Returns this object wrapped in `ByAddress`.
  ByAddress by_address() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return ByAddress(*this);
  }

  // Hashing `InternedString` or `InternedString::Optional` is fast, hashing
  // the pointer.
  template <typename HashState>
  friend HashState AbslHashValue(HashState hash_state, const Optional& self) {
    return HashState::combine(std::move(hash_state), self.repr());
  }

  // Default hash and equality for containers with `InternedString` or
  // `InternedString::Optional` as the key type, hashing and comparing by
  // address, supporting heterogeneous lookup against `NotOptional` and
  // `Optional`.
  struct absl_container_hash;
  struct absl_container_eq;

  // Hash and equality for containers with `InternedString` or
  // `InternedString::Optional` as the key type, consistent with the
  // underlying value, supporting heterogeneous lookup. This is opt-in because
  // heterogeneous hashing is more expensive than pointer hashing.
  struct ValueHash;
  struct ValueEq;

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const Optional* self,
                                        MemoryEstimator& memory_estimator) {
    if (self->repr() == nullptr) return;
    memory_estimator.RegisterSubobjects(&self->shared_repr());
  }

  // Supports `riegeli::Debug()`.
  template <typename DebugStream>
  friend void RiegeliDebug(const Optional& src, DebugStream& dest) {
    if (src == nullptr) {
      dest.Debug(nullptr);
    } else {
      dest.Debug(*src);
    }
  }

  // Returns a snapshot of the current reference count.
  size_t GetRefCount() const { return shared_repr().GetRefCount(); }

 protected:
  using InternedRepr = InternedStringRepr<Encoder, Interner>;
  using SharedRepr = SharedStringRepr<Encoder, Interner>;

  explicit OptionalInternedString(absl_nullable SharedRepr repr)
      : shared_repr_(std::move(repr)) {}

  const SharedRepr& shared_repr() const {
    RIEGELI_ASSERT(shared_repr_ != nullptr)
        << "Failed precondition of InternedString::Optional::shared_repr(): "
           "null pointer";
    return shared_repr_;
  }
  const char* absl_nullable repr() const { return shared_repr_.repr(); }

 private:
  friend NotOptional;  // For `repr()`.
  friend Interner;     // For `Optional(SharedRepr)`.

  absl_nullable SharedRepr shared_repr_;
};

}  // namespace interned_internal

// The recommended name of `BasicInternedString<>` with default template
// parameters is `InternedString`, avoiding spelling `<>` in the common case.
//
// `InternedString` refers to a string, ensuring that equal strings are shared
// to minimize memory usage.
//
// `InternedString` is never null, except when moved-from, in which case most
// operations are undefined. See `InternedString::Optional` for a variant that
// can be null. `InternedString` is generally preferred over
// `InternedString::Optional`.
//
// See `Interned` for a general variant supporting other types of objects.
//
// `InternedString` objects are created by an interner, which maintains a set of
// strings to share. An interner can be global (represented by a stateless type)
// or local (managed explicitly). The default is global.
//
// Interned strings are destroyed and erased from the interner when all
// references to them are dropped. See `ArenaInternedString` for a variant that
// is faster but does not delete strings until the interner is destroyed.
//
// By default, interning a string supports heterogeneous lookup against
// `absl::Cord`. To extend this to other types, provide `Encoder` which provides
// `Hash` and `Eq` type aliases and the following static members:
// ```
//   using Hash = ...;
//   using Eq = ...;
//   static bool EncodedEmpty(const T& src);
//   static size_t EncodedSize(const T& src);
//   static void Encode(const T& src, char* dest);
// ```
//
// Asymptotic memory usage per interned string, assuming length up to 127:
//   global interner: heap(length + 9) + 14.8
//   local interner: heap(length + 17) + 14.8
//
// Breakdown:
//  + entry in `absl::flat_hash_set<const char*>`: 8 / (7 * ln(2)) * (8 + 1)
//  + heap-allocated {
//    + reference count: 8
//    + interner: 8 if local
//    + length: 1, 2, or 8
//    + contents: length
//  }
//
// Interned handle: 8
//
// Among the template parameters, only `Encoder` should be specified explicitly.
// Other parameters should be specified by using `LocallyInternedString` or by
// nested types `WithTag`, `Concurrent`, and `WithAlignment`.
//
// `InternedString` derives from `InternedString::Optional`. See
// `InternedString::Optional` for inherited operations.
template <typename Encoder = DefaultStringEncoder,
          typename InternerParam = interned_internal::GlobalStringInterner<
              Encoder, /*Tag=*/void, absl::Mutex,
              kDefaultInternerNumShards<absl::Mutex>, /*alignment=*/1>>
class BasicInternedString
    : public interned_internal::OptionalInternedString<Encoder, InternerParam>,
      public WithCompare<
          BasicInternedString<Encoder, InternerParam>,
          interned_internal::OptionalInternedString<Encoder, InternerParam>,
          std::nullptr_t> {
 public:
  // Changes the tag type of the interner.
  //
  // Interned strings with distinct tags are managed by separate types of
  // interners, even if other template parameters are the same. This allows
  // annotating the type with its role for improved type safety. This forces
  // separation of interners, which can make lookups more efficient.
  template <typename NewTag>
  using WithTag =
      BasicInternedString<Encoder,
                          typename InternerParam::template WithTag<NewTag>>;

  // Tunes the interner for concurrency.
  //
  // By default, a global interner has multiple shards, while a local interner
  // has a single shard. With more shards, parallel usage is less likely to
  // cause contention.
  //
  // `Mutex` protects the set of object pointers in each shard.
  //
  // A mutex must support `lock()`, `unlock()`, `lock_shared()`, and
  // `unlock_shared()`, analogously to `absl::Mutex`.
  template <typename NewMutex = absl::Mutex,
            size_t new_num_shards = kDefaultInternerNumShards<NewMutex>>
  using Concurrent =
      BasicInternedString<Encoder, typename InternerParam::template Concurrent<
                                       NewMutex, new_num_shards>>;

  // Configures the alignment of string data.
  //
  // If the strings encode another type (supported by custom `Encoder`),
  // they can be `reinterpret_cast` to a type with the required alignment.
  template <size_t new_alignment>
  using WithAlignment = BasicInternedString<
      Encoder, typename InternerParam::template WithAlignment<new_alignment>>;

  // Navigates between `InternedString` and `InternedString::Optional`.
  using NotOptional = typename BasicInternedString::NotOptional;
  using Optional = typename BasicInternedString::Optional;

  // The interner type. It is used for interning new strings, although a global
  // interner is usually accessed implicitly by the constructors of
  // `InternedString`. The interner also provides statistics.
  using Interner = typename BasicInternedString::Interner;

  // A default-constructed `InternedString` holds an empty string. The empty
  // string is immortal, and adjusting its reference count is optimized.
  //
  // This constructor is available when the `Interner` is global.
  //
  // This differs from the default constructor of `InternedString::Optional`.
  BasicInternedString() noexcept : Optional(Interner::InternInternal()) {
    static_assert(std::is_empty_v<Interner>);
  }

  // Constructor from `nullptr` is present in `InternedString::Optional` but
  // deleted in `InternedString`.
  BasicInternedString(std::nullptr_t) = delete;
  BasicInternedString& operator=(std::nullptr_t) = delete;

  // Creates an `InternedString` holding the copied string, or sharing an
  // existing string if an equal string already exists.
  //
  // This constructor is available when the `Interner` is global.
  // For a local interner, `Interner::Intern()` must be used instead.
  template <
      typename Arg, typename DependentInterner = Interner,
      std::enable_if_t<
          std::conjunction_v<
              std::is_empty<DependentInterner>, NotSameRef<Optional, Arg>,
              NotSameRef<std::nullptr_t, Arg>,
              interned_internal::SupportedByEncoderForIntern<Arg, Encoder>>,
          int> = 0>
  explicit BasicInternedString(Arg&& arg) : Optional(std::forward<Arg>(arg)) {}

  // Convert `const char*` to `absl::string_view` early to compute `strlen()`
  // once and to avoid separate template instantiations for `char[length + 1]`.
  template <typename DependentInterner = Interner,
            typename DependentEncoder = Encoder,
            std::enable_if_t<std::conjunction_v<
                                 std::is_empty<DependentInterner>,
                                 interned_internal::SupportedByEncoderForIntern<
                                     absl::string_view, DependentEncoder>>,
                             int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE explicit BasicInternedString(const char* arg)
      : Optional(arg) {}
  template <typename DependentInterner = Interner,
            typename DependentEncoder = Encoder,
            std::enable_if_t<std::conjunction_v<
                                 std::is_empty<DependentInterner>,
                                 interned_internal::SupportedByEncoderForIntern<
                                     absl::string_view, DependentEncoder>>,
                             int> = 0>
  BasicInternedString& operator=(absl::string_view arg) {
    *this = NotOptional(arg);
    return *this;
  }

  BasicInternedString(const BasicInternedString& that) = default;
  BasicInternedString& operator=(const BasicInternedString& that) = default;

  // A moved-from `InternedString` does not contain an object. Most operations
  // are undefined, except for assignment and `valueless_after_move()`.
  BasicInternedString(BasicInternedString&& that) = default;
  BasicInternedString& operator=(BasicInternedString&& that) = default;

  // Returns `true` because `InternedString` is never null, except when
  // moved-from.
  explicit operator bool() const {
    RIEGELI_ASSERT(this->repr() != nullptr) << "Moved-from InternedString";
    return true;
  }

  // Returns `true` when the `InternedString` is null because it was moved-from.
  bool valueless_after_move() const { return this->repr() == nullptr; }

  // Dereferences the pointer.
  /*implicit*/ operator absl::string_view() const
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT(this->repr() != nullptr) << "Moved-from InternedString";
    return this->shared_repr().value();
  }
  absl::string_view value() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT(this->repr() != nullptr) << "Moved-from InternedString";
    return this->shared_repr().value();
  }

  bool empty() const {
    RIEGELI_ASSERT(this->repr() != nullptr) << "Moved-from InternedString";
    return this->shared_repr().empty();
  }
  const char* data() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT(this->repr() != nullptr) << "Moved-from InternedString";
    return this->shared_repr().data();
  }
  size_t size() const {
    RIEGELI_ASSERT(this->repr() != nullptr) << "Moved-from InternedString";
    return this->shared_repr().size();
  }

  const char& operator[](size_t index) const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT(this->repr() != nullptr) << "Moved-from InternedString";
    RIEGELI_ASSERT_LT(index, size())
        << "Failed precondition of InternedString::operator[]: "
           "index out of range";
    return data()[index];
  }

  const char& at(size_t index) const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT(this->repr() != nullptr) << "Moved-from InternedString";
    RIEGELI_CHECK_LT(index, size())
        << "Failed precondition of InternedString::at(): index out of range";
    return data()[index];
  }

  const char& front() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT(this->repr() != nullptr) << "Moved-from InternedString";
    RIEGELI_ASSERT(!empty())
        << "Failed precondition of InternedString::front(): empty string";
    return data()[0];
  }

  const char& back() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT(this->repr() != nullptr) << "Moved-from InternedString";
    RIEGELI_ASSERT(!empty())
        << "Failed precondition of InternedString::back(): empty string";
    return data()[size() - 1];
  }

  // Equality of `InternedString` objects corresponds to equality of the
  // strings they refer to, but is fast, comparing the pointers.
  //
  // Other comparisons are also consistent with the strings, but only the case
  // of equal strings is optimized.
  //
  // All comparisons are valid only for `InternedString` objects coming from the
  // same interner.

  friend bool operator==(const NotOptional& a, const NotOptional& b) {
    RIEGELI_ASSERT(a.repr() != nullptr) << "Moved-from InternedString";
    RIEGELI_ASSERT(b.repr() != nullptr) << "Moved-from InternedString";
    return a.repr() == b.repr();
  }
  friend auto RIEGELI_COMPARE(const NotOptional& a, const NotOptional& b) {
    RIEGELI_ASSERT(a.repr() != nullptr) << "Moved-from InternedString";
    RIEGELI_ASSERT(b.repr() != nullptr) << "Moved-from InternedString";
    using Ordering = decltype(riegeli::Compare(*a, *b));
    if (a.repr() == b.repr()) return Ordering(StrongOrdering::equal);
    return riegeli::Compare(*a, *b);
  }

  friend bool operator==(const NotOptional& a, const Optional& b) {
    RIEGELI_ASSERT(a.repr() != nullptr) << "Moved-from InternedString";
    return a.repr() == b.repr();
  }
  friend auto RIEGELI_COMPARE(const NotOptional& a, const Optional& b) {
    RIEGELI_ASSERT(a.repr() != nullptr) << "Moved-from InternedString";
    using Ordering = decltype(riegeli::Compare(*a, *b));
    if (a.repr() == b.repr()) return Ordering(StrongOrdering::equal);
    if (b.repr() == nullptr) return Ordering(StrongOrdering::greater);
    return riegeli::Compare(*a, *b);
  }

  friend bool operator==(const NotOptional& a, std::nullptr_t) {
    RIEGELI_ASSERT(a.repr() != nullptr) << "Moved-from InternedString";
    return false;
  }
  friend auto RIEGELI_COMPARE(const NotOptional& a, std::nullptr_t) {
    RIEGELI_ASSERT(a.repr() != nullptr) << "Moved-from InternedString";
    return StrongOrdering::greater;
  }

  template <
      typename Other,
      std::enable_if_t<
          std::conjunction_v<
              NotSameRef<Optional, Other>,
              std::negation<std::is_base_of<WithEqualMarker<Other>, Other>>,
              HasEqual<absl::string_view, const Other&>>,
          int> = 0>
  friend bool operator==(const NotOptional& a, const Other& b) {
    RIEGELI_ASSERT(a.repr() != nullptr) << "Moved-from InternedString";
    return *a == b;
  }
  template <
      typename Other,
      std::enable_if_t<
          std::conjunction_v<
              NotSameRef<Optional, Other>,
              std::negation<std::is_base_of<WithCompareMarker<Other>, Other>>,
              std::disjunction<HasCompare<absl::string_view, const Other&>,
                               HasLessThan<absl::string_view, const Other&>>>,
          int> = 0>
  friend auto RIEGELI_COMPARE(const NotOptional& a, const Other& b) {
    RIEGELI_ASSERT(a.repr() != nullptr) << "Moved-from InternedString";
    if constexpr (HasCompare<absl::string_view, const Other&>::value) {
      return riegeli::Compare(*a, b);
    } else {
      if (*a == b) return StrongOrdering::equal;
      if (*a < b) return StrongOrdering::less;
      return StrongOrdering::greater;
    }
  }

#if !__cpp_impl_three_way_comparison
  template <
      typename Other,
      std::enable_if_t<
          std::conjunction_v<
              NotSameRef<Optional, Other>,
              std::negation<std::is_base_of<WithEqualMarker<Other>, Other>>,
              HasEqual<const Other&, absl::string_view>>,
          int> = 0>
  friend bool operator==(const Other& a, const NotOptional& b) {
    RIEGELI_ASSERT(b.repr() != nullptr) << "Moved-from InternedString";
    return a == *b;
  }
  template <
      typename Other,
      std::enable_if_t<
          std::conjunction_v<
              NotSameRef<Optional, Other>,
              std::negation<std::is_base_of<WithCompareMarker<Other>, Other>>,
              std::disjunction<HasCompare<const Other&, absl::string_view>,
                               HasLessThan<const Other&, absl::string_view>>>,
          int> = 0>
  friend auto RIEGELI_COMPARE(const Other& a, const NotOptional& b) {
    RIEGELI_ASSERT(b.repr() != nullptr) << "Moved-from InternedString";
    if constexpr (HasCompare<const Other&, absl::string_view>::value) {
      return riegeli::Compare(a, *b);
    } else {
      if (a == *b) return StrongOrdering::equal;
      if (a < *b) return StrongOrdering::less;
      return StrongOrdering::greater;
    }
  }
#endif

  // Default stringification by `absl::StrCat()` etc.
  template <typename Sink>
  friend void AbslStringify(Sink& dest, const NotOptional& src) {
    dest.Append(absl::string_view(src));
  }

  friend std::ostream& operator<<(std::ostream& dest, const NotOptional& src) {
    return dest << absl::string_view(src);
  }

  // Indicates support for:
  //  * `ExternalRef(const BasicInternedString&)`
  //  * `ExternalRef(BasicInternedString&&)`
  //  * `ExternalRef(const BasicInternedString&, substr)`
  //  * `ExternalRef(BasicInternedString&&, substr)`
  friend void RiegeliSupportsExternalRef(const NotOptional* absl_nullable) {}

  // Supports `ExternalRef`.
  friend ExternalStorage RiegeliToExternalStorage(NotOptional* self) {
    return RiegeliToExternalStorage(&self->shared_repr_);
  }

 private:
  friend Optional;  // For `Optional(SharedRepr)`.
  friend Interner;  // For `InternedString(SharedRepr)`.

  explicit BasicInternedString(typename Optional::SharedRepr repr)
      : Optional(std::move(repr)) {}
};

// `InternedString` is `BasicInternedString<>` with default template parameters,
// avoiding spelling `<>` in the common case. See `BasicInternedString` for
// details.
using InternedString = BasicInternedString<>;

// `BasicLocallyInternedString` is an instantiation of `BasicInternedString`
// with a local interner. See `LocalStringInterner` for details.
template <typename Encoder = DefaultStringEncoder>
using BasicLocallyInternedString =
    BasicInternedString<Encoder, interned_internal::LocalStringInterner<
                                     Encoder, /*Tag=*/void, absl::Mutex,
                                     /*num_shards=*/1, /*alignment=*/1>>;

// `LocallyInternedString` is `BasicLocallyInternedString<>` with default
// template parameters, avoiding spelling `<>` in the common case. See
// `LocalStringInterner` for details.
using LocallyInternedString = BasicLocallyInternedString<>;

namespace interned_internal {

// The public name of `GlobalStringInterner` is `InternedString::Interner`.
//
// `InternedString::Interner` represents a global interner for the given
// template parameters. See `LocallyInternedString::Interner` for a non-global
// version.
template <typename Encoder, typename Tag, typename MutexParam,
          size_t num_shards, size_t alignment>
class GlobalStringInterner {
 public:
  static_assert(absl::has_single_bit(alignment));

  // Changes the tag type of the interner. See `InternedString::WithTag` for
  // details.
  template <typename NewTag>
  using WithTag =
      GlobalStringInterner<Encoder, NewTag, MutexParam, num_shards, alignment>;

  // Tunes the interner for concurrency. See `InternedString::Concurrent` for
  // details.
  //
  // By default, a global interner is tuned for concurrency and has multiple
  // shards.
  template <typename NewMutex = absl::Mutex,
            size_t new_num_shards = kDefaultInternerNumShards<NewMutex>>
  using Concurrent =
      GlobalStringInterner<Encoder, Tag, NewMutex, new_num_shards, alignment>;

  // Configures the alignment of string data. See
  // `InternedString::WithAlignment` for details.
  template <size_t new_alignment>
  using WithAlignment =
      GlobalStringInterner<Encoder, Tag, MutexParam, num_shards, new_alignment>;

  // References to interned strings. See `InternedString` and
  // `InternedString::Optional` for details.
  using Interned = BasicInternedString<Encoder, GlobalStringInterner>;
  using OptionalInterned =
      OptionalInternedString<Encoder, GlobalStringInterner>;

  // Since `InternedString::Interner` is stateless, all instances are
  // equivalent. Member functions are static. Instantiation is provided
  // for consistency with other interner categories.
  GlobalStringInterner() = default;

  GlobalStringInterner(const GlobalStringInterner& that) = default;
  GlobalStringInterner& operator=(const GlobalStringInterner& that) = default;

  // `Intern()` is equivalent to the `InternedString` constructor, which is
  // preferred. `Intern()` is provided for consistency with other interner
  // categories.

  static const Interned& Intern() {
    return riegeli::Global([] { return Interned(InternInternal()); });
  }

  template <typename Arg,
            std::enable_if_t<SupportedByEncoderForIntern<Arg, Encoder>::value,
                             int> = 0>
  static Interned Intern(const Arg& arg) {
    return Interned(InternInternal(arg));
  }

  // Convert `const char*` to `absl::string_view` early to compute `strlen()`
  // once and to avoid separate template instantiations for `char[length + 1]`.
  template <typename DependentEncoder = Encoder,
            std::enable_if_t<SupportedByEncoderForIntern<
                                 absl::string_view, DependentEncoder>::value,
                             int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE static Interned Intern(const char* arg) {
    return Interned(InternInternal(absl::string_view(arg)));
  }

  // Finds an existing `InternedString` matching the given argument, or returns
  // null if there is currently none.
  //
  // Calling `Find()` instead of `Intern()` is useful if further processing is
  // possible only if the string already exists, such as looking up in a map
  // with interned keys.
  template <typename Arg,
            std::enable_if_t<SupportedByEncoderForIntern<Arg, Encoder>::value,
                             int> = 0>
  static OptionalInterned Find(const Arg& arg) {
    return OptionalInterned(FindInternal(arg));
  }

  // Convert `const char*` to `absl::string_view` early to compute `strlen()`
  // once and to avoid separate template instantiations for `char[length + 1]`.
  template <typename DependentEncoder = Encoder,
            std::enable_if_t<SupportedByEncoderForIntern<
                                 absl::string_view, DependentEncoder>::value,
                             int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE static OptionalInterned Find(const char* arg) {
    return OptionalInterned(FindInternal(absl::string_view(arg)));
  }

  // `Immortal()` is equivalent to `InternedString::Immortal()`, which is
  // preferred. `Immortal()` is provided for consistency with other interner
  // categories.
  //
  // See `InternedString::Immortal()` for details.
  template <typename Construct,
            std::enable_if_t<std::conjunction_v<std::is_empty<Construct>,
                                                std::is_invocable<Construct>>,
                             int> = 0>
  static const Interned& Immortal(Construct /*construct*/) {
    return riegeli::Global([] {
      Interned interned{Construct()()};
      interned.shared_repr()->Immortalize();
      return interned;
    });
  }

  // Returns a snapshot of the number of strings managed by the interner.
  static size_t NumObjects() {
    size_t count = 0;
    for (const Shard& shard : GetShards()) {
      count += shard.NumObjects();
    }
    return count;
  }

  // Returns a snapshot of the total number of references to interned strings
  // managed by the interner.
  static size_t TotalNumReferences() {
    size_t count = 0;
    for (const Shard& shard : GetShards()) {
      count += shard.TotalNumReferences();
    }
    return count;
  }

  // Supports `MemoryEstimator`.
  //
  // Given a large number of shared strings, `MemoryEstimatorSimplified` is
  // less accurate but much more efficient than `MemoryEstimatorDefault`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const GlobalStringInterner* /*self*/,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&GetShards());
  }

 private:
  using Mutex = MutexParam;
  using InternedRepr = InternedStringRepr<Encoder, GlobalStringInterner>;
  using SharedRepr = SharedStringRepr<Encoder, GlobalStringInterner>;
  using Shard = Shard<InternedRepr, SharedRepr, typename Encoder::Hash,
                      typename Encoder::Eq, GlobalStringInterner>;
  using ShardArray = std::array<Shard, num_shards>;

  static constexpr size_t kAlignment = alignment;

  friend Interned;          // For `InternInternal()`.
  friend OptionalInterned;  // For `InternInternal()`.
  friend InternedRepr;      // For `kAlignment`.
  friend SharedRepr;        // For `kAlignment`.
  friend Shard;             // For `Mutex` and `GetShard()`.

  template <typename Arg>
  static SharedRepr InternInternal(const Arg& value) {
    if (Encoder::EncodedEmpty(value)) return InternInternal();
    const size_t hash = typename Encoder::Hash()(value);
    return GetShard(hash).Intern(value, hash, GlobalStringInterner());
  }

  template <typename Arg>
  static SharedRepr FindInternal(const Arg& value) {
    if (Encoder::EncodedEmpty(value)) return InternInternal();
    const size_t hash = typename Encoder::Hash()(value);
    return GetShard(hash).Find(value, hash);
  }

  static SharedRepr InternInternal() {
    static constexpr InternedRepr kEmpty;
    RIEGELI_ASSUME_EQ(kEmpty.size(), 0u);
    SharedRepr repr(&kEmpty);
    RIEGELI_ASSUME_EQ(repr.size(), 0u);
    return repr;
  }

  static Shard& GetShard(size_t hash) {
    return GetShards()[ShardIndex<num_shards>(hash)];
  }

  static ShardArray& GetShards() {
    return riegeli::Global([] { return ShardArray(); });
  }
};

// The public name of `LocalStringInterner` is
// `LocallyInternedString::Interner`.
//
// `LocallyInternedString::Interner` represents an explicitly managed interner.
// It is a shared pointer to a set of interned strings. It does not influence
// their lifetime: if strings are dropped, they are erased from the interner,
// and they can also outlive the interner.
//
// See `InternedString::Interner` for a global version. A global interner is
// more convenient to use. A local interner is required if equality of a given
// family of interned strings is meaningful only within that family, and would
// be incorrect across families, e.g. when only some fields are compared because
// other fields are implicitly equivalent within the family. Efficiency depends
// on usage patterns.
template <typename Encoder, typename Tag, typename MutexParam,
          size_t num_shards, size_t alignment>
class LocalStringInterner {
 public:
  static_assert(absl::has_single_bit(alignment));

  // Changes the tag type of the interner. See `InternedString::WithTag` for
  // details.
  template <typename NewTag>
  using WithTag =
      LocalStringInterner<Encoder, NewTag, MutexParam, num_shards, alignment>;

  // Tunes the interner for concurrency. See `InternedString::Concurrent` for
  // details.
  //
  // By default, a local interner is not tuned for concurrency and has a single
  // shard, but it is still thread-safe.
  template <typename NewMutex = absl::Mutex,
            size_t new_num_shards = kDefaultInternerNumShards<NewMutex>>
  using Concurrent =
      LocalStringInterner<Encoder, Tag, NewMutex, new_num_shards, alignment>;

  // Configures the alignment of string data. See
  // `InternedString::WithAlignment` for details.
  template <size_t new_alignment>
  using WithAlignment =
      LocalStringInterner<Encoder, Tag, MutexParam, num_shards, new_alignment>;

  // References to interned strings. See `InternedString` and
  // `InternedString::Optional` for details.
  using Interned = BasicInternedString<Encoder, LocalStringInterner>;
  using OptionalInterned = OptionalInternedString<Encoder, LocalStringInterner>;

  LocalStringInterner() = default;

  LocalStringInterner(const LocalStringInterner& that) = default;
  LocalStringInterner& operator=(const LocalStringInterner& that) = default;

  LocalStringInterner(LocalStringInterner&& that) = default;
  LocalStringInterner& operator=(LocalStringInterner&& that) = default;

  // Prepares the interner for the expected number of distinct strings.
  // This reduces reallocations.
  void Reserve(size_t capacity) {
    RIEGELI_ASSERT(shards_ != nullptr)
        << "Moved-from LocallyInternedString::Interner";
    if (capacity == 0) return;
    const size_t capacity_per_shard = capacity / num_shards;
    if (capacity_per_shard > 0) {
      for (Shard& shard : *shards_) {
        shard.Reserve(capacity_per_shard);
      }
    }
  }

  // Creates an `InternedString` holding the copied string, or sharing an
  // existing string if an equal string already exists.

  Interned Intern() const { return Intern(absl::string_view()); }

  template <typename Arg,
            std::enable_if_t<interned_internal::SupportedByEncoderForIntern<
                                 Arg, Encoder>::value,
                             int> = 0>
  Interned Intern(const Arg& arg) const {
    return Interned(InternInternal(arg));
  }

  // Convert `const char*` to `absl::string_view` early to compute `strlen()`
  // once and to avoid separate template instantiations for `char[length + 1]`.
  template <std::enable_if_t<interned_internal::SupportedByEncoderForIntern<
                                 absl::string_view, Encoder>::value,
                             int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE Interned Intern(const char* arg) const {
    return Interned(InternInternal(absl::string_view(arg)));
  }

  // Finds an existing `InternedString` matching the given argument, or returns
  // null if there is currently none.
  //
  // Calling `Find()` instead of `Intern()` is useful if further processing is
  // possible only if the string already exists, such as looking up in a map
  // with interned keys.
  template <typename Arg,
            std::enable_if_t<interned_internal::SupportedByEncoderForIntern<
                                 Arg, Encoder>::value,
                             int> = 0>
  OptionalInterned Find(const Arg& arg) const {
    return OptionalInterned(FindInternal(arg));
  }

  // Convert `const char*` to `absl::string_view` early to compute `strlen()`
  // once and to avoid separate template instantiations for `char[length + 1]`.
  template <std::enable_if_t<interned_internal::SupportedByEncoderForIntern<
                                 absl::string_view, Encoder>::value,
                             int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE OptionalInterned Find(const char* arg) const {
    return OptionalInterned(FindInternal(absl::string_view(arg)));
  }

  // Returns a snapshot of the number of strings managed by the interner.
  size_t NumObjects() const {
    RIEGELI_ASSERT(shards_ != nullptr)
        << "Moved-from LocallyInternedString::Interner";
    size_t count = 0;
    for (const Shard& shard : *shards_) {
      count += shard.NumObjects();
    }
    return count;
  }

  // Returns a snapshot of the total number of references to interned strings
  // managed by the interner.
  size_t TotalNumReferences() const {
    RIEGELI_ASSERT(shards_ != nullptr)
        << "Moved-from LocallyInternedString::Interner";
    size_t count = 0;
    for (const Shard& shard : *shards_) {
      count += shard.TotalNumReferences();
    }
    return count;
  }

  // Supports `MemoryEstimator`.
  //
  // Given a large number of shared strings, `MemoryEstimatorSimplified` is
  // less accurate but much more efficient than `MemoryEstimatorDefault`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const LocalStringInterner* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&self->shards_);
  }

 private:
  using Mutex = MutexParam;
  using InternedRepr = InternedStringRepr<Encoder, LocalStringInterner>;
  using SharedRepr = SharedStringRepr<Encoder, LocalStringInterner>;
  using Shard = Shard<InternedRepr, SharedRepr, typename Encoder::Hash,
                      typename Encoder::Eq, LocalStringInterner>;
  using ShardArray = std::array<Shard, num_shards>;

  static constexpr size_t kAlignment = alignment;

  friend Interned;      // For `InternInternal()`.
  friend InternedRepr;  // For `kAlignment`.
  friend SharedRepr;    // For `kAlignment`.
  friend Shard;         // For `Mutex` and `GetShard()`.

  template <typename Arg>
  SharedRepr InternInternal(const Arg& value) const {
    const size_t hash = typename Encoder::Hash()(value);
    return GetShard(hash).Intern(value, hash, *this);
  }

  // Undefined. Called only when a `static_assert` fails anyway.
  static SharedRepr InternInternal();

  template <typename Arg>
  SharedRepr FindInternal(const Arg& value) const {
    const size_t hash = typename Encoder::Hash()(value);
    return GetShard(hash).Find(value, hash);
  }

  Shard& GetShard(size_t hash) const {
    RIEGELI_ASSERT(shards_ != nullptr)
        << "Moved-from LocallyInternedString::Interner";
    return (*shards_)[ShardIndex<num_shards>(hash)];
  }

  SharedPtr<ShardArray> shards_{riegeli::Maker()};
};

// Implementation details follow.

template <typename Encoder, typename Interner>
class OptionalInternedString<Encoder, Interner>::ByAddress
    : public WithCompare<ByAddress> {
 public:
  /*implicit*/ ByAddress(const Optional& interned) : repr_(interned.repr()) {}

  ByAddress(const ByAddress& that) = default;
  ByAddress& operator=(const ByAddress& that) = default;

  friend bool operator==(ByAddress a, ByAddress b) {
    return a.repr_ == b.repr_;
  }
  friend StrongOrdering RIEGELI_COMPARE(ByAddress a, ByAddress b) {
    return riegeli::Compare(a.repr_, b.repr_);
  }

 private:
  const char* absl_nullable repr_;
};

template <typename Encoder, typename Interner>
struct OptionalInternedString<Encoder, Interner>::absl_container_hash {
  using is_transparent = void;
  size_t operator()(const Optional& self) const {
    return absl::HashOf(self.repr());
  }
  size_t operator()(const NotOptional& self) const {
    RIEGELI_ASSERT(self.repr() != nullptr) << "Moved-from InternedString";
    return absl::HashOf(self.repr());
  }
  size_t operator()(std::nullptr_t) const { return absl::HashOf(nullptr); }
};

template <typename Encoder, typename Interner>
struct OptionalInternedString<Encoder, Interner>::absl_container_eq {
  using is_transparent = void;
  bool operator()(const Optional& a, const Optional& b) const {
    return a.repr() == b.repr();
  }
  bool operator()(const Optional& a, const NotOptional& b) const {
    RIEGELI_ASSERT(b.repr() != nullptr) << "Moved-from InternedString";
    return a.repr() == b.repr();
  }
  bool operator()(const NotOptional& a, const Optional& b) const {
    RIEGELI_ASSERT(a.repr() != nullptr) << "Moved-from InternedString";
    return a.repr() == b.repr();
  }
  bool operator()(const NotOptional& a, const NotOptional& b) const {
    RIEGELI_ASSERT(a.repr() != nullptr) << "Moved-from InternedString";
    RIEGELI_ASSERT(b.repr() != nullptr) << "Moved-from InternedString";
    return a.repr() == b.repr();
  }
  bool operator()(const Optional& a, std::nullptr_t) const {
    return a.repr() == nullptr;
  }
  bool operator()(std::nullptr_t, const Optional& b) const {
    return b.repr() == nullptr;
  }
  bool operator()(const NotOptional& a, std::nullptr_t) const {
    RIEGELI_ASSERT(a.repr() != nullptr) << "Moved-from InternedString";
    return false;
  }
  bool operator()(std::nullptr_t, const NotOptional& b) const {
    RIEGELI_ASSERT(b.repr() != nullptr) << "Moved-from InternedString";
    return false;
  }
};

template <typename Encoder, typename Interner>
struct OptionalInternedString<Encoder, Interner>::ValueHash {
  using is_transparent = void;
  size_t operator()(const Optional& self) const {
    if (self.repr() == nullptr) {
      if constexpr (HasTransparentNullptrHash<typename Encoder::Hash>::value) {
        return hash(nullptr);
      } else {
        return absl::HashOf(nullptr);
      }
    }
    return hash(*self);
  }
  size_t operator()(const NotOptional& self) const {
    RIEGELI_ASSERT(self.repr() != nullptr) << "Moved-from InternedString";
    return hash(*self);
  }
  size_t operator()(std::nullptr_t) const {
    if constexpr (HasTransparentNullptrHash<typename Encoder::Hash>::value) {
      return hash(nullptr);
    } else {
      return absl::HashOf(nullptr);
    }
  }
  size_t operator()(absl::string_view arg) const { return hash(arg); }
  template <typename PassedKey, typename DependentHash = typename Encoder::Hash,
            typename = typename DependentHash::is_transparent,
            std::enable_if_t<
                std::conjunction_v<
                    NotSameRef<Optional, PassedKey>,
                    NotSameRef<std::nullptr_t, PassedKey>,
                    NotSameRef<absl::string_view, PassedKey>,
                    std::is_invocable<const DependentHash&, const PassedKey&>>,
                int> = 0>
  size_t operator()(const PassedKey& arg) const {
    return hash(arg);
  }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS typename Encoder::Hash hash;
};

template <typename Encoder, typename Interner>
struct OptionalInternedString<Encoder, Interner>::ValueEq {
  using is_transparent = void;
  bool operator()(const Optional& a, const Optional& b) const {
    return a.repr() == b.repr();
  }
  bool operator()(const Optional& a, const NotOptional& b) const {
    RIEGELI_ASSERT(b.repr() != nullptr) << "Moved-from InternedString";
    return a.repr() == b.repr();
  }
  bool operator()(const NotOptional& a, const Optional& b) const {
    RIEGELI_ASSERT(a.repr() != nullptr) << "Moved-from InternedString";
    return a.repr() == b.repr();
  }
  bool operator()(const NotOptional& a, const NotOptional& b) const {
    RIEGELI_ASSERT(a.repr() != nullptr) << "Moved-from InternedString";
    RIEGELI_ASSERT(b.repr() != nullptr) << "Moved-from InternedString";
    return a.repr() == b.repr();
  }
  bool operator()(const Optional& a, std::nullptr_t) const {
    return a.repr() == nullptr;
  }
  bool operator()(std::nullptr_t, const Optional& b) const {
    return b.repr() == nullptr;
  }
  bool operator()(const NotOptional& a, std::nullptr_t) const {
    RIEGELI_ASSERT(a.repr() != nullptr) << "Moved-from InternedString";
    return false;
  }
  bool operator()(std::nullptr_t, const NotOptional& b) const {
    RIEGELI_ASSERT(b.repr() != nullptr) << "Moved-from InternedString";
    return false;
  }
  bool operator()(const Optional& a, absl::string_view b) const {
    if (a.repr() == nullptr) return false;
    return eq(*a, b);
  }
  bool operator()(absl::string_view a, const Optional& b) const {
    if (b.repr() == nullptr) return false;
    return eq(*b, a);
  }
  bool operator()(const NotOptional& a, absl::string_view b) const {
    RIEGELI_ASSERT(a.repr() != nullptr) << "Moved-from InternedString";
    return eq(*a, b);
  }
  bool operator()(absl::string_view a, const NotOptional& b) const {
    RIEGELI_ASSERT(b.repr() != nullptr) << "Moved-from InternedString";
    return eq(*b, a);
  }
  template <typename PassedKey, typename DependentEq = typename Encoder::Eq,
            typename = typename DependentEq::is_transparent,
            std::enable_if_t<
                std::conjunction_v<NotSameRef<Optional, PassedKey>,
                                   NotSameRef<std::nullptr_t, PassedKey>,
                                   NotSameRef<absl::string_view, PassedKey>,
                                   std::is_invocable<const DependentEq&,
                                                     const absl::string_view&,
                                                     const PassedKey&>>,
                int> = 0>
  bool operator()(const Optional& a, const PassedKey& b) const {
    if (a.repr() == nullptr) return false;
    return eq(*a, b);
  }
  template <typename PassedKey, typename DependentEq = typename Encoder::Eq,
            typename = typename DependentEq::is_transparent,
            std::enable_if_t<
                std::conjunction_v<NotSameRef<Optional, PassedKey>,
                                   NotSameRef<std::nullptr_t, PassedKey>,
                                   NotSameRef<absl::string_view, PassedKey>,
                                   std::is_invocable<const DependentEq&,
                                                     const absl::string_view&,
                                                     const PassedKey&>>,
                int> = 0>
  bool operator()(const PassedKey& a, const Optional& b) const {
    if (b.repr() == nullptr) return false;
    return eq(*b, a);
  }
  template <typename PassedKey, typename DependentEq = typename Encoder::Eq,
            typename = typename DependentEq::is_transparent,
            std::enable_if_t<
                std::conjunction_v<NotSameRef<Optional, PassedKey>,
                                   NotSameRef<std::nullptr_t, PassedKey>,
                                   NotSameRef<absl::string_view, PassedKey>,
                                   std::is_invocable<const DependentEq&,
                                                     const absl::string_view&,
                                                     const PassedKey&>>,
                int> = 0>
  bool operator()(const NotOptional& a, const PassedKey& b) const {
    RIEGELI_ASSERT(a.repr() != nullptr) << "Moved-from InternedString";
    return eq(*a, b);
  }
  template <typename PassedKey, typename DependentEq = typename Encoder::Eq,
            typename = typename DependentEq::is_transparent,
            std::enable_if_t<
                std::conjunction_v<NotSameRef<Optional, PassedKey>,
                                   NotSameRef<std::nullptr_t, PassedKey>,
                                   NotSameRef<absl::string_view, PassedKey>,
                                   std::is_invocable<const DependentEq&,
                                                     const absl::string_view&,
                                                     const PassedKey&>>,
                int> = 0>
  bool operator()(const PassedKey& a, const NotOptional& b) const {
    RIEGELI_ASSERT(b.repr() != nullptr) << "Moved-from InternedString";
    return eq(*b, a);
  }

 private:
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS typename Encoder::Eq eq;
};

}  // namespace interned_internal

}  // namespace riegeli

#endif  // RIEGELI_INTERNED_INTERNED_STRING_H_
