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

#ifndef RIEGELI_INTERNED_STRING_ARENA_H_
#define RIEGELI_INTERNED_STRING_ARENA_H_

#include <stddef.h>
#include <stdint.h>

#include <cstddef>
#include <functional>
#include <limits>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/base/thread_annotations.h"
#include "absl/hash/hash.h"
#include "absl/numeric/bits.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/iterable.h"
#include "riegeli/interned/concurrent_vector_internal.h"
#include "riegeli/interned/interned_common_internal.h"
#include "riegeli/interned/string_arena_internal.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

// Mutex type that does not lock.
using interned_internal::NullMutex;

// Default block sizes for `StringArena`.
using interned_internal::kDefaultArenaMaxBlockSize;
using interned_internal::kDefaultArenaMinBlockSize;

namespace interned_internal {

template <size_t alignment>
class BasicArenaString;

template <typename Mutex, bool concurrent_reads, size_t static_min_block_size,
          size_t static_max_block_size>
class BasicStringArena;

// The public name of `OptionalArenaString` is `ArenaString::Optional`.
//
// `ArenaString` refers to a string stored in `StringArena`.
//
// In contrast to `ArenaString`, `ArenaString::Optional` can be null.
//
// See `ArenaString` for details.
template <size_t alignment>
class OptionalArenaString
    : public WithCompare<OptionalArenaString<alignment>, std::nullptr_t,
                         absl::string_view> {
 public:
  static_assert(absl::has_single_bit(alignment));

  // Navigates between `ArenaString` and `ArenaString::Optional`.
  using NotOptional = BasicArenaString<alignment>;
  using Optional = OptionalArenaString;

  // Maximum supported string size.
  static constexpr size_t kMaxSize = std::numeric_limits<size_t>::max() >> 2;

  // Creates a null `ArenaString::Optional`.
  //
  // This differs from the default constructor of `ArenaString`, which creates
  // an empty string.
  OptionalArenaString() = default;
  /*implicit*/ OptionalArenaString(std::nullptr_t) {}
  OptionalArenaString& operator=(std::nullptr_t) {
    repr_ = nullptr;
    return *this;
  }

  OptionalArenaString(const OptionalArenaString& that) = default;
  OptionalArenaString& operator=(const OptionalArenaString& that) = default;

  // Returns `true` if not null.
  explicit operator bool() const { return repr() != nullptr; }

  // Converts from `ArenaString::Optional` to `ArenaString`.
  NotOptional not_optional() const {
    RIEGELI_ASSERT(repr() != nullptr)
        << "Failed precondition of ArenaString::Optional::not_optional(): "
           "null pointer";
    return NotOptional::BackFromData(repr());
  }
  NotOptional NotOptionalOrDie() const {
    RIEGELI_CHECK(repr() != nullptr)
        << "Failed precondition of ArenaString::Optional::NotOptionalOrDie(): "
           "null pointer";
    return NotOptional::BackFromData(repr());
  }

  // Dereferences the pointer.
  absl::string_view operator*() const { return *not_optional(); }
  ArrowProxy<absl::string_view> operator->() const {
    return ArrowProxy<absl::string_view>(*not_optional());
  }

  // Dereferences the pointer, crashing the process if null.
  absl::string_view value() const { return *NotOptionalOrDie(); }

  // Equality corresponds to equality of the strings they refer to, optimized
  // when the pointers are equal.
  //
  // Other comparisons are also consistent with the strings, but only the case
  // of equal strings is optimized.
  friend bool operator==(Optional a, Optional b) {
    if (a.repr() == b.repr()) return true;
    if (a.repr() == nullptr || b.repr() == nullptr) return false;
    return *a == *b;
  }
  friend StrongOrdering RIEGELI_COMPARE(Optional a, Optional b) {
    if (a.repr() == b.repr()) return StrongOrdering::equal;
    if (a.repr() == nullptr) return StrongOrdering::less;
    if (b.repr() == nullptr) return StrongOrdering::greater;
    return riegeli::Compare(*a, *b);
  }

  friend bool operator==(Optional a, std::nullptr_t) {
    return a.repr() == nullptr;
  }
  friend StrongOrdering RIEGELI_COMPARE(Optional a, std::nullptr_t) {
    if (a.repr() == nullptr) return StrongOrdering::equal;
    return StrongOrdering::greater;
  }

  friend bool operator==(Optional a, absl::string_view b) {
    if (a.repr() == nullptr) return false;
    return *a == b;
  }
  friend StrongOrdering RIEGELI_COMPARE(Optional a, absl::string_view b) {
    if (a.repr() == nullptr) return StrongOrdering::less;
    return riegeli::Compare(*a, b);
  }

  // `ArenaString::ByAddress` is implicitly convertible from `ArenaString` or
  // `ArenaString::Optional`, but instances are compared and hashed by address.
  // This is more efficient, but the order is arbitrary, consistent within the
  // process.
  //
  // `std::less<ByAddress>` can be used as a comparator for algorithms over
  // `ArenaString` or `ArenaString::Optional`.
  class ByAddress;

  // Returns this object wrapped in `ByAddress`.
  ByAddress by_address() const { return ByAddress(*this); }

  // Hashing `ArenaString` or `ArenaString::Optional` is consistent with the
  // string value.
  template <typename HashState>
  friend HashState AbslHashValue(HashState hash_state, Optional self) {
    if (self == nullptr) {
      return HashState::combine(std::move(hash_state),
                                static_cast<const char*>(nullptr));
    }
    return HashState::combine(std::move(hash_state), *self);
  }

  // Hash and equality for containers with `ArenaString` or
  // `ArenaString::Optional` as the key type, supporting heterogeneous lookup.
  struct absl_container_hash;
  struct absl_container_eq;

  // Supports `riegeli::Debug()`.
  template <typename DebugStream>
  friend void RiegeliDebug(Optional src, DebugStream& dest) {
    if (src == nullptr) {
      dest.Debug(nullptr);
    } else {
      dest.Debug(*src);
    }
  }

 protected:
  explicit OptionalArenaString(const char* absl_nullable repr) : repr_(repr) {}

  const char* absl_nullable repr() const { return repr_; }

 private:
  // The data are stored at `repr_`. The size is encoded before the data in
  // 1 byte, 2 bytes, or as `size_t`.
  //
  // Decoding distinguishes the cases by the highest 1 or 2 bits of `repr_[-1]`.
  //
  // Encoded size:
  //  * Small  string: 1-byte   `size | 0x00`
  //  * Medium string: 2-byte   `size | 0x8000`
  //  * Large  string: `size_t` `size | 0xc0000000...`
  const char* absl_nullable repr_ = nullptr;
};

// The public name of `BasicArenaString` is `ArenaString`.
//
// `ArenaString` refers to a string stored in `StringArena`.
//
// `ArenaString` is never null. See `ArenaString::Optional` for a variant that
// can be null. `ArenaString` is generally preferred over
// `ArenaString::Optional`.
template <size_t alignment>
class BasicArenaString : public OptionalArenaString<alignment>,
                         public WithCompare<BasicArenaString<alignment>,
                                            OptionalArenaString<alignment>,
                                            std::nullptr_t, absl::string_view> {
 public:
  static_assert(absl::has_single_bit(alignment));

  // Configures the alignment of string data.
  //
  // A larger alignment can be used to tag pointers in the lowest bits,
  // encoding other objects together with the pointers.
  //
  // Also, if the strings encode another type (supported by custom `Encoder`),
  // they can be `reinterpret_cast` to a type with the required alignment.
  template <size_t new_alignment>
  using WithAlignment = BasicArenaString<new_alignment>;

  // Navigates between `ArenaString` and `ArenaString::Optional`.
  using NotOptional = typename BasicArenaString::NotOptional;
  using Optional = typename BasicArenaString::Optional;

  // Maximum supported string size.
  using Optional::kMaxSize;

  // Creates an empty `ArenaString`.
  //
  // This differs from the default constructor of `ArenaString::Optional`,
  // which creates a null instance.
  BasicArenaString() noexcept : Optional(EmptyRepr()) {}

  // Constructor from `nullptr` is present in `ArenaString::Optional` but
  // deleted in `ArenaString`.
  BasicArenaString(std::nullptr_t) = delete;
  BasicArenaString& operator=(std::nullptr_t) = delete;

  BasicArenaString(const BasicArenaString& that) = default;
  BasicArenaString& operator=(const BasicArenaString& that) = default;

  // Restores an `ArenaString` from a raw pointer to a previously allocated
  // string.
  static NotOptional BackFromData(const char* data) {
    RIEGELI_ASSERT(data != nullptr)
        << "Failed precondition of ArenaString::BackFromData(): null pointer";
    return NotOptional(data);
  }

  // Returns `true` because `ArenaString` is never null.
  explicit operator bool() const { return true; }

  // Returns the string.
  /*implicit*/ operator absl::string_view() const { return value(); }
  absl::string_view operator*() const { return value(); }
  ArrowProxy<absl::string_view> operator->() const {
    return ArrowProxy<absl::string_view>(*this);
  }
  absl::string_view value() const { return absl::string_view(data(), size()); }

  bool empty() const { return repr()[-1] == '\0'; }

  const char* data() const { return AssumeAligned<alignment>(repr()); }

  size_t size() const {
    const uint8_t last_byte = static_cast<uint8_t>(repr()[-1]);
    if (ABSL_PREDICT_TRUE((last_byte & 0x80) == 0)) {
      const size_t size = IntCast<size_t>(last_byte);
      RIEGELI_ASSUME_LE(size, kMaxSmallSize);
      return size;
    } else if ((last_byte & 0x40) == 0) {
      const size_t size =
          IntCast<size_t>(ReadLittleEndian16(repr() - 2) & 0x3fff);
      RIEGELI_ASSUME_GT(size, kMaxSmallSize);
      RIEGELI_ASSUME_LE(size, kMaxMediumSize);
      return size;
    } else {
      const size_t size = ReadLittleEndianSize(repr() - sizeof(size_t)) &
                          ~(size_t{3} << (sizeof(size_t) * 8 - 2));
      RIEGELI_ASSUME_GT(size, kMaxMediumSize);
      RIEGELI_ASSUME_LE(size, kMaxSize);
      return size;
    }
  }

  const char& operator[](size_t index) const {
    RIEGELI_ASSERT_LT(index, size())
        << "Failed precondition of ArenaString::operator[]: "
           "index out of range";
    return data()[index];
  }

  const char& at(size_t index) const {
    RIEGELI_CHECK_LT(index, size())
        << "Failed precondition of ArenaString::at(): index out of range";
    return data()[index];
  }

  const char& front() const {
    RIEGELI_ASSERT(!empty())
        << "Failed precondition of ArenaString::front(): empty string";
    return data()[0];
  }

  const char& back() const {
    RIEGELI_ASSERT(!empty())
        << "Failed precondition of ArenaString::back(): empty string";
    return data()[size() - 1];
  }

  // Equality corresponds to equality of the strings they refer to, optimized
  // when the pointers are equal.
  //
  // Other comparisons are also consistent with the strings, but only the case
  // of equal strings is optimized.
  friend bool operator==(NotOptional a, NotOptional b) {
    if (a.repr() == b.repr()) return true;
    return *a == *b;
  }
  friend StrongOrdering RIEGELI_COMPARE(NotOptional a, NotOptional b) {
    if (a.repr() == b.repr()) return StrongOrdering::equal;
    return riegeli::Compare(*a, *b);
  }

  friend bool operator==(NotOptional a, Optional b) {
    if (a.repr() == b.repr()) return true;
    if (b.repr() == nullptr) return false;
    return *a == *b;
  }
  friend StrongOrdering RIEGELI_COMPARE(NotOptional a, Optional b) {
    if (a.repr() == b.repr()) return StrongOrdering::equal;
    if (b.repr() == nullptr) return StrongOrdering::greater;
    return riegeli::Compare(*a, *b);
  }

  friend bool operator==(NotOptional /*a*/, std::nullptr_t) { return false; }
  friend StrongOrdering RIEGELI_COMPARE(NotOptional /*a*/, std::nullptr_t) {
    return StrongOrdering::greater;
  }

  friend bool operator==(NotOptional a, absl::string_view b) { return *a == b; }
  friend StrongOrdering RIEGELI_COMPARE(NotOptional a, absl::string_view b) {
    return riegeli::Compare(*a, b);
  }

  // Default stringification by `absl::StrCat()` etc.
  template <typename Sink>
  friend void AbslStringify(Sink& dest, NotOptional src) {
    dest.Append(absl::string_view(src));
  }

  friend std::ostream& operator<<(std::ostream& dest, NotOptional src) {
    return dest << absl::string_view(src);
  }

 protected:
  const char* repr() const {
    const char* const repr = this->Optional::repr();
    RIEGELI_ASSERT(repr != nullptr)
        << "Failed invariant of ArenaString: null pointer";
    return repr;
  }

 private:
  // For `HeaderSize()` and `Encode()`.
  template <typename Mutex, bool concurrent_reads, size_t static_min_block_size,
            size_t static_max_block_size>
  friend class BasicStringArena;

  explicit BasicArenaString(const char* repr) : Optional(repr) {}

  static constexpr size_t kMaxSmallSize = 0x7f;
  static constexpr size_t kMaxMediumSize = 0x3fff;

  static constexpr size_t HeaderSize(size_t size) {
    if (ABSL_PREDICT_TRUE(size <= kMaxSmallSize)) {
      return 1;
    } else if (size <= kMaxMediumSize) {
      return 2;
    } else {
      return sizeof(size_t);
    }
  }

  template <typename Encoder, typename Arg>
  static void Encode(char* repr, const Arg& value, size_t size) {
    if (ABSL_PREDICT_TRUE(size <= kMaxSmallSize)) {
      repr[-1] = static_cast<char>(size);
    } else if (size <= kMaxMediumSize) {
      WriteLittleEndian16(IntCast<uint16_t>(size | 0x8000), repr - 2);
    } else {
      WriteLittleEndianSize(size | (size_t{3} << (sizeof(size_t) * 8 - 2)),
                            repr - sizeof(size_t));
    }
    Encoder::Encode(value, repr);
  }

  static const char* EmptyRepr() {
    alignas(alignment) static constexpr char kEmptyRepr[alignment] = {};
    return kEmptyRepr + alignment;
  }
};

}  // namespace interned_internal

// The string type stored in `StringArena`.
//
// `ArenaString` is never null. See `ArenaString::Optional` for a variant that
// can be null. `ArenaString` is generally preferred over
// `ArenaString::Optional`.
//
// `ArenaString` can be parameterized with `WithAlignment`.
using ArenaString = interned_internal::BasicArenaString</*alignment=*/1>;

namespace interned_internal {

struct PointerPolicy {
  // Returns the maximum offset where a string can start in the fast path.
  static size_t limit(size_t current_block_size) { return current_block_size; }

  static char* ToAddress(char* repr, size_t /*block_index*/,
                         size_t /*scaled_offset*/) {
    return repr;
  }
};

struct PointerWithAddress {
  char* ptr;
  size_t address;
};

template <size_t static_max_block_size, size_t alignment>
struct WithAddressPolicy {
  static_assert(static_max_block_size > 0);
  static_assert(absl::has_single_bit(alignment));

  // Returns the maximum offset where a string can start. Strings must start
  // strictly before `limit` for their address to fit in `kScaledBlockCapacity`.
  static constexpr size_t limit(size_t current_block_size) {
    return UnsignedMin(current_block_size, static_max_block_size);
  }

  static PointerWithAddress ToAddress(char* repr, size_t block_index,
                                      size_t scaled_offset) {
    RIEGELI_ASSERT_LT(scaled_offset, kScaledBlockCapacity)
        << "Failed invariant of StringArena: scaled offset overflow";
    return PointerWithAddress{
        repr, block_index * kScaledBlockCapacity + scaled_offset};
  }

  static constexpr size_t kScaledBlockCapacity = [] {
    constexpr size_t kMinAlignment = StringArenaBlock::kMinAlignment;
    if constexpr (alignment <= kMinAlignment) {
      return UnsignedMax(RoundUp<alignment>(static_max_block_size) / alignment,
                         RoundUp<alignment>(sizeof(size_t)) / alignment + 1);
    } else {
      return UnsignedMax(
          RoundUp<alignment>(static_max_block_size +
                             (alignment - kMinAlignment)) /
              alignment,
          RoundUp<alignment>(sizeof(size_t) + (alignment - kMinAlignment)) /
                  alignment +
              1);
    }
  }();
};

template <size_t alignment>
inline PointerWithAddress AssumeAligned(PointerWithAddress allocated) {
  return PointerWithAddress{AssumeAligned<alignment>(allocated.ptr),
                            allocated.address};
}

// The public name of `BasicStringArena` is `StringArena`.
//
// Specialization of `BasicStringArena` with a dynamic block size. It is also a
// base class of the specialization with a static block size.
template <typename Mutex, bool concurrent_reads>
class BasicStringArena<Mutex, concurrent_reads, /*static_min_block_size=*/0,
                       /*static_max_block_size=*/0> {
 public:
  // Enables concurrency for `BasicStringArena`.
  //
  // `Mutex` specifies the mutex type, which can be `absl::Mutex` (default)
  // or another type with `lock()`, `unlock()`, `lock_shared()`, and
  // `unlock_shared()`, analogously to `absl::Mutex`.
  template <typename NewMutex = absl::Mutex>
  using Concurrent = BasicStringArena<NewMutex, concurrent_reads,
                                      /*static_min_block_size=*/0,
                                      /*static_max_block_size=*/0>;

  // Allows `ResolveAddress()` to be called concurrently with allocation without
  // locking.
  template <bool new_concurrent_reads = true>
  using WithConcurrentReads =
      BasicStringArena<Mutex, new_concurrent_reads, /*static_min_block_size=*/0,
                       /*static_max_block_size=*/0>;

  // Configures the block size of the arena, in bytes.
  //
  // Strings are allocated in blocks of sizes within this range. A larger block
  // size improves memory locality and reduces the number of allocations, but
  // increases wasted memory if only a small number of strings is allocated.
  template <size_t new_static_min_block_size,
            size_t new_static_max_block_size = new_static_min_block_size>
  using WithBlockSize =
      BasicStringArena<Mutex, concurrent_reads, new_static_min_block_size,
                       new_static_max_block_size>;

  // Configures the block size of the arena to be specified dynamically in the
  // constructor.
  using WithDynamicBlockSize = BasicStringArena;

  // The archive type. See `BasicStringArena::ExtractArchive()` for details.
  using Archive = BasicStringArena<NullMutex, /*concurrent_reads=*/false,
                                   /*static_min_block_size=*/0,
                                   /*static_max_block_size=*/0>;

  // Creates an empty `BasicStringArena` with a fixed block size in bytes.
  explicit BasicStringArena(size_t block_size)
      : max_block_size_(block_size), next_block_size_(block_size) {}

  // Creates an empty `BasicStringArena` with an adaptive block size between
  // `min_block_size` and `max_block_size` in bytes.
  explicit BasicStringArena(size_t min_block_size, size_t max_block_size)
      : max_block_size_(UnsignedMax(min_block_size, max_block_size)),
        next_block_size_(min_block_size) {}

  // A moved-from `BasicStringArena` is left empty.
  BasicStringArena(BasicStringArena&& that) noexcept
      ABSL_NO_THREAD_SAFETY_ANALYSIS
      : max_block_size_(that.max_block_size_),
        next_block_size_(that.next_block_size_),
        current_block_index_(std::exchange(that.current_block_index_, 0)),
        current_block_data_(std::exchange(that.current_block_data_, nullptr)),
        current_block_size_(std::exchange(that.current_block_size_, 0)),
        current_block_used_(std::exchange(that.current_block_used_, 0)),
        blocks_(std::move(that.blocks_)) {}
  BasicStringArena& operator=(BasicStringArena&& that) noexcept
      ABSL_NO_THREAD_SAFETY_ANALYSIS {
    max_block_size_ = that.max_block_size_;
    next_block_size_ = that.next_block_size_;
    current_block_index_ = std::exchange(that.current_block_index_, 0);
    current_block_data_ = std::exchange(that.current_block_data_, nullptr);
    current_block_size_ = std::exchange(that.current_block_size_, 0);
    current_block_used_ = std::exchange(that.current_block_used_, 0);
    DeleteBlocks(std::exchange(blocks_, std::exchange(that.blocks_, {})));
    return *this;
  }

  ~BasicStringArena() { DeleteBlocks(std::move(blocks_)); }

  // Resets the arena to the empty state, with a fixed block size in bytes.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(size_t block_size) {
    Reset(block_size, block_size);
  }

  // Resets the arena to the empty state, with an adaptive block size between
  // `min_block_size` and `max_block_size` in bytes.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(size_t min_block_size,
                                          size_t max_block_size)
      ABSL_NO_THREAD_SAFETY_ANALYSIS;

  // Prepares the arena for the expected total size of allocated strings in
  // bytes. This reduces reallocations.
  void ReserveBytes(size_t capacity);

  // Allocates `header_size + size` bytes in the arena. Returns the pointer
  // after `header_size`. `alignment` applies to that pointer.
  //
  // Does not initialize the allocated memory.
  //
  // Even if `size == 0`, the returned pointer is non-null and does not fall at
  // the end of a physical allocation. If `size == 0`, the pointer can be equal
  // to another pointer returned by `AllocateBytes(0, 0)`.
  template <size_t alignment = 1>
  char* AllocateBytes(size_t size,
                      size_t header_size = 0) ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return AllocateBytesImpl<PointerPolicy, alignment>(size, header_size);
  }

  // Const `AllocateBytes()` overload enabled only when thread-safe.
  template <
      size_t alignment = 1, typename DependentMutex = Mutex,
      std::enable_if_t<!std::is_same_v<DependentMutex, NullMutex>, int> = 0>
  char* AllocateBytes(size_t size, size_t header_size = 0) const
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return AllocateBytesImpl<PointerPolicy, alignment>(size, header_size);
  }

  // Allocates a string from `value` in the arena using `Encoder`.
  //
  // `Encoder` must provide:
  // ```
  //   static size_t EncodedSize(const Arg& src);
  //   static void Encode(const Arg& src, char* dest);
  // ```
  //
  // By default, `Encoder` supports `absl::string_view` and `const absl::Cord&`.
  //
  // Even if the string is empty, the data pointer of the returned `ArenaString`
  // is non-null and does not fall at the end of a physical allocation.
  // If `size == 0`, the pointer can be equal to another pointer returned by
  // `AllocateBytes(0, 0)`.
  template <size_t alignment = 1, typename Encoder = DefaultStringEncoder,
            typename Arg,
            std::enable_if_t<SupportedByEncoderForAllocate<Arg, Encoder>::value,
                             int> = 0>
  BasicArenaString<alignment> Allocate(const Arg& value)
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return AllocateImpl<alignment, Encoder>(value);
  }

  // Convert `const char*` to `absl::string_view` early to compute `strlen()`
  // once and to avoid separate template instantiations for `char[length + 1]`.
  template <size_t alignment = 1, typename Encoder = DefaultStringEncoder,
            std::enable_if_t<SupportedByEncoderForAllocate<absl::string_view,
                                                           Encoder>::value,
                             int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE BasicArenaString<alignment> Allocate(
      const char* value) ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return AllocateImpl<alignment, Encoder>(absl::string_view(value));
  }

  // Const `Allocate()` overload enabled only when thread-safe.
  template <size_t alignment = 1, typename Encoder = DefaultStringEncoder,
            typename Arg, typename DependentMutex = Mutex,
            std::enable_if_t<
                std::conjunction_v<
                    std::negation<std::is_same<DependentMutex, NullMutex>>,
                    SupportedByEncoderForAllocate<Arg, Encoder>>,
                int> = 0>
  BasicArenaString<alignment> Allocate(const Arg& value) const
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return AllocateImpl<alignment, Encoder>(value);
  }

  // Convert `const char*` to `absl::string_view` early to compute `strlen()`
  // once and to avoid separate template instantiations for `char[length + 1]`.
  template <size_t alignment = 1, typename Encoder = DefaultStringEncoder,
            typename DependentMutex = Mutex,
            std::enable_if_t<
                std::conjunction_v<
                    std::negation<std::is_same<DependentMutex, NullMutex>>,
                    SupportedByEncoderForAllocate<absl::string_view, Encoder>>,
                int> = 0>
  ABSL_ATTRIBUTE_ALWAYS_INLINE BasicArenaString<alignment> Allocate(
      const char* value) const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return AllocateImpl<alignment, Encoder>(absl::string_view(value));
  }

  // Undoes `AllocateBytes()`. This is best-effort, and is effective only for
  // the most recent allocation.
  void UndoAllocateBytes(const char* allocated, size_t size,
                         size_t header_size = 0) {
    UndoAllocateBytesImpl(allocated, size, header_size);
  }

  // Const `UndoAllocateBytes()` overload enabled only when thread-safe.
  template <
      typename DependentMutex = Mutex,
      std::enable_if_t<!std::is_same_v<DependentMutex, NullMutex>, int> = 0>
  void UndoAllocateBytes(const char* allocated, size_t size,
                         size_t header_size = 0) const {
    UndoAllocateBytesImpl(allocated, size, header_size);
  }

  // Undoes `Allocate()` or `AllocateWithAddress()`. This is best-effort, and is
  // effective only for the most recent allocation.
  template <size_t alignment>
  void UndoAllocate(BasicArenaString<alignment> allocated) {
    UndoAllocateImpl(allocated);
  }

  // Const `UndoAllocate()` overload enabled only when thread-safe.
  template <
      size_t alignment, typename DependentMutex = Mutex,
      std::enable_if_t<!std::is_same_v<DependentMutex, NullMutex>, int> = 0>
  void UndoAllocate(BasicArenaString<alignment> allocated) const {
    UndoAllocateImpl(allocated);
  }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const BasicStringArena* self,
                                        MemoryEstimator& memory_estimator) {
    ReaderMutexLock<Mutex> lock(self->mutex_);
    memory_estimator.RegisterSubobjects(&self->blocks_);
  }

  // Extracts the storage of the strings as an archive, which holds the same
  // strings as `BasicStringArena`, but does not support concurrency.
  // The `BasicStringArena` is left empty.
  Archive ExtractArchive() && { return Archive(std::move(*this)); }

 private:
  // For `BasicStringArena(BasicStringArena<OtherMutex, other_concurrent_reads,
  //                                        static_min_block_size,
  //                                        static_max_block_size>&&)`.
  template <typename OtherMutex, bool other_concurrent_reads,
            size_t static_min_block_size_param,
            size_t static_max_block_size_param>
  friend class BasicStringArena;

  using Blocks = ConcurrentVector<StringArenaBlock, concurrent_reads, 16>;

  static void DeleteBlocks(Blocks blocks) ABSL_NO_THREAD_SAFETY_ANALYSIS {
    for (size_t i = blocks.size(); i > 0;) {
      --i;
      blocks[i].Delete();
    }
  }

  template <typename OtherMutex, bool other_concurrent_reads>
  explicit BasicStringArena(
      BasicStringArena<OtherMutex, other_concurrent_reads,
                       /*static_min_block_size=*/0,
                       /*static_max_block_size=*/0>&& that)
      : max_block_size_(that.max_block_size_),
        next_block_size_(that.next_block_size_),
        current_block_index_(std::exchange(that.current_block_index_, 0)),
        current_block_data_(std::exchange(that.current_block_data_, nullptr)),
        current_block_size_(std::exchange(that.current_block_size_, 0)),
        current_block_used_(std::exchange(that.current_block_used_, 0)),
        blocks_(std::move(that.blocks_)) {}

  template <typename Policy, size_t alignment>
  auto AllocateBytesImpl(size_t size, size_t header_size) const;

  template <typename Policy, size_t alignment>
  ABSL_ATTRIBUTE_NOINLINE auto AllocateBytesSlow(size_t size,
                                                 size_t header_size) const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  template <size_t alignment, typename Encoder, typename Arg>
  BasicArenaString<alignment> AllocateImpl(const Arg& value) const;

  void UndoAllocateBytesImpl(const char* allocated, size_t size,
                             size_t header_size) const;

  template <size_t alignment>
  void UndoAllocateImpl(BasicArenaString<alignment> allocated) const;

  size_t max_block_size_;
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS mutable Mutex mutex_;
  mutable size_t next_block_size_ ABSL_GUARDED_BY(mutex_);
  mutable size_t current_block_index_ ABSL_GUARDED_BY(mutex_) = 0;
  // If `!blocks_.empty()`, `blocks_[current_block_index_].data()`.
  // Otherwise `nullptr`.
  mutable char* current_block_data_ ABSL_GUARDED_BY(mutex_) = nullptr;
  // If `!blocks_.empty()`, the capacity of `blocks_[current_block_index_]`.
  // Otherwise 0.
  mutable size_t current_block_size_ ABSL_GUARDED_BY(mutex_) = 0;
  // If `!blocks_.empty()`, the number of used bytes in
  // `blocks_[current_block_index_]`. Otherwise 0.
  mutable size_t current_block_used_ ABSL_GUARDED_BY(mutex_) = 0;
  mutable Blocks blocks_ ABSL_GUARDED_BY(mutex_);
};

// Specialization of `BasicStringArena` with a static block size.
template <typename Mutex, bool concurrent_reads, size_t static_min_block_size,
          size_t static_max_block_size>
class BasicStringArena : public BasicStringArena<Mutex, concurrent_reads,
                                                 /*static_min_block_size=*/0,
                                                 /*static_max_block_size=*/0> {
 public:
  static_assert(static_min_block_size > 0 && static_max_block_size > 0,
                "static_min_block_size and static_max_block_size "
                "must be both zero or both positive");

  // Enables concurrency for `BasicStringArena`.
  //
  // `Mutex` specifies the mutex type, which can be `absl::Mutex` (default)
  // or another type with `lock()`, `unlock()`, `lock_shared()`, and
  // `unlock_shared()`, analogously to `absl::Mutex`.
  template <typename NewMutex = absl::Mutex>
  using Concurrent =
      BasicStringArena<NewMutex, concurrent_reads, static_min_block_size,
                       static_max_block_size>;

  // Allows `ResolveAddress()` to be called concurrently with allocation without
  // locking.
  template <bool new_concurrent_reads = true>
  using WithConcurrentReads =
      BasicStringArena<Mutex, new_concurrent_reads, static_min_block_size,
                       static_max_block_size>;

  // Configures the block size of the arena, in bytes.
  //
  // Strings are allocated in blocks of sizes within this range. A larger block
  // size improves memory locality and reduces the number of allocations, but
  // increases wasted memory if only a small number of strings is allocated.
  template <size_t new_static_min_block_size,
            size_t new_static_max_block_size = new_static_min_block_size>
  using WithBlockSize =
      BasicStringArena<Mutex, concurrent_reads, new_static_min_block_size,
                       new_static_max_block_size>;

  // Configures the block size of the arena to be specified dynamically in the
  // constructor.
  using WithDynamicBlockSize =
      BasicStringArena<Mutex, concurrent_reads, /*static_min_block_size=*/0,
                       /*static_max_block_size=*/0>;

  // The archive type. See `BasicStringArena::ExtractArchive()` for details.
  using Archive =
      BasicStringArena<NullMutex, /*concurrent_reads=*/false,
                       static_min_block_size, static_max_block_size>;

  // Creates an empty `BasicStringArena` with a static block size in bytes.
  BasicStringArena() noexcept
      : BasicStringArena<Mutex, concurrent_reads, /*static_min_block_size=*/0,
                         /*static_max_block_size=*/0>(static_min_block_size,
                                                      static_max_block_size) {}

  // A moved-from `BasicStringArena` is left empty.
  BasicStringArena(BasicStringArena&& that) = default;
  BasicStringArena& operator=(BasicStringArena&& that) = default;

  // Resets the arena to the empty state.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset() {
    this->BasicStringArena<
        Mutex, concurrent_reads, /*static_min_block_size=*/0,
        /*static_max_block_size=*/0>::Reset(static_min_block_size,
                                            static_max_block_size);
  }

  // Allocates a string from `value` in the arena using `Encoder`.
  //
  // Returns the address of the allocated string. An address consists of the
  // block index and the offset within the block.
  //
  // Heterogeneous lookup against `value` is supported by `ResolveAddress()`.
  template <size_t alignment = 1, typename Encoder = DefaultStringEncoder,
            typename Arg>
  size_t AllocateWithAddress(const Arg& value) ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return AllocateWithAddressImpl<alignment, Encoder>(value);
  }

  // Const `AllocateWithAddress()` overload enabled only when thread-safe.
  template <
      size_t alignment = 1, typename Encoder = DefaultStringEncoder,
      typename Arg, typename DependentMutex = Mutex,
      std::enable_if_t<!std::is_same_v<DependentMutex, NullMutex>, int> = 0>
  size_t AllocateWithAddress(const Arg& value) const
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return AllocateWithAddressImpl<alignment, Encoder>(value);
  }

  // Resolves an address returned by `AllocateWithAddress()` to the string.
  //
  // If `concurrent_reads` is `true`, this can be called concurrently with
  // allocation without locking.
  template <size_t alignment = 1>
  BasicArenaString<alignment> ResolveAddress(size_t address) const
      ABSL_NO_THREAD_SAFETY_ANALYSIS {
    static_assert(absl::has_single_bit(alignment));
    constexpr size_t kScaledBlockCapacity =
        WithAddressPolicy<static_max_block_size,
                          alignment>::kScaledBlockCapacity;
    const size_t block_index = address / kScaledBlockCapacity;
    const size_t scaled_offset = address % kScaledBlockCapacity;
    RIEGELI_ASSERT_LT(block_index, this->blocks_.size())
        << "Failed precondition of StringArena::ResolveAddress(): "
           "address out of bounds";
    const char* const block_data = this->blocks_[block_index].data();
    constexpr size_t kMinAlignment = StringArenaBlock::kMinAlignment;
    if constexpr (alignment <= kMinAlignment) {
      return BasicArenaString<alignment>::BackFromData(
          block_data + scaled_offset * alignment);
    } else {
      const char* const repr = reinterpret_cast<char*>(
          (reinterpret_cast<uintptr_t>(block_data) & ~(alignment - 1)) +
          scaled_offset * alignment);
      return BasicArenaString<alignment>::BackFromData(repr);
    }
  }

  // Returns an estimate of the usage of the address space.
  template <size_t alignment = 1>
  size_t AddressSpaceUsed() const {
    static_assert(absl::has_single_bit(alignment));
    MutexLock<Mutex> lock(this->mutex_);
    if (this->blocks_.empty()) return 0;
    constexpr size_t kScaledBlockCapacity =
        WithAddressPolicy<static_max_block_size,
                          alignment>::kScaledBlockCapacity;
    const size_t last_block_index = this->blocks_.size() - 1;
    const size_t last_block_used =
        last_block_index == this->current_block_index_
            ? this->current_block_used_
            : this->blocks_.back().size();
    return last_block_index * kScaledBlockCapacity +
           UnsignedMin(RoundUp<alignment>(last_block_used) / alignment,
                       kScaledBlockCapacity);
  }

  // Extracts the storage of the strings as an archive, which holds the same
  // strings as `BasicStringArena`, but does not support concurrency.
  // The `BasicStringArena` is left empty.
  Archive ExtractArchive() && { return Archive(std::move(*this)); }

 private:
  // For `BasicStringArena(BasicStringArena<OtherMutex, other_concurrent_reads,
  //                                        static_min_block_size,
  //                                        static_max_block_size>&&)`.
  template <typename OtherMutex, bool other_concurrent_reads,
            size_t static_min_block_size_param,
            size_t static_max_block_size_param>
  friend class BasicStringArena;

  template <typename OtherMutex, bool other_concurrent_reads>
  explicit BasicStringArena(
      BasicStringArena<OtherMutex, other_concurrent_reads,
                       static_min_block_size, static_max_block_size>&& that)
      : BasicStringArena<Mutex, concurrent_reads, /*static_min_block_size=*/0,
                         /*static_max_block_size=*/0>(std::move(that)) {}

  template <size_t alignment, typename Encoder, typename Arg>
  size_t AllocateWithAddressImpl(const Arg& value) const;
};

}  // namespace interned_internal

// Allocates variable-length strings.
//
// The strings are never moved. They are destroyed when the arena is destroyed.
// Individual deallocation is not supported, except for best-effort undoing of
// the most recent allocation.
//
// Strings are allocated in blocks whose size in bytes is specified statically
// or dynamically, and can adaptively grow between `min_block_size` and
// `max_block_size`. The default is a static size range between 256 bytes and
// 64K.
//
// If `concurrent_reads` is `true`, `ResolveAddress()` can be called
// concurrently with allocation without locking.
//
// `StringArena` can be parameterized with `Concurrent`, `WithConcurrentReads`,
// `WithBlockSize`, and `WithDynamicBlockSize`.
using StringArena =
    interned_internal::BasicStringArena<NullMutex, /*concurrent_reads=*/false,
                                        kDefaultArenaMinBlockSize,
                                        kDefaultArenaMaxBlockSize>;

// Implementation details follow.

namespace interned_internal {

template <size_t alignment>
class OptionalArenaString<alignment>::ByAddress
    : public WithCompare<ByAddress> {
 public:
  /*implicit*/ ByAddress(Optional view) : repr_(view.repr()) {}

  ByAddress(const ByAddress& that) = default;
  ByAddress& operator=(const ByAddress& that) = default;

  friend bool operator==(ByAddress a, ByAddress b) {
    return a.repr_ == b.repr_;
  }
  friend StrongOrdering RIEGELI_COMPARE(ByAddress a, ByAddress b) {
    return riegeli::Compare(a.repr_, b.repr_);
  }

  template <typename HashState>
  friend HashState AbslHashValue(HashState hash_state, ByAddress self) {
    return HashState::combine(std::move(hash_state), self.repr_);
  }

 private:
  const char* absl_nullable repr_;
};

template <size_t alignment>
struct OptionalArenaString<alignment>::absl_container_hash {
  using is_transparent = void;
  size_t operator()(Optional self) const { return absl::HashOf(self); }
  size_t operator()(NotOptional self) const { return absl::HashOf(self); }
  size_t operator()(std::nullptr_t) const { return absl::HashOf(nullptr); }
  size_t operator()(absl::string_view arg) const { return absl::HashOf(arg); }
};

template <size_t alignment>
struct OptionalArenaString<alignment>::absl_container_eq {
  using is_transparent = void;
  bool operator()(Optional a, Optional b) const { return a == b; }
  bool operator()(Optional a, NotOptional b) const { return a == b; }
  bool operator()(NotOptional a, Optional b) const { return a == b; }
  bool operator()(NotOptional a, NotOptional b) const { return a == b; }
  bool operator()(Optional a, std::nullptr_t) const { return a == nullptr; }
  bool operator()(std::nullptr_t, Optional b) const { return b == nullptr; }
  bool operator()(NotOptional /*a*/, std::nullptr_t) const { return false; }
  bool operator()(std::nullptr_t, NotOptional /*b*/) const { return false; }
  bool operator()(Optional a, absl::string_view b) const { return a == b; }
  bool operator()(absl::string_view a, Optional b) const { return a == b; }
  bool operator()(NotOptional a, absl::string_view b) const { return a == b; }
  bool operator()(absl::string_view a, NotOptional b) const { return a == b; }
};

template <typename Mutex, bool concurrent_reads>
inline void BasicStringArena<Mutex, concurrent_reads, 0, 0>::Reset(
    size_t min_block_size, size_t max_block_size) {
  max_block_size_ = UnsignedMax(min_block_size, max_block_size);
  if (current_block_data_ != nullptr &&
      current_block_size_ <= max_block_size_) {
    for (size_t i = blocks_.size(); i > 0;) {
      --i;
      if (i != current_block_index_) blocks_[i].Delete();
    }
    const StringArenaBlock retained_block = blocks_[current_block_index_];
    blocks_.clear();
    blocks_.push_back(retained_block);
    current_block_index_ = 0;
    current_block_data_ = blocks_[0].data();
    current_block_size_ = blocks_[0].size();
    current_block_used_ = 0;
    next_block_size_ =
        UnsignedClamp(current_block_size_ + (current_block_size_ + 1) / 2,
                      min_block_size, max_block_size_);
    return;
  }
  for (size_t i = blocks_.size(); i > 0;) {
    --i;
    blocks_[i].Delete();
  }
  blocks_.clear();
  current_block_index_ = 0;
  current_block_data_ = nullptr;
  current_block_size_ = 0;
  current_block_used_ = 0;
  next_block_size_ = min_block_size;
}

template <typename Mutex, bool concurrent_reads>
inline void BasicStringArena<Mutex, concurrent_reads, 0, 0>::ReserveBytes(
    size_t capacity) {
  if (capacity == 0) return;
  MutexLock<Mutex> lock(mutex_);
  const size_t num_blocks = blocks_.size();
  size_t existing_capacity = 0;
  for (size_t i = 0; i < num_blocks; ++i) {
    existing_capacity += blocks_[i].size();
  }
  if (capacity <= existing_capacity) return;
  const size_t remaining_to_reserve = capacity - existing_capacity;
  if (remaining_to_reserve <= max_block_size_) {
    next_block_size_ = UnsignedMax(next_block_size_, remaining_to_reserve);
  } else {
    next_block_size_ = max_block_size_;
    const size_t num_additional_blocks =
        (remaining_to_reserve - 1) / max_block_size_ + 1;
    blocks_.reserve(num_blocks + num_additional_blocks);
  }
}

template <typename Mutex, bool concurrent_reads>
template <typename Policy, size_t alignment>
inline auto BasicStringArena<Mutex, concurrent_reads, 0, 0>::AllocateBytesImpl(
    size_t size, size_t header_size) const {
  static_assert(absl::has_single_bit(alignment));
  constexpr size_t kMinAlignment = StringArenaBlock::kMinAlignment;
  MutexLock<Mutex> lock(mutex_);
  size_t repr_offset;
  size_t base_misalignment = 0;
  if constexpr (alignment <= kMinAlignment) {
    repr_offset = RoundUp<alignment>(current_block_used_ + header_size);
  } else {
    base_misalignment =
        reinterpret_cast<uintptr_t>(current_block_data_) & (alignment - 1);
    repr_offset = RoundUp<alignment>(current_block_used_ + header_size +
                                     base_misalignment) -
                  base_misalignment;
  }

  const size_t limit = Policy::limit(current_block_size_);
  // The assumptions optimize the code if `alignment == 1 && header_size == 0`.
  RIEGELI_ASSUME_LE(current_block_used_, current_block_size_);
  RIEGELI_ASSUME_LE(limit, current_block_size_);
  // Force the slow path if the returned pointer would fall at the end of the
  // block. We are comparing the end pointer, so if the end pointer is strictly
  // before the end of the block then the fast path is safe.
  if (ABSL_PREDICT_TRUE(size < limit - repr_offset && repr_offset < limit)) {
    // Allocate from the current regular block.
    char* const repr = current_block_data_ + repr_offset;
    current_block_used_ = repr_offset + size;
    return AssumeAligned<alignment>(
        Policy::ToAddress(repr, current_block_index_,
                          (repr_offset + base_misalignment) / alignment));
  }
  return AssumeAligned<alignment>(
      AllocateBytesSlow<Policy, alignment>(size, header_size));
}

template <typename Mutex, bool concurrent_reads>
template <typename Policy, size_t alignment>
ABSL_ATTRIBUTE_NOINLINE auto
BasicStringArena<Mutex, concurrent_reads, 0, 0>::AllocateBytesSlow(
    size_t size, size_t header_size) const {
  RIEGELI_CHECK_LE(size, BasicArenaString<alignment>::kMaxSize)
      << "Failed precondition of StringArena: string size overflow";
  constexpr size_t kMinAlignment = StringArenaBlock::kMinAlignment;
  size_t repr_offset;
  size_t base_misalignment = 0;
  if constexpr (alignment <= kMinAlignment) {
    repr_offset = RoundUp<alignment>(current_block_used_ + header_size);
  } else {
    base_misalignment =
        reinterpret_cast<uintptr_t>(current_block_data_) & (alignment - 1);
    repr_offset = RoundUp<alignment>(current_block_used_ + header_size +
                                     base_misalignment) -
                  base_misalignment;
  }

  const size_t limit = Policy::limit(current_block_size_);
  RIEGELI_ASSUME_LE(current_block_used_, current_block_size_);
  RIEGELI_ASSUME_LE(limit, current_block_size_);
  if (size <= current_block_size_ - repr_offset && repr_offset < limit) {
    // For the address to be representable by `Policy::ToAddress()`, the string
    // must start before the limit. Its end can extend beyond the limit though,
    // up to and including the end of the allocated block. This case was not
    // handled in `AllocateBytesImpl()` because it would need an extra branch.
    char* const repr = current_block_data_ + repr_offset;
    current_block_used_ = repr_offset + size;
    return AssumeAligned<alignment>(
        Policy::ToAddress(repr, current_block_index_,
                          (repr_offset + base_misalignment) / alignment));
  }

  const size_t max_repr_offset =
      RoundUp<alignment>(header_size + SaturatingSub(alignment, kMinAlignment));
  const size_t required = max_repr_offset + size;
  size_t allocated_size;
  bool make_regular_block;
  if (required <= max_block_size_ / 2) {
    // If the allocation fits into half of the maximum block size, allocate a
    // regular block to amortize future small allocations.
    allocated_size = UnsignedClamp(next_block_size_, required, max_block_size_);
    make_regular_block = true;
  } else {
    // If the allocation exceeds half of the maximum block size, allocate a
    // dedicated block sized for this string without abandoning the current
    // regular block.
    allocated_size = max_repr_offset + UnsignedMax(size, size_t{1});
    make_regular_block = false;
  }
  next_block_size_ = UnsignedClamp(allocated_size + (allocated_size + 1) / 2,
                                   next_block_size_, max_block_size_);

  const StringArenaBlock& block = blocks_.emplace_back(allocated_size);
  char* const block_data = block.data();
  if constexpr (alignment <= kMinAlignment) {
    repr_offset = max_repr_offset;
  } else {
    base_misalignment =
        reinterpret_cast<uintptr_t>(block_data) & (alignment - 1);
    repr_offset =
        RoundUp<alignment>(header_size + base_misalignment) - base_misalignment;
  }
  if (make_regular_block) {
    current_block_index_ = blocks_.size() - 1;
    current_block_data_ = block_data;
    current_block_size_ = block.size();
    current_block_used_ = repr_offset + size;
  }
  return Policy::ToAddress(block_data + repr_offset, blocks_.size() - 1,
                           (repr_offset + base_misalignment) / alignment);
}

template <typename Mutex, bool concurrent_reads>
template <size_t alignment, typename Encoder, typename Arg>
inline BasicArenaString<alignment>
BasicStringArena<Mutex, concurrent_reads, 0, 0>::AllocateImpl(
    const Arg& value) const {
  static_assert(absl::has_single_bit(alignment));
  const size_t size = Encoder::EncodedSize(value);
  char* const repr = AllocateBytesImpl<PointerPolicy, alignment>(
      size, BasicArenaString<alignment>::HeaderSize(size));
  BasicArenaString<alignment>::template Encode<Encoder>(repr, value, size);
  return BasicArenaString<alignment>::BackFromData(repr);
}

template <typename Mutex, bool concurrent_reads>
inline void
BasicStringArena<Mutex, concurrent_reads, 0, 0>::UndoAllocateBytesImpl(
    const char* allocated, size_t size, size_t header_size) const {
  MutexLock<Mutex> lock(mutex_);
  if (ABSL_PREDICT_TRUE(current_block_data_ != nullptr &&
                        allocated + size ==
                            current_block_data_ + current_block_used_)) {
    // This was the most recent allocation in the current block. Undo it
    // even if a dedicated block is more recent.
    current_block_used_ -= header_size + size;
    return;
  }

  if (!blocks_.empty() && (current_block_data_ == nullptr ||
                           current_block_index_ != blocks_.size() - 1)) {
    StringArenaBlock& last_block = blocks_.back();
    // `last_block` is a dedicated block.
    if (std::greater_equal<>()(allocated, last_block.data()) &&
        std::less<>()(allocated, last_block.data() + last_block.size())) {
      // This was an allocation in the last block which is a dedicated block.
      // Undo it.
      last_block.Delete();
      blocks_.pop_back();
      return;
    }
  }

  // Undoing is not feasible.
}

template <typename Mutex, bool concurrent_reads>
template <size_t alignment>
inline void BasicStringArena<Mutex, concurrent_reads, 0, 0>::UndoAllocateImpl(
    BasicArenaString<alignment> allocated) const {
  const size_t size = allocated.size();
  UndoAllocateBytesImpl(allocated.data(), size,
                        BasicArenaString<alignment>::HeaderSize(size));
}

template <typename Mutex, bool concurrent_reads, size_t static_min_block_size,
          size_t static_max_block_size>
template <size_t alignment, typename Encoder, typename Arg>
inline size_t BasicStringArena<
    Mutex, concurrent_reads, static_min_block_size,
    static_max_block_size>::AllocateWithAddressImpl(const Arg& value) const {
  static_assert(absl::has_single_bit(alignment));
  const size_t size = Encoder::EncodedSize(value);
  const PointerWithAddress allocated = this->template AllocateBytesImpl<
      WithAddressPolicy<static_max_block_size, alignment>, alignment>(
      size, BasicArenaString<alignment>::HeaderSize(size));
  BasicArenaString<alignment>::template Encode<Encoder>(allocated.ptr, value,
                                                        size);
  return allocated.address;
}

}  // namespace interned_internal

}  // namespace riegeli

#endif  // RIEGELI_INTERNED_STRING_ARENA_H_
