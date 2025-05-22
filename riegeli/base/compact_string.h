// Copyright 2023 Google LLC
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

#ifndef RIEGELI_BASE_COMPACT_STRING_H_
#define RIEGELI_BASE_COMPACT_STRING_H_

#include <stddef.h>
#include <stdint.h>

#include <cstring>
#include <iosfwd>
#include <limits>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/config.h"
#include "absl/base/optimization.h"
#include "absl/hash/hash.h"
#include "absl/meta/type_traits.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/bytes_ref.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/external_data.h"
#include "riegeli/base/new_aligned.h"
#include "riegeli/base/type_traits.h"

namespace riegeli {

// `CompactString` provides a subset of functionality of `std::string`, while
// having less space overhead. It is useful for storing many short strings for
// a long time where each string owns its memory.
//
// A `CompactString` object internally consists of a pointer to heap-allocated
// data. The representation has 4 cases, distinguished by how the pointer is
// aligned modulo 8:
//  * 6 - not really a pointer but short string optimization: the size is
//        stored in bits [3..8), the data are stored in the remaining bytes
//  * 2 - the size is stored before the data as `uint8_t`
//  * 4 - the size is stored before the data as `uint16_t`
//  * 0 - the size is stored before the data as `size_t`
//
// In the last three cases the capacity is stored before the size in the same
// width as the size.
//
// The data are not necessarily NUL-terminated.
//
// Since `data()`, `size()`, `operator[]` etc. involve branches, for iteration
// it is faster to store the result of conversion to `absl::string_view` and
// iterate over that, or use `StringReader`, and for repeated appending it is
// faster to use `CompactStringWriter`.
//
// Memory usage of a `CompactString` of capacity c, assuming 8-byte pointers,
// where H(n) is memory usage of a heap-allocated block of length n:
//
//          c        | `CompactString` memory usage
//   ----------------|------------------------------
//        0 .. 7     | 8
//        8 .. 255   | 8 + H(c + 2)
//      256 .. 65535 | 8 + H(c + 4)
//    65536 .. max   | 8 + H(c + 16)
//
// For sizes up to 255 this is less than libc++ `std::string` by about 15, and
// less than libstdc++ `std::string` by about 23.
class
#ifdef ABSL_ATTRIBUTE_TRIVIAL_ABI
    ABSL_ATTRIBUTE_TRIVIAL_ABI
#endif
    CompactString : public WithCompare<CompactString> {
 public:
  static constexpr size_t max_size() {
    return std::numeric_limits<size_t>::max() - 2 * sizeof(size_t);
  }

  CompactString() = default;

  // Creates a `CompactString` with the given size and uninitialized data.
  explicit CompactString(size_t size) : repr_(MakeRepr(size)) {}

  // Creates a `CompactString` which holds a copy of `src`.
  explicit CompactString(BytesRef src) : repr_(MakeRepr(src)) {}

  // Creates a `CompactString` which holds a copy of `src`. Reserves one extra
  // char so that `c_str()` does not need reallocation.
  static CompactString ForCStr(BytesRef src) {
    return CompactString(FromReprTag(), MakeRepr(src, src.size() + 1));
  }

  CompactString(const CompactString& that);
  CompactString& operator=(const CompactString& that);

  // The source `CompactString` is left empty.
  CompactString(CompactString&& that) noexcept
      : repr_(std::exchange(that.repr_, kInlineTag)) {}
  CompactString& operator=(CompactString&& that) {
    DeleteRepr(std::exchange(repr_, std::exchange(that.repr_, kInlineTag)));
    return *this;
  }

  CompactString& operator=(BytesRef src);

  ~CompactString() { DeleteRepr(repr_); }

  /*implicit*/ operator absl::string_view() const ABSL_ATTRIBUTE_LIFETIME_BOUND;

  bool empty() const { return size() == 0; }
  char* data() ABSL_ATTRIBUTE_LIFETIME_BOUND;              // Never `nullptr`.
  const char* data() const ABSL_ATTRIBUTE_LIFETIME_BOUND;  // Never `nullptr`.
  size_t size() const;
  size_t capacity() const;

  char& operator[](size_t index) ABSL_ATTRIBUTE_LIFETIME_BOUND;
  const char& operator[](size_t index) const ABSL_ATTRIBUTE_LIFETIME_BOUND;
  char& at(size_t index) ABSL_ATTRIBUTE_LIFETIME_BOUND;
  const char& at(size_t index) const ABSL_ATTRIBUTE_LIFETIME_BOUND;
  char& front() ABSL_ATTRIBUTE_LIFETIME_BOUND;
  const char& front() const ABSL_ATTRIBUTE_LIFETIME_BOUND;
  char& back() ABSL_ATTRIBUTE_LIFETIME_BOUND;
  const char& back() const ABSL_ATTRIBUTE_LIFETIME_BOUND;

  void clear() { set_size(0); }

  // Sets the size to `new_size` without reallocation.
  //
  // If `new_size <= size()`, the prefix of data with `new_size` is preserved.
  //
  // If `new_size >= size()`, all existing data are preserved and new data are
  // uninitialized.
  //
  // Precondition: `new_size <= capacity()`
  void set_size(size_t new_size);

  // Sets the size to `new_size`, reallocating if needed, ensuring that repeated
  // growth has the cost proportional to the final size.
  //
  // If `new_size <= size()`, the prefix of data with `new_size` is preserved.
  //
  // If `new_size >= size()`, all existing data are preserved and new data are
  // uninitialized.
  //
  // `resize(new_size)` is equivalent to `reserve(new_size)` followed by
  // `set_size(new_size)`.
  void resize(size_t new_size);

  // Sets the size to `new_size`, ensuring that repeated growth has the cost
  // proportional to the final size.
  //
  // The prefix of data with `used_size` is preserved.
  //
  // If `new_size > size()`, new data are uninitialized.
  //
  // Returns `data() + used_size`, for convenience of appending to previously
  // used data.
  //
  // `resize(new_size, used_size)` is equivalent to `set_size(used_size)`
  // followed by `resize(new_size)` and returning `data() + used_size`.
  // `resize(new_size)` is equivalent to `resize(new_size, size())`.
  //
  // Preconditions:
  //   `used_size <= size()`
  //   `used_size <= new_size`
  char* resize(size_t new_size, size_t used_size) ABSL_ATTRIBUTE_LIFETIME_BOUND;

  // Ensures that `capacity() >= min_capacity`, ensuring that repeated growth
  // has the cost proportional to the final size.
  void reserve(size_t min_capacity);

  void shrink_to_fit();

  // Appends `length` uninitialized data.
  //
  // Returns `data() + used_size` where `used_size` is `size()` before the call,
  // for convenience of appending to previously used data.
  //
  // `append(length)` is equivalent to `resize(size() + length, size())` with
  // a check against overflow of `size() + length`.
  char* append(size_t length) ABSL_ATTRIBUTE_LIFETIME_BOUND;

  // Appends `src`.
  void append(absl::string_view src);

  // Ensures that `data()` are NUL-terminated after `size()` and returns
  // `data()`.
  //
  // In contrast to `std::string::c_str()`, this is a non-const operation.
  // It may reallocate the string and it writes the NUL each time.
  const char* c_str() ABSL_ATTRIBUTE_LIFETIME_BOUND;

  // Returns the representation of the `CompactString` as `uintptr_t`.
  //
  // Ownership is transferred to the `uintptr_t`, the `CompactString` is
  // left empty. The `uintptr_t` must be passed exactly once to
  // `CompactString::MoveFromRaw()` to recover the `CompactString` and free its
  // memory.
  //
  // The returned `uintptr_t` is always even.
  uintptr_t RawMove() && { return std::exchange(repr_, kInlineTag); }

  // Returns a pointer to the representation of the `CompactString` as
  // `uintptr_t`.
  //
  // Ownership is not transferred and the `CompactString` is unchanged.
  //
  // The returned `uintptr_t` is always even.
  const uintptr_t* RawView() const { return &repr_; }

  // Recovers a `CompactString` from the representation returned by
  // `CompactString::RawMove()`.
  //
  // Ownership is transferred to the `CompactString`, `raw` must not be read
  // again.
  //
  // Calling `MoveFromRaw()` and dropping its result frees the memory of the
  // `CompactString`.
  static CompactString MoveFromRaw(const uintptr_t& raw) {
    RIEGELI_ASSERT_EQ(raw & 1, 0u)
        << "Failed precondition of CompactString::MoveFromRaw(): "
           "representation is not even";
    const uintptr_t raw_copy = raw;
    // The original `raw` will possibly hold a pointer which had ownership
    // transferred and thus might no longer be valid. Hence reading `raw` again
    // is most likely a bug.
    MarkPoisoned(reinterpret_cast<const char*>(&raw), sizeof(uintptr_t));
    return CompactString(FromReprTag(), raw_copy);
  }

  // Views contents of a `CompactString` from the representation returned by
  // `CompactString::RawMove()` or `CompactString::RawView()`.
  //
  // Ownership is not transferred and `*raw` is unchanged.
  static absl::string_view ViewFromRaw(
      const uintptr_t* raw ABSL_ATTRIBUTE_LIFETIME_BOUND) {
    RIEGELI_ASSERT_EQ(*raw & 1, 0u)
        << "Failed precondition of CompactString::ViewFromRaw(): "
           "representation is not even";
    const uintptr_t tag = *raw & kTagMask;
    if (tag == kInlineTag) {
      return absl::string_view(inline_data(raw), inline_size(*raw));
    }
    return absl::string_view(allocated_data(*raw),
                             allocated_size_for_tag(tag, *raw));
  }

  // Returns the representation of a copy of the `CompactString` viewed from
  // the representation returned by `CompactString::RawMove()`.
  //
  // Equivalent to `RawMove(CompactString(ViewFromRaw(&raw)))`.
  static uintptr_t CopyRaw(uintptr_t raw) {
    RIEGELI_ASSERT_EQ(raw & 1, 0u)
        << "Failed precondition of CompactString::CopyRaw(): "
           "representation is not even";
    const uintptr_t tag = raw & kTagMask;
    if (tag == kInlineTag) return raw;
    return MakeRepr(absl::string_view(allocated_data(raw),
                                      allocated_size_for_tag(tag, raw)));
  }

  friend bool operator==(const CompactString& a, const CompactString& b) {
    return a.repr_ == b.repr_ || absl::string_view(a) == absl::string_view(b);
  }
  friend StrongOrdering RIEGELI_COMPARE(const CompactString& a,
                                        const CompactString& b) {
    if (a.repr_ == b.repr_) return StrongOrdering::equal;
    return riegeli::Compare(absl::string_view(a), absl::string_view(b));
  }

  template <typename T,
            std::enable_if_t<
                absl::conjunction<NotSameRef<CompactString, T>,
                                  std::is_convertible<T&&, BytesRef>>::value,
                int> = 0>
  friend bool operator==(const CompactString& a, T&& b) {
    return absl::string_view(a) == BytesRef(std::forward<T>(b));
  }
  template <typename T,
            std::enable_if_t<
                absl::conjunction<NotSameRef<CompactString, T>,
                                  std::is_convertible<T&&, BytesRef>>::value,
                int> = 0>
  friend StrongOrdering RIEGELI_COMPARE(const CompactString& a, T&& b) {
    return riegeli::Compare(absl::string_view(a), BytesRef(std::forward<T>(b)));
  }

  template <typename HashState>
  friend HashState AbslHashValue(HashState hash_state,
                                 const CompactString& self) {
    return HashState::combine(std::move(hash_state), absl::string_view(self));
  }

  // Default stringification by `absl::StrCat()` etc.
  template <typename Sink>
  friend void AbslStringify(Sink& dest, const CompactString& src) {
    dest.Append(absl::string_view(src));
  }

  friend std::ostream& operator<<(std::ostream& dest,
                                  const CompactString& src) {
    return dest << absl::string_view(src);
  }

  // Supports `absl::Format(&compact_string, format, args...)`.
  friend void AbslFormatFlush(CompactString* dest, absl::string_view src) {
    dest->append(src);
  }

  // Indicates support for:
  //  * `ExternalRef(CompactString&&)`
  //  * `ExternalRef(CompactString&&, substr)`
  friend void RiegeliSupportsExternalRef(CompactString*) {}

  // Supports `ExternalRef`.
  friend bool RiegeliExternalCopy(const CompactString* self) {
    return (self->repr_ & kTagMask) == kInlineTag;
  }

  // Supports `ExternalRef`.
  friend ExternalStorage RiegeliToExternalStorage(CompactString* self) {
    return ExternalStorage(
        reinterpret_cast<void*>(std::exchange(self->repr_, kInlineTag)),
        [](void* ptr) {
          const uintptr_t repr = reinterpret_cast<uintptr_t>(ptr);
          RIEGELI_ASSUME_NE(repr & kTagMask, kInlineTag)
              << "Failed precondition of "
                 "RiegeliToExternalStorage(CompactString*): "
                 "case excluded by RiegeliExternalCopy()";
          DeleteRepr(repr);
        });
  }

  // Supports `ExternalRef` and `Chain::Block`.
  friend void RiegeliDumpStructure(const CompactString* self,
                                   absl::string_view substr,
                                   std::ostream& dest) {
    self->DumpStructure(substr, dest);
  }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const CompactString* self,
                                        MemoryEstimator& memory_estimator) {
    self->RegisterSubobjects(memory_estimator);
  }

 private:
  struct FromReprTag {
    explicit FromReprTag() = default;
  };

  explicit CompactString(FromReprTag, uintptr_t raw) : repr_(raw) {}

  static constexpr size_t kTagBits = 3;
  static constexpr uintptr_t kTagMask = (1u << kTagBits) - 1;
  static constexpr uintptr_t kInlineTag = 6;

  static constexpr size_t kInlineCapacity =
      UnsignedMin(sizeof(uintptr_t) - 1, size_t{0xff >> kTagBits});

#if ABSL_IS_LITTLE_ENDIAN
  static constexpr size_t kInlineDataOffset = 1;
#elif ABSL_IS_BIG_ENDIAN
  static constexpr size_t kInlineDataOffset = 0;
#else
#error Unknown endianness
#endif

  char* inline_data() { return inline_data(&repr_); }
  const char* inline_data() const { return inline_data(&repr_); }

  static char* inline_data(uintptr_t* repr) {
    RIEGELI_ASSERT_EQ(*repr & kTagMask, kInlineTag)
        << "Failed precondition of CompactString::inline_data(): "
           "representation not inline";
    return reinterpret_cast<char*>(repr) + kInlineDataOffset;
  }
  static const char* inline_data(const uintptr_t* repr) {
    RIEGELI_ASSERT_EQ(*repr & kTagMask, kInlineTag)
        << "Failed precondition of CompactString::inline_data(): "
           "representation not inline";
    return reinterpret_cast<const char*>(repr) + kInlineDataOffset;
  }

  size_t inline_size() const { return inline_size(repr_); }

  static size_t inline_size(uintptr_t repr) {
    RIEGELI_ASSERT_EQ(repr & kTagMask, kInlineTag)
        << "Failed precondition of CompactString::inline_size(): "
           "representation not inline";
    const size_t size = IntCast<size_t>((repr & 0xff) >> kTagBits);
    // This assumption helps the compiler to reason about comparisons with
    // `size()`.
    RIEGELI_ASSUME_LE(size, kInlineCapacity)
        << "Failed invariant of CompactString: "
           "inline size never exceeds kInlineCapacity";
    return size;
  }

  char* allocated_data() const { return allocated_data(repr_); }

  static char* allocated_data(uintptr_t repr) {
    RIEGELI_ASSERT_NE(repr & kTagMask, kInlineTag)
        << "Failed precondition of CompactString::allocated_data(): "
           "representation not allocated";
    return reinterpret_cast<char*>(repr);
  }

  size_t allocated_size_for_tag(uintptr_t tag) const {
    return allocated_size_for_tag(tag, repr_);
  }

  static size_t allocated_size_for_tag(uintptr_t tag, uintptr_t repr) {
    if (tag == 2) return allocated_size<uint8_t>(repr);
    if (tag == 4) return allocated_size<uint16_t>(repr);
    if (tag == 0) return allocated_size<size_t>(repr);
    RIEGELI_ASSUME_UNREACHABLE() << "Impossible tag: " << tag;
  }

  template <typename T>
  size_t allocated_size() const {
    return allocated_size<T>(repr_);
  }

  template <typename T>
  static size_t allocated_size(uintptr_t repr) {
    const uintptr_t tag = repr & kTagMask;
    RIEGELI_ASSERT_EQ(tag == 0 ? 2 * sizeof(size_t) : tag, 2 * sizeof(T))
        << "Failed precondition of CompactString::allocated_size(): "
           "tag does not match size representation";
    T stored_size;
    std::memcpy(&stored_size, allocated_data(repr) - sizeof(T), sizeof(T));
    return size_t{stored_size};
  }

  void set_inline_size(size_t size) { set_inline_size(size, repr_); }

  static void set_inline_size(size_t size, uintptr_t& repr) {
    RIEGELI_ASSERT_EQ(repr & kTagMask, kInlineTag)
        << "Failed precondition of CompactString::set_inline_size(): "
           "representation not inline";
    repr = (repr & ~(0xff & ~kTagMask)) | (size << kTagBits);
  }

  template <typename T>
  void set_allocated_size(size_t size) {
    set_allocated_size<T>(size, repr_);
  }

  template <typename T>
  static void set_allocated_size(size_t size, uintptr_t repr) {
    const uintptr_t tag = repr & kTagMask;
    RIEGELI_ASSERT_EQ(tag == 0 ? 2 * sizeof(size_t) : tag, 2 * sizeof(T))
        << "Failed precondition of CompactString::set_allocated_size(): "
           "tag does not match size representation";
    const T stored_size = IntCast<T>(size);
    std::memcpy(allocated_data(repr) - sizeof(T), &stored_size, sizeof(T));
  }

  void set_allocated_size_for_tag(uintptr_t tag, size_t new_size);

  size_t allocated_capacity_for_tag(uintptr_t tag) const {
    return allocated_capacity_for_tag(tag, repr_);
  }

  static size_t allocated_capacity_for_tag(uintptr_t tag, uint64_t repr) {
    if (tag == 2) return allocated_capacity<uint8_t>(repr);
    if (tag == 4) return allocated_capacity<uint16_t>(repr);
    if (tag == 0) return allocated_capacity<size_t>(repr);
    RIEGELI_ASSUME_UNREACHABLE() << "Impossible tag: " << tag;
  }

  template <typename T>
  size_t allocated_capacity() const {
    return allocated_capacity<T>(repr_);
  }

  template <typename T>
  static size_t allocated_capacity(uint64_t repr) {
    const uintptr_t tag = repr & kTagMask;
    RIEGELI_ASSERT_EQ(tag == 0 ? 2 * sizeof(size_t) : tag, 2 * sizeof(T))
        << "Failed precondition of CompactString::allocated_capacity(): "
           "tag does not match capacity representation";
    T stored_capacity;
    std::memcpy(&stored_capacity, allocated_data(repr) - 2 * sizeof(T),
                sizeof(T));
    // This assumption helps the compiler to reason about comparisons with
    // `capacity()`.
    RIEGELI_ASSUME_GT(stored_capacity, kInlineCapacity)
        << "Failed invariant of CompactString: "
           "allocated capacity always exceeds kInlineCapacity";
    return size_t{stored_capacity};
  }

  template <typename T>
  static void set_allocated_capacity(size_t capacity, uintptr_t repr) {
    const uintptr_t tag = repr & kTagMask;
    RIEGELI_ASSERT_EQ(tag == 0 ? 2 * sizeof(size_t) : tag, 2 * sizeof(T))
        << "Failed precondition of CompactString::set_allocated_capacity(): "
           "tag does not match capacity representation";
    const T stored_capacity = IntCast<T>(capacity);
    std::memcpy(allocated_data(repr) - 2 * sizeof(T), &stored_capacity,
                sizeof(T));
  }

  static char* Allocate(size_t size) {
    return static_cast<char*>(NewAligned<void, 8>(size));
  }
  static void Free(char* ptr, size_t size) {
    DeleteAligned<void, 8>(ptr, size);
  }

  static uintptr_t MakeRepr(size_t size, size_t capacity);
  static uintptr_t MakeReprSlow(size_t size, size_t capacity);
  static uintptr_t MakeRepr(size_t size);
  static uintptr_t MakeRepr(absl::string_view src, size_t capacity);
  static uintptr_t MakeRepr(absl::string_view src);
  static void DeleteRepr(uintptr_t repr);

  void AssignSlow(absl::string_view src);
  void AssignSlow(const CompactString& that);
  char* ResizeSlow(size_t new_size, size_t min_capacity, size_t used_size);
  void ShrinkToFitSlow();
  char* AppendSlow(size_t length);
  void AppendSlow(absl::string_view src);
  void ReserveOneMoreByteSlow();

  void DumpStructure(absl::string_view substr, std::ostream& dest) const;
  template <typename MemoryEstimator>
  void RegisterSubobjects(MemoryEstimator& memory_estimator) const;

  uintptr_t repr_ = kInlineTag;
};

// Hash and equality which support heterogeneous lookup.
struct CompactStringHash {
  using is_transparent = void;
  size_t operator()(const CompactString& value) const {
    return absl::Hash<CompactString>()(value);
  }
  size_t operator()(absl::string_view value) const {
    return absl::Hash<absl::string_view>()(value);
  }
};
struct CompactStringEq {
  using is_transparent = void;
  bool operator()(const CompactString& a, const CompactString& b) const {
    return a == b;
  }
  bool operator()(const CompactString& a, absl::string_view b) const {
    return a == b;
  }
  bool operator()(absl::string_view a, const CompactString& b) const {
    return a == b;
  }
  bool operator()(absl::string_view a, absl::string_view b) const {
    return a == b;
  }
};

// Implementation details follow.

inline uintptr_t CompactString::MakeRepr(size_t size, size_t capacity) {
  RIEGELI_ASSERT_LE(size, capacity)
      << "Failed precondition of CompactString::MakeRepr(): "
         "size greater than capacity";
  if (capacity <= kInlineCapacity) {
    return uintptr_t{(size << kTagBits) + kInlineTag};
  }
  return MakeReprSlow(size, capacity);
}

inline uintptr_t CompactString::MakeRepr(size_t size) {
  return MakeRepr(size, size);
}

inline uintptr_t CompactString::MakeRepr(absl::string_view src,
                                         size_t capacity) {
  uintptr_t repr = MakeRepr(src.size(), capacity);
  // `std::memcpy(_, nullptr, 0)` is undefined.
  if (ABSL_PREDICT_TRUE(!src.empty())) {
    std::memcpy(
        capacity <= kInlineCapacity ? inline_data(&repr) : allocated_data(repr),
        src.data(), src.size());
  }
  return repr;
}

inline uintptr_t CompactString::MakeRepr(absl::string_view src) {
  return MakeRepr(src, src.size());
}

inline void CompactString::DeleteRepr(uintptr_t repr) {
  const uintptr_t tag = repr & kTagMask;
  if (tag == kInlineTag) return;
  const size_t offset = tag == 0 ? 2 * sizeof(size_t) : IntCast<size_t>(tag);
  Free(allocated_data(repr) - offset,
       allocated_capacity_for_tag(tag, repr) + offset);
}

inline CompactString::CompactString(const CompactString& that) {
  const uintptr_t that_tag = that.repr_ & kTagMask;
  if (that_tag == kInlineTag) {
    repr_ = that.repr_;
  } else {
    repr_ = MakeRepr(absl::string_view(that.allocated_data(),
                                       that.allocated_size_for_tag(that_tag)));
  }
}

inline CompactString& CompactString::operator=(const CompactString& that) {
  const uintptr_t that_tag = that.repr_ & kTagMask;
  if (that_tag == kInlineTag) {
    const uintptr_t tag = repr_ & kTagMask;
    if (tag == kInlineTag) {
      repr_ = that.repr_;
    } else {
      set_allocated_size_for_tag(tag, that.inline_size());
      RIEGELI_ASSERT_LE(kInlineCapacity, capacity())
          << "Failed invariant of CompactString: "
             "inline capacity always fits in a capacity";
      // Copy fixed `kInlineCapacity` instead of variable `that.inline_size()`.
      std::memcpy(allocated_data(), that.inline_data(), kInlineCapacity);
      // The `#ifdef` helps the compiler to realize that computing the arguments
      // is unnecessary if `MarkPoisoned()` does nothing.
#ifdef MEMORY_SANITIZER
      // This part got unpoisoned by copying `kInlineCapacity` instead of
      // `that.inline_size()`. Poison it again.
      MarkPoisoned(allocated_data() + that.inline_size(),
                   kInlineCapacity - that.inline_size());
#endif
    }
  } else {
    AssignSlow(that);
  }
  return *this;
}

inline CompactString& CompactString::operator=(BytesRef src) {
  if (ABSL_PREDICT_TRUE(src.size() <= capacity())) {
    set_size(src.size());
    // `std::memmove(_, nullptr, 0)` is undefined.
    if (ABSL_PREDICT_TRUE(!src.empty())) {
      // Use `std::memmove()` to support assigning from a substring of `*this`.
      std::memmove(data(), src.data(), src.size());
    }
  } else {
    AssignSlow(src);
  }
  return *this;
}

inline char* CompactString::data() ABSL_ATTRIBUTE_LIFETIME_BOUND {
  const uintptr_t tag = repr_ & kTagMask;
  if (tag == kInlineTag) return inline_data();
  return allocated_data();
}

inline const char* CompactString::data() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
  const uintptr_t tag = repr_ & kTagMask;
  if (tag == kInlineTag) return inline_data();
  return allocated_data();
}

inline size_t CompactString::size() const {
  const uintptr_t tag = repr_ & kTagMask;
  if (tag == kInlineTag) return inline_size();
  return allocated_size_for_tag(tag);
}

inline size_t CompactString::capacity() const {
  const uintptr_t tag = repr_ & kTagMask;
  if (tag == kInlineTag) return kInlineCapacity;
  return allocated_capacity_for_tag(tag);
}

inline CompactString::operator absl::string_view() const
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  return ViewFromRaw(&repr_);
}

inline char& CompactString::operator[](size_t index)
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  RIEGELI_ASSERT_LT(index, size())
      << "Failed precondition of CompactString::operator[]: index out of range";
  return data()[index];
}

inline const char& CompactString::operator[](size_t index) const
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  RIEGELI_ASSERT_LT(index, size())
      << "Failed precondition of CompactString::operator[]: index out of range";
  return data()[index];
}

inline char& CompactString::at(size_t index) ABSL_ATTRIBUTE_LIFETIME_BOUND {
  RIEGELI_CHECK_LT(index, size())
      << "Failed precondition of CompactString::at(): index out of range";
  return data()[index];
}

inline const char& CompactString::at(size_t index) const
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  RIEGELI_CHECK_LT(index, size())
      << "Failed precondition of CompactString::at(): index out of range";
  return data()[index];
}

inline char& CompactString::front() ABSL_ATTRIBUTE_LIFETIME_BOUND {
  RIEGELI_ASSERT(!empty())
      << "Failed precondition of CompactString::front(): empty string";
  return data()[0];
}

inline const char& CompactString::front() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
  RIEGELI_ASSERT(!empty())
      << "Failed precondition of CompactString::front(): empty string";
  return data()[0];
}

inline char& CompactString::back() ABSL_ATTRIBUTE_LIFETIME_BOUND {
  RIEGELI_ASSERT(!empty())
      << "Failed precondition of CompactString::back(): empty string";
  return data()[size() - 1];
}

inline const char& CompactString::back() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
  RIEGELI_ASSERT(!empty())
      << "Failed precondition of CompactString::back(): empty string";
  return data()[size() - 1];
}

inline void CompactString::set_size(size_t new_size) {
  RIEGELI_ASSERT_LE(new_size, capacity())
      << "Failed precondition of CompactString::SetSize(): size out of range";
  const uintptr_t tag = repr_ & kTagMask;
  if (tag == kInlineTag) {
    set_inline_size(new_size);
    return;
  }
  set_allocated_size_for_tag(tag, new_size);
}

inline void CompactString::set_allocated_size_for_tag(uintptr_t tag,
                                                      size_t new_size) {
  // The `#ifdef` helps the compiler to realize that computing the arguments is
  // unnecessary if `MarkPoisoned()` does nothing.
#ifdef MEMORY_SANITIZER
  if (new_size < allocated_size_for_tag(tag)) {
    MarkPoisoned(allocated_data() + new_size,
                 allocated_size_for_tag(tag) - new_size);
  }
#endif
  if (tag == 2) {
    set_allocated_size<uint8_t>(new_size);
  } else if (tag == 4) {
    set_allocated_size<uint16_t>(new_size);
  } else if (tag == 0) {
    set_allocated_size<size_t>(new_size);
  } else {
    RIEGELI_ASSUME_UNREACHABLE() << "Impossible tag: " << tag;
  }
}

inline void CompactString::resize(size_t new_size) {
  if (ABSL_PREDICT_TRUE(new_size <= capacity())) {
    set_size(new_size);
    return;
  }
  ResizeSlow(new_size, new_size, size());
}

inline char* CompactString::resize(size_t new_size, size_t used_size)
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  RIEGELI_ASSERT_LE(used_size, size())
      << "Failed precondition of CompactString::resize(): "
         "used size exceeds old size";
  RIEGELI_ASSERT_LE(used_size, new_size)
      << "Failed precondition of CompactString::resize(): "
         "used size exceeds new size";
  if (ABSL_PREDICT_TRUE(new_size <= capacity())) {
    // The `#ifdef` helps the compiler to realize that computing the arguments
    // is unnecessary if `MarkPoisoned()` does nothing.
#ifdef MEMORY_SANITIZER
    const uintptr_t tag = repr_ & kTagMask;
    if (tag != kInlineTag) {
      MarkPoisoned(
          allocated_data() + used_size,
          UnsignedMin(allocated_size_for_tag(tag), new_size) - used_size);
    }
#endif
    set_size(new_size);
    return data() + used_size;
  }
  return ResizeSlow(new_size, new_size, used_size);
}

inline void CompactString::reserve(size_t min_capacity) {
  if (ABSL_PREDICT_TRUE(min_capacity <= capacity())) return;
  const size_t used_size = size();
  ResizeSlow(used_size, min_capacity, used_size);
}

inline void CompactString::shrink_to_fit() {
  const uintptr_t tag = repr_ & kTagMask;
  if (tag == kInlineTag) return;
  ShrinkToFitSlow();
}

inline char* CompactString::append(size_t length)
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  const size_t old_size = size();
  const size_t old_capacity = capacity();
  if (ABSL_PREDICT_TRUE(length <= old_capacity - old_size)) {
    set_size(old_size + length);
    return data() + old_size;
  }
  return AppendSlow(length);
}

inline void CompactString::append(absl::string_view src) {
  // `std::memcpy(_, nullptr, 0)` is undefined.
  if (ABSL_PREDICT_TRUE(!src.empty())) {
    const size_t old_size = size();
    const size_t old_capacity = capacity();
    if (ABSL_PREDICT_TRUE(src.size() <= old_capacity - old_size)) {
      set_size(old_size + src.size());
      std::memcpy(data() + old_size, src.data(), src.size());
      return;
    }
    AppendSlow(src);
  }
}

inline const char* CompactString::c_str() ABSL_ATTRIBUTE_LIFETIME_BOUND {
  const size_t used_size = size();
  // Allocate just enough for NUL, do not call `reserve(used_size + 1)` here
  // because that could overallocate by 50%. In `c_str()` it is likely that the
  // string already has its final value.
  if (ABSL_PREDICT_FALSE(used_size == capacity())) ReserveOneMoreByteSlow();
  char* const ptr = data();
  ptr[used_size] = '\0';
  return ptr;
}

template <typename MemoryEstimator>
inline void CompactString::RegisterSubobjects(
    MemoryEstimator& memory_estimator) const {
  const uintptr_t tag = repr_ & kTagMask;
  if (tag == kInlineTag) return;
  const size_t offset = tag == 0 ? 2 * sizeof(size_t) : IntCast<size_t>(tag);
  memory_estimator.RegisterDynamicMemory(
      allocated_data() - offset, offset + allocated_capacity_for_tag(tag));
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_COMPACT_STRING_H_
