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
#include <string>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/hash/hash.h"
#include "absl/strings/string_view.h"
#include "absl/types/compare.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/new_aligned.h"

namespace riegeli {

// `CompactString` provides a subset of functionality of `std::string`, while
// having less space overhead. It is useful for storing many short strings for
// a long time where each string owns its memory.
//
// A `CompactString` object internally consists of a pointer to heap-allocated
// data. The representation has 4 cases, distinguished by how the pointer is
// aligned modulo 8:
//  * 1 - not really a pointer but short string optimization: the size is
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
        CompactString {
 public:
  static constexpr size_t max_size() {
    return std::numeric_limits<size_t>::max() - 2 * sizeof(size_t);
  }

  CompactString() = default;

  // Creates a `CompactString` with the given size and uninitialized data.
  explicit CompactString(size_t size) : repr_(MakeRepr(size)) {}

  // Creates a `CompactString` which holds a copy of `src`.
  explicit CompactString(absl::string_view src) : repr_(MakeRepr(src)) {}

  CompactString(const CompactString& that);
  CompactString& operator=(const CompactString& that);

  CompactString(CompactString&& that) noexcept
      : repr_(std::exchange(that.repr_, kDefaultRepr)) {}
  CompactString& operator=(CompactString&& that) {
    DeleteRepr(std::exchange(repr_, std::exchange(that.repr_, kDefaultRepr)));
    return *this;
  }

  CompactString& operator=(absl::string_view src);

  ~CompactString() { DeleteRepr(repr_); }

  bool empty() const { return size() == 0; }

  // Returns the data pointer. Never `nullptr`.
  char* data();
  const char* data() const;

  size_t size() const;

  size_t capacity() const;

  operator absl::string_view() const;
  explicit operator std::string() const;

  char& operator[](size_t index);
  const char& operator[](size_t index) const;

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
  char* resize(size_t new_size, size_t used_size);

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
  char* append(size_t length);

  // Appends `src`.
  void append(absl::string_view src);

  // Ensures that `data()` are NUL-terminated after `size()` and returns
  // `data()`.
  //
  // In contrast to `std::string::c_str()`, this is a non-const operation.
  // It may reallocate the string and it writes the NUL each time.
  const char* c_str();

  absl::strong_ordering Compare(absl::string_view that) const;

  friend bool operator==(const CompactString& a, const CompactString& b) {
    return a.repr_ == b.repr_ || absl::string_view(a) == absl::string_view(b);
  }
  friend bool operator!=(const CompactString& a, const CompactString& b) {
    return !(a == b);
  }
  friend bool operator<(const CompactString& a, const CompactString& b) {
    return absl::string_view(a) < absl::string_view(b);
  }
  friend bool operator>(const CompactString& a, const CompactString& b) {
    return absl::string_view(a) > absl::string_view(b);
  }
  friend bool operator<=(const CompactString& a, const CompactString& b) {
    return absl::string_view(a) <= absl::string_view(b);
  }
  friend bool operator>=(const CompactString& a, const CompactString& b) {
    return absl::string_view(a) >= absl::string_view(b);
  }

  friend bool operator==(const CompactString& a, absl::string_view b) {
    return absl::string_view(a) == b;
  }
  friend bool operator!=(const CompactString& a, absl::string_view b) {
    return absl::string_view(a) != b;
  }
  friend bool operator<(const CompactString& a, absl::string_view b) {
    return absl::string_view(a) < b;
  }
  friend bool operator>(const CompactString& a, absl::string_view b) {
    return absl::string_view(a) > b;
  }
  friend bool operator<=(const CompactString& a, absl::string_view b) {
    return absl::string_view(a) <= b;
  }
  friend bool operator>=(const CompactString& a, absl::string_view b) {
    return absl::string_view(a) >= b;
  }

  friend bool operator==(absl::string_view a, const CompactString& b) {
    return a == absl::string_view(b);
  }
  friend bool operator!=(absl::string_view a, const CompactString& b) {
    return a != absl::string_view(b);
  }
  friend bool operator<(absl::string_view a, const CompactString& b) {
    return a < absl::string_view(b);
  }
  friend bool operator>(absl::string_view a, const CompactString& b) {
    return a > absl::string_view(b);
  }
  friend bool operator<=(absl::string_view a, const CompactString& b) {
    return a <= absl::string_view(b);
  }
  friend bool operator>=(absl::string_view a, const CompactString& b) {
    return a >= absl::string_view(b);
  }

  template <typename HashState>
  friend HashState AbslHashValue(HashState hash_state,
                                 const CompactString& self) {
    return HashState::combine(std::move(hash_state), absl::string_view(self));
  }

  // Default stringification by `absl::StrCat()` etc.
  template <typename Sink>
  friend void AbslStringify(Sink& sink, const CompactString& self) {
    sink.Append(absl::string_view(self));
  }

  friend std::ostream& operator<<(std::ostream& out,
                                  const CompactString& self) {
    return out << absl::string_view(self);
  }

  // Support `absl::Format(&compact_string, format, args...)`.
  friend void AbslFormatFlush(CompactString* dest, absl::string_view src) {
    dest->append(src);
  }

  // Registers this `CompactString` with `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const CompactString& self,
                                        MemoryEstimator& memory_estimator) {
    self.RegisterSubobjectsImpl(memory_estimator);
  }

 private:
  static constexpr uintptr_t kDefaultRepr = 1;

  static constexpr size_t kInlineCapacity =
      UnsignedMin(sizeof(uintptr_t) - 1, size_t{0xff >> 3});

  // For Little Endian, returns 1.
  // For Big Endian, returns 0.
  //
  // TODO: Use `std::endian` instead when C++20 is available.
  //
  // With the following implementation the result is not known at preprocessing
  // time nor during constexpr evaluation, but this function is in practice
  // optimized out to a constant.
  static size_t InlineDataOffset() {
    const uintptr_t value = 1;
    return size_t{reinterpret_cast<const unsigned char*>(&value)[0]};
  }

  static char* inline_data(uintptr_t& repr) {
    RIEGELI_ASSERT_EQ(repr & 7, 1u)
        << "Failed precondition of CompactString::inline_data(): "
           "representation not inline";
    return reinterpret_cast<char*>(&repr) + InlineDataOffset();
  }
  static const char* inline_data(const uintptr_t& repr) {
    RIEGELI_ASSERT_EQ(repr & 7, 1u)
        << "Failed precondition of CompactString::inline_data(): "
           "representation not inline";
    return reinterpret_cast<const char*>(&repr) + InlineDataOffset();
  }

  char* inline_data() { return inline_data(repr_); }
  const char* inline_data() const { return inline_data(repr_); }

  size_t inline_size() const {
    RIEGELI_ASSERT_EQ(repr_ & 7, 1u)
        << "Failed precondition of CompactString::inline_size(): "
           "representation not inline";
    const size_t size = IntCast<size_t>((repr_ & 0xff) >> 3);
    if (size > kInlineCapacity) {
      // The assertion helps the compiler to reason about comparisons with
      // `size()`.
      RIEGELI_ASSERT_UNREACHABLE()
          << "Inline size never exceeds kInlineCapacity";
    }
    return size;
  }

  static char* allocated_data(uintptr_t repr) {
    RIEGELI_ASSERT_NE(repr & 7, 1u)
        << "Failed precondition of CompactString::allocated_data(): "
           "representation not allocated";
    return reinterpret_cast<char*>(repr);
  }

  char* allocated_data() const { return allocated_data(repr_); }

  template <typename T>
  size_t allocated_size() const {
    const uintptr_t tag = repr_ & 7;
    RIEGELI_ASSERT_EQ(tag == 0 ? 2 * sizeof(size_t) : IntCast<size_t>(tag),
                      2 * sizeof(T))
        << "Failed precondition of CompactString::allocated_size(): "
           "tag does not match size representation";
    T stored_size;
    std::memcpy(&stored_size, allocated_data() - sizeof(T), sizeof(T));
    return size_t{stored_size};
  }

  size_t allocated_size_for_tag(uintptr_t tag) const {
    if (tag == 2) return allocated_size<uint8_t>();
    if (tag == 4) return allocated_size<uint16_t>();
    if (tag == 0) return allocated_size<size_t>();
    RIEGELI_ASSERT_UNREACHABLE() << "Impossible tag: " << tag;
  }

  static void set_inline_size(size_t size, uintptr_t& repr) {
    RIEGELI_ASSERT_EQ(repr & 7, 1u)
        << "Failed precondition of CompactString::set_inline_size(): "
           "representation not inline";
    repr = (repr & ~(0xff & ~7)) | (size << 3);
  }

  void set_inline_size(size_t size) { set_inline_size(size, repr_); }

  template <typename T>
  static void set_allocated_size(size_t size, uintptr_t repr) {
    const uintptr_t tag = repr & 7;
    RIEGELI_ASSERT_EQ(tag == 0 ? 2 * sizeof(size_t) : IntCast<size_t>(tag),
                      2 * sizeof(T))
        << "Failed precondition of CompactString::set_allocated_size(): "
           "tag does not match size representation";
    const T stored_size = IntCast<T>(size);
    std::memcpy(allocated_data(repr) - sizeof(T), &stored_size, sizeof(T));
  }

  template <typename T>
  void set_allocated_size(size_t size) {
    set_allocated_size<T>(size, repr_);
  }

  template <typename T>
  static size_t allocated_capacity(uint64_t repr) {
    const uintptr_t tag = repr & 7;
    RIEGELI_ASSERT_EQ(tag == 0 ? 2 * sizeof(size_t) : IntCast<size_t>(tag),
                      2 * sizeof(T))
        << "Failed precondition of CompactString::allocated_capacity(): "
           "tag does not match capacity representation";
    T stored_capacity;
    std::memcpy(&stored_capacity, allocated_data(repr) - 2 * sizeof(T),
                sizeof(T));
    if (stored_capacity <= kInlineCapacity) {
      // The assertion helps the compiler to reason about comparisons with
      // `capacity()`.
      RIEGELI_ASSERT_UNREACHABLE()
          << "Allocated capacity always exceeds kInlineCapacity";
    }
    return size_t{stored_capacity};
  }

  template <typename T>
  size_t allocated_capacity() const {
    return allocated_capacity<T>(repr_);
  }

  static size_t allocated_capacity_for_tag(uintptr_t tag, uint64_t repr) {
    if (tag == 2) return allocated_capacity<uint8_t>(repr);
    if (tag == 4) return allocated_capacity<uint16_t>(repr);
    if (tag == 0) return allocated_capacity<size_t>(repr);
    RIEGELI_ASSERT_UNREACHABLE() << "Impossible tag: " << tag;
  }

  size_t allocated_capacity_for_tag(uintptr_t tag) const {
    return allocated_capacity_for_tag(tag, repr_);
  }

  template <typename T>
  static void set_allocated_capacity(size_t capacity, uintptr_t repr) {
    const uintptr_t tag = repr & 7;
    RIEGELI_ASSERT_EQ(tag == 0 ? 2 * sizeof(size_t) : IntCast<size_t>(tag),
                      2 * sizeof(T))
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
    return DeleteAligned<void, 8>(ptr, size);
  }

  static uintptr_t MakeRepr(size_t size, size_t capacity);
  static uintptr_t MakeReprSlow(size_t size, size_t capacity);
  static uintptr_t MakeRepr(size_t size);
  static uintptr_t MakeRepr(absl::string_view src, size_t capacity);
  static uintptr_t MakeRepr(absl::string_view src);
  static void DeleteRepr(uintptr_t repr);

  void AssignSlow(absl::string_view src);
  char* ResizeSlow(size_t new_size, size_t min_capacity, size_t used_size);
  void ShrinkToFitSlow();
  char* AppendSlow(size_t length);
  void AppendSlow(absl::string_view src);

  template <typename MemoryEstimator>
  void RegisterSubobjectsImpl(MemoryEstimator& memory_estimator) const;

  uintptr_t repr_ = kDefaultRepr;
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
  if (capacity <= kInlineCapacity) return uintptr_t{(size << 3) | 1};
  return MakeReprSlow(size, capacity);
}

inline uintptr_t CompactString::MakeRepr(size_t size) {
  return MakeRepr(size, size);
}

inline uintptr_t CompactString::MakeRepr(absl::string_view src,
                                         size_t capacity) {
  uintptr_t repr = MakeRepr(src.size(), capacity);
  if (ABSL_PREDICT_TRUE(
          // `std::memcpy(_, nullptr, 0)` is undefined.
          !src.empty())) {
    std::memcpy(
        capacity <= kInlineCapacity ? inline_data(repr) : allocated_data(repr),
        src.data(), src.size());
  }
  return repr;
}

inline uintptr_t CompactString::MakeRepr(absl::string_view src) {
  return MakeRepr(src, src.size());
}

inline void CompactString::DeleteRepr(uintptr_t repr) {
  const uintptr_t tag = repr & 7;
  if (tag == 1) return;
  const size_t offset = tag == 0 ? 2 * sizeof(size_t) : IntCast<size_t>(tag);
  Free(allocated_data(repr) - offset,
       allocated_capacity_for_tag(tag, repr) + offset);
}

inline CompactString::CompactString(const CompactString& that) {
  const uintptr_t that_tag = that.repr_ & 7;
  if (that_tag == 1) {
    repr_ = that.repr_;
  } else {
    repr_ = MakeRepr(absl::string_view(that.allocated_data(),
                                       that.allocated_size_for_tag(that_tag)));
  }
}

inline CompactString& CompactString::operator=(const CompactString& that) {
  const uintptr_t that_tag = that.repr_ & 7;
  if (that_tag == 1) {
    const size_t that_size = that.inline_size();
    RIEGELI_ASSERT_LE(that_size, capacity())
        << "Inline size always fits in a capacity";
    set_size(that_size);
    // Use `std::memmove()` to support assigning from `*this`.
    std::memmove(data(), that.inline_data(), that.inline_size());
  } else {
    const size_t that_size = that.allocated_size_for_tag(that_tag);
    if (ABSL_PREDICT_TRUE(that_size <= capacity())) {
      set_size(that_size);
      // Use `std::memmove()` to support assigning from `*this`.
      std::memmove(data(), that.allocated_data(), that_size);
    } else {
      AssignSlow(absl::string_view(that.allocated_data(), that_size));
    }
  }
  return *this;
}

inline CompactString& CompactString::operator=(absl::string_view src) {
  if (ABSL_PREDICT_TRUE(src.size() <= capacity())) {
    set_size(src.size());
    if (ABSL_PREDICT_TRUE(
            // `std::memmove(_, nullptr, 0)` is undefined.
            !src.empty())) {
      // Use `std::memmove()` to support assigning from a substring of `*this`.
      std::memmove(data(), src.data(), src.size());
    }
  } else {
    AssignSlow(src);
  }
  return *this;
}

inline char* CompactString::data() {
  const uintptr_t tag = repr_ & 7;
  if (tag == 1) return inline_data();
  return allocated_data();
}

inline const char* CompactString::data() const {
  const uintptr_t tag = repr_ & 7;
  if (tag == 1) return inline_data();
  return allocated_data();
}

inline size_t CompactString::size() const {
  const uintptr_t tag = repr_ & 7;
  if (tag == 1) return inline_size();
  return allocated_size_for_tag(tag);
}

inline size_t CompactString::capacity() const {
  const uintptr_t tag = repr_ & 7;
  if (tag == 1) return kInlineCapacity;
  return allocated_capacity_for_tag(tag);
}

inline CompactString::operator absl::string_view() const {
  const uintptr_t tag = repr_ & 7;
  if (tag == 1) return absl::string_view(inline_data(), inline_size());
  return absl::string_view(allocated_data(), allocated_size_for_tag(tag));
}

inline CompactString::operator std::string() const {
  return std::string(absl::string_view(*this));
}

inline char& CompactString::operator[](size_t index) {
  RIEGELI_ASSERT_LT(index, size())
      << "Failed precondition of CompactString::operator[]: index out of range";
  return data()[index];
}

inline const char& CompactString::operator[](size_t index) const {
  RIEGELI_ASSERT_LT(index, size())
      << "Failed precondition of CompactString::operator[]: index out of range";
  return data()[index];
}

inline void CompactString::set_size(size_t new_size) {
  RIEGELI_ASSERT_LE(new_size, capacity())
      << "Failed precondition of CompactString::SetSize(): size out of range";
  // The `#ifdef` helps the compiler to realize that computing the arguments is
  // unnecessary if `MarkPoisoned()` does nothing.
#ifdef MEMORY_SANITIZER
  if (new_size < size()) MarkPoisoned(data() + new_size, size() - new_size);
#endif
  const uintptr_t tag = repr_ & 7;
  if (tag == 1) {
    set_inline_size(new_size);
  } else if (tag == 2) {
    set_allocated_size<uint8_t>(new_size);
  } else if (tag == 4) {
    set_allocated_size<uint16_t>(new_size);
  } else if (tag == 0) {
    set_allocated_size<size_t>(new_size);
  } else {
    RIEGELI_ASSERT_UNREACHABLE() << "Impossible tag: " << tag;
  }
}

inline void CompactString::resize(size_t new_size) {
  if (ABSL_PREDICT_TRUE(new_size <= capacity())) {
    set_size(new_size);
    return;
  }
  ResizeSlow(new_size, new_size, size());
}

inline char* CompactString::resize(size_t new_size, size_t used_size) {
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
    MarkPoisoned(data() + used_size, UnsignedMin(size(), new_size) - used_size);
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
  const uintptr_t tag = repr_ & 7;
  if (tag == 1) return;
  return ShrinkToFitSlow();
}

inline char* CompactString::append(size_t length) {
  const size_t old_size = size();
  const size_t old_capacity = capacity();
  if (ABSL_PREDICT_TRUE(length <= old_capacity - old_size)) {
    set_size(old_size + length);
    return data() + old_size;
  }
  return AppendSlow(length);
}

inline void CompactString::append(absl::string_view src) {
  if (ABSL_PREDICT_TRUE(
          // `std::memcpy(_, nullptr, 0)` is undefined.
          !src.empty())) {
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

inline const char* CompactString::c_str() {
  const size_t used_size = size();
  reserve(used_size + 1);
  char* const ptr = data();
  ptr[used_size] = '\0';
  return ptr;
}

inline absl::strong_ordering CompactString::Compare(
    absl::string_view that) const {
  const int result = absl::string_view(*this).compare(that);
  if (result < 0) return absl::strong_ordering::less;
  if (result > 0) return absl::strong_ordering::greater;
  return absl::strong_ordering::equivalent;
}

template <typename MemoryEstimator>
inline void CompactString::RegisterSubobjectsImpl(
    MemoryEstimator& memory_estimator) const {
  const uintptr_t tag = repr_ & 7;
  if (tag == 1) return;
  const size_t offset = tag == 0 ? 2 * sizeof(size_t) : IntCast<size_t>(tag);
  memory_estimator.RegisterDynamicMemory(
      allocated_data() - offset, allocated_capacity_for_tag(tag) + offset);
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_COMPACT_STRING_H_
