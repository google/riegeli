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

#include <stdint.h>

#include <cstddef>
#include <cstring>
#include <iosfwd>
#include <limits>
#include <new>
#include <string>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/hash/hash.h"
#include "absl/strings/string_view.h"
#include "absl/types/compare.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"

namespace riegeli {

// `CompactString` provides a subset of functionality of `std::string`, while
// having less space overhead (by about 2 pointers compared to libc++ and by
// about 3 pointers compared to libstdc++). It is useful for storing many short
// strings for a long time where each string owns its memory.
//
// A `CompactString` object internally consists of a pointer to heap-allocated
// data. The representation has 4 cases, distinguished by how the pointer is
// aligned modulo 4:
//  * 3 - not really a pointer but short string optimization: the size is stored
//        in bits [2..8), the data are stored in the remaining bytes
//  * 1 - the size is stored before the data as `uint8_t`
//  * 2 - the size is stored before the data as `uint16_t`
//  * 0 - the size is stored before the data as `size_t`
//
// The data are not followed by NUL.
//
// Since `CompactString` does not track `capacity()`, it does not provide
// appending, which would need to reallocate it each time. Use
// `CompactStringWriter` instead.
//
// Since `data()`, `size()`, `operator[]` etc. compute branches, for iteration
// it is faster to store the result of conversion to `absl::string_view` and
// iterate over that, or use `StringReader`.
class
#ifdef ABSL_ATTRIBUTE_TRIVIAL_ABI
    ABSL_ATTRIBUTE_TRIVIAL_ABI
#endif
        CompactString {
 public:
  CompactString() = default;

  // Creates a `CompactString` with the given size and uninitialized contents.
  explicit CompactString(size_t size) : repr_(MakeRepr(size)) {}

  // Creates a `CompactString` which holds a copy of `src`.
  explicit CompactString(absl::string_view src) : repr_(MakeRepr(src)) {}

  CompactString(const CompactString& that) : repr_(MakeRepr(that)) {}
  CompactString& operator=(const CompactString& that) {
    DeleteRepr(std::exchange(repr_, MakeRepr(that)));
    return *this;
  }

  CompactString(CompactString&& that) noexcept
      : repr_(std::exchange(that.repr_, kDefaultRepr)) {}
  CompactString& operator=(CompactString&& that) {
    DeleteRepr(std::exchange(repr_, std::exchange(that.repr_, kDefaultRepr)));
    return *this;
  }

  CompactString& operator=(absl::string_view src) {
    DeleteRepr(std::exchange(repr_, MakeRepr(src)));
    return *this;
  }

  ~CompactString() { DeleteRepr(repr_); }

  bool empty() const { return size() == 0; }

  char* data();
  const char* data() const;

  size_t size() const;

  operator absl::string_view() const;
  explicit operator std::string() const;

  char& operator[](size_t index);
  const char& operator[](size_t index) const;

  // Clear contents, freeing the memory.
  void clear() { DeleteRepr(std::exchange(repr_, kDefaultRepr)); }

  // Resizes to `index` which must be at most `size()`.
  void erase(size_t index);

  // Appends some uninitialized space if this can be done without reallocation.
  void GrowToCapacity();

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

  // Registers this `CompactString` with `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const CompactString& self,
                                        MemoryEstimator& memory_estimator) {
    self.RegisterSubobjectsImpl(memory_estimator);
  }

 private:
  static constexpr uintptr_t kDefaultRepr = 3;

  static constexpr size_t kMaxInlineSize =
      UnsignedMin(sizeof(uintptr_t) - 1, size_t{0xff >> 2});

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
    return reinterpret_cast<char*>(&repr) + InlineDataOffset();
  }
  static const char* inline_data(const uintptr_t& repr) {
    return reinterpret_cast<const char*>(&repr) + InlineDataOffset();
  }

  char* inline_data() { return inline_data(repr_); }
  const char* inline_data() const { return inline_data(repr_); }

  size_t inline_size() const { return IntCast<size_t>((repr_ & 0xff) >> 2); }

  static char* allocated_data(uintptr_t repr) {
    return reinterpret_cast<char*>(repr);
  }

  char* allocated_data() const { return allocated_data(repr_); }

  template <typename T>
  size_t allocated_size() const {
    T size;
    std::memcpy(&size, allocated_data() - sizeof(T), sizeof(T));
    return size_t{size};
  }

  size_t allocated_size(uintptr_t tag) const {
    return tag == 1   ? allocated_size<uint8_t>()
           : tag == 2 ? allocated_size<uint16_t>()
                      : allocated_size<size_t>();
  }

  template <typename T>
  static void set_allocated_size(uintptr_t repr, size_t size) {
    const T stored_size = IntCast<T>(size);
    std::memcpy(allocated_data(repr) - sizeof(T), &stored_size, sizeof(T));
  }

  template <typename T>
  void set_allocated_size(size_t size) {
    set_allocated_size<T>(repr_, size);
  }

  static char* Allocate(size_t size);
  static void Free(char* ptr);

  static uintptr_t MakeRepr(size_t size);
  static uintptr_t MakeRepr(absl::string_view src);
  static void DeleteRepr(uintptr_t repr);

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

inline char* CompactString::Allocate(size_t size) {
#if __cpp_aligned_new
#if __STDCPP_DEFAULT_NEW_ALIGNMENT__ >= 4
  return static_cast<char*>(operator new(size));
#else
  return static_cast<char*>(operator new(size, std::align_val_t(4)));
#endif
#else
#ifdef __STDCPP_DEFAULT_NEW_ALIGNMENT__
  static_assert(__STDCPP_DEFAULT_NEW_ALIGNMENT__ >= 4,
                "Unsupported default alignment");
#else
  static_assert(alignof(max_align_t) >= 4, "Unsupported default alignment");
#endif
  return static_cast<char*>(operator new(size));
#endif
}

inline void CompactString::Free(char* ptr) { operator delete(ptr); }

inline uintptr_t CompactString::MakeRepr(size_t size) {
  if (size <= kMaxInlineSize) return uintptr_t{(size << 2) | 3};
  uintptr_t repr;
  if (size <= 0xff) {
    repr = reinterpret_cast<uintptr_t>(Allocate(size + 1) + 1);
    set_allocated_size<uint8_t>(repr, size);
  } else if (size <= 0xffff) {
    repr = reinterpret_cast<uintptr_t>(Allocate(size + 2) + 2);
    set_allocated_size<uint16_t>(repr, size);
  } else {
    static_assert(sizeof(size_t) % 4 == 0, "Unsupported size_t size");
    RIEGELI_CHECK_LE(size, std::numeric_limits<size_t>::max() - sizeof(size_t))
        << "CompactString size overflow";
    repr = reinterpret_cast<uintptr_t>(Allocate(size + sizeof(size_t)) +
                                       sizeof(size_t));
    set_allocated_size<size_t>(repr, size);
  }
  return repr;
}

inline uintptr_t CompactString::MakeRepr(absl::string_view src) {
  uintptr_t repr = MakeRepr(src.size());
  // `std::memcpy(_, nullptr, 0)` is undefined.
  if (ABSL_PREDICT_TRUE(!src.empty())) {
    std::memcpy(
        src.size() <= kMaxInlineSize ? inline_data(repr) : allocated_data(repr),
        src.data(), src.size());
  }
  return repr;
}

inline void CompactString::DeleteRepr(uintptr_t repr) {
  uintptr_t tag = repr & 3;
  if (tag == 3) return;
  const size_t offset = tag == 0 ? sizeof(size_t) : IntCast<size_t>(tag);
  Free(allocated_data(repr) - offset);
}

inline char* CompactString::data() {
  const uintptr_t tag = repr_ & 3;
  return tag == 3 ? inline_data() : allocated_data();
}

inline const char* CompactString::data() const {
  const uintptr_t tag = repr_ & 3;
  return tag == 3 ? inline_data() : allocated_data();
}

inline size_t CompactString::size() const {
  const uintptr_t tag = repr_ & 3;
  return tag == 3 ? inline_size() : allocated_size(tag);
}

inline CompactString::operator absl::string_view() const {
  const uintptr_t tag = repr_ & 3;
  return tag == 3 ? absl::string_view(inline_data(), inline_size())
                  : absl::string_view(allocated_data(), allocated_size(tag));
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

inline void CompactString::erase(size_t index) {
  RIEGELI_ASSERT_LE(index, size())
      << "Failed precondition of CompactString::erase(): index out of range";
  const uintptr_t tag = repr_ & 3;
  if (tag == 3) {
    repr_ = (repr_ & ~(0xff & ~3)) | (index << 2);
    return;
  }
  if (tag == 1) {
    set_allocated_size<uint8_t>(index);
  } else if (tag == 2) {
    set_allocated_size<uint16_t>(index);
  } else {
    set_allocated_size<size_t>(index);
  }
}

inline void CompactString::GrowToCapacity() {
  const uintptr_t tag = repr_ & 3;
  if (tag == 3) repr_ = (repr_ & ~(0xff & ~3)) | (kMaxInlineSize << 2);
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
  uintptr_t tag = repr_ & 3;
  if (tag == 3) return;
  const size_t offset = tag == 0 ? sizeof(size_t) : IntCast<size_t>(tag);
  memory_estimator.RegisterDynamicMemory(allocated_data() - offset,
                                         allocated_size(tag) + offset);
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_COMPACT_STRING_H_
