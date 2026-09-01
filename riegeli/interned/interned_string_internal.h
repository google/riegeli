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

#ifndef RIEGELI_INTERNED_INTERNED_STRING_INTERNAL_H_
#define RIEGELI_INTERNED_INTERNED_STRING_INTERNAL_H_

#include <stddef.h>
#include <stdint.h>

#include <atomic>
#include <cstddef>
#include <limits>
#include <memory>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/numeric/bits.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/external_data.h"
#include "riegeli/base/new_aligned.h"
#include "riegeli/base/ownership.h"
#include "riegeli/interned/interned_common_internal.h"
#include "riegeli/interned/interned_internal.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli::interned_internal {

template <typename Encoder, typename Interner>
class SharedStringRepr;

// Stores an interned string with its reference count and interner pointer
// (for local interners).
//
// An `InternedStringRepr` is referenced from one `Shard` and from some number
// of `InternedString::Optional` instances. The reference from `Shard` is
// conceptually a weak reference and is not included in the reference count.
template <typename Encoder, typename Interner>
class alignas(UnsignedMax(Interner::kAlignment, sizeof(size_t)))
    InternedStringRepr {
 private:
  struct Deleter {
    void operator()(const InternedStringRepr* ptr) const {
      DeleteAligned<const InternedStringRepr>(ptr, ptr->object_size());
    }
  };

 public:
  using UniqueRepr = std::unique_ptr<const InternedStringRepr, Deleter>;

  static constexpr size_t kMaxSize = std::numeric_limits<size_t>::max() >> 2;

  // Supports `Shard::Intern()`.
  template <typename Arg>
  static UniqueRepr New(const Arg& value, Interner interner) {
    const size_t size = Encoder::EncodedSize(value);
    RIEGELI_CHECK_LE(size, kMaxSize)
        << "Failed precondition of InternedString: string size overflow";
    return UniqueRepr(
        NewAligned<const InternedStringRepr>(object_size_for_size(size), value,
                                             size, std::move(interner)),
        Deleter());
  }

  constexpr InternedStringRepr() noexcept
      : ref_count_(Shard::kImmortal), encoded_{} {}

  // Public for `NewAligned()`.
  template <typename Arg>
  explicit InternedStringRepr(const Arg& value, size_t size, Interner interner)
      : interner_(std::move(interner)) {
    char* dest;
    if constexpr (kAlignment == 1) {
      if (ABSL_PREDICT_TRUE(size <= kMaxSmallSize)) {
        encoded_[0] = static_cast<char>(size << 1);
        dest = small_data();
      } else if (size <= kMaxMediumSize) {
        WriteLittleEndian16(IntCast<uint16_t>((size << 2) | 1), encoded_);
        dest = medium_data();
      } else {
        WriteLittleEndianSize((size << 2) | 3, encoded_);
        dest = large_data();
      }
    } else if constexpr (kAlignment < sizeof(size_t)) {
      if (ABSL_PREDICT_TRUE(size <= kMaxMediumSize)) {
        WriteLittleEndian16(IntCast<uint16_t>(size << 1), encoded_);
        dest = medium_data();
      } else {
        WriteLittleEndianSize((size << 1) | 1, encoded_);
        dest = large_data();
      }
    } else {
      WriteLittleEndianSize(size, encoded_);
      dest = large_data();
    }
    Encoder::Encode(value, dest);
  }

  InternedStringRepr(const InternedStringRepr&) = delete;
  InternedStringRepr& operator=(const InternedStringRepr&) = delete;

  static const InternedStringRepr* FromData(const char* data) {
    if constexpr (kAlignment < sizeof(size_t)) {
      return reinterpret_cast<const InternedStringRepr*>(
          RoundUp<sizeof(size_t)>(reinterpret_cast<uintptr_t>(data) -
                                  sizeof(InternedStringRepr)));
    } else {
      return reinterpret_cast<const InternedStringRepr*>(
          data - sizeof(InternedStringRepr));
    }
  }

  static size_t SizeFromData(const char* data) {
    if constexpr (kAlignment == 1) {
      const uintptr_t ptr = reinterpret_cast<uintptr_t>(data);
      if (ABSL_PREDICT_TRUE((ptr & 1) != 0)) {
        const size_t size = size_t{static_cast<uint8_t>(data[-1])} >> 1;
        RIEGELI_ASSUME_LE(size, kMaxSmallSize);
        return size;
      } else if ((ptr & 2) != 0) {
        const size_t size =
            size_t{ReadLittleEndian16(data - kMediumDataOffset)} >> 2;
        RIEGELI_ASSUME_GT(size, kMaxSmallSize);
        RIEGELI_ASSUME_LE(size, kMaxMediumSize);
        return size;
      } else {
        const size_t size = ReadLittleEndianSize(data - kLargeDataOffset) >> 2;
        RIEGELI_ASSUME_GT(size, kMaxMediumSize);
        RIEGELI_ASSUME_LE(size, kMaxSize);
        return size;
      }
    } else if constexpr (kAlignment < sizeof(size_t)) {
      const uintptr_t ptr = reinterpret_cast<uintptr_t>(data);
      if (ABSL_PREDICT_TRUE((ptr & kAlignment) != 0)) {
        const size_t size =
            size_t{ReadLittleEndian16(data - kMediumDataOffset)} >> 1;
        RIEGELI_ASSUME_LE(size, kMaxMediumSize);
        return size;
      } else {
        const size_t size = ReadLittleEndianSize(data - kLargeDataOffset) >> 1;
        RIEGELI_ASSUME_GT(size, kMaxMediumSize);
        RIEGELI_ASSUME_LE(size, kMaxSize);
        return size;
      }
    } else {
      const size_t size = ReadLittleEndianSize(data - kLargeDataOffset);
      RIEGELI_ASSUME_LE(size, kMaxSize);
      return size;
    }
  }

  static bool EmptyFromData(const char* data) {
    if constexpr (kAlignment == 1) {
      const uintptr_t ptr = reinterpret_cast<uintptr_t>(data);
      return ABSL_PREDICT_TRUE((ptr & 1) != 0) && data[-1] == '\0';
    } else if constexpr (kAlignment < sizeof(size_t)) {
      const uintptr_t ptr = reinterpret_cast<uintptr_t>(data);
      return ABSL_PREDICT_TRUE((ptr & kAlignment) != 0) &&
             ReadLittleEndian16(data - kMediumDataOffset) == 0;
    } else {
      return ReadLittleEndianSize(data - kLargeDataOffset) == 0;
    }
  }

  // Supports `SharedStringRepr`.
  void Ref() const { Shard::Ref(*this); }
  void Unref() const { Shard::Unref(*this); }
  size_t GetCount() const { return Shard::GetCount(*this); }

  void Immortalize() const {
    ref_count_.store(Shard::kImmortal, std::memory_order_relaxed);
  }

  const char* data() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    if constexpr (kAlignment == 1) {
      const uint8_t first_byte = static_cast<uint8_t>(encoded_[0]);
      if (ABSL_PREDICT_TRUE((first_byte & 1) == 0)) {
        return small_data();
      } else if ((first_byte & 3) == 1) {
        return medium_data();
      } else {
        return large_data();
      }
    } else if constexpr (kAlignment < sizeof(size_t)) {
      const uint8_t first_byte = static_cast<uint8_t>(encoded_[0]);
      if (ABSL_PREDICT_TRUE((first_byte & 1) == 0)) {
        return medium_data();
      } else {
        return large_data();
      }
    } else {
      return large_data();
    }
  }

  size_t size() const {
    if constexpr (kAlignment == 1) {
      const uint8_t first_byte = static_cast<uint8_t>(encoded_[0]);
      if (ABSL_PREDICT_TRUE((first_byte & 1) == 0)) {
        const size_t size = size_t{first_byte} >> 1;
        RIEGELI_ASSUME_LE(size, kMaxSmallSize);
        return size;
      } else if ((first_byte & 3) == 1) {
        const size_t size = size_t{ReadLittleEndian16(encoded_)} >> 2;
        RIEGELI_ASSUME_GT(size, kMaxSmallSize);
        RIEGELI_ASSUME_LE(size, kMaxMediumSize);
        return size;
      } else {
        const size_t size = ReadLittleEndianSize(encoded_) >> 2;
        RIEGELI_ASSUME_GT(size, kMaxMediumSize);
        RIEGELI_ASSUME_LE(size, kMaxSize);
        return size;
      }
    } else if constexpr (kAlignment < sizeof(size_t)) {
      const uint8_t first_byte = static_cast<uint8_t>(encoded_[0]);
      if (ABSL_PREDICT_TRUE((first_byte & 1) == 0)) {
        const size_t size = size_t{ReadLittleEndian16(encoded_)} >> 1;
        RIEGELI_ASSUME_LE(size, kMaxMediumSize);
        return size;
      } else {
        const size_t size = ReadLittleEndianSize(encoded_) >> 1;
        RIEGELI_ASSUME_GT(size, kMaxMediumSize);
        RIEGELI_ASSUME_LE(size, kMaxSize);
        return size;
      }
    } else {
      const size_t size = ReadLittleEndianSize(encoded_);
      RIEGELI_ASSUME_LE(size, kMaxSize);
      return size;
    }
  }

  absl::string_view value() const {
    if constexpr (kAlignment == 1) {
      const uint8_t first_byte = static_cast<uint8_t>(encoded_[0]);
      if (ABSL_PREDICT_TRUE((first_byte & 1) == 0)) {
        const size_t size = size_t{first_byte} >> 1;
        RIEGELI_ASSUME_LE(size, kMaxSmallSize);
        return absl::string_view(small_data(), size);
      } else if ((first_byte & 3) == 1) {
        const size_t size = size_t{ReadLittleEndian16(encoded_)} >> 2;
        RIEGELI_ASSUME_GT(size, kMaxSmallSize);
        RIEGELI_ASSUME_LE(size, kMaxMediumSize);
        return absl::string_view(medium_data(), size);
      } else {
        const size_t size = ReadLittleEndianSize(encoded_) >> 2;
        RIEGELI_ASSUME_GT(size, kMaxMediumSize);
        RIEGELI_ASSUME_LE(size, kMaxSize);
        return absl::string_view(large_data(), size);
      }
    } else if constexpr (kAlignment < sizeof(size_t)) {
      const uint8_t first_byte = static_cast<uint8_t>(encoded_[0]);
      if (ABSL_PREDICT_TRUE((first_byte & 1) == 0)) {
        const size_t size = size_t{ReadLittleEndian16(encoded_)} >> 1;
        RIEGELI_ASSUME_LE(size, kMaxMediumSize);
        return absl::string_view(medium_data(), size);
      } else {
        const size_t size = ReadLittleEndianSize(encoded_) >> 1;
        RIEGELI_ASSUME_GT(size, kMaxMediumSize);
        RIEGELI_ASSUME_LE(size, kMaxSize);
        return absl::string_view(large_data(), size);
      }
    } else {
      const size_t size = ReadLittleEndianSize(encoded_);
      RIEGELI_ASSUME_LE(size, kMaxSize);
      return absl::string_view(large_data(), size);
    }
  }

  std::atomic<size_t>& ref_count() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return ref_count_;
  }
  const Interner& interner() const { return interner_; }

  // Supports `MemoryEstimator`.
  friend size_t RiegeliDynamicSizeOf(const InternedStringRepr* self) {
    return self->object_size();
  }

 private:
  using Shard = Shard<InternedStringRepr, SharedStringRepr<Encoder, Interner>,
                      typename Encoder::Hash, typename Encoder::Eq, Interner>;

  static constexpr size_t kAlignment = Interner::kAlignment;
  static_assert(absl::has_single_bit(kAlignment));
  static constexpr size_t kEncodedOffset =
      offsetof(InternedStringRepr, encoded_);
  static constexpr size_t kMediumDataOffset =
      UnsignedMax(kAlignment, size_t{2});
  static constexpr size_t kLargeDataOffset =
      sizeof(InternedStringRepr) - kEncodedOffset;

  static constexpr size_t kMaxSmallSize = 0x7f;
  static constexpr size_t kMaxMediumSize = kAlignment == 1 ? 0x3fff : 0x7fff;

  const char* small_data() const {
    return reinterpret_cast<const char*>(this) + (kEncodedOffset + 1);
  }
  char* small_data() {
    return reinterpret_cast<char*>(this) + (kEncodedOffset + 1);
  }

  const char* medium_data() const {
    return AssumeAligned<kAlignment>(reinterpret_cast<const char*>(this) +
                                     (kEncodedOffset + kMediumDataOffset));
  }
  char* medium_data() {
    return AssumeAligned<kAlignment>(reinterpret_cast<char*>(this) +
                                     (kEncodedOffset + kMediumDataOffset));
  }

  const char* large_data() const {
    return AssumeAligned<kAlignment>(reinterpret_cast<const char*>(this + 1));
  }
  char* large_data() {
    return AssumeAligned<kAlignment>(reinterpret_cast<char*>(this + 1));
  }

  static constexpr size_t small_object_size(size_t size) {
    return kEncodedOffset + UnsignedMax(size_t{1} + size, sizeof(size_t));
  }
  static constexpr size_t medium_object_size(size_t size) {
    return kEncodedOffset +
           UnsignedMax(kMediumDataOffset + size, sizeof(size_t));
  }
  static constexpr size_t large_object_size(size_t size) {
    return sizeof(InternedStringRepr) + size;
  }

  static size_t object_size_for_size(size_t size) {
    if constexpr (kAlignment == 1) {
      if (ABSL_PREDICT_TRUE(size <= kMaxSmallSize)) {
        return small_object_size(size);
      } else if (size <= kMaxMediumSize) {
        return medium_object_size(size);
      } else {
        return large_object_size(size);
      }
    } else if constexpr (kAlignment < sizeof(size_t)) {
      if (ABSL_PREDICT_TRUE(size <= kMaxMediumSize)) {
        return medium_object_size(size);
      } else {
        return large_object_size(size);
      }
    } else {
      return large_object_size(size);
    }
  }

  size_t object_size() const {
    if constexpr (kAlignment == 1) {
      const uint8_t first_byte = static_cast<uint8_t>(encoded_[0]);
      if (ABSL_PREDICT_TRUE((first_byte & 1) == 0)) {
        const size_t size = size_t{first_byte} >> 1;
        RIEGELI_ASSUME_LE(size, kMaxSmallSize);
        return small_object_size(size);
      } else if ((first_byte & 3) == 1) {
        const size_t size = size_t{ReadLittleEndian16(encoded_)} >> 2;
        RIEGELI_ASSUME_GT(size, kMaxSmallSize);
        RIEGELI_ASSUME_LE(size, kMaxMediumSize);
        return medium_object_size(size);
      } else {
        const size_t size = ReadLittleEndianSize(encoded_) >> 2;
        RIEGELI_ASSUME_GT(size, kMaxMediumSize);
        RIEGELI_ASSUME_LE(size, kMaxSize);
        return large_object_size(size);
      }
    } else if constexpr (kAlignment < sizeof(size_t)) {
      const uint8_t first_byte = static_cast<uint8_t>(encoded_[0]);
      if (ABSL_PREDICT_TRUE((first_byte & 1) == 0)) {
        const size_t size = size_t{ReadLittleEndian16(encoded_)} >> 1;
        RIEGELI_ASSUME_LE(size, kMaxMediumSize);
        return medium_object_size(size);
      } else {
        const size_t size = ReadLittleEndianSize(encoded_) >> 1;
        RIEGELI_ASSUME_GT(size, kMaxMediumSize);
        RIEGELI_ASSUME_LE(size, kMaxSize);
        return large_object_size(size);
      }
    } else {
      const size_t size = ReadLittleEndianSize(encoded_);
      RIEGELI_ASSUME_LE(size, kMaxSize);
      return large_object_size(size);
    }
  }

  mutable std::atomic<size_t> ref_count_ = 1;
  ABSL_ATTRIBUTE_NO_UNIQUE_ADDRESS Interner interner_;

  // The size is encoded in 1 byte, 2 bytes, or as `size_t`, at the beginning of
  // `encoded_`. The data follow, aligned to `kAlignment`.
  //
  // Decoding from `InternedStringRepr` distinguishes the cases by the lowest
  // 1 or 2 bits of `encoded_[0]`, except that if `kAlignment >= sizeof(size_t)`
  // then only `large_data()` is used and the size is stored as `size_t`.
  //
  // Decoding from `SharedStringRepr`, i.e. from the data pointer,
  // distinguishes the cases by the alignment of the pointer. This is why
  // `encoded_` is aligned to `sizeof(size_t)`.
  //
  // Encoded size if `kAlignment == 1`:
  //  * Small  string: 1-byte   `(size << 1) | 0`
  //  * Medium string: 2-byte   `(size << 2) | 1`
  //  * Large  string: `size_t` `(size << 2) | 3`
  //
  // Encoded size if `kAlignment > 1 && kAlignment < sizeof(size_t)`:
  //  * Small or medium string: 2-byte   `(size << 1) | 0`
  //  * Large           string: `size_t` `(size << 1) | 1`
  //
  // Encoded size if `kAlignment >= sizeof(size_t)`: `size_t size`
  alignas(sizeof(size_t)) char encoded_[sizeof(size_t)];
};

// `SharedStringRepr` is like `IntrusiveSharedPtr<const InternedStringRepr>`
// but stores the interior pointer of `data()`. This makes `data()` faster.
template <typename Encoder, typename Interner>
class ABSL_ATTRIBUTE_TRIVIAL_ABI ABSL_NULLABILITY_COMPATIBLE SharedStringRepr
    : public WithEqual<SharedStringRepr<Encoder, Interner>, std::nullptr_t> {
 private:
  using InternedStringRepr = InternedStringRepr<Encoder, Interner>;

 public:
  SharedStringRepr() = default;
  /*implicit*/ SharedStringRepr(std::nullptr_t) {}
  SharedStringRepr& operator=(std::nullptr_t) {
    Unref(std::exchange(data_, nullptr));
    return *this;
  }

  explicit SharedStringRepr(const InternedStringRepr* repr,
                            PassOwnership = kPassOwnership)
      : data_(repr->data()) {}
  explicit SharedStringRepr(const InternedStringRepr* repr, ShareOwnership)
      : data_(Ref(repr->data())) {}

  SharedStringRepr(const SharedStringRepr& that) : data_(Ref(that.data_)) {}
  SharedStringRepr& operator=(const SharedStringRepr& that) {
    Unref(std::exchange(data_, Ref(that.data_)));
    return *this;
  }

  SharedStringRepr(SharedStringRepr&& that) noexcept
      : data_(std::exchange(that.data_, nullptr)) {}
  SharedStringRepr& operator=(SharedStringRepr&& that) noexcept {
    Unref(std::exchange(data_, std::exchange(that.data_, nullptr)));
    return *this;
  }

  ~SharedStringRepr() { Unref(data_); }

  const char* absl_nullable repr() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return data_;
  }

  const InternedStringRepr& operator*() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT(data_ != nullptr)
        << "Failed precondition of SharedStringRepr::operator*: null pointer";
    return *InternedStringRepr::FromData(data_);
  }
  const InternedStringRepr* operator->() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT(data_ != nullptr)
        << "Failed precondition of SharedStringRepr::operator->: null pointer";
    return InternedStringRepr::FromData(data_);
  }

  bool empty() const {
    RIEGELI_ASSERT(data_ != nullptr)
        << "Failed precondition of SharedStringRepr::empty(): null pointer";
    return InternedStringRepr::EmptyFromData(data_);
  }
  const char* data() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT(data_ != nullptr)
        << "Failed precondition of SharedStringRepr::data(): null pointer";
    return AssumeAligned<kAlignment>(data_);
  }
  size_t size() const {
    RIEGELI_ASSERT(data_ != nullptr)
        << "Failed precondition of SharedStringRepr::size(): null pointer";
    return InternedStringRepr::SizeFromData(data_);
  }
  absl::string_view value() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    RIEGELI_ASSERT(data_ != nullptr)
        << "Failed precondition of SharedStringRepr::value(): null pointer";
    return absl::string_view(data(), size());
  }

  // Returns the current reference count.
  //
  // If the `SharedStringRepr` is accessed by multiple threads, this is a
  // snapshot of the count which may change asynchronously, hence usage of
  // `GetRefCount()` should be limited to cases not important for correctness,
  // like producing debugging output.
  size_t GetRefCount() const {
    if (data_ == nullptr) return 0;
    return InternedStringRepr::FromData(data_)->GetCount();
  }

  friend bool operator==(const SharedStringRepr& a, const SharedStringRepr& b) {
    return a.data_ == b.data_;
  }
  friend bool operator==(const SharedStringRepr& a, std::nullptr_t) {
    return a.data_ == nullptr;
  }

  // Supports `ExternalRef`.
  friend ExternalStorage RiegeliToExternalStorage(SharedStringRepr* self) {
    return ExternalStorage(
        const_cast<char*>(std::exchange(self->data_, nullptr)), [](void* ptr) {
          if (ptr != nullptr) {
            InternedStringRepr::FromData(static_cast<const char*>(ptr))
                ->Unref();
          }
        });
  }

  // Supports `MemoryEstimator`.
  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const SharedStringRepr* self,
                                        MemoryEstimator& memory_estimator) {
    if (self->repr() == nullptr) return;
    const InternedStringRepr* const repr =
        InternedStringRepr::FromData(self->repr());
    if (memory_estimator.RegisterNode(repr)) {
      memory_estimator.RegisterDynamicObject(repr);
    }
  }

 private:
  using pointer = InternedStringRepr*;  // For `ABSL_NULLABILITY_COMPATIBLE`.

  static constexpr size_t kAlignment = Interner::kAlignment;

  const char* Ref(const char* absl_nullable data) {
    if (data != nullptr) InternedStringRepr::FromData(data)->Ref();
    return data;
  }
  void Unref(const char* absl_nullable data) {
    if (data != nullptr) InternedStringRepr::FromData(data)->Unref();
  }

  const char* absl_nullable data_ = nullptr;
};

}  // namespace riegeli::interned_internal

#endif  // RIEGELI_INTERNED_INTERNED_STRING_INTERNAL_H_
