// Copyright 2017 Google LLC
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

#ifndef RIEGELI_BASE_CHAIN_DETAILS_H_
#define RIEGELI_BASE_CHAIN_DETAILS_H_

// IWYU pragma: private, include "riegeli/base/chain.h"

#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <cstring>
#include <functional>
#include <iosfwd>
#include <iterator>
#include <memory>
#include <new>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/meta/type_traits.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain_base.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/external_data.h"
#include "riegeli/base/external_ref_base.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/intrusive_shared_ptr.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/memory_estimator.h"
#include "riegeli/base/new_aligned.h"
#include "riegeli/base/ownership.h"
#include "riegeli/base/temporary_storage.h"

namespace riegeli {

// Represents either `const BlockPtr*`, or one of two special values
// (`kBeginShortData` and `kEndShortData`) behaving as if they were pointers in
// a single-element `BlockPtr` array.
class Chain::BlockPtrPtr : public WithCompare<BlockPtrPtr> {
 public:
  explicit constexpr BlockPtrPtr(uintptr_t repr) : repr_(repr) {}
  static BlockPtrPtr from_ptr(const BlockPtr* ptr);

  bool is_special() const;
  const BlockPtr* as_ptr() const;

  BlockPtrPtr operator+(ptrdiff_t n) const;
  BlockPtrPtr operator-(ptrdiff_t n) const;
  friend ptrdiff_t operator-(BlockPtrPtr a, BlockPtrPtr b) {
    return Subtract(a, b);
  }

  friend bool operator==(BlockPtrPtr a, BlockPtrPtr b) {
    return a.repr_ == b.repr_;
  }
  friend StrongOrdering RIEGELI_COMPARE(BlockPtrPtr a, BlockPtrPtr b) {
    RIEGELI_ASSERT_EQ(a.is_special(), b.is_special())
        << "Incompatible BlockPtrPtr values";
    if (a.is_special()) return riegeli::Compare(a.repr_, b.repr_);
    return riegeli::Compare(a.as_ptr(), b.as_ptr());
  }

 private:
  // `operator-` body is defined in a member function to gain access to private
  // `Chain::RawBlock` under gcc.
  static ptrdiff_t Subtract(BlockPtrPtr a, BlockPtrPtr b) {
    RIEGELI_ASSERT_EQ(a.is_special(), b.is_special())
        << "Incompatible BlockPtrPtr values";
    if (a.is_special()) {
      const ptrdiff_t byte_diff =
          static_cast<ptrdiff_t>(a.repr_) - static_cast<ptrdiff_t>(b.repr_);
      // Pointer subtraction with the element size being a power of 2 typically
      // rounds in the same way as right shift (towards -inf), not as division
      // (towards zero), so the right shift allows the compiler to eliminate the
      // `is_special()` check.
      switch (sizeof(RawBlock*)) {
        case 1 << 2:
          return byte_diff >> 2;
        case 1 << 3:
          return byte_diff >> 3;
        default:
          return byte_diff / ptrdiff_t{sizeof(RawBlock*)};
      }
    }
    return a.as_ptr() - b.as_ptr();
  }

  uintptr_t repr_;
};

class Chain::BlockRef {
 public:
  explicit BlockRef(RawBlock* block);
  explicit BlockRef(IntrusiveSharedPtr<RawBlock> block);

  BlockRef(BlockRef&& that) = default;
  BlockRef& operator=(BlockRef&& that) = default;

  // Support `ExternalRef`.
  friend size_t RiegeliAllocatedMemory(const BlockRef* self) {
    return self->AllocatedMemory();
  }

  // Support `ExternalRef`.
  friend IntrusiveSharedPtr<RawBlock> RiegeliToChainRawBlock(
      BlockRef* self, absl::string_view substr) {
    return std::move(*self).ToChainRawBlock(substr);
  }

  // Support `ExternalRef`.
  friend absl::Cord RiegeliToCord(BlockRef* self, absl::string_view substr) {
    return std::move(*self).ToCord(substr);
  }

  // Support `ExternalRef`.
  friend ExternalStorage RiegeliToExternalStorage(BlockRef* self) {
    return std::move(*self).ToExternalStorage();
  }

  // Support `Chain::FromExternal()` and `ExternalRef`.
  friend void RiegeliDumpStructure(const BlockRef* self,
                                   absl::string_view substr,
                                   std::ostream& out) {
    self->DumpStructure(substr, out);
  }

  template <typename MemoryEstimator>
  friend void RiegeliRegisterSubobjects(const BlockRef* self,
                                        MemoryEstimator& memory_estimator) {
    memory_estimator.RegisterSubobjects(&self->block_);
  }

 private:
  size_t AllocatedMemory() const;
  IntrusiveSharedPtr<RawBlock> ToChainRawBlock(absl::string_view substr) &&;
  absl::Cord ToCord(absl::string_view substr) &&;
  ExternalStorage ToExternalStorage() &&;
  void DumpStructure(absl::string_view substr, std::ostream& out) const;

  IntrusiveSharedPtr<RawBlock> block_;
};

class Chain::BlockIterator : public WithCompare<BlockIterator> {
 public:
  using iterator_concept = std::random_access_iterator_tag;
  // `iterator_category` is only `std::input_iterator_tag` because the
  // `LegacyForwardIterator` requirement and above require `reference` to be
  // a true reference type.
  using iterator_category = std::input_iterator_tag;
  using value_type = absl::string_view;
  using reference = value_type;
  using difference_type = ptrdiff_t;

  class pointer {
   public:
    const reference* operator->() const { return &ref_; }

   private:
    friend class BlockIterator;
    explicit pointer(reference ref) : ref_(ref) {}
    reference ref_;
  };

  using BlockRefMaker = MakerTypeFor<BlockRef, RawBlock*>;

  BlockIterator() = default;

  explicit BlockIterator(const Chain* chain, size_t block_index);

  BlockIterator(const BlockIterator& that) = default;
  BlockIterator& operator=(const BlockIterator& that) = default;

  const Chain* chain() const { return chain_; }
  size_t block_index() const;

  // Returns the char index relative to the beginning of the chain, given the
  // corresponding char index relative to the beginning of the block.
  //
  // The opposite conversion is `Chain::BlockAndCharIndex()`.
  size_t CharIndexInChain(size_t char_index_in_block = 0) const;

  reference operator*() const;
  pointer operator->() const;
  BlockIterator& operator++();
  BlockIterator operator++(int);
  BlockIterator& operator--();
  BlockIterator operator--(int);
  BlockIterator& operator+=(difference_type n);
  BlockIterator operator+(difference_type n) const;
  BlockIterator& operator-=(difference_type n);
  BlockIterator operator-(difference_type n) const;
  reference operator[](difference_type n) const;

  friend bool operator==(BlockIterator a, BlockIterator b) {
    RIEGELI_ASSERT(a.chain_ == b.chain_)
        << "Failed precondition of operator==(Chain::BlockIterator): "
           "incomparable iterators";
    return a.ptr_ == b.ptr_;
  }
  friend StrongOrdering RIEGELI_COMPARE(BlockIterator a, BlockIterator b) {
    RIEGELI_ASSERT(a.chain_ == b.chain_)
        << "Failed precondition of operator<=>(Chain::BlockIterator): "
           "incomparable iterators";
    return riegeli::Compare(a.ptr_, b.ptr_);
  }
  friend difference_type operator-(BlockIterator a, BlockIterator b) {
    RIEGELI_ASSERT(a.chain_ == b.chain_)
        << "Failed precondition of operator-(Chain::BlockIterator): "
           "incomparable iterators";
    return a.ptr_ - b.ptr_;
  }
  friend BlockIterator operator+(difference_type n, BlockIterator a) {
    return a + n;
  }

  // Converts the block pointed to `*this` to `ExternalRef`.
  //
  // `temporary_storage` and `external_storage` must outlive usages of the
  // returned `ExternalRef`.
  //
  // Precondition: `*this` is not past the end iterator
  ExternalRef ToExternalRef(
      TemporaryStorage<BlockRefMaker>&& temporary_storage
          ABSL_ATTRIBUTE_LIFETIME_BOUND = TemporaryStorage<BlockRefMaker>(),
      ExternalRef::StorageSubstr<BlockRefMaker&&>&& external_storage
          ABSL_ATTRIBUTE_LIFETIME_BOUND =
              ExternalRef::StorageSubstr<BlockRefMaker&&>()) const;

  // Converts a substring of the block pointed to `*this` to `ExternalRef`.
  //
  // `temporary_storage` and `external_storage` must outlive usages of the
  // returned `ExternalRef`.
  //
  // Preconditions:
  //   `*this` is not past the end iterator
  //   if `!substr.empty()` then `substr` is a substring of `**this`
  ExternalRef ToExternalRef(
      absl::string_view substr,
      TemporaryStorage<BlockRefMaker>&& temporary_storage
          ABSL_ATTRIBUTE_LIFETIME_BOUND = TemporaryStorage<BlockRefMaker>(),
      ExternalRef::StorageSubstr<BlockRefMaker&&>&& external_storage
          ABSL_ATTRIBUTE_LIFETIME_BOUND =
              ExternalRef::StorageSubstr<BlockRefMaker&&>()) const;

  // Returns a pointer to the external object if this points to an external
  // block holding an object of type `T`, otherwise returns `nullptr`.
  //
  // Precondition: `*this` is not past the end iterator
  template <typename T>
  const T* external_object() const;

 private:
  friend class Chain;

  static constexpr BlockPtrPtr kBeginShortData{0};
  static constexpr BlockPtrPtr kEndShortData{sizeof(BlockPtr)};

  explicit BlockIterator(const Chain* chain, BlockPtrPtr ptr) noexcept;

  size_t CharIndexInChainInternal() const;

  const Chain* chain_ = nullptr;
  // If `chain_ == nullptr`, `kBeginShortData`.
  // If `*chain_` has no block pointers and no short data, `kEndShortData`.
  // If `*chain_` has short data, `kBeginShortData` or `kEndShortData`.
  // If `*chain_` has block pointers, a pointer to the block pointer array.
  BlockPtrPtr ptr_ = kBeginShortData;
};

class Chain::Blocks {
 public:
  using value_type = absl::string_view;
  using reference = value_type;
  using const_reference = reference;
  using iterator = BlockIterator;
  using const_iterator = iterator;
  using reverse_iterator = std::reverse_iterator<iterator>;
  using const_reverse_iterator = reverse_iterator;
  using size_type = size_t;
  using difference_type = ptrdiff_t;

  Blocks() = default;

  Blocks(const Blocks& that) = default;
  Blocks& operator=(const Blocks& that) = default;

  iterator begin() const;
  iterator cbegin() const { return begin(); }
  iterator end() const;
  iterator cend() const { return end(); }

  reverse_iterator rbegin() const { return reverse_iterator(end()); }
  reverse_iterator crbegin() const { return rbegin(); }
  reverse_iterator rend() const { return reverse_iterator(begin()); }
  reverse_iterator crend() const { return rend(); }

  bool empty() const;
  size_type size() const;

  reference operator[](size_type n) const;
  reference at(size_type n) const;
  reference front() const;
  reference back() const;

 private:
  friend class Chain;

  explicit Blocks(const Chain* chain) noexcept : chain_(chain) {}

  const Chain* chain_ = nullptr;
};

// Represents the position of a character in a `Chain`.
//
// A `CharIterator` is not provided because it is more efficient to iterate by
// blocks and process character ranges within a block.
struct Chain::BlockAndChar {
  // Intended invariant:
  //   if `block_iter == block_iter.chain()->blocks().cend()`
  //       then `char_index == 0`
  //       else `char_index < block_iter->size()`
  BlockIterator block_iter;
  size_t char_index;
};

// Returns the given number of zero bytes.
Chain ChainOfZeros(size_t length);

// Implementation details follow.

struct Chain::ExternalMethods {
  void (*delete_block)(RawBlock* block);
  void (*dump_structure)(const RawBlock& block, std::ostream& out);
  size_t dynamic_sizeof;
  void (*register_subobjects)(const RawBlock* block,
                              MemoryEstimator& memory_estimator);
};

namespace chain_internal {

template <typename T, typename Enable = void>
struct HasCallOperatorSubstr : std::false_type {};

template <typename T>
struct HasCallOperatorSubstr<T, absl::void_t<decltype(std::declval<T&&>()(
                                    std::declval<absl::string_view>()))>>
    : std::true_type {};

template <typename T, typename Enable = void>
struct HasCallOperatorWhole : std::false_type {};

template <typename T>
struct HasCallOperatorWhole<T, absl::void_t<decltype(std::declval<T&&>()())>>
    : std::true_type {};

template <typename T>
struct HasCallOperator
    : absl::disjunction<HasCallOperatorSubstr<T>, HasCallOperatorWhole<T>> {};

template <typename T,
          std::enable_if_t<HasCallOperatorSubstr<T>::value, int> = 0>
inline void CallOperator(T&& object, absl::string_view substr) {
  std::forward<T>(object)(substr);
}

template <
    typename T,
    std::enable_if_t<absl::conjunction<absl::negation<HasCallOperatorSubstr<T>>,
                                       HasCallOperatorWhole<T>>::value,
                     int> = 0>
inline void CallOperator(T&& object,
                         ABSL_ATTRIBUTE_UNUSED absl::string_view substr) {
  std::forward<T>(object)();
}

template <typename T,
          std::enable_if_t<
              absl::conjunction<absl::negation<HasCallOperatorSubstr<T>>,
                                absl::negation<HasCallOperatorWhole<T>>>::value,
              int> = 0>
inline void CallOperator(ABSL_ATTRIBUTE_UNUSED T&& object,
                         ABSL_ATTRIBUTE_UNUSED absl::string_view substr) {}

template <typename T,
          std::enable_if_t<MemoryEstimator::RegisterSubobjectsIsGood<T>::value,
                           int> = 0>
inline void RegisterSubobjects(const T* object,
                               ABSL_ATTRIBUTE_UNUSED absl::string_view substr,
                               MemoryEstimator& memory_estimator) {
  memory_estimator.RegisterSubobjects(object);
}

template <typename T,
          std::enable_if_t<!MemoryEstimator::RegisterSubobjectsIsGood<T>::value,
                           int> = 0>
inline void RegisterSubobjects(ABSL_ATTRIBUTE_UNUSED const T* object,
                               absl::string_view substr,
                               MemoryEstimator& memory_estimator) {
  memory_estimator.RegisterUnknownType<T>();
  // As an approximation of memory usage of an unknown type, register just the
  // stored data if unique.
  if (memory_estimator.RegisterNode(substr.data())) {
    memory_estimator.RegisterDynamicMemory(substr.size());
  }
}

template <typename T, typename Enable = void>
struct HasRiegeliDumpStructureWithSubstr : std::false_type {};

template <typename T>
struct HasRiegeliDumpStructureWithSubstr<
    T, absl::void_t<decltype(RiegeliDumpStructure(
           std::declval<const T*>(), std::declval<absl::string_view>(),
           std::declval<std::ostream&>()))>> : std::true_type {};

template <typename T, typename Enable = void>
struct HasRiegeliDumpStructureWithoutData : std::false_type {};

template <typename T>
struct HasRiegeliDumpStructureWithoutData<
    T, absl::void_t<decltype(RiegeliDumpStructure(
           std::declval<const T*>(), std::declval<std::ostream&>()))>>
    : std::true_type {};

void DumpStructureDefault(std::ostream& out);

template <typename T, std::enable_if_t<
                          HasRiegeliDumpStructureWithSubstr<T>::value, int> = 0>
inline void DumpStructure(const T* object, absl::string_view substr,
                          std::ostream& out) {
  RiegeliDumpStructure(object, substr, out);
}

template <
    typename T,
    std::enable_if_t<
        absl::conjunction<absl::negation<HasRiegeliDumpStructureWithSubstr<T>>,
                          HasRiegeliDumpStructureWithoutData<T>>::value,
        int> = 0>
inline void DumpStructure(const T* object,
                          ABSL_ATTRIBUTE_UNUSED absl::string_view substr,
                          std::ostream& out) {
  RiegeliDumpStructure(object, out);
}

template <typename T,
          std::enable_if_t<
              absl::conjunction<
                  absl::negation<HasRiegeliDumpStructureWithSubstr<T>>,
                  absl::negation<HasRiegeliDumpStructureWithoutData<T>>>::value,
              int> = 0>
inline void DumpStructure(ABSL_ATTRIBUTE_UNUSED const T* object,
                          ABSL_ATTRIBUTE_UNUSED absl::string_view substr,
                          std::ostream& out) {
  chain_internal::DumpStructureDefault(out);
}

}  // namespace chain_internal

// Support `Chain::FromExternal()` and `ExternalRef`.
void RiegeliDumpStructure(const std::string* self, std::ostream& out);

template <typename T>
struct Chain::ExternalMethodsFor {
  // Creates an external block containing an external object constructed from
  // `object`, and sets block data to `absl::string_view(new_object)`.
  static IntrusiveSharedPtr<RawBlock> NewBlock(Initializer<T> object);

  // Creates an external block containing an external object constructed from
  // `object`, and sets block data to `data`.
  static IntrusiveSharedPtr<RawBlock> NewBlock(Initializer<T> object,
                                               absl::string_view substr);

 private:
  static void DeleteBlock(RawBlock* block);
  static void DumpStructure(const RawBlock& block, std::ostream& out);
  static constexpr size_t DynamicSizeOf();
  static void RegisterSubobjects(const RawBlock* block,
                                 MemoryEstimator& memory_estimator);

 public:
  static constexpr ExternalMethods kMethods = {
      DeleteBlock, DumpStructure, DynamicSizeOf(), RegisterSubobjects};
};

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if !__cpp_inline_variables
template <typename T>
constexpr Chain::ExternalMethods Chain::ExternalMethodsFor<T>::kMethods;
#endif

template <typename T>
inline IntrusiveSharedPtr<Chain::RawBlock>
Chain::ExternalMethodsFor<T>::NewBlock(Initializer<T> object) {
  return IntrusiveSharedPtr<RawBlock>(
      NewAligned<RawBlock, UnsignedMax(alignof(RawBlock), alignof(T))>(
          RawBlock::kExternalAllocatedSize<T>(), std::move(object)));
}

template <typename T>
inline IntrusiveSharedPtr<Chain::RawBlock>
Chain::ExternalMethodsFor<T>::NewBlock(Initializer<T> object,
                                       absl::string_view substr) {
  return IntrusiveSharedPtr<RawBlock>(
      NewAligned<RawBlock, UnsignedMax(alignof(RawBlock), alignof(T))>(
          RawBlock::kExternalAllocatedSize<T>(), std::move(object), substr));
}

template <typename T>
void Chain::ExternalMethodsFor<T>::DeleteBlock(RawBlock* block) {
  chain_internal::CallOperator(std::move(block->unchecked_external_object<T>()),
                               absl::string_view(*block));
  block->unchecked_external_object<T>().~T();
  DeleteAligned<RawBlock, UnsignedMax(alignof(RawBlock), alignof(T))>(
      block, RawBlock::kExternalAllocatedSize<T>());
}

template <typename T>
void Chain::ExternalMethodsFor<T>::DumpStructure(const RawBlock& block,
                                                 std::ostream& out) {
  chain_internal::DumpStructure(&block.unchecked_external_object<T>(),
                                absl::string_view(block), out);
}

template <typename T>
constexpr size_t Chain::ExternalMethodsFor<T>::DynamicSizeOf() {
  return RawBlock::kExternalAllocatedSize<T>();
}

template <typename T>
void Chain::ExternalMethodsFor<T>::RegisterSubobjects(
    const RawBlock* block, MemoryEstimator& memory_estimator) {
  chain_internal::RegisterSubobjects(&block->unchecked_external_object<T>(),
                                     absl::string_view(*block),
                                     memory_estimator);
}

template <typename T>
inline Chain::RawBlock::RawBlock(Initializer<T> object) {
  external_.methods = &ExternalMethodsFor<T>::kMethods;
  new (&unchecked_external_object<T>()) T(std::move(object).Construct());
  const absl::string_view data(unchecked_external_object<T>());
  data_ = data.data();
  size_ = data.size();
  RIEGELI_ASSERT(is_external()) << "A RawBlock with allocated_end_ == nullptr "
                                   "should be considered external";
}

template <typename T>
inline Chain::RawBlock::RawBlock(Initializer<T> object,
                                 absl::string_view substr)
    : data_(substr.data()), size_(substr.size()) {
  external_.methods = &ExternalMethodsFor<T>::kMethods;
  new (&unchecked_external_object<T>()) T(std::move(object).Construct());
  RIEGELI_ASSERT(is_external()) << "A RawBlock with allocated_end_ == nullptr "
                                   "should be considered external";
}

constexpr size_t Chain::RawBlock::kInternalAllocatedOffset() {
  return offsetof(RawBlock, allocated_begin_);
}

template <typename T>
constexpr size_t Chain::RawBlock::kExternalObjectOffset() {
  return RoundUp<alignof(T)>(offsetof(RawBlock, external_) +
                             offsetof(External, object_lower_bound));
}

template <typename T>
constexpr size_t Chain::RawBlock::kExternalAllocatedSize() {
  return kExternalObjectOffset<T>() + sizeof(T);
}

template <typename Ownership>
inline Chain::RawBlock* Chain::RawBlock::Ref() {
  ref_count_.Ref<Ownership>();
  return this;
}

template <typename Ownership>
inline void Chain::RawBlock::Unref() {
  if (ref_count_.Unref<Ownership>()) {
    if (is_internal()) {
      DeleteAligned<RawBlock>(this, kInternalAllocatedOffset() + capacity());
    } else {
      external_.methods->delete_block(this);
    }
  }
}

inline bool Chain::RawBlock::has_unique_owner() const {
  return ref_count_.HasUniqueOwner();
}

inline size_t Chain::RawBlock::AllocatedMemory() const {
  if (is_internal()) {
    return kInternalAllocatedOffset() + capacity();
  } else {
    return external_.methods->dynamic_sizeof + size();
  }
}

inline size_t Chain::RawBlock::capacity() const {
  RIEGELI_ASSERT(is_internal())
      << "Failed precondition of Chain::RawBlock::capacity(): "
         "block not internal";
  return PtrDistance(allocated_begin_, allocated_end_);
}

template <typename T>
inline T& Chain::RawBlock::unchecked_external_object() {
  RIEGELI_ASSERT(is_external())
      << "Failed precondition of Chain::RawBlock::unchecked_external_object(): "
      << "block not external";
  return *
#if __cpp_lib_launder >= 201606
      std::launder
#endif
      (reinterpret_cast<T*>(reinterpret_cast<char*>(this) +
                            kExternalObjectOffset<T>()));
}

template <typename T>
inline const T& Chain::RawBlock::unchecked_external_object() const {
  RIEGELI_ASSERT(is_external())
      << "Failed precondition of Chain::RawBlock::unchecked_external_object(): "
      << "block not external";
  return *
#if __cpp_lib_launder >= 201606
      std::launder
#endif
      (reinterpret_cast<const T*>(reinterpret_cast<const char*>(this) +
                                  kExternalObjectOffset<T>()));
}

template <typename T>
inline const T* Chain::RawBlock::checked_external_object() const {
  return is_external() && external_.methods == &ExternalMethodsFor<T>::kMethods
             ? &unchecked_external_object<T>()
             : nullptr;
}

template <typename T>
inline T* Chain::RawBlock::checked_external_object_with_unique_owner() {
  return is_external() &&
                 external_.methods == &ExternalMethodsFor<T>::kMethods &&
                 has_unique_owner()
             ? &unchecked_external_object<T>()
             : nullptr;
}

inline bool Chain::RawBlock::TryClear() {
  if (is_mutable()) {
    size_ = 0;
    return true;
  }
  return false;
}

inline bool Chain::RawBlock::TryRemoveSuffix(size_t length) {
  RIEGELI_ASSERT_LE(length, size())
      << "Failed precondition of Chain::RawBlock::TryRemoveSuffix(): "
      << "length to remove greater than current size";
  if (is_mutable()) {
    size_ -= length;
    return true;
  }
  return false;
}

inline bool Chain::RawBlock::TryRemovePrefix(size_t length) {
  RIEGELI_ASSERT_LE(length, size())
      << "Failed precondition of Chain::RawBlock::TryRemovePrefix(): "
      << "length to remove greater than current size";
  if (is_mutable()) {
    data_ += length;
    size_ -= length;
    return true;
  }
  return false;
}

inline Chain::BlockPtrPtr Chain::BlockPtrPtr::from_ptr(const BlockPtr* ptr) {
  return BlockPtrPtr(reinterpret_cast<uintptr_t>(ptr));
}

inline bool Chain::BlockPtrPtr::is_special() const {
  return repr_ <= sizeof(BlockPtr);
}

inline const Chain::BlockPtr* Chain::BlockPtrPtr::as_ptr() const {
  RIEGELI_ASSERT(!is_special()) << "Unexpected special BlockPtrPtr value";
  return reinterpret_cast<const BlockPtr*>(repr_);
}

// Code conditional on `is_special()` is written such that both branches
// typically compile to the same code, allowing the compiler eliminate the
// `is_special()` checks.

inline Chain::BlockPtrPtr Chain::BlockPtrPtr::operator+(ptrdiff_t n) const {
  if (is_special()) {
    return BlockPtrPtr(IntCast<uintptr_t>(IntCast<ptrdiff_t>(repr_) +
                                          n * ptrdiff_t{sizeof(RawBlock*)}));
  }
  return BlockPtrPtr::from_ptr(as_ptr() + n);
}

inline Chain::BlockPtrPtr Chain::BlockPtrPtr::operator-(ptrdiff_t n) const {
  if (is_special()) {
    return BlockPtrPtr(IntCast<uintptr_t>(IntCast<ptrdiff_t>(repr_) -
                                          n * ptrdiff_t{sizeof(RawBlock*)}));
  }
  return BlockPtrPtr::from_ptr(as_ptr() - n);
}

inline Chain::BlockIterator::BlockIterator(const Chain* chain,
                                           size_t block_index)
    : chain_(chain),
      ptr_((ABSL_PREDICT_FALSE(chain_ == nullptr) ? kBeginShortData
            : chain_->begin_ == chain_->end_
                ? (chain_->empty() ? kEndShortData : kBeginShortData)
                : BlockPtrPtr::from_ptr(chain_->begin_)) +
           IntCast<ptrdiff_t>(block_index)) {}

inline Chain::BlockIterator::BlockIterator(const Chain* chain,
                                           BlockPtrPtr ptr) noexcept
    : chain_(chain), ptr_(ptr) {}

inline size_t Chain::BlockIterator::block_index() const {
  if (ptr_ == kBeginShortData) {
    return 0;
  } else if (ptr_ == kEndShortData) {
    return chain_->empty() ? 0 : 1;
  } else {
    return PtrDistance(chain_->begin_, ptr_.as_ptr());
  }
}

inline size_t Chain::BlockIterator::CharIndexInChain(
    size_t char_index_in_block) const {
  return CharIndexInChainInternal() + char_index_in_block;
}

inline Chain::BlockIterator::reference Chain::BlockIterator::operator*() const {
  RIEGELI_ASSERT(ptr_ != kEndShortData)
      << "Failed precondition of Chain::BlockIterator::operator*: "
         "iterator is end()";
  if (ptr_ == kBeginShortData) {
    return chain_->short_data();
  } else {
    return absl::string_view(*ptr_.as_ptr()->block_ptr);
  }
}

inline Chain::BlockIterator::pointer Chain::BlockIterator::operator->() const {
  return pointer(**this);
}

inline Chain::BlockIterator& Chain::BlockIterator::operator++() {
  ptr_ = ptr_ + 1;
  return *this;
}

inline Chain::BlockIterator Chain::BlockIterator::operator++(int) {
  const BlockIterator tmp = *this;
  ++*this;
  return tmp;
}

inline Chain::BlockIterator& Chain::BlockIterator::operator--() {
  ptr_ = ptr_ - 1;
  return *this;
}

inline Chain::BlockIterator Chain::BlockIterator::operator--(int) {
  const BlockIterator tmp = *this;
  --*this;
  return tmp;
}

inline Chain::BlockIterator& Chain::BlockIterator::operator+=(
    difference_type n) {
  ptr_ = ptr_ + n;
  return *this;
}

inline Chain::BlockIterator Chain::BlockIterator::operator+(
    difference_type n) const {
  return BlockIterator(*this) += n;
}

inline Chain::BlockIterator& Chain::BlockIterator::operator-=(
    difference_type n) {
  ptr_ = ptr_ - n;
  return *this;
}

inline Chain::BlockIterator Chain::BlockIterator::operator-(
    difference_type n) const {
  return BlockIterator(*this) -= n;
}

inline Chain::BlockIterator::reference Chain::BlockIterator::operator[](
    difference_type n) const {
  return *(*this + n);
}

inline ExternalRef Chain::BlockIterator::ToExternalRef(
    TemporaryStorage<BlockRefMaker>&& temporary_storage,
    ExternalRef::StorageSubstr<BlockRefMaker&&>&& external_storage) const {
  RIEGELI_ASSERT(ptr_ != kEndShortData)
      << "Failed precondition of Chain::BlockIterator::ToExternalRef(): "
         "iterator is end()";
  if (ptr_ == kBeginShortData) {
    return ExternalRef(chain_->short_data(), std::move(external_storage));
  } else {
    return ExternalRef(
        std::move(temporary_storage).emplace(ptr_.as_ptr()->block_ptr),
        absl::string_view(*ptr_.as_ptr()->block_ptr),
        std::move(external_storage));
  }
}

inline ExternalRef Chain::BlockIterator::ToExternalRef(
    absl::string_view substr,
    TemporaryStorage<BlockRefMaker>&& temporary_storage,
    ExternalRef::StorageSubstr<BlockRefMaker&&>&& external_storage) const {
  RIEGELI_ASSERT(ptr_ != kEndShortData)
      << "Failed precondition of Chain::BlockIterator::ToExternalRef(): "
         "iterator is end()";
  if (!substr.empty()) {
    RIEGELI_ASSERT(std::greater_equal<>()(substr.data(), (*this)->data()))
        << "Failed precondition of SizedSharedBuffer::ToExternalRef(): "
           "substring not contained in the buffer";
    RIEGELI_ASSERT(std::less_equal<>()(substr.data() + substr.size(),
                                       (*this)->data() + (*this)->size()))
        << "Failed precondition of SizedSharedBuffer::ToExternalRef(): "
           "substring not contained in the buffer";
  }
  if (ptr_ == kBeginShortData) {
    return ExternalRef(substr, std::move(external_storage));
  } else {
    return ExternalRef(
        std::move(temporary_storage).emplace(ptr_.as_ptr()->block_ptr), substr,
        std::move(external_storage));
  }
}

template <typename T>
inline const T* Chain::BlockIterator::external_object() const {
  RIEGELI_ASSERT(ptr_ != kEndShortData)
      << "Failed precondition of Chain::BlockIterator::external_object(): "
         "iterator is end()";
  if (ptr_ == kBeginShortData) {
    return nullptr;
  } else {
    return ptr_.as_ptr()->block_ptr->checked_external_object<T>();
  }
}

inline Chain::BlockRef::BlockRef(RawBlock* block) {
  if (const BlockRef* const block_ref =
          block->checked_external_object<BlockRef>()) {
    // `block` is already a `BlockRef`. Refer to its target instead.
    block = block_ref->block_.get();
  }
  block_.Reset(block, kShareOwnership);
}

inline Chain::BlockRef::BlockRef(IntrusiveSharedPtr<RawBlock> block) {
  if (const BlockRef* const block_ref =
          block->checked_external_object<BlockRef>()) {
    // `block` is already a `BlockRef`. Refer to its target instead.
    block = block_ref->block_;
  }
  block_ = std::move(block);
}

inline size_t Chain::BlockRef::AllocatedMemory() const {
  return block_->AllocatedMemory();
}

inline ExternalStorage Chain::BlockRef::ToExternalStorage() && {
  return ExternalStorage(block_.Release(), [](void* ptr) {
    static_cast<RawBlock*>(ptr)->Unref();
  });
}

inline Chain::Blocks::iterator Chain::Blocks::begin() const {
  return BlockIterator(chain_,
                       chain_->begin_ == chain_->end_
                           ? (chain_->empty() ? BlockIterator::kEndShortData
                                              : BlockIterator::kBeginShortData)
                           : BlockPtrPtr::from_ptr(chain_->begin_));
}

inline Chain::Blocks::iterator Chain::Blocks::end() const {
  return BlockIterator(chain_, chain_->begin_ == chain_->end_
                                   ? BlockIterator::kEndShortData
                                   : BlockPtrPtr::from_ptr(chain_->end_));
}

inline Chain::Blocks::size_type Chain::Blocks::size() const {
  if (chain_->begin_ == chain_->end_) {
    return chain_->empty() ? 0 : 1;
  } else {
    return PtrDistance(chain_->begin_, chain_->end_);
  }
}

inline bool Chain::Blocks::empty() const {
  return chain_->begin_ == chain_->end_ && chain_->empty();
}

inline Chain::Blocks::reference Chain::Blocks::operator[](size_type n) const {
  RIEGELI_ASSERT_LT(n, size())
      << "Failed precondition of Chain::Blocks::operator[]: "
         "block index out of range";
  if (ABSL_PREDICT_FALSE(chain_->begin_ == chain_->end_)) {
    return chain_->short_data();
  } else {
    return absl::string_view(*chain_->begin_[n].block_ptr);
  }
}

inline Chain::Blocks::reference Chain::Blocks::at(size_type n) const {
  RIEGELI_CHECK_LT(n, size()) << "Failed precondition of Chain::Blocks::at(): "
                                 "block index out of range";
  if (ABSL_PREDICT_FALSE(chain_->begin_ == chain_->end_)) {
    return chain_->short_data();
  } else {
    return absl::string_view(*chain_->begin_[n].block_ptr);
  }
}

inline Chain::Blocks::reference Chain::Blocks::front() const {
  RIEGELI_ASSERT(!empty())
      << "Failed precondition of Chain::Blocks::front(): no blocks";
  if (ABSL_PREDICT_FALSE(chain_->begin_ == chain_->end_)) {
    return chain_->short_data();
  } else {
    return absl::string_view(*chain_->begin_[0].block_ptr);
  }
}

inline Chain::Blocks::reference Chain::Blocks::back() const {
  RIEGELI_ASSERT(!empty())
      << "Failed precondition of Chain::Blocks::back(): no blocks";
  if (ABSL_PREDICT_FALSE(chain_->begin_ == chain_->end_)) {
    return chain_->short_data();
  } else {
    return absl::string_view(*chain_->end_[-1].block_ptr);
  }
}

template <typename T,
          std::enable_if_t<std::is_constructible<absl::string_view,
                                                 InitializerTargetT<T>&>::value,
                           int>>
inline Chain Chain::FromExternal(T&& object) {
  return Chain(ExternalMethodsFor<InitializerTargetT<T>>::NewBlock(
      std::forward<T>(object)));
}

template <typename T>
inline Chain Chain::FromExternal(T&& object, absl::string_view substr) {
  return Chain(ExternalMethodsFor<InitializerTargetT<T>>::NewBlock(
      std::forward<T>(object), substr));
}

template <typename T>
constexpr size_t Chain::kExternalAllocatedSize() {
  return RawBlock::kExternalAllocatedSize<T>();
}

inline Chain::Chain(IntrusiveSharedPtr<RawBlock> block) {
  Initialize(std::move(block));
}

inline Chain::Chain(absl::string_view src) { Initialize(src); }

template <typename Src,
          std::enable_if_t<std::is_same<Src, std::string>::value, int>>
inline Chain::Chain(Src&& src) {
  // `std::move(src)` is correct and `std::forward<Src>(src)` is not necessary:
  // `Src` is always `std::string`, never an lvalue reference.
  Initialize(std::move(src));
}

inline Chain::Chain(Chain&& that) noexcept
    : size_(std::exchange(that.size_, 0)) {
  // Use `std::memcpy()` instead of copy constructor to silence
  // `-Wmaybe-uninitialized` in gcc.
  std::memcpy(&block_ptrs_, &that.block_ptrs_, sizeof(BlockPtrs));
  if (that.has_here()) {
    // `that.has_here()` implies that `that.begin_ == that.block_ptrs_.here`
    // already.
    begin_ = block_ptrs_.here;
    end_ = block_ptrs_.here + (std::exchange(that.end_, that.block_ptrs_.here) -
                               that.block_ptrs_.here);
  } else {
    begin_ = std::exchange(that.begin_, that.block_ptrs_.here);
    end_ = std::exchange(that.end_, that.block_ptrs_.here);
  }
  // It does not matter what is left in `that.block_ptrs_` because `that.begin_`
  // and `that.end_` point to the empty prefix of `that.block_ptrs_.here[]`.
}

inline Chain& Chain::operator=(Chain&& that) noexcept {
  // Exchange `that.begin_` and `that.end_` early to support self-assignment.
  BlockPtr* begin;
  BlockPtr* end;
  if (that.has_here()) {
    // `that.has_here()` implies that `that.begin_ == that.block_ptrs_.here`
    // already.
    begin = block_ptrs_.here;
    end = block_ptrs_.here + (std::exchange(that.end_, that.block_ptrs_.here) -
                              that.block_ptrs_.here);
  } else {
    begin = std::exchange(that.begin_, that.block_ptrs_.here);
    end = std::exchange(that.end_, that.block_ptrs_.here);
  }
  UnrefBlocks();
  DeleteBlockPtrs();
  // It does not matter what is left in `that.block_ptrs_` because `that.begin_`
  // and `that.end_` point to the empty prefix of `that.block_ptrs_.here[]`. Use
  // `std::memcpy()` instead of assignment to silence `-Wmaybe-uninitialized` in
  // gcc.
  std::memcpy(&block_ptrs_, &that.block_ptrs_, sizeof(BlockPtrs));
  begin_ = begin;
  end_ = end;
  size_ = std::exchange(that.size_, 0);
  return *this;
}

inline Chain::~Chain() {
  UnrefBlocks();
  DeleteBlockPtrs();
}

inline void Chain::Reset() { Clear(); }

extern template void Chain::Reset(std::string&& src);

inline void Chain::Clear() {
  size_ = 0;
  if (begin_ != end_) ClearSlow();
}

inline void Chain::Initialize(IntrusiveSharedPtr<RawBlock> block) {
  size_ = block->size();
  (end_++)->block_ptr = block.Release();
}

inline void Chain::Initialize(absl::string_view src) {
  if (src.size() <= kMaxShortDataSize) {
    if (src.empty()) return;
    size_ = src.size();
    std::memcpy(block_ptrs_.short_data, src.data(), src.size());
    return;
  }
  InitializeSlow(src);
}

template <typename Src,
          std::enable_if_t<std::is_same<Src, std::string>::value, int>>
inline void Chain::Initialize(Src&& src) {
  if (src.size() <= kMaxShortDataSize) {
    if (src.empty()) return;
    size_ = src.size();
    std::memcpy(block_ptrs_.short_data, src.data(), src.size());
    return;
  }
  // `std::move(src)` is correct and `std::forward<Src>(src)` is not necessary:
  // `Src` is always `std::string`, never an lvalue reference.
  InitializeSlow(std::move(src));
}

extern template void Chain::InitializeSlow(std::string&& src);

inline absl::string_view Chain::short_data() const {
  RIEGELI_ASSERT(begin_ == end_)
      << "Failed precondition of Chain::short_data(): blocks exist";
  return absl::string_view(block_ptrs_.short_data, size_);
}

inline void Chain::DeleteBlockPtrs() {
  if (has_allocated()) {
    std::allocator<BlockPtr>().deallocate(
        block_ptrs_.allocated.begin,
        2 * PtrDistance(block_ptrs_.allocated.begin,
                        block_ptrs_.allocated.end));
  }
}

inline void Chain::UnrefBlocks() { UnrefBlocks(begin_, end_); }

inline void Chain::UnrefBlocks(const BlockPtr* begin, const BlockPtr* end) {
  if (begin != end) UnrefBlocksSlow(begin, end);
}

inline Chain::Blocks Chain::blocks() const { return Blocks(this); }

inline absl::optional<absl::string_view> Chain::TryFlat() const {
  switch (end_ - begin_) {
    case 0:
      return short_data();
    case 1:
      return absl::string_view(*front());
    default:
      return absl::nullopt;
  }
}

inline absl::string_view Chain::Flatten() {
  switch (end_ - begin_) {
    case 0:
      return short_data();
    case 1:
      return absl::string_view(*front());
    default:
      return FlattenSlow();
  }
}

inline absl::Span<char> Chain::AppendFixedBuffer(size_t length,
                                                 Options options) {
  return AppendBuffer(length, length, length, options);
}

inline absl::Span<char> Chain::PrependFixedBuffer(size_t length,
                                                  Options options) {
  return PrependBuffer(length, length, length, options);
}

extern template void Chain::Append(std::string&& src, Options options);
extern template void Chain::Prepend(std::string&& src, Options options);

template <typename HashState>
HashState Chain::HashValue(HashState hash_state) const {
  {
    const absl::optional<absl::string_view> flat = TryFlat();
    if (flat != absl::nullopt) {
      return HashState::combine(std::move(hash_state), *flat);
    }
  }
  typename HashState::AbslInternalPiecewiseCombiner combiner;
  for (const absl::string_view block : blocks()) {
    hash_state =
        combiner.add_buffer(std::move(hash_state), block.data(), block.size());
  }
  return HashState::combine(combiner.finalize(std::move(hash_state)), size());
}

template <typename Sink>
void Chain::Stringify(Sink& sink) const {
  for (const absl::string_view block : blocks()) sink.Append(block);
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_CHAIN_DETAILS_H_
