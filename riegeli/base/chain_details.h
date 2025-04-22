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
#include <iosfwd>
#include <iterator>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/meta/type_traits.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/bytes_ref.h"
#include "riegeli/base/chain_base.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/external_data.h"
#include "riegeli/base/external_ref_base.h"
#include "riegeli/base/external_ref_support.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/intrusive_shared_ptr.h"
#include "riegeli/base/memory_estimator.h"
#include "riegeli/base/new_aligned.h"
#include "riegeli/base/ownership.h"
#include "riegeli/base/type_traits.h"

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

// Access private constructors of `Chain::Block`.
struct Chain::MakeBlock {
  Block operator()(IntrusiveSharedPtr<RawBlock> block) const {
    return Block(std::move(block));
  }
  Block operator()(RawBlock* block) const { return Block(block); }
};

class Chain::BlockRef {
 public:
  BlockRef(const BlockRef& that) = default;
  BlockRef& operator=(const BlockRef& that) = default;

  /*implicit*/ operator absl::string_view() const;

  bool empty() const;
  const char* data() const;
  size_t size() const;

  // Indicates support for:
  //  * `ExternalRef(BlockRef)`
  //  * `ExternalRef(BlockRef, substr)`
  friend void RiegeliSupportsExternalRef(const BlockRef*) {}

  // Supports `ExternalRef`.
  friend bool RiegeliExternalCopy(const BlockRef* self) {
    return self->ExternalCopy();
  }

  // Supports `ExternalRef`.
  friend Chain::Block RiegeliToChainBlock(const BlockRef* self,
                                          absl::string_view substr) {
    return self->ToChainBlock(substr);
  }

  // Supports `ExternalRef`.
  template <typename Callback>
  friend void RiegeliExternalDelegate(const BlockRef* self,
                                      absl::string_view substr,
                                      Callback&& delegate_to) {
    self->ExternalDelegate(substr, std::forward<Callback>(delegate_to));
  }

  // Returns a pointer to the external object if this is an external block
  // holding an object of type `T`, otherwise returns `nullptr`.
  template <typename T>
  const T* external_object() const;

 private:
  friend class Chain;  // For `BlockRef()`.

  explicit BlockRef(const Chain* chain, BlockPtrPtr ptr)
      : chain_(chain), ptr_(ptr) {}

  bool ExternalCopy() const;
  Chain::Block ToChainBlock(absl::string_view substr) const;
  template <typename Callback>
  void ExternalDelegate(absl::string_view substr, Callback&& delegate_to) const;

  const Chain* chain_;
  // If `*chain_` has short data, `kBeginShortData`.
  // If `*chain_` has block pointers, a pointer to an element of the block
  // pointer array.
  BlockPtrPtr ptr_;
};

class Chain::BlockIterator : public WithCompare<BlockIterator> {
 public:
  using iterator_concept = std::random_access_iterator_tag;
  // `iterator_category` is only `std::input_iterator_tag` because the
  // `LegacyForwardIterator` requirement and above require `reference` to be
  // a true reference type.
  using iterator_category = std::input_iterator_tag;
  using value_type = BlockRef;
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

  BlockIterator() = default;

  explicit BlockIterator(const Chain* chain ABSL_ATTRIBUTE_LIFETIME_BOUND,
                         size_t block_index);

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
    RIEGELI_ASSERT_EQ(a.chain_, b.chain_)
        << "Failed precondition of operator==(Chain::BlockIterator): "
           "incomparable iterators";
    return a.ptr_ == b.ptr_;
  }
  friend StrongOrdering RIEGELI_COMPARE(BlockIterator a, BlockIterator b) {
    RIEGELI_ASSERT_EQ(a.chain_, b.chain_)
        << "Failed precondition of operator<=>(Chain::BlockIterator): "
           "incomparable iterators";
    return riegeli::Compare(a.ptr_, b.ptr_);
  }
  friend difference_type operator-(BlockIterator a, BlockIterator b) {
    RIEGELI_ASSERT_EQ(a.chain_, b.chain_)
        << "Failed precondition of operator-(Chain::BlockIterator): "
           "incomparable iterators";
    return a.ptr_ - b.ptr_;
  }
  friend BlockIterator operator+(difference_type n, BlockIterator a) {
    return a + n;
  }

 private:
  friend class Chain;

  static constexpr BlockPtrPtr kBeginShortData{0};
  static constexpr BlockPtrPtr kEndShortData{sizeof(BlockPtr)};

  explicit BlockIterator(const Chain* chain, BlockPtrPtr ptr);

  size_t CharIndexInChainInternal() const;

  const Chain* chain_ = nullptr;
  // If `chain_ == nullptr`, `kBeginShortData`.
  // If `*chain_` has no block pointers and no short data, `kEndShortData`.
  // If `*chain_` has short data, `kBeginShortData` or `kEndShortData`.
  // If `*chain_` has block pointers, a pointer to an element of the block
  // pointer array.
  BlockPtrPtr ptr_ = kBeginShortData;
};

class Chain::Blocks {
 public:
  using value_type = BlockRef;
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

// Implementation details follow.

struct Chain::ExternalMethods {
  void (*delete_block)(RawBlock* block);
  void (*dump_structure)(const RawBlock& block, std::ostream& dest);
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

void DumpStructureDefault(std::ostream& dest);

template <typename T, std::enable_if_t<
                          HasRiegeliDumpStructureWithSubstr<T>::value, int> = 0>
inline void DumpStructure(const T* object, absl::string_view substr,
                          std::ostream& dest) {
  RiegeliDumpStructure(object, substr, dest);
}

template <
    typename T,
    std::enable_if_t<
        absl::conjunction<absl::negation<HasRiegeliDumpStructureWithSubstr<T>>,
                          HasRiegeliDumpStructureWithoutData<T>>::value,
        int> = 0>
inline void DumpStructure(const T* object,
                          ABSL_ATTRIBUTE_UNUSED absl::string_view substr,
                          std::ostream& dest) {
  RiegeliDumpStructure(object, dest);
}

template <typename T,
          std::enable_if_t<
              absl::conjunction<
                  absl::negation<HasRiegeliDumpStructureWithSubstr<T>>,
                  absl::negation<HasRiegeliDumpStructureWithoutData<T>>>::value,
              int> = 0>
inline void DumpStructure(ABSL_ATTRIBUTE_UNUSED const T* object,
                          ABSL_ATTRIBUTE_UNUSED absl::string_view substr,
                          std::ostream& dest) {
  chain_internal::DumpStructureDefault(dest);
}

}  // namespace chain_internal

// Supports `ExternalRef` and `Chain::Block`.
void RiegeliDumpStructure(const std::string* self, std::ostream& dest);

template <typename T>
struct Chain::ExternalMethodsFor {
  // Creates an external block containing an external object constructed from
  // `object`, and sets block data to `BytesRef(new_object)`.
  static IntrusiveSharedPtr<RawBlock> NewBlock(Initializer<T> object);

  // Creates an external block containing an external object constructed from
  // `object`, and sets block data to `data`.
  static IntrusiveSharedPtr<RawBlock> NewBlock(Initializer<T> object,
                                               absl::string_view substr);

 private:
  static void DeleteBlock(RawBlock* block);
  static void DumpStructure(const RawBlock& block, std::ostream& dest);
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
                               *block);
  block->unchecked_external_object<T>().~T();
  DeleteAligned<RawBlock, UnsignedMax(alignof(RawBlock), alignof(T))>(
      block, RawBlock::kExternalAllocatedSize<T>());
}

template <typename T>
void Chain::ExternalMethodsFor<T>::DumpStructure(const RawBlock& block,
                                                 std::ostream& dest) {
  chain_internal::DumpStructure(&block.unchecked_external_object<T>(), block,
                                dest);
}

template <typename T>
constexpr size_t Chain::ExternalMethodsFor<T>::DynamicSizeOf() {
  return RawBlock::kExternalAllocatedSize<T>();
}

template <typename T>
void Chain::ExternalMethodsFor<T>::RegisterSubobjects(
    const RawBlock* block, MemoryEstimator& memory_estimator) {
  chain_internal::RegisterSubobjects(&block->unchecked_external_object<T>(),
                                     *block, memory_estimator);
}

template <typename T>
inline Chain::RawBlock::RawBlock(Initializer<T> object) {
  external_.methods = &ExternalMethodsFor<T>::kMethods;
  std::move(object).ConstructAt(&unchecked_external_object<T>());
  substr_ = BytesRef(unchecked_external_object<T>());
  RIEGELI_ASSERT(is_external()) << "A RawBlock with allocated_end_ == nullptr "
                                   "should be considered external";
}

template <typename T>
inline Chain::RawBlock::RawBlock(Initializer<T> object,
                                 absl::string_view substr)
    : substr_(substr) {
  external_.methods = &ExternalMethodsFor<T>::kMethods;
  std::move(object).ConstructAt(&unchecked_external_object<T>());
  RIEGELI_ASSERT(is_external()) << "A RawBlock with allocated_end_ == nullptr "
                                   "should be considered external";
  AssertSubstr(unchecked_external_object<T>(), substr);
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
    substr_ = substr_.substr(0, 0);
    return true;
  }
  return false;
}

inline bool Chain::RawBlock::TryRemoveSuffix(size_t length) {
  RIEGELI_ASSERT_LE(length, size())
      << "Failed precondition of Chain::RawBlock::TryRemoveSuffix(): "
      << "length to remove greater than current size";
  if (is_mutable()) {
    substr_.remove_suffix(length);
    return true;
  }
  return false;
}

inline bool Chain::RawBlock::TryRemovePrefix(size_t length) {
  RIEGELI_ASSERT_LE(length, size())
      << "Failed precondition of Chain::RawBlock::TryRemovePrefix(): "
      << "length to remove greater than current size";
  if (is_mutable()) {
    substr_.remove_prefix(length);
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

inline Chain::BlockRef::operator absl::string_view() const {
  if (ptr_ == BlockIterator::kBeginShortData) {
    return chain_->short_data();
  } else {
    return *ptr_.as_ptr()->block_ptr;
  }
}

inline bool Chain::BlockRef::empty() const {
  return ptr_ != BlockIterator::kBeginShortData &&
         ptr_.as_ptr()->block_ptr->empty();
}

inline const char* Chain::BlockRef::data() const {
  if (ptr_ == BlockIterator::kBeginShortData) {
    return chain_->short_data_begin();
  } else {
    return ptr_.as_ptr()->block_ptr->data_begin();
  }
}

inline size_t Chain::BlockRef::size() const {
  if (ptr_ == BlockIterator::kBeginShortData) {
    return chain_->size_;
  } else {
    return ptr_.as_ptr()->block_ptr->size();
  }
}

inline bool Chain::BlockRef::ExternalCopy() const {
  return ptr_ == BlockIterator::kBeginShortData;
}

inline Chain::Block Chain::BlockRef::ToChainBlock(
    absl::string_view substr) const {
  RIEGELI_ASSERT(ptr_ != BlockIterator::kBeginShortData)
      << "Failed precondition of RiegeliToChainBlock(const Chain::BlockRef*): "
         "case excluded by RiegeliExternalCopy()";
  return Block(ptr_.as_ptr()->block_ptr, substr);
}

template <typename Callback>
inline void Chain::BlockRef::ExternalDelegate(absl::string_view substr,
                                              Callback&& delegate_to) const {
  RIEGELI_ASSERT(ptr_ != BlockIterator::kBeginShortData)
      << "Failed precondition of "
         "RiegeliExternalDelegate(const Chain::BlockRef*): "
         "case excluded by RiegeliExternalCopy()";
  std::forward<Callback>(delegate_to)(Block(ptr_.as_ptr()->block_ptr), substr);
}

template <typename T>
inline const T* Chain::BlockRef::external_object() const {
  if (ptr_ == BlockIterator::kBeginShortData) {
    return nullptr;
  } else {
    return ptr_.as_ptr()->block_ptr->checked_external_object<T>();
  }
}

inline Chain::BlockIterator::BlockIterator(
    const Chain* chain ABSL_ATTRIBUTE_LIFETIME_BOUND, size_t block_index)
    : chain_(chain),
      ptr_((ABSL_PREDICT_FALSE(chain_ == nullptr) ? kBeginShortData
            : chain_->begin_ == chain_->end_
                ? (chain_->empty() ? kEndShortData : kBeginShortData)
                : BlockPtrPtr::from_ptr(chain_->begin_)) +
           IntCast<ptrdiff_t>(block_index)) {}

inline Chain::BlockIterator::BlockIterator(const Chain* chain, BlockPtrPtr ptr)
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
  return BlockRef(chain_, ptr_);
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

template <
    typename T,
    std::enable_if_t<
        absl::conjunction<NotSelfCopy<Chain::Block, TargetT<T>>,
                          std::is_convertible<TargetT<T>, BytesRef>>::value,
        int>>
inline Chain::Block::Block(T&& object)
    : block_(
          ExternalMethodsFor<TargetT<T>>::NewBlock(std::forward<T>(object))) {}

template <typename T>
inline Chain::Block::Block(T&& object, absl::string_view substr)
    : block_(ExternalMethodsFor<TargetT<T>>::NewBlock(std::forward<T>(object),
                                                      substr)) {}

inline Chain::Block::Block(RawBlock* block, absl::string_view substr) {
  if (block->size() == substr.size()) {
    block_.Reset(block, kShareOwnership);
    return;
  }
  if (const Block* const block_ptr = block->checked_external_object<Block>()) {
    // `block` is already a `Block`. Refer to its target instead.
    block = block_ptr->block_.get();
  }
  block_.Reset(block, kShareOwnership);
  block_ = ExternalMethodsFor<Block>::NewBlock(std::move(*this), substr);
}

inline Chain::Block::Block(RawBlock* block) {
  if (const Block* const block_ptr = block->checked_external_object<Block>()) {
    // `block` is already a `Block`. Refer to its target instead.
    block = block_ptr->block_.get();
  }
  block_.Reset(block, kShareOwnership);
}

inline Chain::Block::Block(IntrusiveSharedPtr<RawBlock> block) {
  if (const Block* const block_ptr = block->checked_external_object<Block>()) {
    // `block` is already a `Block`. Refer to its target instead.
    block = block_ptr->block_;
  }
  block_ = std::move(block);
}

inline ExternalStorage Chain::Block::ToExternalStorage() && {
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
  return BlockRef(chain_, chain_->begin_ == chain_->end_
                              ? BlockIterator::kBeginShortData
                              : BlockPtrPtr::from_ptr(chain_->begin_ + n));
}

inline Chain::Blocks::reference Chain::Blocks::at(size_type n) const {
  RIEGELI_CHECK_LT(n, size()) << "Failed precondition of Chain::Blocks::at(): "
                                 "block index out of range";
  return BlockRef(chain_, chain_->begin_ == chain_->end_
                              ? BlockIterator::kBeginShortData
                              : BlockPtrPtr::from_ptr(chain_->begin_ + n));
}

inline Chain::Blocks::reference Chain::Blocks::front() const {
  RIEGELI_ASSERT(!empty())
      << "Failed precondition of Chain::Blocks::front(): no blocks";
  return BlockRef(chain_, chain_->begin_ == chain_->end_
                              ? BlockIterator::kBeginShortData
                              : BlockPtrPtr::from_ptr(chain_->begin_));
}

inline Chain::Blocks::reference Chain::Blocks::back() const {
  RIEGELI_ASSERT(!empty())
      << "Failed precondition of Chain::Blocks::back(): no blocks";
  return BlockRef(chain_, chain_->begin_ == chain_->end_
                              ? BlockIterator::kBeginShortData
                              : BlockPtrPtr::from_ptr(chain_->end_ - 1));
}

template <typename T>
constexpr size_t Chain::kExternalAllocatedSize() {
  return RawBlock::kExternalAllocatedSize<T>();
}

inline Chain::Chain(BytesRef src) { Initialize(src); }

inline Chain::Chain(ExternalRef src) { std::move(src).InitializeTo(*this); }

template <typename Src,
          std::enable_if_t<SupportsExternalRefWhole<Src>::value, int>>
inline Chain::Chain(Src&& src) {
  ExternalRef(std::forward<Src>(src)).InitializeTo(*this);
}

inline Chain::Chain(Block src) {
  if (src.raw_block() != nullptr) Initialize(std::move(src));
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

inline void Chain::Reset(ExternalRef src) { std::move(src).AssignTo(*this); }

template <typename Src,
          std::enable_if_t<SupportsExternalRefWhole<Src>::value, int>>
inline void Chain::Reset(Src&& src) {
  ExternalRef(std::forward<Src>(src)).AssignTo(*this);
}

inline void Chain::Clear() {
  size_ = 0;
  if (begin_ != end_) ClearSlow();
}

inline void Chain::Initialize(absl::string_view src) {
  RIEGELI_ASSERT_EQ(size_, 0u)
      << "Failed precondition of Chain::Initialize(string_view): "
         "size not reset";
  if (src.size() <= kMaxShortDataSize) {
    if (src.empty()) return;
    EnsureHasHere();
    size_ = src.size();
    std::memcpy(short_data_begin(), src.data(), src.size());
    return;
  }
  InitializeSlow(src);
}

inline void Chain::Initialize(Block src) {
  size_ = src.raw_block()->size();
  (end_++)->block_ptr = std::move(src).raw_block().Release();
}

inline absl::string_view Chain::short_data() const {
  return absl::string_view(short_data_begin(), size_);
}

inline char* Chain::short_data_begin() {
  RIEGELI_ASSERT_EQ(begin_, end_)
      << "Failed precondition of Chain::short_data_begin(): blocks exist";
  RIEGELI_ASSERT(empty() || has_here())
      << "Failed precondition of Chain::short_data_begin(): "
         "block pointer array is allocated";
  return block_ptrs_.short_data;
}

inline const char* Chain::short_data_begin() const {
  RIEGELI_ASSERT_EQ(begin_, end_)
      << "Failed precondition of Chain::short_data_begin(): blocks exist";
  RIEGELI_ASSERT(empty() || has_here())
      << "Failed precondition of Chain::short_data_begin(): "
         "block pointer array is allocated";
  return block_ptrs_.short_data;
}

inline void Chain::DeleteBlockPtrs() {
  if (has_allocated()) {
    std::allocator<BlockPtr>().deallocate(
        block_ptrs_.allocated.begin,
        2 * PtrDistance(block_ptrs_.allocated.begin,
                        block_ptrs_.allocated.end));
  }
}

inline void Chain::EnsureHasHere() {
  RIEGELI_ASSERT_EQ(begin_, end_)
      << "Failed precondition of Chain::EnsureHasHere(): blocks exist";
  if (ABSL_PREDICT_FALSE(has_allocated())) {
    DeleteBlockPtrs();
    begin_ = block_ptrs_.here;
    end_ = block_ptrs_.here;
  }
}

inline void Chain::UnrefBlocks() { UnrefBlocks(begin_, end_); }

inline void Chain::UnrefBlocks(const BlockPtr* begin, const BlockPtr* end) {
  if (begin != end) UnrefBlocksSlow(begin, end);
}

inline Chain::Blocks Chain::blocks() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
  return Blocks(this);
}

inline absl::optional<absl::string_view> Chain::TryFlat() const
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  switch (end_ - begin_) {
    case 0:
      return short_data();
    case 1:
      return *front();
    default:
      return absl::nullopt;
  }
}

inline absl::string_view Chain::Flatten() ABSL_ATTRIBUTE_LIFETIME_BOUND {
  switch (end_ - begin_) {
    case 0:
      return short_data();
    case 1:
      return *front();
    default:
      return FlattenSlow();
  }
}

inline absl::Span<char> Chain::AppendFixedBuffer(size_t length, Options options)
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  return AppendBuffer(length, length, length, options);
}

inline absl::Span<char> Chain::PrependFixedBuffer(
    size_t length, Options options) ABSL_ATTRIBUTE_LIFETIME_BOUND {
  return PrependBuffer(length, length, length, options);
}

inline void Chain::Append(ExternalRef src) { std::move(src).AppendTo(*this); }

inline void Chain::Append(ExternalRef src, Options options) {
  std::move(src).AppendTo(*this, options);
}

inline void Chain::Prepend(ExternalRef src) { std::move(src).PrependTo(*this); }

inline void Chain::Prepend(ExternalRef src, Options options) {
  std::move(src).PrependTo(*this, options);
}

template <typename Src,
          std::enable_if_t<SupportsExternalRefWhole<Src>::value, int>>
inline void Chain::Append(Src&& src) {
  ExternalRef(std::forward<Src>(src)).AppendTo(*this);
}

template <typename Src,
          std::enable_if_t<SupportsExternalRefWhole<Src>::value, int>>
inline void Chain::Append(Src&& src, Options options) {
  ExternalRef(std::forward<Src>(src)).AppendTo(*this, options);
}

template <typename Src,
          std::enable_if_t<SupportsExternalRefWhole<Src>::value, int>>
inline void Chain::Prepend(Src&& src) {
  ExternalRef(std::forward<Src>(src)).PrependTo(*this);
}

template <typename Src,
          std::enable_if_t<SupportsExternalRefWhole<Src>::value, int>>
inline void Chain::Prepend(Src&& src, Options options) {
  ExternalRef(std::forward<Src>(src)).PrependTo(*this, options);
}

template <typename HashState>
HashState Chain::HashValue(HashState hash_state) const {
  // TODO: this code relies on two internal Abseil APIs:
  // 1. AbslInternalPiecewiseCombiner
  // 2. WeaklyMixedInteger
  // Reimplement this in terms of the public Abseil API.
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
  return HashState::combine(combiner.finalize(std::move(hash_state)),
                            absl::hash_internal::WeaklyMixedInteger{size()});
}

template <typename Sink>
void Chain::Stringify(Sink& dest) const {
  for (const absl::string_view block : blocks()) dest.Append(block);
}

template <typename DebugStream>
void Chain::Debug(DebugStream& dest) const {
  dest.DebugStringQuote();
  typename DebugStream::EscapeState escape_state;
  for (const absl::string_view fragment : blocks()) {
    dest.DebugStringFragment(fragment, escape_state);
  }
  dest.DebugStringQuote();
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_CHAIN_DETAILS_H_
