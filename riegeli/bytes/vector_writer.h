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

#ifndef RIEGELI_BYTES_VECTOR_WRITER_H_
#define RIEGELI_BYTES_VECTOR_WRITER_H_

#include <stddef.h>

#include <type_traits>

#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/object.h"
#include "riegeli/base/uninitialized_vector.h"
#include "riegeli/bytes/resizable_writer.h"

namespace riegeli {

namespace vector_writer_internal {

template <typename VectorType, typename Enable = void>
struct HasAllocatorType : std::false_type {};

template <typename VectorType>
struct HasAllocatorType<VectorType,
                        std::void_t<typename VectorType::allocator_type>>
    : std::true_type {};

// `ResizableTraits` for `std::vector<T, Alloc>` including
// `UninitializedVector<T>`, `absl::InlinedVector<T, inlined_size, Alloc>`
// including `UninitializedInlinedVector<T, inlined_size>`, or a similar type.
// Its value type must be trivially copyable, usually `char`.
//
// The vector type must support at least the following members:
//
// ```
//   using value_type = ...;
//
//   value_type* data();
//   size_t size() const;
//   size_t max_size() const;
//   size_t capacity() const;
//
//   iterator begin();
//   iterator end();
//
//   void resize(size_t new_size);
//   void reserve(size_t new_capacity);
//   void erase(iterator first, iterator last);
// ```
//
// Warning: byte contents are reinterpreted as values of type `T`, and the size
// is rounded up to a multiple of the element type.
template <typename VectorType>
struct VectorResizableTraits {
 private:
  using T = typename VectorType::value_type;

 public:
  static_assert(std::is_trivially_copyable_v<T>,
                "Value type of the parameter of VectorResizableTraits must be "
                "trivially copyable");

  using Resizable = VectorType;
  static char* Data(Resizable& dest) {
    return reinterpret_cast<char*>(dest.data());
  }
  static size_t Size(const Resizable& dest) { return dest.size() * sizeof(T); }
  static constexpr bool kIsStable = true;
  static bool Resize(Resizable& dest, size_t new_size, size_t used_size) {
    RIEGELI_ASSERT_LE(used_size, dest.size() * sizeof(T))
        << "Failed precondition of ResizableTraits::Resize(): "
           "used size exceeds old size";
    RIEGELI_ASSERT_LE(used_size, new_size)
        << "Failed precondition of ResizableTraits::Resize(): "
           "used size exceeds new size";
    const size_t new_num_elements = SizeToNumElements(new_size);
    Reserve(dest, new_num_elements, used_size);
    dest.resize(new_num_elements);
    return true;
  }
  static bool Grow(Resizable& dest, size_t new_size, size_t used_size) {
    RIEGELI_ASSERT_GT(new_size, dest.size() * sizeof(T))
        << "Failed precondition of ResizableTraits::Grow(): "
           "no need to grow";
    RIEGELI_ASSERT_LE(used_size, dest.size() * sizeof(T))
        << "Failed precondition of ResizableTraits::Grow(): "
           "used size exceeds old size";
    RIEGELI_ASSERT_LE(used_size, new_size)
        << "Failed precondition of ResizableTraits::Grow(): "
           "used size exceeds new size";
    const size_t new_num_elements = SizeToNumElements(new_size);
    Reserve(dest, new_num_elements, used_size);
    if (!GrowUnderCapacity(dest, new_size)) RIEGELI_ASSUME_UNREACHABLE();
    return true;
  }
  static bool GrowUnderCapacity(Resizable& dest, size_t new_size) {
    RIEGELI_ASSERT_GT(new_size, dest.size() * sizeof(T))
        << "Failed precondition of ResizableTraits::GrowUnderCapacity(): "
           "no need to grow";
    const size_t new_num_elements = SizeToNumElements(new_size);
    if (new_num_elements > dest.capacity()) return false;
    if constexpr (HasAllocatorType<Resizable>::value) {
      if constexpr (std::is_same_v<typename Resizable::allocator_type,
                                   UninitializedAllocator<T>>) {
        dest.resize(dest.capacity());
        return true;
      }
    }
    dest.resize(UnsignedClamp(
        dest.size() + UnsignedClamp(dest.size(),
                                    SizeToNumElements(kDefaultMinBlockSize),
                                    SizeToNumElements(kDefaultMaxBlockSize)),
        new_num_elements, dest.capacity()));
    return true;
  }

 private:
  static size_t SizeToNumElements(size_t size) {
    return size / sizeof(T) + (size % sizeof(T) == 0 ? 0 : 1);
  }
  static void Reserve(Resizable& dest, size_t new_num_elements,
                      size_t used_size) {
    if (new_num_elements > dest.capacity()) {
      dest.erase(dest.begin() + SizeToNumElements(used_size), dest.end());
      if constexpr (std::is_default_constructible_v<Resizable>) {
        if (dest.capacity() <= Resizable().capacity()) {
          dest.reserve(new_num_elements);
          return;
        }
      }
      dest.reserve(UnsignedClamp(dest.capacity() + dest.capacity() / 2,
                                 new_num_elements, dest.max_size()));
    }
    RIEGELI_ASSUME_GE(dest.capacity(), new_num_elements);
  }
};

}  // namespace vector_writer_internal

// Template parameter independent part of `VectorWriter`.
using VectorWriterBase = ResizableWriterBase;

// A `Writer` which writes to `std::vector<T, Alloc>` including
// `UninitializedVector<T>`, `absl::InlinedVector<T, inlined_size, Alloc>`,
// including `UninitializedInlinedVector<T, inlined_size>`, or a similar
// type supported by `VectorResizableTraits`, called `VectorType` here.
// Its value type must be trivially copyable, usually `char`.
//
// If `Options::append()` is `false` (the default), replaces existing contents
// of the `VectorType`, clearing it first. If `Options::append()` is `true`,
// appends to existing contents of the `VectorType`.
//
// It supports `Seek()` and `ReadMode()`.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the `VectorType` being written to. `Dest` must support
// `Dependency<VectorType*, Dest>`, e.g. `VectorType*` (not owned, default),
// `VectorType` (owned), `Any<VectorType*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as
// `UninitializedVector<char>` if there are no constructor arguments or the only
// argument is `Options`, otherwise as `TargetT` of the type of the first
// constructor argument, except that CTAD is deleted if the first constructor
// argument is a `VectorType&` or `const VectorType&` (to avoid writing to an
// unintentionally separate copy of an existing object).
//
// The `VectorType` must not be accessed until the `VectorWriter` is closed or
// no longer used, except that it is allowed to read the `VectorType`
// immediately after `Flush()`.
//
// `VectorWriter` with `UninitializedVector<char>`
// or `UninitializedInlinedVector<char, inlined_size>` is more efficient
// than `StringWriter` because the destination can be resized with uninitialized
// space. `CompactStringWriter` can also be used for this purpose.
template <typename Dest = UninitializedVector<char>*>
class VectorWriter
    : public ResizableWriter<
          vector_writer_internal::VectorResizableTraits<std::remove_pointer_t<
              typename Dependency<void*, TargetT<Dest>>::Subhandle>>,
          Dest> {
 public:
  using VectorWriter::ResizableWriter::ResizableWriter;

  VectorWriter(VectorWriter&& that) = default;
  VectorWriter& operator=(VectorWriter&& that) = default;
};

explicit VectorWriter(Closed) -> VectorWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit VectorWriter(Dest&& dest, VectorWriterBase::Options options =
                                       VectorWriterBase::Options())
    -> VectorWriter<std::conditional_t<
        std::conjunction_v<
            std::is_lvalue_reference<Dest>,
            std::is_convertible<std::remove_reference_t<Dest>*,
                                const std::remove_pointer_t<typename Dependency<
                                    void*, TargetT<Dest>>::Subhandle>*>>,
        DeleteCtad<Dest&&>, TargetT<Dest>>>;
explicit VectorWriter(
    VectorWriterBase::Options options = VectorWriterBase::Options())
    -> VectorWriter<UninitializedVector<char>>;

}  // namespace riegeli

#endif  // RIEGELI_BYTES_VECTOR_WRITER_H_
