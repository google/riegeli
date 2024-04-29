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

#include "riegeli/base/compact_string.h"

#include <stddef.h>  // IWYU pragma: keep
#include <stdint.h>

#include <cstring>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/estimated_allocated_size.h"

namespace riegeli {

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if !__cpp_inline_variables
constexpr uintptr_t CompactString::kDefaultRepr;
constexpr size_t CompactString::kInlineCapacity;
constexpr size_t CompactString::kInlineDataOffset;
#endif

void CompactString::AssignSlow(absl::string_view src) {
  const size_t old_capacity = capacity();
  DeleteRepr(std::exchange(
      repr_,
      MakeRepr(src, UnsignedMax(src.size(), old_capacity + old_capacity / 2))));
}

void CompactString::AssignSlow(const CompactString& that) {
  const uintptr_t that_tag = that.repr_ & 7;
  const size_t that_size = that.allocated_size_for_tag(that_tag);
  if (ABSL_PREDICT_TRUE(that_size <= capacity())) {
    set_size(that_size);
    // Use `std::memmove()` to support assigning from `*this`.
    std::memmove(data(), that.allocated_data(), that_size);
  } else {
    AssignSlow(absl::string_view(that.allocated_data(), that_size));
  }
}

uintptr_t CompactString::MakeReprSlow(size_t size, size_t capacity) {
  RIEGELI_ASSERT_LE(size, capacity)
      << "Failed precondition of CompactString::MakeReprSlow(): "
         "size exceeds capacity";
  RIEGELI_ASSERT_GT(capacity, kInlineCapacity)
      << "Failed precondition of CompactString::MakeReprSlow(): "
         "representation is inline, use MakeRepr() instead";
  uintptr_t repr;
  if (capacity <= 0xff) {
    const size_t requested =
        UnsignedMin(EstimatedAllocatedSize(capacity + 2), size_t{0xff + 2});
    repr = reinterpret_cast<uintptr_t>(Allocate(requested) + 2);
    set_allocated_capacity<uint8_t>(requested - 2, repr);
    set_allocated_size<uint8_t>(size, repr);
  } else if (capacity <= 0xffff) {
    const size_t requested =
        UnsignedMin(EstimatedAllocatedSize(capacity + 4), size_t{0xffff + 4});
    repr = reinterpret_cast<uintptr_t>(Allocate(requested) + 4);
    set_allocated_capacity<uint16_t>(requested - 4, repr);
    set_allocated_size<uint16_t>(size, repr);
  } else {
    static_assert(sizeof(size_t) % 4 == 0, "Unsupported size_t size");
    RIEGELI_CHECK_LE(capacity, max_size()) << "CompactString capacity overflow";
    const size_t requested =
        EstimatedAllocatedSize(capacity + 2 * sizeof(size_t));
    repr =
        reinterpret_cast<uintptr_t>(Allocate(requested) + 2 * sizeof(size_t));
    set_allocated_capacity<size_t>(requested - 2 * sizeof(size_t), repr);
    set_allocated_size<size_t>(size, repr);
  }
  return repr;
}

char* CompactString::ResizeSlow(size_t new_size, size_t min_capacity,
                                size_t used_size) {
  RIEGELI_ASSERT_LE(new_size, min_capacity)
      << "Failed precondition of CompactString::ResizeSlow(): "
         "size exceeds capacity";
  RIEGELI_ASSERT_LE(used_size, size())
      << "Failed precondition of CompactString::ResizeSlow(): "
         "used size exceeds old size";
  RIEGELI_ASSERT_LE(used_size, new_size)
      << "Failed precondition of CompactString::ResizeSlow(): "
         "used size exceeds new size";
  const size_t old_capacity = capacity();
  RIEGELI_ASSERT_GT(min_capacity, kInlineCapacity)
      << "Inline representation has a fixed capacity, so reallocation is never "
         "needed when the new capacity can use inline representation";
  const uintptr_t new_repr = MakeReprSlow(
      new_size, UnsignedMax(min_capacity, old_capacity + old_capacity / 2));
  char* ptr = allocated_data(new_repr);
  std::memcpy(ptr, data(), used_size);
  ptr += used_size;
  DeleteRepr(std::exchange(repr_, new_repr));
  return ptr;
}

void CompactString::ShrinkToFitSlow() {
  const uintptr_t tag = repr_ & 7;
  RIEGELI_ASSERT_NE(tag, 1u)
      << "Failed precondition of CompactString::ShrinkToFitSlow(): "
         "representation is inline, use shrink_to_fit() instead";
  size_t size;
  if (tag == 2) {
    size = allocated_size<uint8_t>();
    if (size > kInlineCapacity &&
        allocated_capacity<uint8_t>() + 2 <=
            UnsignedMin(EstimatedAllocatedSize(size + 2), size_t{0xff + 2})) {
      return;
    }
  } else if (tag == 4) {
    size = allocated_size<uint16_t>();
    if (size > 0xff &&
        allocated_capacity<uint16_t>() + 4 <=
            UnsignedMin(EstimatedAllocatedSize(size + 4), size_t{0xffff + 4})) {
      return;
    }
  } else if (tag == 0) {
    size = allocated_size<size_t>();
    if (size > 0xffff &&
        allocated_capacity<size_t>() + 2 * sizeof(size_t) <=
            EstimatedAllocatedSize(size + 2 * sizeof(size_t))) {
      return;
    }
  } else {
    RIEGELI_ASSERT_UNREACHABLE() << "Impossible tag: " << tag;
  }
  DeleteRepr(std::exchange(
      repr_, MakeRepr(absl::string_view(allocated_data(), size))));
}

char* CompactString::AppendSlow(size_t length) {
  const size_t old_size = size();
  RIEGELI_CHECK_LE(length, max_size() - old_size)
      << "CompactString size overflow";
  const size_t new_size = old_size + length;
  return ResizeSlow(new_size, new_size, old_size);
}

void CompactString::AppendSlow(absl::string_view src) {
  RIEGELI_ASSERT(!src.empty())
      << "Failed precondition of CompactString::AppendSlow(): "
         "nothing to append";
  const size_t old_size = size();
  const size_t old_capacity = capacity();
  RIEGELI_CHECK_LE(src.size(), max_size() - old_size)
      << "CompactString size overflow";
  const size_t new_size = old_size + src.size();
  RIEGELI_ASSERT_GT(new_size, kInlineCapacity)
      << "Inline representation has a fixed capacity, so reallocation is never "
         "needed when the new capacity can use inline representation";
  const uintptr_t new_repr = MakeReprSlow(
      new_size, UnsignedMax(new_size, old_capacity + old_capacity / 2));
  char* ptr = allocated_data(new_repr);
  std::memcpy(ptr, data(), old_size);
  ptr += old_size;
  // Copy from `src` before deleting `repr_` to support appending from a
  // substring of `*this`.
  std::memcpy(ptr, src.data(), src.size());
  DeleteRepr(std::exchange(repr_, new_repr));
}

void CompactString::ReserveOneMoreByteSlow() {
  const size_t used_size = size();
  RIEGELI_ASSERT_GT(used_size + 1, kInlineCapacity)
      << "Inline representation has a fixed capacity, so reallocation is never "
         "needed when the new capacity can use inline representation";
  const uintptr_t new_repr = MakeReprSlow(used_size, used_size + 1);
  char* const ptr = allocated_data(new_repr);
  std::memcpy(ptr, data(), used_size);
  DeleteRepr(std::exchange(repr_, new_repr));
}

}  // namespace riegeli
