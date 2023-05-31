// Copyright 2021 Google LLC
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

#include "riegeli/containers/linear_sorted_string_set.h"

#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <cstring>
#include <initializer_list>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/numeric/bits.h"
#include "absl/status/status.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/compact_string.h"
#include "riegeli/base/memory_estimator.h"
#include "riegeli/bytes/compact_string_writer.h"
#include "riegeli/endian/endian_reading.h"
#include "riegeli/varint/varint_reading.h"
#include "riegeli/varint/varint_writing.h"

namespace riegeli {

namespace {

inline size_t SharedLength(absl::string_view a, absl::string_view b) {
  const size_t min_length = UnsignedMin(a.size(), b.size());
  size_t length = 0;
  if (min_length < sizeof(uint64_t)) {
    // Compare byte by byte.
    while (length < min_length) {
      if (a[length] != b[length]) return length;
      ++length;
    }
    return length;
  }

  // Compare whole blocks, except for the last pair.
  const size_t limit = min_length - sizeof(uint64_t);
  while (length < limit) {
    const uint64_t xor_result = ReadLittleEndian64(a.data() + length) ^
                                ReadLittleEndian64(b.data() + length);
    if (xor_result != 0) {
      return length + IntCast<size_t>(absl::countr_zero(xor_result)) / 8;
    }
    length += sizeof(uint64_t);
  }
  // Compare the last, possible incomplete blocks as whole blocks shifted
  // backwards.
  const uint64_t xor_result = ReadLittleEndian64(a.data() + limit) ^
                              ReadLittleEndian64(b.data() + limit);
  if (xor_result != 0) {
    return limit + IntCast<size_t>(absl::countr_zero(xor_result)) / 8;
  }
  return limit + sizeof(uint64_t);
}

}  // namespace

LinearSortedStringSet LinearSortedStringSet::FromSorted(
    std::initializer_list<absl::string_view> src) {
  return FromSorted<>(src);
}

LinearSortedStringSet LinearSortedStringSet::FromUnsorted(
    std::initializer_list<absl::string_view> src) {
  return FromUnsorted<>(src);
}

inline LinearSortedStringSet::LinearSortedStringSet(CompactString&& encoded)
    : encoded_(std::move(encoded)) {}

size_t LinearSortedStringSet::size() const {
  size_t size = 0;
  for (Iterator iter = cbegin(); iter != cend(); ++iter) {
    ++size;
  }
  return size;
}

absl::string_view LinearSortedStringSet::first() const {
  RIEGELI_ASSERT(!empty())
      << "Failed precondition of LinearSortedStringSet::first(): "
         "empty set";
  const absl::string_view encoded_view = encoded_;
  uint64_t tagged_length;
  const absl::optional<const char*> ptr =
      ReadVarint64(encoded_view.data(),
                   encoded_view.data() + encoded_view.size(), tagged_length);
  RIEGELI_ASSERT(ptr != absl::nullopt)
      << "Malformed LinearSortedStringSet encoding (tagged_length)";
  RIEGELI_ASSERT_EQ(tagged_length & 1, 0u)
      << "Malformed LinearSortedStringSet encoding "
         "(first element has shared_length > 0)";
  const uint64_t length = tagged_length >> 1;
  RIEGELI_ASSERT_LE(
      length, IntCast<size_t>(encoded_view.data() + encoded_view.size() - *ptr))
      << "Malformed LinearSortedStringSet encoding (unshared)";
  return absl::string_view(*ptr, IntCast<size_t>(length));
}

bool LinearSortedStringSet::contains(absl::string_view element) const {
  // Length of the prefix shared between the previous `*iterator` and the
  // current `*iterator`.
  size_t shared_length = 0;
  // Length of the prefix shared between `element` and `*iterator`.
  size_t common_length = 0;
  for (Iterator iterator = cbegin(); iterator != cend();
       shared_length = iterator.Next()) {
    // It would be incorrect to assume that if `shared_length < common_length`
    // then `*iterator > element` because `shared_length` is not guaranteed to
    // be maximal.
    common_length = UnsignedMin(common_length, shared_length);
    const absl::string_view found = *iterator;
    common_length +=
        SharedLength(absl::string_view(found.data() + common_length,
                                       found.size() - common_length),
                     absl::string_view(element.data() + common_length,
                                       element.size() - common_length));
    RIEGELI_ASSERT_EQ(absl::string_view(found.data(), common_length),
                      absl::string_view(element.data(), common_length))
        << "common_length incorrectly updated";
    const absl::string_view found_suffix(found.data() + common_length,
                                         found.size() - common_length);
    const absl::string_view element_suffix(element.data() + common_length,
                                           element.size() - common_length);
    if (found_suffix >= element_suffix) return found_suffix.empty();
  }
  return false;  // Not found.
}

bool LinearSortedStringSet::EqualImpl(const LinearSortedStringSet& a,
                                      const LinearSortedStringSet& b) {
  return std::equal(a.cbegin(), a.cend(), b.cbegin(), b.cend());
}

bool LinearSortedStringSet::LessImpl(const LinearSortedStringSet& a,
                                     const LinearSortedStringSet& b) {
  return std::lexicographical_compare(a.cbegin(), a.cend(), b.cbegin(),
                                      b.cend());
}

size_t LinearSortedStringSet::EstimateMemory() const {
  MemoryEstimator memory_estimator;
  memory_estimator.RegisterMemory(sizeof(LinearSortedStringSet));
  memory_estimator.RegisterSubobjects(*this);
  return memory_estimator.TotalMemory();
}

size_t LinearSortedStringSet::Iterator::Next() {
  RIEGELI_ASSERT(cursor_ != nullptr)
      << "Failed precondition of LinearSortedStringSet::Iterator::Next(): "
         "iterator is end()";
  if (cursor_ == limit_) {
    // `end()` was reached.
    cursor_ = nullptr;           // Mark `end()`.
    current_ = CompactString();  // Free memory.
    return 0;
  }
  const char* ptr = cursor_;
  uint64_t tagged_length;
  {
    const absl::optional<const char*> next =
        ReadVarint64(ptr, limit_, tagged_length);
    if (next == absl::nullopt) {
      RIEGELI_ASSERT_UNREACHABLE()
          << "Malformed LinearSortedStringSet encoding (tagged_length)";
    } else {
      ptr = *next;
    }
  }
  const uint64_t unshared_length = tagged_length >> 1;
  uint64_t shared_length = 0;
  if ((tagged_length & 1) == 0) {
    // `shared_length == 0` and is not stored.
  } else {
    // `shared_length` is stored.
    {
      const absl::optional<const char*> next =
          ReadVarint64(ptr, limit_, shared_length);
      if (next == absl::nullopt) {
        RIEGELI_ASSERT_UNREACHABLE()
            << "Malformed LinearSortedStringSet encoding (shared_length)";
      } else {
        ptr = *next;
      }
    }
    RIEGELI_ASSERT_LE(shared_length, current_.size())
        << "Malformed LinearSortedStringSet encoding "
           "(shared_length larger than previous element)";
  }
  RIEGELI_ASSERT_LE(unshared_length, PtrDistance(ptr, limit_))
      << "Malformed LinearSortedStringSet encoding (unshared)";
  const size_t new_size = IntCast<size_t>(shared_length + unshared_length);
  char* const current_unshared =
      current_.resize(new_size, IntCast<size_t>(shared_length));
  std::memcpy(current_unshared, ptr, IntCast<size_t>(unshared_length));
  ptr += IntCast<size_t>(unshared_length);
  cursor_ = ptr;
  return IntCast<size_t>(shared_length);
}

LinearSortedStringSet::Builder::Builder() = default;

LinearSortedStringSet::Builder::Builder(Builder&& that) noexcept = default;

LinearSortedStringSet::Builder& LinearSortedStringSet::Builder::operator=(
    Builder&& that) noexcept = default;

LinearSortedStringSet::Builder::~Builder() = default;

void LinearSortedStringSet::Builder::Reset() {
  writer_.Reset();
  last_.clear();
}

void LinearSortedStringSet::Builder::InsertNext(absl::string_view element) {
  const absl::Status status = TryInsertNext(element);
  RIEGELI_CHECK(status.ok())
      << "Failed precondition of LinearSortedStringSet::Builder::InsertNext(): "
      << status.message();
}

template <typename Element,
          std::enable_if_t<std::is_same<Element, std::string>::value, int>>
void LinearSortedStringSet::Builder::InsertNext(Element&& element) {
  // `std::move(element)` is correct and `std::forward<Element>(element)` is not
  // necessary: `Element` is always `std::string`, never an lvalue reference.
  const absl::Status status = TryInsertNext(std::move(element));
  RIEGELI_CHECK(status.ok())
      << "Failed precondition of LinearSortedStringSet::Builder::InsertNext(): "
      << status.message();
}

template void LinearSortedStringSet::Builder::InsertNext(std::string&& element);

absl::Status LinearSortedStringSet::Builder::TryInsertNext(
    absl::string_view element) {
  return InsertNextImpl(
      element, [this](absl::string_view element, size_t shared_length) {
        last_.erase(shared_length);
        const absl::string_view unshared(element.data() + shared_length,
                                         element.size() - shared_length);
        // TODO: When `absl::string_view` becomes C++17
        // `std::string_view`: `last_.append(unshared);`
        last_.append(unshared.data(), unshared.size());
        RIEGELI_ASSERT_EQ(last_, element) << "last_ incorrectly reconstructed";
        return unshared;
      });
}

template <typename Element,
          std::enable_if_t<std::is_same<Element, std::string>::value, int>>
absl::Status LinearSortedStringSet::Builder::TryInsertNext(Element&& element) {
  // `std::move(element)` is correct and `std::forward<Element>(element)` is not
  // necessary: `Element` is always `std::string`, never an lvalue reference.
  return InsertNextImpl(
      std::move(element), [this](std::string&& element, size_t shared_length) {
        last_ = std::move(element);
        return absl::string_view(last_.data() + shared_length,
                                 last_.size() - shared_length);
      });
}

template absl::Status LinearSortedStringSet::Builder::TryInsertNext(
    std::string&& element);

template <typename Element, typename UpdateLast>
absl::Status LinearSortedStringSet::Builder::InsertNextImpl(
    Element&& element, UpdateLast update_last) {
  RIEGELI_ASSERT(writer_.is_open())
      << "Failed precondition of "
         "LinearSortedStringSet::Builder::TryInsertNext(): "
         "set already built or moved from";
  const size_t shared_length = SharedLength(last_, element);
  if (ABSL_PREDICT_FALSE(absl::string_view(element.data() + shared_length,
                                           element.size() - shared_length) <=
                         absl::string_view(last_.data() + shared_length,
                                           last_.size() - shared_length)) &&
      !empty()) {
    return OutOfOrder(element);
  }
  const absl::string_view unshared =
      update_last(std::forward<Element>(element), shared_length);
  const size_t unshared_length = unshared.size();
  // `shared_length` is stored if `shared_length > 0`.
  const uint64_t tagged_length =
      (uint64_t{unshared_length} << 1) |
      (shared_length > 0 ? uint64_t{1} : uint64_t{0});
  WriteVarint64(tagged_length, writer_);
  if (shared_length > 0) WriteVarint64(uint64_t{shared_length}, writer_);
  writer_.Write(unshared);
  return absl::OkStatus();
}

absl::Status LinearSortedStringSet::Builder::OutOfOrder(
    absl::string_view element) const {
  return absl::FailedPreconditionError(
      element == last()
          ? absl::StrCat("Elements are not unique: new \"",
                         absl::CHexEscape(element), "\" == last")
          : absl::StrCat("Elements are not sorted: new \"",
                         absl::CHexEscape(element), "\" < last \"",
                         absl::CHexEscape(last()), "\""));
}

LinearSortedStringSet LinearSortedStringSet::Builder::Build() && {
  RIEGELI_ASSERT(writer_.is_open())
      << "Failed precondition of LinearSortedStringSet::Builder::Build(): "
         "set already built or moved from";
  if (ABSL_PREDICT_FALSE(!writer_.Close())) {
    RIEGELI_ASSERT_UNREACHABLE()
        << "A CompactStringWriter has no reason to fail: " << writer_.status();
  }
  writer_.dest().shrink_to_fit();
  return LinearSortedStringSet(std::move(writer_.dest()));
}

}  // namespace riegeli
