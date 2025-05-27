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

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/numeric/bits.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/compact_string.h"
#include "riegeli/base/compare.h"
#include "riegeli/base/debug.h"
#include "riegeli/bytes/compact_string_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"
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
  size_t current_length = 0;
  const absl::string_view encoded_view = encoded_;
  const char* ptr = encoded_view.data();
  const char* const limit = ptr + encoded_view.size();
  while (ptr != limit) {
    uint64_t tagged_length;
    if (const absl::optional<const char*> next =
            ReadVarint64(ptr, limit, tagged_length);
        next == absl::nullopt) {
      RIEGELI_ASSUME_UNREACHABLE()
          << "Malformed LinearSortedStringSet encoding (tagged_length)";
    } else {
      ptr = *next;
    }
    const uint64_t unshared_length = tagged_length >> 1;
    uint64_t shared_length;
    if ((tagged_length & 1) == 0) {
      // `shared_length == 0` and is not stored.
      shared_length = 0;
    } else {
      // `shared_length > 0` and is stored.
      if (const absl::optional<const char*> next =
              ReadVarint64(ptr, limit, shared_length);
          next == absl::nullopt) {
        RIEGELI_ASSUME_UNREACHABLE()
            << "Malformed LinearSortedStringSet encoding (shared_length)";
      } else {
        ptr = *next;
      }
      // Compare `<` instead of `<=`, before `++shared_length`.
      RIEGELI_ASSERT_LT(shared_length, current_length)
          << "Malformed LinearSortedStringSet encoding "
             "(shared_length larger than previous element)";
      ++shared_length;
    }
    RIEGELI_ASSERT_LE(unshared_length, PtrDistance(ptr, limit))
        << "Malformed LinearSortedStringSet encoding (unshared)";
    current_length = IntCast<size_t>(shared_length + unshared_length);
    ptr += IntCast<size_t>(unshared_length);
    ++size;
  }
  return size;
}

absl::string_view LinearSortedStringSet::first() const
    ABSL_ATTRIBUTE_LIFETIME_BOUND {
  RIEGELI_ASSERT(!empty())
      << "Failed precondition of LinearSortedStringSet::first(): "
         "empty set";
  const absl::string_view encoded_view = encoded_;
  uint64_t tagged_length;
  const absl::optional<const char*> ptr =
      ReadVarint64(encoded_view.data(),
                   encoded_view.data() + encoded_view.size(), tagged_length);
  RIEGELI_ASSERT_NE(ptr, absl::nullopt)
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

bool LinearSortedStringSet::ContainsImpl(absl::string_view element,
                                         SplitElementIterator iterator,
                                         size_t& cumulative_index) {
  // Length of the prefix shared between `element` and `*iterator`.
  size_t common_length = 0;
  for (; iterator != SplitElementIterator(); ++iterator, ++cumulative_index) {
    // It would be incorrect to assume that if
    // `found.prefix().size() < common_length` then `*iterator > element`
    // because `found.prefix().size()` is not guaranteed to be maximal.
    const SplitElement found = *iterator;
    common_length = UnsignedMin(common_length, found.prefix().size());
    RIEGELI_ASSUME_LE(common_length, element.size())
        << "The invariant common_length <= element.size() should hold";
    if (common_length < found.prefix().size()) {
      common_length += SharedLength(found.prefix().substr(common_length),
                                    element.substr(common_length));
      if (common_length < found.prefix().size()) {
        RIEGELI_ASSUME_LE(common_length, element.size())
            << "The invariant common_length <= element.size() should hold";
        // The first difference, if any, is at
        // `found.prefix().data() + common_length`.
        RIEGELI_ASSERT_EQ(found.prefix().substr(0, common_length),
                          element.substr(0, common_length))
            << "common_length should cover an equal prefix";
        const absl::string_view found_middle =
            found.prefix().substr(common_length);
        const absl::string_view element_suffix = element.substr(common_length);
        RIEGELI_ASSERT(!found_middle.empty())
            << "Implied by common_length < found_prefix().size()";
        RIEGELI_ASSERT(element_suffix.empty() ||
                       found_middle.front() != element_suffix.front())
            << "common_length should cover the maximal common prefix";
        if (element_suffix.empty() ||
            static_cast<unsigned char>(found_middle.front()) >
                static_cast<unsigned char>(element_suffix.front())) {
          return false;
        }
        continue;
      }
    }

    RIEGELI_ASSERT_GE(common_length, found.prefix().size())
        << "common_length < found.prefix().size() was handled above";
    size_t common_length_in_suffix = common_length - found.prefix().size();
    RIEGELI_ASSUME_LE(common_length_in_suffix, found.suffix().size())
        << "The invariant common_length <= found.size() should hold";
    RIEGELI_ASSUME_LE(common_length, element.size())
        << "The invariant common_length <= element.size() should hold";
    common_length +=
        SharedLength(found.suffix().substr(common_length_in_suffix),
                     element.substr(common_length));
    common_length_in_suffix = common_length - found.prefix().size();
    RIEGELI_ASSUME_LE(common_length_in_suffix, found.suffix().size())
        << "The invariant common_length <= found.size() should hold";
    RIEGELI_ASSUME_LE(common_length, element.size())
        << "The invariant common_length <= element.size() should hold";
    // The first difference, if any, is at
    // `found.suffix().data() + (common_length - found_prefix().size())`.
    RIEGELI_ASSERT_EQ(
        absl::StrCat(found.prefix(),
                     found.suffix().substr(0, common_length_in_suffix)),
        element.substr(0, common_length))
        << "common_length should cover an equal prefix";
    const absl::string_view found_suffix =
        found.suffix().substr(common_length_in_suffix);
    const absl::string_view element_suffix = element.substr(common_length);
    RIEGELI_ASSERT(found_suffix.empty() || element_suffix.empty() ||
                   found_suffix.front() != element_suffix.front())
        << "common_length should cover the maximal common prefix";
    if (found_suffix.empty()) {
      if (element_suffix.empty()) return true;
    } else {
      if (element_suffix.empty() ||
          static_cast<unsigned char>(found_suffix.front()) >
              static_cast<unsigned char>(element_suffix.front())) {
        return false;
      }
    }
  }
  return false;  // Not found.
}

bool LinearSortedStringSet::Equal(const LinearSortedStringSet& a,
                                  const LinearSortedStringSet& b) {
  return std::equal(a.cbegin(), a.cend(), b.cbegin(), b.cend());
}

StrongOrdering LinearSortedStringSet::Compare(const LinearSortedStringSet& a,
                                              const LinearSortedStringSet& b) {
  Iterator a_iter = a.cbegin();
  Iterator b_iter = b.cbegin();
  while (a_iter != a.cend()) {
    if (b_iter == b.cend()) return StrongOrdering::greater;
    if (const StrongOrdering ordering = riegeli::Compare(*a_iter, *b_iter);
        ordering != 0) {
      return ordering;
    }
    ++a_iter;
    ++b_iter;
  }
  return b_iter == b.cend() ? StrongOrdering::equal : StrongOrdering::less;
}

absl::Status LinearSortedStringSet::EncodeImpl(Writer& dest) const {
  if (ABSL_PREDICT_FALSE(!WriteVarint64(uint64_t{encoded_.size()}, dest))) {
    return dest.status();
  }
  if (ABSL_PREDICT_FALSE(!dest.Write(encoded_))) return dest.status();
  return absl::OkStatus();
}

absl::Status LinearSortedStringSet::DecodeImpl(Reader& src,
                                               DecodeOptions options) {
  uint64_t encoded_size;
  if (ABSL_PREDICT_FALSE(!ReadVarint64(src, encoded_size))) {
    return src.StatusOrAnnotate(
        absl::InvalidArgumentError("Malformed LinearSortedStringSet encoding "
                                   "(encoded_size)"));
  }
  if (ABSL_PREDICT_FALSE(encoded_size > options.max_encoded_size())) {
    return src.AnnotateStatus(absl::ResourceExhaustedError(absl::StrCat(
        "Maximum LinearSortedStringSet encoded length exceeded: ", encoded_size,
        " > ", options.max_encoded_size())));
  }
  CompactString encoded(IntCast<size_t>(encoded_size));
  if (ABSL_PREDICT_FALSE(
          !src.Read(IntCast<size_t>(encoded_size), encoded.data()))) {
    return src.StatusOrAnnotate(
        absl::InvalidArgumentError("Malformed LinearSortedStringSet encoding "
                                   "(encoded)"));
  }

  // Validate `encoded` and update `*options.decode_state()`.
  size_t size = 0;
  size_t current_length = 0;
  CompactString current_if_validated_and_shared;
  absl::optional<absl::string_view> current_if_validated;
  if (options.decode_state() != nullptr) {
    current_if_validated = options.decode_state()->last;
  }
  const absl::string_view encoded_view = encoded;
  const char* ptr = encoded_view.data();
  const char* const limit = ptr + encoded_view.size();
  while (ptr != limit) {
    uint64_t tagged_length;
    if (const absl::optional<const char*> next =
            ReadVarint64(ptr, limit, tagged_length);
        next == absl::nullopt) {
      return src.AnnotateStatus(absl::InvalidArgumentError(
          "Malformed LinearSortedStringSet encoding (tagged_length)"));
    } else {
      ptr = *next;
    }
    const uint64_t unshared_length = tagged_length >> 1;
    if ((tagged_length & 1) == 0) {
      // `shared_length == 0` and is not stored.
      if (ABSL_PREDICT_FALSE(unshared_length > PtrDistance(ptr, limit))) {
        return src.AnnotateStatus(absl::InvalidArgumentError(
            "Malformed LinearSortedStringSet encoding (unshared)"));
      }
      current_length = IntCast<size_t>(unshared_length);
      if (options.validate()) {
        if (ABSL_PREDICT_TRUE(current_if_validated != absl::nullopt) &&
            ABSL_PREDICT_FALSE(absl::string_view(ptr, current_length) <=
                               *current_if_validated)) {
          return src.AnnotateStatus(absl::InvalidArgumentError(absl::StrCat(
              "Elements are not sorted and unique: new ",
              riegeli::Debug(absl::string_view(ptr, current_length)),
              " <= last ", riegeli::Debug(*current_if_validated))));
        }
        current_if_validated_and_shared.clear();
        current_if_validated = absl::string_view(ptr, current_length);
      }
    } else {
      // `shared_length > 0` and is stored.
      uint64_t shared_length;
      if (const absl::optional<const char*> next =
              ReadVarint64(ptr, limit, shared_length);
          next == absl::nullopt) {
        return src.AnnotateStatus(absl::InvalidArgumentError(
            "Malformed LinearSortedStringSet encoding (shared_length)"));
      } else {
        ptr = *next;
      }
      // Compare `>=` instead of `>`, before `++shared_length`.
      if (ABSL_PREDICT_FALSE(shared_length >= current_length)) {
        return src.AnnotateStatus(absl::InvalidArgumentError(
            "Malformed LinearSortedStringSet encoding "
            "(shared_length larger than previous element)"));
      }
      ++shared_length;
      if (ABSL_PREDICT_FALSE(unshared_length > PtrDistance(ptr, limit))) {
        return src.AnnotateStatus(absl::InvalidArgumentError(
            "Malformed LinearSortedStringSet encoding (unshared)"));
      }
      current_length = IntCast<size_t>(shared_length + unshared_length);
      if (options.validate()) {
        if (ABSL_PREDICT_TRUE(current_if_validated != absl::nullopt) &&
            ABSL_PREDICT_FALSE(absl::string_view(ptr, unshared_length) <=
                               current_if_validated->substr(shared_length))) {
          return src.AnnotateStatus(absl::InvalidArgumentError(absl::StrCat(
              "Elements are not sorted and unique: new ",
              riegeli::Debug(
                  absl::StrCat(current_if_validated->substr(0, shared_length),
                               absl::string_view(ptr, unshared_length))),
              " <= last ", riegeli::Debug(*current_if_validated))));
        }
        // The unshared part of the next element will be written here.
        char* current_unshared;
        if (current_if_validated_and_shared.empty()) {
          RIEGELI_ASSERT(current_if_validated != absl::nullopt)
              << "shared_length > 0 implies that this is not the first element";
          char* const current_data =
              current_if_validated_and_shared.resize(current_length, 0);
          std::memcpy(current_data, current_if_validated->data(),
                      IntCast<size_t>(shared_length));
          current_unshared = current_data + IntCast<size_t>(shared_length);
        } else {
          current_unshared = current_if_validated_and_shared.resize(
              current_length, IntCast<size_t>(shared_length));
        }
        std::memcpy(current_unshared, ptr, IntCast<size_t>(unshared_length));
        current_if_validated = current_if_validated_and_shared;
      }
    }
    ptr += IntCast<size_t>(unshared_length);
    ++size;
  }
  if (options.decode_state() != nullptr && size > 0) {
    options.decode_state()->cumulative_size += size;
    if (options.validate()) {
      if (current_if_validated_and_shared.empty()) {
        options.decode_state()->last = *current_if_validated;
      } else {
        options.decode_state()->last =
            std::move(current_if_validated_and_shared);
      }
    }
  }
  encoded_ = std::move(encoded);
  return absl::OkStatus();
}

LinearSortedStringSet::Iterator& LinearSortedStringSet::Iterator::operator++() {
  RIEGELI_ASSERT_NE(cursor_, nullptr)
      << "Failed precondition of "
         "LinearSortedStringSet::Iterator::operator++: "
         "iterator is end()";
  if (cursor_ == limit_) {
    // `end()` was reached.
    cursor_ = nullptr;  // Mark `end()`.
    length_if_unshared_ = 0;
    current_if_shared_ = CompactString();  // Free memory.
    return *this;
  }
  const char* ptr = cursor_;
  uint64_t tagged_length;
  if (const absl::optional<const char*> next =
          ReadVarint64(ptr, limit_, tagged_length);
      next == absl::nullopt) {
    RIEGELI_ASSUME_UNREACHABLE()
        << "Malformed LinearSortedStringSet encoding (tagged_length)";
  } else {
    ptr = *next;
  }
  const uint64_t unshared_length = tagged_length >> 1;
  if ((tagged_length & 1) == 0) {
    // `shared_length == 0` and is not stored.
    RIEGELI_ASSERT_LE(unshared_length, PtrDistance(ptr, limit_))
        << "Malformed LinearSortedStringSet encoding (unshared)";
    current_if_shared_.clear();
    length_if_unshared_ = IntCast<size_t>(unshared_length);
    ptr += IntCast<size_t>(unshared_length);
    cursor_ = ptr;
    return *this;
  }
  // `shared_length > 0` and is stored.
  uint64_t shared_length;
  if (const absl::optional<const char*> next =
          ReadVarint64(ptr, limit_, shared_length);
      next == absl::nullopt) {
    RIEGELI_ASSUME_UNREACHABLE()
        << "Malformed LinearSortedStringSet encoding (shared_length)";
  } else {
    ptr = *next;
  }
  // Compare `<` instead of `<=`, before `++shared_length`.
  RIEGELI_ASSERT_LT(shared_length, length_if_unshared_ > 0
                                       ? length_if_unshared_
                                       : current_if_shared_.size())
      << "Malformed LinearSortedStringSet encoding "
         "(shared_length larger than previous element)";
  ++shared_length;
  RIEGELI_ASSERT_LE(unshared_length, PtrDistance(ptr, limit_))
      << "Malformed LinearSortedStringSet encoding (unshared)";
  const size_t new_size = IntCast<size_t>(shared_length + unshared_length);
  // The unshared part of the next element will be written here.
  char* current_unshared;
  if (length_if_unshared_ > 0) {
    char* const current_data = current_if_shared_.resize(new_size, 0);
    std::memcpy(current_data, cursor_ - length_if_unshared_,
                IntCast<size_t>(shared_length));
    current_unshared = current_data + IntCast<size_t>(shared_length);
  } else {
    current_unshared =
        current_if_shared_.resize(new_size, IntCast<size_t>(shared_length));
  }
  std::memcpy(current_unshared, ptr, IntCast<size_t>(unshared_length));
  length_if_unshared_ = 0;
  ptr += IntCast<size_t>(unshared_length);
  cursor_ = ptr;
  return *this;
}

LinearSortedStringSet::SplitElementIterator&
LinearSortedStringSet::SplitElementIterator::operator++() {
  RIEGELI_ASSERT_NE(cursor_, nullptr)
      << "Failed precondition of "
         "LinearSortedStringSet::SplitElementIterator::operator++: "
         "iterator is end()";
  if (cursor_ == limit_) {
    // `end()` was reached.
    cursor_ = nullptr;                    // Mark `end()`.
    prefix_if_stored_ = CompactString();  // Free memory.
    prefix_ = absl::string_view();
    suffix_length_ = 0;
    return *this;
  }
  const char* ptr = cursor_;
  uint64_t tagged_length;
  if (const absl::optional<const char*> next =
          ReadVarint64(ptr, limit_, tagged_length);
      next == absl::nullopt) {
    RIEGELI_ASSUME_UNREACHABLE()
        << "Malformed LinearSortedStringSet encoding (tagged_length)";
  } else {
    ptr = *next;
  }
  const uint64_t unshared_length = tagged_length >> 1;
  if ((tagged_length & 1) == 0) {
    // `shared_length == 0` and is not stored.
    RIEGELI_ASSERT_LE(unshared_length, PtrDistance(ptr, limit_))
        << "Malformed LinearSortedStringSet encoding (unshared)";
    prefix_ = absl::string_view();
    suffix_length_ = IntCast<size_t>(unshared_length);
    ptr += IntCast<size_t>(unshared_length);
    cursor_ = ptr;
    return *this;
  }
  // `shared_length > 0` and is stored.
  uint64_t shared_length;
  if (const absl::optional<const char*> next =
          ReadVarint64(ptr, limit_, shared_length);
      next == absl::nullopt) {
    RIEGELI_ASSUME_UNREACHABLE()
        << "Malformed LinearSortedStringSet encoding (shared_length)";
  } else {
    ptr = *next;
  }
  // Compare `<` instead of `<=`, before `++shared_length`.
  RIEGELI_ASSERT_LT(shared_length, prefix_.size() + suffix_length_)
      << "Malformed LinearSortedStringSet encoding "
         "(shared_length larger than previous element)";
  ++shared_length;
  RIEGELI_ASSERT_LE(unshared_length, PtrDistance(ptr, limit_))
      << "Malformed LinearSortedStringSet encoding (unshared)";
  if (shared_length <= prefix_.size()) {
    prefix_ = prefix_.substr(0, IntCast<size_t>(shared_length));
  } else if (prefix_.empty()) {
    prefix_ = absl::string_view(cursor_ - suffix_length_,
                                IntCast<size_t>(shared_length));
  } else {
    // Append
    // `absl::string_view(cursor_ - suffix_length_,
    //                    IntCast<size_t>(shared_length) - prefix_.size())`
    // to `prefix_`, using `prefix_if_stored_` for storage.

    // The new prefix.
    char* prefix_data;
    // The suffix of the new prefix which is not shared with the previous
    // element will be written here.
    char* prefix_unshared;
    if (prefix_if_stored_.data() == prefix_.data()) {
      RIEGELI_ASSERT_GE(prefix_if_stored_.size(), prefix_.size())
          << "Failed invariant of LinearSortedStringSet::SplitElementIterator: "
             "prefix_ overflows prefix_if_stored_";
      // `prefix_if_stored_` already begins with `prefix_`.
      prefix_unshared = prefix_if_stored_.resize(IntCast<size_t>(shared_length),
                                                 prefix_.size());
      prefix_data = prefix_unshared - prefix_.size();
    } else {
      // Copy `prefix_` to the beginning of `prefix_if_stored_` first.
      prefix_data = prefix_if_stored_.resize(IntCast<size_t>(shared_length), 0);
      std::memcpy(prefix_data, prefix_.data(), prefix_.size());
      prefix_unshared = prefix_data + prefix_.size();
    }
    std::memcpy(prefix_unshared, cursor_ - suffix_length_,
                IntCast<size_t>(shared_length) - prefix_.size());
    prefix_ = absl::string_view(prefix_data, IntCast<size_t>(shared_length));
  }
  ptr += IntCast<size_t>(unshared_length);
  cursor_ = ptr;
  suffix_length_ = IntCast<size_t>(unshared_length);
  return *this;
}

LinearSortedStringSet::Builder::Builder() = default;

LinearSortedStringSet::Builder::Builder(Builder&& that) noexcept
    : writer_(
          std::exchange(that.writer_, CompactStringWriter<CompactString>())),
      size_(std::exchange(that.size_, 0)),
      last_(std::exchange(that.last_, std::string())) {}

LinearSortedStringSet::Builder& LinearSortedStringSet::Builder::operator=(
    Builder&& that) noexcept {
  writer_ = std::exchange(that.writer_, CompactStringWriter<CompactString>());
  size_ = std::exchange(that.size_, 0);
  last_ = std::exchange(that.last_, std::string());
  return *this;
}

LinearSortedStringSet::Builder::~Builder() = default;

void LinearSortedStringSet::Builder::Reset() {
  writer_.Reset();
  size_ = 0;
  last_.clear();
}

bool LinearSortedStringSet::Builder::InsertNext(absl::string_view element) {
  const absl::StatusOr<bool> inserted = TryInsertNext(element);
  RIEGELI_CHECK_OK(inserted)
      << "Failed precondition of LinearSortedStringSet::Builder::InsertNext()";
  return *inserted;
}

template <typename Element,
          std::enable_if_t<std::is_same_v<Element, std::string>, int>>
bool LinearSortedStringSet::Builder::InsertNext(Element&& element) {
  // `std::move(element)` is correct and `std::forward<Element>(element)` is not
  // necessary: `Element` is always `std::string`, never an lvalue reference.
  const absl::StatusOr<bool> inserted = TryInsertNext(std::move(element));
  RIEGELI_CHECK_OK(inserted)
      << "Failed precondition of LinearSortedStringSet::Builder::InsertNext()";
  return *inserted;
}

template bool LinearSortedStringSet::Builder::InsertNext(std::string&& element);

absl::StatusOr<bool> LinearSortedStringSet::Builder::TryInsertNext(
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
          std::enable_if_t<std::is_same_v<Element, std::string>, int>>
absl::StatusOr<bool> LinearSortedStringSet::Builder::TryInsertNext(
    Element&& element) {
  // `std::move(element)` is correct and `std::forward<Element>(element)` is not
  // necessary: `Element` is always `std::string`, never an lvalue reference.
  return InsertNextImpl(std::move(element),
                        [this](std::string&& element, size_t shared_length) {
                          last_ = std::move(element);
                          return absl::string_view(last_).substr(shared_length);
                        });
}

template absl::StatusOr<bool> LinearSortedStringSet::Builder::TryInsertNext(
    std::string&& element);

template <typename Element, typename UpdateLast>
absl::StatusOr<bool> LinearSortedStringSet::Builder::InsertNextImpl(
    Element&& element, UpdateLast update_last) {
  RIEGELI_ASSERT(writer_.is_open())
      << "Failed precondition of "
         "LinearSortedStringSet::Builder::TryInsertNext(): "
         "set already built or moved from";
  size_t shared_length = SharedLength(last_, element);
  const absl::string_view unshared_element(element.data() + shared_length,
                                           element.size() - shared_length);
  const absl::string_view unshared_last(last_.data() + shared_length,
                                        last_.size() - shared_length);
  if (ABSL_PREDICT_FALSE(unshared_element <= unshared_last) && !empty()) {
    if (ABSL_PREDICT_TRUE(unshared_element == unshared_last)) return false;
    return absl::FailedPreconditionError(
        absl::StrCat("Elements are not sorted: new ", riegeli::Debug(element),
                     " < last ", riegeli::Debug(last())));
  }
  if (shared_length == 1) {
    // If only the first byte is shared, write the element fully unshared.
    // The encoded length is the same, and this allows `Iterator` to avoid
    // allocating the string.
    shared_length = 0;
  }
  const absl::string_view unshared =
      update_last(std::forward<Element>(element), shared_length);
  const size_t unshared_length = unshared.size();
  // `shared_length` is stored if `shared_length > 0`.
  const uint64_t tagged_length =
      (uint64_t{unshared_length} << 1) |
      (shared_length > 0 ? uint64_t{1} : uint64_t{0});
  WriteVarint64(tagged_length, writer_);
  if (shared_length > 0) WriteVarint64(uint64_t{shared_length - 1}, writer_);
  writer_.Write(unshared);
  ++size_;
  return true;
}

LinearSortedStringSet LinearSortedStringSet::Builder::Build() {
  RIEGELI_ASSERT(writer_.is_open())
      << "Failed precondition of LinearSortedStringSet::Builder::Build(): "
         "set already built or moved from";
  RIEGELI_EVAL_ASSERT(writer_.Close())
      << "A CompactStringWriter has no reason to fail: " << writer_.status();
  writer_.dest().shrink_to_fit();
  LinearSortedStringSet set(std::move(writer_.dest()));
  writer_.Reset();
  size_ = 0;
  last_.clear();
  RIEGELI_ASSERT(empty())
      << "Failed postcondition of LinearSortedStringSet::Builder::Build(): "
         "builder should be empty";
  return set;
}

}  // namespace riegeli
