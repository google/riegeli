// Copyright 2025 Google LLC
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

#ifndef RIEGELI_BASE_CORD_ITERATOR_SPAN_H_
#define RIEGELI_BASE_CORD_ITERATOR_SPAN_H_

#include <stddef.h>

#include <cstring>
#include <optional>
#include <string>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/dependency.h"

ABSL_POINTERS_DEFAULT_NONNULL

namespace riegeli {

// `CordIteratorSpan` specifies a span of `absl::Cord::CharIterator` contents
// from the current position with the given length.
//
// This can express the span as a single object, which is sometimes convenient.
class CordIteratorSpan {
 public:
  // Returns the number of bytes from `src` to the end of the `absl::Cord`.
  static size_t Remaining(const absl::Cord::CharIterator& src) {
    return IntCast<size_t>(
        absl::Cord::Distance(src, absl::Cord::CharIterator()));
  }

  // Copies `length` bytes from `src` to `dest[]`.
  static void Read(absl::Cord::CharIterator& src, size_t length,
                   char* absl_nullable dest);

  // Specifies the span from the current position of `*src` with `length`.
  explicit CordIteratorSpan(absl::Cord::CharIterator* src
                                ABSL_ATTRIBUTE_LIFETIME_BOUND,
                            size_t length)
      : iterator_(src), length_(length) {
    RIEGELI_ASSERT_LE(length, Remaining(*iterator_))
        << "Failed precondition of CordIteratorSpan: not enough remaining data";
  }

  CordIteratorSpan(CordIteratorSpan&& that) = default;
  CordIteratorSpan& operator=(CordIteratorSpan&& that) = default;

  absl::Cord::CharIterator& iterator() const { return *iterator_; }
  size_t length() const { return length_; }

  // Destructively reads the contents of the span to an `absl::Cord`.
  //
  // An implicit conversion allows to use a `CordIteratorSpan` when an
  // `absl::Cord` is expected. Some functions treat a parameter of type
  // `CordIteratorSpan` specially to enable a more efficient implementation.
  /*implicit*/ operator absl::Cord() && { return std::move(*this).ToCord(); }

  // Destructively reads the contents of the span to an `absl::Cord`.
  absl::Cord ToCord() && {
    return absl::Cord::AdvanceAndRead(iterator_, length_);
  }

  // Destructively reads the contents of the span to an `absl::string_view`.
  //
  // May use `scratch` for storage for the result.
  absl::string_view ToStringView(std::string& scratch) &&;

  // Destructively reads the contents of the span to an existing `std::string`.
  void ToString(std::string& dest) &&;

  // Returns the contents of the span as an `absl::string_view` if it is flat.
  // Otherwise returns `std::nullopt`.
  std::optional<absl::string_view> TryFlat() const;

 private:
  static void ReadSlow(absl::Cord::CharIterator& src, size_t length,
                       char* dest);

  absl::Cord::CharIterator* iterator_;
  size_t length_;
};

// Specialization of `DependencyImpl<const absl::Cord*, CordIteratorSpan>`.
//
// This allows to pass a `CordIteratorSpan` as a parameter of `CordReader`.
template <>
class DependencyImpl<const absl::Cord*, CordIteratorSpan> {
 public:
  explicit DependencyImpl(CordIteratorSpan span)
      : span_(std::move(span)), cord_(std::move(span_)) {}

  CordIteratorSpan& manager() ABSL_ATTRIBUTE_LIFETIME_BOUND { return span_; }
  const CordIteratorSpan& manager() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return span_;
  }

  const absl::Cord* get() const ABSL_ATTRIBUTE_LIFETIME_BOUND { return &cord_; }

  bool IsOwning() const { return false; }

  static constexpr bool kIsStable = false;

 protected:
  DependencyImpl(DependencyImpl&& that) = default;
  DependencyImpl& operator=(DependencyImpl&& that) = default;

  ~DependencyImpl() = default;

 private:
  CordIteratorSpan span_;
  const absl::Cord cord_;
};

// Implementation details follow.

inline void CordIteratorSpan::Read(absl::Cord::CharIterator& src, size_t length,
                                   char* absl_nullable dest) {
  RIEGELI_ASSERT_LE(length, Remaining(src))
      << "Failed precondition of CordIteratorSpan::Read(): "
         "not enough remaining data";
  if (length == 0) return;
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of CordIteratorSpan::Read(): "
         "non-empty span from nullptr";
  const absl::string_view chunk = absl::Cord::ChunkRemaining(src);
  if (ABSL_PREDICT_FALSE(chunk.size() < length)) {
    ReadSlow(src, length, dest);
    return;
  }
  std::memcpy(dest, chunk.data(), length);
  absl::Cord::Advance(&src, length);
}

inline std::optional<absl::string_view> CordIteratorSpan::TryFlat() const {
  if (length_ == 0) return absl::string_view();
  absl::string_view chunk = absl::Cord::ChunkRemaining(*iterator_);
  if (ABSL_PREDICT_FALSE(chunk.size() < length_)) return std::nullopt;
  return chunk.substr(0, length_);
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_CORD_ITERATOR_SPAN_H_
