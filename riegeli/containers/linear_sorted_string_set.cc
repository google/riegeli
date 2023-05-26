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

#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/compact_string.h"
#include "riegeli/base/memory_estimator.h"
#include "riegeli/bytes/compact_string_writer.h"
#include "riegeli/bytes/string_reader.h"
#include "riegeli/varint/varint_reading.h"
#include "riegeli/varint/varint_writing.h"

namespace riegeli {

namespace {

inline size_t SharedLength(absl::string_view a, absl::string_view b) {
  const size_t min_length = UnsignedMin(a.size(), b.size());
  for (size_t length = 0; length < min_length; ++length) {
    if (a[length] != b[length]) return length;
  }
  return min_length;
}

}  // namespace

inline LinearSortedStringSet::LinearSortedStringSet(CompactString&& encoded)
    : encoded_(std::move(encoded)) {}

absl::string_view LinearSortedStringSet::first() const {
  RIEGELI_ASSERT(!empty())
      << "Failed precondition of LinearSortedStringSet::first(): "
         "empty set";
  uint64_t tagged_length;
  const absl::optional<const char*> ptr = ReadVarint64(
      encoded_.data(), encoded_.data() + encoded_.size(), tagged_length);
  RIEGELI_ASSERT(ptr != absl::nullopt)
      << "Malformed LinearSortedStringSet encoding (tagged_length)";
  RIEGELI_ASSERT_EQ(tagged_length & 1, 0u)
      << "Malformed LinearSortedStringSet encoding "
         "(first element has shared_length > 0)";
  const uint64_t length = tagged_length >> 1;
  RIEGELI_ASSERT_GE(IntCast<size_t>(encoded_.data() + encoded_.size() - *ptr),
                    length)
      << "Malformed LinearSortedStringSet encoding (unshared)";
  return absl::string_view(*ptr, IntCast<size_t>(length));
}

bool LinearSortedStringSet::contains(absl::string_view element) const {
  Iterator iterator(this);
  while (const absl::optional<absl::string_view> found = iterator.Next()) {
    if (*found >= element) return *found == element;
  }
  return false;  // Not found.
}

size_t LinearSortedStringSet::EstimateMemory() const {
  MemoryEstimator memory_estimator;
  memory_estimator.RegisterMemory(sizeof(LinearSortedStringSet));
  memory_estimator.RegisterSubobjects(*this);
  return memory_estimator.TotalMemory();
}

void LinearSortedStringSet::Builder::Reset() {
  writer_.Reset();
  last_.clear();
}

bool LinearSortedStringSet::Builder::InsertNext(absl::string_view element) {
  RIEGELI_ASSERT(writer_.is_open())
      << "Failed precondition of LinearSortedStringSet::Builder::InsertNext(): "
         "set already built or moved from";
  const size_t shared_length = SharedLength(last_, element);
  const absl::string_view unshared = element.substr(shared_length);
  if (ABSL_PREDICT_FALSE(unshared <=
                         absl::string_view(last_).substr(shared_length)) &&
      !empty()) {
    return false;  // Out of order.
  }
  last_.erase(shared_length);
  // TODO: When `absl::string_view` becomes C++17 `std::string_view`:
  // last_.append(unshared);
  last_.append(unshared.data(), unshared.size());
  RIEGELI_ASSERT_EQ(last_, element) << "last_ incorrectly reconstructed";
  // `shared_length` is stored if `shared_length > 0`.
  const uint64_t tagged_length =
      (uint64_t{unshared.size()} << 1) |
      (shared_length > 0 ? uint64_t{1} : uint64_t{0});
  WriteVarint64(tagged_length, writer_);
  if (shared_length > 0) {
    WriteVarint64(uint64_t{shared_length}, writer_);
  }
  writer_.Write(unshared);
  return true;
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

LinearSortedStringSet::Iterator::Iterator(const LinearSortedStringSet* set)
    : reader_(set->encoded_) {}

void LinearSortedStringSet::Iterator::Reset(const LinearSortedStringSet* set) {
  reader_.Reset(set->encoded_);
  current_.clear();
}

absl::optional<absl::string_view> LinearSortedStringSet::Iterator::Next() {
  uint64_t tagged_length;
  if (ABSL_PREDICT_FALSE(!ReadVarint64(reader_, tagged_length))) {
    RIEGELI_ASSERT(!reader_.Pull())
        << "Malformed LinearSortedStringSet encoding (tagged_length)";
    return absl::nullopt;
  }
  const uint64_t unshared_length = tagged_length >> 1;
  if ((tagged_length & 1) == 0) {
    // `shared_length == 0` and is not stored.
    current_.clear();
  } else {
    // `shared_length > 0` and is stored.
    uint64_t shared_length;
    if (ABSL_PREDICT_FALSE(!ReadVarint64(reader_, shared_length))) {
      RIEGELI_ASSERT_UNREACHABLE()
          << "Malformed LinearSortedStringSet encoding (shared_length)";
    }
    RIEGELI_ASSERT_LE(shared_length, current_.size())
        << "Malformed LinearSortedStringSet encoding "
           "(shared_length larger than previous element)";
    current_.erase(IntCast<size_t>(shared_length));
  }
  if (ABSL_PREDICT_FALSE(
          !reader_.ReadAndAppend(IntCast<size_t>(unshared_length), current_))) {
    RIEGELI_ASSERT_UNREACHABLE()
        << "Malformed LinearSortedStringSet encoding (unshared)";
  }
  return current_;
}

}  // namespace riegeli
