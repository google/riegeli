// Copyright 2022 Google LLC
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

#include "riegeli/bytes/buffer_options.h"

#include <stddef.h>

#include "absl/numeric/bits.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/types.h"

namespace riegeli {

namespace {

// Recommends the length of a buffer.
//
// The following constraints are applied, in the order of weakest to strongest:
//  * If `single_run` and `pos` did not reach `size_hint` yet, the remaining
//    length, otherwise the base recommendation of `length`.
//  * At least `max(min_length, recommended_length)`.
//  * At most `max_length`.
//  * Aligned so that the next position is a multiple of the length so far
//    rounded up to the nearest power of 2, but at least `min_length`.
inline size_t ApplySizeHintAndRoundPos(Position base_length, size_t min_length,
                                       size_t recommended_length,
                                       size_t max_length,
                                       absl::optional<Position> size_hint,
                                       Position pos, bool single_run) {
  RIEGELI_ASSERT_GT(min_length, 0u)
      << "Failed precondition of ApplySizeHintAndRoundPos(): zero min_length";
  RIEGELI_ASSERT_GT(max_length, 0u)
      << "Failed precondition of ApplySizeHintAndRoundPos(): zero max_length";
  if (single_run) base_length = ApplySizeHint(base_length, size_hint, pos);
  const size_t length_for_rounding = UnsignedMin(
      UnsignedMax(base_length, min_length, recommended_length), max_length);
  const size_t rounding_mask = absl::bit_ceil(length_for_rounding) - 1;
  const size_t rounded_length = (~pos & rounding_mask) + 1;
  if (rounded_length < min_length) {
    // Return at least `min_length`, keeping the same remainder modulo
    // `rounding_mask + 1` as `rounded_length`.
    return ((min_length - rounded_length + rounding_mask) & ~rounding_mask) +
           rounded_length;
  }
  return rounded_length;
}

}  // namespace

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if __cplusplus < 201703
constexpr size_t BufferOptions::kDefaultMinBufferSize;
constexpr size_t BufferOptions::kDefaultMaxBufferSize;
#endif

size_t ReadBufferSizer::BufferLength(Position pos, size_t min_length,
                                     size_t recommended_length) const {
  RIEGELI_ASSERT_GT(min_length, 0u)
      << "Failed precondition of WriteBufferSizer::BufferLength(): "
         "zero min_length";
  RIEGELI_ASSERT_GE(pos, base_pos_)
      << "Failed precondition of ReadBufferSizer::ReadBufferLength(): "
      << "position earlier than base position of the run";
  const size_t length = ApplySizeHintAndRoundPos(
      UnsignedMax(pos - base_pos_, buffer_length_from_last_run_,
                  buffer_options_.min_buffer_size()),
      min_length, recommended_length, buffer_options_.max_buffer_size(),
      exact_size(), pos, read_all_hint_);
  if (exact_size() != absl::nullopt) {
    return UnsignedMin(length, SaturatingSub(*exact_size(), pos));
  }
  return length;
}

size_t WriteBufferSizer::BufferLength(Position pos, size_t min_length,
                                      size_t recommended_length) const {
  RIEGELI_ASSERT_GT(min_length, 0u)
      << "Failed precondition of WriteBufferSizer::BufferLength(): "
         "zero min_length";
  RIEGELI_ASSERT_GE(pos, base_pos_)
      << "Failed precondition of WriteBufferSizer::WriteBufferLength(): "
      << "position earlier than base position of the run";
  const size_t length = ApplySizeHintAndRoundPos(
      UnsignedMax(pos - base_pos_, buffer_length_from_last_run_,
                  buffer_options_.min_buffer_size()),
      min_length, recommended_length, buffer_options_.max_buffer_size(),
      size_hint(), pos, buffer_length_from_last_run_ == 0);
  if (size_hint() != absl::nullopt && pos < *size_hint()) {
    return UnsignedClamp(length, min_length, *size_hint() - pos);
  }
  return length;
}

}  // namespace riegeli
