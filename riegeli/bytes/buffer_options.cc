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

#include <limits>

#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/types.h"

namespace riegeli {

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
  return BufferLengthImpl<ApplyReadSizeHint>(pos, min_length,
                                             recommended_length);
}

size_t ReadBufferSizer::LengthToReadDirectly(Position pos,
                                             size_t start_to_limit,
                                             size_t available) const {
  RIEGELI_ASSERT_GE(pos, base_pos_)
      << "Failed precondition of ReadBufferSizer::LengthToReadDirectly(): "
      << "position earlier than base position of the run";
  RIEGELI_ASSERT_LE(available, start_to_limit)
      << "Failed precondition of ReadBufferSizer::LengthToReadDirectly(): "
         "length out of range";
  // Use `ApplyWriteSizeHint` and not `ApplyReadSizeHint` because reading one
  // byte past the size hint is not needed in this context.
  const size_t length = BufferLengthImpl<ApplyWriteSizeHint>(pos, 1, 0);
  if (start_to_limit > 0) {
    // The buffer is already filled. Under the assumption that `start_to_limit`
    // is a reasonable buffer size, after appending all the data currently
    // available in the buffer to the destination, any amount of data read
    // directly to the destination leads to every other pull from the source
    // having the length of at least a reasonable buffer length.
    return UnsignedMin(length, available);
  }
  return length;
}

template <Position (*ApplySizeHint)(Position recommended_length,
                                    absl::optional<Position> size_hint,
                                    Position pos, bool multiple_runs)>
inline size_t ReadBufferSizer::BufferLengthImpl(
    Position pos, size_t min_length, size_t recommended_length) const {
  return UnsignedClamp(
      UnsignedMax(ApplySizeHint(
                      UnsignedMax(pos - base_pos_, buffer_length_from_last_run_,
                                  buffer_options_.min_buffer_size()),
                      exact_size(), pos, !read_all_hint_),
                  recommended_length),
      min_length, buffer_options_.max_buffer_size());
}

size_t WriteBufferSizer::BufferLength(Position pos, size_t min_length,
                                      size_t recommended_length) const {
  RIEGELI_ASSERT_GT(min_length, 0u)
      << "Failed precondition of WriteBufferSizer::BufferLength(): "
         "zero min_length";
  RIEGELI_ASSERT_GE(pos, base_pos_)
      << "Failed precondition of WriteBufferSizer::WriteBufferLength(): "
      << "position earlier than base position of the run";
  return UnsignedClamp(
      UnsignedMax(ApplyWriteSizeHint(
                      UnsignedMax(pos - base_pos_, buffer_length_from_last_run_,
                                  buffer_options_.min_buffer_size()),
                      size_hint(), pos, buffer_length_from_last_run_ > 0),
                  recommended_length),
      min_length, buffer_options_.max_buffer_size());
}

size_t WriteBufferSizer::LengthToWriteDirectly(Position pos,
                                               size_t start_to_limit,
                                               size_t available) const {
  RIEGELI_ASSERT_GE(pos, base_pos_)
      << "Failed precondition of WriteBufferSizer::LengthToWriteDirectly(): "
      << "position earlier than base position of the run";
  RIEGELI_ASSERT_LE(available, start_to_limit)
      << "Failed precondition of WriteBufferSizer::LengthToWriteDirectly(): "
         "length out of range";
  const size_t length = BufferLength(pos, 1, 0);
  if (start_to_limit > available) {
    // The buffer already contains some data. Under the assumption that
    // `start_to_limit` is a reasonable buffer size, if the source is at least
    // as large as the remaining space in the buffer, pushing buffered data to
    // the destination and then writing directly from the source leads to as
    // many pushes as writing through the buffer.
    return UnsignedMin(length, available);
  }
  return length;
}

}  // namespace riegeli
