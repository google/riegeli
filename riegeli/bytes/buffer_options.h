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

#ifndef RIEGELI_BYTES_BUFFER_OPTIONS_H_
#define RIEGELI_BYTES_BUFFER_OPTIONS_H_

#include <stddef.h>

#include <type_traits>
#include <utility>

#include "absl/flags/declare.h"
#include "absl/flags/flag.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"

ABSL_DECLARE_FLAG(bool, riegeli_use_adaptive_buffer_sizes);

namespace riegeli {

// Common options related to buffering in a `Reader` or `Writer`.
class BufferOptions {
 public:
  BufferOptions() noexcept {}

  // Tunes the minimal buffer size, which determines how much data at a time is
  // typically read from the source / written to the destination.
  //
  // The actual buffer size changes between `min_buffer_size()` and
  // `max_buffer_size()` depending on the access pattern.
  //
  // Preconditions:
  //   `min_buffer_size() > 0`
  //   `min_buffer_size() <= max_buffer_size()`
  //
  // Default: `kDefaultMinBufferSize` (4K).
  static constexpr size_t kDefaultMinBufferSize = size_t{4} << 10;
  BufferOptions& set_min_buffer_size(size_t min_buffer_size) & {
    RIEGELI_ASSERT_GT(min_buffer_size, 0u)
        << "Failed precondition of BufferOptions::set_min_buffer_size(): "
           "zero buffer size";
    min_buffer_size_ = min_buffer_size;
    return *this;
  }
  BufferOptions&& set_min_buffer_size(size_t min_buffer_size) && {
    return std::move(set_min_buffer_size(min_buffer_size));
  }
  size_t min_buffer_size() const { return min_buffer_size_; }

  // Tunes the minimal buffer size, which determines how much data at a time is
  // typically read from the source / written to the destination.
  //
  // The actual buffer size changes between `min_buffer_size()` and
  // `max_buffer_size()` depending on the access pattern.
  //
  // Precondition: `min_buffer_size() <= max_buffer_size()`
  //
  // Default: `kDefaultMaxBufferSize` (64K).
  static constexpr size_t kDefaultMaxBufferSize = size_t{64} << 10;
  BufferOptions& set_max_buffer_size(size_t max_buffer_size) & {
    RIEGELI_ASSERT_GT(max_buffer_size, 0u)
        << "Failed precondition of BufferOptions::set_max_buffer_size(): "
           "zero buffer size";
    max_buffer_size_ = max_buffer_size;
    return *this;
  }
  BufferOptions&& set_max_buffer_size(size_t max_buffer_size) && {
    return std::move(set_max_buffer_size(max_buffer_size));
  }
  size_t max_buffer_size() const { return max_buffer_size_; }

  // A shortcut for `set_min_buffer_size(buffer_size)` with
  // `set_max_buffer_size(buffer_size)`.
  BufferOptions& set_buffer_size(size_t buffer_size) & {
    RIEGELI_ASSERT_GT(buffer_size, 0u)
        << "Failed precondition of BufferOptions::set_buffer_size(): "
           "zero buffer size";
    return set_min_buffer_size(buffer_size).set_max_buffer_size(buffer_size);
  }
  BufferOptions&& set_buffer_size(size_t buffer_size) && {
    return std::move(set_buffer_size(buffer_size));
  }

  // Expected maximum position reached, or `absl::nullopt` if unknown. This may
  // improve performance and memory usage.
  //
  // If the size hint turns out to not match reality, nothing breaks.
  //
  // Default: `absl::nullopt`.
  BufferOptions& set_size_hint(absl::optional<Position> size_hint) & {
    size_hint_ = size_hint;
    return *this;
  }
  BufferOptions&& set_size_hint(absl::optional<Position> size_hint) && {
    return std::move(set_size_hint(size_hint));
  }
  absl::optional<Position> size_hint() const { return size_hint_; }

 private:
  size_t min_buffer_size_ = kDefaultMinBufferSize;
  size_t max_buffer_size_ = kDefaultMaxBufferSize;
  absl::optional<Position> size_hint_;
};

// Deriving `Options` from `BufferOptionsBase<Options>` makes it easier to
// provide options related to buffering among `Options` (deriving from
// `BufferOptions` would yield wrong result types of setters).
//
// Default values of `{min,max}_buffer_size()` can be overridden by
// `Options::kDefault{Min,Max}BufferSize`.
template <typename Options>
class BufferOptionsBase {
 public:
  BufferOptionsBase() noexcept {
    static_assert(std::is_base_of<BufferOptionsBase<Options>, Options>::value,
                  "The template argument Options in BufferOptionsBase<Options> "
                  "must be the class derived from BufferOptionsBase<Options>");
    set_min_buffer_size(absl::GetFlag(FLAGS_riegeli_use_adaptive_buffer_sizes)
                            ? Options::kDefaultMinBufferSize
                            : Options::kDefaultMaxBufferSize);
    set_max_buffer_size(Options::kDefaultMaxBufferSize);
  }

  // See `BufferOptions::set_min_buffer_size()`.
  static constexpr size_t kDefaultMinBufferSize =
      BufferOptions::kDefaultMinBufferSize;
  Options& set_min_buffer_size(size_t min_buffer_size) & {
    buffer_options_.set_min_buffer_size(min_buffer_size);
    return static_cast<Options&>(*this);
  }
  Options&& set_min_buffer_size(size_t min_buffer_size) && {
    return std::move(set_min_buffer_size(min_buffer_size));
  }
  size_t min_buffer_size() const { return buffer_options_.min_buffer_size(); }

  // See `BufferOptions::set_max_buffer_size()`.
  static constexpr size_t kDefaultMaxBufferSize =
      BufferOptions::kDefaultMaxBufferSize;
  Options& set_max_buffer_size(size_t max_buffer_size) & {
    buffer_options_.set_max_buffer_size(max_buffer_size);
    return static_cast<Options&>(*this);
  }
  Options&& set_max_buffer_size(size_t max_buffer_size) && {
    return std::move(set_max_buffer_size(max_buffer_size));
  }
  size_t max_buffer_size() const { return buffer_options_.max_buffer_size(); }

  // See `BufferOptions::set_buffer_size()`.
  Options& set_buffer_size(size_t buffer_size) & {
    buffer_options_.set_buffer_size(buffer_size);
    return static_cast<Options&>(*this);
  }
  Options&& set_buffer_size(size_t buffer_size) && {
    return std::move(set_buffer_size(buffer_size));
  }

  // See `BufferOptions::set_size_hint()`.
  Options& set_size_hint(absl::optional<Position> size_hint) & {
    buffer_options_.set_size_hint(size_hint);
    return static_cast<Options&>(*this);
  }
  Options&& set_size_hint(absl::optional<Position> size_hint) && {
    return std::move(set_size_hint(size_hint));
  }
  absl::optional<Position> size_hint() const {
    return buffer_options_.size_hint();
  }

  // Grouped options related to buffering.
  Options& set_buffer_options(const BufferOptions& buffer_options) & {
    buffer_options_ = buffer_options;
    return static_cast<Options&>(*this);
  }
  Options&& set_buffer_options(const BufferOptions& buffer_options) && {
    return std::move(set_buffer_options(buffer_options));
  }
  BufferOptions& buffer_options() { return buffer_options_; }
  const BufferOptions& buffer_options() const { return buffer_options_; }

 protected:
  ~BufferOptionsBase() = default;

 private:
  BufferOptions buffer_options_;
};

// Recommends an adaptive buffer length based on the access pattern of a
// `Reader` or `Writer`.
//
// The buffer length grows geometrically from `min_buffer_size` to
// `max_buffer_size` through each run of sequential reading or writing
// operations.
//
// A new run may begin from `{Reader,Writer}::Seek()`, `Reader::Sync()`,
// `Writer::Flush()`, or `Writer::Truncate()`. The buffer length for the new run
// is optimized for the case when the new run will have a similar length to the
// previous non-empty run.
//
// If `size_hint` is given, it may improve performance and memory usage by
// additional tuning of the buffer size depending on how far from the current
// position is the size hint, and whether there are multiple runs.
class BufferSizer {
 public:
  BufferSizer() noexcept {}

  explicit BufferSizer(const BufferOptions& buffer_options);

  BufferSizer(const BufferSizer& that) noexcept;
  BufferSizer& operator=(const BufferSizer& that) noexcept;

  void Reset();
  void Reset(const BufferOptions& buffer_options);

  // Returns the options passed to the constructor.
  const BufferOptions& buffer_options() const { return buffer_options_; }

  // Provides access to `size_hint()` after construction.
  void set_size_hint(absl::optional<Position> size) {
    buffer_options_.set_size_hint(size);
  }
  absl::optional<Position> size_hint() const {
    return buffer_options_.size_hint();
  }

  // Called at the beginning of a run.
  //
  // This must be called during initialization if reading or writing starts from
  // a position greater than 0.
  void BeginRun(Position pos) { base_pos_ = pos; }

  // Called at the end of a run.
  //
  // Precondition:
  //   `pos >= base_pos`, where `base_pos` is the argument of the last call to
  //       `BeginRun()`, if `BeginRun()` has been called
  void EndRun(Position pos);

  // Proposed a buffer length for reading at `pos`.
  //
  // The length will be at least `min_length`, preferably `recommended_length`.
  //
  // Precondition:
  //   `pos >= base_pos`, where `base_pos` is the argument of the last call to
  //       `BeginRun()`, if `BeginRun()` has been called
  size_t ReadBufferLength(Position pos, size_t min_length = 1,
                          size_t recommended_length = 0) const;

  // Proposed a buffer length for writing at `pos`.
  //
  // The length will be at least `min_length`, preferably `recommended_length`.
  //
  // Precondition:
  //   `pos >= base_pos`, where `base_pos` is the argument of the last call to
  //       `BeginRun()`, if `BeginRun()` has been called
  size_t WriteBufferLength(Position pos, size_t min_length = 1,
                           size_t recommended_length = 0) const;

  // Minimum read length, for which it is better to append the data currently
  // available in the buffer to the destination and then read directly to the
  // destination, than to read through the buffer.
  //
  // Reading directly is preferred if at least every other pull from the source
  // has the length of at least a reasonable buffer size, or if the number of
  // pulls is at most as large as if this was true.
  //
  // `pos` is the current position. `start_to_limit` is the current size of the
  // buffer. `available` is the length of the final part of the buffer, with
  // data pulled from the source but not yet returned to the callers.
  //
  // Preconditions:
  //   `pos >= base_pos`, where `base_pos` is the argument of the last call to
  //       `BeginRun()`, if `BeginRun()` has been called
  //   `available <= start_to_limit`
  size_t LengthToReadDirectly(Position pos, size_t start_to_limit,
                              size_t available) const;

  // Minimum write length, for which it is better to push buffered data from the
  // buffer to the destination and then write directly from the source, than to
  // write through the buffer.
  //
  // Writing directly is preferred if at least every other push to the
  // destination has the length of at least a reasonable buffer size, or if the
  // number of pushes is at most a large as if this was true.
  //
  // `pos` is the current position. `start_to_limit` is the current size of the
  // buffer. `available` is the length of the final part of the buffer, with
  // space not yet filled by the callers.
  //
  // Preconditions:
  //   `pos >= base_pos`, where `base_pos` is the argument of the last call to
  //       `BeginRun()`, if `BeginRun()` has been called
  //   `available <= start_to_limit`
  size_t LengthToWriteDirectly(Position pos, size_t start_to_limit,
                               size_t available) const;

 private:
  template <Position (*ApplySizeHint)(Position recommended_length,
                                      absl::optional<Position> size_hint,
                                      Position pos, bool multiple_runs)>
  size_t BufferLengthImpl(Position pos, size_t min_length,
                          size_t recommended_length, bool multiple_runs) const;

  BufferOptions buffer_options_;
  // Position where the current run started.
  Position base_pos_ = 0;
  // Buffer size recommended by the previous run.
  size_t buffer_length_from_last_run_ = 0;
};

// Implementation details follow.

// Before C++17 if a constexpr static data member is ODR-used, its definition at
// namespace scope is required. Since C++17 these definitions are deprecated:
// http://en.cppreference.com/w/cpp/language/static
#if __cplusplus < 201703
template <typename Options>
constexpr size_t BufferOptionsBase<Options>::kDefaultMinBufferSize;
template <typename Options>
constexpr size_t BufferOptionsBase<Options>::kDefaultMaxBufferSize;
#endif

inline BufferSizer::BufferSizer(const BufferOptions& buffer_options)
    : buffer_options_(buffer_options) {
  RIEGELI_ASSERT_LE(buffer_options.min_buffer_size(),
                    buffer_options.max_buffer_size())
      << "Failed precondition of BufferSizer: "
         "BufferOptions::min_buffer_size() > BufferOptions::max_buffer_size()";
}

inline BufferSizer::BufferSizer(const BufferSizer& that) noexcept
    : buffer_options_(that.buffer_options_),
      base_pos_(that.base_pos_),
      buffer_length_from_last_run_(that.buffer_length_from_last_run_) {}

inline BufferSizer& BufferSizer::operator=(const BufferSizer& that) noexcept {
  buffer_options_ = that.buffer_options_;
  base_pos_ = that.base_pos_;
  buffer_length_from_last_run_ = that.buffer_length_from_last_run_;
  return *this;
}

inline void BufferSizer::Reset() {
  buffer_options_ = BufferOptions();
  base_pos_ = 0;
  buffer_length_from_last_run_ = 0;
}

inline void BufferSizer::Reset(const BufferOptions& buffer_options) {
  RIEGELI_ASSERT_LE(buffer_options.min_buffer_size(),
                    buffer_options.max_buffer_size())
      << "Failed precondition of BufferSizer: "
         "BufferOptions::min_buffer_size() > BufferOptions::max_buffer_size()";
  buffer_options_ = buffer_options;
  base_pos_ = 0;
  buffer_length_from_last_run_ = 0;
}

inline void BufferSizer::EndRun(Position pos) {
  RIEGELI_ASSERT_GE(pos, base_pos_)
      << "Failed precondition of BufferSizer::EndRun(): "
      << "position earlier than base position of the run";
  if (pos == base_pos_) return;
  const size_t length = SaturatingIntCast<size_t>(pos - base_pos_);
  buffer_length_from_last_run_ = SaturatingAdd(length, length / 4);
}

inline size_t BufferSizer::ReadBufferLength(Position pos, size_t min_length,
                                            size_t recommended_length) const {
  RIEGELI_ASSERT_GE(pos, base_pos_)
      << "Failed precondition of BufferSizer::ReadBufferLength(): "
      << "position earlier than base position of the run";
  return BufferLengthImpl<ApplyReadSizeHint>(pos, min_length,
                                             recommended_length, true);
}

inline size_t BufferSizer::WriteBufferLength(Position pos, size_t min_length,
                                             size_t recommended_length) const {
  RIEGELI_ASSERT_GE(pos, base_pos_)
      << "Failed precondition of BufferSizer::WriteBufferLength(): "
      << "position earlier than base position of the run";
  return BufferLengthImpl<ApplyWriteSizeHint>(
      pos, min_length, recommended_length, buffer_length_from_last_run_ > 0);
}

inline size_t BufferSizer::LengthToReadDirectly(Position pos,
                                                size_t start_to_limit,
                                                size_t available) const {
  RIEGELI_ASSERT_GE(pos, base_pos_)
      << "Failed precondition of BufferSizer::LengthToReadDirectly(): "
      << "position earlier than base position of the run";
  RIEGELI_ASSERT_LE(available, start_to_limit)
      << "Failed precondition of BufferSizer::LengthToReadDirectly(): "
         "length out of range";
  // Use `ApplyWriteSizeHint` and not `ApplyReadSizeHint` because reading one
  // byte past the size hint is not needed in this context.
  const size_t length = BufferLengthImpl<ApplyWriteSizeHint>(pos, 1, 0, true);
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

inline size_t BufferSizer::LengthToWriteDirectly(Position pos,
                                                 size_t start_to_limit,
                                                 size_t available) const {
  RIEGELI_ASSERT_GE(pos, base_pos_)
      << "Failed precondition of BufferSizer::LengthToWriteDirectly(): "
      << "position earlier than base position of the run";
  RIEGELI_ASSERT_LE(available, start_to_limit)
      << "Failed precondition of BufferSizer::LengthToWriteDirectly(): "
         "length out of range";
  const size_t length = BufferLengthImpl<ApplyWriteSizeHint>(
      pos, 1, 0, buffer_length_from_last_run_ > 0);
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

template <Position (*ApplySizeHint)(Position recommended_length,
                                    absl::optional<Position> size_hint,
                                    Position pos, bool multiple_runs)>
inline size_t BufferSizer::BufferLengthImpl(Position pos, size_t min_length,
                                            size_t recommended_length,
                                            bool multiple_runs) const {
  return UnsignedClamp(
      UnsignedMax(ApplySizeHint(
                      UnsignedMax(pos - base_pos_, buffer_length_from_last_run_,
                                  buffer_options_.min_buffer_size()),
                      buffer_options_.size_hint(), pos, multiple_runs),
                  recommended_length),
      min_length, buffer_options_.max_buffer_size());
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_BUFFER_OPTIONS_H_
