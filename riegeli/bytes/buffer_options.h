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
#include <stdint.h>

#include <utility>

#include "absl/base/attributes.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"

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
  // Default: `kDefaultMinBufferSize` (4K).
  static constexpr size_t kDefaultMinBufferSize = size_t{4} << 10;
  BufferOptions& set_min_buffer_size(size_t min_buffer_size) & {
    min_buffer_size_ = UnsignedMin(min_buffer_size, uint32_t{1} << 31);
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
  // Default: `kDefaultMaxBufferSize` (64K).
  static constexpr size_t kDefaultMaxBufferSize = size_t{64} << 10;
  BufferOptions& set_max_buffer_size(size_t max_buffer_size) & {
    max_buffer_size_ = UnsignedMin(max_buffer_size, uint32_t{1} << 31);
    return *this;
  }
  BufferOptions&& set_max_buffer_size(size_t max_buffer_size) && {
    return std::move(set_max_buffer_size(max_buffer_size));
  }
  size_t max_buffer_size() const { return max_buffer_size_; }

  // A shortcut for `set_min_buffer_size(buffer_size)` with
  // `set_max_buffer_size(buffer_size)`.
  BufferOptions& set_buffer_size(size_t buffer_size) & {
    return set_min_buffer_size(buffer_size).set_max_buffer_size(buffer_size);
  }
  BufferOptions&& set_buffer_size(size_t buffer_size) && {
    return std::move(set_buffer_size(buffer_size));
  }

 private:
  // Use `uint32_t` instead of `size_t` to reduce the object size.
  uint32_t min_buffer_size_ = uint32_t{kDefaultMinBufferSize};
  uint32_t max_buffer_size_ = uint32_t{kDefaultMaxBufferSize};
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
    set_min_buffer_size(Options::kDefaultMinBufferSize);
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
// `Reader`.
//
// The buffer length grows geometrically from `min_buffer_size` to
// `max_buffer_size` through each run of sequential reading operations.
//
// A new run may begin from `Reader::Seek()` or `Reader::Sync()`. The buffer
// length for the new run is optimized for the case when the new run will have a
// similar length to the previous non-empty run.
class ReadBufferSizer {
 public:
  ReadBufferSizer() noexcept {}

  explicit ReadBufferSizer(const BufferOptions& buffer_options);

  ReadBufferSizer(const ReadBufferSizer& that) noexcept;
  ReadBufferSizer& operator=(const ReadBufferSizer& that) noexcept;

  ABSL_ATTRIBUTE_REINITIALIZES void Reset();
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(const BufferOptions& buffer_options);

  // Returns the options passed to the constructor.
  const BufferOptions& buffer_options() const { return buffer_options_; }

  // Intended storage for the hint set by `Reader::SetReadAllHint()`.
  //
  // If `true` and `exact_size()` is not `absl::nullptr`, this causes larger
  // buffer sizes to be used before reaching `*exact_size()`.
  void set_read_all_hint(bool read_all_hint) { read_all_hint_ = read_all_hint; }
  bool read_all_hint() const { return read_all_hint_; }

  // Intended storage for an exact size of the source, as discovered by the
  // `Reader` itself.
  //
  // If not `absl::nullptr` and `read_all_hint()` is `true`, this causes larger
  // buffer sizes to be used before reaching `*exact_size()`.
  //
  // Also, if not `absl::nullptr`, this causes a smaller buffer size to be used
  // when reaching `*exact_size()`.
  void set_exact_size(absl::optional<Position> exact_size) {
    exact_size_ = exact_size;
  }
  absl::optional<Position> exact_size() const { return exact_size_; }

  // Called at the beginning of a run.
  //
  // This must be called during initialization if reading starts from a position
  // greater than 0.
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
  // Preconditions:
  //   `min_length > 0`
  //   `pos >= base_pos`, where `base_pos` is the argument of the last call to
  //       `BeginRun()`, if `BeginRun()` has been called
  size_t BufferLength(Position pos, size_t min_length = 1,
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

 private:
  template <Position (*ApplySizeHint)(Position recommended_length,
                                      absl::optional<Position> size_hint,
                                      Position pos, bool multiple_runs)>
  size_t BufferLengthImpl(Position pos, size_t min_length,
                          size_t recommended_length) const;

  BufferOptions buffer_options_;
  // Position where the current run started.
  Position base_pos_ = 0;
  // Buffer size recommended by the previous run.
  size_t buffer_length_from_last_run_ = 0;
  bool read_all_hint_ = false;
  absl::optional<Position> exact_size_;
};

// Recommends an adaptive buffer length based on the access pattern of a
// `Writer`.
//
// The buffer length grows geometrically from `min_buffer_size` to
// `max_buffer_size` through each run of sequential writing operations.
//
// A new run may begin from `Writer::Seek()`, `Writer::Flush()`, or
// `Writer::Truncate()`. The buffer length for the new run is optimized for the
// case when the new run will have a similar length to the previous non-empty
// run.
class WriteBufferSizer {
 public:
  WriteBufferSizer() noexcept {}

  explicit WriteBufferSizer(const BufferOptions& buffer_options);

  WriteBufferSizer(const WriteBufferSizer& that) noexcept;
  WriteBufferSizer& operator=(const WriteBufferSizer& that) noexcept;

  ABSL_ATTRIBUTE_REINITIALIZES void Reset();
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(const BufferOptions& buffer_options);

  // Returns the options passed to the constructor.
  const BufferOptions& buffer_options() const { return buffer_options_; }

  // Intended storage for the hint set by
  // `{,Backward}Writer::SetWriteSizeHint()`.
  //
  // If not `absl::nullptr`, this causes larger buffer sizes to be used before
  // reaching `*size_hint()`, and a smaller buffer size to be used when reaching
  // `*exact_size()`.
  void set_write_size_hint(Position pos,
                           absl::optional<Position> write_size_hint) {
    size_hint_ =
        write_size_hint == absl::nullopt
            ? absl::nullopt
            : absl::make_optional(SaturatingAdd(pos, *write_size_hint));
  }
  absl::optional<Position> size_hint() const { return size_hint_; }

  // Called at the beginning of a run.
  //
  // This must be called during initialization if writing starts from a position
  // greater than 0.
  void BeginRun(Position pos) { base_pos_ = pos; }

  // Called at the end of a run.
  //
  // Precondition:
  //   `pos >= base_pos`, where `base_pos` is the argument of the last call to
  //       `BeginRun()`, if `BeginRun()` has been called
  void EndRun(Position pos);

  // Proposed a buffer length for writing at `pos`.
  //
  // The length will be at least `min_length`, preferably `recommended_length`.
  //
  // Preconditions:
  //   `min_length > 0`
  //   `pos >= base_pos`, where `base_pos` is the argument of the last call to
  //       `BeginRun()`, if `BeginRun()` has been called
  size_t BufferLength(Position pos, size_t min_length = 1,
                      size_t recommended_length = 0) const;

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
  BufferOptions buffer_options_;
  // Position where the current run started.
  Position base_pos_ = 0;
  // Buffer size recommended by the previous run.
  size_t buffer_length_from_last_run_ = 0;
  absl::optional<Position> size_hint_;
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

inline ReadBufferSizer::ReadBufferSizer(const BufferOptions& buffer_options)
    : buffer_options_(buffer_options) {}

inline ReadBufferSizer::ReadBufferSizer(const ReadBufferSizer& that) noexcept
    : buffer_options_(that.buffer_options_),
      base_pos_(that.base_pos_),
      buffer_length_from_last_run_(that.buffer_length_from_last_run_),
      read_all_hint_(that.read_all_hint_),
      exact_size_(that.exact_size_) {}

inline ReadBufferSizer& ReadBufferSizer::operator=(
    const ReadBufferSizer& that) noexcept {
  buffer_options_ = that.buffer_options_;
  base_pos_ = that.base_pos_;
  buffer_length_from_last_run_ = that.buffer_length_from_last_run_;
  read_all_hint_ = that.read_all_hint_;
  exact_size_ = that.exact_size_;
  return *this;
}

inline void ReadBufferSizer::Reset() {
  buffer_options_ = BufferOptions();
  base_pos_ = 0;
  buffer_length_from_last_run_ = 0;
  read_all_hint_ = false;
  exact_size_ = absl::nullopt;
}

inline void ReadBufferSizer::Reset(const BufferOptions& buffer_options) {
  buffer_options_ = buffer_options;
  base_pos_ = 0;
  buffer_length_from_last_run_ = 0;
  read_all_hint_ = false;
  exact_size_ = absl::nullopt;
}

inline void ReadBufferSizer::EndRun(Position pos) {
  RIEGELI_ASSERT_GE(pos, base_pos_)
      << "Failed precondition of ReadBufferSizer::EndRun(): "
      << "position earlier than base position of the run";
  if (pos == base_pos_) return;
  const size_t length = SaturatingIntCast<size_t>(pos - base_pos_);
  buffer_length_from_last_run_ = SaturatingAdd(length, length / 4);
}

inline WriteBufferSizer::WriteBufferSizer(const BufferOptions& buffer_options)
    : buffer_options_(buffer_options) {}

inline WriteBufferSizer::WriteBufferSizer(const WriteBufferSizer& that) noexcept
    : buffer_options_(that.buffer_options_),
      base_pos_(that.base_pos_),
      buffer_length_from_last_run_(that.buffer_length_from_last_run_),
      size_hint_(that.size_hint_) {}

inline WriteBufferSizer& WriteBufferSizer::operator=(
    const WriteBufferSizer& that) noexcept {
  buffer_options_ = that.buffer_options_;
  base_pos_ = that.base_pos_;
  buffer_length_from_last_run_ = that.buffer_length_from_last_run_;
  size_hint_ = that.size_hint_;
  return *this;
}

inline void WriteBufferSizer::Reset() {
  buffer_options_ = BufferOptions();
  base_pos_ = 0;
  buffer_length_from_last_run_ = 0;
  size_hint_ = absl::nullopt;
}

inline void WriteBufferSizer::Reset(const BufferOptions& buffer_options) {
  buffer_options_ = buffer_options;
  base_pos_ = 0;
  buffer_length_from_last_run_ = 0;
  size_hint_ = absl::nullopt;
}

inline void WriteBufferSizer::EndRun(Position pos) {
  RIEGELI_ASSERT_GE(pos, base_pos_)
      << "Failed precondition of WriteBufferSizer::EndRun(): "
      << "position earlier than base position of the run";
  if (pos == base_pos_) return;
  const size_t length = SaturatingIntCast<size_t>(pos - base_pos_);
  buffer_length_from_last_run_ = SaturatingAdd(length, length / 4);
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_BUFFER_OPTIONS_H_
