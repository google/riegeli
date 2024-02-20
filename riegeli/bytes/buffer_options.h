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

#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/types.h"

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

  // Tunes the maximal buffer size, which determines how much data at a time is
  // typically read from the source / written to the destination.
  //
  // The actual buffer size changes between `min_buffer_size()` and
  // `max_buffer_size()` depending on the access pattern.
  //
  // Default: `kDefaultMaxBufferSize` (64K).
  static constexpr size_t kDefaultMaxBufferSize = size_t{64} << 10;
  BufferOptions& set_max_buffer_size(size_t max_buffer_size) & {
    RIEGELI_ASSERT_GT(max_buffer_size, 0u)
        << "Failed precondition of BufferOptions::set_max_buffer_size(): "
           "zero buffer size";
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
  Options& set_buffer_options(BufferOptions buffer_options) & {
    buffer_options_ = buffer_options;
    return static_cast<Options&>(*this);
  }
  Options&& set_buffer_options(BufferOptions buffer_options) && {
    return std::move(set_buffer_options(buffer_options));
  }
  BufferOptions& buffer_options() { return buffer_options_; }
  const BufferOptions& buffer_options() const { return buffer_options_; }

 protected:
  BufferOptionsBase(const BufferOptionsBase& that) = default;
  BufferOptionsBase& operator=(const BufferOptionsBase& that) = default;

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
// A new run may begin from a function which forces using a new buffer
// (`Reader::Seek()` or `Reader::Sync()`). The buffer length for the new run is
// optimized for the case when the new run will have a similar length to the
// previous non-empty run.
//
// The lengths aim at letting absolute positions be multiples of sufficiently
// large powers of 2. Aligned positions might make reading from the source more
// efficient. This is also important for filling the remaining part of the
// buffer if the source returned less data than asked for.
class ReadBufferSizer {
 public:
  ReadBufferSizer() = default;

  explicit ReadBufferSizer(BufferOptions buffer_options);

  ReadBufferSizer(const ReadBufferSizer& that) = default;
  ReadBufferSizer& operator=(const ReadBufferSizer& that) = default;

  ABSL_ATTRIBUTE_REINITIALIZES void Reset();
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(BufferOptions buffer_options);

  // Returns the options passed to the constructor.
  BufferOptions buffer_options() const { return buffer_options_; }

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
  //
  // `BeginRun()` may be called again without an intervening `EndRun()`.
  void BeginRun(Position pos) { base_pos_ = pos; }

  // Called at the end of a run.
  //
  // `EndRun()` may be called again without an intervening `BeginRun()`.
  //
  // Precondition:
  //   `pos >= base_pos`, where `base_pos` is the argument of the last call to
  //       `BeginRun()`, if `BeginRun()` has been called
  void EndRun(Position pos);

  // Proposed a buffer length for reading at `pos`.
  //
  // The length will not let the next position exceed `exact_size()`,
  // in particular it is 0 if `exact_size() != nullptr && pos >= *exact_size()`.
  //
  // It will be at least `min_length` unless `exact_size()` is reached,
  // preferably `recommended_length`.
  //
  // Preconditions:
  //   `min_length > 0`
  //   `pos >= base_pos`, where `base_pos` is the argument of the last call to
  //       `BeginRun()`, if `BeginRun()` has been called
  size_t BufferLength(Position pos, size_t min_length = 1,
                      size_t recommended_length = 0) const;

 private:
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
// A new run may begin from a function which forces using a new buffer (mainly
// `Writer::Seek()` or `Writer::Flush()`). The buffer length for the new run is
// optimized for the case when the new run will have a similar length to the
// previous non-empty run.
//
// The lengths aim at letting absolute positions be multiples of sufficiently
// large powers of 2. Aligned positions might make writing to the destination
// more efficient.
class WriteBufferSizer {
 public:
  WriteBufferSizer() = default;

  explicit WriteBufferSizer(BufferOptions buffer_options);

  WriteBufferSizer(const WriteBufferSizer& that) = default;
  WriteBufferSizer& operator=(const WriteBufferSizer& that) = default;

  ABSL_ATTRIBUTE_REINITIALIZES void Reset();
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(BufferOptions buffer_options);

  // Returns the options passed to the constructor.
  BufferOptions buffer_options() const { return buffer_options_; }

  // Intended storage for the hint set by
  // `{,Backward}Writer::SetWriteSizeHint()`.
  //
  // If not `absl::nullptr`, this causes larger buffer sizes to be used before
  // reaching `*size_hint()`, and a smaller buffer size to be used when reaching
  // `*size_hint()`.
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
  //
  // `BeginRun()` may be called again without an intervening `EndRun()`.
  void BeginRun(Position pos) { base_pos_ = pos; }

  // Called at the end of a run.
  //
  // `EndRun()` may be called again without an intervening `BeginRun()`.
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
#if !__cpp_inline_variables
template <typename Options>
constexpr size_t BufferOptionsBase<Options>::kDefaultMinBufferSize;
template <typename Options>
constexpr size_t BufferOptionsBase<Options>::kDefaultMaxBufferSize;
#endif

inline ReadBufferSizer::ReadBufferSizer(BufferOptions buffer_options)
    : buffer_options_(buffer_options) {}

inline void ReadBufferSizer::Reset() {
  buffer_options_ = BufferOptions();
  base_pos_ = 0;
  buffer_length_from_last_run_ = 0;
  read_all_hint_ = false;
  exact_size_ = absl::nullopt;
}

inline void ReadBufferSizer::Reset(BufferOptions buffer_options) {
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
  // Increase the length to compensate for variability of the lengths, and for
  // rounding the positions so that even after rounding the length down to a
  // power of 2 the last length is covered.
  buffer_length_from_last_run_ = SaturatingAdd(length, length - 1);
}

inline WriteBufferSizer::WriteBufferSizer(BufferOptions buffer_options)
    : buffer_options_(buffer_options) {}

inline void WriteBufferSizer::Reset() {
  buffer_options_ = BufferOptions();
  base_pos_ = 0;
  buffer_length_from_last_run_ = 0;
  size_hint_ = absl::nullopt;
}

inline void WriteBufferSizer::Reset(BufferOptions buffer_options) {
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
  // Increase the length to compensate for variability of the lengths, and for
  // rounding the positions so that even after rounding the length down to a
  // power of 2 the last length is covered.
  buffer_length_from_last_run_ = SaturatingAdd(length, length - 1);
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_BUFFER_OPTIONS_H_
