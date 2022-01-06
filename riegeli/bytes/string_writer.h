// Copyright 2017 Google LLC
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

#ifndef RIEGELI_BYTES_STRING_WRITER_H_
#define RIEGELI_BYTES_STRING_WRITER_H_

#include <stddef.h>

#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

template <typename Src>
class StringReader;

// Template parameter independent part of `StringWriter`.
class StringWriterBase : public Writer {
 public:
  class Options {
   public:
    Options() noexcept {}

    // If `true`, appends to existing contents of the destination.
    //
    // If `false`, replaces existing contents of the destination, clearing it
    // first.
    //
    // Default: `false`.
    Options& set_append(bool append) & {
      append_ = append;
      return *this;
    }
    Options&& set_append(bool append) && {
      return std::move(set_append(append));
    }
    bool append() const { return append_; }

    // Expected final size, or `absl::nullopt` if unknown. This may improve
    // performance and memory usage.
    //
    // If the size hint turns out to not match reality, nothing breaks.
    //
    // Default: `absl::nullopt`.
    Options& set_size_hint(absl::optional<Position> size_hint) & {
      size_hint_ = size_hint;
      return *this;
    }
    Options&& set_size_hint(absl::optional<Position> size_hint) && {
      return std::move(set_size_hint(size_hint));
    }
    absl::optional<Position> size_hint() const { return size_hint_; }

    // Minimal size of a block of buffered data after the initial capacity of
    // the destination.
    //
    // This is used initially, while data buffered after the destination is
    // small.
    //
    // Default: `kMinBufferSize` (256).
    Options& set_min_buffer_size(size_t min_buffer_size) & {
      min_buffer_size_ = min_buffer_size;
      return *this;
    }
    Options&& set_min_buffer_size(size_t min_buffer_size) && {
      return std::move(set_min_buffer_size(min_buffer_size));
    }
    size_t min_buffer_size() const { return min_buffer_size_; }

    // Maximal size of a block of buffered data after the initial capacity of
    // the destination.
    //
    // Default: `kMaxBufferSize` (64K).
    Options& set_max_buffer_size(size_t max_buffer_size) & {
      RIEGELI_ASSERT_GT(max_buffer_size, 0u)
          << "Failed precondition of "
             "StringWriterBase::Options::set_max_buffer_size(): "
             "zero buffer size";
      max_buffer_size_ = max_buffer_size;
      return *this;
    }
    Options&& set_max_buffer_size(size_t max_buffer_size) && {
      return std::move(set_max_buffer_size(max_buffer_size));
    }
    size_t max_buffer_size() const { return max_buffer_size_; }

   private:
    bool append_ = false;
    absl::optional<Position> size_hint_;
    size_t min_buffer_size_ = kMinBufferSize;
    size_t max_buffer_size_ = kDefaultBufferSize;
  };

  // Returns the `std::string` being written to. Unchanged by `Close()`.
  virtual std::string* dest_string() = 0;
  virtual const std::string* dest_string() const = 0;

  bool SupportsSize() override { return true; }
  bool SupportsTruncate() override { return true; }
  bool SupportsReadMode() override { return true; }

 protected:
  explicit StringWriterBase(Closed) noexcept : Writer(kClosed) {}

  explicit StringWriterBase(size_t min_buffer_size, size_t max_buffer_size);

  StringWriterBase(StringWriterBase&& that) noexcept;
  StringWriterBase& operator=(StringWriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset(size_t min_buffer_size, size_t max_buffer_size);
  void Initialize(std::string* dest, bool append,
                  absl::optional<Position> size_hint);
  void MoveSecondaryBuffer(StringWriterBase&& that);
  void MoveSecondaryBufferAndBufferPointers(StringWriterBase&& that);

  void Done() override;
  bool PushSlow(size_t min_length, size_t recommended_length) override;
  using Writer::WriteSlow;
  bool WriteSlow(const Chain& src) override;
  bool WriteSlow(Chain&& src) override;
  bool WriteSlow(const absl::Cord& src) override;
  bool WriteSlow(absl::Cord&& src) override;
  bool WriteZerosSlow(Position length) override;
  bool FlushImpl(FlushType flush_type) override;
  absl::optional<Position> SizeImpl() override;
  bool TruncateImpl(Position new_size) override;
  Reader* ReadModeImpl(Position initial_pos) override;

 private:
  // Discards uninitialized space from the end of `dest`, so that it contains
  // only actual data written.
  void SyncDestBuffer(std::string& dest);

  // Appends some uninitialized space to `dest` if this can be done without
  // reallocation.
  void MakeDestBuffer(std::string& dest);

  // Discards uninitialized space from the end of `secondary_buffer_`, so that
  // it contains only actual data written.
  void SyncSecondaryBuffer();

  // Appends uninitialized space to `secondary_buffer_`.
  void MakeSecondaryBuffer(size_t min_length = 0,
                           size_t recommended_length = 0);

  Chain::Options options_;
  // Buffered data which did not fit under `dest_string()->capacity()`.
  Chain secondary_buffer_;

  AssociatedReader<StringReader<absl::string_view>> associated_reader_;

  // Invariants if `healthy()`:
  //   `(start() == &(*dest_string())[0] &&
  //     start_to_limit() == dest_string()->size() &&
  //     secondary_buffer_.empty()) ||
  //    limit() == nullptr ||
  //    limit() == secondary_buffer_.blocks().back().data() +
  //               secondary_buffer_.blocks().back().size()`
  //   `limit_pos() == dest_string()->size() + secondary_buffer_.size()`
};

// A `Writer` which appends to a `std::string`, resizing it as necessary.
//
// It supports `ReadMode()`.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the `std::string` being written to. `Dest` must support
// `Dependency<std::string*, Dest>`, e.g. `std::string*` (not owned, default),
// `std::string` (owned).
//
// By relying on CTAD the template argument can be deduced as `std::string`
// if there are no constructor arguments or the only argument is `Options`,
// otherwise as the value type of the first constructor argument, except that
// CTAD is deleted if the first constructor argument is a `std::string&` or
// `const std::string&` (to avoid writing to an unintentionally separate copy of
// an existing object). This requires C++17.
//
// The `std::string` must not be accessed until the `StringWriter` is closed or
// no longer used, except that it is allowed to read the `std::string`
// immediately after `Flush()`.
template <typename Dest = std::string*>
class StringWriter : public StringWriterBase {
 public:
  // Creates a closed `StringWriter`.
  explicit StringWriter(Closed) noexcept : StringWriterBase(kClosed) {}

  // Will append to an owned `std::string` which can be accessed by `dest()`.
  // This constructor is present only if `Dest` is `std::string`.
  template <typename DependentDest = Dest,
            std::enable_if_t<std::is_same<DependentDest, std::string>::value,
                             int> = 0>
  explicit StringWriter(Options options = Options());

  // Will append to the `std::string` provided by `dest`.
  explicit StringWriter(const Dest& dest, Options options = Options());
  explicit StringWriter(Dest&& dest, Options options = Options());

  // Will append to the `std::string` provided by a `Dest` constructed from
  // elements of `dest_args`. This avoids constructing a temporary `Dest` and
  // moving from it.
  template <typename... DestArgs>
  explicit StringWriter(std::tuple<DestArgs...> dest_args,
                        Options options = Options());

  StringWriter(StringWriter&& that) noexcept;
  StringWriter& operator=(StringWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `StringWriter`. This avoids
  // constructing a temporary `StringWriter` and moving from it.
  void Reset(Closed);
  template <typename DependentDest = Dest,
            std::enable_if_t<std::is_same<DependentDest, std::string>::value,
                             int> = 0>
  void Reset(Options options = Options());
  void Reset(const Dest& dest, Options options = Options());
  void Reset(Dest&& dest, Options options = Options());
  template <typename... DestArgs>
  void Reset(std::tuple<DestArgs...> dest_args, Options options = Options());

  // Returns the object providing and possibly owning the `std::string` being
  // written to. Unchanged by `Close()`.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  std::string* dest_string() override { return dest_.get(); }
  const std::string* dest_string() const override { return dest_.get(); }

 private:
  void MoveDestAndSecondaryBuffer(StringWriter&& that);

  // The object providing and possibly owning the `std::string` being written
  // to, with uninitialized space appended (possibly empty); `cursor()` points
  // to the uninitialized space.
  Dependency<std::string*, Dest> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit StringWriter(Closed)->StringWriter<DeleteCtad<Closed>>;
explicit StringWriter(
    StringWriterBase::Options options = StringWriterBase::Options())
    ->StringWriter<std::string>;
template <typename Dest>
explicit StringWriter(const Dest& dest, StringWriterBase::Options options =
                                            StringWriterBase::Options())
    -> StringWriter<std::conditional_t<
        std::is_convertible<const Dest*, const std::string*>::value,
        DeleteCtad<const Dest&>, std::decay_t<Dest>>>;
template <typename Dest>
explicit StringWriter(Dest&& dest, StringWriterBase::Options options =
                                       StringWriterBase::Options())
    -> StringWriter<std::conditional_t<
        std::is_lvalue_reference<Dest>::value &&
            std::is_convertible<std::remove_reference_t<Dest>*,
                                const std::string*>::value,
        DeleteCtad<Dest&&>, std::decay_t<Dest>>>;
template <typename... DestArgs>
explicit StringWriter(
    std::tuple<DestArgs...> dest_args,
    StringWriterBase::Options options = StringWriterBase::Options())
    -> StringWriter<DeleteCtad<std::tuple<DestArgs...>>>;
#endif

// Implementation details follow.

inline StringWriterBase::StringWriterBase(size_t min_buffer_size,
                                          size_t max_buffer_size)
    : options_(Chain::Options()
                   .set_min_block_size(min_buffer_size)
                   .set_max_block_size(max_buffer_size)) {}

inline StringWriterBase::StringWriterBase(StringWriterBase&& that) noexcept
    : Writer(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      options_(that.options_),
      associated_reader_(std::move(that.associated_reader_)) {}

inline StringWriterBase& StringWriterBase::operator=(
    StringWriterBase&& that) noexcept {
  Writer::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class
  // part was moved.
  options_ = that.options_;
  associated_reader_ = std::move(that.associated_reader_);
  return *this;
}

inline void StringWriterBase::Reset(Closed) {
  Writer::Reset(kClosed);
  options_ = Chain::Options();
  secondary_buffer_ = Chain();
  associated_reader_.Reset();
}

inline void StringWriterBase::Reset(size_t min_buffer_size,
                                    size_t max_buffer_size) {
  Writer::Reset();
  options_ = Chain::Options()
                 .set_min_block_size(min_buffer_size)
                 .set_max_block_size(max_buffer_size);
  secondary_buffer_.Clear();
  associated_reader_.Reset();
}

inline void StringWriterBase::Initialize(std::string* dest, bool append,
                                         absl::optional<Position> size_hint) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of StringWriter: null string pointer";
  if (!append) dest->clear();
  if (size_hint != absl::nullopt) {
    const size_t adjusted_size_hint = UnsignedMin(*size_hint, dest->max_size());
    if (dest->capacity() < adjusted_size_hint) {
      dest->reserve(adjusted_size_hint);
    }
  }
  MakeDestBuffer(*dest);
}

inline void StringWriterBase::MoveSecondaryBuffer(StringWriterBase&& that) {
  secondary_buffer_ = std::move(that.secondary_buffer_);
}

inline void StringWriterBase::MoveSecondaryBufferAndBufferPointers(
    StringWriterBase&& that) {
  const size_t buffer_size = start_to_limit();
  const size_t cursor_index = start_to_cursor();
  secondary_buffer_ = std::move(that.secondary_buffer_);
  set_buffer(const_cast<char*>(secondary_buffer_.blocks().back().data() +
                               secondary_buffer_.blocks().back().size()) -
                 buffer_size,
             buffer_size, cursor_index);
}

inline void StringWriterBase::MakeDestBuffer(std::string& dest) {
  const size_t cursor_index = dest.size();
  dest.resize(dest.capacity());
  set_buffer(&dest[0], dest.size(), cursor_index);
}

template <typename Dest>
template <
    typename DependentDest,
    std::enable_if_t<std::is_same<DependentDest, std::string>::value, int>>
inline StringWriter<Dest>::StringWriter(Options options)
    : StringWriter(std::forward_as_tuple(), std::move(options)) {}

template <typename Dest>
inline StringWriter<Dest>::StringWriter(const Dest& dest, Options options)
    : StringWriterBase(options.min_buffer_size(), options.max_buffer_size()),
      dest_(dest) {
  Initialize(dest_.get(), options.append(), options.size_hint());
}

template <typename Dest>
inline StringWriter<Dest>::StringWriter(Dest&& dest, Options options)
    : StringWriterBase(options.min_buffer_size(), options.max_buffer_size()),
      dest_(std::move(dest)) {
  Initialize(dest_.get(), options.append(), options.size_hint());
}

template <typename Dest>
template <typename... DestArgs>
inline StringWriter<Dest>::StringWriter(std::tuple<DestArgs...> dest_args,
                                        Options options)
    : StringWriterBase(options.min_buffer_size(), options.max_buffer_size()),
      dest_(std::move(dest_args)) {
  Initialize(dest_.get(), options.append(), options.size_hint());
}

template <typename Dest>
inline StringWriter<Dest>::StringWriter(StringWriter&& that) noexcept
    : StringWriterBase(std::move(that)) {
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  MoveDestAndSecondaryBuffer(std::move(that));
}

template <typename Dest>
inline StringWriter<Dest>& StringWriter<Dest>::operator=(
    StringWriter&& that) noexcept {
  StringWriterBase::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  MoveDestAndSecondaryBuffer(std::move(that));
  return *this;
}

template <typename Dest>
inline void StringWriter<Dest>::Reset(Closed) {
  StringWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
template <
    typename DependentDest,
    std::enable_if_t<std::is_same<DependentDest, std::string>::value, int>>
inline void StringWriter<Dest>::Reset(Options options) {
  Reset(std::forward_as_tuple(), std::move(options));
}

template <typename Dest>
inline void StringWriter<Dest>::Reset(const Dest& dest, Options options) {
  StringWriterBase::Reset(options.min_buffer_size(), options.max_buffer_size());
  dest_.Reset(dest);
  Initialize(dest_.get(), options.append(), options.size_hint());
}

template <typename Dest>
inline void StringWriter<Dest>::Reset(Dest&& dest, Options options) {
  StringWriterBase::Reset(options.min_buffer_size(), options.max_buffer_size());
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), options.append(), options.size_hint());
}

template <typename Dest>
template <typename... DestArgs>
inline void StringWriter<Dest>::Reset(std::tuple<DestArgs...> dest_args,
                                      Options options) {
  StringWriterBase::Reset(options.min_buffer_size(), options.max_buffer_size());
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get(), options.append(), options.size_hint());
}

template <typename Dest>
inline void StringWriter<Dest>::MoveDestAndSecondaryBuffer(
    StringWriter&& that) {
  if (start() == nullptr) {
    dest_ = std::move(that.dest_);
    MoveSecondaryBuffer(std::move(that));
  } else if (start() == &(*that.dest_)[0]) {
    if (dest_.kIsStable()) {
      dest_ = std::move(that.dest_);
    } else {
      const size_t cursor_index = start_to_cursor();
      dest_ = std::move(that.dest_);
      set_buffer(&(*dest_)[0], dest_->size(), cursor_index);
    }
    MoveSecondaryBuffer(std::move(that));
  } else {
    dest_ = std::move(that.dest_);
    MoveSecondaryBufferAndBufferPointers(std::move(that));
  }
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_STRING_WRITER_H_
