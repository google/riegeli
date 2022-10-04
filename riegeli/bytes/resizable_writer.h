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

#ifndef RIEGELI_BYTES_RESIZABLE_WRITER_H_
#define RIEGELI_BYTES_RESIZABLE_WRITER_H_

#include <stddef.h>
#include <stdint.h>

#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

class Reader;
template <typename Src>
class StringReader;

// Template parameter independent part of `ResizableWriter`.
class ResizableWriterBase : public Writer {
 public:
  class Options {
   public:
    Options() noexcept {}

    // If `false`, replaces existing contents of the destination, clearing it
    // first.
    //
    // If `true`, appends to existing contents of the destination.
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

    // Minimal size of a block of buffered data after the initial capacity of
    // the destination.
    //
    // This is used initially, while data buffered after the destination is
    // small.
    //
    // Default: `kDefaultMinBlockSize` (256).
    Options& set_min_buffer_size(size_t min_buffer_size) & {
      min_buffer_size_ = UnsignedMin(min_buffer_size, uint32_t{1} << 31);
      return *this;
    }
    Options&& set_min_buffer_size(size_t min_buffer_size) && {
      return std::move(set_min_buffer_size(min_buffer_size));
    }
    size_t min_buffer_size() const { return min_buffer_size_; }

    // Maximal size of a block of buffered data after the initial capacity of
    // the destination.
    //
    // Default: `kDefaultMaxBlockSize` (64K).
    Options& set_max_buffer_size(size_t max_buffer_size) & {
      RIEGELI_ASSERT_GT(max_buffer_size, 0u)
          << "Failed precondition of "
             "ResizableWriterBase::Options::set_max_buffer_size(): "
             "zero buffer size";
      max_buffer_size_ = UnsignedMin(max_buffer_size, uint32_t{1} << 31);
      return *this;
    }
    Options&& set_max_buffer_size(size_t max_buffer_size) && {
      return std::move(set_max_buffer_size(max_buffer_size));
    }
    size_t max_buffer_size() const { return max_buffer_size_; }

   private:
    bool append_ = false;
    // Use `uint32_t` instead of `size_t` to reduce the object size.
    uint32_t min_buffer_size_ = uint32_t{kDefaultMinBlockSize};
    uint32_t max_buffer_size_ = uint32_t{kDefaultMaxBlockSize};
  };

  bool SupportsTruncate() override { return true; }
  bool SupportsReadMode() override { return true; }

 protected:
  explicit ResizableWriterBase(Closed) noexcept : Writer(kClosed) {}

  explicit ResizableWriterBase(size_t min_buffer_size, size_t max_buffer_size);

  ResizableWriterBase(ResizableWriterBase&& that) noexcept;
  ResizableWriterBase& operator=(ResizableWriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset(size_t min_buffer_size, size_t max_buffer_size);
  bool uses_secondary_buffer() const { return !secondary_buffer_.empty(); }
  size_t SecondaryBufferSize() const { return secondary_buffer_.size(); }
  void MoveSecondaryBuffer(ResizableWriterBase&& that);
  void MoveSecondaryBufferAndBufferPointers(ResizableWriterBase&& that);

  // Sets the size of the destination to `pos()`. Sets buffer pointers to the
  // destination.
  //
  // Precondition: if `uses_secondary_buffer()` then `available() == 0`
  virtual bool ResizeDest() = 0;

  // Sets buffer pointers to the destination.
  //
  // Precondition: `!uses_secondary_buffer()`
  virtual void MakeDestBuffer() = 0;

  // Appends some uninitialized space to the destination if this can be done
  // without reallocation. Sets buffer pointers to the destination.
  //
  // Precondition: `!uses_secondary_buffer()`
  virtual void GrowDestToCapacityAndMakeBuffer() = 0;

  // Appends some uninitialized space to the destination to guarantee at least
  // `new_size` of size. Sets buffer pointers to the destination.
  //
  // Precondition: if `uses_secondary_buffer()` then `available() == 0`
  virtual bool GrowDestAndMakeBuffer(size_t new_size) = 0;

  void Done() override;
  void SetWriteSizeHintImpl(absl::optional<Position> write_size_hint) override;
  bool PushSlow(size_t min_length, size_t recommended_length) override;
  using Writer::WriteSlow;
  bool WriteSlow(const Chain& src) override;
  bool WriteSlow(Chain&& src) override;
  bool WriteSlow(const absl::Cord& src) override;
  bool WriteSlow(absl::Cord&& src) override;
  bool WriteZerosSlow(Position length) override;
  bool FlushImpl(FlushType flush_type) override;
  bool TruncateImpl(Position new_size) override;
  Reader* ReadModeImpl(Position initial_pos) override;

 private:
  // Discards uninitialized space from the end of `secondary_buffer_`, so that
  // it contains only actual data written.
  void SyncSecondaryBuffer();

  // Appends uninitialized space to `secondary_buffer_`.
  void MakeSecondaryBuffer(size_t min_length = 1,
                           size_t recommended_length = 0);

  Chain::Options options_;
  // Buffered data which did not fit into the destination.
  Chain secondary_buffer_;

  AssociatedReader<StringReader<absl::string_view>> associated_reader_;

  // If `!uses_secondary_buffer()`, then the destination contains the data
  // followed by `available()` free space.
  //
  // If `uses_secondary_buffer()`, then the destination contains some prefix of
  // the data followed by some free space, and `secondary_buffer_` contains the
  // rest of the data followed by `available()` free space.
  //
  // Invariants if `ok()` (`dest_` is defined in `ResizableWriter`):
  //   `(!uses_secondary_buffer() &&
  //     start_pos() == 0 &&
  //     start() == ResizableTraits::Data(*dest_) &&
  //     start_to_limit() == ResizableTraits::Size(*dest_)) ||
  //    (uses_secondary_buffer() &&
  //     limit() == secondary_buffer_.blocks().back().data() +
  //                secondary_buffer_.blocks().back().size()) ||
  //    start() == nullptr`
  //   `limit_pos() >= secondary_buffer_.size()`
  //   `limit_pos() <= ResizableTraits::Size(*dest_) + secondary_buffer_.size()`
  //   if `!uses_secondary_buffer()` then
  //       `limit_pos() == ResizableTraits::Size(*dest_)`
};

// A `Writer` which appends to a resizable array, resizing it as necessary.
// It generalizes `StringWriter` to other objects with a flat representation.
//
// It supports `ReadMode()`.
//
// The `ResizableTraits` template parameter specifies how the resizable is
// represented. It should contain at least the following static members:
//
// ```
//   // The type of the resizable. It should be movable if
//   // `!Dependency<Resizable*, Dest>::kIsStable` and the `ResizableWriter`
//   // itself is being moved.
//   using Resizable = ...;
//
//   // Returns the current data pointer.
//   static char* Data(Resizable& dest);
//
//   // Returns the current size.
//   static size_t Size(const Resizable& dest);
//
//   // If `true`, `Data(dest)` stays unchanged when a `Resizable` is moved.
//   // `kIsStable` does not have to be defined, except if
//   // `!Dependency<Resizable*, Dest>::kIsStable` and the `ResizableWriter`
//   // itself is being moved.
//   static constexpr bool kIsStable;
//
//   // Sets the size of `dest` to `new_size`. The prefix of data with
//   // `used_size` is preserved. Remaining space is unspecified. Returns
//   // `true` on success, or `false` on failure.
//   //
//   // Preconditions:
//   //   `used_size <= Size(dest)`
//   //   `used_size <= new_size`
//   static bool Resize(Resizable& dest, size_t new_size, size_t used_size);
//
//   // Increases the size of `dest` if this can be done without reallocation
//   // and without invalidating existing data. New space is unspecified.
//   static void GrowToCapacity(Resizable& dest);
//
//   // Increases the size of `dest` at least to `new_size`, or more to ensure
//   // amortized constant time of reallocation, or more if this can be done
//   // without allocating more. Does not decrease the size of `dest`.
//   // The prefix of data with `used_size` is preserved. Remaining space is
//   // unspecified. Returns the `true` on success, or `false` on failure.
//   //
//   // Preconditions:
//   //   `used_size <= Size(dest)`
//   //   `used_size <= new_size`
//   static bool Grow(Resizable& dest, size_t new_size, size_t used_size);
// ```
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the `ResizableTraits::Resizable` being written to. `Dest`
// must support `Dependency<ResizableTraits::Resizable*, Dest>`, e.g.
// `ResizableTraits::Resizable*` (not owned, default),
// `ResizableTraits::Resizable` (owned).
//
// The `ResizableTraits::Resizable` must not be accessed until the
// `ResizableWriter` is closed or no longer used, except that it is allowed
// to read the `ResizableTraits::Resizable` immediately after `Flush()`.
template <typename ResizableTraits,
          typename Dest = typename ResizableTraits::Resizable*>
class ResizableWriter : public ResizableWriterBase {
 public:
  using Resizable = typename ResizableTraits::Resizable;

  // Creates a closed `ResizableWriter`.
  explicit ResizableWriter(Closed) noexcept : ResizableWriterBase(kClosed) {}

  // Will append to an owned `Resizable` which can be accessed by `dest()`.
  // This constructor is present only if `Dest` is `Resizable` which is
  // default-constructible.
  template <
      typename DependentDest = Dest,
      std::enable_if_t<std::is_same<DependentDest, Resizable>::value &&
                           std::is_default_constructible<Resizable>::value,
                       int> = 0>
  explicit ResizableWriter(Options options = Options());

  // Will append to the `Resizable` provided by `dest`.
  explicit ResizableWriter(const Dest& dest, Options options = Options());
  explicit ResizableWriter(Dest&& dest, Options options = Options());

  // Will append to the `Resizable` provided by a `Dest` constructed from
  // elements of `dest_args`. This avoids constructing a temporary `Dest` and
  // moving from it.
  template <typename... DestArgs>
  explicit ResizableWriter(std::tuple<DestArgs...> dest_args,
                           Options options = Options());

  ResizableWriter(ResizableWriter&& that) noexcept;
  ResizableWriter& operator=(ResizableWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `ResizableWriter`. This
  // avoids constructing a temporary `ResizableWriter` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  template <
      typename DependentDest = Dest,
      std::enable_if_t<std::is_same<DependentDest, Resizable>::value &&
                           std::is_default_constructible<Resizable>::value,
                       int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Options options = Options());
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(const Dest& dest,
                                          Options options = Options());
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Dest&& dest,
                                          Options options = Options());
  template <typename... DestArgs>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(std::tuple<DestArgs...> dest_args,
                                          Options options = Options());

  // Returns the object providing and possibly owning the `Resizable` being
  // written to. Unchanged by `Close()`.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }

  // Returns the `Resizable` being written to. Unchanged by `Close()`.
  Resizable* DestResizable() { return dest_.get(); }
  const Resizable* DestResizable() const { return dest_.get(); }

 protected:
  void Initialize(Resizable* dest, bool append);

  bool ResizeDest() override;
  void MakeDestBuffer() override;
  void GrowDestToCapacityAndMakeBuffer() override;
  bool GrowDestAndMakeBuffer(size_t new_size) override;

 private:
  void MoveDestAndSecondaryBuffer(ResizableWriter&& that);

  // The object providing and possibly owning the `Resizable` being written
  // to, with uninitialized space appended (possibly empty); `cursor()` points
  // to the uninitialized space.
  Dependency<Resizable*, Dest> dest_;
};

// `ResizableTraits` for `std::string` with an arbitrary allocator, i.e.
// `std::basic_string<char, std::char_traits<char>, Alloc>`.
template <typename Alloc = std::allocator<char>>
struct StringResizableTraits {
  using Resizable = std::basic_string<char, std::char_traits<char>, Alloc>;
  static char* Data(Resizable& dest) { return &dest[0]; }
  static size_t Size(const Resizable& dest) { return dest.size(); }
  static constexpr bool kIsStable = false;
  static bool Resize(Resizable& dest, size_t new_size, size_t used_size) {
    RIEGELI_ASSERT_LE(used_size, dest.size())
        << "Failed precondition of ResizableTraits::Resize(): "
           "used size exceeds old size";
    RIEGELI_ASSERT_LE(used_size, new_size)
        << "Failed precondition of ResizableTraits::Resize(): "
           "used size exceeds new size";
    Reserve(dest, new_size, used_size);
    dest.resize(new_size);
    return true;
  }
  static void GrowToCapacity(Resizable& dest) { dest.resize(dest.capacity()); }
  static bool Grow(Resizable& dest, size_t new_size, size_t used_size) {
    RIEGELI_ASSERT_LE(used_size, dest.size())
        << "Failed precondition of ResizableTraits::Grow(): "
           "used size exceeds old size";
    RIEGELI_ASSERT_LE(used_size, new_size)
        << "Failed precondition of ResizableTraits::Grow(): "
           "used size exceeds new size";
    Reserve(dest, new_size, used_size);
    GrowToCapacity(dest);
    return true;
  }

 private:
  static void Reserve(Resizable& dest, size_t new_size, size_t used_size) {
    if (new_size > dest.capacity()) {
      dest.erase(used_size);
      dest.reserve(UnsignedMax(
          new_size,
          UnsignedMin(dest.capacity() + dest.capacity() / 2, dest.max_size())));
    }
  }
};

// Implementation details follow.

inline ResizableWriterBase::ResizableWriterBase(size_t min_buffer_size,
                                                size_t max_buffer_size)
    : options_(Chain::Options()
                   .set_min_block_size(min_buffer_size)
                   .set_max_block_size(max_buffer_size)) {}

inline ResizableWriterBase::ResizableWriterBase(
    ResizableWriterBase&& that) noexcept
    : Writer(static_cast<Writer&&>(that)),
      options_(that.options_),
      // `secondary_buffer_` will be moved by
      // `ResizableWriter::ResizableWriter()`.
      associated_reader_(std::move(that.associated_reader_)) {}

inline ResizableWriterBase& ResizableWriterBase::operator=(
    ResizableWriterBase&& that) noexcept {
  Writer::operator=(static_cast<Writer&&>(that));
  options_ = that.options_;
  // `secondary_buffer_` will be moved by `ResizableWriter::operator=`.
  associated_reader_ = std::move(that.associated_reader_);
  return *this;
}

inline void ResizableWriterBase::Reset(Closed) {
  Writer::Reset(kClosed);
  options_ = Chain::Options();
  secondary_buffer_ = Chain();
  associated_reader_.Reset();
}

inline void ResizableWriterBase::Reset(size_t min_buffer_size,
                                       size_t max_buffer_size) {
  Writer::Reset();
  options_ = Chain::Options()
                 .set_min_block_size(min_buffer_size)
                 .set_max_block_size(max_buffer_size);
  secondary_buffer_.Clear();
  associated_reader_.Reset();
}

inline void ResizableWriterBase::MoveSecondaryBuffer(
    ResizableWriterBase&& that) {
  secondary_buffer_ = std::move(that.secondary_buffer_);
}

inline void ResizableWriterBase::MoveSecondaryBufferAndBufferPointers(
    ResizableWriterBase&& that) {
  const size_t buffer_size = start_to_limit();
  const size_t cursor_index = start_to_cursor();
  secondary_buffer_ = std::move(that.secondary_buffer_);
  set_buffer(const_cast<char*>(secondary_buffer_.blocks().back().data() +
                               secondary_buffer_.blocks().back().size()) -
                 buffer_size,
             buffer_size, cursor_index);
}

template <typename ResizableTraits, typename Dest>
template <
    typename DependentDest,
    std::enable_if_t<std::is_same<DependentDest,
                                  typename ResizableTraits::Resizable>::value &&
                         std::is_default_constructible<
                             typename ResizableTraits::Resizable>::value,
                     int>>
inline ResizableWriter<ResizableTraits, Dest>::ResizableWriter(Options options)
    : ResizableWriter(std::forward_as_tuple(), std::move(options)) {}

template <typename ResizableTraits, typename Dest>
inline ResizableWriter<ResizableTraits, Dest>::ResizableWriter(const Dest& dest,
                                                               Options options)
    : ResizableWriterBase(options.min_buffer_size(), options.max_buffer_size()),
      dest_(dest) {
  Initialize(dest_.get(), options.append());
}

template <typename ResizableTraits, typename Dest>
inline ResizableWriter<ResizableTraits, Dest>::ResizableWriter(Dest&& dest,
                                                               Options options)
    : ResizableWriterBase(options.min_buffer_size(), options.max_buffer_size()),
      dest_(std::move(dest)) {
  Initialize(dest_.get(), options.append());
}

template <typename ResizableTraits, typename Dest>
template <typename... DestArgs>
inline ResizableWriter<ResizableTraits, Dest>::ResizableWriter(
    std::tuple<DestArgs...> dest_args, Options options)
    : ResizableWriterBase(options.min_buffer_size(), options.max_buffer_size()),
      dest_(std::move(dest_args)) {
  Initialize(dest_.get(), options.append());
}

template <typename ResizableTraits, typename Dest>
inline ResizableWriter<ResizableTraits, Dest>::ResizableWriter(
    ResizableWriter&& that) noexcept
    : ResizableWriterBase(static_cast<ResizableWriterBase&&>(that)) {
  MoveDestAndSecondaryBuffer(std::move(that));
}

template <typename ResizableTraits, typename Dest>
inline ResizableWriter<ResizableTraits, Dest>&
ResizableWriter<ResizableTraits, Dest>::operator=(
    ResizableWriter&& that) noexcept {
  ResizableWriterBase::operator=(static_cast<ResizableWriterBase&&>(that));
  MoveDestAndSecondaryBuffer(std::move(that));
  return *this;
}

template <typename ResizableTraits, typename Dest>
inline void ResizableWriter<ResizableTraits, Dest>::Reset(Closed) {
  ResizableWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename ResizableTraits, typename Dest>
template <
    typename DependentDest,
    std::enable_if_t<std::is_same<DependentDest,
                                  typename ResizableTraits::Resizable>::value &&
                         std::is_default_constructible<
                             typename ResizableTraits::Resizable>::value,
                     int>>
inline void ResizableWriter<ResizableTraits, Dest>::Reset(Options options) {
  Reset(std::forward_as_tuple(), std::move(options));
}

template <typename ResizableTraits, typename Dest>
inline void ResizableWriter<ResizableTraits, Dest>::Reset(const Dest& dest,
                                                          Options options) {
  ResizableWriterBase::Reset(options.min_buffer_size(),
                             options.max_buffer_size());
  dest_.Reset(dest);
  Initialize(dest_.get(), options.append());
}

template <typename ResizableTraits, typename Dest>
inline void ResizableWriter<ResizableTraits, Dest>::Reset(Dest&& dest,
                                                          Options options) {
  ResizableWriterBase::Reset(options.min_buffer_size(),
                             options.max_buffer_size());
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), options.append());
}

template <typename ResizableTraits, typename Dest>
template <typename... DestArgs>
inline void ResizableWriter<ResizableTraits, Dest>::Reset(
    std::tuple<DestArgs...> dest_args, Options options) {
  ResizableWriterBase::Reset(options.min_buffer_size(),
                             options.max_buffer_size());
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get(), options.append());
}

template <typename ResizableTraits, typename Dest>
inline void ResizableWriter<ResizableTraits, Dest>::Initialize(Resizable* dest,
                                                               bool append) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of ResizableWriter: null Resizable pointer";
  if (append) set_start_pos(ResizableTraits::Size(*dest));
}

namespace resizable_writer_internal {

template <bool dependency_is_stable, typename ResizableTraits,
          typename Enable = void>
struct DestIsStable : public std::false_type {};

template <typename ResizableTraits>
struct DestIsStable<true, ResizableTraits> : public std::true_type {};

template <typename ResizableTraits>
struct DestIsStable<false, ResizableTraits,
                    std::enable_if_t<ResizableTraits::kIsStable>>
    : public std::true_type {};

}  // namespace resizable_writer_internal

template <typename ResizableTraits, typename Dest>
inline void ResizableWriter<ResizableTraits, Dest>::MoveDestAndSecondaryBuffer(
    ResizableWriter&& that) {
  if (!that.uses_secondary_buffer()) {
    MoveSecondaryBuffer(std::move(that));
    if (resizable_writer_internal::DestIsStable<
            Dependency<Resizable*, Dest>::kIsStable, ResizableTraits>()) {
      dest_ = std::move(that.dest_);
    } else {
      const size_t dest_size = start_to_limit();
      const size_t cursor_index = start_to_cursor();
      dest_ = std::move(that.dest_);
      if (start() != nullptr) {
        set_buffer(ResizableTraits::Data(*dest_), dest_size, cursor_index);
      }
    }
  } else {
    MoveSecondaryBufferAndBufferPointers(std::move(that));
    dest_ = std::move(that.dest_);
  }
}

template <typename ResizableTraits, typename Dest>
bool ResizableWriter<ResizableTraits, Dest>::ResizeDest() {
  if (uses_secondary_buffer()) {
    RIEGELI_ASSERT_EQ(available(), 0u)
        << "Failed precondition of ResizableWriter::ResizeDest(): "
        << "secondary buffer has free space";
  }
  RIEGELI_ASSERT_GE(limit_pos(), SecondaryBufferSize())
      << "Failed invariant of ResizableWriter: "
         "negative destination size";
  const size_t new_size = IntCast<size_t>(pos());
  if (ABSL_PREDICT_FALSE(!ResizableTraits::Resize(
          *dest_, new_size, new_size - SecondaryBufferSize()))) {
    return FailOverflow();
  }
  RIEGELI_ASSERT_EQ(ResizableTraits::Size(*dest_), new_size)
      << "Failed postcondition of ResizableTraits::Reaize(): "
         "not resized to requested size";
  set_buffer(ResizableTraits::Data(*dest_), new_size, new_size);
  set_start_pos(0);
  return true;
}

template <typename ResizableTraits, typename Dest>
void ResizableWriter<ResizableTraits, Dest>::MakeDestBuffer() {
  RIEGELI_ASSERT(!uses_secondary_buffer())
      << "Failed precondition in ResizableWriter::MakeDestBuffer(): "
         "secondary buffer is used";
  const size_t cursor_index = IntCast<size_t>(pos());
  set_buffer(ResizableTraits::Data(*dest_), ResizableTraits::Size(*dest_),
             cursor_index);
  set_start_pos(0);
}

template <typename ResizableTraits, typename Dest>
void ResizableWriter<ResizableTraits, Dest>::GrowDestToCapacityAndMakeBuffer() {
  RIEGELI_ASSERT(!uses_secondary_buffer())
      << "Failed precondition in "
         "ResizableWriter::GrowDestToCapacityAndMakeBuffer(): "
         "secondary buffer is used";
  const size_t cursor_index = IntCast<size_t>(pos());
  ResizableTraits::GrowToCapacity(*dest_);
  set_buffer(ResizableTraits::Data(*dest_), ResizableTraits::Size(*dest_),
             cursor_index);
  set_start_pos(0);
}

template <typename ResizableTraits, typename Dest>
bool ResizableWriter<ResizableTraits, Dest>::GrowDestAndMakeBuffer(
    size_t new_size) {
  if (uses_secondary_buffer()) {
    RIEGELI_ASSERT_EQ(available(), 0u)
        << "Failed precondition of ResizableWriter::GrowDestAndMakeBuffer(): "
        << "secondary buffer has free space";
  }
  RIEGELI_ASSERT_GE(limit_pos(), SecondaryBufferSize())
      << "Failed invariant of ResizableWriter: "
         "negative destination size";
  const size_t cursor_index = IntCast<size_t>(pos());
  if (ABSL_PREDICT_FALSE(!ResizableTraits::Grow(
          *dest_, new_size, cursor_index - SecondaryBufferSize()))) {
    return FailOverflow();
  }
  set_buffer(ResizableTraits::Data(*dest_), ResizableTraits::Size(*dest_),
             cursor_index);
  set_start_pos(0);
  return true;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_RESIZABLE_WRITER_H_
