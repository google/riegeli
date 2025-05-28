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

#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/byte_fill.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/external_ref.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/moving_dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/buffer_options.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

class Reader;
template <typename Src>
class StringReader;

// Template parameter independent part of `ResizableWriter`.
class ResizableWriterBase : public Writer {
 public:
  class Options : public BufferOptionsBase<Options> {
   public:
    Options() noexcept {}

    // If `false`, replaces existing contents of the destination, clearing it
    // first.
    //
    // If `true`, appends to existing contents of the destination.
    //
    // Default: `false`.
    Options& set_append(bool append) & ABSL_ATTRIBUTE_LIFETIME_BOUND {
      append_ = append;
      return *this;
    }
    Options&& set_append(bool append) && ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_append(append));
    }
    bool append() const { return append_; }

   private:
    bool append_ = false;
  };

  bool SupportsRandomAccess() override { return true; }
  bool SupportsReadMode() override { return true; }

 protected:
  explicit ResizableWriterBase(Closed) noexcept : Writer(kClosed) {}

  explicit ResizableWriterBase(BufferOptions buffer_options);

  ResizableWriterBase(ResizableWriterBase&& that) noexcept;
  ResizableWriterBase& operator=(ResizableWriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset(BufferOptions buffer_options);
  bool uses_secondary_buffer() const { return !secondary_buffer_.empty(); }

  // Returns the amount of data written, either to the destination or to
  // `secondary_buffer_`.
  size_t used_size() const;

  // Returns the amount of data written to the destination. Does not include
  // data written to `secondary_buffer_`.
  //
  // Precondition: if `uses_secondary_buffer()` then `available() == 0`
  size_t used_dest_size() const;

  // Sets the size of the destination to `used_size()`. Sets buffer pointers to
  // the destination.
  //
  // Precondition: if `uses_secondary_buffer()` then `available() == 0`
  virtual bool ResizeDest() = 0;

  // Sets buffer pointers to the destination.
  //
  // Precondition: `!uses_secondary_buffer()`
  virtual void MakeDestBuffer(size_t cursor_index) = 0;

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
  void SetWriteSizeHintImpl(std::optional<Position> write_size_hint) override;
  bool PushSlow(size_t min_length, size_t recommended_length) override;
  using Writer::WriteSlow;
  bool WriteSlow(ExternalRef src) override;
  bool WriteSlow(const Chain& src) override;
  bool WriteSlow(Chain&& src) override;
  bool WriteSlow(const absl::Cord& src) override;
  bool WriteSlow(absl::Cord&& src) override;
  bool WriteSlow(ByteFill src) override;
  bool FlushImpl(FlushType flush_type) override;
  bool SeekSlow(Position new_pos) override;
  std::optional<Position> SizeImpl() override;
  bool TruncateImpl(Position new_size) override;
  Reader* ReadModeImpl(Position initial_pos) override;

 private:
  // Discards uninitialized space from the end of `secondary_buffer_`, so that
  // it contains only actual data written.
  void SyncSecondaryBuffer();

  // Appends uninitialized space to `secondary_buffer_`.
  void MakeSecondaryBuffer(size_t min_length = 0,
                           size_t recommended_length = 0);

  // Move `secondary_buffer_`, adjusting buffer pointers if they point to it.
  void MoveSecondaryBuffer(ResizableWriterBase& that);

  Chain::Options options_;
  // Buffered data which did not fit into the destination.
  Chain secondary_buffer_;

  // Size of written data is always `UnsignedMax(pos(), written_size_)`.
  // This is used to determine the size after seeking backwards.
  //
  // Invariant: if `uses_secondary_buffer()` then `written_size_ == 0`.
  size_t written_size_ = 0;

  AssociatedReader<StringReader<absl::string_view>> associated_reader_;

  // If `!uses_secondary_buffer()`, then the destination contains the data
  // before the current position of length `pos()`, followed by the data after
  // the current position of length `SaturatingSub(written_size_, pos())`,
  // followed by free space of length
  // `ResizableTraits::Size(*dest_) - UnsignedMax(pos(), written_size_)`.
  //
  // If `uses_secondary_buffer()`, then the destination contains the prefix of
  // the data of length `limit_pos() - secondary_buffer_.size()` followed by
  // free space, and `secondary_buffer_` contains the rest of the data of length
  // `secondary_buffer_.size() - available()` followed by free space of length
  // `available()`. In this case there is no data after the current position.
  //
  // Invariants if `ok()` (`dest_` is defined in `ResizableWriter`):
  //   `!uses_secondary_buffer() &&
  //    start() == ResizableTraits::Data(*dest_) &&
  //    start_to_limit() == ResizableTraits::Size(*dest_) &&
  //    start_pos() == 0` or
  //       `uses_secondary_buffer() &&
  //        limit() == secondary_buffer_.blocks().back().data() +
  //                   secondary_buffer_.blocks().back().size()` or
  //       `start() == nullptr`
  //   `limit_pos() >= secondary_buffer_.size()`
  //   `ResizableTraits::Size(*dest_) >= limit_pos() - secondary_buffer_.size()`
};

// A `Writer` which appends to a resizable array, resizing it as necessary.
// It generalizes `StringWriter` to other objects with a flat representation.
//
// It supports `Seek()` and `ReadMode()`.
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
//   // The intent is to resize exactly to `new_size`, but the size reported by
//   // `Size(dest)` can be larger than `new_size` if `dest` cannot represent
//   // all sizes exactly.
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
// `ResizableTraits::Resizable` (owned),
// `std::unique_ptr<ResizableTraits::Resizable>` (owned),
// `Any<ResizableTraits::Resizable*>` (maybe owned).
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

  // Will append to the `Resizable` provided by `dest`.
  explicit ResizableWriter(Initializer<Dest> dest, Options options = Options());

  // Will append to an owned `Resizable` which can be accessed by `dest()`.
  // This constructor is present only if `Dest` is `Resizable` which is
  // default-constructible.
  template <typename DependentDest = Dest,
            std::enable_if_t<
                std::conjunction_v<std::is_same<DependentDest, Resizable>,
                                   std::is_default_constructible<Resizable>>,
                int> = 0>
  explicit ResizableWriter(Options options = Options());

  ResizableWriter(ResizableWriter&& that) = default;
  ResizableWriter& operator=(ResizableWriter&& that) = default;

  // Makes `*this` equivalent to a newly constructed `ResizableWriter`. This
  // avoids constructing a temporary `ResizableWriter` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Dest> dest,
                                          Options options = Options());
  template <typename DependentDest = Dest,
            std::enable_if_t<
                std::conjunction_v<std::is_same<DependentDest, Resizable>,
                                   std::is_default_constructible<Resizable>>,
                int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Options options = Options());

  // Returns the object providing and possibly owning the `Resizable` being
  // written to. Unchanged by `Close()`.
  Dest& dest() ABSL_ATTRIBUTE_LIFETIME_BOUND { return dest_.manager(); }
  const Dest& dest() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return dest_.manager();
  }

  // Returns the `Resizable` being written to. Unchanged by `Close()`.
  Resizable* DestResizable() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return dest_.get();
  }
  Resizable& Digest() ABSL_ATTRIBUTE_LIFETIME_BOUND {
    Flush();
    return *DestResizable();
  }

 protected:
  void Initialize(Resizable* dest, bool append);

  bool ResizeDest() override;
  void MakeDestBuffer(size_t cursor_index) override;
  void GrowDestToCapacityAndMakeBuffer() override;
  bool GrowDestAndMakeBuffer(size_t new_size) override;

 private:
  class Mover;

  // The object providing and possibly owning the `Resizable` being written
  // to, with uninitialized space appended (possibly empty); `cursor()` points
  // to the uninitialized space.
  MovingDependency<Resizable*, Dest, Mover> dest_;
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
      // Use `std::string().capacity()` instead of `Resizable().capacity()`
      // because `Resizable` is not necessarily default-constructible. They are
      // normally the same, and even if they are not, this is a matter of
      // performance tuning, not correctness.
      dest.reserve(
          dest.capacity() <= std::string().capacity()
              ? new_size
              : UnsignedMax(new_size,
                            UnsignedMin(dest.capacity() + dest.capacity() / 2,
                                        dest.max_size())));
    }
  }
};

// `ResizableTraits` for `std::vector<T>`.
//
// Warning: byte contents are reinterpreted as values of type `T`, and the size
// is rounded up to a multiple of the element type.
template <typename T, typename Allocator = std::allocator<T>>
struct VectorResizableTraits {
  static_assert(
      std::is_trivially_copyable_v<T>,
      "Parameter of VectorResizableTraits must be trivially copyable");

  using Resizable = std::vector<T, Allocator>;
  static char* Data(Resizable& dest) {
    return reinterpret_cast<char*>(dest.data());
  }
  static size_t Size(const Resizable& dest) { return dest.size() * sizeof(T); }
  static constexpr bool kIsStable = true;
  static bool Resize(Resizable& dest, size_t new_size, size_t used_size) {
    RIEGELI_ASSERT_LE(used_size, dest.size() * sizeof(T))
        << "Failed precondition of ResizableTraits::Resize(): "
           "used size exceeds old size";
    RIEGELI_ASSERT_LE(used_size, new_size)
        << "Failed precondition of ResizableTraits::Resize(): "
           "used size exceeds new size";
    const size_t new_num_elements = SizeToNumElements(new_size);
    Reserve(dest, new_num_elements, used_size);
    dest.resize(new_num_elements);
    return true;
  }
  static void GrowToCapacity(Resizable& dest) { dest.resize(dest.capacity()); }
  static bool Grow(Resizable& dest, size_t new_size, size_t used_size) {
    RIEGELI_ASSERT_LE(used_size, dest.size() * sizeof(T))
        << "Failed precondition of ResizableTraits::Grow(): "
           "used size exceeds old size";
    RIEGELI_ASSERT_LE(used_size, new_size)
        << "Failed precondition of ResizableTraits::Grow(): "
           "used size exceeds new size";
    Reserve(dest, SizeToNumElements(new_size), used_size);
    GrowToCapacity(dest);
    return true;
  }

 private:
  static size_t SizeToNumElements(size_t size) {
    return size / sizeof(T) + (size % sizeof(T) == 0 ? 0 : 1);
  }
  static void Reserve(Resizable& dest, size_t new_num_elements,
                      size_t used_size) {
    if (new_num_elements > dest.capacity()) {
      dest.erase(dest.begin() + SizeToNumElements(used_size), dest.end());
      dest.reserve(UnsignedMax(
          new_num_elements,
          UnsignedMin(dest.capacity() + dest.capacity() / 2, dest.max_size())));
    }
  }
};

// Implementation details follow.

inline ResizableWriterBase::ResizableWriterBase(BufferOptions buffer_options)
    : options_(Chain::Options()
                   .set_min_block_size(buffer_options.min_buffer_size())
                   .set_max_block_size(buffer_options.max_buffer_size())) {}

inline ResizableWriterBase::ResizableWriterBase(
    ResizableWriterBase&& that) noexcept
    : Writer(static_cast<Writer&&>(that)),
      options_(that.options_),
      written_size_(that.written_size_),
      associated_reader_(std::move(that.associated_reader_)) {
  MoveSecondaryBuffer(that);
}

inline ResizableWriterBase& ResizableWriterBase::operator=(
    ResizableWriterBase&& that) noexcept {
  Writer::operator=(static_cast<Writer&&>(that));
  options_ = that.options_;
  written_size_ = that.written_size_;
  associated_reader_ = std::move(that.associated_reader_);
  MoveSecondaryBuffer(that);
  return *this;
}

inline void ResizableWriterBase::Reset(Closed) {
  Writer::Reset(kClosed);
  options_ = Chain::Options();
  secondary_buffer_ = Chain();
  written_size_ = 0;
  associated_reader_.Reset();
}

inline void ResizableWriterBase::Reset(BufferOptions buffer_options) {
  Writer::Reset();
  options_ = Chain::Options()
                 .set_min_block_size(buffer_options.min_buffer_size())
                 .set_max_block_size(buffer_options.max_buffer_size());
  secondary_buffer_.Clear();
  written_size_ = 0;
  associated_reader_.Reset();
}

inline void ResizableWriterBase::MoveSecondaryBuffer(
    ResizableWriterBase& that) {
  // Buffer pointers are already moved so `start()` is taken from `*this`.
  // `secondary_buffer_` is not moved yet so `uses_secondary_buffer()` is called
  // on `that`.
  const bool uses_buffer = start() != nullptr && that.uses_secondary_buffer();
  const size_t saved_start_to_limit = start_to_limit();
  const size_t saved_start_to_cursor = start_to_cursor();
  if (uses_buffer) {
    RIEGELI_ASSERT(that.secondary_buffer_.blocks().back().data() +
                       that.secondary_buffer_.blocks().back().size() ==
                   limit())
        << "Failed invariant of ResizableWriter: "
           "secondary buffer inconsistent with buffer pointers";
  }
  secondary_buffer_ = std::move(that.secondary_buffer_);
  if (uses_buffer) {
    const absl::string_view last_block = secondary_buffer_.blocks().back();
    set_buffer(const_cast<char*>(last_block.data() + last_block.size()) -
                   saved_start_to_limit,
               saved_start_to_limit, saved_start_to_cursor);
  }
}

inline size_t ResizableWriterBase::used_size() const {
  return UnsignedMax(IntCast<size_t>(pos()), written_size_);
}

inline size_t ResizableWriterBase::used_dest_size() const {
  if (uses_secondary_buffer()) {
    RIEGELI_ASSERT_EQ(available(), 0u)
        << "Failed precondition of ResizableWriterBase::used_dest_size(): "
        << "secondary buffer has free space";
  }
  RIEGELI_ASSERT_GE(used_size(), secondary_buffer_.size())
      << "Failed invariant of ResizableWriterBase: "
         "negative destination size";
  return used_size() - secondary_buffer_.size();
}

template <typename ResizableTraits, typename Dest>
class ResizableWriter<ResizableTraits, Dest>::Mover {
 public:
  static auto member() { return &ResizableWriter::dest_; }

  explicit Mover(ResizableWriter& self, ResizableWriter& that)
      : uses_buffer_(!ResizableTraits::kIsStable && self.start() != nullptr &&
                     !self.uses_secondary_buffer()),
        start_to_cursor_(self.start_to_cursor()) {
    if (uses_buffer_) {
      RIEGELI_ASSERT_EQ(ResizableTraits::Data(*that.dest_), self.start())
          << "ResizableWriter destination changed unexpectedly";
      RIEGELI_ASSERT_EQ(ResizableTraits::Size(*that.dest_),
                        self.start_to_limit())
          << "ResizableWriter destination changed unexpectedly";
    }
  }

  void Done(ResizableWriter& self) {
    if (uses_buffer_) {
      Resizable& dest = *self.dest_;
      self.set_buffer(ResizableTraits::Data(dest), ResizableTraits::Size(dest),
                      start_to_cursor_);
    }
  }

 private:
  bool uses_buffer_;
  size_t start_to_cursor_;
};

template <typename ResizableTraits, typename Dest>
inline ResizableWriter<ResizableTraits, Dest>::ResizableWriter(
    Initializer<Dest> dest, Options options)
    : ResizableWriterBase(options.buffer_options()), dest_(std::move(dest)) {
  Initialize(dest_.get(), options.append());
}

template <typename ResizableTraits, typename Dest>
template <
    typename DependentDest,
    std::enable_if_t<
        std::conjunction_v<
            std::is_same<DependentDest, typename ResizableTraits::Resizable>,
            std::is_default_constructible<typename ResizableTraits::Resizable>>,
        int>>
inline ResizableWriter<ResizableTraits, Dest>::ResizableWriter(Options options)
    : ResizableWriter(riegeli::Maker(), std::move(options)) {}

template <typename ResizableTraits, typename Dest>
inline void ResizableWriter<ResizableTraits, Dest>::Reset(Closed) {
  ResizableWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename ResizableTraits, typename Dest>
inline void ResizableWriter<ResizableTraits, Dest>::Reset(
    Initializer<Dest> dest, Options options) {
  ResizableWriterBase::Reset(options.buffer_options());
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), options.append());
}

template <typename ResizableTraits, typename Dest>
template <
    typename DependentDest,
    std::enable_if_t<
        std::conjunction_v<
            std::is_same<DependentDest, typename ResizableTraits::Resizable>,
            std::is_default_constructible<typename ResizableTraits::Resizable>>,
        int>>
inline void ResizableWriter<ResizableTraits, Dest>::Reset(Options options) {
  Reset(riegeli::Maker(), std::move(options));
}

template <typename ResizableTraits, typename Dest>
inline void ResizableWriter<ResizableTraits, Dest>::Initialize(Resizable* dest,
                                                               bool append) {
  RIEGELI_ASSERT_NE(dest, nullptr)
      << "Failed precondition of ResizableWriter: null Resizable pointer";
  if (append) set_start_pos(ResizableTraits::Size(*dest));
}

template <typename ResizableTraits, typename Dest>
bool ResizableWriter<ResizableTraits, Dest>::ResizeDest() {
  if (uses_secondary_buffer()) {
    RIEGELI_ASSERT_EQ(available(), 0u)
        << "Failed precondition of ResizableWriter::ResizeDest(): "
        << "secondary buffer has free space";
  }
  const size_t new_size = used_size();
  const size_t cursor_index = IntCast<size_t>(pos());
  if (ABSL_PREDICT_FALSE(
          !ResizableTraits::Resize(*dest_, new_size, used_dest_size()))) {
    return FailOverflow();
  }
  Resizable& dest = *dest_;
  RIEGELI_ASSERT_GE(ResizableTraits::Size(dest), new_size)
      << "Failed postcondition of ResizableTraits::Resize(): "
         "not resized to at least requested size";
  set_buffer(ResizableTraits::Data(dest), ResizableTraits::Size(dest),
             cursor_index);
  set_start_pos(0);
  return true;
}

template <typename ResizableTraits, typename Dest>
void ResizableWriter<ResizableTraits, Dest>::MakeDestBuffer(
    size_t cursor_index) {
  RIEGELI_ASSERT(!uses_secondary_buffer())
      << "Failed precondition in ResizableWriter::MakeDestBuffer(): "
         "secondary buffer is used";
  Resizable& dest = *dest_;
  set_buffer(ResizableTraits::Data(dest), ResizableTraits::Size(dest),
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
  Resizable& dest = *dest_;
  set_buffer(ResizableTraits::Data(dest), ResizableTraits::Size(dest),
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
  const size_t cursor_index = IntCast<size_t>(pos());
  if (ABSL_PREDICT_FALSE(
          !ResizableTraits::Grow(*dest_, new_size, used_dest_size()))) {
    return FailOverflow();
  }
  Resizable& dest = *dest_;
  RIEGELI_ASSERT_GE(ResizableTraits::Size(dest), new_size)
      << "Failed postcondition of ResizableTraits::Grow(): "
         "not resized to at least requested size";
  set_buffer(ResizableTraits::Data(dest), ResizableTraits::Size(dest),
             cursor_index);
  set_start_pos(0);
  return true;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_RESIZABLE_WRITER_H_
