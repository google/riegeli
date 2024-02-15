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

#ifndef RIEGELI_BYTES_CORD_WRITER_H_
#define RIEGELI_BYTES_CORD_WRITER_H_

#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/meta/type_traits.h"
#include "absl/strings/cord.h"
#include "absl/strings/cord_buffer.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

template <typename Src>
class CordReader;
class Reader;

// Template parameter independent part of `CordWriter`.
class CordWriterBase : public Writer {
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

    // Minimal size of a block of allocated data.
    //
    // This is used initially, while the destination is small.
    //
    // Default: `kDefaultMinBlockSize` (256).
    Options& set_min_block_size(size_t min_block_size) & {
      min_block_size_ = UnsignedMin(min_block_size, uint32_t{1} << 31);
      return *this;
    }
    Options&& set_min_block_size(size_t min_block_size) && {
      return std::move(set_min_block_size(min_block_size));
    }
    size_t min_block_size() const { return min_block_size_; }

    // Maximal size of a block of allocated data.
    //
    // This is for performance tuning, not a guarantee: does not apply to
    // objects allocated separately and then written to this `CordWriter`.
    //
    // Default: `kDefaultMaxBlockSize - 13` (65523).
    Options& set_max_block_size(size_t max_block_size) & {
      RIEGELI_ASSERT_GT(max_block_size, 0u)
          << "Failed precondition of "
             "CordWriterBase::Options::set_max_block_size(): "
             "zero block size";
      max_block_size_ = UnsignedMin(max_block_size, uint32_t{1} << 31);
      return *this;
    }
    Options&& set_max_block_size(size_t max_block_size) && {
      return std::move(set_max_block_size(max_block_size));
    }
    size_t max_block_size() const { return max_block_size_; }

   private:
    bool append_ = false;
    // Use `uint32_t` instead of `size_t` to reduce the object size.
    uint32_t min_block_size_ = uint32_t{kDefaultMinBlockSize};
    uint32_t max_block_size_ =
        uint32_t{absl::CordBuffer::MaximumPayload(kDefaultMaxBlockSize)};
  };

  // Returns the `absl::Cord` being written to. Unchanged by `Close()`.
  virtual absl::Cord* DestCord() const = 0;
  absl::Cord& Digest() {
    Flush();
    return *DestCord();
  }

  bool SupportsRandomAccess() override { return true; }
  bool SupportsReadMode() override { return true; }

 protected:
  explicit CordWriterBase(Closed) noexcept : Writer(kClosed) {}

  explicit CordWriterBase(const Options& options);

  CordWriterBase(CordWriterBase&& that) noexcept;
  CordWriterBase& operator=(CordWriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset(const Options& options);
  void Initialize(absl::Cord* dest, bool append);

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
  bool SeekSlow(Position new_pos) override;
  absl::optional<Position> SizeImpl() override;
  bool TruncateImpl(Position new_size) override;
  Reader* ReadModeImpl(Position initial_pos) override;

 private:
  static constexpr size_t kCordBufferBlockSize =
      UnsignedMin(kDefaultMaxBlockSize, absl::CordBuffer::kCustomLimit);
  static constexpr size_t kCordBufferMaxSize =
      absl::CordBuffer::MaximumPayload(kCordBufferBlockSize);

  // If the buffer is not empty, appends it to `dest`. Ensures that data which
  // follow the current position are separated in `*tail_`.
  void SyncBuffer(absl::Cord& dest);

  // Moves `length` of data from the beginning of `*tail_` to the end of `dest`.
  void MoveFromTail(size_t length, absl::Cord& dest);

  // Moves `length` of data from the end of `dest` to the beginning of `*tail_`.
  void MoveToTail(size_t length, absl::Cord& dest);

  // Returns `true` if data which follow the current position are appended to
  // `dest`.
  bool HasAppendedTail(const absl::Cord& dest) const;

  // Moves data which follow the current position from being appended to `dest`
  // to being separated in `*tail_`.
  void ExtractTail(absl::Cord& dest);

  // Moves data which follow the current position from being separated in
  // `*tail_` to being appended to `dest`.
  void AppendTail(absl::Cord& dest);

  // Removes a prefix of `*tail_` of the given `length`, staturated at clearing
  // the whole `*tail_`.
  void ShrinkTail(size_t length);

  absl::optional<Position> size_hint_;
  // Use `uint32_t` instead of `size_t` to reduce the object size.
  uint32_t min_block_size_ = uint32_t{kDefaultMinBlockSize};
  uint32_t max_block_size_ =
      uint32_t{absl::CordBuffer::MaximumPayload(kDefaultMaxBlockSize)};

  // Buffered data to be appended, in either `cord_buffer_` or `buffer_`.
  absl::CordBuffer cord_buffer_;
  Buffer buffer_;

  // If `start_pos() < DestCord()->size()`, then data after the current
  // position are appended to `*DestCord()`, buffer pointers are `nullptr`,
  // and `tail_ == nullptr || tail_->empty()`.
  //
  // Otherwise, if `start_pos() == DestCord()->size() && tail_ != nullptr`,
  // data after the current position are separated in `*tail_`, ignoring a
  // prefix of `*tail_` with length `start_to_cursor()`, saturated at ignoring
  // the whole `*tail_` (the ignored prefix is being overwritten with buffered
  // data).
  //
  // Otherwise `start_pos() == DestCord()->size() && tail_ == nullptr`, and
  // there are no data after the current position.
  //
  // `tail_` is stored behind `std::unique_ptr` to reduce the object size in the
  // common case when random access is not used.
  std::unique_ptr<absl::Cord> tail_;

  AssociatedReader<CordReader<const absl::Cord*>> associated_reader_;

  // Invariants:
  //   `start() == nullptr` or `start() == cord_buffer_.data()`
  //       or `start() == buffer_.data()`
  //   if `ok()` then `start_pos() <= DestCord()->size()`
  //   if `ok() && start_pos() < DestCord()->size()` then
  //       `start() == nullptr && (tail_ == nullptr || tail_->empty())`
};

// A `Writer` which appends to an `absl::Cord`.
//
// It supports `Seek()` and `ReadMode()`.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the `absl::Cord` being written to. `Dest` must support
// `Dependency<absl::Cord*, Dest>`, e.g. `absl::Cord*` (not owned, default),
// `absl::Cord` (owned), `AnyDependency<absl::Cord*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as `absl::Cord`
// if there are no constructor arguments or the only argument is `Options`,
// otherwise as the value type of the first constructor argument, except that
// CTAD is deleted if the first constructor argument is an `absl::Cord&` or
// `const absl::Cord&` (to avoid writing to an unintentionally separate copy of
// an existing object). This requires C++17.
//
// The `absl::Cord` must not be accessed until the `CordWriter` is closed or no
// longer used, except that it is allowed to read the `absl::Cord` immediately
// after `Flush()`.
template <typename Dest = absl::Cord*>
class CordWriter : public CordWriterBase {
 public:
  // Creates a closed `CordWriter`.
  explicit CordWriter(Closed) noexcept : CordWriterBase(kClosed) {}

  // Will append to the `absl::Cord` provided by `dest`.
  explicit CordWriter(const Dest& dest, Options options = Options());
  explicit CordWriter(Dest&& dest, Options options = Options());

  // Will append to the `absl::Cord` provided by a `Dest` constructed from
  // elements of `dest_args`. This avoids constructing a temporary `Dest` and
  // moving from it.
  template <typename... DestArgs>
  explicit CordWriter(std::tuple<DestArgs...> dest_args,
                      Options options = Options());

  // Will append to an owned `absl::Cord` which can be accessed by `dest()`.
  // This constructor is present only if `Dest` is `absl::Cord`.
  template <
      typename DependentDest = Dest,
      std::enable_if_t<std::is_same<DependentDest, absl::Cord>::value, int> = 0>
  explicit CordWriter(Options options = Options());

  CordWriter(CordWriter&& that) noexcept;
  CordWriter& operator=(CordWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `CordWriter`. This avoids
  // constructing a temporary `CordWriter` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(const Dest& dest,
                                          Options options = Options());
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Dest&& dest,
                                          Options options = Options());
  template <typename... DestArgs>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(std::tuple<DestArgs...> dest_args,
                                          Options options = Options());
  template <
      typename DependentDest = Dest,
      std::enable_if_t<std::is_same<DependentDest, absl::Cord>::value, int> = 0>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Options options = Options());

  // Returns the object providing and possibly owning the `absl::Cord` being
  // written to. Unchanged by `Close()`.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  absl::Cord* DestCord() const override { return dest_.get(); }

 private:
  // The object providing and possibly owning the `absl::Cord` being written to.
  Dependency<absl::Cord*, Dest> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit CordWriter(Closed) -> CordWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit CordWriter(const Dest& dest,
                    CordWriterBase::Options options = CordWriterBase::Options())
    -> CordWriter<std::conditional_t<
        std::is_convertible<const Dest*, const absl::Cord*>::value,
        DeleteCtad<const Dest&>, std::decay_t<Dest>>>;
template <typename Dest>
explicit CordWriter(Dest&& dest,
                    CordWriterBase::Options options = CordWriterBase::Options())
    -> CordWriter<std::conditional_t<
        absl::conjunction<std::is_lvalue_reference<Dest>,
                          std::is_convertible<std::remove_reference_t<Dest>*,
                                              const absl::Cord*>>::value,
        DeleteCtad<Dest&&>, std::decay_t<Dest>>>;
template <typename... DestArgs>
explicit CordWriter(std::tuple<DestArgs...> dest_args,
                    CordWriterBase::Options options = CordWriterBase::Options())
    -> CordWriter<DeleteCtad<std::tuple<DestArgs...>>>;
explicit CordWriter(CordWriterBase::Options options = CordWriterBase::Options())
    -> CordWriter<absl::Cord>;
#endif

// Implementation details follow.

inline CordWriterBase::CordWriterBase(const Options& options)
    : min_block_size_(IntCast<uint32_t>(options.min_block_size())),
      max_block_size_(IntCast<uint32_t>(options.max_block_size())) {}

inline CordWriterBase::CordWriterBase(CordWriterBase&& that) noexcept
    : Writer(static_cast<Writer&&>(that)),
      size_hint_(that.size_hint_),
      min_block_size_(that.min_block_size_),
      max_block_size_(that.max_block_size_),
      buffer_(std::move(that.buffer_)),
      tail_(std::move(that.tail_)),
      associated_reader_(std::move(that.associated_reader_)) {
  if (start() == that.cord_buffer_.data()) {
    cord_buffer_ = std::move(that.cord_buffer_);
    set_buffer(cord_buffer_.data(), start_to_limit(), start_to_cursor());
  } else {
    cord_buffer_ = std::move(that.cord_buffer_);
  }
}

inline CordWriterBase& CordWriterBase::operator=(
    CordWriterBase&& that) noexcept {
  Writer::operator=(static_cast<Writer&&>(that));
  size_hint_ = that.size_hint_;
  min_block_size_ = that.min_block_size_;
  max_block_size_ = that.max_block_size_;
  buffer_ = std::move(that.buffer_);
  tail_ = std::move(that.tail_);
  associated_reader_ = std::move(that.associated_reader_);
  if (start() == that.cord_buffer_.data()) {
    cord_buffer_ = std::move(that.cord_buffer_);
    set_buffer(cord_buffer_.data(), start_to_limit(), start_to_cursor());
  } else {
    cord_buffer_ = std::move(that.cord_buffer_);
  }
  return *this;
}

inline void CordWriterBase::Reset(Closed) {
  Writer::Reset(kClosed);
  size_hint_ = absl::nullopt;
  min_block_size_ = uint32_t{kDefaultMinBlockSize};
  max_block_size_ =
      uint32_t{absl::CordBuffer::MaximumPayload(kDefaultMaxBlockSize)};
  cord_buffer_ = absl::CordBuffer();
  buffer_ = Buffer();
  tail_.reset();
  associated_reader_.Reset();
}

inline void CordWriterBase::Reset(const Options& options) {
  Writer::Reset();
  size_hint_ = absl::nullopt;
  min_block_size_ = IntCast<uint32_t>(options.min_block_size());
  max_block_size_ = IntCast<uint32_t>(options.max_block_size());
  if (tail_ != nullptr) tail_->Clear();
  associated_reader_.Reset();
}

inline void CordWriterBase::Initialize(absl::Cord* dest, bool append) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of CordWriter: null Cord pointer";
  if (append) {
    cord_buffer_ = dest->GetAppendBuffer(0, 1);
    set_start_pos(dest->size());
    const size_t existing_length = cord_buffer_.length();
    if (existing_length > 0) {
      cord_buffer_.SetLength(
          UnsignedMin(cord_buffer_.capacity(),
                      std::numeric_limits<size_t>::max() - dest->size()));
      set_buffer(cord_buffer_.data(), cord_buffer_.length(), existing_length);
    }
  } else {
    cord_buffer_ = dest->GetAppendBuffer(0, 0);
    dest->Clear();
    cord_buffer_.SetLength(0);
  }
}

template <typename Dest>
inline CordWriter<Dest>::CordWriter(const Dest& dest, Options options)
    : CordWriterBase(options), dest_(dest) {
  Initialize(dest_.get(), options.append());
}

template <typename Dest>
inline CordWriter<Dest>::CordWriter(Dest&& dest, Options options)
    : CordWriterBase(options), dest_(std::move(dest)) {
  Initialize(dest_.get(), options.append());
}

template <typename Dest>
template <typename... DestArgs>
inline CordWriter<Dest>::CordWriter(std::tuple<DestArgs...> dest_args,
                                    Options options)
    : CordWriterBase(options), dest_(std::move(dest_args)) {
  Initialize(dest_.get(), options.append());
}

template <typename Dest>
template <typename DependentDest,
          std::enable_if_t<std::is_same<DependentDest, absl::Cord>::value, int>>
inline CordWriter<Dest>::CordWriter(Options options)
    : CordWriter(std::forward_as_tuple(), std::move(options)) {}

template <typename Dest>
inline CordWriter<Dest>::CordWriter(CordWriter&& that) noexcept
    : CordWriterBase(static_cast<CordWriterBase&&>(that)),
      dest_(std::move(that.dest_)) {}

template <typename Dest>
inline CordWriter<Dest>& CordWriter<Dest>::operator=(
    CordWriter&& that) noexcept {
  CordWriterBase::operator=(static_cast<CordWriterBase&&>(that));
  dest_ = std::move(that.dest_);
  return *this;
}

template <typename Dest>
inline void CordWriter<Dest>::Reset(Closed) {
  CordWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void CordWriter<Dest>::Reset(const Dest& dest, Options options) {
  CordWriterBase::Reset(options);
  dest_.Reset(dest);
  Initialize(dest_.get(), options.append());
}

template <typename Dest>
inline void CordWriter<Dest>::Reset(Dest&& dest, Options options) {
  CordWriterBase::Reset(options);
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), options.append());
}

template <typename Dest>
template <typename... DestArgs>
inline void CordWriter<Dest>::Reset(std::tuple<DestArgs...> dest_args,
                                    Options options) {
  CordWriterBase::Reset(options);
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get(), options.append());
}

template <typename Dest>
template <typename DependentDest,
          std::enable_if_t<std::is_same<DependentDest, absl::Cord>::value, int>>
inline void CordWriter<Dest>::Reset(Options options) {
  Reset(std::forward_as_tuple(), std::move(options));
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_CORD_WRITER_H_
