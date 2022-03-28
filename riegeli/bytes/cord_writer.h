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

#include <cstring>
#include <limits>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/strings/cord.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
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

    // Minimal size of a block of allocated data.
    //
    // This is used initially, while the destination is small.
    //
    // Default: `kDefaultMinBlockSize` (256).
    Options& set_min_block_size(size_t min_block_size) & {
      min_block_size_ = min_block_size;
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
    // Default: `kDefaultMaxBlockSize` (64K).
    Options& set_max_block_size(size_t max_block_size) & {
      RIEGELI_ASSERT_GT(max_block_size, 0u)
          << "Failed precondition of "
             "CordWriterBase::Options::set_max_block_size(): "
             "zero block size";
      max_block_size_ = max_block_size;
      return *this;
    }
    Options&& set_max_block_size(size_t max_block_size) && {
      return std::move(set_max_block_size(max_block_size));
    }
    size_t max_block_size() const { return max_block_size_; }

   private:
    bool append_ = false;
    absl::optional<Position> size_hint_;
    size_t min_block_size_ = kDefaultMinBlockSize;
    size_t max_block_size_ = kDefaultMaxBlockSize;
  };

  // Returns the `absl::Cord` being written to. Unchanged by `Close()`.
  virtual absl::Cord* dest_cord() = 0;
  virtual const absl::Cord* dest_cord() const = 0;

  bool SupportsSize() override { return true; }
  bool SupportsTruncate() override { return true; }
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
  static constexpr size_t kShortBufferSize = 64;

  // If the buffer is not empty, appends it to `dest`.
  void SyncBuffer(absl::Cord& dest);

  size_t size_hint_ = 0;
  size_t min_block_size_ = kDefaultMinBlockSize;
  size_t max_block_size_ = kDefaultMaxBlockSize;

  // Buffered data to be appended, in either `short_buffer_` or `buffer_`.
  char short_buffer_[kShortBufferSize];
  Buffer buffer_;

  AssociatedReader<CordReader<const absl::Cord*>> associated_reader_;

  // Invariants:
  //   `start() == nullptr` or `start() == short_buffer_`
  //       or `start() == buffer_.data()`
  //   if `ok()` then `start_pos() == dest_cord()->size()`
};

// A `Writer` which appends to an `absl::Cord`.
//
// It supports `ReadMode()`.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the `absl::Cord` being written to. `Dest` must support
// `Dependency<absl::Cord*, Dest>`, e.g. `absl::Cord*` (not owned, default),
// `absl::Cord` (owned).
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

  // Will append to an owned `absl::Cord` which can be accessed by `dest()`.
  // This constructor is present only if `Dest` is `absl::Cord`.
  template <
      typename DependentDest = Dest,
      std::enable_if_t<std::is_same<DependentDest, absl::Cord>::value, int> = 0>
  explicit CordWriter(Options options = Options());

  // Will append to the `absl::Cord` provided by `dest`.
  explicit CordWriter(const Dest& dest, Options options = Options());
  explicit CordWriter(Dest&& dest, Options options = Options());

  // Will append to the `absl::Cord` provided by a `Dest` constructed from
  // elements of `dest_args`. This avoids constructing a temporary `Dest` and
  // moving from it.
  template <typename... DestArgs>
  explicit CordWriter(std::tuple<DestArgs...> dest_args,
                      Options options = Options());

  CordWriter(CordWriter&& that) noexcept;
  CordWriter& operator=(CordWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `CordWriter`. This avoids
  // constructing a temporary `CordWriter` and moving from it.
  void Reset(Closed);
  template <
      typename DependentDest = Dest,
      std::enable_if_t<std::is_same<DependentDest, absl::Cord>::value, int> = 0>
  void Reset(Options options = Options());
  void Reset(const Dest& dest, Options options = Options());
  void Reset(Dest&& dest, Options options = Options());
  template <typename... DestArgs>
  void Reset(std::tuple<DestArgs...> dest_args, Options options = Options());

  // Returns the object providing and possibly owning the `absl::Cord` being
  // written to. Unchanged by `Close()`.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  absl::Cord* dest_cord() override { return dest_.get(); }
  const absl::Cord* dest_cord() const override { return dest_.get(); }

 private:
  // The object providing and possibly owning the `absl::Cord` being written to.
  Dependency<absl::Cord*, Dest> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit CordWriter(Closed)->CordWriter<DeleteCtad<Closed>>;
explicit CordWriter(CordWriterBase::Options options = CordWriterBase::Options())
    ->CordWriter<absl::Cord>;
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
        std::is_lvalue_reference<Dest>::value &&
            std::is_convertible<std::remove_reference_t<Dest>*,
                                const absl::Cord*>::value,
        DeleteCtad<Dest&&>, std::decay_t<Dest>>>;
template <typename... DestArgs>
explicit CordWriter(std::tuple<DestArgs...> dest_args,
                    CordWriterBase::Options options = CordWriterBase::Options())
    -> CordWriter<DeleteCtad<std::tuple<DestArgs...>>>;
#endif

// Implementation details follow.

inline CordWriterBase::CordWriterBase(const Options& options)
    : size_hint_(SaturatingIntCast<size_t>(options.size_hint().value_or(0))),
      min_block_size_(options.min_block_size()),
      max_block_size_(options.max_block_size()) {}

inline CordWriterBase::CordWriterBase(CordWriterBase&& that) noexcept
    : Writer(static_cast<Writer&&>(that)),
      size_hint_(that.size_hint_),
      min_block_size_(that.min_block_size_),
      max_block_size_(that.max_block_size_),
      buffer_(std::move(that.buffer_)),
      associated_reader_(std::move(that.associated_reader_)) {
  if (start() == that.short_buffer_) {
    std::memcpy(short_buffer_, that.short_buffer_, kShortBufferSize);
    set_buffer(short_buffer_, start_to_limit(), start_to_cursor());
  }
}

inline CordWriterBase& CordWriterBase::operator=(
    CordWriterBase&& that) noexcept {
  Writer::operator=(static_cast<Writer&&>(that));
  size_hint_ = that.size_hint_;
  min_block_size_ = that.min_block_size_;
  max_block_size_ = that.max_block_size_;
  buffer_ = std::move(that.buffer_);
  associated_reader_ = std::move(that.associated_reader_);
  if (start() == that.short_buffer_) {
    std::memcpy(short_buffer_, that.short_buffer_, kShortBufferSize);
    set_buffer(short_buffer_, start_to_limit(), start_to_cursor());
  }
  return *this;
}

inline void CordWriterBase::Reset(Closed) {
  Writer::Reset(kClosed);
  size_hint_ = 0;
  min_block_size_ = kDefaultMinBlockSize;
  max_block_size_ = kDefaultMaxBlockSize;
  buffer_ = Buffer();
  associated_reader_.Reset();
}

inline void CordWriterBase::Reset(const Options& options) {
  Writer::Reset();
  size_hint_ = SaturatingIntCast<size_t>(options.size_hint().value_or(0));
  min_block_size_ = options.min_block_size();
  max_block_size_ = options.max_block_size();
  associated_reader_.Reset();
}

inline void CordWriterBase::Initialize(absl::Cord* dest, bool append) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of CordWriter: null Cord pointer";
  if (append) {
    set_start_pos(dest->size());
    const size_t buffer_length = UnsignedMin(
        kShortBufferSize, std::numeric_limits<size_t>::max() - dest->size());
    if (size_hint_ <= dest->size() + buffer_length) {
      set_buffer(short_buffer_, buffer_length);
    }
  } else {
    dest->Clear();
    if (size_hint_ <= kShortBufferSize) {
      set_buffer(short_buffer_, kShortBufferSize);
    }
  }
}

template <typename Dest>
template <typename DependentDest,
          std::enable_if_t<std::is_same<DependentDest, absl::Cord>::value, int>>
inline CordWriter<Dest>::CordWriter(Options options)
    : CordWriter(std::forward_as_tuple(), std::move(options)) {}

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
template <typename DependentDest,
          std::enable_if_t<std::is_same<DependentDest, absl::Cord>::value, int>>
inline void CordWriter<Dest>::Reset(Options options) {
  Reset(std::forward_as_tuple(), std::move(options));
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

}  // namespace riegeli

#endif  // RIEGELI_BYTES_CORD_WRITER_H_
