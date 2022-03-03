// Copyright 2019 Google LLC
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

#ifndef RIEGELI_SNAPPY_SNAPPY_WRITER_H_
#define RIEGELI_SNAPPY_SNAPPY_WRITER_H_

#include <stddef.h>

#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

template <typename Src>
class ChainReader;
class Reader;

// Template parameter independent part of `SnappyWriter`.
class SnappyWriterBase : public Writer {
 public:
  class Options {
   public:
    Options() noexcept {}

    // Expected uncompressed size, or `absl::nullopt` if unknown. This may
    // improve performance and memory usage.
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

   private:
    absl::optional<Position> size_hint_;
  };

  // Returns the compressed `Writer`. Unchanged by `Close()`.
  virtual Writer* dest_writer() = 0;
  virtual const Writer* dest_writer() const = 0;

  bool SupportsReadMode() override { return true; }

 protected:
  explicit SnappyWriterBase(Closed) noexcept : Writer(kClosed) {}

  explicit SnappyWriterBase(absl::optional<Position> size_hint);

  SnappyWriterBase(SnappyWriterBase&& that) noexcept;
  SnappyWriterBase& operator=(SnappyWriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset(absl::optional<Position> size_hint);
  void Initialize(Writer* dest);
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateOverDest(absl::Status status);

  void Done() override;
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;
  bool PushSlow(size_t min_length, size_t recommended_length) override;
  using Writer::WriteSlow;
  bool WriteSlow(const Chain& src) override;
  bool WriteSlow(Chain&& src) override;
  bool WriteSlow(const absl::Cord& src) override;
  bool WriteSlow(absl::Cord&& src) override;
  bool WriteZerosSlow(Position length) override;
  Reader* ReadModeImpl(Position initial_pos) override;

 private:
  // `snappy::kBlockSize`
  static constexpr size_t kBlockSize = size_t{64} << 10;

  void MoveUncompressed(SnappyWriterBase&& that);

  // Prefer sharing instead of copying data at least of this length.
  size_t MinBytesToShare() const;

  // Discards uninitialized space from the end of `uncompressed_`, so that it
  // contains only actual data written.
  void SyncBuffer();

  Chain::Options options_;

  // `Writer` methods are similar to `ChainWriter` methods writing to
  // `uncompressed_`.
  //
  // `snappy::Compress()` reads data in 64KB blocks, and copies a block to a
  // scratch buffer if it is not contiguous. Hence `Writer` methods try to
  // ensure that each 64KB block of `uncompressed_` is contiguous (unless that
  // would require earlier memory copies).
  Chain uncompressed_;

  AssociatedReader<ChainReader<const Chain*>> associated_reader_;
};

// A `Writer` which compresses data with Snappy before passing it to another
// `Writer`.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the compressed `Writer`. `Dest` must support
// `Dependency<Writer*, Dest>`, e.g. `Writer*` (not owned, default),
// `std::unique_ptr<Writer>` (owned), `ChainWriter<>` (owned).
//
// By relying on CTAD the template argument can be deduced as the value type of
// the first constructor argument. This requires C++17.
//
// The compressed `Writer` must not be accessed until the `SnappyWriter` is
// closed or no longer used.
//
// `SnappyWriter` does not compress incrementally but buffers uncompressed data
// and compresses them all in `Close()`.
//
// `Flush()` does nothing. It does not make data written so far visible.
template <typename Dest = Writer*>
class SnappyWriter : public SnappyWriterBase {
 public:
  // Creates a closed `SnappyWriter`.
  explicit SnappyWriter(Closed) noexcept : SnappyWriterBase(kClosed) {}

  // Will write to the compressed `Writer` provided by `dest`.
  explicit SnappyWriter(const Dest& dest, Options options = Options());
  explicit SnappyWriter(Dest&& dest, Options options = Options());

  // Will write to the compressed `Writer` provided by a `Dest` constructed from
  // elements of `dest_args`. This avoids constructing a temporary `Dest` and
  // moving from it.
  template <typename... DestArgs>
  explicit SnappyWriter(std::tuple<DestArgs...> dest_args,
                        Options options = Options());

  SnappyWriter(SnappyWriter&& that) noexcept;
  SnappyWriter& operator=(SnappyWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `SnappyWriter`. This avoids
  // constructing a temporary `SnappyWriter` and moving from it.
  void Reset(Closed);
  void Reset(const Dest& dest, Options options = Options());
  void Reset(Dest&& dest, Options options = Options());
  template <typename... DestArgs>
  void Reset(std::tuple<DestArgs...> dest_args, Options options = Options());

  // Returns the object providing and possibly owning the compressed `Writer`.
  // Unchanged by `Close()`.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  Writer* dest_writer() override { return dest_.get(); }
  const Writer* dest_writer() const override { return dest_.get(); }

 protected:
  void Done() override;

 private:
  // The object providing and possibly owning the compressed `Writer`.
  Dependency<Writer*, Dest> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit SnappyWriter(Closed)->SnappyWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit SnappyWriter(const Dest& dest, SnappyWriterBase::Options options =
                                            SnappyWriterBase::Options())
    -> SnappyWriter<std::decay_t<Dest>>;
template <typename Dest>
explicit SnappyWriter(Dest&& dest, SnappyWriterBase::Options options =
                                       SnappyWriterBase::Options())
    -> SnappyWriter<std::decay_t<Dest>>;
template <typename... DestArgs>
explicit SnappyWriter(
    std::tuple<DestArgs...> dest_args,
    SnappyWriterBase::Options options = SnappyWriterBase::Options())
    -> SnappyWriter<DeleteCtad<std::tuple<DestArgs...>>>;
#endif

// Options for `SnappyCompress()`.
class SnappyCompressOptions {
 public:
  SnappyCompressOptions() noexcept {}

  // If `absl::nullopt`, the uncompressed `Reader` must support `Size()`.
  //
  // If not `absl::nullopt`, overrides that size.
  //
  // Default: `absl::nullopt`.
  SnappyCompressOptions& set_assumed_size(
      absl::optional<Position> assumed_size) & {
    assumed_size_ = assumed_size;
    return *this;
  }
  SnappyCompressOptions&& set_assumed_size(
      absl::optional<Position> assumed_size) && {
    return std::move(set_assumed_size(assumed_size));
  }
  absl::optional<Position> assumed_size() const { return assumed_size_; }

 private:
  absl::optional<Position> assumed_size_;
};

// An alternative interface to Snappy which avoids buffering uncompressed data.
// Calling `SnappyCompress()` is equivalent to copying all data from `src` to a
// `SnappyWriter<Dest>`.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the uncompressed `Reader`. `Src` must support
// `Dependency<Reader*, Src&&>`, e.g. `Reader&` (not owned),
// `ChainReader<>` (owned), `std::unique_ptr<Reader>` (owned).
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the compressed `Writer`. `Dest` must support
// `Dependency<Writer*, Dest&&>`, e.g. `Writer&` (not owned),
// `ChainWriter<>` (owned), `std::unique_ptr<Writer>` (owned).
//
// The uncompressed `Reader` must support `Size()` if
// `SnappyCompressOptions::assumed_size() == absl::nullopt`.
template <typename Src, typename Dest,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value &&
                               IsValidDependency<Writer*, Dest&&>::value,
                           int> = 0>
absl::Status SnappyCompress(
    Src&& src, Dest&& dest,
    SnappyCompressOptions options = SnappyCompressOptions());

// Returns the maximum compressed size produced by the Snappy compressor for
// data of the given uncompressed size.
size_t SnappyMaxCompressedSize(size_t uncompressed_size);

// Implementation details follow.

inline SnappyWriterBase::SnappyWriterBase(absl::optional<Position> size_hint)
    : options_(
          Chain::Options()
              .set_size_hint(SaturatingIntCast<size_t>(size_hint.value_or(0)))
              .set_min_block_size(kBlockSize)
              .set_max_block_size(kBlockSize)) {}

inline SnappyWriterBase::SnappyWriterBase(SnappyWriterBase&& that) noexcept
    : Writer(static_cast<Writer&&>(that)),
      options_(that.options_),
      associated_reader_(std::move(that.associated_reader_)) {
  MoveUncompressed(std::move(that));
}

inline SnappyWriterBase& SnappyWriterBase::operator=(
    SnappyWriterBase&& that) noexcept {
  Writer::operator=(static_cast<Writer&&>(that));
  options_ = that.options_;
  associated_reader_ = std::move(that.associated_reader_);
  MoveUncompressed(std::move(that));
  return *this;
}

inline void SnappyWriterBase::Reset(Closed) {
  Writer::Reset(kClosed);
  options_ = Chain::Options();
  uncompressed_ = Chain();
  associated_reader_.Reset();
}

inline void SnappyWriterBase::Reset(absl::optional<Position> size_hint) {
  Writer::Reset();
  options_ =
      Chain::Options()
          .set_size_hint(SaturatingIntCast<size_t>(size_hint.value_or(0)))
          .set_min_block_size(kBlockSize)
          .set_max_block_size(kBlockSize);
  uncompressed_.Clear();
  associated_reader_.Reset();
}

inline void SnappyWriterBase::Initialize(Writer* dest) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of SnappyWriter: null Writer pointer";
  if (ABSL_PREDICT_FALSE(!dest->ok())) {
    FailWithoutAnnotation(AnnotateOverDest(dest->status()));
  }
}

inline void SnappyWriterBase::MoveUncompressed(SnappyWriterBase&& that) {
  const size_t cursor_index = start_to_cursor();
  uncompressed_ = std::move(that.uncompressed_);
  if (start() != nullptr) {
    const size_t buffer_size =
        uncompressed_.size() - IntCast<size_t>(start_pos());
    set_buffer(const_cast<char*>(uncompressed_.blocks().back().data() +
                                 uncompressed_.blocks().back().size()) -
                   buffer_size,
               buffer_size, cursor_index);
  }
}

template <typename Dest>
inline SnappyWriter<Dest>::SnappyWriter(const Dest& dest, Options options)
    : SnappyWriterBase(options.size_hint()), dest_(dest) {
  Initialize(dest_.get());
}

template <typename Dest>
inline SnappyWriter<Dest>::SnappyWriter(Dest&& dest, Options options)
    : SnappyWriterBase(options.size_hint()), dest_(std::move(dest)) {
  Initialize(dest_.get());
}

template <typename Dest>
template <typename... DestArgs>
inline SnappyWriter<Dest>::SnappyWriter(std::tuple<DestArgs...> dest_args,
                                        Options options)
    : SnappyWriterBase(options.size_hint()), dest_(std::move(dest_args)) {
  Initialize(dest_.get());
}

template <typename Dest>
inline SnappyWriter<Dest>::SnappyWriter(SnappyWriter&& that) noexcept
    : SnappyWriterBase(static_cast<SnappyWriterBase&&>(that)),
      dest_(std::move(that.dest_)) {}

template <typename Dest>
inline SnappyWriter<Dest>& SnappyWriter<Dest>::operator=(
    SnappyWriter&& that) noexcept {
  SnappyWriterBase::operator=(static_cast<SnappyWriterBase&&>(that));
  dest_ = std::move(that.dest_);
  return *this;
}

template <typename Dest>
inline void SnappyWriter<Dest>::Reset(Closed) {
  SnappyWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void SnappyWriter<Dest>::Reset(const Dest& dest, Options options) {
  SnappyWriterBase::Reset(options.size_hint());
  dest_.Reset(dest);
  Initialize(dest_.get());
}

template <typename Dest>
inline void SnappyWriter<Dest>::Reset(Dest&& dest, Options options) {
  SnappyWriterBase::Reset(options.size_hint());
  dest_.Reset(std::move(dest));
  Initialize(dest_.get());
}

template <typename Dest>
template <typename... DestArgs>
inline void SnappyWriter<Dest>::Reset(std::tuple<DestArgs...> dest_args,
                                      Options options) {
  SnappyWriterBase::Reset(options.size_hint());
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get());
}

template <typename Dest>
void SnappyWriter<Dest>::Done() {
  SnappyWriterBase::Done();
  if (dest_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) {
      FailWithoutAnnotation(AnnotateOverDest(dest_->status()));
    }
  }
}

namespace snappy_internal {

absl::Status SnappyCompressImpl(Reader& src, Writer& dest,
                                SnappyCompressOptions options);

}  // namespace snappy_internal

template <typename Src, typename Dest,
          std::enable_if_t<IsValidDependency<Reader*, Src&&>::value &&
                               IsValidDependency<Writer*, Dest&&>::value,
                           int>>
inline absl::Status SnappyCompress(Src&& src, Dest&& dest,
                                   SnappyCompressOptions options) {
  Dependency<Reader*, Src&&> src_ref(std::forward<Src>(src));
  Dependency<Writer*, Dest&&> dest_ref(std::forward<Dest>(dest));
  absl::Status status = snappy_internal::SnappyCompressImpl(*src_ref, *dest_ref,
                                                            std::move(options));
  if (dest_ref.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_ref->Close())) {
      status.Update(dest_ref->status());
    }
  }
  if (src_ref.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_ref->VerifyEndAndClose())) {
      status.Update(src_ref->status());
    }
  }
  return status;
}

}  // namespace riegeli

#endif  // RIEGELI_SNAPPY_SNAPPY_WRITER_H_
