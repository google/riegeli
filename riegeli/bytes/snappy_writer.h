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

#ifndef RIEGELI_BYTES_SNAPPY_WRITER_H_
#define RIEGELI_BYTES_SNAPPY_WRITER_H_

#include <stddef.h>

#include <limits>
#include <tuple>
#include <utility>

#include "absl/base/optimization.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/resetter.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Template parameter invariant part of `SnappyWriter`.
class SnappyWriterBase : public Writer {
 public:
  class Options {
   public:
    Options() noexcept {}

    // Expected uncompressed size, or 0 if unknown. This may improve performance
    // and memory usage.
    //
    // If the size hint turns out to not match reality, nothing breaks.
    Options& set_size_hint(Position size_hint) & {
      size_hint_ = size_hint;
      return *this;
    }
    Options&& set_size_hint(Position size_hint) && {
      return std::move(set_size_hint(size_hint));
    }

   private:
    template <typename Dest>
    friend class SnappyWriter;

    Position size_hint_ = 0;
  };

  // Returns the compressed `Writer`. Unchanged by `Close()`.
  virtual Writer* dest_writer() = 0;
  virtual const Writer* dest_writer() const = 0;

  bool Flush(FlushType flush_type) override;

 protected:
  SnappyWriterBase() noexcept : Writer(kInitiallyClosed) {}

  explicit SnappyWriterBase(Position size_hint);

  SnappyWriterBase(SnappyWriterBase&& that) noexcept;
  SnappyWriterBase& operator=(SnappyWriterBase&& that) noexcept;

  void Reset();
  void Reset(Position size_hint);
  void Initialize(Writer* dest);

  void Done() override;
  bool PushSlow(size_t min_length, size_t recommended_length) override;
  using Writer::WriteSlow;
  bool WriteSlow(const Chain& src) override;

 private:
  void MoveUncompressed(SnappyWriterBase&& that);

  // Discards uninitialized space from the end of `uncompressed_`, so that it
  // contains only actual data written.
  void SyncBuffer();

  // Appends uninitialized space to `uncompressed_`.
  void MakeBuffer(size_t min_length = 0);

  size_t size_hint_ = 0;
  // `Writer` methods are similar to `ChainWriter` methods writing to
  // `uncompressed_`.
  //
  // `snappy::Compress()` reads data in 64KB blocks, and copies a block to a
  // scratch buffer if it is not contiguous. Hence `Writer` methods try to
  // ensure that each 64KB block of `uncompressed_` is contiguous (this can be
  // violated by `Push(min_length)` with `min_length > 1`).
  Chain uncompressed_;
};

// A `Writer` which compresses data with Snappy before passing it to another
// `Writer`.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the compressed `Writer`. `Dest` must support
// `Dependency<Writer*, Dest>`, e.g. `Writer*` (not owned, default),
// `std::unique_ptr<Writer>` (owned), `ChainWriter<>` (owned).
//
// The compressed `Writer` must not be accessed until the `SnappyWriter` is
// closed or no longer used.
//
// `SnappyWriter` does not compress incrementally but buffers uncompressed data
// and compresses them all in `Close()`.
//
// `Flush()` does nothing. It does not make data written so far visible.
//
// For each 64KB block of uncompressed data except the last one, the compressed
// `Writer` has `Writer::Push(76490)` called, then `cursor()` is moved by the
// compressed length of the block. If data compress well, this can be
// inefficient, depending on the `Writer`.
template <typename Dest = Writer*>
class SnappyWriter : public SnappyWriterBase {
 public:
  // Creates a closed `SnappyWriter`.
  SnappyWriter() noexcept {}

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
  void Reset();
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

// Implementation details follow.

inline SnappyWriterBase::SnappyWriterBase(Position size_hint)
    : Writer(kInitiallyOpen),
      size_hint_(UnsignedMin(size_hint, std::numeric_limits<size_t>::max())) {}

inline SnappyWriterBase::SnappyWriterBase(SnappyWriterBase&& that) noexcept
    : Writer(std::move(that)), size_hint_(that.size_hint_) {
  MoveUncompressed(std::move(that));
}

inline SnappyWriterBase& SnappyWriterBase::operator=(
    SnappyWriterBase&& that) noexcept {
  Writer::operator=(std::move(that));
  size_hint_ = that.size_hint_;
  MoveUncompressed(std::move(that));
  return *this;
}

inline void SnappyWriterBase::Reset() {
  Writer::Reset(kInitiallyClosed);
  size_hint_ = 0;
  uncompressed_.Clear();
}

inline void SnappyWriterBase::Reset(Position size_hint) {
  Writer::Reset(kInitiallyOpen);
  size_hint_ = UnsignedMin(size_hint, std::numeric_limits<size_t>::max());
  uncompressed_.Clear();
}

inline void SnappyWriterBase::Initialize(Writer* dest) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of SnappyWriter: null Writer pointer";
  if (ABSL_PREDICT_FALSE(!dest->healthy())) Fail(*dest);
}

inline void SnappyWriterBase::MoveUncompressed(SnappyWriterBase&& that) {
  const size_t cursor_index = written_to_buffer();
  uncompressed_ = std::move(that.uncompressed_);
  if (start_ != nullptr) {
    limit_ = const_cast<char*>(uncompressed_.blocks().back().data() +
                               uncompressed_.blocks().back().size());
    start_ = limit_ - (uncompressed_.size() - IntCast<size_t>(start_pos_));
    cursor_ = start_ + cursor_index;
  }
}

template <typename Dest>
inline SnappyWriter<Dest>::SnappyWriter(const Dest& dest, Options options)
    : SnappyWriterBase(options.size_hint_), dest_(dest) {
  Initialize(dest_.get());
}

template <typename Dest>
inline SnappyWriter<Dest>::SnappyWriter(Dest&& dest, Options options)
    : SnappyWriterBase(options.size_hint_), dest_(std::move(dest)) {
  Initialize(dest_.get());
}

template <typename Dest>
template <typename... DestArgs>
inline SnappyWriter<Dest>::SnappyWriter(std::tuple<DestArgs...> dest_args,
                                        Options options)
    : SnappyWriterBase(options.size_hint_), dest_(std::move(dest_args)) {
  Initialize(dest_.get());
}

template <typename Dest>
inline SnappyWriter<Dest>::SnappyWriter(SnappyWriter&& that) noexcept
    : SnappyWriterBase(std::move(that)), dest_(std::move(that.dest_)) {}

template <typename Dest>
inline SnappyWriter<Dest>& SnappyWriter<Dest>::operator=(
    SnappyWriter&& that) noexcept {
  SnappyWriterBase::operator=(std::move(that));
  dest_ = std::move(that.dest_);
  return *this;
}

template <typename Dest>
inline void SnappyWriter<Dest>::Reset() {
  SnappyWriterBase::Reset();
  dest_.Reset();
}

template <typename Dest>
inline void SnappyWriter<Dest>::Reset(const Dest& dest, Options options) {
  SnappyWriterBase::Reset(options.size_hint_);
  dest_.Reset(dest);
  Initialize(dest_.get());
}

template <typename Dest>
inline void SnappyWriter<Dest>::Reset(Dest&& dest, Options options) {
  SnappyWriterBase::Reset(options.size_hint_);
  dest_.Reset(std::move(dest));
  Initialize(dest_.get());
}

template <typename Dest>
template <typename... DestArgs>
inline void SnappyWriter<Dest>::Reset(std::tuple<DestArgs...> dest_args,
                                      Options options) {
  SnappyWriterBase::Reset(options.size_hint_);
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get());
}

template <typename Dest>
void SnappyWriter<Dest>::Done() {
  SnappyWriterBase::Done();
  if (dest_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) Fail(*dest_);
  }
}

template <typename Dest>
struct Resetter<SnappyWriter<Dest>> : ResetterByReset<SnappyWriter<Dest>> {};

}  // namespace riegeli

#endif  // RIEGELI_BYTES_SNAPPY_WRITER_H_
