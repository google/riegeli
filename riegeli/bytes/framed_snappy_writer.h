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

#ifndef RIEGELI_BYTES_FRAMED_SNAPPY_WRITER_H_
#define RIEGELI_BYTES_FRAMED_SNAPPY_WRITER_H_

#include <stddef.h>

#include <tuple>
#include <utility>

#include "absl/base/optimization.h"
#include "riegeli/base/base.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/resetter.h"
#include "riegeli/bytes/pushable_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Template parameter independent part of `FramedSnappyWriter`.
class FramedSnappyWriterBase : public PushableWriter {
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
    Position size_hint() const { return size_hint_; }

   private:
    Position size_hint_ = 0;
  };

  // Returns the compressed `Writer`. Unchanged by `Close()`.
  virtual Writer* dest_writer() = 0;
  virtual const Writer* dest_writer() const = 0;

  bool Flush(FlushType flush_type) override;

 protected:
  FramedSnappyWriterBase() noexcept : PushableWriter(kInitiallyClosed) {}

  explicit FramedSnappyWriterBase(Position size_hint);

  FramedSnappyWriterBase(FramedSnappyWriterBase&& that) noexcept;
  FramedSnappyWriterBase& operator=(FramedSnappyWriterBase&& that) noexcept;

  void Reset();
  void Reset(Position size_hint);
  void Initialize(Writer* dest);

  void Done() override;
  bool PushSlow(size_t min_length, size_t recommended_length) override;

 private:
  // Compresses buffered data, but unlike `PushSlow()`, does not ensure that a
  // buffer is allocated.
  //
  // Precondition: `healthy()`
  //
  // Postcondition: `written_to_buffer() == 0`
  bool PushInternal(Writer* dest);

  Position size_hint_ = 0;
  // Buffered uncompressed data.
  Buffer uncompressed_;
};

// A `Writer` which compresses data with framed Snappy format before passing it
// to another `Writer`:
// https://github.com/google/snappy/blob/master/framing_format.txt
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the compressed `Writer`. `Dest` must support
// `Dependency<Writer*, Dest>`, e.g. `Writer*` (not owned, default),
// `std::unique_ptr<Writer>` (owned), `ChainWriter<>` (owned).
//
// The compressed `Writer` must not be accessed until the `FramedSnappyWriter`
// is closed or no longer used.
template <typename Dest = Writer*>
class FramedSnappyWriter : public FramedSnappyWriterBase {
 public:
  // Creates a closed `FramedSnappyWriter`.
  FramedSnappyWriter() noexcept {}

  // Will write to the compressed `Writer` provided by `dest`.
  explicit FramedSnappyWriter(const Dest& dest, Options options = Options());
  explicit FramedSnappyWriter(Dest&& dest, Options options = Options());

  // Will write to the compressed `Writer` provided by a `Dest` constructed from
  // elements of `dest_args`. This avoids constructing a temporary `Dest` and
  // moving from it.
  template <typename... DestArgs>
  explicit FramedSnappyWriter(std::tuple<DestArgs...> dest_args,
                              Options options = Options());

  FramedSnappyWriter(FramedSnappyWriter&& that) noexcept;
  FramedSnappyWriter& operator=(FramedSnappyWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `FramedSnappyWriter`. This
  // avoids constructing a temporary `FramedSnappyWriter` and moving from it.
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

inline FramedSnappyWriterBase::FramedSnappyWriterBase(Position size_hint)
    : PushableWriter(kInitiallyOpen), size_hint_(size_hint) {}

inline FramedSnappyWriterBase::FramedSnappyWriterBase(
    FramedSnappyWriterBase&& that) noexcept
    : PushableWriter(std::move(that)),
      size_hint_(that.size_hint_),
      uncompressed_(std::move(that.uncompressed_)) {}

inline FramedSnappyWriterBase& FramedSnappyWriterBase::operator=(
    FramedSnappyWriterBase&& that) noexcept {
  PushableWriter::operator=(std::move(that));
  size_hint_ = that.size_hint_;
  uncompressed_ = std::move(that.uncompressed_);
  return *this;
}

inline void FramedSnappyWriterBase::Reset() {
  PushableWriter::Reset(kInitiallyClosed);
  size_hint_ = 0;
}

inline void FramedSnappyWriterBase::Reset(Position size_hint) {
  PushableWriter::Reset(kInitiallyOpen);
  size_hint_ = SaturatingIntCast<size_t>(size_hint);
}

template <typename Dest>
inline FramedSnappyWriter<Dest>::FramedSnappyWriter(const Dest& dest,
                                                    Options options)
    : FramedSnappyWriterBase(options.size_hint()), dest_(dest) {
  Initialize(dest_.get());
}

template <typename Dest>
inline FramedSnappyWriter<Dest>::FramedSnappyWriter(Dest&& dest,
                                                    Options options)
    : FramedSnappyWriterBase(options.size_hint()), dest_(std::move(dest)) {
  Initialize(dest_.get());
}

template <typename Dest>
template <typename... DestArgs>
inline FramedSnappyWriter<Dest>::FramedSnappyWriter(
    std::tuple<DestArgs...> dest_args, Options options)
    : FramedSnappyWriterBase(options.size_hint()), dest_(std::move(dest_args)) {
  Initialize(dest_.get());
}

template <typename Dest>
inline FramedSnappyWriter<Dest>::FramedSnappyWriter(
    FramedSnappyWriter&& that) noexcept
    : FramedSnappyWriterBase(std::move(that)), dest_(std::move(that.dest_)) {}

template <typename Dest>
inline FramedSnappyWriter<Dest>& FramedSnappyWriter<Dest>::operator=(
    FramedSnappyWriter&& that) noexcept {
  FramedSnappyWriterBase::operator=(std::move(that));
  dest_ = std::move(that.dest_);
  return *this;
}

template <typename Dest>
inline void FramedSnappyWriter<Dest>::Reset() {
  FramedSnappyWriterBase::Reset();
  dest_.Reset();
}

template <typename Dest>
inline void FramedSnappyWriter<Dest>::Reset(const Dest& dest, Options options) {
  FramedSnappyWriterBase::Reset(options.size_hint());
  dest_.Reset(dest);
  Initialize(dest_.get());
}

template <typename Dest>
inline void FramedSnappyWriter<Dest>::Reset(Dest&& dest, Options options) {
  FramedSnappyWriterBase::Reset(options.size_hint());
  dest_.Reset(std::move(dest));
  Initialize(dest_.get());
}

template <typename Dest>
template <typename... DestArgs>
inline void FramedSnappyWriter<Dest>::Reset(std::tuple<DestArgs...> dest_args,
                                            Options options) {
  FramedSnappyWriterBase::Reset(options.size_hint());
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get());
}

template <typename Dest>
void FramedSnappyWriter<Dest>::Done() {
  FramedSnappyWriterBase::Done();
  if (dest_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) Fail(*dest_);
  }
}

template <typename Dest>
struct Resetter<FramedSnappyWriter<Dest>>
    : ResetterByReset<FramedSnappyWriter<Dest>> {};

}  // namespace riegeli

#endif  // RIEGELI_BYTES_FRAMED_SNAPPY_WRITER_H_
