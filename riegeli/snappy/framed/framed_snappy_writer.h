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

#ifndef RIEGELI_SNAPPY_FRAMED_FRAMED_SNAPPY_WRITER_H_
#define RIEGELI_SNAPPY_FRAMED_FRAMED_SNAPPY_WRITER_H_

#include <stddef.h>

#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/types/optional.h"
#include "riegeli/base/buffer.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/pushable_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

template <typename Src>
class FramedSnappyReader;
class Reader;

// Template parameter independent part of `FramedSnappyWriter`.
class FramedSnappyWriterBase : public PushableWriter {
 public:
  class Options {};

  // Returns the compressed `Writer`. Unchanged by `Close()`.
  virtual Writer* DestWriter() = 0;
  virtual const Writer* DestWriter() const = 0;

  bool SupportsReadMode() override;

 protected:
  explicit FramedSnappyWriterBase(Closed) noexcept : PushableWriter(kClosed) {}

  explicit FramedSnappyWriterBase() {}

  FramedSnappyWriterBase(FramedSnappyWriterBase&& that) noexcept;
  FramedSnappyWriterBase& operator=(FramedSnappyWriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset();
  void Initialize(Writer* dest);
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateOverDest(absl::Status status);

  void Done() override;
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;
  void SetWriteSizeHintImpl(absl::optional<Position> write_size_hint) override;
  bool PushBehindScratch(size_t recommended_length) override;
  bool FlushBehindScratch(FlushType flush_type);
  Reader* ReadModeBehindScratch(Position initial_pos) override;

 private:
  // Compresses buffered data, but unlike `PushSlow()`, does not ensure that a
  // buffer is allocated.
  //
  // Precondition: `ok()`
  //
  // Postcondition: `start_to_cursor() == 0`
  bool PushInternal(Writer& dest);

  absl::optional<Position> size_hint_;
  Position initial_compressed_pos_ = 0;
  // Buffered uncompressed data.
  Buffer uncompressed_;

  AssociatedReader<FramedSnappyReader<Reader*>> associated_reader_;

  // Invariants if scratch is not used:
  //   `start() == nullptr` or `start() == uncompressed_.data()`
  //   `start_to_limit() <= snappy::kBlockSize`
};

// A `Writer` which compresses data with framed Snappy format before passing it
// to another `Writer`:
// https://github.com/google/snappy/blob/master/framing_format.txt
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the compressed `Writer`. `Dest` must support
// `Dependency<Writer*, Dest>`, e.g. `Writer*` (not owned, default),
// `ChainWriter<>` (owned), `std::unique_ptr<Writer>` (owned),
// `AnyDependency<Writer*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as the value type of
// the first constructor argument. This requires C++17.
//
// The compressed `Writer` must not be accessed until the `FramedSnappyWriter`
// is closed or no longer used.
template <typename Dest = Writer*>
class FramedSnappyWriter : public FramedSnappyWriterBase {
 public:
  // Creates a closed `FramedSnappyWriter`.
  explicit FramedSnappyWriter(Closed) noexcept
      : FramedSnappyWriterBase(kClosed) {}

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
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(const Dest& dest,
                                          Options options = Options());
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Dest&& dest,
                                          Options options = Options());
  template <typename... DestArgs>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(std::tuple<DestArgs...> dest_args,
                                          Options options = Options());

  // Returns the object providing and possibly owning the compressed `Writer`.
  // Unchanged by `Close()`.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  Writer* DestWriter() override { return dest_.get(); }
  const Writer* DestWriter() const override { return dest_.get(); }

 protected:
  void Done() override;
  bool FlushImpl(FlushType flush_type) override;

 private:
  // The object providing and possibly owning the compressed `Writer`.
  Dependency<Writer*, Dest> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit FramedSnappyWriter(Closed) -> FramedSnappyWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit FramedSnappyWriter(
    const Dest& dest,
    FramedSnappyWriterBase::Options options = FramedSnappyWriterBase::Options())
    -> FramedSnappyWriter<std::decay_t<Dest>>;
template <typename Dest>
explicit FramedSnappyWriter(
    Dest&& dest,
    FramedSnappyWriterBase::Options options = FramedSnappyWriterBase::Options())
    -> FramedSnappyWriter<std::decay_t<Dest>>;
template <typename... DestArgs>
explicit FramedSnappyWriter(
    std::tuple<DestArgs...> dest_args,
    FramedSnappyWriterBase::Options options = FramedSnappyWriterBase::Options())
    -> FramedSnappyWriter<DeleteCtad<std::tuple<DestArgs...>>>;
#endif

// Implementation details follow.

inline FramedSnappyWriterBase::FramedSnappyWriterBase(
    FramedSnappyWriterBase&& that) noexcept
    : PushableWriter(static_cast<PushableWriter&&>(that)),
      size_hint_(that.size_hint_),
      initial_compressed_pos_(that.initial_compressed_pos_),
      uncompressed_(std::move(that.uncompressed_)),
      associated_reader_(std::move(that.associated_reader_)) {}

inline FramedSnappyWriterBase& FramedSnappyWriterBase::operator=(
    FramedSnappyWriterBase&& that) noexcept {
  PushableWriter::operator=(static_cast<PushableWriter&&>(that));
  size_hint_ = that.size_hint_;
  initial_compressed_pos_ = that.initial_compressed_pos_;
  uncompressed_ = std::move(that.uncompressed_);
  associated_reader_ = std::move(that.associated_reader_);
  return *this;
}

inline void FramedSnappyWriterBase::Reset(Closed) {
  PushableWriter::Reset(kClosed);
  size_hint_ = absl::nullopt;
  initial_compressed_pos_ = 0;
  uncompressed_ = Buffer();
  associated_reader_.Reset();
}

inline void FramedSnappyWriterBase::Reset() {
  PushableWriter::Reset();
  size_hint_ = absl::nullopt;
  initial_compressed_pos_ = 0;
  associated_reader_.Reset();
}

template <typename Dest>
inline FramedSnappyWriter<Dest>::FramedSnappyWriter(
    const Dest& dest, ABSL_ATTRIBUTE_UNUSED Options options)
    : dest_(dest) {
  Initialize(dest_.get());
}

template <typename Dest>
inline FramedSnappyWriter<Dest>::FramedSnappyWriter(
    Dest&& dest, ABSL_ATTRIBUTE_UNUSED Options options)
    : dest_(std::move(dest)) {
  Initialize(dest_.get());
}

template <typename Dest>
template <typename... DestArgs>
inline FramedSnappyWriter<Dest>::FramedSnappyWriter(
    std::tuple<DestArgs...> dest_args, ABSL_ATTRIBUTE_UNUSED Options options)
    : dest_(std::move(dest_args)) {
  Initialize(dest_.get());
}

template <typename Dest>
inline FramedSnappyWriter<Dest>::FramedSnappyWriter(
    FramedSnappyWriter&& that) noexcept
    : FramedSnappyWriterBase(static_cast<FramedSnappyWriterBase&&>(that)),
      dest_(std::move(that.dest_)) {}

template <typename Dest>
inline FramedSnappyWriter<Dest>& FramedSnappyWriter<Dest>::operator=(
    FramedSnappyWriter&& that) noexcept {
  FramedSnappyWriterBase::operator=(
      static_cast<FramedSnappyWriterBase&&>(that));
  dest_ = std::move(that.dest_);
  return *this;
}

template <typename Dest>
inline void FramedSnappyWriter<Dest>::Reset(Closed) {
  FramedSnappyWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void FramedSnappyWriter<Dest>::Reset(
    const Dest& dest, ABSL_ATTRIBUTE_UNUSED Options options) {
  FramedSnappyWriterBase::Reset();
  dest_.Reset(dest);
  Initialize(dest_.get());
}

template <typename Dest>
inline void FramedSnappyWriter<Dest>::Reset(
    Dest&& dest, ABSL_ATTRIBUTE_UNUSED Options options) {
  FramedSnappyWriterBase::Reset();
  dest_.Reset(std::move(dest));
  Initialize(dest_.get());
}

template <typename Dest>
template <typename... DestArgs>
inline void FramedSnappyWriter<Dest>::Reset(
    std::tuple<DestArgs...> dest_args, ABSL_ATTRIBUTE_UNUSED Options options) {
  FramedSnappyWriterBase::Reset();
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get());
}

template <typename Dest>
void FramedSnappyWriter<Dest>::Done() {
  FramedSnappyWriterBase::Done();
  if (dest_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) {
      FailWithoutAnnotation(AnnotateOverDest(dest_->status()));
    }
  }
}

template <typename Dest>
bool FramedSnappyWriter<Dest>::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!FramedSnappyWriterBase::FlushImpl(flush_type))) {
    return false;
  }
  if (flush_type != FlushType::kFromObject || dest_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Flush(flush_type))) {
      return FailWithoutAnnotation(AnnotateOverDest(dest_->status()));
    }
  }
  return true;
}

}  // namespace riegeli

#endif  // RIEGELI_SNAPPY_FRAMED_FRAMED_SNAPPY_WRITER_H_
