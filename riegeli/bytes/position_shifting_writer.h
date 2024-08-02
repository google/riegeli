// Copyright 2021 Google LLC
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

#ifndef RIEGELI_BYTES_POSITION_SHIFTING_WRITER_H_
#define RIEGELI_BYTES_POSITION_SHIFTING_WRITER_H_

#include <stddef.h>

#include <limits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/byte_fill.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/external_ref.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/moving_dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

template <typename Src>
class PositionShiftingReader;
class Reader;

// Template parameter independent part of `PositionShiftingWriter`.
class PositionShiftingWriterBase : public Writer {
 public:
  class Options {
   public:
    Options() noexcept {}

    // The base position of the new `Writer`.
    //
    // Default: 0.
    Options& set_base_pos(Position base_pos) & {
      base_pos_ = base_pos;
      return *this;
    }
    Options&& set_base_pos(Position base_pos) && {
      return std::move(set_base_pos(base_pos));
    }
    Position base_pos() const { return base_pos_; }

   private:
    Position base_pos_ = 0;
  };

  // Returns the new `Writer`. Unchanged by `Close()`.
  virtual Writer* DestWriter() const = 0;

  // Returns the base position of the original `Writer`.
  Position base_pos() const { return base_pos_; }

  bool SupportsRandomAccess() override;
  bool SupportsTruncate() override;
  bool SupportsReadMode() override;

 protected:
  explicit PositionShiftingWriterBase(Closed) noexcept : Writer(kClosed) {}

  explicit PositionShiftingWriterBase(Position base_pos);

  PositionShiftingWriterBase(PositionShiftingWriterBase&& that) noexcept;
  PositionShiftingWriterBase& operator=(
      PositionShiftingWriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset(Position base_pos);
  void Initialize(Writer* dest);
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateOverDest(absl::Status status);

  // Sets cursor of `dest` to cursor of `*this`.
  void SyncBuffer(Writer& dest);

  // Sets buffer pointers of `*this` to buffer pointers of `dest`, adjusting
  // `start()` to hide data already written. Fails `*this` if `dest` failed.
  void MakeBuffer(Writer& dest);

  void Done() override;
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;
  bool PushSlow(size_t min_length, size_t recommended_length) override;
  using Writer::WriteSlow;
  bool WriteSlow(absl::string_view src) override;
  bool WriteSlow(const Chain& src) override;
  bool WriteSlow(Chain&& src) override;
  bool WriteSlow(const absl::Cord& src) override;
  bool WriteSlow(absl::Cord&& src) override;
  bool WriteSlow(ExternalRef src) override;
  bool WriteSlow(ByteFill src) override;
  bool SeekSlow(Position new_pos) override;
  absl::optional<Position> SizeImpl() override;
  bool TruncateImpl(Position new_size) override;
  Reader* ReadModeImpl(Position initial_pos) override;

 private:
  ABSL_ATTRIBUTE_COLD bool FailUnderflow(Position new_pos, Object& object);

  // This template is defined and used only in position_shifting_writer.cc.
  template <typename Src>
  bool WriteInternal(Src&& src);

  Position base_pos_ = 0;

  AssociatedReader<PositionShiftingReader<Reader*>> associated_reader_;

  // Invariants if `ok()`:
  //   `start() == DestWriter()->cursor()`
  //   `limit() == DestWriter()->limit()`
  //   `start_pos() == DestWriter()->pos() + base_pos_`
};

// A `Writer` which writes to another `Writer`, reporting positions shifted so
// that the beginning appears as the given base position. Seeking back before
// the base position fails.
//
// `PrefixLimitingWriter` can be used for shifting positions in the other
// direction.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the original `Writer`. `Dest` must support
// `Dependency<Writer*, Dest>`, e.g. `Writer*` (not owned, default),
// `ChainWriter<>` (owned), `std::unique_ptr<Writer>` (owned),
// `Any<Writer*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as
// `InitializerTargetT` of the type of the first constructor argument.
// This requires C++17.
//
// The original `Writer` must not be accessed until the `PositionShiftingWriter`
// is closed or no longer used, except that it is allowed to read the
// destination of the original `Writer` immediately after `Flush()`.
template <typename Dest = Writer*>
class PositionShiftingWriter : public PositionShiftingWriterBase {
 public:
  // Creates a closed `PositionShiftingWriter`.
  explicit PositionShiftingWriter(Closed) noexcept
      : PositionShiftingWriterBase(kClosed) {}

  // Will write to the original `Writer` provided by `dest`.
  explicit PositionShiftingWriter(Initializer<Dest> dest,
                                  Options options = Options());

  PositionShiftingWriter(PositionShiftingWriter&& that) = default;
  PositionShiftingWriter& operator=(PositionShiftingWriter&& that) = default;

  // Makes `*this` equivalent to a newly constructed `PositionShiftingWriter`.
  // This avoids constructing a temporary `PositionShiftingWriter` and moving
  // from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Dest> dest,
                                          Options options = Options());

  // Returns the object providing and possibly owning the original `Writer`.
  // Unchanged by `Close()`.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  Writer* DestWriter() const override { return dest_.get(); }

 protected:
  void Done() override;
  void SetWriteSizeHintImpl(absl::optional<Position> write_size_hint) override;
  bool FlushImpl(FlushType flush_type) override;

 private:
  class Mover;

  // The object providing and possibly owning the original `Writer`.
  MovingDependency<Writer*, Dest, Mover> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit PositionShiftingWriter(Closed)
    -> PositionShiftingWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit PositionShiftingWriter(Dest&& dest,
                                PositionShiftingWriterBase::Options options =
                                    PositionShiftingWriterBase::Options())
    -> PositionShiftingWriter<InitializerTargetT<Dest>>;
#endif

// Implementation details follow.

inline PositionShiftingWriterBase::PositionShiftingWriterBase(Position base_pos)
    : base_pos_(base_pos) {}

inline PositionShiftingWriterBase::PositionShiftingWriterBase(
    PositionShiftingWriterBase&& that) noexcept
    : Writer(static_cast<Writer&&>(that)),
      base_pos_(that.base_pos_),
      associated_reader_(std::move(that.associated_reader_)) {}

inline PositionShiftingWriterBase& PositionShiftingWriterBase::operator=(
    PositionShiftingWriterBase&& that) noexcept {
  Writer::operator=(static_cast<Writer&&>(that));
  base_pos_ = that.base_pos_;
  associated_reader_ = std::move(that.associated_reader_);
  return *this;
}

inline void PositionShiftingWriterBase::Reset(Closed) {
  Writer::Reset(kClosed);
  base_pos_ = 0;
  associated_reader_.Reset();
}

inline void PositionShiftingWriterBase::Reset(Position base_pos) {
  Writer::Reset();
  base_pos_ = base_pos;
  associated_reader_.Reset();
}

inline void PositionShiftingWriterBase::Initialize(Writer* dest) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of PositionShiftingWriter: null Writer pointer";
  MakeBuffer(*dest);
}

inline void PositionShiftingWriterBase::SyncBuffer(Writer& dest) {
  dest.set_cursor(cursor());
}

inline void PositionShiftingWriterBase::MakeBuffer(Writer& dest) {
  if (ABSL_PREDICT_FALSE(dest.pos() >
                         std::numeric_limits<Position>::max() - base_pos_)) {
    FailOverflow();
    return;
  }
  set_buffer(dest.cursor(), dest.available());
  set_start_pos(dest.pos() + base_pos_);
  if (ABSL_PREDICT_FALSE(!dest.ok())) {
    FailWithoutAnnotation(AnnotateOverDest(dest.status()));
  }
}

template <typename Dest>
class PositionShiftingWriter<Dest>::Mover {
 public:
  static auto member() { return &PositionShiftingWriter::dest_; }

  explicit Mover(PositionShiftingWriter& self, PositionShiftingWriter& that)
      : uses_buffer_(self.start() != nullptr) {
    // Buffer pointers are already moved so `SyncBuffer()` is called on `self`.
    // `dest_` is not moved yet so `dest_` is taken from `that`.
    if (uses_buffer_) self.SyncBuffer(*that.dest_);
  }

  void Done(PositionShiftingWriter& self) {
    if (uses_buffer_) self.MakeBuffer(*self.dest_);
  }

 private:
  bool uses_buffer_;
};

template <typename Dest>
inline PositionShiftingWriter<Dest>::PositionShiftingWriter(
    Initializer<Dest> dest, Options options)
    : PositionShiftingWriterBase(options.base_pos()), dest_(std::move(dest)) {
  Initialize(dest_.get());
}

template <typename Dest>
inline void PositionShiftingWriter<Dest>::Reset(Closed) {
  PositionShiftingWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void PositionShiftingWriter<Dest>::Reset(Initializer<Dest> dest,
                                                Options options) {
  PositionShiftingWriterBase::Reset(options.base_pos());
  dest_.Reset(std::move(dest));
  Initialize(dest_.get());
}

template <typename Dest>
void PositionShiftingWriter<Dest>::Done() {
  PositionShiftingWriterBase::Done();
  if (dest_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) {
      FailWithoutAnnotation(AnnotateOverDest(dest_->status()));
    }
  }
}

template <typename Dest>
void PositionShiftingWriter<Dest>::SetWriteSizeHintImpl(
    absl::optional<Position> write_size_hint) {
  if (dest_.IsOwning()) {
    SyncBuffer(*dest_);
    dest_->SetWriteSizeHint(
        write_size_hint == absl::nullopt
            ? absl::nullopt
            : absl::make_optional(SaturatingAdd(base_pos(), *write_size_hint)));
    MakeBuffer(*dest_);
  }
}

template <typename Dest>
bool PositionShiftingWriter<Dest>::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  SyncBuffer(*dest_);
  bool flush_ok = true;
  if (flush_type != FlushType::kFromObject || dest_.IsOwning()) {
    flush_ok = dest_->Flush(flush_type);
  }
  MakeBuffer(*dest_);
  return flush_ok;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_POSITION_SHIFTING_WRITER_H_
