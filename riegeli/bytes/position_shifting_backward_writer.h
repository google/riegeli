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

#ifndef RIEGELI_BYTES_POSITION_SHIFTING_BACKWARD_WRITER_H_
#define RIEGELI_BYTES_POSITION_SHIFTING_BACKWARD_WRITER_H_

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
#include "riegeli/bytes/backward_writer.h"

namespace riegeli {

// Template parameter independent part of `PositionShiftingBackwardWriter`.
class PositionShiftingBackwardWriterBase : public BackwardWriter {
 public:
  class Options {
   public:
    Options() noexcept {}

    // The base position of the new `BackwardWriter`.
    //
    // Default: 0.
    Options& set_base_pos(Position base_pos) & ABSL_ATTRIBUTE_LIFETIME_BOUND {
      base_pos_ = base_pos;
      return *this;
    }
    Options&& set_base_pos(Position base_pos) && ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_base_pos(base_pos));
    }
    Position base_pos() const { return base_pos_; }

   private:
    Position base_pos_ = 0;
  };

  // Returns the new `BackwardWriter`. Unchanged by `Close()`.
  virtual BackwardWriter* DestWriter() const = 0;

  // Returns the base position of the original `BackwardWriter`.
  Position base_pos() const { return base_pos_; }

  bool SupportsTruncate() override;

 protected:
  explicit PositionShiftingBackwardWriterBase(Closed) noexcept
      : BackwardWriter(kClosed) {}

  explicit PositionShiftingBackwardWriterBase(Position base_pos);

  PositionShiftingBackwardWriterBase(
      PositionShiftingBackwardWriterBase&& that) noexcept;
  PositionShiftingBackwardWriterBase& operator=(
      PositionShiftingBackwardWriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset(Position base_pos);
  void Initialize(BackwardWriter* dest);
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateOverDest(absl::Status status);

  // Sets cursor of `dest` to cursor of `*this`.
  void SyncBuffer(BackwardWriter& dest);

  // Sets buffer pointers of `*this` to buffer pointers of `dest`, adjusting
  // `start()` to hide data already written. Fails `*this` if `dest` failed
  // or there is not enough `Position` space for `min_length`.
  bool MakeBuffer(BackwardWriter& dest, size_t min_length = 0);

  void Done() override;
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;
  bool PushSlow(size_t min_length, size_t recommended_length) override;
  using BackwardWriter::WriteSlow;
  bool WriteSlow(absl::string_view src) override;
  bool WriteSlow(ExternalRef src) override;
  bool WriteSlow(const Chain& src) override;
  bool WriteSlow(Chain&& src) override;
  bool WriteSlow(const absl::Cord& src) override;
  bool WriteSlow(absl::Cord&& src) override;
  bool WriteSlow(ByteFill src) override;
  bool TruncateImpl(Position new_size) override;

 private:
  ABSL_ATTRIBUTE_COLD bool FailUnderflow(Position new_pos, Object& object);

  // This template is defined and used only in position_shifting_writer.cc.
  template <typename Src>
  bool WriteInternal(Src&& src);

  Position base_pos_ = 0;

  // Invariants if `ok()`:
  //   `start() == DestWriter()->cursor()`
  //   `limit() == DestWriter()->limit()`
  //   `start_pos() == DestWriter()->pos() + base_pos_`
};

// A `BackwardWriter` which writes to another `BackwardWriter`, reporting
// positions shifted so that the beginning appears as the given base position.
//
// `PrefixLimitingBackwardWriter` can be used for shifting positions in the
// other direction.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the original `BackwardWriter`. `Dest` must support
// `Dependency<BackwardWriter*, Dest>`, e.g.
// `BackwardWriter*` (not owned, default),
// `ChainNackwardWriter<>` (owned), `std::unique_ptr<BackwardWriter>` (owned),
// `Any<BackwardWriter*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as `TargetT` of the
// type of the first constructor argument.
//
// The original `BackwardWriter` must not be accessed until the
// `PositionShiftingBackwardWriter` is closed or no longer used, except that
// it is allowed to read the destination of the original `BackwardWriter`
// immediately after `Flush()`.
template <typename Dest = BackwardWriter*>
class PositionShiftingBackwardWriter
    : public PositionShiftingBackwardWriterBase {
 public:
  // Creates a closed `PositionShiftingBackwardWriter`.
  explicit PositionShiftingBackwardWriter(Closed) noexcept
      : PositionShiftingBackwardWriterBase(kClosed) {}

  // Will write to the original `BackwardWriter` provided by `dest`.
  explicit PositionShiftingBackwardWriter(Initializer<Dest> dest,
                                          Options options = Options());

  PositionShiftingBackwardWriter(PositionShiftingBackwardWriter&& that) =
      default;
  PositionShiftingBackwardWriter& operator=(
      PositionShiftingBackwardWriter&& that) = default;

  // Makes `*this` equivalent to a newly constructed
  // `PositionShiftingBackwardWriter`. This avoids constructing a temporary
  // `PositionShiftingBackwardWriter` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Dest> dest,
                                          Options options = Options());

  // Returns the object providing and possibly owning the original
  // `BackwardWriter`. Unchanged by `Close()`.
  Dest& dest() ABSL_ATTRIBUTE_LIFETIME_BOUND { return dest_.manager(); }
  const Dest& dest() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return dest_.manager();
  }
  BackwardWriter* DestWriter() const ABSL_ATTRIBUTE_LIFETIME_BOUND override {
    return dest_.get();
  }

 protected:
  void Done() override;
  void SetWriteSizeHintImpl(absl::optional<Position> write_size_hint) override;
  bool FlushImpl(FlushType flush_type) override;

 private:
  class Mover;

  // The object providing and possibly owning the original `BackwardWriter`.
  MovingDependency<BackwardWriter*, Dest, Mover> dest_;
};

explicit PositionShiftingBackwardWriter(Closed)
    -> PositionShiftingBackwardWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit PositionShiftingBackwardWriter(
    Dest&& dest, PositionShiftingBackwardWriterBase::Options options =
                     PositionShiftingBackwardWriterBase::Options())
    -> PositionShiftingBackwardWriter<TargetT<Dest>>;

// Implementation details follow.

inline PositionShiftingBackwardWriterBase::PositionShiftingBackwardWriterBase(
    Position base_pos)
    : base_pos_(base_pos) {}

inline PositionShiftingBackwardWriterBase::PositionShiftingBackwardWriterBase(
    PositionShiftingBackwardWriterBase&& that) noexcept
    : BackwardWriter(static_cast<BackwardWriter&&>(that)),
      base_pos_(that.base_pos_) {}

inline PositionShiftingBackwardWriterBase&
PositionShiftingBackwardWriterBase::operator=(
    PositionShiftingBackwardWriterBase&& that) noexcept {
  BackwardWriter::operator=(static_cast<BackwardWriter&&>(that));
  base_pos_ = that.base_pos_;
  return *this;
}

inline void PositionShiftingBackwardWriterBase::Reset(Closed) {
  BackwardWriter::Reset(kClosed);
  base_pos_ = 0;
}

inline void PositionShiftingBackwardWriterBase::Reset(Position base_pos) {
  BackwardWriter::Reset();
  base_pos_ = base_pos;
}

inline void PositionShiftingBackwardWriterBase::Initialize(
    BackwardWriter* dest) {
  RIEGELI_ASSERT_NE(dest, nullptr)
      << "Failed precondition of PositionShiftingBackwardWriter: "
         "null BackwardWriter pointer";
  MakeBuffer(*dest);
}

inline void PositionShiftingBackwardWriterBase::SyncBuffer(
    BackwardWriter& dest) {
  dest.set_cursor(cursor());
}

inline bool PositionShiftingBackwardWriterBase::MakeBuffer(BackwardWriter& dest,
                                                           size_t min_length) {
  const Position max_pos = std::numeric_limits<Position>::max() - base_pos_;
  if (ABSL_PREDICT_FALSE(dest.limit_pos() > max_pos)) {
    if (ABSL_PREDICT_FALSE(dest.pos() > max_pos)) {
      set_buffer(dest.cursor());
      set_start_pos(std::numeric_limits<Position>::max());
      return FailOverflow();
    }
    set_buffer(dest.cursor() - IntCast<size_t>(max_pos - dest.pos()),
               IntCast<size_t>(max_pos - dest.pos()));
    set_start_pos(dest.pos() + base_pos_);
    if (ABSL_PREDICT_FALSE(available() < min_length)) return FailOverflow();
  } else {
    set_buffer(dest.limit(), dest.available());
    set_start_pos(dest.pos() + base_pos_);
  }
  if (ABSL_PREDICT_FALSE(!dest.ok())) {
    return FailWithoutAnnotation(AnnotateOverDest(dest.status()));
  }
  return true;
}

template <typename Dest>
class PositionShiftingBackwardWriter<Dest>::Mover {
 public:
  static auto member() { return &PositionShiftingBackwardWriter::dest_; }

  explicit Mover(PositionShiftingBackwardWriter& self,
                 PositionShiftingBackwardWriter& that)
      : uses_buffer_(self.start() != nullptr) {
    // Buffer pointers are already moved so `SyncBuffer()` is called on `self`.
    // `dest_` is not moved yet so `dest_` is taken from `that`.
    if (uses_buffer_) self.SyncBuffer(*that.dest_);
  }

  void Done(PositionShiftingBackwardWriter& self) {
    if (uses_buffer_) self.MakeBuffer(*self.dest_);
  }

 private:
  bool uses_buffer_;
};

template <typename Dest>
inline PositionShiftingBackwardWriter<Dest>::PositionShiftingBackwardWriter(
    Initializer<Dest> dest, Options options)
    : PositionShiftingBackwardWriterBase(options.base_pos()),
      dest_(std::move(dest)) {
  Initialize(dest_.get());
}

template <typename Dest>
inline void PositionShiftingBackwardWriter<Dest>::Reset(Closed) {
  PositionShiftingBackwardWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void PositionShiftingBackwardWriter<Dest>::Reset(Initializer<Dest> dest,
                                                        Options options) {
  PositionShiftingBackwardWriterBase::Reset(options.base_pos());
  dest_.Reset(std::move(dest));
  Initialize(dest_.get());
}

template <typename Dest>
void PositionShiftingBackwardWriter<Dest>::Done() {
  PositionShiftingBackwardWriterBase::Done();
  if (dest_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) {
      FailWithoutAnnotation(AnnotateOverDest(dest_->status()));
    }
  }
}

template <typename Dest>
void PositionShiftingBackwardWriter<Dest>::SetWriteSizeHintImpl(
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
bool PositionShiftingBackwardWriter<Dest>::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  SyncBuffer(*dest_);
  bool flush_ok = true;
  if (flush_type != FlushType::kFromObject || dest_.IsOwning()) {
    flush_ok = dest_->Flush(flush_type);
  }
  return MakeBuffer(*dest_) && flush_ok;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_POSITION_SHIFTING_BACKWARD_WRITER_H_
