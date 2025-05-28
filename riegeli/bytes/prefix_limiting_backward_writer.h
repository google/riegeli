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

#ifndef RIEGELI_BYTES_PREFIX_LIMITING_BACKWARD_WRITER_H_
#define RIEGELI_BYTES_PREFIX_LIMITING_BACKWARD_WRITER_H_

#include <stddef.h>

#include <optional>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
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

template <typename Src>
class PrefixLimitingReader;
class Reader;

// Template parameter independent part of `PrefixLimitingBackwardWriter`.
class PrefixLimitingBackwardWriterBase : public BackwardWriter {
 public:
  class Options {
   public:
    Options() noexcept {}

    // The base position of the original `BackwardWriter`. It must be at least
    // as large as the initial position.
    //
    // `std::nullopt` means the current position.
    //
    // Default: `std::nullopt`.
    Options& set_base_pos(std::optional<Position> base_pos) &
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      base_pos_ = base_pos;
      return *this;
    }
    Options&& set_base_pos(std::optional<Position> base_pos) &&
        ABSL_ATTRIBUTE_LIFETIME_BOUND {
      return std::move(set_base_pos(base_pos));
    }
    std::optional<Position> base_pos() const { return base_pos_; }

   private:
    std::optional<Position> base_pos_;
  };

  // Returns the original `BackwardWriter`. Unchanged by `Close()`.
  virtual BackwardWriter* DestWriter() const ABSL_ATTRIBUTE_LIFETIME_BOUND = 0;

  // Returns the base position of the original `BackwardWriter`.
  Position base_pos() const { return base_pos_; }
  bool SupportsTruncate() override;

 protected:
  using BackwardWriter::BackwardWriter;

  PrefixLimitingBackwardWriterBase(
      PrefixLimitingBackwardWriterBase&& that) noexcept;
  PrefixLimitingBackwardWriterBase& operator=(
      PrefixLimitingBackwardWriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset();
  void Initialize(BackwardWriter* dest, std::optional<Position> base_pos);
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateOverDest(absl::Status status);

  // Sets cursor of `dest` to cursor of `*this`.
  void SyncBuffer(BackwardWriter& dest);

  // Sets buffer pointers of `*this` to buffer pointers of `dest`, adjusting
  // `start()` to hide data already written. Fails `*this` if `dest` failed.
  void MakeBuffer(BackwardWriter& dest);

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
  // This template is defined and used only in prefix_limiting_writer.cc.
  template <typename Src>
  bool WriteInternal(Src&& src);

  Position base_pos_ = 0;

  // Invariants if `ok()`:
  //   `start() == DestWriter()->cursor()`
  //   `limit() == DestWriter()->limit()`
  //   `start_pos() == DestWriter()->pos() - base_pos_`
};

// A `BackwardWriter` which writes to another `BackwardWriter`, hiding data
// before a base position, and reporting positions shifted so that the base
// position appears as 0.
//
// `PositionShiftingBackwardWriter` can be used for shifting positions in the
// other direction.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the original `BackwardWriter`. `Dest` must support
// `Dependency<BackwardWriter*, Dest>`, e.g.
// `BackwardWriter*` (not owned, default), `ChainBackwardWriter<>` (owned),
// `std::unique_ptr<BackwardWriter>` (owned),
// `Any<BackwardWriter*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as `TargetT` of the
// type of the first constructor argument.
//
// The original `BackwardWriter` must not be accessed until the
// `PrefixLimitingBackwardWriter` is closed or no longer used, except that it is
// allowed to read the destination of the original `BackwardWriter` immediately
// after `Flush()`.
template <typename Dest = BackwardWriter*>
class PrefixLimitingBackwardWriter : public PrefixLimitingBackwardWriterBase {
 public:
  // Creates a closed `PrefixLimitingBackwardWriter`.
  explicit PrefixLimitingBackwardWriter(Closed) noexcept
      : PrefixLimitingBackwardWriterBase(kClosed) {}

  // Will write to the original `BackwardWriter` provided by `dest`.
  explicit PrefixLimitingBackwardWriter(Initializer<Dest> dest,
                                        Options options = Options());

  PrefixLimitingBackwardWriter(PrefixLimitingBackwardWriter&& that) = default;
  PrefixLimitingBackwardWriter& operator=(PrefixLimitingBackwardWriter&& that) =
      default;

  // Makes `*this` equivalent to a newly constructed
  // `PrefixLimitingBackwardWriter`. This avoids constructing a temporary
  // `PrefixLimitingBackwardWriter` and moving from it.
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
  void SetWriteSizeHintImpl(std::optional<Position> write_size_hint) override;
  bool FlushImpl(FlushType flush_type) override;

 private:
  class Mover;

  // The object providing and possibly owning the original `BackwardWriter`.
  MovingDependency<BackwardWriter*, Dest, Mover> dest_;
};

explicit PrefixLimitingBackwardWriter(Closed)
    -> PrefixLimitingBackwardWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit PrefixLimitingBackwardWriter(
    Dest&& dest, PrefixLimitingBackwardWriterBase::Options options =
                     PrefixLimitingBackwardWriterBase::Options())
    -> PrefixLimitingBackwardWriter<TargetT<Dest>>;

// Implementation details follow.

inline PrefixLimitingBackwardWriterBase::PrefixLimitingBackwardWriterBase(
    PrefixLimitingBackwardWriterBase&& that) noexcept
    : BackwardWriter(static_cast<BackwardWriter&&>(that)),
      base_pos_(that.base_pos_) {}

inline PrefixLimitingBackwardWriterBase&
PrefixLimitingBackwardWriterBase::operator=(
    PrefixLimitingBackwardWriterBase&& that) noexcept {
  BackwardWriter::operator=(static_cast<BackwardWriter&&>(that));
  base_pos_ = that.base_pos_;
  return *this;
}

inline void PrefixLimitingBackwardWriterBase::Reset(Closed) {
  BackwardWriter::Reset(kClosed);
  base_pos_ = 0;
}

inline void PrefixLimitingBackwardWriterBase::Reset() {
  BackwardWriter::Reset();
  // `base_pos_` will be set by `Initialize()`.
}

inline void PrefixLimitingBackwardWriterBase::Initialize(
    BackwardWriter* dest, std::optional<Position> base_pos) {
  RIEGELI_ASSERT_NE(dest, nullptr)
      << "Failed precondition of PrefixLimitingBackwardWriter: "
         "null BackwardWriter pointer";
  if (base_pos == std::nullopt) {
    base_pos_ = dest->pos();
  } else {
    RIEGELI_ASSERT_LE(*base_pos, dest->pos())
        << "Failed precondition of PrefixLimitingBackwardWriter: "
           "current position below the base position";
    base_pos_ = *base_pos;
  }
  MakeBuffer(*dest);
}

inline void PrefixLimitingBackwardWriterBase::SyncBuffer(BackwardWriter& dest) {
  dest.set_cursor(cursor());
}

inline void PrefixLimitingBackwardWriterBase::MakeBuffer(BackwardWriter& dest) {
  RIEGELI_ASSERT_GE(dest.pos(), base_pos_)
      << "PrefixLimitingBackwardWriter destination "
         "changed position unexpectedly";
  set_buffer(dest.limit(), dest.available());
  set_start_pos(dest.pos() - base_pos_);
  if (ABSL_PREDICT_FALSE(!dest.ok())) {
    FailWithoutAnnotation(AnnotateOverDest(dest.status()));
  }
}

template <typename Dest>
class PrefixLimitingBackwardWriter<Dest>::Mover {
 public:
  static auto member() { return &PrefixLimitingBackwardWriter::dest_; }

  explicit Mover(PrefixLimitingBackwardWriter& self,
                 PrefixLimitingBackwardWriter& that)
      : uses_buffer_(self.start() != nullptr) {
    // Buffer pointers are already moved so `SyncBuffer()` is called on `self`.
    // `dest_` is not moved yet so `dest_` is taken from `that`.
    if (uses_buffer_) self.SyncBuffer(*that.dest_);
  }

  void Done(PrefixLimitingBackwardWriter& self) {
    if (uses_buffer_) self.MakeBuffer(*self.dest_);
  }

 private:
  bool uses_buffer_;
};

template <typename Dest>
inline PrefixLimitingBackwardWriter<Dest>::PrefixLimitingBackwardWriter(
    Initializer<Dest> dest, Options options)
    : dest_(std::move(dest)) {
  Initialize(dest_.get(), options.base_pos());
}

template <typename Dest>
inline void PrefixLimitingBackwardWriter<Dest>::Reset(Closed) {
  PrefixLimitingBackwardWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void PrefixLimitingBackwardWriter<Dest>::Reset(Initializer<Dest> dest,
                                                      Options options) {
  PrefixLimitingBackwardWriterBase::Reset();
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), options.base_pos());
}

template <typename Dest>
void PrefixLimitingBackwardWriter<Dest>::Done() {
  PrefixLimitingBackwardWriterBase::Done();
  if (dest_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) {
      FailWithoutAnnotation(AnnotateOverDest(dest_->status()));
    }
  }
}

template <typename Dest>
void PrefixLimitingBackwardWriter<Dest>::SetWriteSizeHintImpl(
    std::optional<Position> write_size_hint) {
  if (dest_.IsOwning()) {
    SyncBuffer(*dest_);
    dest_->SetWriteSizeHint(
        write_size_hint == std::nullopt
            ? std::nullopt
            : std::make_optional(SaturatingAdd(base_pos(), *write_size_hint)));
    MakeBuffer(*dest_);
  }
}

template <typename Dest>
bool PrefixLimitingBackwardWriter<Dest>::FlushImpl(FlushType flush_type) {
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

#endif  // RIEGELI_BYTES_PREFIX_LIMITING_BACKWARD_WRITER_H_
