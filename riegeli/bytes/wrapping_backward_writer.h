// Copyright 2020 Google LLC
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

#ifndef RIEGELI_BYTES_WRAPPING_BACKWARD_WRITER_H_
#define RIEGELI_BYTES_WRAPPING_BACKWARD_WRITER_H_

#include <stddef.h>

#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
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

// Template parameter independent part of `WrappingBackwardWriter`.
class WrappingBackwardWriterBase : public BackwardWriter {
 public:
  // Returns the original `BackwardWriter`. Unchanged by `Close()`.
  virtual BackwardWriter* DestWriter() const ABSL_ATTRIBUTE_LIFETIME_BOUND = 0;

  bool SupportsTruncate() override;

 protected:
  using BackwardWriter::BackwardWriter;

  WrappingBackwardWriterBase(WrappingBackwardWriterBase&& that) noexcept;
  WrappingBackwardWriterBase& operator=(
      WrappingBackwardWriterBase&& that) noexcept;

  void Initialize(BackwardWriter* dest);

  // Sets cursor of `dest` to cursor of `*this`.
  void SyncBuffer(BackwardWriter& dest);

  // Sets buffer pointers of `*this` to buffer pointers of `dest`. Fails `*this`
  // if `dest` failed.
  void MakeBuffer(BackwardWriter& dest);

  void Done() override;
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;
  bool PushSlow(size_t min_length, size_t recommended_length) override;
  using BackwardWriter::WriteSlow;
  bool WriteSlow(absl::string_view src) override;
  bool WriteSlow(const Chain& src) override;
  bool WriteSlow(Chain&& src) override;
  bool WriteSlow(const absl::Cord& src) override;
  bool WriteSlow(absl::Cord&& src) override;
  bool WriteSlow(ExternalRef src) override;
  bool WriteSlow(ByteFill src) override;
  bool TruncateImpl(Position new_size) override;

 private:
  // This template is defined and used only in wrapping_backward_writer.cc.
  template <typename Src>
  bool WriteInternal(Src&& src);

  // Invariants if `ok()`:
  //   `start() == DestWriter()->start()`
  //   `limit() == DestWriter()->limit()`
  //   `start_pos() == DestWriter()->start_pos()`
};

// A `BackwardWriter` which juts writes to another `BackwardWriter`.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the original `BackwardWriter`. `Dest` must support
// `Dependency<BackwardWriter*, Dest>`, e.g.
// `BackwardWriter*` (not owned, default),
// `ChainBackwardWriter<>` (owned), `std::unique_ptr<BackwardWriter>` (owned),
// `Any<BackwardWriter*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as `TargetT` of the
// type of the first constructor argument. This requires C++17.
//
// The original `BackwardWriter` must not be accessed until the
// `WrappingBackwardWriter` is closed or no longer used, except that it is
// allowed to read the destination of the original `BackwardWriter` immediately
// after `Flush()`.
template <typename Dest = BackwardWriter*>
class WrappingBackwardWriter : public WrappingBackwardWriterBase {
 public:
  // Creates a closed `WrappingBackwardWriter`.
  explicit WrappingBackwardWriter(Closed) noexcept
      : WrappingBackwardWriterBase(kClosed) {}

  // Will write to the original `BackwardWriter` provided by `dest`.
  explicit WrappingBackwardWriter(Initializer<Dest> dest);

  WrappingBackwardWriter(WrappingBackwardWriter&& that) = default;
  WrappingBackwardWriter& operator=(WrappingBackwardWriter&& that) = default;

  // Makes `*this` equivalent to a newly constructed `WrappingBackwardWriter`.
  // This avoids constructing a temporary `WrappingBackwardWriter` and moving
  // from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Dest> dest);

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

// Support CTAD.
#if __cpp_deduction_guides
explicit WrappingBackwardWriter(Closed)
    -> WrappingBackwardWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit WrappingBackwardWriter(Dest&& dest)
    -> WrappingBackwardWriter<TargetT<Dest>>;
#endif

// Implementation details follow.

inline WrappingBackwardWriterBase::WrappingBackwardWriterBase(
    WrappingBackwardWriterBase&& that) noexcept
    : BackwardWriter(static_cast<BackwardWriter&&>(that)) {}

inline WrappingBackwardWriterBase& WrappingBackwardWriterBase::operator=(
    WrappingBackwardWriterBase&& that) noexcept {
  BackwardWriter::operator=(static_cast<BackwardWriter&&>(that));
  return *this;
}

inline void WrappingBackwardWriterBase::Initialize(BackwardWriter* dest) {
  RIEGELI_ASSERT_NE(dest, nullptr)
      << "Failed precondition of WrappingBackwardWriter: "
         "null BackwardWriter pointer";
  MakeBuffer(*dest);
}

inline void WrappingBackwardWriterBase::SyncBuffer(BackwardWriter& dest) {
  dest.set_cursor(cursor());
}

inline void WrappingBackwardWriterBase::MakeBuffer(BackwardWriter& dest) {
  set_buffer(dest.limit(), dest.start_to_limit(), dest.start_to_cursor());
  set_start_pos(dest.start_pos());
  if (ABSL_PREDICT_FALSE(!dest.ok())) FailWithoutAnnotation(dest.status());
}

template <typename Dest>
class WrappingBackwardWriter<Dest>::Mover {
 public:
  static auto member() { return &WrappingBackwardWriter::dest_; }

  explicit Mover(WrappingBackwardWriter& self, WrappingBackwardWriter& that)
      : uses_buffer_(self.start() != nullptr) {
    // Buffer pointers are already moved so `SyncBuffer()` is called on `self`.
    // `dest_` is not moved yet so `dest_` is taken from `that`.
    if (uses_buffer_) self.SyncBuffer(*that.dest_);
  }

  void Done(WrappingBackwardWriter& self) {
    if (uses_buffer_) self.MakeBuffer(*self.dest_);
  }

 private:
  bool uses_buffer_;
};

template <typename Dest>
inline WrappingBackwardWriter<Dest>::WrappingBackwardWriter(
    Initializer<Dest> dest)
    : dest_(std::move(dest)) {
  Initialize(dest_.get());
}

template <typename Dest>
inline void WrappingBackwardWriter<Dest>::Reset(Closed) {
  WrappingBackwardWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void WrappingBackwardWriter<Dest>::Reset(Initializer<Dest> dest) {
  WrappingBackwardWriterBase::Reset();
  dest_.Reset(std::move(dest));
  Initialize(dest_.get());
}

template <typename Dest>
void WrappingBackwardWriter<Dest>::Done() {
  WrappingBackwardWriterBase::Done();
  if (dest_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) {
      FailWithoutAnnotation(dest_->status());
    }
  }
}

template <typename Dest>
void WrappingBackwardWriter<Dest>::SetWriteSizeHintImpl(
    absl::optional<Position> write_size_hint) {
  if (dest_.IsOwning()) {
    SyncBuffer(*dest_);
    dest_->SetWriteSizeHint(write_size_hint);
    MakeBuffer(*dest_);
  }
}

template <typename Dest>
bool WrappingBackwardWriter<Dest>::FlushImpl(FlushType flush_type) {
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

#endif  // RIEGELI_BYTES_WRAPPING_BACKWARD_WRITER_H_
