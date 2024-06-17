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

#ifndef RIEGELI_BYTES_WRAPPING_WRITER_H_
#define RIEGELI_BYTES_WRAPPING_WRITER_H_

#include <stddef.h>

#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/moving_dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

class Reader;

// Template parameter independent part of `WrappingWriter`.
class WrappingWriterBase : public Writer {
 public:
  // Returns the original `Writer`. Unchanged by `Close()`.
  virtual Writer* DestWriter() const = 0;

  bool PrefersCopying() const override;
  bool SupportsRandomAccess() override;
  bool SupportsTruncate() override;
  bool SupportsReadMode() override;

 protected:
  using Writer::Writer;

  WrappingWriterBase(WrappingWriterBase&& that) noexcept;
  WrappingWriterBase& operator=(WrappingWriterBase&& that) noexcept;

  void Initialize(Writer* dest);

  // Sets cursor of `dest` to cursor of `*this`.
  void SyncBuffer(Writer& dest);

  // Sets buffer pointers of `*this` to buffer pointers of `dest`. Fails `*this`
  // if `dest` failed.
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
  bool WriteZerosSlow(Position length) override;
  bool SeekSlow(Position new_pos) override;
  absl::optional<Position> SizeImpl() override;
  bool TruncateImpl(Position new_size) override;
  Reader* ReadModeImpl(Position initial_pos) override;

 private:
  // This template is defined and used only in wrapping_writer.cc.
  template <typename Src>
  bool WriteInternal(Src&& src);

  // Invariants if `ok()`:
  //   `start() == DestWriter()->start()`
  //   `limit() == DestWriter()->limit()`
  //   `start_pos() == DestWriter()->start_pos()`
};

// A `Writer` which just writes to another `Writer`.
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
// The original `Writer` must not be accessed until the `WrappingWriter` is
// closed or no longer used, except that it is allowed to read the destination
// of the original `Writer` immediately after `Flush()`.
template <typename Dest = Writer*>
class WrappingWriter : public WrappingWriterBase {
 public:
  // Creates a closed `WrappingWriter`.
  explicit WrappingWriter(Closed) noexcept : WrappingWriterBase(kClosed) {}

  // Will write to the original `Writer` provided by `dest`.
  explicit WrappingWriter(Initializer<Dest> dest);

  WrappingWriter(WrappingWriter&& that) = default;
  WrappingWriter& operator=(WrappingWriter&& that) = default;

  // Makes `*this` equivalent to a newly constructed `WrappingWriter`. This
  // avoids constructing a temporary `WrappingWriter` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Initializer<Dest> dest);

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
explicit WrappingWriter(Closed) -> WrappingWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit WrappingWriter(Dest&& dest)
    -> WrappingWriter<InitializerTargetT<Dest>>;
#endif

// Implementation details follow.

inline WrappingWriterBase::WrappingWriterBase(
    WrappingWriterBase&& that) noexcept
    : Writer(static_cast<Writer&&>(that)) {}

inline WrappingWriterBase& WrappingWriterBase::operator=(
    WrappingWriterBase&& that) noexcept {
  Writer::operator=(static_cast<Writer&&>(that));
  return *this;
}

inline void WrappingWriterBase::Initialize(Writer* dest) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of WrappingWriter: null Writer pointer";
  MakeBuffer(*dest);
}

inline void WrappingWriterBase::SyncBuffer(Writer& dest) {
  dest.set_cursor(cursor());
}

inline void WrappingWriterBase::MakeBuffer(Writer& dest) {
  set_buffer(dest.start(), dest.start_to_limit(), dest.start_to_cursor());
  set_start_pos(dest.start_pos());
  if (ABSL_PREDICT_FALSE(!dest.ok())) FailWithoutAnnotation(dest.status());
}

template <typename Dest>
class WrappingWriter<Dest>::Mover {
 public:
  static auto member() { return &WrappingWriter::dest_; }

  explicit Mover(WrappingWriter& self, WrappingWriter& that)
      : uses_buffer_(self.start() != nullptr) {
    // Buffer pointers are already moved so `SyncBuffer()` is called on `self`.
    // `dest_` is not moved yet so `dest_` is taken from `that`.
    if (uses_buffer_) self.SyncBuffer(*that.dest_);
  }

  void Done(WrappingWriter& self) {
    if (uses_buffer_) self.MakeBuffer(*self.dest_);
  }

 private:
  bool uses_buffer_;
};

template <typename Dest>
inline WrappingWriter<Dest>::WrappingWriter(Initializer<Dest> dest)
    : dest_(std::move(dest)) {
  Initialize(dest_.get());
}

template <typename Dest>
inline void WrappingWriter<Dest>::Reset(Closed) {
  WrappingWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void WrappingWriter<Dest>::Reset(Initializer<Dest> dest) {
  WrappingWriterBase::Reset();
  dest_.Reset(std::move(dest));
  Initialize(dest_.get());
}

template <typename Dest>
void WrappingWriter<Dest>::Done() {
  WrappingWriterBase::Done();
  if (dest_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) {
      FailWithoutAnnotation(dest_->status());
    }
  }
}

template <typename Dest>
void WrappingWriter<Dest>::SetWriteSizeHintImpl(
    absl::optional<Position> write_size_hint) {
  if (dest_.IsOwning()) {
    SyncBuffer(*dest_);
    dest_->SetWriteSizeHint(write_size_hint);
    MakeBuffer(*dest_);
  }
}

template <typename Dest>
bool WrappingWriter<Dest>::FlushImpl(FlushType flush_type) {
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

#endif  // RIEGELI_BYTES_WRAPPING_WRITER_H_
