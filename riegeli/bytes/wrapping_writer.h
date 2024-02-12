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

#include <tuple>
#include <type_traits>
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
// `AnyDependency<Writer*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as the value type of
// the first constructor argument. This requires C++17.
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
  explicit WrappingWriter(const Dest& dest);
  explicit WrappingWriter(Dest&& dest);

  // Will write to the original `Writer` provided by a `Dest` constructed from
  // elements of `dest_args`. This avoids constructing a temporary `Dest` and
  // moving from it.
  template <typename... DestArgs>
  explicit WrappingWriter(std::tuple<DestArgs...> dest_args);

  WrappingWriter(WrappingWriter&& that) noexcept;
  WrappingWriter& operator=(WrappingWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `WrappingWriter`. This
  // avoids constructing a temporary `WrappingWriter` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(const Dest& dest);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Dest&& dest);
  template <typename... DestArgs>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(std::tuple<DestArgs...> dest_args);

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
  // Moves `that.dest_` to `dest_`. Buffer pointers are already moved from
  // `dest_` to `*this`; adjust them to match `dest_`.
  void MoveDest(WrappingWriter&& that);

  // The object providing and possibly owning the original `Writer`.
  Dependency<Writer*, Dest> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit WrappingWriter(Closed) -> WrappingWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit WrappingWriter(const Dest& dest) -> WrappingWriter<std::decay_t<Dest>>;
template <typename Dest>
explicit WrappingWriter(Dest&& dest) -> WrappingWriter<std::decay_t<Dest>>;
template <typename... DestArgs>
explicit WrappingWriter(std::tuple<DestArgs...> dest_args)
    -> WrappingWriter<DeleteCtad<std::tuple<DestArgs...>>>;
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
inline WrappingWriter<Dest>::WrappingWriter(const Dest& dest) : dest_(dest) {
  Initialize(dest_.get());
}

template <typename Dest>
inline WrappingWriter<Dest>::WrappingWriter(Dest&& dest)
    : dest_(std::move(dest)) {
  Initialize(dest_.get());
}

template <typename Dest>
template <typename... DestArgs>
inline WrappingWriter<Dest>::WrappingWriter(std::tuple<DestArgs...> dest_args)
    : dest_(std::move(dest_args)) {
  Initialize(dest_.get());
}

template <typename Dest>
inline WrappingWriter<Dest>::WrappingWriter(WrappingWriter&& that) noexcept
    : WrappingWriterBase(static_cast<WrappingWriterBase&&>(that)) {
  MoveDest(std::move(that));
}

template <typename Dest>
inline WrappingWriter<Dest>& WrappingWriter<Dest>::operator=(
    WrappingWriter&& that) noexcept {
  WrappingWriterBase::operator=(static_cast<WrappingWriterBase&&>(that));
  MoveDest(std::move(that));
  return *this;
}

template <typename Dest>
inline void WrappingWriter<Dest>::Reset(Closed) {
  WrappingWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void WrappingWriter<Dest>::Reset(const Dest& dest) {
  WrappingWriterBase::Reset();
  dest_.Reset(dest);
  Initialize(dest_.get());
}

template <typename Dest>
inline void WrappingWriter<Dest>::Reset(Dest&& dest) {
  WrappingWriterBase::Reset();
  dest_.Reset(std::move(dest));
  Initialize(dest_.get());
}

template <typename Dest>
template <typename... DestArgs>
inline void WrappingWriter<Dest>::Reset(std::tuple<DestArgs...> dest_args) {
  WrappingWriterBase::Reset();
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get());
}

template <typename Dest>
inline void WrappingWriter<Dest>::MoveDest(WrappingWriter&& that) {
  if (dest_.kIsStable || that.dest_ == nullptr) {
    dest_ = std::move(that.dest_);
  } else {
    // Buffer pointers are already moved so `SyncBuffer()` is called on `*this`,
    // `dest_` is not moved yet so `dest_` is taken from `that`.
    SyncBuffer(*that.dest_);
    dest_ = std::move(that.dest_);
    MakeBuffer(*dest_);
  }
}

template <typename Dest>
void WrappingWriter<Dest>::Done() {
  WrappingWriterBase::Done();
  if (dest_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) {
      FailWithoutAnnotation(dest_->status());
    }
  }
}

template <typename Dest>
void WrappingWriter<Dest>::SetWriteSizeHintImpl(
    absl::optional<Position> write_size_hint) {
  if (dest_.is_owning()) {
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
  if (flush_type != FlushType::kFromObject || dest_.is_owning()) {
    flush_ok = dest_->Flush(flush_type);
  }
  MakeBuffer(*dest_);
  return flush_ok;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_WRAPPING_WRITER_H_
