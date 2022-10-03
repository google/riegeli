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

#ifndef RIEGELI_BYTES_WRAPPED_BACKWARD_WRITER_H_
#define RIEGELI_BYTES_WRAPPED_BACKWARD_WRITER_H_

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
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/backward_writer.h"

namespace riegeli {

// Template parameter independent part of `WrappedBackwardWriter`.
class WrappedBackwardWriterBase : public BackwardWriter {
 public:
  // Returns the original `BackwardWriter`. Unchanged by `Close()`.
  virtual BackwardWriter* DestWriter() = 0;
  virtual const BackwardWriter* DestWriter() const = 0;

  bool PrefersCopying() const override;
  bool SupportsTruncate() override;

 protected:
  using BackwardWriter::BackwardWriter;

  WrappedBackwardWriterBase(WrappedBackwardWriterBase&& that) noexcept;
  WrappedBackwardWriterBase& operator=(
      WrappedBackwardWriterBase&& that) noexcept;

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
  bool WriteZerosSlow(Position length) override;
  bool TruncateImpl(Position new_size) override;

 private:
  // This template is defined and used only in wrapped_backward_writer.cc.
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
// `std::unique_ptr<BackwardWriter>` (owned), `ChainBackwardWriter<>` (owned).
//
// By relying on CTAD the template argument can be deduced as the value type of
// the first constructor argument. This requires C++17.
//
// The original `BackwardWriter` must not be accessed until the
// `WrappedBackwardWriter` is closed or no longer used, except that it is
// allowed to read the destination of the original `BackwardWriter` immediately
// after `Flush()`.
template <typename Dest = BackwardWriter*>
class WrappedBackwardWriter : public WrappedBackwardWriterBase {
 public:
  // Creates a closed `WrappedBackwardWriter`.
  explicit WrappedBackwardWriter(Closed) noexcept
      : WrappedBackwardWriterBase(kClosed) {}

  // Will write to the original `BackwardWriter` provided by `dest`.
  explicit WrappedBackwardWriter(const Dest& dest);
  explicit WrappedBackwardWriter(Dest&& dest);

  // Will write to the original `BackwardWriter` provided by a `Dest`
  // constructed from elements of `dest_args`. This avoids constructing a
  // temporary `Dest` and moving from it.
  template <typename... DestArgs>
  explicit WrappedBackwardWriter(std::tuple<DestArgs...> dest_args);

  WrappedBackwardWriter(WrappedBackwardWriter&& that) noexcept;
  WrappedBackwardWriter& operator=(WrappedBackwardWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `WrappedBackwardWriter`.
  // This avoids constructing a temporary `WrappedBackwardWriter` and moving
  // from it.
  void Reset(Closed);
  void Reset(const Dest& dest);
  void Reset(Dest&& dest);
  template <typename... DestArgs>
  void Reset(std::tuple<DestArgs...> dest_args);

  // Returns the object providing and possibly owning the original
  // `BackwardWriter`. Unchanged by `Close()`.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  BackwardWriter* DestWriter() override { return dest_.get(); }
  const BackwardWriter* DestWriter() const override { return dest_.get(); }

 protected:
  void Done() override;
  void SetWriteSizeHintImpl(absl::optional<Position> write_size_hint) override;
  bool FlushImpl(FlushType flush_type) override;

 private:
  void MoveDest(WrappedBackwardWriter&& that);

  // The object providing and possibly owning the original `BackwardWriter`.
  Dependency<BackwardWriter*, Dest> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit WrappedBackwardWriter(Closed)
    ->WrappedBackwardWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit WrappedBackwardWriter(const Dest& dest)
    -> WrappedBackwardWriter<std::decay_t<Dest>>;
template <typename Dest>
explicit WrappedBackwardWriter(Dest&& dest)
    -> WrappedBackwardWriter<std::decay_t<Dest>>;
template <typename... DestArgs>
explicit WrappedBackwardWriter(std::tuple<DestArgs...> dest_args)
    -> WrappedBackwardWriter<DeleteCtad<std::tuple<DestArgs...>>>;
#endif

// Implementation details follow.

inline WrappedBackwardWriterBase::WrappedBackwardWriterBase(
    WrappedBackwardWriterBase&& that) noexcept
    : BackwardWriter(static_cast<BackwardWriter&&>(that)) {}

inline WrappedBackwardWriterBase& WrappedBackwardWriterBase::operator=(
    WrappedBackwardWriterBase&& that) noexcept {
  BackwardWriter::operator=(static_cast<BackwardWriter&&>(that));
  return *this;
}

inline void WrappedBackwardWriterBase::Initialize(BackwardWriter* dest) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of WrappedBackwardWriter: "
         "null BackwardWriter pointer";
  MakeBuffer(*dest);
}

inline void WrappedBackwardWriterBase::SyncBuffer(BackwardWriter& dest) {
  dest.set_cursor(cursor());
}

inline void WrappedBackwardWriterBase::MakeBuffer(BackwardWriter& dest) {
  set_buffer(dest.limit(), dest.start_to_limit(), dest.start_to_cursor());
  set_start_pos(dest.start_pos());
  if (ABSL_PREDICT_FALSE(!dest.ok())) FailWithoutAnnotation(dest.status());
}

template <typename Dest>
inline WrappedBackwardWriter<Dest>::WrappedBackwardWriter(const Dest& dest)
    : dest_(dest) {
  Initialize(dest_.get());
}

template <typename Dest>
inline WrappedBackwardWriter<Dest>::WrappedBackwardWriter(Dest&& dest)
    : dest_(std::move(dest)) {
  Initialize(dest_.get());
}

template <typename Dest>
template <typename... DestArgs>
inline WrappedBackwardWriter<Dest>::WrappedBackwardWriter(
    std::tuple<DestArgs...> dest_args)
    : dest_(std::move(dest_args)) {
  Initialize(dest_.get());
}

template <typename Dest>
inline WrappedBackwardWriter<Dest>::WrappedBackwardWriter(
    WrappedBackwardWriter&& that) noexcept
    : WrappedBackwardWriterBase(
          static_cast<WrappedBackwardWriterBase&&>(that)) {
  MoveDest(std::move(that));
}

template <typename Dest>
inline WrappedBackwardWriter<Dest>& WrappedBackwardWriter<Dest>::operator=(
    WrappedBackwardWriter&& that) noexcept {
  WrappedBackwardWriterBase::operator=(
      static_cast<WrappedBackwardWriterBase&&>(that));
  MoveDest(std::move(that));
  return *this;
}

template <typename Dest>
inline void WrappedBackwardWriter<Dest>::Reset(Closed) {
  WrappedBackwardWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void WrappedBackwardWriter<Dest>::Reset(const Dest& dest) {
  WrappedBackwardWriterBase::Reset();
  dest_.Reset(dest);
  Initialize(dest_.get());
}

template <typename Dest>
inline void WrappedBackwardWriter<Dest>::Reset(Dest&& dest) {
  WrappedBackwardWriterBase::Reset();
  dest_.Reset(std::move(dest));
  Initialize(dest_.get());
}

template <typename Dest>
template <typename... DestArgs>
inline void WrappedBackwardWriter<Dest>::Reset(
    std::tuple<DestArgs...> dest_args) {
  WrappedBackwardWriterBase::Reset();
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get());
}

template <typename Dest>
inline void WrappedBackwardWriter<Dest>::MoveDest(
    WrappedBackwardWriter&& that) {
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
void WrappedBackwardWriter<Dest>::Done() {
  WrappedBackwardWriterBase::Done();
  if (dest_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) {
      FailWithoutAnnotation(dest_->status());
    }
  }
}

template <typename Dest>
void WrappedBackwardWriter<Dest>::SetWriteSizeHintImpl(
    absl::optional<Position> write_size_hint) {
  if (dest_.is_owning()) dest_->SetWriteSizeHint(write_size_hint);
}

template <typename Dest>
bool WrappedBackwardWriter<Dest>::FlushImpl(FlushType flush_type) {
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

#endif  // RIEGELI_BYTES_WRAPPED_BACKWARD_WRITER_H_
