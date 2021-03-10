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

#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
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
  virtual BackwardWriter* dest_writer() = 0;
  virtual const BackwardWriter* dest_writer() const = 0;

  bool PrefersCopying() const override;
  bool Flush(FlushType flush_type) override;
  bool SupportsTruncate() const override;
  bool Truncate(Position new_size) override;

 protected:
  explicit WrappedBackwardWriterBase(InitiallyClosed) noexcept
      : BackwardWriter(kInitiallyClosed) {}
  explicit WrappedBackwardWriterBase(InitiallyOpen) noexcept
      : BackwardWriter(kInitiallyOpen) {}

  WrappedBackwardWriterBase(WrappedBackwardWriterBase&& that) noexcept;
  WrappedBackwardWriterBase& operator=(
      WrappedBackwardWriterBase&& that) noexcept;

  void Initialize(BackwardWriter* dest);

  void Done() override;
  bool PushSlow(size_t min_length, size_t recommended_length) override;
  using BackwardWriter::WriteSlow;
  bool WriteSlow(absl::string_view src) override;
  bool WriteSlow(const Chain& src) override;
  bool WriteSlow(Chain&& src) override;
  bool WriteSlow(const absl::Cord& src) override;
  bool WriteSlow(absl::Cord&& src) override;
  bool WriteZerosSlow(Position length) override;
  void WriteHintSlow(size_t length) override;

  // Sets cursor of `dest` to cursor of `*this`.
  void SyncBuffer(BackwardWriter& dest);

  // Sets buffer pointers of `*this` to buffer pointers of `dest`. Fails `*this`
  // if `dest` failed.
  void MakeBuffer(BackwardWriter& dest);

 private:
  // This template is defined and used only in wrapped_backward_writer.cc.
  template <typename Src>
  bool WriteInternal(Src&& src);

  // Invariants if `healthy()`:
  //   `start() == dest_writer()->start()`
  //   `limit() == dest_writer()->limit()`
  //   `start_pos() == dest_writer()->start_pos()`
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
  WrappedBackwardWriter() noexcept
      : WrappedBackwardWriterBase(kInitiallyClosed) {}

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
  void Reset();
  void Reset(const Dest& dest);
  void Reset(Dest&& dest);
  template <typename... DestArgs>
  void Reset(std::tuple<DestArgs...> dest_args);

  // Returns the object providing and possibly owning the original
  // `BackwardWriter`. Unchanged by `Close()`.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  BackwardWriter* dest_writer() override { return dest_.get(); }
  const BackwardWriter* dest_writer() const override { return dest_.get(); }

 protected:
  void Done() override;

 private:
  void MoveDest(WrappedBackwardWriter&& that);

  // The object providing and possibly owning the original `BackwardWriter`.
  Dependency<BackwardWriter*, Dest> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
template <typename Dest>
WrappedBackwardWriter(Dest&& dest) -> WrappedBackwardWriter<std::decay_t<Dest>>;
template <typename... DestArgs>
WrappedBackwardWriter(std::tuple<DestArgs...> dest_args)
    -> WrappedBackwardWriter<void>;  // Delete.
#endif

// Implementation details follow.

inline WrappedBackwardWriterBase::WrappedBackwardWriterBase(
    WrappedBackwardWriterBase&& that) noexcept
    : BackwardWriter(std::move(that)) {}

inline WrappedBackwardWriterBase& WrappedBackwardWriterBase::operator=(
    WrappedBackwardWriterBase&& that) noexcept {
  BackwardWriter::operator=(std::move(that));
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
  set_buffer(dest.limit(), dest.buffer_size(), dest.written_to_buffer());
  set_start_pos(dest.pos() - dest.written_to_buffer());
  if (ABSL_PREDICT_FALSE(!dest.healthy())) FailWithoutAnnotation(dest);
}

template <typename Dest>
inline WrappedBackwardWriter<Dest>::WrappedBackwardWriter(const Dest& dest)
    : WrappedBackwardWriterBase(kInitiallyOpen), dest_(dest) {
  Initialize(dest_.get());
}

template <typename Dest>
inline WrappedBackwardWriter<Dest>::WrappedBackwardWriter(Dest&& dest)
    : WrappedBackwardWriterBase(kInitiallyOpen), dest_(std::move(dest)) {
  Initialize(dest_.get());
}

template <typename Dest>
template <typename... DestArgs>
inline WrappedBackwardWriter<Dest>::WrappedBackwardWriter(
    std::tuple<DestArgs...> dest_args)
    : WrappedBackwardWriterBase(kInitiallyOpen), dest_(std::move(dest_args)) {
  Initialize(dest_.get());
}

template <typename Dest>
inline WrappedBackwardWriter<Dest>::WrappedBackwardWriter(
    WrappedBackwardWriter&& that) noexcept
    : WrappedBackwardWriterBase(std::move(that)) {
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  MoveDest(std::move(that));
}

template <typename Dest>
inline WrappedBackwardWriter<Dest>& WrappedBackwardWriter<Dest>::operator=(
    WrappedBackwardWriter&& that) noexcept {
  WrappedBackwardWriterBase::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  MoveDest(std::move(that));
  return *this;
}

template <typename Dest>
inline void WrappedBackwardWriter<Dest>::Reset() {
  WrappedBackwardWriterBase::Reset(kInitiallyClosed);
  dest_.Reset();
}

template <typename Dest>
inline void WrappedBackwardWriter<Dest>::Reset(const Dest& dest) {
  WrappedBackwardWriterBase::Reset(kInitiallyOpen);
  dest_.Reset(dest);
  Initialize(dest_.get());
}

template <typename Dest>
inline void WrappedBackwardWriter<Dest>::Reset(Dest&& dest) {
  WrappedBackwardWriterBase::Reset(kInitiallyOpen);
  dest_.Reset(std::move(dest));
  Initialize(dest_.get());
}

template <typename Dest>
template <typename... DestArgs>
inline void WrappedBackwardWriter<Dest>::Reset(
    std::tuple<DestArgs...> dest_args) {
  WrappedBackwardWriterBase::Reset(kInitiallyOpen);
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get());
}

template <typename Dest>
inline void WrappedBackwardWriter<Dest>::MoveDest(
    WrappedBackwardWriter&& that) {
  if (dest_.kIsStable()) {
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
    if (ABSL_PREDICT_FALSE(!dest_->Close())) FailWithoutAnnotation(*dest_);
  }
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_WRAPPED_BACKWARD_WRITER_H_
