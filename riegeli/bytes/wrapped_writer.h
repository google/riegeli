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

#ifndef RIEGELI_BYTES_WRAPPED_WRITER_H_
#define RIEGELI_BYTES_WRAPPED_WRITER_H_

#include <stddef.h>

#include <tuple>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/resetter.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Template parameter independent part of `WrappedWriter`.
class WrappedWriterBase : public Writer {
 public:
  // Returns the original `Writer`. Unchanged by `Close()`.
  virtual Writer* dest_writer() = 0;
  virtual const Writer* dest_writer() const = 0;

  bool Flush(FlushType flush_type) override;
  bool SupportsRandomAccess() const override;
  absl::optional<Position> Size() override;
  bool SupportsTruncate() const override;
  bool Truncate(Position new_size) override;

 protected:
  explicit WrappedWriterBase(InitiallyClosed) noexcept
      : Writer(kInitiallyClosed) {}
  explicit WrappedWriterBase(InitiallyOpen) noexcept : Writer(kInitiallyOpen) {}

  WrappedWriterBase(WrappedWriterBase&& that) noexcept;
  WrappedWriterBase& operator=(WrappedWriterBase&& that) noexcept;

  void Initialize(Writer* dest);

  void Done() override;
  bool PushSlow(size_t min_length, size_t recommended_length) override;
  using Writer::WriteSlow;
  bool WriteSlow(absl::string_view src) override;
  bool WriteSlow(const Chain& src) override;
  bool WriteSlow(Chain&& src) override;
  bool WriteSlow(const absl::Cord& src) override;
  bool WriteSlow(absl::Cord&& src) override;
  void WriteHintSlow(size_t length) override;
  bool SeekSlow(Position new_pos) override;

  // Sets cursor of `*dest` to cursor of `*this`.
  void SyncBuffer(Writer* dest);

  // Sets buffer pointers of `*this` to buffer pointers of `*dest`. Fails
  // `*this` if `*dest` failed.
  void MakeBuffer(Writer* dest);

 private:
  // This template is defined and used only in wrapped_writer.cc.
  template <typename Src>
  bool WriteInternal(Src&& src);

  // Invariants if `healthy()`:
  //   `start() == dest_writer()->start()`
  //   `limit() == dest_writer()->limit()`
  //   `start_pos() == dest_writer()->start_pos()`
};

// A `Writer` which just writes to another `Writer`.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the original `Writer`. `Dest` must support
// `Dependency<Writer*, Dest>`, e.g. `Writer*` (not owned, default),
// `std::unique_ptr<Writer>` (owned), `ChainWriter<>` (owned).
//
// The original `Writer` must not be accessed until the `WrappedWriter` is
// closed or no longer used, except that it is allowed to read the destination
// of the original `Writer` immediately after `Flush()`.
template <typename Dest = Writer*>
class WrappedWriter : public WrappedWriterBase {
 public:
  // Creates a closed `WrappedWriter`.
  WrappedWriter() noexcept : WrappedWriterBase(kInitiallyClosed) {}

  // Will write to the original `Writer` provided by `dest`.
  explicit WrappedWriter(const Dest& dest);
  explicit WrappedWriter(Dest&& dest);

  // Will write to the original `Writer` provided by a `Dest` constructed from
  // elements of `dest_args`. This avoids constructing a temporary `Dest` and
  // moving from it.
  template <typename... DestArgs>
  explicit WrappedWriter(std::tuple<DestArgs...> dest_args);

  WrappedWriter(WrappedWriter&& that) noexcept;
  WrappedWriter& operator=(WrappedWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `WrappedWriter`. This
  // avoids constructing a temporary `WrappedWriter` and moving from it.
  void Reset();
  void Reset(const Dest& dest);
  void Reset(Dest&& dest);
  template <typename... DestArgs>
  void Reset(std::tuple<DestArgs...> dest_args);

  // Returns the object providing and possibly owning the original `Writer`.
  // Unchanged by `Close()`.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  Writer* dest_writer() override { return dest_.get(); }
  const Writer* dest_writer() const override { return dest_.get(); }

 protected:
  void Done() override;

 private:
  void MoveDest(WrappedWriter&& that);

  // The object providing and possibly owning the original `Writer`.
  Dependency<Writer*, Dest> dest_;
};

// Implementation details follow.

inline WrappedWriterBase::WrappedWriterBase(WrappedWriterBase&& that) noexcept
    : Writer(std::move(that)) {}

inline WrappedWriterBase& WrappedWriterBase::operator=(
    WrappedWriterBase&& that) noexcept {
  Writer::operator=(std::move(that));
  return *this;
}

inline void WrappedWriterBase::Initialize(Writer* dest) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of WrappedWriter: null Writer pointer";
  MakeBuffer(dest);
}

inline void WrappedWriterBase::SyncBuffer(Writer* dest) {
  dest->set_cursor(cursor());
}

inline void WrappedWriterBase::MakeBuffer(Writer* dest) {
  set_buffer(dest->start(), dest->buffer_size(), dest->written_to_buffer());
  set_start_pos(dest->pos() - written_to_buffer());
  if (ABSL_PREDICT_FALSE(!dest->healthy())) FailWithoutAnnotation(*dest);
}

template <typename Dest>
inline WrappedWriter<Dest>::WrappedWriter(const Dest& dest)
    : WrappedWriterBase(kInitiallyOpen), dest_(dest) {
  Initialize(dest_.get());
}

template <typename Dest>
inline WrappedWriter<Dest>::WrappedWriter(Dest&& dest)
    : WrappedWriterBase(kInitiallyOpen), dest_(std::move(dest)) {
  Initialize(dest_.get());
}

template <typename Dest>
template <typename... DestArgs>
inline WrappedWriter<Dest>::WrappedWriter(std::tuple<DestArgs...> dest_args)
    : WrappedWriterBase(kInitiallyOpen), dest_(std::move(dest_args)) {
  Initialize(dest_.get());
}

template <typename Dest>
inline WrappedWriter<Dest>::WrappedWriter(WrappedWriter&& that) noexcept
    : WrappedWriterBase(std::move(that)) {
  MoveDest(std::move(that));
}

template <typename Dest>
inline WrappedWriter<Dest>& WrappedWriter<Dest>::operator=(
    WrappedWriter&& that) noexcept {
  WrappedWriterBase::operator=(std::move(that));
  MoveDest(std::move(that));
  return *this;
}

template <typename Dest>
inline void WrappedWriter<Dest>::Reset() {
  WrappedWriterBase::Reset(kInitiallyClosed);
  dest_.Reset();
}

template <typename Dest>
inline void WrappedWriter<Dest>::Reset(const Dest& dest) {
  WrappedWriterBase::Reset(kInitiallyOpen);
  dest_.Reset(dest);
  Initialize(dest_.get());
}

template <typename Dest>
inline void WrappedWriter<Dest>::Reset(Dest&& dest) {
  WrappedWriterBase::Reset(kInitiallyOpen);
  dest_.Reset(std::move(dest));
  Initialize(dest_.get());
}

template <typename Dest>
template <typename... DestArgs>
inline void WrappedWriter<Dest>::Reset(std::tuple<DestArgs...> dest_args) {
  WrappedWriterBase::Reset(kInitiallyOpen);
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get());
}

template <typename Dest>
inline void WrappedWriter<Dest>::MoveDest(WrappedWriter&& that) {
  if (dest_.kIsStable()) {
    dest_ = std::move(that.dest_);
  } else {
    // Buffer pointers are already moved so `SyncBuffer()` is called on `*this`,
    // `dest_` is not moved yet so `dest_` is taken from `that`.
    SyncBuffer(that.dest_.get());
    dest_ = std::move(that.dest_);
    MakeBuffer(dest_.get());
  }
}

template <typename Dest>
void WrappedWriter<Dest>::Done() {
  WrappedWriterBase::Done();
  if (dest_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) FailWithoutAnnotation(*dest_);
  }
}

template <typename Dest>
struct Resetter<WrappedWriter<Dest>> : ResetterByReset<WrappedWriter<Dest>> {};

}  // namespace riegeli

#endif  // RIEGELI_BYTES_WRAPPED_WRITER_H_
