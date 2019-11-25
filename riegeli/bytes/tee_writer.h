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

#ifndef RIEGELI_BYTES_TEE_WRITER_H_
#define RIEGELI_BYTES_TEE_WRITER_H_

#include <stddef.h>

#include <tuple>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/resetter.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Template parameter independent part of `TeeWriter`.
class TeeWriterBase : public Writer {
 public:
  // Returns the main `Writer`. Unchanged by `Close()`.
  virtual Writer* dest_writer() = 0;
  virtual const Writer* dest_writer() const = 0;

  // Returns the side `Writer`. Unchanged by `Close()`.
  virtual Writer* side_dest_writer() = 0;
  virtual const Writer* side_dest_writer() const = 0;

  bool Flush(FlushType flush_type) override;

 protected:
  explicit TeeWriterBase(InitiallyClosed) noexcept : Writer(kInitiallyClosed) {}
  explicit TeeWriterBase(InitiallyOpen) noexcept : Writer(kInitiallyOpen) {}

  TeeWriterBase(TeeWriterBase&& that) noexcept;
  TeeWriterBase& operator=(TeeWriterBase&& that) noexcept;

  void Initialize(Writer* dest, Writer* side_dest);

  void Done() override;
  bool PushSlow(size_t min_length, size_t recommended_length) override;
  using Writer::WriteSlow;
  bool WriteSlow(absl::string_view src) override;
  bool WriteSlow(const Chain& src) override;
  bool WriteSlow(Chain&& src) override;

  // Sets cursor of `*dest` to cursor of `*this`, writing what has been written
  // to the buffer (until `cursor()`) to `*side_dest`. Fails `*this` and returns
  // `false` if `*side_dest` failed.
  bool SyncBuffer(Writer* dest, Writer* side_dest);

  // Sets buffer pointers of `*this` to buffer pointers of `*dest`, adjusting
  // `start()` to hide data already written to `*side_dest`. Fails `*this` if
  // `*dest` failed.
  void MakeBuffer(Writer* dest);

 private:
  template <typename Src>
  bool WriteInternal(Src&& src);

  // Invariants if `healthy()`:
  //   `start() == dest_writer()->cursor()`
  //   `limit() == dest_writer()->limit()`
  //   `start_pos() == dest_writer()->pos()`
};

// A `Writer` which writes the same data to a main `Writer` and a side
// `Writer`.
//
// `TeeWriter` does not support random access even if the main `Writer` and the
// side `Writer` do.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the main `Writer`. `Dest` must support
// `Dependency<Writer*, Dest>`, e.g. `Writer*` (not owned, default),
// `std::unique_ptr<Writer>` (owned), `ChainWriter<>` (owned).
//
// Similarly, the `SideDest` template parameter specifies the type of the object
// providing and possibly owning the side `Writer`. `SideDest` must support
// `Dependency<Writer*, SideDest>`.
//
// Neither the main `Writer` nor the side `Writer` may be accessed until the
// `TeeWriter` is closed or no longer used, except that it is allowed to read
// the destination of the main `Writer` or side `Writer` immediately after
// `Flush()`.
template <typename Dest = Writer*, typename SideDest = Writer*>
class TeeWriter : public TeeWriterBase {
 public:
  // Creates a closed `TeeWriter`.
  TeeWriter() noexcept : TeeWriterBase(kInitiallyClosed) {}

  // Will write to the main `Writer` provided by `dest`, and to the side
  // `Writer` provided by `side_dest_init` (or constructed from elements of
  // `side_dest_init` if it is a tuple).
  template <typename SideDestInit>
  explicit TeeWriter(const Dest& dest, SideDestInit&& side_dest_init);
  template <typename SideDestInit>
  explicit TeeWriter(Dest&& dest, SideDestInit&& side_dest_init);

  // Will write to the main `Writer` provided by a `Dest` constructed from
  // elements of `dest_args`, and to the side `Writer` provided by
  // `side_dest_init` (or constructed from elements of `side_dest_init` if it is
  // a tuple). This avoids constructing a temporary `Dest` and moving from it.
  template <typename... DestArgs, typename SideDestInit>
  explicit TeeWriter(std::tuple<DestArgs...> dest_args,
                     SideDestInit&& side_dest_init);

  TeeWriter(TeeWriter&& that) noexcept;
  TeeWriter& operator=(TeeWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `TeeWriter`. This avoids
  // constructing a temporary `TeeWriter` and moving from it.
  void Reset();
  template <typename SideDestInit>
  void Reset(const Dest& dest, SideDestInit&& side_dest_init);
  template <typename SideDestInit>
  void Reset(Dest&& dest, SideDestInit&& side_dest_init);
  template <typename... DestArgs, typename SideDestInit>
  void Reset(std::tuple<DestArgs...> dest_args, SideDestInit&& side_dest_init);

  // Returns the object providing and possibly owning the main `Writer`.
  // Unchanged by `Close()`.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  Writer* dest_writer() override { return dest_.get(); }
  const Writer* dest_writer() const override { return dest_.get(); }

  // Returns the object providing and possibly owning the side `Writer`.
  // Unchanged by `Close()`.
  SideDest& side_dest() { return side_dest_.manager(); }
  const SideDest& side_dest() const { return side_dest_.manager(); }
  Writer* side_dest_writer() override { return side_dest_.get(); }
  const Writer* side_dest_writer() const override { return side_dest_.get(); }

 protected:
  void Done() override;

 private:
  void MoveDest(TeeWriter&& that, Writer* side_dest);

  // The object providing and possibly owning the main `Writer`.
  Dependency<Writer*, Dest> dest_;

  // The object providing and possibly owning the side `Writer`.
  Dependency<Writer*, SideDest> side_dest_;
};

// Implementation details follow.

inline TeeWriterBase::TeeWriterBase(TeeWriterBase&& that) noexcept
    : Writer(std::move(that)) {}

inline TeeWriterBase& TeeWriterBase::operator=(TeeWriterBase&& that) noexcept {
  Writer::operator=(std::move(that));
  return *this;
}

inline void TeeWriterBase::Initialize(Writer* dest, Writer* side_dest) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of TeeWriter: null main Writer pointer";
  RIEGELI_ASSERT(side_dest != nullptr)
      << "Failed precondition of TeeWriter: null side Writer pointer";
  MakeBuffer(dest);
}

inline bool TeeWriterBase::SyncBuffer(Writer* dest, Writer* side_dest) {
  RIEGELI_ASSERT(start() == dest->cursor())
      << "Failed invariant of TeeWriterBase: "
         "cursor of the original Writer changed unexpectedly";
  if (ABSL_PREDICT_FALSE(
          !side_dest->Write(absl::string_view(start(), written_to_buffer())))) {
    return Fail(*side_dest);
  }
  dest->set_cursor(cursor());
  return true;
}

inline void TeeWriterBase::MakeBuffer(Writer* dest) {
  set_buffer(dest->cursor(), dest->available());
  set_start_pos(dest->pos());
  if (ABSL_PREDICT_FALSE(!dest->healthy())) Fail(*dest);
}

template <typename Dest, typename SideDest>
template <typename SideDestInit>
inline TeeWriter<Dest, SideDest>::TeeWriter(const Dest& dest,
                                            SideDestInit&& side_dest_init)
    : TeeWriterBase(kInitiallyOpen),
      dest_(dest),
      side_dest_(std::forward<SideDestInit>(side_dest_init)) {
  Initialize(dest_.get(), side_dest_.get());
}

template <typename Dest, typename SideDest>
template <typename SideDestInit>
inline TeeWriter<Dest, SideDest>::TeeWriter(Dest&& dest,
                                            SideDestInit&& side_dest_init)
    : TeeWriterBase(kInitiallyOpen),
      dest_(std::move(dest)),
      side_dest_(std::forward<SideDestInit>(side_dest_init)) {
  Initialize(dest_.get(), side_dest_.get());
}

template <typename Dest, typename SideDest>
template <typename... DestArgs, typename SideDestInit>
inline TeeWriter<Dest, SideDest>::TeeWriter(std::tuple<DestArgs...> dest_args,
                                            SideDestInit&& side_dest_init)
    : TeeWriterBase(kInitiallyOpen),
      dest_(std::move(dest_args)),
      side_dest_(std::forward<SideDestInit>(side_dest_init)) {
  Initialize(dest_.get(), side_dest_.get());
}

template <typename Dest, typename SideDest>
inline TeeWriter<Dest, SideDest>::TeeWriter(TeeWriter&& that) noexcept
    : TeeWriterBase(std::move(that)), side_dest_(std::move(that.side_dest_)) {
  MoveDest(std::move(that), side_dest_.get());
}

template <typename Dest, typename SideDest>
inline TeeWriter<Dest, SideDest>& TeeWriter<Dest, SideDest>::operator=(
    TeeWriter&& that) noexcept {
  TeeWriterBase::operator=(std::move(that));
  side_dest_ = std::move(that.side_dest_);
  MoveDest(std::move(that), side_dest_.get());
  return *this;
}

template <typename Dest, typename SideDest>
inline void TeeWriter<Dest, SideDest>::Reset() {
  TeeWriterBase::Reset(kInitiallyClosed);
  dest_.Reset();
  side_dest_.Reset();
}

template <typename Dest, typename SideDest>
template <typename SideDestInit>
inline void TeeWriter<Dest, SideDest>::Reset(const Dest& dest,
                                             SideDestInit&& side_dest_init) {
  TeeWriterBase::Reset(kInitiallyOpen);
  dest_.Reset(dest);
  side_dest_.Reset(std::forward<SideDestInit>(side_dest_init));
  Initialize(dest_.get(), side_dest_.get());
}

template <typename Dest, typename SideDest>
template <typename SideDestInit>
inline void TeeWriter<Dest, SideDest>::Reset(Dest&& dest,
                                             SideDestInit&& side_dest_init) {
  TeeWriterBase::Reset(kInitiallyOpen);
  dest_.Reset(std::move(dest));
  side_dest_.Reset(std::forward<SideDestInit>(side_dest_init));
  Initialize(dest_.get(), side_dest_.get());
}

template <typename Dest, typename SideDest>
template <typename... DestArgs, typename SideDestInit>
inline void TeeWriter<Dest, SideDest>::Reset(std::tuple<DestArgs...> dest_args,
                                             SideDestInit&& side_dest_init) {
  TeeWriterBase::Reset(kInitiallyOpen);
  dest_.Reset(std::move(dest_args));
  side_dest_.Reset(std::forward<SideDestInit>(side_dest_init));
  Initialize(dest_.get(), side_dest_.get());
}

template <typename Dest, typename SideDest>
inline void TeeWriter<Dest, SideDest>::MoveDest(TeeWriter&& that,
                                                Writer* side_dest) {
  if (dest_.kIsStable() || ABSL_PREDICT_FALSE(!healthy())) {
    dest_ = std::move(that.dest_);
  } else {
    // Buffer pointers are already moved so `SyncBuffer()` is called on `*this`,
    // `dest_` is not moved yet so `dest_` is taken from `that`.
    const bool ok = SyncBuffer(that.dest_.get(), side_dest);
    dest_ = std::move(that.dest_);
    if (ABSL_PREDICT_TRUE(ok)) MakeBuffer(dest_.get());
  }
}

template <typename Dest, typename SideDest>
void TeeWriter<Dest, SideDest>::Done() {
  TeeWriterBase::Done();
  if (side_dest_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!side_dest_->Close())) Fail(*side_dest_);
  }
  if (dest_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) Fail(*dest_);
  }
}

template <typename Dest, typename SideDest>
struct Resetter<TeeWriter<Dest, SideDest>>
    : ResetterByReset<TeeWriter<Dest, SideDest>> {};

}  // namespace riegeli

#endif  // RIEGELI_BYTES_TEE_WRITER_H_
