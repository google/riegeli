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

#ifndef RIEGELI_BYTES_TEE_READER_H_
#define RIEGELI_BYTES_TEE_READER_H_

#include <stddef.h>

#include <tuple>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/resetter.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Template parameter independent part of `TeeReader`.
class TeeReaderBase : public Reader {
 public:
  // Returns the original `Reader`. Unchanged by `Close()`.
  virtual Reader* src_reader() = 0;
  virtual const Reader* src_reader() const = 0;

  // Returns the side `Writer`. Unchanged by `Close()`.
  virtual Writer* side_dest_writer() = 0;
  virtual const Writer* side_dest_writer() const = 0;

  bool Sync() override;

 protected:
  explicit TeeReaderBase(InitiallyClosed) noexcept : Reader(kInitiallyClosed) {}
  explicit TeeReaderBase(InitiallyOpen) noexcept : Reader(kInitiallyOpen) {}

  TeeReaderBase(TeeReaderBase&& that) noexcept;
  TeeReaderBase& operator=(TeeReaderBase&& that) noexcept;

  void Initialize(Reader* src);

  void Done() override;
  bool PullSlow(size_t min_length, size_t recommended_length) override;
  using Reader::ReadSlow;
  bool ReadSlow(char* dest, size_t length) override;
  bool ReadSlow(Chain* dest, size_t length) override;
  bool ReadSlow(absl::Cord* dest, size_t length) override;
  void ReadHintSlow(size_t length) override;

  // Sets cursor of `*src` to cursor of `*this`, writing what has been read from
  // the buffer (until `cursor()`) to `*side_dest`. Fails `*this` and returns
  // `false` if `*side_dest` failed.
  bool SyncBuffer(Reader* src, Writer* side_dest);

  // Sets buffer pointers of `*this` to buffer pointers of `*src`, adjusting
  // `start()` to hide data already written to `*side_dest`. Fails `*this` if
  // `*src` failed.
  void MakeBuffer(Reader* src);

  // Invariants if `!closed()`:
  //   `start() == src_reader()->cursor()`
  //   `limit() == src_reader()->limit()`
  //   `limit_pos() == src_reader()->limit_pos()`
};

// A `Reader` which reads from another `Reader` and writes the same data to a
// side `Writer`.
//
// `TeeReader` does not support random access even if the original `Reader` and
// the side `Writer` do.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the original `Reader`. `Src` must support
// `Dependency<Reader*, Src>`, e.g. `Reader*` (not owned, default),
// `std::unique_ptr<Reader>` (owned), `ChainReader<>` (owned).
//
// The `SideDest` template parameter specifies the type of the object providing
// and possibly owning the side `Writer`. `SideDest` must support
// `Dependency<Writer*, SideDest>`, e.g. `Writer*` (not owned, default),
// `std::unique_ptr<Writer>` (owned), `ChainWriter<>` (owned).
//
// Neither the original `Reader` nor the side `Writer` may be accessed until the
// `TeeReader` is closed or no longer used, except that it is allowed to read
// the destination of the side `Writer` immediately after `Sync()`.
template <typename Src = Reader*, typename SideDest = Writer*>
class TeeReader : public TeeReaderBase {
 public:
  // Creates a closed `TeeReader`.
  TeeReader() noexcept : TeeReaderBase(kInitiallyClosed) {}

  // Will read from the original `Reader` provided by `src`, and write to the
  // side `Writer` provided by `side_dest_init` (or constructed from elements of
  // `side_dest_init` if it is a tuple).
  template <typename SideDestInit>
  explicit TeeReader(const Src& src, SideDestInit&& side_dest_init);
  template <typename SideDestInit>
  explicit TeeReader(Src&& src, SideDestInit&& side_dest_init);

  // Will read from the original `Reader` provided by a `Src` constructed from
  // elements of `src_args`, and write to the side `Writer` provided by
  // `side_dest_init` (or constructed from elements of `side_dest_init` if it is
  // a tuple). This avoids constructing a temporary `Src` and moving from it.
  template <typename... SrcArgs, typename SideDestInit>
  explicit TeeReader(std::tuple<SrcArgs...> src_args,
                     SideDestInit&& side_dest_init);

  TeeReader(TeeReader&& that) noexcept;
  TeeReader& operator=(TeeReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `TeeReader`. This
  // avoids constructing a temporary `TeeReader` and moving from it.
  void Reset();
  template <typename SideDestInit>
  void Reset(const Src& src, SideDestInit&& side_dest_init);
  template <typename SideDestInit>
  void Reset(Src&& src, SideDestInit&& side_dest_init);
  template <typename... SrcArgs, typename SideDestInit>
  void Reset(std::tuple<SrcArgs...> src_args, SideDestInit&& side_dest_init);

  // Returns the object providing and possibly owning the original `Reader`.
  // Unchanged by `Close()`.
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  Reader* src_reader() override { return src_.get(); }
  const Reader* src_reader() const override { return src_.get(); }

  // Returns the object providing and possibly owning the side `Writer`.
  // Unchanged by `Close()`.
  SideDest& side_dest() { return side_dest_.manager(); }
  const SideDest& side_dest() const { return side_dest_.manager(); }
  Writer* side_dest_writer() override { return side_dest_.get(); }
  const Writer* side_dest_writer() const override { return side_dest_.get(); }

  void VerifyEnd() override;

 protected:
  void Done() override;

 private:
  void MoveSrc(TeeReader&& that, Writer* side_dest);

  // The object providing and possibly owning the original `Reader`.
  Dependency<Reader*, Src> src_;

  // The object providing and possibly owning the side `Writer`.
  Dependency<Writer*, SideDest> side_dest_;
};

// Implementation details follow.

inline TeeReaderBase::TeeReaderBase(TeeReaderBase&& that) noexcept
    : Reader(std::move(that)) {}

inline TeeReaderBase& TeeReaderBase::operator=(TeeReaderBase&& that) noexcept {
  Reader::operator=(std::move(that));
  return *this;
}

inline void TeeReaderBase::Initialize(Reader* src) {
  RIEGELI_ASSERT(src != nullptr)
      << "Failed precondition of TeeReader: null Reader pointer";
  MakeBuffer(src);
}

inline bool TeeReaderBase::SyncBuffer(Reader* src, Writer* side_dest) {
  RIEGELI_ASSERT(start() == src->cursor())
      << "Failed invariant of TeeReaderBase: "
         "cursor of the original Reader changed unexpectedly";
  if (ABSL_PREDICT_FALSE(
          !side_dest->Write(absl::string_view(start(), read_from_buffer())))) {
    return Fail(*side_dest);
  }
  src->set_cursor(cursor());
  return true;
}

inline void TeeReaderBase::MakeBuffer(Reader* src) {
  set_buffer(src->cursor(), src->available());
  set_limit_pos(src->pos() + available());
  if (ABSL_PREDICT_FALSE(!src->healthy())) Fail(*src);
}

template <typename Src, typename SideDest>
template <typename SideDestInit>
inline TeeReader<Src, SideDest>::TeeReader(const Src& src,
                                           SideDestInit&& side_dest_init)
    : TeeReaderBase(kInitiallyOpen),
      src_(src),
      side_dest_(std::forward<SideDestInit>(side_dest_init)) {
  Initialize(src_.get());
}

template <typename Src, typename SideDest>
template <typename SideDestInit>
inline TeeReader<Src, SideDest>::TeeReader(Src&& src,
                                           SideDestInit&& side_dest_init)
    : TeeReaderBase(kInitiallyOpen),
      src_(std::move(src)),
      side_dest_(std::forward<SideDestInit>(side_dest_init)) {
  Initialize(src_.get());
}

template <typename Src, typename SideDest>
template <typename... SrcArgs, typename SideDestInit>
inline TeeReader<Src, SideDest>::TeeReader(std::tuple<SrcArgs...> src_args,
                                           SideDestInit&& side_dest_init)
    : TeeReaderBase(kInitiallyOpen),
      src_(std::move(src_args)),
      side_dest_(std::forward<SideDestInit>(side_dest_init)) {
  Initialize(src_.get());
}

template <typename Src, typename SideDest>
inline TeeReader<Src, SideDest>::TeeReader(TeeReader&& that) noexcept
    : TeeReaderBase(std::move(that)), side_dest_(std::move(that.side_dest_)) {
  MoveSrc(std::move(that), side_dest_.get());
}

template <typename Src, typename SideDest>
inline TeeReader<Src, SideDest>& TeeReader<Src, SideDest>::operator=(
    TeeReader&& that) noexcept {
  TeeReaderBase::operator=(std::move(that));
  side_dest_ = std::move(that.side_dest_);
  MoveSrc(std::move(that), side_dest_.get());
  return *this;
}

template <typename Src, typename SideDest>
inline void TeeReader<Src, SideDest>::Reset() {
  TeeReaderBase::Reset(kInitiallyClosed);
  src_.Reset();
}

template <typename Src, typename SideDest>
template <typename SideDestInit>
inline void TeeReader<Src, SideDest>::Reset(const Src& src,
                                            SideDestInit&& side_dest_init) {
  TeeReaderBase::Reset(kInitiallyOpen);
  src_.Reset(src);
  side_dest_.Reset(std::forward<SideDestInit>(side_dest_init));
  Initialize(src_.get());
}

template <typename Src, typename SideDest>
template <typename SideDestInit>
inline void TeeReader<Src, SideDest>::Reset(Src&& src,
                                            SideDestInit&& side_dest_init) {
  TeeReaderBase::Reset(kInitiallyOpen);
  src_.Reset(std::move(src));
  side_dest_.Reset(std::forward<SideDestInit>(side_dest_init));
  Initialize(src_.get());
}

template <typename Src, typename SideDest>
template <typename... SrcArgs, typename SideDestInit>
inline void TeeReader<Src, SideDest>::Reset(std::tuple<SrcArgs...> src_args,
                                            SideDestInit&& side_dest_init) {
  TeeReaderBase::Reset(kInitiallyOpen);
  src_.Reset(std::move(src_args));
  side_dest_.Reset(std::forward<SideDestInit>(side_dest_init));
  Initialize(src_.get());
}

template <typename Src, typename SideDest>
inline void TeeReader<Src, SideDest>::MoveSrc(TeeReader&& that,
                                              Writer* side_dest) {
  if (src_.kIsStable() || ABSL_PREDICT_FALSE(start() == nullptr)) {
    src_ = std::move(that.src_);
  } else {
    // Buffer pointers are already moved so `SyncBuffer()` is called on `*this`,
    // `src_` is not moved yet so `src_` is taken from `that`.
    const bool ok = SyncBuffer(that.src_.get(), side_dest);
    src_ = std::move(that.src_);
    if (ABSL_PREDICT_TRUE(ok)) MakeBuffer(src_.get());
  }
}

template <typename Src, typename SideDest>
void TeeReader<Src, SideDest>::Done() {
  TeeReaderBase::Done();
  if (side_dest_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!side_dest_->Close())) Fail(*side_dest_);
  }
  if (src_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) Fail(*src_);
  }
}

template <typename Src, typename SideDest>
void TeeReader<Src, SideDest>::VerifyEnd() {
  TeeReaderBase::VerifyEnd();
  if (src_.is_owning() && ABSL_PREDICT_TRUE(healthy())) {
    const bool ok = SyncBuffer(src_.get(), side_dest_.get());
    src_->VerifyEnd();
    if (ABSL_PREDICT_TRUE(ok)) MakeBuffer(src_.get());
  }
}

template <typename Src, typename SideDest>
struct Resetter<TeeReader<Src, SideDest>>
    : ResetterByReset<TeeReader<Src, SideDest>> {};

}  // namespace riegeli

#endif  // RIEGELI_BYTES_TEE_READER_H_
