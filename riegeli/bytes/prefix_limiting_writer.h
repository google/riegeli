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

#ifndef RIEGELI_BYTES_PREFIX_LIMITING_WRITER_H_
#define RIEGELI_BYTES_PREFIX_LIMITING_WRITER_H_

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
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

template <typename Src>
class PrefixLimitingReader;
class Reader;

// Template parameter independent part of `PrefixLimitingWriter`.
class PrefixLimitingWriterBase : public Writer {
 public:
  class Options {
   public:
    Options() noexcept {}

    // The base position of the original `Reader`. It must be at least as large
    // as the initial position.
    //
    // `absl::nullopt` means the current position.
    //
    // Default: `absl::nullopt`.
    Options& set_base_pos(absl::optional<Position> base_pos) & {
      base_pos_ = base_pos;
      return *this;
    }
    Options&& set_base_pos(absl::optional<Position> base_pos) && {
      return std::move(set_base_pos(base_pos));
    }
    absl::optional<Position> base_pos() const { return base_pos_; }

   private:
    absl::optional<Position> base_pos_;
  };

  // Returns the original `Writer`. Unchanged by `Close()`.
  virtual Writer* DestWriter() = 0;
  virtual const Writer* DestWriter() const = 0;

  // Returns the base position of the origial `Writer`.
  Position base_pos() const { return base_pos_; }

  bool PrefersCopying() const override;
  bool SupportsRandomAccess() override;
  bool SupportsTruncate() override;
  bool SupportsReadMode() override;

 protected:
  using Writer::Writer;

  PrefixLimitingWriterBase(PrefixLimitingWriterBase&& that) noexcept;
  PrefixLimitingWriterBase& operator=(PrefixLimitingWriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset();
  void Initialize(Writer* dest, absl::optional<Position> base_pos);
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateOverDest(absl::Status status);

  // Sets cursor of `dest` to cursor of `*this`.
  void SyncBuffer(Writer& dest);

  // Sets buffer pointers of `*this` to buffer pointers of `dest`, adjusting
  // `start()` to hide data already written. Fails `*this` if `dest` failed.
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
  // This template is defined and used only in prefix_limiting_writer.cc.
  template <typename Src>
  bool WriteInternal(Src&& src);

  Position base_pos_ = 0;

  AssociatedReader<PrefixLimitingReader<Reader*>> associated_reader_;

  // Invariants if `ok()`:
  //   `start() == DestWriter()->cursor()`
  //   `limit() == DestWriter()->limit()`
  //   `start_pos() == DestWriter()->pos() - base_pos_`
};

// A `Writer` which writes to another `Writer`, hiding data before a base
// position, and reporting positions shifted so that the base position appears
// as 0.
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
// The original `Writer` must not be accessed until the `PrefixLimitingWriter`
// is closed or no longer used, except that it is allowed to read the
// destination of the original `Writer` immediately after `Flush()`.
template <typename Dest = Writer*>
class PrefixLimitingWriter : public PrefixLimitingWriterBase {
 public:
  // Creates a closed `PrefixLimitingWriter`.
  explicit PrefixLimitingWriter(Closed) noexcept
      : PrefixLimitingWriterBase(kClosed) {}

  // Will write to the original `Writer` provided by `dest`.
  explicit PrefixLimitingWriter(const Dest& dest, Options options = Options());
  explicit PrefixLimitingWriter(Dest&& dest, Options options = Options());

  // Will write to the original `Writer` provided by a `Dest` constructed from
  // elements of `dest_args`. This avoids constructing a temporary `Dest` and
  // moving from it.
  template <typename... DestArgs>
  explicit PrefixLimitingWriter(std::tuple<DestArgs...> dest_args,
                                Options options = Options());

  PrefixLimitingWriter(PrefixLimitingWriter&& that) noexcept;
  PrefixLimitingWriter& operator=(PrefixLimitingWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `PrefixLimitingWriter`.
  // This avoids constructing a temporary `PrefixLimitingWriter` and moving
  // from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(const Dest& dest,
                                          Options options = Options());
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Dest&& dest,
                                          Options options = Options());
  template <typename... DestArgs>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(std::tuple<DestArgs...> dest_args,
                                          Options options = Options());

  // Returns the object providing and possibly owning the original `Writer`.
  // Unchanged by `Close()`.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  Writer* DestWriter() override { return dest_.get(); }
  const Writer* DestWriter() const override { return dest_.get(); }

 protected:
  void Done() override;
  void SetWriteSizeHintImpl(absl::optional<Position> write_size_hint) override;
  bool FlushImpl(FlushType flush_type) override;

 private:
  void MoveDest(PrefixLimitingWriter&& that);

  // The object providing and possibly owning the original `Writer`.
  Dependency<Writer*, Dest> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit PrefixLimitingWriter(Closed)
    -> PrefixLimitingWriter<DeleteCtad<Closed>>;
template <typename Dest>
explicit PrefixLimitingWriter(const Dest& dest,
                              PrefixLimitingWriterBase::Options options =
                                  PrefixLimitingWriterBase::Options())
    -> PrefixLimitingWriter<std::decay_t<Dest>>;
template <typename Dest>
explicit PrefixLimitingWriter(Dest&& dest,
                              PrefixLimitingWriterBase::Options options =
                                  PrefixLimitingWriterBase::Options())
    -> PrefixLimitingWriter<std::decay_t<Dest>>;
template <typename... DestArgs>
explicit PrefixLimitingWriter(std::tuple<DestArgs...> dest_args,
                              PrefixLimitingWriterBase::Options options =
                                  PrefixLimitingWriterBase::Options())
    -> PrefixLimitingWriter<DeleteCtad<std::tuple<DestArgs...>>>;
#endif

// Implementation details follow.

inline PrefixLimitingWriterBase::PrefixLimitingWriterBase(
    PrefixLimitingWriterBase&& that) noexcept
    : Writer(static_cast<Writer&&>(that)),
      base_pos_(that.base_pos_),
      associated_reader_(std::move(that.associated_reader_)) {}

inline PrefixLimitingWriterBase& PrefixLimitingWriterBase::operator=(
    PrefixLimitingWriterBase&& that) noexcept {
  Writer::operator=(static_cast<Writer&&>(that));
  base_pos_ = that.base_pos_;
  associated_reader_ = std::move(that.associated_reader_);
  return *this;
}

inline void PrefixLimitingWriterBase::Reset(Closed) {
  Writer::Reset(kClosed);
  base_pos_ = 0;
  associated_reader_.Reset();
}

inline void PrefixLimitingWriterBase::Reset() {
  Writer::Reset();
  // `base_pos_` will be set by `Initialize()`.
  associated_reader_.Reset();
}

inline void PrefixLimitingWriterBase::Initialize(
    Writer* dest, absl::optional<Position> base_pos) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of PrefixLimitingWriter: null Writer pointer";
  if (base_pos == absl::nullopt) {
    base_pos_ = dest->pos();
  } else {
    RIEGELI_ASSERT_LE(*base_pos, dest->pos())
        << "Failed precondition of PrefixLimitingWriter: "
           "current position below the base position";
    base_pos_ = *base_pos;
  }
  MakeBuffer(*dest);
}

inline void PrefixLimitingWriterBase::SyncBuffer(Writer& dest) {
  dest.set_cursor(cursor());
}

inline void PrefixLimitingWriterBase::MakeBuffer(Writer& dest) {
  RIEGELI_ASSERT_GE(dest.pos(), base_pos_)
      << "PrefixLimitingWriter destination changed position unexpectedly";
  set_buffer(dest.cursor(), dest.available());
  set_start_pos(dest.pos() - base_pos_);
  if (ABSL_PREDICT_FALSE(!dest.ok())) {
    FailWithoutAnnotation(AnnotateOverDest(dest.status()));
  }
}

template <typename Dest>
inline PrefixLimitingWriter<Dest>::PrefixLimitingWriter(const Dest& dest,
                                                        Options options)
    : dest_(dest) {
  Initialize(dest_.get(), options.base_pos());
}

template <typename Dest>
inline PrefixLimitingWriter<Dest>::PrefixLimitingWriter(Dest&& dest,
                                                        Options options)
    : dest_(std::move(dest)) {
  Initialize(dest_.get(), options.base_pos());
}

template <typename Dest>
template <typename... DestArgs>
inline PrefixLimitingWriter<Dest>::PrefixLimitingWriter(
    std::tuple<DestArgs...> dest_args, Options options)
    : dest_(std::move(dest_args)) {
  Initialize(dest_.get(), options.base_pos());
}

template <typename Dest>
inline PrefixLimitingWriter<Dest>::PrefixLimitingWriter(
    PrefixLimitingWriter&& that) noexcept
    : PrefixLimitingWriterBase(static_cast<PrefixLimitingWriterBase&&>(that)) {
  MoveDest(std::move(that));
}

template <typename Dest>
inline PrefixLimitingWriter<Dest>& PrefixLimitingWriter<Dest>::operator=(
    PrefixLimitingWriter&& that) noexcept {
  PrefixLimitingWriterBase::operator=(
      static_cast<PrefixLimitingWriterBase&&>(that));
  MoveDest(std::move(that));
  return *this;
}

template <typename Dest>
inline void PrefixLimitingWriter<Dest>::Reset(Closed) {
  PrefixLimitingWriterBase::Reset(kClosed);
  dest_.Reset();
}

template <typename Dest>
inline void PrefixLimitingWriter<Dest>::Reset(const Dest& dest,
                                              Options options) {
  PrefixLimitingWriterBase::Reset();
  dest_.Reset(dest);
  Initialize(dest_.get(), options.base_pos());
}

template <typename Dest>
inline void PrefixLimitingWriter<Dest>::Reset(Dest&& dest, Options options) {
  PrefixLimitingWriterBase::Reset();
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), options.base_pos());
}

template <typename Dest>
template <typename... DestArgs>
inline void PrefixLimitingWriter<Dest>::Reset(std::tuple<DestArgs...> dest_args,
                                              Options options) {
  PrefixLimitingWriterBase::Reset();
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get(), options.base_pos());
}

template <typename Dest>
inline void PrefixLimitingWriter<Dest>::MoveDest(PrefixLimitingWriter&& that) {
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
void PrefixLimitingWriter<Dest>::Done() {
  PrefixLimitingWriterBase::Done();
  if (dest_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) {
      FailWithoutAnnotation(AnnotateOverDest(dest_->status()));
    }
  }
}

template <typename Dest>
void PrefixLimitingWriter<Dest>::SetWriteSizeHintImpl(
    absl::optional<Position> write_size_hint) {
  if (dest_.is_owning()) {
    dest_->SetWriteSizeHint(
        write_size_hint == absl::nullopt
            ? absl::nullopt
            : absl::make_optional(SaturatingAdd(base_pos(), *write_size_hint)));
  }
}

template <typename Dest>
bool PrefixLimitingWriter<Dest>::FlushImpl(FlushType flush_type) {
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

#endif  // RIEGELI_BYTES_PREFIX_LIMITING_WRITER_H_
