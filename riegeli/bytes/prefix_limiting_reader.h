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

#ifndef RIEGELI_BYTES_PREFIX_LIMITING_READER_H_
#define RIEGELI_BYTES_PREFIX_LIMITING_READER_H_

#include <stddef.h>

#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

class BackwardWriter;
class Writer;

// Template parameter independent part of `PrefixLimitingReader`.
class PrefixLimitingReaderBase : public Reader {
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

  // Returns the original `Reader`. Unchanged by `Close()`.
  virtual Reader* src_reader() = 0;
  virtual const Reader* src_reader() const = 0;

  // Returns the base position of the origial `Reader`.
  Position base_pos() const { return base_pos_; }

  bool ToleratesReadingAhead() override;
  bool SupportsRandomAccess() override;
  bool SupportsRewind() override;
  bool SupportsSize() override;
  bool SupportsNewReader() override;

 protected:
  using Reader::Reader;

  PrefixLimitingReaderBase(PrefixLimitingReaderBase&& that) noexcept;
  PrefixLimitingReaderBase& operator=(PrefixLimitingReaderBase&& that) noexcept;

  void Reset(Closed);
  void Reset();
  void Initialize(Reader* src, absl::optional<Position> base_pos);
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateOverSrc(absl::Status status);

  void Done() override;
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;
  bool PullSlow(size_t min_length, size_t recommended_length) override;
  using Reader::ReadSlow;
  bool ReadSlow(size_t length, char* dest) override;
  bool ReadSlow(size_t length, Chain& dest) override;
  bool ReadSlow(size_t length, absl::Cord& dest) override;
  using Reader::CopySlow;
  bool CopySlow(Position length, Writer& dest) override;
  bool CopySlow(size_t length, BackwardWriter& dest) override;
  void ReadHintSlow(size_t min_length, size_t recommended_length) override;
  bool SeekSlow(Position new_pos) override;
  absl::optional<Position> SizeImpl() override;
  std::unique_ptr<Reader> NewReaderImpl(Position initial_pos) override;

  // Sets cursor of `src` to cursor of `*this`.
  void SyncBuffer(Reader& src);

  // Sets buffer pointers of `*this` to buffer pointers of `src`, adjusting
  // `start()` to hide data already read. Fails `*this` if `src` failed.
  void MakeBuffer(Reader& src);

 private:
  // This template is defined and used only in prefix_limiting_reader.cc.
  template <typename Dest>
  bool ReadInternal(size_t length, Dest& dest);

  Position base_pos_ = 0;

  // Invariants if `is_open()`:
  //   `start() >= src_reader()->cursor()`
  //   `limit() == src_reader()->limit()`
  //   `limit_pos() == src_reader()->limit_pos() - base_pos_`
};

// A `Reader` which reads from another `Reader`, hiding data before a base
// position, and reporting positions shifted so that the base position appears
// as 0.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the original `Reader`. `Src` must support
// `Dependency<Reader*, Src>`, e.g. `Reader*` (not owned, default),
// `std::unique_ptr<Reader>` (owned), `ChainReader<>` (owned).
//
// By relying on CTAD the template argument can be deduced as the value type of
// the first constructor argument. This requires C++17.
//
// The original `Reader` must not be accessed until the `PrefixLimitingReader`
// is closed or no longer used.
template <typename Src = Reader*>
class PrefixLimitingReader : public PrefixLimitingReaderBase {
 public:
  // Creates a closed `PrefixLimitingReader`.
  explicit PrefixLimitingReader(Closed) noexcept
      : PrefixLimitingReaderBase(kClosed) {}

  // Will read from the original `Reader` provided by `src`.
  explicit PrefixLimitingReader(const Src& src, Options options = Options());
  explicit PrefixLimitingReader(Src&& src, Options options = Options());

  // Will read from the original `Reader` provided by a `Src` constructed from
  // elements of `src_args`. This avoids constructing a temporary `Src` and
  // moving from it.
  template <typename... SrcArgs>
  explicit PrefixLimitingReader(std::tuple<SrcArgs...> src_args,
                                Options options = Options());

  PrefixLimitingReader(PrefixLimitingReader&& that) noexcept;
  PrefixLimitingReader& operator=(PrefixLimitingReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `PrefixLimitingReader`.
  // This avoids constructing a temporary `PrefixLimitingReader` and moving
  // from it.
  void Reset(Closed);
  void Reset(const Src& src, Options options = Options());
  void Reset(Src&& src, Options options = Options());
  template <typename... SrcArgs>
  void Reset(std::tuple<SrcArgs...> src_args, Options options = Options());

  // Returns the object providing and possibly owning the original `Reader`.
  // Unchanged by `Close()`.
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  Reader* src_reader() override { return src_.get(); }
  const Reader* src_reader() const override { return src_.get(); }

  void SetReadAllHint(bool read_all_hint) override;

 protected:
  void Done() override;
  void VerifyEndImpl() override;
  bool SyncImpl(SyncType sync_type) override;

 private:
  void MoveSrc(PrefixLimitingReader&& that);

  // The object providing and possibly owning the original `Reader`.
  Dependency<Reader*, Src> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit PrefixLimitingReader(Closed)->PrefixLimitingReader<DeleteCtad<Closed>>;
template <typename Src>
explicit PrefixLimitingReader(const Src& src,
                              PrefixLimitingReaderBase::Options options =
                                  PrefixLimitingReaderBase::Options())
    -> PrefixLimitingReader<std::decay_t<Src>>;
template <typename Src>
explicit PrefixLimitingReader(Src&& src,
                              PrefixLimitingReaderBase::Options options =
                                  PrefixLimitingReaderBase::Options())
    -> PrefixLimitingReader<std::decay_t<Src>>;
template <typename... SrcArgs>
explicit PrefixLimitingReader(std::tuple<SrcArgs...> src_args,
                              PrefixLimitingReaderBase::Options options =
                                  PrefixLimitingReaderBase::Options())
    -> PrefixLimitingReader<DeleteCtad<std::tuple<SrcArgs...>>>;
#endif

// Implementation details follow.

inline PrefixLimitingReaderBase::PrefixLimitingReaderBase(
    PrefixLimitingReaderBase&& that) noexcept
    : Reader(static_cast<Reader&&>(that)), base_pos_(that.base_pos_) {}

inline PrefixLimitingReaderBase& PrefixLimitingReaderBase::operator=(
    PrefixLimitingReaderBase&& that) noexcept {
  Reader::operator=(static_cast<Reader&&>(that));
  base_pos_ = that.base_pos_;
  return *this;
}

inline void PrefixLimitingReaderBase::Reset(Closed) {
  Reader::Reset(kClosed);
  base_pos_ = 0;
}

inline void PrefixLimitingReaderBase::Reset() {
  Reader::Reset();
  // `base_pos_` will be set by `Initialize()`.
}

inline void PrefixLimitingReaderBase::Initialize(
    Reader* src, absl::optional<Position> base_pos) {
  RIEGELI_ASSERT(src != nullptr)
      << "Failed precondition of PrefixLimitingReader: null Reader pointer";
  if (base_pos == absl::nullopt) {
    base_pos_ = src->pos();
  } else {
    RIEGELI_ASSERT_LE(*base_pos, src->pos())
        << "Failed precondition of PrefixLimitingReader: "
           "current position below the base position";
    base_pos_ = *base_pos;
  }
  MakeBuffer(*src);
}

inline void PrefixLimitingReaderBase::SyncBuffer(Reader& src) {
  src.set_cursor(cursor());
}

inline void PrefixLimitingReaderBase::MakeBuffer(Reader& src) {
  RIEGELI_ASSERT_GE(src.pos(), base_pos_)
      << "PrefixLimitingReader source changed position unexpectedly";
  set_buffer(src.cursor(), src.available());
  set_limit_pos(src.limit_pos() - base_pos_);
  if (ABSL_PREDICT_FALSE(!src.ok())) {
    FailWithoutAnnotation(AnnotateOverSrc(src.status()));
  }
}

template <typename Src>
inline PrefixLimitingReader<Src>::PrefixLimitingReader(const Src& src,
                                                       Options options)
    : src_(src) {
  Initialize(src_.get(), options.base_pos());
}

template <typename Src>
inline PrefixLimitingReader<Src>::PrefixLimitingReader(Src&& src,
                                                       Options options)
    : src_(std::move(src)) {
  Initialize(src_.get(), options.base_pos());
}

template <typename Src>
template <typename... SrcArgs>
inline PrefixLimitingReader<Src>::PrefixLimitingReader(
    std::tuple<SrcArgs...> src_args, Options options)
    : src_(std::move(src_args)) {
  Initialize(src_.get(), options.base_pos());
}

template <typename Src>
inline PrefixLimitingReader<Src>::PrefixLimitingReader(
    PrefixLimitingReader&& that) noexcept
    : PrefixLimitingReaderBase(static_cast<PrefixLimitingReaderBase&&>(that)) {
  MoveSrc(std::move(that));
}

template <typename Src>
inline PrefixLimitingReader<Src>& PrefixLimitingReader<Src>::operator=(
    PrefixLimitingReader&& that) noexcept {
  PrefixLimitingReaderBase::operator=(
      static_cast<PrefixLimitingReaderBase&&>(that));
  MoveSrc(std::move(that));
  return *this;
}

template <typename Src>
inline void PrefixLimitingReader<Src>::Reset(Closed) {
  PrefixLimitingReaderBase::Reset(kClosed);
  src_.Reset();
}

template <typename Src>
inline void PrefixLimitingReader<Src>::Reset(const Src& src, Options options) {
  PrefixLimitingReaderBase::Reset();
  src_.Reset(src);
  Initialize(src_.get(), options.base_pos());
}

template <typename Src>
inline void PrefixLimitingReader<Src>::Reset(Src&& src, Options options) {
  PrefixLimitingReaderBase::Reset();
  src_.Reset(std::move(src));
  Initialize(src_.get(), options.base_pos());
}

template <typename Src>
template <typename... SrcArgs>
inline void PrefixLimitingReader<Src>::Reset(std::tuple<SrcArgs...> src_args,
                                             Options options) {
  PrefixLimitingReaderBase::Reset();
  src_.Reset(std::move(src_args));
  Initialize(src_.get(), options.base_pos());
}

template <typename Src>
inline void PrefixLimitingReader<Src>::MoveSrc(PrefixLimitingReader&& that) {
  if (src_.kIsStable || that.src_ == nullptr) {
    src_ = std::move(that.src_);
  } else {
    // Buffer pointers are already moved so `SyncBuffer()` is called on `*this`,
    // `src_` is not moved yet so `src_` is taken from `that`.
    SyncBuffer(*that.src_);
    src_ = std::move(that.src_);
    MakeBuffer(*src_);
  }
}

template <typename Src>
void PrefixLimitingReader<Src>::Done() {
  PrefixLimitingReaderBase::Done();
  if (src_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) {
      FailWithoutAnnotation(AnnotateOverSrc(src_->status()));
    }
  }
}

template <typename Src>
void PrefixLimitingReader<Src>::SetReadAllHint(bool read_all_hint) {
  if (src_.is_owning()) src_->SetReadAllHint(read_all_hint);
}

template <typename Src>
void PrefixLimitingReader<Src>::VerifyEndImpl() {
  if (!src_.is_owning()) {
    PrefixLimitingReaderBase::VerifyEndImpl();
  } else if (ABSL_PREDICT_TRUE(ok())) {
    SyncBuffer(*src_);
    src_->VerifyEnd();
    MakeBuffer(*src_);
  }
}

template <typename Src>
bool PrefixLimitingReader<Src>::SyncImpl(SyncType sync_type) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  SyncBuffer(*src_);
  bool sync_ok = true;
  if (sync_type != SyncType::kFromObject || src_.is_owning()) {
    sync_ok = src_->Sync(sync_type);
  }
  MakeBuffer(*src_);
  return sync_ok;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_PREFIX_LIMITING_READER_H_
