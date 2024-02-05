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

#ifndef RIEGELI_BYTES_WRAPPING_READER_H_
#define RIEGELI_BYTES_WRAPPING_READER_H_

#include <stddef.h>

#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/types/optional.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

class BackwardWriter;
class Writer;

// Template parameter independent part of `WrappingReader`.
class WrappingReaderBase : public Reader {
 public:
  // Returns the original `Reader`. Unchanged by `Close()`.
  virtual Reader* SrcReader() = 0;
  virtual const Reader* SrcReader() const = 0;

  bool ToleratesReadingAhead() override;
  bool SupportsRandomAccess() override;
  bool SupportsRewind() override;
  bool SupportsSize() override;
  bool SupportsNewReader() override;

 protected:
  using Reader::Reader;

  WrappingReaderBase(WrappingReaderBase&& that) noexcept;
  WrappingReaderBase& operator=(WrappingReaderBase&& that) noexcept;

  void Initialize(Reader* src);

  // Sets cursor of `src` to cursor of `*this`.
  void SyncBuffer(Reader& src);

  // Sets buffer pointers of `*this` to buffer pointers of `src`. Fails `*this`
  // if `src` failed.
  void MakeBuffer(Reader& src);

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
  using Reader::ReadOrPullSomeSlow;
  bool ReadOrPullSomeSlow(size_t max_length,
                          absl::FunctionRef<char*(size_t&)> get_dest) override;
  void ReadHintSlow(size_t min_length, size_t recommended_length) override;
  bool SeekSlow(Position new_pos) override;
  absl::optional<Position> SizeImpl() override;
  std::unique_ptr<Reader> NewReaderImpl(Position initial_pos) override;

 private:
  // This template is defined and used only in wrapping_reader.cc.
  template <typename Dest>
  bool ReadInternal(size_t length, Dest& dest);

  // Invariants if `is_open()`:
  //   `start() == SrcReader()->start()`
  //   `limit() == SrcReader()->limit()`
  //   `limit_pos() == SrcReader()->limit_pos()`
};

// A `Reader` which just reads from another `Reader`.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the original `Reader`. `Src` must support
// `Dependency<Reader*, Src>`, e.g. `Reader*` (not owned, default),
// `ChainReader<>` (owned), `std::unique_ptr<Reader>` (owned),
// `AnyDependency<Reader*>` (maybe owned).
//
// By relying on CTAD the template argument can be deduced as the value type of
// the first constructor argument. This requires C++17.
//
// The original `Reader` must not be accessed until the `WrappingReader` is
// closed or no longer used.
template <typename Src = Reader*>
class WrappingReader : public WrappingReaderBase {
 public:
  // Creates a closed `WrappingReader`.
  explicit WrappingReader(Closed) noexcept : WrappingReaderBase(kClosed) {}

  // Will read from the original `Reader` provided by `src`.
  explicit WrappingReader(const Src& src);
  explicit WrappingReader(Src&& src);

  // Will read from the original `Reader` provided by a `Src` constructed from
  // elements of `src_args`. This avoids constructing a temporary `Src` and
  // moving from it.
  template <typename... SrcArgs>
  explicit WrappingReader(std::tuple<SrcArgs...> src_args);

  WrappingReader(WrappingReader&& that) noexcept;
  WrappingReader& operator=(WrappingReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `WrappingReader`. This
  // avoids constructing a temporary `WrappingReader` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(const Src& src);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Src&& src);
  template <typename... SrcArgs>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(std::tuple<SrcArgs...> src_args);

  // Returns the object providing and possibly owning the original `Reader`.
  // Unchanged by `Close()`.
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  Reader* SrcReader() override { return src_.get(); }
  const Reader* SrcReader() const override { return src_.get(); }

 protected:
  void Done() override;
  void SetReadAllHintImpl(bool read_all_hint) override;
  void VerifyEndImpl() override;
  bool SyncImpl(SyncType sync_type) override;

 private:
  // Moves `that.src_` to `src_`. Buffer pointers are already moved from `src_`
  // to `*this`; adjust them to match `src_`.
  void MoveSrc(WrappingReader&& that);

  // The object providing and possibly owning the original `Reader`.
  Dependency<Reader*, Src> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit WrappingReader(Closed) -> WrappingReader<DeleteCtad<Closed>>;
template <typename Src>
explicit WrappingReader(const Src& src) -> WrappingReader<std::decay_t<Src>>;
template <typename Src>
explicit WrappingReader(Src&& src) -> WrappingReader<std::decay_t<Src>>;
template <typename... SrcArgs>
explicit WrappingReader(std::tuple<SrcArgs...> src_args)
    -> WrappingReader<DeleteCtad<std::tuple<SrcArgs...>>>;
#endif

// Implementation details follow.

inline WrappingReaderBase::WrappingReaderBase(
    WrappingReaderBase&& that) noexcept
    : Reader(static_cast<Reader&&>(that)) {}

inline WrappingReaderBase& WrappingReaderBase::operator=(
    WrappingReaderBase&& that) noexcept {
  Reader::operator=(static_cast<Reader&&>(that));
  return *this;
}

inline void WrappingReaderBase::Initialize(Reader* src) {
  RIEGELI_ASSERT(src != nullptr)
      << "Failed precondition of WrappingReader: null Reader pointer";
  MakeBuffer(*src);
}

inline void WrappingReaderBase::SyncBuffer(Reader& src) {
  src.set_cursor(cursor());
}

inline void WrappingReaderBase::MakeBuffer(Reader& src) {
  set_buffer(src.start(), src.start_to_limit(), src.start_to_cursor());
  set_limit_pos(src.limit_pos());
  if (ABSL_PREDICT_FALSE(!src.ok())) FailWithoutAnnotation(src.status());
}

template <typename Src>
inline WrappingReader<Src>::WrappingReader(const Src& src) : src_(src) {
  Initialize(src_.get());
}

template <typename Src>
inline WrappingReader<Src>::WrappingReader(Src&& src) : src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline WrappingReader<Src>::WrappingReader(std::tuple<SrcArgs...> src_args)
    : src_(std::move(src_args)) {
  Initialize(src_.get());
}

template <typename Src>
inline WrappingReader<Src>::WrappingReader(WrappingReader&& that) noexcept
    : WrappingReaderBase(static_cast<WrappingReaderBase&&>(that)) {
  MoveSrc(std::move(that));
}

template <typename Src>
inline WrappingReader<Src>& WrappingReader<Src>::operator=(
    WrappingReader&& that) noexcept {
  WrappingReaderBase::operator=(static_cast<WrappingReaderBase&&>(that));
  MoveSrc(std::move(that));
  return *this;
}

template <typename Src>
inline void WrappingReader<Src>::Reset(Closed) {
  WrappingReaderBase::Reset(kClosed);
  src_.Reset();
}

template <typename Src>
inline void WrappingReader<Src>::Reset(const Src& src) {
  WrappingReaderBase::Reset();
  src_.Reset(src);
  Initialize(src_.get());
}

template <typename Src>
inline void WrappingReader<Src>::Reset(Src&& src) {
  WrappingReaderBase::Reset();
  src_.Reset(std::move(src));
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline void WrappingReader<Src>::Reset(std::tuple<SrcArgs...> src_args) {
  WrappingReaderBase::Reset();
  src_.Reset(std::move(src_args));
  Initialize(src_.get());
}

template <typename Src>
inline void WrappingReader<Src>::MoveSrc(WrappingReader&& that) {
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
void WrappingReader<Src>::Done() {
  WrappingReaderBase::Done();
  if (src_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) {
      FailWithoutAnnotation(src_->status());
    }
  }
}

template <typename Src>
void WrappingReader<Src>::SetReadAllHintImpl(bool read_all_hint) {
  WrappingReaderBase::SetReadAllHintImpl(read_all_hint);
  if (src_.is_owning()) {
    SyncBuffer(*src_);
    src_->SetReadAllHint(read_all_hint);
    MakeBuffer(*src_);
  }
}

template <typename Src>
void WrappingReader<Src>::VerifyEndImpl() {
  if (!src_.is_owning()) {
    WrappingReaderBase::VerifyEndImpl();
  } else if (ABSL_PREDICT_TRUE(ok())) {
    SyncBuffer(*src_);
    src_->VerifyEnd();
    MakeBuffer(*src_);
  }
}

template <typename Src>
bool WrappingReader<Src>::SyncImpl(SyncType sync_type) {
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

#endif  // RIEGELI_BYTES_WRAPPING_READER_H_
