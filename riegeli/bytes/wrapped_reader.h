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

#ifndef RIEGELI_BYTES_WRAPPED_READER_H_
#define RIEGELI_BYTES_WRAPPED_READER_H_

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

// Template parameter independent part of `WrappedReader`.
class WrappedReaderBase : public Reader {
 public:
  // Returns the original `Reader`. Unchanged by `Close()`.
  virtual Reader* src_reader() = 0;
  virtual const Reader* src_reader() const = 0;

  bool SupportsRandomAccess() override;
  bool SupportsRewind() override;
  bool SupportsSize() override;
  bool SupportsNewReader() override;

 protected:
  using Reader::Reader;

  WrappedReaderBase(WrappedReaderBase&& that) noexcept;
  WrappedReaderBase& operator=(WrappedReaderBase&& that) noexcept;

  void Initialize(Reader* src);

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

  // Sets buffer pointers of `*this` to buffer pointers of `src`. Fails `*this`
  // if `src` failed.
  void MakeBuffer(Reader& src);

 private:
  // This template is defined and used only in wrapped_reader.cc.
  template <typename Dest>
  bool ReadInternal(size_t length, Dest& dest);

  // Invariants if `is_open()`:
  //   `start() == src_reader()->start()`
  //   `limit() == src_reader()->limit()`
  //   `limit_pos() == src_reader()->limit_pos()`
};

// A `Reader` which just reads from another `Reader`.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the original `Reader`. `Src` must support
// `Dependency<Reader*, Src>`, e.g. `Reader*` (not owned, default),
// `std::unique_ptr<Reader>` (owned), `ChainReader<>` (owned).
//
// By relying on CTAD the template argument can be deduced as the value type of
// the first constructor argument. This requires C++17.
//
// The original `Reader` must not be accessed until the `WrappedReader` is
// closed or no longer used.
template <typename Src = Reader*>
class WrappedReader : public WrappedReaderBase {
 public:
  // Creates a closed `WrappedReader`.
  explicit WrappedReader(Closed) noexcept : WrappedReaderBase(kClosed) {}

  // Will read from the original `Reader` provided by `src`.
  explicit WrappedReader(const Src& src);
  explicit WrappedReader(Src&& src);

  // Will read from the original `Reader` provided by a `Src` constructed from
  // elements of `src_args`. This avoids constructing a temporary `Src` and
  // moving from it.
  template <typename... SrcArgs>
  explicit WrappedReader(std::tuple<SrcArgs...> src_args);

  WrappedReader(WrappedReader&& that) noexcept;
  WrappedReader& operator=(WrappedReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `WrappedReader`. This
  // avoids constructing a temporary `WrappedReader` and moving from it.
  void Reset(Closed);
  void Reset(const Src& src);
  void Reset(Src&& src);
  template <typename... SrcArgs>
  void Reset(std::tuple<SrcArgs...> src_args);

  // Returns the object providing and possibly owning the original `Reader`.
  // Unchanged by `Close()`.
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  Reader* src_reader() override { return src_.get(); }
  const Reader* src_reader() const override { return src_.get(); }

  void VerifyEnd() override;

 protected:
  void Done() override;
  bool SyncImpl(SyncType sync_type) override;

 private:
  void MoveSrc(WrappedReader&& that);

  // The object providing and possibly owning the original `Reader`.
  Dependency<Reader*, Src> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit WrappedReader(Closed)->WrappedReader<DeleteCtad<Closed>>;
template <typename Src>
explicit WrappedReader(const Src& src) -> WrappedReader<std::decay_t<Src>>;
template <typename Src>
explicit WrappedReader(Src&& src) -> WrappedReader<std::decay_t<Src>>;
template <typename... SrcArgs>
explicit WrappedReader(std::tuple<SrcArgs...> src_args)
    -> WrappedReader<DeleteCtad<std::tuple<SrcArgs...>>>;
#endif

// Implementation details follow.

inline WrappedReaderBase::WrappedReaderBase(WrappedReaderBase&& that) noexcept
    : Reader(std::move(that)) {}

inline WrappedReaderBase& WrappedReaderBase::operator=(
    WrappedReaderBase&& that) noexcept {
  Reader::operator=(std::move(that));
  return *this;
}

inline void WrappedReaderBase::Initialize(Reader* src) {
  RIEGELI_ASSERT(src != nullptr)
      << "Failed precondition of WrappedReader: null Reader pointer";
  MakeBuffer(*src);
}

inline void WrappedReaderBase::SyncBuffer(Reader& src) {
  src.set_cursor(cursor());
}

inline void WrappedReaderBase::MakeBuffer(Reader& src) {
  set_buffer(src.start(), src.start_to_limit(), src.start_to_cursor());
  set_limit_pos(src.limit_pos());
  if (ABSL_PREDICT_FALSE(!src.healthy())) FailWithoutAnnotation(src.status());
}

template <typename Src>
inline WrappedReader<Src>::WrappedReader(const Src& src) : src_(src) {
  Initialize(src_.get());
}

template <typename Src>
inline WrappedReader<Src>::WrappedReader(Src&& src) : src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline WrappedReader<Src>::WrappedReader(std::tuple<SrcArgs...> src_args)
    : src_(std::move(src_args)) {
  Initialize(src_.get());
}

template <typename Src>
inline WrappedReader<Src>::WrappedReader(WrappedReader&& that) noexcept
    : WrappedReaderBase(std::move(that)) {
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  MoveSrc(std::move(that));
}

template <typename Src>
inline WrappedReader<Src>& WrappedReader<Src>::operator=(
    WrappedReader&& that) noexcept {
  WrappedReaderBase::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
  MoveSrc(std::move(that));
  return *this;
}

template <typename Src>
inline void WrappedReader<Src>::Reset(Closed) {
  WrappedReaderBase::Reset(kClosed);
  src_.Reset();
}

template <typename Src>
inline void WrappedReader<Src>::Reset(const Src& src) {
  WrappedReaderBase::Reset();
  src_.Reset(src);
  Initialize(src_.get());
}

template <typename Src>
inline void WrappedReader<Src>::Reset(Src&& src) {
  WrappedReaderBase::Reset();
  src_.Reset(std::move(src));
  Initialize(src_.get());
}

template <typename Src>
template <typename... SrcArgs>
inline void WrappedReader<Src>::Reset(std::tuple<SrcArgs...> src_args) {
  WrappedReaderBase::Reset();
  src_.Reset(std::move(src_args));
  Initialize(src_.get());
}

template <typename Src>
inline void WrappedReader<Src>::MoveSrc(WrappedReader&& that) {
  if (src_.kIsStable() || that.src_.get() == nullptr) {
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
void WrappedReader<Src>::Done() {
  WrappedReaderBase::Done();
  if (src_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) {
      FailWithoutAnnotation(src_->status());
    }
  }
}

template <typename Src>
void WrappedReader<Src>::VerifyEnd() {
  WrappedReaderBase::VerifyEnd();
  if (src_.is_owning() && ABSL_PREDICT_TRUE(healthy())) {
    SyncBuffer(*src_);
    src_->VerifyEnd();
    MakeBuffer(*src_);
  }
}

template <typename Src>
bool WrappedReader<Src>::SyncImpl(SyncType sync_type) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  SyncBuffer(*src_);
  bool ok = true;
  if (sync_type != SyncType::kFromObject || src_.is_owning()) {
    ok = src_->Sync(sync_type);
  }
  MakeBuffer(*src_);
  return ok;
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_WRAPPED_READER_H_
