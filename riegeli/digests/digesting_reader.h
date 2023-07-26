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

#ifndef RIEGELI_DIGESTS_DIGESTING_READER_H_
#define RIEGELI_DIGESTS_DIGESTING_READER_H_

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
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/reset.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/digests/digesting_internal.h"

namespace riegeli {

// Template parameter independent part of `DigestingReader`.
class DigestingReaderBase : public Reader {
 public:
  // Returns the original `Reader`. Unchanged by `Close()`.
  virtual Reader* SrcReader() = 0;
  virtual const Reader* SrcReader() const = 0;

  bool SupportsSize() override;
  bool SupportsNewReader() override;

 protected:
  using Reader::Reader;

  DigestingReaderBase(DigestingReaderBase&& that) noexcept;
  DigestingReaderBase& operator=(DigestingReaderBase&& that) noexcept;

  void Initialize(Reader* src);

  void Done() override;
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;

  // Sets cursor of `src` to cursor of `*this`, digesting what has been read
  // from the buffer (until `cursor()`).
  void SyncBuffer(Reader& src);

  // Sets buffer pointers of `*this` to buffer pointers of `src`, adjusting
  // `start()` to hide data already digested. Fails `*this` if `src` failed.
  void MakeBuffer(Reader& src);

  virtual void DigesterWrite(absl::string_view src) = 0;
  void DigesterWrite(const Chain& src);
  void DigesterWrite(const absl::Cord& src);

  bool PullSlow(size_t min_length, size_t recommended_length) override;
  using Reader::ReadSlow;
  bool ReadSlow(size_t length, char* dest) override;
  bool ReadSlow(size_t length, Chain& dest) override;
  bool ReadSlow(size_t length, absl::Cord& dest) override;
  using Reader::ReadSomeDirectlySlow;
  bool ReadSomeDirectlySlow(
      size_t max_length, absl::FunctionRef<char*(size_t&)> get_dest) override;
  void ReadHintSlow(size_t min_length, size_t recommended_length) override;
  absl::optional<Position> SizeImpl() override;
  std::unique_ptr<Reader> NewReaderImpl(Position initial_pos) override;

  // Invariants if `is_open()`:
  //   `start() == SrcReader()->cursor()`
  //   `limit() == SrcReader()->limit()`
  //   `limit_pos() == SrcReader()->limit_pos()`
};

// A `Reader` which reads from another `Reader`, and lets another object observe
// data being read and return some data called a digest, e.g. a checksum.
//
// The `Digester` template parameter specifies how data are being digested.
// `DigestingReader` forwards basic operations to `Digester`: constructor
// with forwarded parameters after `src`, move constructor, move assignment,
// destructor, and optionally `Reset()`. Apart from that, `Digester` should
// support:
//
// ```
//   // Called with consecutive fragments of data.
//   void Write(absl::string_view src);
//
//   // `WriteZeros()` is not used by `DigestingReader` but is used by .
//   // `DigestingWriter`.
//
//   // Called when nothing more will be digested. Resources can be freed.
//   //
//   // This method is optional. If that is not defined, nothing is done.
//   void Close();
//
//   // Returns the digest. Its type and meaning depends on the `Digester`.
//   // Unchanged by `Close()`.
//   //
//   // This method is optional. If that is not defined, nothing is done and
//   // `void` is returned.
//   DigestType Digest();
// ```
//
// Alternatively, `Digester` can be a pointer or smart pointer to a type
// supporting the above operations. This allows to extract the digest after
// the `DigestingReader` is destroyed.
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the original `Reader`. `Src` must support
// `Dependency<Reader*, Src>`, e.g. `Reader*` (not owned, default),
// `ChainReader<>` (owned), `std::unique_ptr<Reader>` (owned),
// `AnyDependency<Reader*>` (maybe owned).
//
// By relying on CTAD the first template argument can be deduced as the value
// type of the second constructor argument (there must be two constructor
// arguments for CTAD), and the second template argument can be deduced as the
// value type of the first constructor argument. This requires C++17.
//
// The original `Reader` must not be accessed until the `DigestingReader` is
// closed or no longer used.
template <typename Digester, typename Src = Reader*>
class DigestingReader : public DigestingReaderBase {
 public:
  // The type of the digest.
  using DigestType = digesting_internal::DigestType<Digester>;

  // Creates a closed `DigestingReader`.
  explicit DigestingReader(Closed) noexcept : DigestingReaderBase(kClosed) {}

  // Will read from the original `Reader` provided by `src`. Constructs a
  // `Digester` from `digester_args`.
  template <typename... DigesterArgs>
  explicit DigestingReader(const Src& src, DigesterArgs&&... digester_args);
  template <typename... DigesterArgs>
  explicit DigestingReader(Src&& src, DigesterArgs&&... digester_args);

  // Will read from the original `Reader` provided by a `Src` constructed from
  // elements of `src_args`. This avoids constructing a temporary `Src` and
  // moving from it.
  template <typename... SrcArgs, typename... DigesterArgs>
  explicit DigestingReader(std::tuple<SrcArgs...> src_args,
                           DigesterArgs&&... digester_args);

  DigestingReader(DigestingReader&& that) noexcept;
  DigestingReader& operator=(DigestingReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `DigestingReader`. This
  // avoids constructing a temporary `DigestingReader` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  template <typename... DigesterArgs>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(const Src& src,
                                          DigesterArgs&&... digester_args);
  template <typename... DigesterArgs>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Src&& src,
                                          DigesterArgs&&... digester_args);
  template <typename... SrcArgs, typename... DigesterArgs>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(std::tuple<SrcArgs...> src_args,
                                          DigesterArgs&&... digester_args);

  // Digests buffered data if needed, and returns the digest.
  DigestType Digest();

  // Returns the object providing and possibly owning the original `Reader`.
  // Unchanged by `Close()`.
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  Reader* SrcReader() override { return src_.get(); }
  const Reader* SrcReader() const override { return src_.get(); }

 protected:
  void Done() override;

  using DigestingReaderBase::DigesterWrite;
  void DigesterWrite(absl::string_view src) override;

  void SetReadAllHintImpl(bool read_all_hint) override;
  void VerifyEndImpl() override;
  bool SyncImpl(SyncType sync_type) override;

 private:
  // Moves `that.src_` to `src_`. Buffer pointers are already moved from `src_`
  // to `*this`; adjust them to match `src_`.
  void MoveSrc(DigestingReader&& that);

  Digester digester_;
  // The object providing and possibly owning the original `Reader`.
  Dependency<Reader*, Src> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit DigestingReader(Closed) -> DigestingReader<void, DeleteCtad<Closed>>;
template <typename Digester, typename Src>
explicit DigestingReader(const Src& src, Digester&& digester)
    -> DigestingReader<std::decay_t<Digester>, std::decay_t<Src>>;
template <typename Digester, typename Src>
explicit DigestingReader(Src&& src, Digester&& digester)
    -> DigestingReader<std::decay_t<Digester>, std::decay_t<Src>>;
template <typename Digester, typename... SrcArgs>
explicit DigestingReader(std::tuple<SrcArgs...> src_args, Digester&& digester)
    -> DigestingReader<void, DeleteCtad<std::tuple<SrcArgs...>>>;
#endif

// Implementation details follow.

inline DigestingReaderBase::DigestingReaderBase(
    DigestingReaderBase&& that) noexcept
    : Reader(static_cast<Reader&&>(that)) {}

inline DigestingReaderBase& DigestingReaderBase::operator=(
    DigestingReaderBase&& that) noexcept {
  Reader::operator=(static_cast<Reader&&>(that));
  return *this;
}

inline void DigestingReaderBase::Initialize(Reader* src) {
  RIEGELI_ASSERT(src != nullptr)
      << "Failed precondition of DigestingReader: null Reader pointer";
  MakeBuffer(*src);
}

inline void DigestingReaderBase::SyncBuffer(Reader& src) {
  RIEGELI_ASSERT(start() == src.cursor())
      << "Failed invariant of DigestingReaderBase: "
         "cursor of the original Reader changed unexpectedly";
  if (start_to_cursor() > 0) {
    DigesterWrite(absl::string_view(start(), start_to_cursor()));
  }
  src.set_cursor(cursor());
}

inline void DigestingReaderBase::MakeBuffer(Reader& src) {
  set_buffer(src.cursor(), src.available());
  set_limit_pos(src.limit_pos());
  if (ABSL_PREDICT_FALSE(!src.ok())) FailWithoutAnnotation(src.status());
}

template <typename Digester, typename Src>
template <typename... DigesterArgs>
inline DigestingReader<Digester, Src>::DigestingReader(
    const Src& src, DigesterArgs&&... digester_args)
    : digester_(std::forward<DigesterArgs>(digester_args)...), src_(src) {
  Initialize(src_.get());
}

template <typename Digester, typename Src>
template <typename... DigesterArgs>
inline DigestingReader<Digester, Src>::DigestingReader(
    Src&& src, DigesterArgs&&... digester_args)
    : digester_(std::forward<DigesterArgs>(digester_args)...),
      src_(std::move(src)) {
  Initialize(src_.get());
}

template <typename Digester, typename Src>
template <typename... SrcArgs, typename... DigesterArgs>
inline DigestingReader<Digester, Src>::DigestingReader(
    std::tuple<SrcArgs...> src_args, DigesterArgs&&... digester_args)
    : digester_(std::forward<DigesterArgs>(digester_args)...),
      src_(std::move(src_args)) {
  Initialize(src_.get());
}

template <typename Digester, typename Src>
inline DigestingReader<Digester, Src>::DigestingReader(
    DigestingReader&& that) noexcept
    : DigestingReaderBase(static_cast<DigestingReaderBase&&>(that)),
      digester_(std::move(that.digester_)) {
  MoveSrc(std::move(that));
}

template <typename Digester, typename Src>
inline DigestingReader<Digester, Src>&
DigestingReader<Digester, Src>::operator=(DigestingReader&& that) noexcept {
  DigestingReaderBase::operator=(static_cast<DigestingReaderBase&&>(that));
  digester_ = std::move(that.digester_);
  MoveSrc(std::move(that));
  return *this;
}

template <typename Digester, typename Src>
inline void DigestingReader<Digester, Src>::Reset(Closed) {
  DigestingReaderBase::Reset(kClosed);
  riegeli::Reset(digester_);
  src_.Reset();
}

template <typename Digester, typename Src>
template <typename... DigesterArgs>
inline void DigestingReader<Digester, Src>::Reset(
    const Src& src, DigesterArgs&&... digester_args) {
  DigestingReaderBase::Reset();
  riegeli::Reset(digester_, std::forward<DigesterArgs>(digester_args)...);
  src_.Reset(src);
  Initialize(src_.get());
}

template <typename Digester, typename Src>
template <typename... DigesterArgs>
inline void DigestingReader<Digester, Src>::Reset(
    Src&& src, DigesterArgs&&... digester_args) {
  DigestingReaderBase::Reset();
  riegeli::Reset(digester_, std::forward<DigesterArgs>(digester_args)...);
  src_.Reset(std::move(src));
  Initialize(src_.get());
}

template <typename Digester, typename Src>
template <typename... SrcArgs, typename... DigesterArgs>
inline void DigestingReader<Digester, Src>::Reset(
    std::tuple<SrcArgs...> src_args, DigesterArgs&&... digester_args) {
  DigestingReaderBase::Reset();
  riegeli::Reset(digester_, std::forward<DigesterArgs>(digester_args)...);
  src_.Reset(std::move(src_args));
  Initialize(src_.get());
}

template <typename Digester, typename Src>
inline void DigestingReader<Digester, Src>::MoveSrc(DigestingReader&& that) {
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

template <typename Digester, typename Src>
inline typename DigestingReader<Digester, Src>::DigestType
DigestingReader<Digester, Src>::Digest() {
  if (start_to_cursor() > 0) {
    DigesterWrite(absl::string_view(start(), start_to_cursor()));
    set_buffer(cursor(), available());
  }
  return digesting_internal::Digest(digesting_internal::Dereference(digester_));
}

template <typename Digester, typename Src>
void DigestingReader<Digester, Src>::Done() {
  DigestingReaderBase::Done();
  if (src_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) {
      FailWithoutAnnotation(src_->status());
    }
  }
  digesting_internal::Close(digesting_internal::Dereference(digester_));
}

template <typename Digester, typename Src>
void DigestingReader<Digester, Src>::DigesterWrite(absl::string_view src) {
  digesting_internal::Dereference(digester_).Write(src);
}

template <typename Digester, typename Src>
void DigestingReader<Digester, Src>::SetReadAllHintImpl(bool read_all_hint) {
  if (src_.is_owning()) src_->SetReadAllHint(read_all_hint);
}

template <typename Digester, typename Src>
void DigestingReader<Digester, Src>::VerifyEndImpl() {
  if (!src_.is_owning()) {
    DigestingReaderBase::VerifyEndImpl();
  } else if (ABSL_PREDICT_TRUE(ok())) {
    SyncBuffer(*src_);
    src_->VerifyEnd();
    MakeBuffer(*src_);
  }
}

template <typename Digester, typename Src>
bool DigestingReader<Digester, Src>::SyncImpl(SyncType sync_type) {
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

#endif  // RIEGELI_DIGESTS_DIGESTING_READER_H_
