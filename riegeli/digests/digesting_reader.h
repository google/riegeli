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
#include "absl/meta/type_traits.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/object.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/digests/digest_converter.h"
#include "riegeli/digests/digester_handle.h"

namespace riegeli {

// Template parameter independent part of `DigestingReader`.
class DigestingReaderBase : public Reader {
 public:
  // Returns the `DigesterBaseHandle`. Unchanged by `Close()`.
  virtual DigesterBaseHandle GetDigester() const = 0;

  // Returns the original `Reader`. Unchanged by `Close()`.
  virtual Reader* SrcReader() const = 0;

  bool SupportsSize() override;
  bool SupportsNewReader() override;

 protected:
  using Reader::Reader;

  DigestingReaderBase(DigestingReaderBase&& that) noexcept;
  DigestingReaderBase& operator=(DigestingReaderBase&& that) noexcept;

  void Initialize(Reader* src, DigesterBaseHandle digester);
  ABSL_ATTRIBUTE_COLD bool FailFromDigester();

  void Done() override;
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;

  virtual bool WriteToDigester(absl::string_view src) = 0;

  // Sets cursor of `src` to cursor of `*this`, digesting what has been read
  // from the buffer (until `cursor()`).
  bool SyncBuffer(Reader& src);

  // Sets buffer pointers of `*this` to buffer pointers of `src`, adjusting
  // `start()` to hide data already digested. Fails `*this` if `src` failed.
  void MakeBuffer(Reader& src);

  bool PullSlow(size_t min_length, size_t recommended_length) override;
  using Reader::ReadSlow;
  bool ReadSlow(size_t length, char* dest) override;
  bool ReadSlow(size_t length, Chain& dest) override;
  bool ReadSlow(size_t length, absl::Cord& dest) override;
  using Reader::ReadOrPullSomeSlow;
  bool ReadOrPullSomeSlow(size_t max_length,
                          absl::FunctionRef<char*(size_t&)> get_dest) override;
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
// The `DigesterType` template parameter specifies the type of the object
// providing and possibly owning the digester. `DigesterType` must support
// `Dependency<DigesterBaseHandle, DigesterType>`, e.g.
// `DigesterHandle<uint32_t>` (not owned), `Crc32cDigester` (owned),
// `AnyDependency<DigesterHandle<uint32_t>>` (maybe owned).
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the original `Reader`. `Src` must support
// `Dependency<Reader*, Src>`, e.g. `Reader*` (not owned, default),
// `ChainReader<>` (owned), `std::unique_ptr<Reader>` (owned),
// `AnyDependency<Reader*>` (maybe owned).
//
// By relying on CTAD the `DigesterType` template argument can be deduced as the
// value type of the `digester` constructor argument, and the `Src` template
// argument can be deduced as the value type of the `src` constructor argument.
// This requires C++17.
//
// The original `Reader` must not be accessed until the `DigestingReader` is
// closed or no longer used.
template <typename DigesterType, typename Src = Reader*>
class DigestingReader : public DigestingReaderBase {
 public:
  // The type of the digest.
  using DigestType = DigestOf<DigesterType>;

  // Creates a closed `DigestingReader`.
  explicit DigestingReader(Closed) noexcept : DigestingReaderBase(kClosed) {}

  // Will read from the original `Reader` provided by `src`, using the
  // digester provided by `digester`.
  explicit DigestingReader(
      Initializer<Src> src,
      Initializer<DigesterType> digester = std::forward_as_tuple());

  DigestingReader(DigestingReader&& that) noexcept;
  DigestingReader& operator=(DigestingReader&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `DigestingReader`. This
  // avoids constructing a temporary `DigestingReader` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      Initializer<Src> src,
      Initializer<DigesterType> digester = std::forward_as_tuple());

  // Digests buffered data if needed, and returns the digest.
  //
  // The digest is converted to `DesiredDigestType` using `DigestConverter`.
  template <
      typename DesiredDigestType = DigestType,
      std::enable_if_t<HasDigestConverter<DigestType, DesiredDigestType>::value,
                       int> = 0>
  DesiredDigestType Digest() {
    if (start_to_cursor() > 0) {
      if (ABSL_PREDICT_FALSE(!digester_.get().Write(
              absl::string_view(start(), start_to_cursor())))) {
        FailFromDigester();
      } else {
        set_buffer(cursor(), available());
        src_->set_cursor(cursor());
      }
    }
    return digester_.get().template Digest<DesiredDigestType>();
  }

  // Returns the object providing and possibly owning the digester. Unchanged by
  // `Close()`.
  DigesterType& digester() { return digester_.manager(); }
  const DigesterType& digester() const { return digester_.manager(); }
  DigesterBaseHandle GetDigester() const override { return digester_.get(); }

  // Returns the object providing and possibly owning the original `Reader`.
  // Unchanged by `Close()`.
  Src& src() { return src_.manager(); }
  const Src& src() const { return src_.manager(); }
  Reader* SrcReader() const override { return src_.get(); }

 protected:
  void Done() override;
  bool WriteToDigester(absl::string_view src) override {
    return digester_.get().Write(src);
  }
  void SetReadAllHintImpl(bool read_all_hint) override;
  void VerifyEndImpl() override;
  bool SyncImpl(SyncType sync_type) override;

 private:
  // Moves `that.src_` to `src_`. Buffer pointers are already moved from `src_`
  // to `*this`; adjust them to match `src_`.
  void MoveSrc(DigestingReader&& that);

  // The object providing and possibly owning the digester.
  Dependency<DigesterBaseHandle, DigesterType> digester_;
  // The object providing and possibly owning the original `Reader`.
  Dependency<Reader*, Src> src_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit DigestingReader(Closed) -> DigestingReader<void, DeleteCtad<Closed>>;
template <typename DigesterType, typename Src>
explicit DigestingReader(Src&& src, DigesterType&& digester)
    -> DigestingReader<std::decay_t<DigesterType>, std::decay_t<Src>>;
template <typename... DigesterArgs, typename Src>
explicit DigestingReader(Src&& src, std::tuple<DigesterArgs...> digester_args =
                                        std::forward_as_tuple())
    -> DigestingReader<DeleteCtad<std::tuple<DigesterArgs...>>,
                       std::decay_t<Src>>;
template <typename DigesterType, typename... SrcArgs>
explicit DigestingReader(std::tuple<SrcArgs...> src_args,
                         DigesterType&& digester)
    -> DigestingReader<std::decay_t<DigesterType>,
                       DeleteCtad<std::tuple<SrcArgs...>>>;
template <typename... DigesterArgs, typename... SrcArgs>
explicit DigestingReader(
    std::tuple<SrcArgs...> src_args,
    std::tuple<DigesterArgs...> digester_args = std::forward_as_tuple())
    -> DigestingReader<DeleteCtad<std::tuple<DigesterArgs...>>,
                       DeleteCtad<std::tuple<SrcArgs...>>>;
#endif

// Reads all remaining data from `src` and returns their digest.
//
// If `length_read != nullptr` then sets `*length_read` to the length read.
// This is equal to the difference between `src.pos()` after and before the
// call.
//
// The `DigesterType` template parameter specifies the type of the object
// providing and possibly owning the digester. `DigesterType` must support
// `Dependency<DigesterBaseHandle, DigesterType&&>` and must provide a member
// function `DigestType Digest()` for some `DigestType`, e.g.
// `DigesterHandle<uint32_t>` (not owned), `Crc32cDigester` (owned),
// `AnyDependency<DigesterHandle<uint32_t>>` (maybe owned).
//
// The `Src` template parameter specifies the type of the object providing and
// possibly owning the `Reader`. `Src` must support
// `Dependency<Reader*, Src&&>`, e.g. `Reader&` (not owned),
// `ChainReader<>` (owned), `std::unique_ptr<Reader>` (owned),
// `AnyDependency<Reader*>` (maybe owned).
//
// The digest is converted to `DesiredDigestType` using `DigestConverter`.
template <typename DesiredDigestType = digest_converter_internal::NoConversion,
          typename DigesterType, typename Src,
          std::enable_if_t<
              absl::conjunction<
                  IsValidDependency<DigesterBaseHandle, DigesterType&&>,
                  IsValidDependency<Reader*, Src&&>,
                  digest_converter_internal::HasDigestConverterOrNoConversion<
                      DigestOf<DigesterType&&>, DesiredDigestType>>::value,
              int> = 0>
StatusOrMakerT<digest_converter_internal::ResolveNoConversion<
    DigestOf<DigesterType&&>, DesiredDigestType>>
DigestFromReader(Src&& src, DigesterType&& digester,
                 Position* length_read = nullptr);

// Implementation details follow.

inline DigestingReaderBase::DigestingReaderBase(
    DigestingReaderBase&& that) noexcept
    : Reader(static_cast<Reader&&>(that)) {}

inline DigestingReaderBase& DigestingReaderBase::operator=(
    DigestingReaderBase&& that) noexcept {
  Reader::operator=(static_cast<Reader&&>(that));
  return *this;
}

inline void DigestingReaderBase::Initialize(Reader* src,
                                            DigesterBaseHandle digester) {
  RIEGELI_ASSERT(src != nullptr)
      << "Failed precondition of DigestingReader: null Reader pointer";
  MakeBuffer(*src);
  absl::Status status = digester.status();
  if (ABSL_PREDICT_FALSE(!status.ok())) Fail(std::move(status));
}

inline bool DigestingReaderBase::SyncBuffer(Reader& src) {
  RIEGELI_ASSERT(start() == src.cursor())
      << "Failed invariant of DigestingReaderBase: "
         "cursor of the original Reader changed unexpectedly";
  if (start_to_cursor() > 0) {
    if (ABSL_PREDICT_FALSE(
            !WriteToDigester(absl::string_view(start(), start_to_cursor())))) {
      if (FailFromDigester()) RIEGELI_ASSERT_UNREACHABLE();
      return false;
    }
    src.set_cursor(cursor());
  }
  return true;
}

inline void DigestingReaderBase::MakeBuffer(Reader& src) {
  set_buffer(src.cursor(), src.available());
  set_limit_pos(src.limit_pos());
  if (ABSL_PREDICT_FALSE(!src.ok())) FailWithoutAnnotation(src.status());
}

template <typename DigesterType, typename Src>
inline DigestingReader<DigesterType, Src>::DigestingReader(
    Initializer<Src> src, Initializer<DigesterType> digester)
    : digester_(std::move(digester)), src_(std::move(src)) {
  Initialize(src_.get(), digester_.get());
}

template <typename DigesterType, typename Src>
inline DigestingReader<DigesterType, Src>::DigestingReader(
    DigestingReader&& that) noexcept
    : DigestingReaderBase(static_cast<DigestingReaderBase&&>(that)),
      digester_(std::move(that.digester_)) {
  MoveSrc(std::move(that));
}

template <typename DigesterType, typename Src>
inline DigestingReader<DigesterType, Src>&
DigestingReader<DigesterType, Src>::operator=(DigestingReader&& that) noexcept {
  DigestingReaderBase::operator=(static_cast<DigestingReaderBase&&>(that));
  digester_ = std::move(that.digester_);
  MoveSrc(std::move(that));
  return *this;
}

template <typename DigesterType, typename Src>
inline void DigestingReader<DigesterType, Src>::Reset(Closed) {
  DigestingReaderBase::Reset(kClosed);
  digester_.Reset();
  src_.Reset();
}

template <typename DigesterType, typename Src>
inline void DigestingReader<DigesterType, Src>::Reset(
    Initializer<Src> src, Initializer<DigesterType> digester) {
  DigestingReaderBase::Reset();
  digester_.Reset(std::move(digester));
  src_.Reset(std::move(src));
  Initialize(src_.get(), digester_.get());
}

template <typename DigesterType, typename Src>
inline void DigestingReader<DigesterType, Src>::MoveSrc(
    DigestingReader&& that) {
  if (src_.kIsStable || that.src_ == nullptr) {
    src_ = std::move(that.src_);
  } else {
    // Buffer pointers are already moved so `SyncBuffer()` is called on `*this`,
    // `src_` is not moved yet so `src_` is taken from `that`.
    const bool sync_buffer_ok = SyncBuffer(*that.src_);
    src_ = std::move(that.src_);
    if (ABSL_PREDICT_TRUE(sync_buffer_ok)) MakeBuffer(*src_);
  }
}

template <typename DigesterType, typename Src>
void DigestingReader<DigesterType, Src>::Done() {
  DigestingReaderBase::Done();
  if (src_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!src_->Close())) {
      FailWithoutAnnotation(src_->status());
    }
  }
  if (digester_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!digester_.get().Close())) FailFromDigester();
  }
}

template <typename DigesterType, typename Src>
void DigestingReader<DigesterType, Src>::SetReadAllHintImpl(
    bool read_all_hint) {
  if (src_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!SyncBuffer(*src_))) return;
    src_->SetReadAllHint(read_all_hint);
    MakeBuffer(*src_);
  }
}

template <typename DigesterType, typename Src>
void DigestingReader<DigesterType, Src>::VerifyEndImpl() {
  if (!src_.IsOwning()) {
    DigestingReaderBase::VerifyEndImpl();
  } else if (ABSL_PREDICT_TRUE(ok())) {
    if (ABSL_PREDICT_FALSE(!SyncBuffer(*src_))) return;
    src_->VerifyEnd();
    MakeBuffer(*src_);
  }
}

template <typename DigesterType, typename Src>
bool DigestingReader<DigesterType, Src>::SyncImpl(SyncType sync_type) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (ABSL_PREDICT_FALSE(!SyncBuffer(*src_))) return false;
  bool sync_ok = true;
  if (sync_type != SyncType::kFromObject || src_.IsOwning()) {
    sync_ok = src_->Sync(sync_type);
  }
  MakeBuffer(*src_);
  return sync_ok;
}

template <typename DesiredDigestType, typename DigesterType, typename Src,
          std::enable_if_t<
              absl::conjunction<
                  IsValidDependency<DigesterBaseHandle, DigesterType&&>,
                  IsValidDependency<Reader*, Src&&>,
                  digest_converter_internal::HasDigestConverterOrNoConversion<
                      DigestOf<DigesterType&&>, DesiredDigestType>>::value,
              int>>
inline StatusOrMakerT<digest_converter_internal::ResolveNoConversion<
    DigestOf<DigesterType&&>, DesiredDigestType>>
DigestFromReader(Src&& src, DigesterType&& digester, Position* length_read) {
  using DigestType =
      digest_converter_internal::ResolveNoConversion<DigestOf<DigesterType&&>,
                                                     DesiredDigestType>;
  using Maker = StatusOrMaker<DigestType>;
  DigestingReader<DigesterType&&, Src&&> reader(
      std::forward<Src>(src), std::forward<DigesterType&&>(digester));
  reader.SetReadAllHint(true);
  const Position pos_before = reader.pos();
  do {
    reader.move_cursor(reader.available());
  } while (reader.Pull());
  RIEGELI_ASSERT_GE(reader.pos(), pos_before)
      << "DigestingReader decreased src.pos()";
  if (length_read != nullptr) *length_read = reader.pos() - pos_before;
  if (ABSL_PREDICT_FALSE(!reader.VerifyEndAndClose())) {
    return Maker::FromStatus(reader.status());
  }
  return Maker::FromWork(
      [&]() -> DigestType { return reader.template Digest<DigestType>(); });
}

}  // namespace riegeli

#endif  // RIEGELI_DIGESTS_DIGESTING_READER_H_
