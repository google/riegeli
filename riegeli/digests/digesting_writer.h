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

#ifndef RIEGELI_DIGESTS_DIGESTING_WRITER_H_
#define RIEGELI_DIGESTS_DIGESTING_WRITER_H_

#include <stddef.h>

#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/meta/type_traits.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/external_ref.h"
#include "riegeli/base/initializer.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/moving_dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/type_traits.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/null_writer.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/digests/digest_converter.h"
#include "riegeli/digests/digester_handle.h"

namespace riegeli {

class Reader;

// Template parameter independent part of `DigestingWriter`.
class DigestingWriterBase : public Writer {
 public:
  // Returns the `DigesterBaseHandle`. Unchanged by `Close()`.
  virtual DigesterBaseHandle GetDigester() const = 0;

  // Returns the original `Writer`. Unchanged by `Close()`.
  virtual Writer* DestWriter() const = 0;

  bool SupportsReadMode() override;

 protected:
  using Writer::Writer;

  DigestingWriterBase(DigestingWriterBase&& that) noexcept;
  DigestingWriterBase& operator=(DigestingWriterBase&& that) noexcept;

  void Initialize(Writer* dest, DigesterBaseHandle digester);
  ABSL_ATTRIBUTE_COLD bool FailFromDigester();

  void Done() override;
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;

  virtual bool WriteToDigester(absl::string_view src) = 0;

  // Sets cursor of `dest` to cursor of `*this`, digesting what has been written
  // to the buffer (until `cursor()`).
  bool SyncBuffer(Writer& dest);

  // Sets buffer pointers of `*this` to buffer pointers of `dest`, adjusting
  // `start()` to hide data already digested. Fails `*this` if `dest` failed.
  void MakeBuffer(Writer& dest);

  bool PushSlow(size_t min_length, size_t recommended_length) override;
  using Writer::WriteSlow;
  bool WriteSlow(absl::string_view src) override;
  bool WriteSlow(const Chain& src) override;
  bool WriteSlow(Chain&& src) override;
  bool WriteSlow(const absl::Cord& src) override;
  bool WriteSlow(absl::Cord&& src) override;
  bool WriteSlow(ExternalRef src) override;
  bool WriteZerosSlow(Position length) override;
  Reader* ReadModeImpl(Position initial_pos) override;

 private:
  // This template is defined and used only in digesting_writer.cc.
  template <typename Src>
  bool WriteInternal(Src&& src);

  // Invariants if `ok()`:
  //   `start() == DestWriter()->cursor()`
  //   `limit() == DestWriter()->limit()`
  //   `start_pos() == DestWriter()->pos()`
};

// A `Writer` which writes to another `Writer`, and lets another object observe
// data being written and return some data called a digest, e.g. a checksum.
//
// The `Digester` template parameter specifies the type of the object providing
// and possibly owning the digester. `Digester` must support
// `Dependency<DigesterBaseHandle, Digester>`, e.g.
// `DigesterHandle<uint32_t>` (not owned), `Crc32cDigester` (owned),
// `AnyDigester<uint32_t>>` (maybe owned).
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the original `Writer`. `Dest` must support
// `Dependency<Writer*, Dest>`, e.g. `Writer*` (not owned, default),
// `ChainWriter<>` (owned), `std::unique_ptr<Writer>` (owned),
// `Any<Writer*>` (maybe owned).
//
// By relying on CTAD the `Digester` template argument can be deduced as
// `InitializerTargetT` of the type of the `digester` constructor argument, and
// the `Dest` template argument can be deduced as `InitializerTargetT` of the
// type of the `dest` constructor argument. This requires C++17.
//
// The original `Writer` must not be accessed until the `DigestingWriter` is
// closed or no longer used, except that it is allowed to read the destination
// of the original `Writer` immediately after `Flush()`.
template <typename Digester, typename Dest = Writer*>
class DigestingWriter : public DigestingWriterBase {
 public:
  // The type of the digest.
  using DigestType = DigestOf<Digester>;

  // Creates a closed `DigestingWriter`.
  explicit DigestingWriter(Closed) noexcept : DigestingWriterBase(kClosed) {}

  // Will write to the original `Writer` provided by `dest`, using the
  // digester provided by `digester`.
  explicit DigestingWriter(Initializer<Dest> dest,
                           Initializer<Digester> digester = riegeli::Maker());

  DigestingWriter(DigestingWriter&& that) = default;
  DigestingWriter& operator=(DigestingWriter&& that) = default;

  // Makes `*this` equivalent to a newly constructed `DigestingWriter`. This
  // avoids constructing a temporary `DigestingWriter` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      Initializer<Dest> dest,
      Initializer<Digester> digester = riegeli::Maker());

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
        set_start_pos(pos());
        set_buffer(cursor(), available());
        dest_->set_cursor(cursor());
      }
    }
    return digester_.get().template Digest<DesiredDigestType>();
  }

  // Returns the object providing and possibly owning the digester. Unchanged by
  // `Close()`.
  Digester& digester() { return digester_.manager(); }
  const Digester& digester() const { return digester_.manager(); }
  DigesterBaseHandle GetDigester() const override { return digester_.get(); }

  // Returns the object providing and possibly owning the original `Writer`.
  // Unchanged by `Close()`.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  Writer* DestWriter() const override { return dest_.get(); }

 protected:
  void Done() override;
  bool WriteToDigester(absl::string_view src) override {
    return digester_.get().Write(src);
  }
  void SetWriteSizeHintImpl(absl::optional<Position> write_size_hint) override;
  bool FlushImpl(FlushType flush_type) override;

 private:
  class Mover;

  // The object providing and possibly owning the digester.
  Dependency<DigesterBaseHandle, Digester> digester_;
  // The object providing and possibly owning the original `Writer`.
  MovingDependency<Writer*, Dest, Mover> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit DigestingWriter(Closed) -> DigestingWriter<void, DeleteCtad<Closed>>;
template <typename Digester, typename Dest>
explicit DigestingWriter(Dest&& dest, Digester&& digester = riegeli::Maker())
    -> DigestingWriter<InitializerTargetT<Digester>, InitializerTargetT<Dest>>;
#endif

// Returns the digest of the concatenation of stringifiable values.
//
// The last argument is the digester of some type `Digester`. The remaining
// arguments are the values.
//
// `Digester` specifies the type of the object providing and possibly owning
// the digester. `Digester` must support
// `Dependency<DigesterBaseHandle, Digester&&>` and must provide a member
// function `DigestType Digest()` for some `DigestType`, e.g.
// `DigesterHandle<uint32_t>` (not owned), `Crc32cDigester` (owned),
// `AnyDigester<uint32_t>>` (maybe owned).
//
// The digester should not be expected to fail. If it fails, the process
// terminates.
//
// The digest is converted to `DesiredDigestType` using `DigestConverter`.
template <typename DesiredDigestType = digest_converter_internal::NoConversion,
          typename... Args,
          std::enable_if_t<
              absl::conjunction<
                  IsValidDependency<DigesterBaseHandle,
                                    GetTypeFromEndT<1, Args&&...>>,
                  TupleElementsSatisfy<RemoveTypesFromEndT<1, Args&&...>,
                                       IsStringifiable>,
                  digest_converter_internal::HasDigestConverterOrNoConversion<
                      DigestOf<GetTypeFromEndT<1, Args&&...>>,
                      DesiredDigestType>>::value,
              int> = 0>
digest_converter_internal::ResolveNoConversion<
    DigestOf<GetTypeFromEndT<1, Args&&...>>, DesiredDigestType>
DigestFrom(Args&&... args);

// Implementation details follow.

inline DigestingWriterBase::DigestingWriterBase(
    DigestingWriterBase&& that) noexcept
    : Writer(static_cast<Writer&&>(that)) {}

inline DigestingWriterBase& DigestingWriterBase::operator=(
    DigestingWriterBase&& that) noexcept {
  Writer::operator=(static_cast<Writer&&>(that));
  return *this;
}

inline void DigestingWriterBase::Initialize(Writer* dest,
                                            DigesterBaseHandle digester) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of DigestingWriter: null Writer pointer";
  MakeBuffer(*dest);
  absl::Status status = digester.status();
  if (ABSL_PREDICT_FALSE(!status.ok())) Fail(std::move(status));
}

inline bool DigestingWriterBase::SyncBuffer(Writer& dest) {
  RIEGELI_ASSERT(start() == dest.cursor())
      << "Failed invariant of DigestingWriterBase: "
         "cursor of the original Writer changed unexpectedly";
  if (start_to_cursor() > 0) {
    if (ABSL_PREDICT_FALSE(
            !WriteToDigester(absl::string_view(start(), start_to_cursor())))) {
      if (FailFromDigester()) RIEGELI_ASSERT_UNREACHABLE();
      return false;
    }
    dest.set_cursor(cursor());
  }
  return true;
}

inline void DigestingWriterBase::MakeBuffer(Writer& dest) {
  set_buffer(dest.cursor(), dest.available());
  set_start_pos(dest.pos());
  if (ABSL_PREDICT_FALSE(!dest.ok())) FailWithoutAnnotation(dest.status());
}

template <typename Digester, typename Dest>
class DigestingWriter<Digester, Dest>::Mover {
 public:
  static auto member() { return &DigestingWriter::dest_; }

  explicit Mover(DigestingWriter& self, DigestingWriter& that)
      : uses_buffer_(self.start() != nullptr) {
    // Buffer pointers are already moved so `SyncBuffer()` is called on `self`.
    // `dest_` is not moved yet so `dest_` is taken from `that`.
    if (uses_buffer_) {
      if (ABSL_PREDICT_FALSE(!self.SyncBuffer(*that.dest_))) {
        uses_buffer_ = false;
      }
    }
  }

  void Done(DigestingWriter& self) {
    if (uses_buffer_) self.MakeBuffer(*self.dest_);
  }

 private:
  bool uses_buffer_;
};

template <typename Digester, typename Dest>
inline DigestingWriter<Digester, Dest>::DigestingWriter(
    Initializer<Dest> dest, Initializer<Digester> digester)
    : digester_(std::move(digester)), dest_(std::move(dest)) {
  Initialize(dest_.get(), digester_.get());
}

template <typename Digester, typename Dest>
inline void DigestingWriter<Digester, Dest>::Reset(Closed) {
  DigestingWriterBase::Reset(kClosed);
  digester_.Reset();
  dest_.Reset();
}

template <typename Digester, typename Dest>
inline void DigestingWriter<Digester, Dest>::Reset(
    Initializer<Dest> dest, Initializer<Digester> digester) {
  DigestingWriterBase::Reset();
  digester_.Reset(std::move(digester));
  dest_.Reset(std::move(dest));
  Initialize(dest_.get(), digester_.get());
}

template <typename Digester, typename Dest>
void DigestingWriter<Digester, Dest>::Done() {
  DigestingWriterBase::Done();
  if (dest_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) {
      FailWithoutAnnotation(dest_->status());
    }
  }
  if (digester_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!digester_.get().Close())) FailFromDigester();
  }
}

template <typename Digester, typename Dest>
void DigestingWriter<Digester, Dest>::SetWriteSizeHintImpl(
    absl::optional<Position> write_size_hint) {
  if (dest_.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!SyncBuffer(*dest_))) return;
    dest_->SetWriteSizeHint(write_size_hint);
    MakeBuffer(*dest_);
  }
}

template <typename Digester, typename Dest>
bool DigestingWriter<Digester, Dest>::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  if (ABSL_PREDICT_FALSE(!SyncBuffer(*dest_))) return false;
  bool flush_ok = true;
  if (flush_type != FlushType::kFromObject || dest_.IsOwning()) {
    flush_ok = dest_->Flush(flush_type);
  }
  MakeBuffer(*dest_);
  return flush_ok;
}

namespace digesting_writer_internal {

ABSL_ATTRIBUTE_COLD absl::Status FailedStatus(DigesterBaseHandle digester);

template <typename T, typename Enable = void>
struct SupportedByDigesterHandle : std::false_type {};

template <typename T>
struct SupportedByDigesterHandle<
    T, absl::void_t<decltype(std::declval<DigesterBaseHandle&>().Write(
           std::declval<const T&>()))>> : std::true_type {};

template <size_t index, typename... Srcs,
          std::enable_if_t<(index == sizeof...(Srcs)), int> = 0>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool WriteTuple(
    ABSL_ATTRIBUTE_UNUSED const std::tuple<Srcs...>& srcs,
    ABSL_ATTRIBUTE_UNUSED DigesterBaseHandle digester) {
  return true;
}

template <size_t index, typename... Srcs,
          std::enable_if_t<(index < sizeof...(Srcs)), int> = 0>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool WriteTuple(
    const std::tuple<Srcs...>& srcs, DigesterBaseHandle digester) {
  return digester.Write(std::get<index>(srcs)) &&
         WriteTuple<index + 1>(srcs, digester);
}

template <
    typename DesiredDigestType, typename Digester, typename... Srcs,
    std::enable_if_t<
        absl::conjunction<SupportedByDigesterHandle<Srcs>...>::value, int> = 0>
inline DesiredDigestType DigestFromImpl(std::tuple<Srcs...> srcs,
                                        Digester&& digester) {
  Dependency<DigesterBaseHandle, Digester&&> digester_dep(
      std::forward<Digester>(digester));
  bool ok = WriteTuple<0>(srcs, digester_dep.get());
  if (digester_dep.IsOwning()) {
    if (ABSL_PREDICT_FALSE(!digester_dep.get().Close())) ok = false;
  }
  RIEGELI_CHECK(ok) << FailedStatus(digester_dep.get());
  return digester_dep.get().template Digest<DesiredDigestType>();
}

template <
    typename DesiredDigestType, typename Digester, typename... Srcs,
    std::enable_if_t<
        !absl::conjunction<SupportedByDigesterHandle<Srcs>...>::value, int> = 0>
inline DesiredDigestType DigestFromImpl(std::tuple<Srcs...> srcs,
                                        Digester&& digester) {
  DigestingWriter<Digester&&, NullWriter> writer(
      riegeli::Maker(), std::forward<Digester>(digester));
  writer.WriteTuple(srcs);
  RIEGELI_CHECK(writer.Close()) << writer.status();
  return writer.template Digest<DesiredDigestType>();
}

}  // namespace digesting_writer_internal

template <typename DesiredDigestType, typename... Args,
          std::enable_if_t<
              absl::conjunction<
                  IsValidDependency<DigesterBaseHandle,
                                    GetTypeFromEndT<1, Args&&...>>,
                  TupleElementsSatisfy<RemoveTypesFromEndT<1, Args&&...>,
                                       IsStringifiable>,
                  digest_converter_internal::HasDigestConverterOrNoConversion<
                      DigestOf<GetTypeFromEndT<1, Args&&...>>,
                      DesiredDigestType>>::value,
              int>>
digest_converter_internal::ResolveNoConversion<
    DigestOf<GetTypeFromEndT<1, Args&&...>>, DesiredDigestType>
DigestFrom(Args&&... args) {
  return digesting_writer_internal::DigestFromImpl<
      digest_converter_internal::ResolveNoConversion<
          DigestOf<GetTypeFromEndT<1, Args&&...>>, DesiredDigestType>>(
      RemoveFromEnd<1>(std::forward<Args>(args)...),
      GetFromEnd<1>(std::forward<Args>(args)...));
}

}  // namespace riegeli

#endif  // RIEGELI_DIGESTS_DIGESTING_WRITER_H_
