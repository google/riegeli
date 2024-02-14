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
#include "riegeli/base/object.h"
#include "riegeli/base/type_traits.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/null_writer.h"
#include "riegeli/bytes/writer.h"
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

  bool PrefersCopying() const override;
  bool SupportsReadMode() override;

 protected:
  using Writer::Writer;

  DigestingWriterBase(DigestingWriterBase&& that) noexcept;
  DigestingWriterBase& operator=(DigestingWriterBase&& that) noexcept;

  void Initialize(Writer* dest);

  void Done() override;
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;

  virtual void WriteToDigester(absl::string_view src) = 0;

  // Sets cursor of `dest` to cursor of `*this`, digesting what has been written
  // to the buffer (until `cursor()`).
  void SyncBuffer(Writer& dest);

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
// The `DigesterType` template parameter specifies the type of the object
// providing and possibly owning the digester. `DigesterType` must support
// `Dependency<DigesterBaseHandle, DigesterType>`, e.g.
// `DigesterHandle<uint32_t>` (not owned), `Crc32cDigester` (owned),
// `AnyDependency<DigesterHandle<uint32_t>>` (maybe owned).
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the original `Writer`. `Dest` must support
// `Dependency<Writer*, Dest>`, e.g. `Writer*` (not owned, default),
// `ChainWriter<>` (owned), `std::unique_ptr<Writer>` (owned),
// `AnyDependency<Writer*>` (maybe owned).
//
// By relying on CTAD the `DigesterType` template argument can be deduced as the
// value type of the `digester` constructor argument, and the `Dest` template
// argument can be deduced as the value type of the `dest` constructor argument.
// This requires C++17.
//
// The original `Writer` must not be accessed until the `DigestingWriter` is
// closed or no longer used, except that it is allowed to read the destination
// of the original `Writer` immediately after `Flush()`.
template <typename DigesterType, typename Dest = Writer*>
class DigestingWriter : public DigestingWriterBase {
 public:
  // The type of the digest.
  using DigestType = DigestOf<DigesterType>;

  // Creates a closed `DigestingWriter`.
  explicit DigestingWriter(Closed) noexcept : DigestingWriterBase(kClosed) {}

  // Will write to the original `Writer` provided by `dest`, using the digester
  // provided by `digester` or constructed from elements of `digester_args`.
  explicit DigestingWriter(const Dest& dest, const DigesterType& digester);
  explicit DigestingWriter(const Dest& dest, DigesterType&& digester);
  template <typename... DigesterArgs>
  explicit DigestingWriter(
      const Dest& dest,
      std::tuple<DigesterArgs...> digester_args = std::forward_as_tuple());
  explicit DigestingWriter(Dest&& dest, const DigesterType& digester);
  explicit DigestingWriter(Dest&& dest, DigesterType&& digester);
  template <typename... DigesterArgs>
  explicit DigestingWriter(
      Dest&& dest,
      std::tuple<DigesterArgs...> digester_args = std::forward_as_tuple());

  // Will write to the original `Writer` provided by a `Dest` constructed from
  // elements of `dest_args`. This avoids constructing a temporary `Dest` and
  // moving from it.
  template <typename... DestArgs>
  explicit DigestingWriter(std::tuple<DestArgs...> dest_args,
                           const DigesterType& digester);
  template <typename... DestArgs>
  explicit DigestingWriter(std::tuple<DestArgs...> dest_args,
                           DigesterType&& digester);
  template <typename... DestArgs, typename... DigesterArgs>
  explicit DigestingWriter(
      std::tuple<DestArgs...> dest_args,
      std::tuple<DigesterArgs...> digester_args = std::forward_as_tuple());

  DigestingWriter(DigestingWriter&& that) noexcept;
  DigestingWriter& operator=(DigestingWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `DigestingWriter`. This
  // avoids constructing a temporary `DigestingWriter` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(const Dest& dest,
                                          const DigesterType& digester);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(const Dest& dest,
                                          DigesterType&& digester);
  template <typename... DigesterArgs>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      const Dest& dest,
      std::tuple<DigesterArgs...> digester_args = std::forward_as_tuple());
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Dest&& dest,
                                          const DigesterType& digester);
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Dest&& dest, DigesterType&& digester);
  template <typename... DigesterArgs>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      Dest&& dest,
      std::tuple<DigesterArgs...> digester_args = std::forward_as_tuple());
  template <typename... DestArgs>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(std::tuple<DestArgs...> dest_args,
                                          const DigesterType& digester);
  template <typename... DestArgs>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(std::tuple<DestArgs...> dest_args,
                                          DigesterType&& digester);
  template <typename... DestArgs, typename... DigesterArgs>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(
      std::tuple<DestArgs...> dest_args,
      std::tuple<DigesterArgs...> digester_args = std::forward_as_tuple());

  // Digests buffered data if needed, and returns the digest.
  DigestType Digest();

  // Returns the object providing and possibly owning the digester. Unchanged by
  // `Close()`.
  DigesterType& digester() { return digester_.manager(); }
  const DigesterType& digester() const { return digester_.manager(); }
  DigesterBaseHandle GetDigester() const override { return digester_.get(); }

  // Returns the object providing and possibly owning the original `Writer`.
  // Unchanged by `Close()`.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  Writer* DestWriter() const override { return dest_.get(); }

 protected:
  void Done() override;
  void WriteToDigester(absl::string_view src) override {
    digester_.get().Write(src);
  }
  void SetWriteSizeHintImpl(absl::optional<Position> write_size_hint) override;
  bool FlushImpl(FlushType flush_type) override;

 private:
  // Moves `that.dest_` to `dest_`. Buffer pointers are already moved from
  // `dest_` to `*this`; adjust them to match `dest_`.
  void MoveDest(DigestingWriter&& that);

  // The object providing and possibly owning the digester.
  Dependency<DigesterBaseHandle, DigesterType> digester_;
  // The object providing and possibly owning the original `Writer`.
  Dependency<Writer*, Dest> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit DigestingWriter(Closed) -> DigestingWriter<void, DeleteCtad<Closed>>;
template <typename DigesterType, typename Dest>
explicit DigestingWriter(const Dest& dest, const DigesterType& digester)
    -> DigestingWriter<std::decay_t<DigesterType>, std::decay_t<Dest>>;
template <typename DigesterType, typename Dest>
explicit DigestingWriter(const Dest& dest, DigesterType&& digester)
    -> DigestingWriter<std::decay_t<DigesterType>, std::decay_t<Dest>>;
template <typename... DigesterArgs, typename Dest>
explicit DigestingWriter(const Dest& dest,
                         std::tuple<DigesterArgs...> digester_args)
    -> DigestingWriter<DeleteCtad<std::tuple<DigesterArgs...>>,
                       std::decay_t<Dest>>;
template <typename DigesterType, typename Dest>
explicit DigestingWriter(Dest&& dest, const DigesterType& digester)
    -> DigestingWriter<std::decay_t<DigesterType>, std::decay_t<Dest>>;
template <typename DigesterType, typename Dest>
explicit DigestingWriter(Dest&& dest, DigesterType&& digester)
    -> DigestingWriter<std::decay_t<DigesterType>, std::decay_t<Dest>>;
template <typename... DigesterArgs, typename Dest>
explicit DigestingWriter(Dest&& dest, std::tuple<DigesterArgs...> digester_args)
    -> DigestingWriter<DeleteCtad<std::tuple<DigesterArgs...>>,
                       std::decay_t<Dest>>;
template <typename DigesterType, typename... DestArgs>
explicit DigestingWriter(std::tuple<DestArgs...> dest_args,
                         const DigesterType& digester)
    -> DigestingWriter<std::decay_t<DigesterType>,
                       DeleteCtad<std::tuple<DestArgs...>>>;
template <typename DigesterType, typename... DestArgs>
explicit DigestingWriter(std::tuple<DestArgs...> dest_args,
                         DigesterType&& digester)
    -> DigestingWriter<std::decay_t<DigesterType>,
                       DeleteCtad<std::tuple<DestArgs...>>>;
template <typename... DigesterArgs, typename... DestArgs>
explicit DigestingWriter(std::tuple<DestArgs...> dest_args,
                         std::tuple<DigesterArgs...> digester_args)
    -> DigestingWriter<DeleteCtad<std::tuple<DigesterArgs...>>,
                       DeleteCtad<std::tuple<DestArgs...>>>;
#endif

// Returns the digest of the concatenation of stringifiable values.
//
// The last argument is the digester of some type `DigesterType`. The remaining
// arguments are the values.
//
// `DigesterType` specifies the type of the object providing and possibly owning
// the digester. `DigesterType` must support
// `Dependency<DigesterBaseHandle, DigesterType&&>` and must provide a member
// function `DigestType Digest()` for some `DigestType`, e.g.
// `DigesterHandle<uint32_t>` (not owned), `Crc32cDigester` (owned),
// `AnyDependency<DigesterHandle<uint32_t>>` (maybe owned).
template <
    typename... Args,
    std::enable_if_t<absl::conjunction<
                         IsValidDependency<DigesterBaseHandle,
                                           GetTypeFromEndT<1, Args&&...>>,
                         TupleElementsSatisfy<RemoveTypesFromEndT<1, Args&&...>,
                                              IsStringifiable>>::value,
                     int> = 0>
DigestOf<GetTypeFromEndT<1, Args&&...>> DigestFrom(Args&&... args);

// Implementation details follow.

inline DigestingWriterBase::DigestingWriterBase(
    DigestingWriterBase&& that) noexcept
    : Writer(static_cast<Writer&&>(that)) {}

inline DigestingWriterBase& DigestingWriterBase::operator=(
    DigestingWriterBase&& that) noexcept {
  Writer::operator=(static_cast<Writer&&>(that));
  return *this;
}

inline void DigestingWriterBase::Initialize(Writer* dest) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of DigestingWriter: null Writer pointer";
  MakeBuffer(*dest);
}

inline void DigestingWriterBase::SyncBuffer(Writer& dest) {
  RIEGELI_ASSERT(start() == dest.cursor())
      << "Failed invariant of DigestingWriterBase: "
         "cursor of the original Writer changed unexpectedly";
  if (start_to_cursor() > 0) {
    WriteToDigester(absl::string_view(start(), start_to_cursor()));
    dest.set_cursor(cursor());
  }
}

inline void DigestingWriterBase::MakeBuffer(Writer& dest) {
  set_buffer(dest.cursor(), dest.available());
  set_start_pos(dest.pos());
  if (ABSL_PREDICT_FALSE(!dest.ok())) FailWithoutAnnotation(dest.status());
}

template <typename DigesterType, typename Dest>
inline DigestingWriter<DigesterType, Dest>::DigestingWriter(
    const Dest& dest, const DigesterType& digester)
    : digester_(digester), dest_(dest) {
  Initialize(dest_.get());
}

template <typename DigesterType, typename Dest>
inline DigestingWriter<DigesterType, Dest>::DigestingWriter(
    const Dest& dest, DigesterType&& digester)
    : digester_(std::move(digester)), dest_(dest) {
  Initialize(dest_.get());
}

template <typename DigesterType, typename Dest>
template <typename... DigesterArgs>
inline DigestingWriter<DigesterType, Dest>::DigestingWriter(
    const Dest& dest, std::tuple<DigesterArgs...> digester_args)
    : digester_(std::move(digester_args)), dest_(dest) {
  Initialize(dest_.get());
}

template <typename DigesterType, typename Dest>
inline DigestingWriter<DigesterType, Dest>::DigestingWriter(
    Dest&& dest, const DigesterType& digester)
    : digester_(digester), dest_(std::move(dest)) {
  Initialize(dest_.get());
}

template <typename DigesterType, typename Dest>
inline DigestingWriter<DigesterType, Dest>::DigestingWriter(
    Dest&& dest, DigesterType&& digester)
    : digester_(std::move(digester)), dest_(std::move(dest)) {
  Initialize(dest_.get());
}

template <typename DigesterType, typename Dest>
template <typename... DigesterArgs>
inline DigestingWriter<DigesterType, Dest>::DigestingWriter(
    Dest&& dest, std::tuple<DigesterArgs...> digester_args)
    : digester_(std::move(digester_args)), dest_(std::move(dest)) {
  Initialize(dest_.get());
}

template <typename DigesterType, typename Dest>
template <typename... DestArgs>
inline DigestingWriter<DigesterType, Dest>::DigestingWriter(
    std::tuple<DestArgs...> dest_args, const DigesterType& digester)
    : digester_(digester), dest_(std::move(dest_args)) {
  Initialize(dest_.get());
}

template <typename DigesterType, typename Dest>
template <typename... DestArgs>
inline DigestingWriter<DigesterType, Dest>::DigestingWriter(
    std::tuple<DestArgs...> dest_args, DigesterType&& digester)
    : digester_(std::move(digester)), dest_(std::move(dest_args)) {
  Initialize(dest_.get());
}

template <typename DigesterType, typename Dest>
template <typename... DestArgs, typename... DigesterArgs>
inline DigestingWriter<DigesterType, Dest>::DigestingWriter(
    std::tuple<DestArgs...> dest_args,
    std::tuple<DigesterArgs...> digester_args)
    : digester_(std::move(digester_args)), dest_(std::move(dest_args)) {
  Initialize(dest_.get());
}

template <typename DigesterType, typename Dest>
inline DigestingWriter<DigesterType, Dest>::DigestingWriter(
    DigestingWriter&& that) noexcept
    : DigestingWriterBase(static_cast<DigestingWriterBase&&>(that)),
      digester_(std::move(that.digester_)) {
  MoveDest(std::move(that));
}

template <typename DigesterType, typename Dest>
inline DigestingWriter<DigesterType, Dest>&
DigestingWriter<DigesterType, Dest>::operator=(
    DigestingWriter&& that) noexcept {
  DigestingWriterBase::operator=(static_cast<DigestingWriterBase&&>(that));
  digester_ = std::move(that.digester_);
  MoveDest(std::move(that));
  return *this;
}

template <typename DigesterType, typename Dest>
inline void DigestingWriter<DigesterType, Dest>::Reset(Closed) {
  DigestingWriterBase::Reset(kClosed);
  digester_.Reset();
  dest_.Reset();
}

template <typename DigesterType, typename Dest>
inline void DigestingWriter<DigesterType, Dest>::Reset(
    const Dest& dest, const DigesterType& digester) {
  DigestingWriterBase::Reset();
  digester_.Reset(digester);
  dest_.Reset(dest);
  Initialize(dest_.get());
}

template <typename DigesterType, typename Dest>
inline void DigestingWriter<DigesterType, Dest>::Reset(
    const Dest& dest, DigesterType&& digester) {
  DigestingWriterBase::Reset();
  digester_.Reset(std::move(digester));
  dest_.Reset(dest);
  Initialize(dest_.get());
}

template <typename DigesterType, typename Dest>
template <typename... DigesterArgs>
inline void DigestingWriter<DigesterType, Dest>::Reset(
    const Dest& dest, std::tuple<DigesterArgs...> digester_args) {
  DigestingWriterBase::Reset();
  digester_.Reset(std::move(digester_args));
  dest_.Reset(dest);
  Initialize(dest_.get());
}

template <typename DigesterType, typename Dest>
inline void DigestingWriter<DigesterType, Dest>::Reset(
    Dest&& dest, const DigesterType& digester) {
  DigestingWriterBase::Reset();
  digester_.Reset(digester);
  dest_.Reset(std::move(dest));
  Initialize(dest_.get());
}

template <typename DigesterType, typename Dest>
inline void DigestingWriter<DigesterType, Dest>::Reset(
    Dest&& dest, DigesterType&& digester) {
  DigestingWriterBase::Reset();
  digester_.Reset(std::move(digester));
  dest_.Reset(std::move(dest));
  Initialize(dest_.get());
}

template <typename DigesterType, typename Dest>
template <typename... DigesterArgs>
inline void DigestingWriter<DigesterType, Dest>::Reset(
    Dest&& dest, std::tuple<DigesterArgs...> digester_args) {
  DigestingWriterBase::Reset();
  digester_.Reset(std::move(digester_args));
  dest_.Reset(std::move(dest));
  Initialize(dest_.get());
}

template <typename DigesterType, typename Dest>
template <typename... DestArgs>
inline void DigestingWriter<DigesterType, Dest>::Reset(
    std::tuple<DestArgs...> dest_args, const DigesterType& digester) {
  DigestingWriterBase::Reset();
  digester_.Reset(digester);
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get());
}

template <typename DigesterType, typename Dest>
template <typename... DestArgs>
inline void DigestingWriter<DigesterType, Dest>::Reset(
    std::tuple<DestArgs...> dest_args, DigesterType&& digester) {
  DigestingWriterBase::Reset();
  digester_.Reset(std::move(digester));
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get());
}

template <typename DigesterType, typename Dest>
template <typename... DestArgs, typename... DigesterArgs>
inline void DigestingWriter<DigesterType, Dest>::Reset(
    std::tuple<DestArgs...> dest_args,
    std::tuple<DigesterArgs...> digester_args) {
  DigestingWriterBase::Reset();
  digester_.Reset(std::move(digester_args));
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get());
}

template <typename DigesterType, typename Dest>
inline void DigestingWriter<DigesterType, Dest>::MoveDest(
    DigestingWriter&& that) {
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

template <typename DigesterType, typename Dest>
void DigestingWriter<DigesterType, Dest>::Done() {
  DigestingWriterBase::Done();
  if (dest_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) {
      FailWithoutAnnotation(dest_->status());
    }
  }
  if (digester_.is_owning()) digester_.get().Close();
}

template <typename DigesterType, typename Dest>
inline typename DigestingWriter<DigesterType, Dest>::DigestType
DigestingWriter<DigesterType, Dest>::Digest() {
  if (start_to_cursor() > 0) {
    digester_.get().Write(absl::string_view(start(), start_to_cursor()));
    set_start_pos(pos());
    set_buffer(cursor(), available());
    dest_->set_cursor(cursor());
  }
  return digester_.get().Digest();
}

template <typename DigesterType, typename Dest>
void DigestingWriter<DigesterType, Dest>::SetWriteSizeHintImpl(
    absl::optional<Position> write_size_hint) {
  if (dest_.is_owning()) {
    SyncBuffer(*dest_);
    dest_->SetWriteSizeHint(write_size_hint);
    MakeBuffer(*dest_);
  }
}

template <typename DigesterType, typename Dest>
bool DigestingWriter<DigesterType, Dest>::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  SyncBuffer(*dest_);
  bool flush_ok = true;
  if (flush_type != FlushType::kFromObject || dest_.is_owning()) {
    flush_ok = dest_->Flush(flush_type);
  }
  MakeBuffer(*dest_);
  return flush_ok;
}

namespace digesting_writer_internal {

template <typename T, typename Enable = void>
struct SupportedByDigesterHandle : std::false_type {};

template <typename T>
struct SupportedByDigesterHandle<
    T, absl::void_t<decltype(std::declval<DigesterBaseHandle&>().Write(
           std::declval<const T&>()))>> : std::true_type {};

template <size_t index, typename... Srcs,
          std::enable_if_t<(index == sizeof...(Srcs)), int> = 0>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline void WriteTuple(
    ABSL_ATTRIBUTE_UNUSED const std::tuple<Srcs...>& srcs,
    ABSL_ATTRIBUTE_UNUSED DigesterBaseHandle digester) {}

template <size_t index, typename... Srcs,
          std::enable_if_t<(index < sizeof...(Srcs)), int> = 0>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline void WriteTuple(
    const std::tuple<Srcs...>& srcs, DigesterBaseHandle digester) {
  digester.Write(std::get<index>(srcs));
  WriteTuple<index + 1>(srcs, digester);
}

template <
    typename DigesterType, typename... Srcs,
    std::enable_if_t<
        absl::conjunction<SupportedByDigesterHandle<Srcs>...>::value, int> = 0>
inline DigestOf<DigesterType&&> DigestFromImpl(std::tuple<Srcs...> srcs,
                                               DigesterType&& digester) {
  Dependency<DigesterBaseHandle, DigesterType&&> digester_dep(
      std::forward<DigesterType>(digester));
  WriteTuple<0>(srcs, digester_dep.get());
  if (digester_dep.is_owning()) digester_dep.get().Close();
  return digester_dep.get().Digest();
}

template <
    typename DigesterType, typename... Srcs,
    std::enable_if_t<
        !absl::conjunction<SupportedByDigesterHandle<Srcs>...>::value, int> = 0>
inline DigestOf<DigesterType&&> DigestFromImpl(std::tuple<Srcs...> srcs,
                                               DigesterType&& digester) {
  DigestingWriter<DigesterType&&, NullWriter> writer(
      std::forward_as_tuple(), std::forward<DigesterType>(digester));
  writer.WriteTuple(srcs);
  RIEGELI_CHECK(writer.Close())
      << "DigestingWriter<DigesterType, NullWriter> can fail only "
         "if the size overflows: "
      << writer.status();
  return writer.Digest();
}

}  // namespace digesting_writer_internal

template <
    typename... Args,
    std::enable_if_t<absl::conjunction<
                         IsValidDependency<DigesterBaseHandle,
                                           GetTypeFromEndT<1, Args&&...>>,
                         TupleElementsSatisfy<RemoveTypesFromEndT<1, Args&&...>,
                                              IsStringifiable>>::value,
                     int>>
DigestOf<GetTypeFromEndT<1, Args&&...>> DigestFrom(Args&&... args) {
  return digesting_writer_internal::DigestFromImpl(
      RemoveFromEnd<1>(std::forward<Args>(args)...),
      GetFromEnd<1>(std::forward<Args>(args)...));
}

}  // namespace riegeli

#endif  // RIEGELI_DIGESTS_DIGESTING_WRITER_H_
