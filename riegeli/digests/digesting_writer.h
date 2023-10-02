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
#include "absl/numeric/int128.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/reset.h"
#include "riegeli/base/type_traits.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/null_writer.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/digests/digesting_internal.h"

namespace riegeli {

class Reader;

// Template parameter independent part of `DigestingWriter`.
class DigestingWriterBase : public Writer {
 public:
  // Returns the original `Writer`. Unchanged by `Close()`.
  virtual Writer* DestWriter() = 0;
  virtual const Writer* DestWriter() const = 0;

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

  // Sets cursor of `dest` to cursor of `*this`, digesting what has been written
  // to the buffer (until `cursor()`).
  void SyncBuffer(Writer& dest);

  // Sets buffer pointers of `*this` to buffer pointers of `dest`, adjusting
  // `start()` to hide data already digested. Fails `*this` if `dest` failed.
  void MakeBuffer(Writer& dest);

  virtual void DigesterWrite(absl::string_view src) = 0;
  void DigesterWrite(const Chain& src);
  void DigesterWrite(const absl::Cord& src);
  virtual void DigesterWriteZeros(Position length) = 0;

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
// The `Digester` template parameter specifies how data are being digested.
// `DigestingWriter` forwards basic operations to `Digester`: constructor with
// forwarded parameters after `dest`, move constructor, move assignment,
// destructor, and optionally `Reset()`. Apart from that, `Digester` should
// support:
//
// ```
//   // Called with consecutive fragments of data.
//   void Write(absl::string_view src);
//
//   // Can be called instead of `Write()` when data consists of zeros.
//   //
//   // This method is optional. If that is not defined, `Write()` is used
//   // instead.
//   void WriteZeros(riegeli::Position length);
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
// the `DigestingWriter` is destroyed.
//
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the original `Writer`. `Dest` must support
// `Dependency<Writer*, Dest>`, e.g. `Writer*` (not owned, default),
// `ChainWriter<>` (owned), `std::unique_ptr<Writer>` (owned),
// `AnyDependency<Writer*>` (maybe owned).
//
// By relying on CTAD the `Digester` template argument can be deduced as the
// value type of the `digester_args` constructor argument (there must be only
// one `digester_args` for CTAD), and the `Dest` template argument can be
// deduced as the value type of the `dest` constructor argument. This requires
// C++17.
//
// The original `Writer` must not be accessed until the `DigestingWriter` is
// closed or no longer used, except that it is allowed to read the destination
// of the original `Writer` immediately after `Flush()`.
template <typename Digester, typename Dest = Writer*>
class DigestingWriter : public DigestingWriterBase {
 public:
  // The type of the digest.
  using DigestType = digesting_internal::DigestType<Digester>;

  // Creates a closed `DigestingWriter`.
  explicit DigestingWriter(Closed) noexcept : DigestingWriterBase(kClosed) {}

  // Will write to the original `Writer` provided by `dest`. Constructs a
  // `Digester` from `digester_args`.
  template <typename... DigesterArgs>
  explicit DigestingWriter(const Dest& dest, DigesterArgs&&... digester_args);
  template <typename... DigesterArgs>
  explicit DigestingWriter(Dest&& dest, DigesterArgs&&... digester_args);

  // Will write to the original `Writer` provided by a `Dest` constructed from
  // elements of `dest_args`. This avoids constructing a temporary `Dest` and
  // moving from it.
  template <typename... DestArgs, typename... DigesterArgs>
  explicit DigestingWriter(std::tuple<DestArgs...> dest_args,
                           DigesterArgs&&... digester_args);

  DigestingWriter(DigestingWriter&& that) noexcept;
  DigestingWriter& operator=(DigestingWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `DigestingWriter`. This
  // avoids constructing a temporary `DigestingWriter` and moving from it.
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Closed);
  template <typename... DigesterArgs>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(const Dest& dest,
                                          DigesterArgs&&... digester_args);
  template <typename... DigesterArgs>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(Dest&& dest,
                                          DigesterArgs&&... digester_args);
  template <typename... DestArgs, typename... DigesterArgs>
  ABSL_ATTRIBUTE_REINITIALIZES void Reset(std::tuple<DestArgs...> dest_args,
                                          DigesterArgs&&... digester_args);

  // Digests buffered data if needed, and returns the digest.
  DigestType Digest();

  // Returns the object providing and possibly owning the original `Writer`.
  // Unchanged by `Close()`.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  Writer* DestWriter() override { return dest_.get(); }
  const Writer* DestWriter() const override { return dest_.get(); }

 protected:
  void Done() override;

  using DigestingWriterBase::DigesterWrite;
  void DigesterWrite(absl::string_view src) override;
  void DigesterWriteZeros(Position length) override;

  void SetWriteSizeHintImpl(absl::optional<Position> write_size_hint) override;
  bool FlushImpl(FlushType flush_type) override;

 private:
  // Moves `that.dest_` to `dest_`. Buffer pointers are already moved from
  // `dest_` to `*this`; adjust them to match `dest_`.
  void MoveDest(DigestingWriter&& that);

  Digester digester_;
  // The object providing and possibly owning the original `Writer`.
  Dependency<Writer*, Dest> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit DigestingWriter(Closed) -> DigestingWriter<void, DeleteCtad<Closed>>;
template <typename Digester, typename Dest>
explicit DigestingWriter(const Dest& dest, Digester&& digester)
    -> DigestingWriter<std::decay_t<Digester>, std::decay_t<Dest>>;
template <typename Digester, typename Dest>
explicit DigestingWriter(Dest&& dest, Digester&& digester)
    -> DigestingWriter<std::decay_t<Digester>, std::decay_t<Dest>>;
template <typename Digester, typename... DestArgs>
explicit DigestingWriter(std::tuple<DestArgs...> dest_args, Digester&& digester)
    -> DigestingWriter<void, DeleteCtad<std::tuple<DestArgs...>>>;
#endif

// Returns the digest of `src`.
//
// The `Digester` template argument can be deduced as the value type of the
// `digester` argument (there must be only one `digester_args` for this).
template <typename Digester, typename Src,
          std::enable_if_t<IsStringifiable<Src>::value, int> = 0>
digesting_internal::DigestType<Digester> DigestFrom(Src&& src,
                                                    const Digester& digester);
template <typename Digester, typename Src,
          std::enable_if_t<IsStringifiable<Src>::value, int> = 0>
digesting_internal::DigestType<Digester> DigestFrom(Src&& src,
                                                    Digester&& digester);
template <typename Digester, typename Src, typename... DigesterArgs,
          std::enable_if_t<
              absl::conjunction<IsStringifiable<Src>,
                                std::integral_constant<
                                    bool, sizeof...(DigesterArgs) != 1>>::value,
              int> = 0>
digesting_internal::DigestType<Digester> DigestFrom(
    Src&& src, DigesterArgs&&... digester_args);

// Returns the digest of elements of `srcs`.
//
// The `Digester` template argument can be deduced as the value type of the
// `digester` argument (there must be only one `digester_args` for this).
template <typename Digester, typename... Srcs,
          std::enable_if_t<absl::conjunction<IsStringifiable<Srcs>...>::value,
                           int> = 0>
digesting_internal::DigestType<Digester> DigestFromTuple(
    const std::tuple<Srcs...>& srcs, const Digester& digester);
template <typename Digester, typename... Srcs,
          std::enable_if_t<absl::conjunction<IsStringifiable<Srcs>...>::value,
                           int> = 0>
digesting_internal::DigestType<Digester> DigestFromTuple(
    const std::tuple<Srcs...>& srcs, Digester&& digester);
template <typename Digester, typename... Srcs, typename... DigesterArgs,
          std::enable_if_t<
              absl::conjunction<IsStringifiable<Srcs>...,
                                std::integral_constant<
                                    bool, sizeof...(DigesterArgs) != 1>>::value,
              int> = 0>
digesting_internal::DigestType<Digester> DigestFromTuple(
    const std::tuple<Srcs...>& srcs, DigesterArgs&&... digester_args);

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
    DigesterWrite(absl::string_view(start(), start_to_cursor()));
    dest.set_cursor(cursor());
  }
}

inline void DigestingWriterBase::MakeBuffer(Writer& dest) {
  set_buffer(dest.cursor(), dest.available());
  set_start_pos(dest.pos());
  if (ABSL_PREDICT_FALSE(!dest.ok())) FailWithoutAnnotation(dest.status());
}

template <typename Digester, typename Dest>
template <typename... DigesterArgs>
inline DigestingWriter<Digester, Dest>::DigestingWriter(
    const Dest& dest, DigesterArgs&&... digester_args)
    : digester_(std::forward<DigesterArgs>(digester_args)...), dest_(dest) {
  Initialize(dest_.get());
}

template <typename Digester, typename Dest>
template <typename... DigesterArgs>
inline DigestingWriter<Digester, Dest>::DigestingWriter(
    Dest&& dest, DigesterArgs&&... digester_args)
    : digester_(std::forward<DigesterArgs>(digester_args)...),
      dest_(std::move(dest)) {
  Initialize(dest_.get());
}

template <typename Digester, typename Dest>
template <typename... DestArgs, typename... DigesterArgs>
inline DigestingWriter<Digester, Dest>::DigestingWriter(
    std::tuple<DestArgs...> dest_args, DigesterArgs&&... digester_args)
    : digester_(std::forward<DigesterArgs>(digester_args)...),
      dest_(std::move(dest_args)) {
  Initialize(dest_.get());
}

template <typename Digester, typename Dest>
inline DigestingWriter<Digester, Dest>::DigestingWriter(
    DigestingWriter&& that) noexcept
    : DigestingWriterBase(static_cast<DigestingWriterBase&&>(that)),
      digester_(std::move(that.digester_)) {
  MoveDest(std::move(that));
}

template <typename Digester, typename Dest>
inline DigestingWriter<Digester, Dest>&
DigestingWriter<Digester, Dest>::operator=(DigestingWriter&& that) noexcept {
  DigestingWriterBase::operator=(static_cast<DigestingWriterBase&&>(that));
  digester_ = std::move(that.digester_);
  MoveDest(std::move(that));
  return *this;
}

template <typename Digester, typename Dest>
inline void DigestingWriter<Digester, Dest>::Reset(Closed) {
  DigestingWriterBase::Reset(kClosed);
  riegeli::Reset(digester_);
  dest_.Reset();
}

template <typename Digester, typename Dest>
template <typename... DigesterArgs>
inline void DigestingWriter<Digester, Dest>::Reset(
    const Dest& dest, DigesterArgs&&... digester_args) {
  DigestingWriterBase::Reset();
  riegeli::Reset(digester_, std::forward<DigesterArgs>(digester_args)...);
  dest_.Reset(dest);
  Initialize(dest_.get());
}

template <typename Digester, typename Dest>
template <typename... DigesterArgs>
inline void DigestingWriter<Digester, Dest>::Reset(
    Dest&& dest, DigesterArgs&&... digester_args) {
  DigestingWriterBase::Reset();
  riegeli::Reset(digester_, std::forward<DigesterArgs>(digester_args)...);
  dest_.Reset(std::move(dest));
  Initialize(dest_.get());
}

template <typename Digester, typename Dest>
template <typename... DestArgs, typename... DigesterArgs>
inline void DigestingWriter<Digester, Dest>::Reset(
    std::tuple<DestArgs...> dest_args, DigesterArgs&&... digester_args) {
  DigestingWriterBase::Reset();
  riegeli::Reset(digester_, std::forward<DigesterArgs>(digester_args)...);
  dest_.Reset(std::move(dest_args));
  Initialize(dest_.get());
}

template <typename Digester, typename Dest>
inline void DigestingWriter<Digester, Dest>::MoveDest(DigestingWriter&& that) {
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

template <typename Digester, typename Dest>
void DigestingWriter<Digester, Dest>::Done() {
  DigestingWriterBase::Done();
  if (dest_.is_owning()) {
    if (ABSL_PREDICT_FALSE(!dest_->Close())) {
      FailWithoutAnnotation(dest_->status());
    }
  }
  digesting_internal::Close(digester_);
}

template <typename Digester, typename Dest>
void DigestingWriter<Digester, Dest>::DigesterWrite(absl::string_view src) {
  digesting_internal::Write(digester_, src);
}

template <typename Digester, typename Dest>
void DigestingWriter<Digester, Dest>::DigesterWriteZeros(Position length) {
  digesting_internal::WriteZeros(digester_, length);
}

template <typename Digester, typename Dest>
inline typename DigestingWriter<Digester, Dest>::DigestType
DigestingWriter<Digester, Dest>::Digest() {
  if (start_to_cursor() > 0) {
    DigesterWrite(absl::string_view(start(), start_to_cursor()));
    set_start_pos(pos());
    set_buffer(cursor(), available());
    dest_->set_cursor(cursor());
  }
  return digesting_internal::Digest(digester_);
}

template <typename Digester, typename Dest>
void DigestingWriter<Digester, Dest>::SetWriteSizeHintImpl(
    absl::optional<Position> write_size_hint) {
  if (dest_.is_owning()) dest_->SetWriteSizeHint(write_size_hint);
}

template <typename Digester, typename Dest>
bool DigestingWriter<Digester, Dest>::FlushImpl(FlushType flush_type) {
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

// `digesting_writer_internal::IsStringLike()` of a stringifiable value is
// declared as returning `int` for types having an optimized `DigestFromImpl()`
// implementation, and `void` otherwise.
//
// It has the same overloads as `Writer::Write()`, assuming that the parameter
// is passed by const reference.
void IsStringLike(char src);
#if __cpp_char8_t
void IsStringLike(char8_t src);
#endif
int IsStringLike(absl::string_view src);
int IsStringLike(const char* src);
int IsStringLike(const Chain& src);
int IsStringLike(const absl::Cord& src);
void IsStringLike(signed char);
void IsStringLike(unsigned char);
void IsStringLike(short src);
void IsStringLike(unsigned short src);
void IsStringLike(int src);
void IsStringLike(unsigned src);
void IsStringLike(long src);
void IsStringLike(unsigned long src);
void IsStringLike(long long src);
void IsStringLike(unsigned long long src);
void IsStringLike(absl::int128 src);
void IsStringLike(absl::uint128 src);
void IsStringLike(float);
void IsStringLike(double);
void IsStringLike(long double);
template <typename Src, std::enable_if_t<HasAbslStringify<Src>::value, int> = 0>
void IsStringLike(const Src&);
void IsStringLike(bool) = delete;
void IsStringLike(wchar_t) = delete;
void IsStringLike(char16_t) = delete;
void IsStringLike(char32_t) = delete;

template <typename Digester, typename... DigesterArgs>
inline digesting_internal::DigestType<Digester> DigestFromImpl(
    absl::string_view src, DigesterArgs&&... digester_args) {
  Digester digester(std::forward<DigesterArgs>(digester_args)...);
  digesting_internal::Write(digester, src);
  digesting_internal::Close(digester);
  return digesting_internal::Digest(digester);
}

template <typename Digester, typename... DigesterArgs>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline digesting_internal::DigestType<Digester>
DigestFromImpl(const char* src, DigesterArgs&&... digester_args) {
  return DigestFromImpl(absl::string_view(src),
                        std::forward<DigesterArgs>(digester_args)...);
}

template <typename Digester, typename... DigesterArgs>
inline digesting_internal::DigestType<Digester> DigestFromImpl(
    const Chain& src, DigesterArgs&&... digester_args) {
  Digester digester(std::forward<DigesterArgs>(digester_args)...);
  for (const absl::string_view fragment : src.blocks()) {
    digesting_internal::Write(digester, fragment);
  }
  digesting_internal::Close(digester);
  return digesting_internal::Digest(digester);
}

template <typename Digester, typename... DigesterArgs>
inline digesting_internal::DigestType<Digester> DigestFromImpl(
    const absl::Cord& src, DigesterArgs&&... digester_args) {
  Digester digester(std::forward<DigesterArgs>(digester_args)...);
  {
    const absl::optional<absl::string_view> flat = src.TryFlat();
    if (flat != absl::nullopt) {
      digesting_internal::Write(digester, *flat);
    } else {
      for (const absl::string_view fragment : src.Chunks()) {
        digesting_internal::Write(digester, fragment);
      }
    }
  }
  digesting_internal::Close(digester);
  return digesting_internal::Digest(digester);
}

template <typename Digester, typename Src, typename... DigesterArgs,
          std::enable_if_t<
              std::is_same<decltype(digesting_writer_internal::IsStringLike(
                               std::declval<const Src&>())),
                           void>::value,
              int> = 0>
inline digesting_internal::DigestType<Digester> DigestFromImpl(
    Src&& src, DigesterArgs&&... digester_args) {
  DigestingWriter<Digester, NullWriter> writer(
      std::forward_as_tuple(), std::forward<DigesterArgs>(digester_args)...);
  writer.Write(std::forward<Src>(src));
  RIEGELI_CHECK(writer.Close())
      << "DigestingWriter<Digester, NullWriter> can fail only "
         "if the size overflows: "
      << writer.status();
  return writer.Digest();
}

template <typename Digester, typename... Srcs, typename... DigesterArgs>
inline digesting_internal::DigestType<Digester> DigestFromTupleImpl(
    const std::tuple<Srcs...>& srcs, DigesterArgs&&... digester_args) {
  DigestingWriter<Digester, NullWriter> writer(
      std::forward_as_tuple(), std::forward<DigesterArgs>(digester_args)...);
  writer.WriteTuple(srcs);
  RIEGELI_CHECK(writer.Close())
      << "DigestingWriter<Digester, NullWriter> can fail only "
         "if the size overflows: "
      << writer.status();
  return writer.Digest();
}

}  // namespace digesting_writer_internal

template <typename Digester, typename Src,
          std::enable_if_t<IsStringifiable<Src>::value, int>>
inline digesting_internal::DigestType<Digester> DigestFrom(
    Src&& src, const Digester& digester) {
  return digesting_writer_internal::DigestFromImpl<std::decay_t<Digester>>(
      std::forward<Src>(src), digester);
}

template <typename Digester, typename Src,
          std::enable_if_t<IsStringifiable<Src>::value, int>>
inline digesting_internal::DigestType<Digester> DigestFrom(
    Src&& src, Digester&& digester) {
  return digesting_writer_internal::DigestFromImpl<std::decay_t<Digester>>(
      std::forward<Src>(src), std::forward<Digester>(digester));
}

template <typename Digester, typename Src, typename... DigesterArgs,
          std::enable_if_t<
              absl::conjunction<IsStringifiable<Src>,
                                std::integral_constant<
                                    bool, sizeof...(DigesterArgs) != 1>>::value,
              int>>
inline digesting_internal::DigestType<Digester> DigestFrom(
    Src&& src, DigesterArgs&&... digester_args) {
  return digesting_writer_internal::DigestFromImpl<Digester>(
      std::forward<Src>(src), std::forward<DigesterArgs>(digester_args)...);
}

template <
    typename Digester, typename... Srcs,
    std::enable_if_t<absl::conjunction<IsStringifiable<Srcs>...>::value, int>>
inline digesting_internal::DigestType<Digester> DigestFromTuple(
    const std::tuple<Srcs...>& srcs, const Digester& digester) {
  return digesting_writer_internal::DigestFromTupleImpl<std::decay_t<Digester>>(
      srcs, digester);
}

template <
    typename Digester, typename... Srcs,
    std::enable_if_t<absl::conjunction<IsStringifiable<Srcs>...>::value, int>>
inline digesting_internal::DigestType<Digester> DigestFromTuple(
    const std::tuple<Srcs...>& srcs, Digester&& digester) {
  return digesting_writer_internal::DigestFromTupleImpl<std::decay_t<Digester>>(
      srcs, std::forward<Digester>(digester));
}

template <typename Digester, typename... Srcs, typename... DigesterArgs,
          std::enable_if_t<
              absl::conjunction<IsStringifiable<Srcs>...,
                                std::integral_constant<
                                    bool, sizeof...(DigesterArgs) != 1>>::value,
              int>>
inline digesting_internal::DigestType<Digester> DigestFromTuple(
    const std::tuple<Srcs...>& srcs, DigesterArgs&&... digester_args) {
  return digesting_writer_internal::DigestFromTupleImpl<Digester>(
      srcs, std::forward<DigesterArgs>(digester_args)...);
}

}  // namespace riegeli

#endif  // RIEGELI_DIGESTS_DIGESTING_WRITER_H_
