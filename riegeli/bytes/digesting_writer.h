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

#ifndef RIEGELI_BYTES_DIGESTING_WRITER_H_
#define RIEGELI_BYTES_DIGESTING_WRITER_H_

#include <stddef.h>

#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/reset.h"
#include "riegeli/bytes/digesting_common.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Template parameter independent part of `DigestingWriter`.
class DigestingWriterBase : public Writer {
 public:
  // Returns the original `Writer`. Unchanged by `Close()`.
  virtual Writer* dest_writer() = 0;
  virtual const Writer* dest_writer() const = 0;

  bool PrefersCopying() const override;
  bool SupportsSize() override;

 protected:
  using Writer::Writer;

  DigestingWriterBase(DigestingWriterBase&& that) noexcept;
  DigestingWriterBase& operator=(DigestingWriterBase&& that) noexcept;

  void Initialize(Writer* dest);

  void Done() override;
  bool PushSlow(size_t min_length, size_t recommended_length) override;
  using Writer::WriteSlow;
  bool WriteSlow(absl::string_view src) override;
  bool WriteSlow(const Chain& src) override;
  bool WriteSlow(Chain&& src) override;
  bool WriteSlow(const absl::Cord& src) override;
  bool WriteSlow(absl::Cord&& src) override;
  bool WriteZerosSlow(Position length) override;
  absl::optional<Position> SizeImpl();

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

 private:
  // This template is defined and used only in digesting_writer.cc.
  template <typename Src>
  bool WriteInternal(Src&& src);

  // Invariants if `healthy()`:
  //   `start() == dest_writer()->cursor()`
  //   `limit() == dest_writer()->limit()`
  //   `start_pos() == dest_writer()->pos()`
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
// The `Dest` template parameter specifies the type of the object providing and
// possibly owning the original `Writer`. `Dest` must support
// `Dependency<Writer*, Dest>`, e.g. `Writer*` (not owned, default),
// `std::unique_ptr<Writer>` (owned), `ChainWriter<>` (owned).
//
// By relying on CTAD the template argument can be deduced as the value type of
// the first constructor argument. This requires C++17.
//
// The original `Writer` must not be accessed until the `DigestingWriter` is
// closed or no longer used, except that it is allowed to read the destination
// of the original `Writer` immediately after `Flush()`.
template <typename Digester, typename Dest = Writer*>
class DigestingWriter : public DigestingWriterBase {
 public:
  // The type of the digest.
  using DigestType = internal::DigestType<Digester>;

  // Creates a closed `DigestingWriter`.
  explicit DigestingWriter(Closed) noexcept : DigestingWriterBase(kClosed) {}

  ABSL_DEPRECATED("Use kClosed constructor instead")
  DigestingWriter() noexcept : DigestingWriter(kClosed) {}

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
  void Reset(Closed);
  template <typename... DigesterArgs>
  void Reset(const Dest& dest, DigesterArgs&&... digester_args);
  template <typename... DigesterArgs>
  void Reset(Dest&& dest, DigesterArgs&&... digester_args);
  template <typename... DestArgs, typename... DigesterArgs>
  void Reset(std::tuple<DestArgs...> dest_args,
             DigesterArgs&&... digester_args);

  // Digests buffered data if needed, and returns the digest.
  DigestType Digest();

  // Returns the object providing and possibly owning the original `Writer`.
  // Unchanged by `Close()`.
  Dest& dest() { return dest_.manager(); }
  const Dest& dest() const { return dest_.manager(); }
  Writer* dest_writer() override { return dest_.get(); }
  const Writer* dest_writer() const override { return dest_.get(); }

 protected:
  void Done() override;
  bool FlushImpl(FlushType flush_type) override;

  using DigestingWriterBase::DigesterWrite;
  void DigesterWrite(absl::string_view src) override;
  void DigesterWriteZeros(Position length) override;

 private:
  void MoveDest(DigestingWriter&& that);

  Digester digester_;
  // The object providing and possibly owning the original `Writer`.
  Dependency<Writer*, Dest> dest_;
};

// Support CTAD.
#if __cpp_deduction_guides
explicit DigestingWriter(Closed)->DigestingWriter<void, DeleteCtad<Closed>>;
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

// Implementation details follow.

inline DigestingWriterBase::DigestingWriterBase(
    DigestingWriterBase&& that) noexcept
    : Writer(std::move(that)) {}

inline DigestingWriterBase& DigestingWriterBase::operator=(
    DigestingWriterBase&& that) noexcept {
  Writer::operator=(std::move(that));
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
  }
  dest.set_cursor(cursor());
}

inline void DigestingWriterBase::MakeBuffer(Writer& dest) {
  set_buffer(dest.cursor(), dest.available());
  set_start_pos(dest.pos());
  if (ABSL_PREDICT_FALSE(!dest.healthy())) FailWithoutAnnotation(dest);
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
    : DigestingWriterBase(std::move(that)),
      // Using `that` after it was moved is correct because only the base class
      // part was moved.
      digester_(std::move(that.digester_)) {
  MoveDest(std::move(that));
}

template <typename Digester, typename Dest>
inline DigestingWriter<Digester, Dest>&
DigestingWriter<Digester, Dest>::operator=(DigestingWriter&& that) noexcept {
  DigestingWriterBase::operator=(std::move(that));
  // Using `that` after it was moved is correct because only the base class part
  // was moved.
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
  if (dest_.kIsStable()) {
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
    if (ABSL_PREDICT_FALSE(!dest_->Close())) FailWithoutAnnotation(*dest_);
  }
  internal::DigesterClose(digester_);
}

template <typename Digester, typename Dest>
inline typename DigestingWriter<Digester, Dest>::DigestType
DigestingWriter<Digester, Dest>::Digest() {
  if (start_to_cursor() > 0) {
    DigesterWrite(absl::string_view(start(), start_to_cursor()));
    set_start_pos(pos());
    set_buffer(cursor(), available());
  }
  return internal::DigesterDigest(digester_);
}

template <typename Digester, typename Dest>
bool DigestingWriter<Digester, Dest>::FlushImpl(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  SyncBuffer(*dest_);
  bool ok = true;
  if (flush_type != FlushType::kFromObject || dest_.is_owning()) {
    ok = dest_->Flush(flush_type);
  }
  MakeBuffer(*dest_);
  return ok;
}

template <typename Digester, typename Dest>
void DigestingWriter<Digester, Dest>::DigesterWrite(absl::string_view src) {
  digester_.Write(src);
}

template <typename Digester, typename Dest>
void DigestingWriter<Digester, Dest>::DigesterWriteZeros(Position length) {
  internal::DigesterWriteZeros(digester_, length);
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_DIGESTING_WRITER_H_
