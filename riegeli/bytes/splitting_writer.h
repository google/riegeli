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

#ifndef RIEGELI_BYTES_SPLITTING_WRITER_H_
#define RIEGELI_BYTES_SPLITTING_WRITER_H_

#include <stddef.h>

#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/pushable_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Template parameter independent part of `SplittingWriter`.
class SplittingWriterBase : public PushableWriter {
 public:
  void SetWriteSizeHint(absl::optional<Position> write_size_hint) override;

 protected:
  using PushableWriter::PushableWriter;

  SplittingWriterBase(SplittingWriterBase&& that) noexcept;
  SplittingWriterBase& operator=(SplittingWriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset();

  void DoneBehindScratch() override;

  // Returns the shard `Writer`.
  virtual Writer* shard_writer() = 0;
  virtual const Writer* shard_writer() const = 0;

  // Opens the next shard as `shard()`. Or opens a temporary destination for
  // shard data as `shard()`, to be moved to the final destination later.
  //
  // Preconditions:
  //   `ok()`
  //   `!shard_is_open()`
  //
  // Return values:
  //  * size limit      - success (`ok()`, `shard_is_open()`)
  //  * `absl::nullopt` - failure (`!ok()`)
  //
  // When the size limit would be exceeded, the shard is closed and a new shard
  // is opened.
  //
  // `OpenShardImpl()` must be overridden but should not be called directly
  // because it does not synchronize buffer pointers of `*shard_writer()` with
  // `*this`. See `OpenShard()` for that.
  virtual absl::optional<Position> OpenShardImpl() = 0;

  // Closes `shard()`. If `shard()` is a temporary destination for shard data,
  // moves it to the final destination.
  //
  // Preconditions:
  //   `ok()`
  //   `shard_is_open()`
  //
  // Return values:
  //  * `true`  - success (`ok()`, `!shard_is_open()`)
  //  * `false` - failure (`!ok()`, `!shard_is_open()`)
  //
  // The default implementation calls `shard_witer()->Close()` and propagates
  // failures from that.
  //
  // `CloseShardImpl()` can be overridden but should not be called directly
  // because it does not synchronize buffer pointers of `*this` with
  // `*shard_writer()`. See `CloseShard()` for that.
  virtual bool CloseShardImpl();

  // Calls `OpenShardImpl()` and synchronizes buffer pointers of
  // `*shard_writer()` with `*this`.
  //
  // Preconditions:
  //   `ok()`
  //   `!shard_is_open()`
  //
  // Return values:
  //  * `true`  - success (`ok()`, `shard_is_open()`)
  //  * `false` - failure (`!ok()`)
  bool OpenShard();

  // Synchronizes buffer pointers of `*this` with `*shard_writer()` and calls
  // `CloseShardImpl()`.
  //
  // Preconditions:
  //   `ok()`
  //   `shard_is_open()`
  //
  // Return values:
  //  * `true`  - success (`ok()`, `!shard_is_open()`)
  //  * `false` - failure (`!ok()`, `!shard_is_open()`)
  bool CloseShard();

  // Returns `true` if a shard is open.
  //
  // Same as `shard != nullptr && shard->is_open()`, with the default `shard` of
  // `shard_writer()`.
  bool shard_is_open() const;
  bool shard_is_open(const Writer* shard) const;

  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateOverShard(absl::Status status);

  // Sets cursor of `shard` to cursor of `*this`. Sets buffer pointers of
  // `*this` to `nullptr`.
  void SyncBuffer(Writer& shard);

  // Sets buffer pointers of `*this` to buffer pointers of `shard`. Fails
  // `*this` if `shard` failed.
  void MakeBuffer(Writer& shard);

  bool PushBehindScratch(size_t recommended_length) override;
  bool WriteBehindScratch(absl::string_view src) override;
  bool WriteBehindScratch(const Chain& src) override;
  bool WriteBehindScratch(Chain&& src) override;
  bool WriteBehindScratch(const absl::Cord& src) override;
  bool WriteBehindScratch(absl::Cord&& src) override;
  bool WriteZerosBehindScratch(Position length) override;

  // Flushes the current shard if `flush_type != FlushType::kFromObject`.
  // Then closes the current shard.
  bool FlushBehindScratch(FlushType flush_type) override;

 private:
  bool OpenShardInternal();
  bool CloseShardInternal();

  // This template is defined and used only in splitting_writer.cc.
  template <typename SrcReader, typename Src>
  bool WriteInternal(Src&& src);

  absl::optional<Position> size_hint_;

  // The limit of `pos()` for data written to the current shard.
  Position shard_pos_limit_ = 0;

  // Invariants if `ok()` and scratch is not used:
  //   `start() == (shard_is_open() ? shard_writer()->cursor() : nullptr)`
  //   `limit() <= (shard_is_open() ? shard_writer()->limit() : nullptr)`
  //   `pos() <= shard_pos_limit_`
};

// Abstract class of a `Writer` which splits data into multiple shards. When a
// new shard is opened, the size limit of this shard is declared.
//
// The `Shard` template parameter specifies the type of the object providing and
// possibly owning the shard `Writer`. `Shard` must support
// `Dependency<Writer*, Shard>`, e.g. `Writer*` (not owned),
// `std::unique_ptr<Writer>` (owned), `ChainWriter<>` (owned).
template <typename Shard>
class SplittingWriter : public SplittingWriterBase {
 protected:
  using SplittingWriterBase::SplittingWriterBase;

  SplittingWriter(SplittingWriter&& that) noexcept;
  SplittingWriter& operator=(SplittingWriter&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `SplittingWriter`. This
  // avoids constructing a temporary `SplittingWriter` and moving from it.
  // Derived classes which override `Reset()` should include a call to
  // `SplittingWriter::Reset()`.
  void Reset(Closed);
  void Reset();

  void Done() override;

  // Returns the object providing and possibly owning the shard `Writer`.
  Shard& shard() { return shard_.manager(); }
  const Shard& shard() const { return shard_.manager(); }
  Writer* shard_writer() override { return shard_.get(); }
  const Writer* shard_writer() const override { return shard_.get(); }

 private:
  void MoveShard(SplittingWriter&& that);

  // The object providing and possibly owning the shard `Writer`.
  Dependency<Writer*, Shard> shard_;
};

// Implementation details follow.

inline SplittingWriterBase::SplittingWriterBase(
    SplittingWriterBase&& that) noexcept
    : PushableWriter(static_cast<PushableWriter&&>(that)),
      size_hint_(that.size_hint_),
      shard_pos_limit_(that.shard_pos_limit_) {}

inline SplittingWriterBase& SplittingWriterBase::operator=(
    SplittingWriterBase&& that) noexcept {
  PushableWriter::operator=(static_cast<PushableWriter&&>(that));
  size_hint_ = that.size_hint_;
  shard_pos_limit_ = that.shard_pos_limit_;
  return *this;
}

inline void SplittingWriterBase::Reset(Closed) {
  PushableWriter::Reset(kClosed);
  size_hint_ = absl::nullopt;
  shard_pos_limit_ = 0;
}

inline void SplittingWriterBase::Reset() {
  PushableWriter::Reset();
  size_hint_ = absl::nullopt;
  shard_pos_limit_ = 0;
}

inline bool SplittingWriterBase::shard_is_open() const {
  return shard_is_open(shard_writer());
}

inline bool SplittingWriterBase::shard_is_open(const Writer* shard) const {
  return shard != nullptr && shard->is_open();
}

inline void SplittingWriterBase::SyncBuffer(Writer& shard) {
  RIEGELI_ASSERT(shard_is_open(&shard))
      << "Failed precondition of SplittingWriterBase::SyncBuffer(): "
         "shard is closed";
  shard.set_cursor(cursor());
  move_start_pos(start_to_cursor());
  set_buffer();
}

inline void SplittingWriterBase::MakeBuffer(Writer& shard) {
  RIEGELI_ASSERT(shard_is_open(&shard))
      << "Failed precondition of SplittingWriterBase::MakeBuffer(): "
         "shard is closed";
  RIEGELI_ASSERT_LE(start_pos(), shard_pos_limit_)
      << "Failed invariant of SplittingWriter: "
         "current position exceeds the shard limit";
  set_buffer(shard.cursor(),
             UnsignedMin(shard.available(), shard_pos_limit_ - start_pos()));
  if (ABSL_PREDICT_FALSE(!shard.ok())) {
    FailWithoutAnnotation(AnnotateOverShard(shard.status()));
  }
}

template <typename Shard>
inline SplittingWriter<Shard>::SplittingWriter(SplittingWriter&& that) noexcept
    : SplittingWriterBase(static_cast<SplittingWriterBase&&>(that)) {
  MoveShard(std::move(that));
}

template <typename Shard>
inline SplittingWriter<Shard>& SplittingWriter<Shard>::operator=(
    SplittingWriter&& that) noexcept {
  SplittingWriterBase::operator=(static_cast<SplittingWriterBase&&>(that));
  MoveShard(std::move(that));
  return *this;
}

template <typename Shard>
inline void SplittingWriter<Shard>::Reset(Closed) {
  SplittingWriterBase::Reset(kClosed);
  shard_.Reset();
}

template <typename Shard>
inline void SplittingWriter<Shard>::Reset() {
  SplittingWriterBase::Reset();
  shard_.Reset();
}

template <typename Shard>
void SplittingWriter<Shard>::Done() {
  SplittingWriterBase::Done();
  shard_.Reset();
}

template <typename Shard>
inline void SplittingWriter<Shard>::MoveShard(SplittingWriter&& that) {
  if (shard_.kIsStable || !shard_is_open(that.shard_.get())) {
    shard_ = std::move(that.shard_);
  } else {
    BehindScratch behind_scratch(this);
    // Buffer pointers are already moved so `SyncBuffer()` is called on `*this`,
    // `shard_` is not moved yet so `shard_` is taken from `that`.
    SyncBuffer(*that.shard_);
    shard_ = std::move(that.shard_);
    MakeBuffer(*shard_);
  }
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_SPLITTING_WRITER_H_
