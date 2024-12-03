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

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/byte_fill.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/moving_dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/pushable_writer.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

// Template parameter independent part of `SplittingWriter`.
class SplittingWriterBase : public PushableWriter {
 protected:
  using PushableWriter::PushableWriter;

  SplittingWriterBase(SplittingWriterBase&& that) noexcept;
  SplittingWriterBase& operator=(SplittingWriterBase&& that) noexcept;

  void Reset(Closed);
  void Reset();

  void DoneBehindScratch() override;
  void SetWriteSizeHintImpl(absl::optional<Position> write_size_hint) override;

  // Returns the shard `Writer`.
  virtual Writer* ShardWriter() ABSL_ATTRIBUTE_LIFETIME_BOUND = 0;
  virtual const Writer* ShardWriter() const ABSL_ATTRIBUTE_LIFETIME_BOUND = 0;

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
  // because it does not synchronize buffer pointers of `*ShardWriter()` with
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
  // `*ShardWriter()`. See `CloseShard()` for that.
  virtual bool CloseShardImpl();

  // Calls `OpenShardImpl()` and synchronizes buffer pointers of
  // `*ShardWriter()` with `*this`.
  //
  // Preconditions:
  //   `ok()`
  //   `!shard_is_open()`
  //
  // Return values:
  //  * `true`  - success (`ok()`, `shard_is_open()`)
  //  * `false` - failure (`!ok()`)
  bool OpenShard();

  // Synchronizes buffer pointers of `*this` with `*ShardWriter()` and calls
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
  // `ShardWriter()`.
  bool shard_is_open() const;
  bool shard_is_open(const Writer* shard) const;

  // Should return `true` if `OpenShardImpl()` and then `CloseShardImpl()` may
  // be called with no data written to `shard()`. This makes `Push()` at a shard
  // boundary more efficient, because the buffer will be created directly in the
  // shard, but `OpenShardImpl()` and `CloseShardImpl()` must be able to deal
  // with empty shards.
  //
  // Should return `false` if `OpenShardImpl()` may be called only if the shard
  // will definitely have some data written before calling `CloseShardImpl()`.
  // This makes `Push()` at a shard boundary less efficient, because the
  // beginning of the data will be written to a scratch buffer and then copied
  // to the shard, but `OpenShardImpl()` and `CloseShardImpl()` can assume that
  // all shards are non-empty.
  virtual bool AllowEmptyShards() { return false; }

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
  bool WriteBehindScratch(ByteFill src) override;

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
  //   `start() == (shard_is_open() ? ShardWriter()->cursor() : nullptr)`
  //   `limit() <= (shard_is_open() ? ShardWriter()->limit() : nullptr)`
  //   `pos() <= shard_pos_limit_`
};

// Abstract class of a `Writer` which splits data into multiple shards. When a
// new shard is opened, the size limit of this shard is declared.
//
// The `Shard` template parameter specifies the type of the object providing and
// possibly owning the shard `Writer`. `Shard` must support
// `Dependency<Writer*, Shard>`, e.g. `Writer*` (not owned),
// `ChainWriter<>` (owned), `std::unique_ptr<Writer>` (owned),
// `Any<Writer*>` (maybe owned).
template <typename Shard>
class SplittingWriter : public SplittingWriterBase {
 protected:
  using SplittingWriterBase::SplittingWriterBase;

  SplittingWriter(SplittingWriter&& that) = default;
  SplittingWriter& operator=(SplittingWriter&& that) = default;

  // Makes `*this` equivalent to a newly constructed `SplittingWriter`. This
  // avoids constructing a temporary `SplittingWriter` and moving from it.
  // Derived classes which override `Reset()` should include a call to
  // `SplittingWriter::Reset()`.
  void Reset(Closed);
  void Reset();

  void Done() override;

  // Returns the object providing and possibly owning the shard `Writer`.
  Shard& shard() ABSL_ATTRIBUTE_LIFETIME_BOUND { return shard_.manager(); }
  const Shard& shard() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return shard_.manager();
  }
  Writer* ShardWriter() ABSL_ATTRIBUTE_LIFETIME_BOUND override {
    return shard_.get();
  }
  const Writer* ShardWriter() const ABSL_ATTRIBUTE_LIFETIME_BOUND override {
    return shard_.get();
  }

 private:
  class Mover;

  // The object providing and possibly owning the shard `Writer`.
  MovingDependency<Writer*, Shard, Mover> shard_;
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
  return shard_is_open(ShardWriter());
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
class SplittingWriter<Shard>::Mover {
 public:
  static auto member() { return &SplittingWriter::shard_; }

  explicit Mover(SplittingWriter& self, SplittingWriter& that)
      : behind_scratch_(&self), uses_buffer_(self.start() != nullptr) {
    // Buffer pointers are already moved so `SyncBuffer()` is called on `self`.
    // `shard_` is not moved yet so `shard_` is taken from `that`.
    if (uses_buffer_) self.SyncBuffer(*that.shard_);
  }

  void Done(SplittingWriter& self) {
    if (uses_buffer_) self.MakeBuffer(*self.shard_);
  }

 private:
  BehindScratch behind_scratch_;
  bool uses_buffer_;
};

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

}  // namespace riegeli

#endif  // RIEGELI_BYTES_SPLITTING_WRITER_H_
