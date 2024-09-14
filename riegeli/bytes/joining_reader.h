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

#ifndef RIEGELI_BYTES_JOINING_READER_H_
#define RIEGELI_BYTES_JOINING_READER_H_

#include <stddef.h>

#include <limits>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/moving_dependency.h"
#include "riegeli/base/object.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/pullable_reader.h"
#include "riegeli/bytes/reader.h"

namespace riegeli {

class Writer;

// Template parameter independent part of `JoiningReader`.
class JoiningReaderBase : public PullableReader {
 public:
  bool ToleratesReadingAhead() override { return read_all_hint_; }

 protected:
  using PullableReader::PullableReader;

  JoiningReaderBase(JoiningReaderBase&& that) noexcept;
  JoiningReaderBase& operator=(JoiningReaderBase&& that) noexcept;

  void Reset(Closed);
  void Reset();

  void Done() override;
  void SetReadAllHintImpl(bool read_all_hint) override;

  // Returns the shard `Reader`.
  virtual Reader* ShardReader() ABSL_ATTRIBUTE_LIFETIME_BOUND = 0;
  virtual const Reader* ShardReader() const ABSL_ATTRIBUTE_LIFETIME_BOUND = 0;

  // Opens the next shard as `shard()` if it exists.
  //
  // Preconditions:
  //   `ok()`
  //   `!shard_is_open()`
  //
  // Return values:
  //  * `true`                 - success (`ok()`, `shard_is_open()`)
  //  * `false` (when `ok()`)  - there is no next shard
  //  * `false` (when `!ok()`) - failure
  //
  // `OpenShardImpl()` must be overridden but should not be called directly
  // because it does not synchronize buffer pointers of `*ShardReader()` with
  // `*this`. See `OpenShard()` for that.
  virtual bool OpenShardImpl() = 0;

  // Closes `shard()`.
  //
  // Preconditions:
  //   `ok()`
  //   `shard_is_open()`
  //
  // Return values:
  //  * `true`  - success (`ok()`, `!shard_is_open()`)
  //  * `false` - failure (`!ok()`, `!shard_is_open()`)
  //
  // The default implementation calls `ShardReader()->Close()` and propagates
  // failures from that.
  //
  // `CloseShardImpl()` can be overridden but should not be called directly
  // because it does not synchronize buffer pointers of `*this` with
  // `*ShardReader()`. See `CloseShard()` for that.
  virtual bool CloseShardImpl();

  // Calls `OpenShardImpl()` and synchronizes buffer pointers of
  // `*ShardReader()` with `*this`.
  //
  // Preconditions:
  //   `ok()`
  //   `!shard_is_open()`
  //
  // Return values:
  //  * `true`                 - success (`ok()`, `shard_is_open()`)
  //  * `false` (when `ok()`)  - there is no next shard
  //  * `false` (when `!ok()`) - failure
  bool OpenShard();

  // Synchronizes buffer pointers of `*this` with `*ShardReader()` and calls
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
  // `ShardReader()`.
  bool shard_is_open() const;
  bool shard_is_open(const Reader* shard) const;

  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatusImpl(
      absl::Status status) override;
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateOverShard(absl::Status status);

  // Sets cursor of `shard` to cursor of `*this`. Sets buffer pointers of
  // `*this` to `nullptr`.
  void SyncBuffer(Reader& shard);

  // Sets buffer pointers of `*this` to buffer pointers of `shard`. Fails
  // `*this` if `shard` failed.
  void MakeBuffer(Reader& shard);

  bool PullBehindScratch(size_t recommended_length) override;
  using PullableReader::ReadBehindScratch;
  bool ReadBehindScratch(size_t length, char* dest) override;
  bool ReadBehindScratch(size_t length, Chain& dest) override;
  bool ReadBehindScratch(size_t length, absl::Cord& dest) override;
  using PullableReader::CopyBehindScratch;
  bool CopyBehindScratch(Position length, Writer& dest) override;
  using PullableReader::ReadOrPullSomeBehindScratch;
  bool ReadOrPullSomeBehindScratch(
      size_t max_length, absl::FunctionRef<char*(size_t&)> get_dest) override;
  void ReadHintBehindScratch(size_t min_length,
                             size_t recommended_length) override;

 private:
  bool OpenShardInternal();
  bool CloseShardInternal();

  // This template is defined and used only in joining_reader.cc.
  template <typename Dest>
  bool ReadInternal(size_t length, Dest& dest);

  bool read_all_hint_ = false;

  // Invariants if `ok()` and scratch is not used:
  //   `start() == (shard_is_open() ? ShardReader()->cursor() : nullptr)`
  //   `limit() <= (shard_is_open() ? ShardReader()->limit() : nullptr)`
};

// Abstract class of a `Reader` which joins data from multiple shards.
//
// The `Shard` template parameter specifies the type of the object providing and
// possibly owning the shard `Reader`. `Shard` must support
// `Dependency<Reader*, Shard>`, e.g. `Reader*` (not owned),
// `std::unique_ptr<Reader>` (owned), `ChainReader<>` (owned).
template <typename Shard>
class JoiningReader : public JoiningReaderBase {
 protected:
  using JoiningReaderBase::JoiningReaderBase;

  JoiningReader(JoiningReader&& that) = default;
  JoiningReader& operator=(JoiningReader&& that) = default;

  // Makes `*this` equivalent to a newly constructed `JoiningReader`. This
  // avoids constructing a temporary `JoiningReader` and moving from it.
  // Derived classes which override `Reset()` should include a call to
  // `JoiningReader::Reset()`.
  void Reset(Closed);
  void Reset();

  void Done() override;

  // Returns the object providing and possibly owning the shard `Reader`.
  Shard& shard() ABSL_ATTRIBUTE_LIFETIME_BOUND { return shard_.manager(); }
  const Shard& shard() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return shard_.manager();
  }
  Reader* ShardReader() ABSL_ATTRIBUTE_LIFETIME_BOUND override {
    return shard_.get();
  }
  const Reader* ShardReader() const ABSL_ATTRIBUTE_LIFETIME_BOUND override {
    return shard_.get();
  }

 private:
  class Mover;

  // The object providing and possibly owning the shard `Reader`.
  MovingDependency<Reader*, Shard, Mover> shard_;
};

// Implementation details follow.

inline JoiningReaderBase::JoiningReaderBase(JoiningReaderBase&& that) noexcept
    : PullableReader(static_cast<PullableReader&&>(that)),
      read_all_hint_(that.read_all_hint_) {}

inline JoiningReaderBase& JoiningReaderBase::operator=(
    JoiningReaderBase&& that) noexcept {
  PullableReader::operator=(static_cast<PullableReader&&>(that));
  read_all_hint_ = that.read_all_hint_;
  return *this;
}

inline void JoiningReaderBase::Reset(Closed) {
  PullableReader::Reset(kClosed);
  read_all_hint_ = false;
}

inline void JoiningReaderBase::Reset() {
  PullableReader::Reset();
  read_all_hint_ = false;
}

inline bool JoiningReaderBase::shard_is_open() const {
  return shard_is_open(ShardReader());
}

inline bool JoiningReaderBase::shard_is_open(const Reader* shard) const {
  return shard != nullptr && shard->is_open();
}

inline void JoiningReaderBase::SyncBuffer(Reader& shard) {
  RIEGELI_ASSERT(shard_is_open(&shard))
      << "Failed precondition of JoiningReaderBase::SyncBuffer(): "
         "shard is closed";
  shard.set_cursor(cursor());
  set_limit_pos(pos());
  set_buffer();
}

inline void JoiningReaderBase::MakeBuffer(Reader& shard) {
  RIEGELI_ASSERT(shard_is_open(&shard))
      << "Failed precondition of JoiningReaderBase::MakeBuffer(): "
         "shard is closed";
  set_buffer(shard.cursor(),
             UnsignedMin(shard.available(),
                         std::numeric_limits<Position>::max() - limit_pos()));
  move_limit_pos(available());
  if (ABSL_PREDICT_FALSE(!shard.ok())) {
    FailWithoutAnnotation(AnnotateOverShard(shard.status()));
  }
}

template <typename Shard>
class JoiningReader<Shard>::Mover {
 public:
  static auto member() { return &JoiningReader::shard_; }

  explicit Mover(JoiningReader& self, JoiningReader& that)
      : behind_scratch_(&self), uses_buffer_(self.start() != nullptr) {
    // Buffer pointers are already moved so `SyncBuffer()` is called on `self`.
    // `shard_` is not moved yet so `shard_` is taken from `that`.
    if (uses_buffer_) self.SyncBuffer(*that.shard_);
  }

  void Done(JoiningReader& self) {
    if (uses_buffer_) self.MakeBuffer(*self.shard_);
  }

 private:
  BehindScratch behind_scratch_;
  bool uses_buffer_;
};

template <typename Shard>
inline void JoiningReader<Shard>::Reset(Closed) {
  JoiningReaderBase::Reset(kClosed);
  shard_.Reset();
}

template <typename Shard>
inline void JoiningReader<Shard>::Reset() {
  JoiningReaderBase::Reset();
  shard_.Reset();
}

template <typename Shard>
void JoiningReader<Shard>::Done() {
  JoiningReaderBase::Done();
  shard_.Reset();
}

}  // namespace riegeli

#endif  // RIEGELI_BYTES_JOINING_READER_H_
