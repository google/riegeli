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

#include "riegeli/bytes/joining_reader.h"

#include <stddef.h>

#include <limits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/pullable_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void JoiningReaderBase::Done() {
  PullableReader::Done();
  if (ABSL_PREDICT_TRUE(ok())) {
    Reader* shard = ShardReader();
    if (shard_is_open(shard)) CloseShardInternal();
  }
}

bool JoiningReaderBase::CloseShardImpl() {
  RIEGELI_ASSERT_OK(*this)
      << "Failed precondition of JoiningReaderBase::CloseShardImpl()";
  RIEGELI_ASSERT(shard_is_open())
      << "Failed precondition of JoiningReaderBase::CloseShardImpl(): "
         "shard already closed";
  Reader* shard = ShardReader();
  if (ABSL_PREDICT_FALSE(!shard->Close())) {
    return FailWithoutAnnotation(AnnotateOverShard(shard->status()));
  }
  return true;
}

inline bool JoiningReaderBase::OpenShardInternal() {
  RIEGELI_ASSERT_OK(*this)
      << "Failed precondition of JoiningReaderBase::OpenShardInternal()";
  RIEGELI_ASSERT(!shard_is_open())
      << "Failed precondition of JoiningReaderBase::OpenShardInternal(): "
         "shard already opened";
  if (ABSL_PREDICT_FALSE(!OpenShardImpl())) return false;
  RIEGELI_ASSERT_OK(*this)
      << "Failed postcondition of JoiningReaderBase::OpenShardImpl()";
  RIEGELI_ASSERT(shard_is_open())
      << "Failed postcondition of JoiningReaderBase::OpenShardImpl(): "
         "shard not opened";
  if (read_all_hint_) {
    Reader* shard = ShardReader();
    shard->SetReadAllHint(true);
  }
  return true;
}

inline bool JoiningReaderBase::CloseShardInternal() {
  RIEGELI_ASSERT_OK(*this)
      << "Failed precondition of JoiningReaderBase::CloseShardInternal()";
  RIEGELI_ASSERT(shard_is_open())
      << "Failed precondition of JoiningReaderBase::CloseShardInternal(): "
         "shard already closed";
  const bool close_shard_ok = CloseShardImpl();
  RIEGELI_ASSERT(!shard_is_open())
      << "Failed postcondition of JoiningReaderBase::CloseShardImpl(): "
         "shard not closed";
  if (ABSL_PREDICT_FALSE(!close_shard_ok)) {
    RIEGELI_ASSERT(!ok())
        << "Failed postcondition of JoiningReaderBase::CloseShardImpl(): "
           "false returned but JoiningReaderBase OK";
    return false;
  }
  RIEGELI_ASSERT_OK(*this)
      << "Failed postcondition of JoiningReaderBase::CloseShardImpl()";
  return true;
}

bool JoiningReaderBase::OpenShard() {
  RIEGELI_ASSERT_OK(*this)
      << "Failed precondition of JoiningReaderBase::OpenShard()";
  RIEGELI_ASSERT(!shard_is_open())
      << "Failed precondition of JoiningReaderBase::OpenShard(): "
         "shard already opened";
  if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return false;
  Reader* shard = ShardReader();
  MakeBuffer(*shard);
  return true;
}

bool JoiningReaderBase::CloseShard() {
  RIEGELI_ASSERT_OK(*this)
      << "Failed precondition of JoiningReaderBase::CloseShard()";
  RIEGELI_ASSERT(shard_is_open())
      << "Failed precondition of JoiningReaderBase::CloseShard(): "
         "shard already closed";
  Reader* shard = ShardReader();
  SyncBuffer(*shard);
  return CloseShardInternal();
}

absl::Status JoiningReaderBase::AnnotateStatusImpl(absl::Status status) {
  Reader* shard = ShardReader();
  if (shard_is_open(shard)) status = shard->AnnotateStatus(std::move(status));
  // The status might have been annotated by `*shard` with the position within
  // the shard. Clarify that the current position is the position across shards
  // instead of delegating to `PullableReader::AnnotateStatusImpl()`.
  return AnnotateOverShard(std::move(status));
}

absl::Status JoiningReaderBase::AnnotateOverShard(absl::Status status) {
  if (is_open()) {
    return Annotate(status, absl::StrCat("across shards at byte ", pos()));
  }
  return status;
}

void JoiningReaderBase::SetReadAllHintImpl(bool read_all_hint) {
  if (ABSL_PREDICT_FALSE(!ok())) return;
  read_all_hint_ = read_all_hint;
  Reader* shard = ShardReader();
  if (!shard_is_open(shard)) return;
  BehindScratch behind_scratch(this);
  SyncBuffer(*shard);
  shard->SetReadAllHint(read_all_hint_);
  MakeBuffer(*shard);
}

bool JoiningReaderBase::PullBehindScratch(size_t recommended_length) {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of PullableReader::PullBehindScratch(): "
         "enough data available, use Pull() instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::PullBehindScratch(): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader* shard = ShardReader();
  if (shard_is_open(shard)) {
    SyncBuffer(*shard);
  } else {
    if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return false;
    shard = ShardReader();
  }
  while (ABSL_PREDICT_FALSE(!shard->Pull(1, recommended_length))) {
    if (ABSL_PREDICT_FALSE(!shard->ok())) {
      return FailWithoutAnnotation(AnnotateOverShard(shard->status()));
    }
    if (ABSL_PREDICT_FALSE(!CloseShardInternal())) return false;
    if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return false;
    shard = ShardReader();
  }
  if (ABSL_PREDICT_FALSE(limit_pos() == std::numeric_limits<Position>::max())) {
    return FailOverflow();
  }
  MakeBuffer(*shard);
  return true;
}

bool JoiningReaderBase::ReadBehindScratch(size_t length, char* dest) {
  RIEGELI_ASSERT_LT(available(), length)
      << "Failed precondition of PullableReader::ReadBehindScratch(char*): "
         "enough data available, use Read(char*) instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::ReadBehindScratch(char*): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader* shard = ShardReader();
  if (shard_is_open(shard)) {
    SyncBuffer(*shard);
  } else {
    if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return false;
    shard = ShardReader();
  }
  for (;;) {
    const size_t length_to_read =
        UnsignedMin(length, std::numeric_limits<Position>::max() - limit_pos());
    size_t length_read;
    const bool read_ok = shard->Read(length_to_read, dest, &length_read);
    move_limit_pos(length_read);
    if (ABSL_PREDICT_TRUE(read_ok)) {
      if (ABSL_PREDICT_FALSE(length_to_read < length)) return FailOverflow();
      break;
    }
    if (ABSL_PREDICT_FALSE(!shard->ok())) {
      return FailWithoutAnnotation(AnnotateOverShard(shard->status()));
    }
    dest += length_read;
    length -= length_read;
    if (ABSL_PREDICT_FALSE(!CloseShardInternal())) return false;
    if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return false;
    shard = ShardReader();
  }
  MakeBuffer(*shard);
  return true;
}

bool JoiningReaderBase::ReadBehindScratch(size_t length, Chain& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of PullableReader::ReadBehindScratch(Chain&): "
         "enough data available, use Read(Chain&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of PullableReader::ReadBehindScratch(Chain&): "
         "Chain size overflow";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::ReadBehindScratch(Chain&): "
         "scratch used";
  return ReadInternal(length, dest);
}

bool JoiningReaderBase::ReadBehindScratch(size_t length, absl::Cord& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of PullableReader::ReadBehindScratch(Cord&): "
         "enough data available, use Read(Cord&) instead";
  RIEGELI_ASSERT_LE(length, std::numeric_limits<size_t>::max() - dest.size())
      << "Failed precondition of PullableReader::ReadBehindScratch(Cord&): "
         "Cord size overflow";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::ReadBehindScratch(Cord&): "
         "scratch used";
  return ReadInternal(length, dest);
}

template <typename Dest>
inline bool JoiningReaderBase::ReadInternal(size_t length, Dest& dest) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader* shard = ShardReader();
  if (shard_is_open(shard)) {
    SyncBuffer(*shard);
  } else {
    if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return false;
    shard = ShardReader();
  }
  for (;;) {
    const size_t length_to_read =
        UnsignedMin(length, std::numeric_limits<Position>::max() - limit_pos());
    size_t length_read;
    const bool read_ok =
        shard->ReadAndAppend(length_to_read, dest, &length_read);
    move_limit_pos(length_read);
    if (ABSL_PREDICT_TRUE(read_ok)) {
      if (ABSL_PREDICT_FALSE(length_to_read < length)) return FailOverflow();
      break;
    }
    if (ABSL_PREDICT_FALSE(!shard->ok())) {
      return FailWithoutAnnotation(AnnotateOverShard(shard->status()));
    }
    length -= length_read;
    if (ABSL_PREDICT_FALSE(!CloseShardInternal())) return false;
    if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return false;
    shard = ShardReader();
  }
  MakeBuffer(*shard);
  return true;
}

bool JoiningReaderBase::CopyBehindScratch(Position length, Writer& dest) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of PullableReader::CopyBehindScratch(Writer&): "
         "enough data available, use Copy(Writer&) instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::CopyBehindScratch(Writer&): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader* shard = ShardReader();
  if (shard_is_open(shard)) {
    SyncBuffer(*shard);
  } else {
    if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return false;
    shard = ShardReader();
  }
  for (;;) {
    const size_t length_to_read =
        UnsignedMin(length, std::numeric_limits<Position>::max() - limit_pos());
    Position length_read;
    const bool copy_ok = shard->Copy(length_to_read, dest, &length_read);
    move_limit_pos(length_read);
    if (ABSL_PREDICT_TRUE(copy_ok)) {
      if (ABSL_PREDICT_FALSE(length_to_read < length)) return FailOverflow();
      break;
    }
    if (ABSL_PREDICT_FALSE(!shard->ok())) {
      return FailWithoutAnnotation(AnnotateOverShard(shard->status()));
    }
    if (ABSL_PREDICT_FALSE(!dest.ok())) {
      MakeBuffer(*shard);
      return false;
    }
    length -= length_read;
    if (ABSL_PREDICT_FALSE(!CloseShardInternal())) return false;
    if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return false;
    shard = ShardReader();
  }
  MakeBuffer(*shard);
  return true;
}

bool JoiningReaderBase::ReadSomeBehindScratch(size_t max_length, char* dest) {
  RIEGELI_ASSERT_GT(max_length, 0u)
      << "Failed precondition of PullableReader::ReadSomeBehindScratch(char*): "
         "nothing to read, use ReadSome(char*) instead";
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of PullableReader::ReadSomeBehindScratch(char*): "
         "some data available, use ReadSome(char*) instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::ReadSomeBehindScratch(char*): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader* shard = ShardReader();
  if (shard_is_open(shard)) {
    SyncBuffer(*shard);
  } else {
    if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return false;
    shard = ShardReader();
  }
  const Position remaining = std::numeric_limits<Position>::max() - limit_pos();
  if (ABSL_PREDICT_FALSE(remaining == 0)) return FailOverflow();
  max_length = UnsignedMin(max_length, remaining);
  for (;;) {
    size_t length_read;
    if (ABSL_PREDICT_TRUE(shard->ReadSome(max_length, dest, &length_read))) {
      move_limit_pos(length_read);
      break;
    }
    if (ABSL_PREDICT_FALSE(!shard->ok())) {
      return FailWithoutAnnotation(AnnotateOverShard(shard->status()));
    }
    if (ABSL_PREDICT_FALSE(!CloseShardInternal())) return false;
    if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return false;
    shard = ShardReader();
  }
  MakeBuffer(*shard);
  return true;
}

bool JoiningReaderBase::CopySomeBehindScratch(size_t max_length, Writer& dest) {
  RIEGELI_ASSERT_GT(max_length, 0u)
      << "Failed precondition of "
         "PullableReader::CopySomeBehindScratch(Writer&): "
         "nothing to read, use CopySome(Writer&) instead";
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of "
         "PullableReader::CopySomeBehindScratch(Writer&): "
         "some data available, use CopySome(Writer&) instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of "
         "PullableReader::CopySomeBehindScratch(Writer&): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  Reader* shard = ShardReader();
  if (shard_is_open(shard)) {
    SyncBuffer(*shard);
  } else {
    if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return false;
    shard = ShardReader();
  }
  const Position remaining = std::numeric_limits<Position>::max() - limit_pos();
  if (ABSL_PREDICT_FALSE(remaining == 0)) return FailOverflow();
  max_length = UnsignedMin(max_length, remaining);
  for (;;) {
    size_t length_read;
    const bool copy_ok = shard->CopySome(max_length, dest, &length_read);
    move_limit_pos(length_read);
    if (ABSL_PREDICT_TRUE(copy_ok)) break;
    if (ABSL_PREDICT_FALSE(!shard->ok())) {
      return FailWithoutAnnotation(AnnotateOverShard(shard->status()));
    }
    if (ABSL_PREDICT_FALSE(!dest.ok())) {
      MakeBuffer(*shard);
      return false;
    }
    if (ABSL_PREDICT_FALSE(!CloseShardInternal())) return false;
    if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return false;
    shard = ShardReader();
  }
  MakeBuffer(*shard);
  return true;
}

void JoiningReaderBase::ReadHintBehindScratch(size_t min_length,
                                              size_t recommended_length) {
  RIEGELI_ASSERT_LT(available(), min_length)
      << "Failed precondition of PullableReader::ReadHintBehindScratch(): "
         "enough data available, use ReadHint() instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::ReadHintBehindScratch(): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!ok())) return;
  Reader* shard = ShardReader();
  if (shard_is_open(shard)) {
    SyncBuffer(*shard);
  } else {
    if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return;
    shard = ShardReader();
  }
  shard->ReadHint(min_length, recommended_length);
  MakeBuffer(*shard);
}

}  // namespace riegeli
