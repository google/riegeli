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
#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/pullable_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void JoiningReaderBase::Done() {
  PullableReader::Done();
  if (ABSL_PREDICT_TRUE(healthy())) {
    Reader* shard = shard_reader();
    if (shard_is_open(shard)) CloseShardInternal();
  }
}

bool JoiningReaderBase::CloseShardImpl() {
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of JoiningReaderBase::CloseShardImpl(): "
      << status();
  RIEGELI_ASSERT(shard_is_open())
      << "Failed precondition of JoiningReaderBase::CloseShardImpl(): "
         "shard already closed";
  Reader* shard = shard_reader();
  if (ABSL_PREDICT_FALSE(!shard->Close())) {
    return FailWithoutAnnotation(AnnotateOverShard(shard->status()));
  }
  return true;
}

inline bool JoiningReaderBase::OpenShardInternal() {
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of JoiningReaderBase::OpenShardInternal(): "
      << status();
  RIEGELI_ASSERT(!shard_is_open())
      << "Failed precondition of JoiningReaderBase::OpenShardInternal(): "
         "shard already opened";
  if (ABSL_PREDICT_FALSE(!OpenShardImpl())) return false;
  RIEGELI_ASSERT(healthy())
      << "Failed postcondition of JoiningReaderBase::OpenShardImpl(): "
      << status();
  RIEGELI_ASSERT(shard_is_open())
      << "Failed postcondition of JoiningReaderBase::OpenShardImpl(): "
         "shard not opened";
  return true;
}

inline bool JoiningReaderBase::CloseShardInternal() {
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of JoiningReaderBase::CloseShardInternal(): "
      << status();
  RIEGELI_ASSERT(shard_is_open())
      << "Failed precondition of JoiningReaderBase::CloseShardInternal(): "
         "shard already closed";
  const bool ok = CloseShardImpl();
  RIEGELI_ASSERT(!shard_is_open())
      << "Failed postcondition of JoiningReaderBase::CloseShardImpl(): "
         "shard not closed";
  if (ABSL_PREDICT_FALSE(!ok)) {
    RIEGELI_ASSERT(!healthy())
        << "Failed postcondition of JoiningReaderBase::CloseShardImpl(): "
           "false returned but JoiningReaderBase healthy";
    return false;
  }
  RIEGELI_ASSERT(healthy())
      << "Failed postcondition of JoiningReaderBase::CloseShardImpl(): "
      << status();
  return true;
}

bool JoiningReaderBase::OpenShard() {
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of JoiningReaderBase::OpenShard(): " << status();
  RIEGELI_ASSERT(!shard_is_open())
      << "Failed precondition of JoiningReaderBase::OpenShard(): "
         "shard already opened";
  if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return false;
  Reader* shard = shard_reader();
  MakeBuffer(*shard);
  return true;
}

bool JoiningReaderBase::CloseShard() {
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of JoiningReaderBase::CloseShard(): " << status();
  RIEGELI_ASSERT(shard_is_open())
      << "Failed precondition of JoiningReaderBase::CloseShard(): "
         "shard already closed";
  Reader* shard = shard_reader();
  SyncBuffer(*shard);
  return CloseShardInternal();
}

absl::Status JoiningReaderBase::AnnotateStatusImpl(absl::Status status) {
  Reader* shard = shard_reader();
  if (shard_is_open(shard)) status = shard->AnnotateStatus(std::move(status));
  // The status might have been annotated by `*shard_reader()` with the position
  // within the shard. Clarify that the current position is the position across
  // shards instead of delegating to `PullableReader::AnnotateStatusImpl()`.
  return AnnotateOverShard(std::move(status));
}

absl::Status JoiningReaderBase::AnnotateOverShard(absl::Status status) {
  if (is_open()) {
    return Annotate(status, absl::StrCat("across shards at byte ", pos()));
  }
  return status;
}

bool JoiningReaderBase::PullBehindScratch() {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of PullableReader::PullBehindScratch(): "
         "enough data available, use Pull() instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PullableReader::PullBehindScratch(): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(pos() == std::numeric_limits<Position>::max())) {
    return FailOverflow();
  }
  Reader* shard = shard_reader();
  if (shard_is_open(shard)) {
    SyncBuffer(*shard);
  } else {
    if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return false;
    shard = shard_reader();
  }
  while (ABSL_PREDICT_FALSE(!shard->Pull())) {
    if (ABSL_PREDICT_FALSE(!shard->healthy())) {
      return FailWithoutAnnotation(AnnotateOverShard(shard->status()));
    }
    if (ABSL_PREDICT_FALSE(!CloseShardInternal())) return false;
    if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return false;
    shard = shard_reader();
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
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(length >
                         std::numeric_limits<Position>::max() - pos())) {
    return FailOverflow();
  }
  Reader* shard = shard_reader();
  if (shard_is_open(shard)) {
    SyncBuffer(*shard);
  } else {
    if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return false;
    shard = shard_reader();
  }
  for (;;) {
    const Position pos_before = shard->pos();
    if (ABSL_PREDICT_TRUE(shard->Read(length, dest))) {
      move_limit_pos(length);
      break;
    }
    if (ABSL_PREDICT_FALSE(!shard->healthy())) {
      return FailWithoutAnnotation(AnnotateOverShard(shard->status()));
    }
    RIEGELI_ASSERT_GE(shard->pos(), pos_before)
        << "Reader::Read() decreased pos()";
    const Position length_read = shard->pos() - pos_before;
    RIEGELI_ASSERT_LE(length_read, length)
        << "Reader::Read() read more than requested";
    move_limit_pos(length_read);
    dest += IntCast<size_t>(length_read);
    length -= IntCast<size_t>(length_read);
    if (ABSL_PREDICT_FALSE(!CloseShardInternal())) return false;
    if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return false;
    shard = shard_reader();
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
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(length >
                         std::numeric_limits<Position>::max() - pos())) {
    return FailOverflow();
  }
  Reader* shard = shard_reader();
  if (shard_is_open(shard)) {
    SyncBuffer(*shard);
  } else {
    if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return false;
    shard = shard_reader();
  }
  for (;;) {
    const Position pos_before = shard->pos();
    if (ABSL_PREDICT_TRUE(shard->ReadAndAppend(length, dest))) {
      move_limit_pos(length);
      break;
    }
    if (ABSL_PREDICT_FALSE(!shard->healthy())) {
      return FailWithoutAnnotation(AnnotateOverShard(shard->status()));
    }
    RIEGELI_ASSERT_GE(shard->pos(), pos_before)
        << "Reader::Read() decreased pos()";
    const Position length_read = shard->pos() - pos_before;
    RIEGELI_ASSERT_LE(length_read, length)
        << "Reader::Read() read more than requested";
    move_limit_pos(length_read);
    length -= IntCast<size_t>(length_read);
    if (ABSL_PREDICT_FALSE(!CloseShardInternal())) return false;
    if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return false;
    shard = shard_reader();
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
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  if (ABSL_PREDICT_FALSE(length >
                         std::numeric_limits<Position>::max() - pos())) {
    return FailOverflow();
  }
  Reader* shard = shard_reader();
  if (shard_is_open(shard)) {
    SyncBuffer(*shard);
  } else {
    if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return false;
    shard = shard_reader();
  }
  for (;;) {
    const Position pos_before = shard->pos();
    if (ABSL_PREDICT_TRUE(shard->Copy(length, dest))) {
      move_limit_pos(length);
      break;
    }
    if (ABSL_PREDICT_FALSE(!dest.healthy())) return false;
    if (ABSL_PREDICT_FALSE(!shard->healthy())) {
      return FailWithoutAnnotation(AnnotateOverShard(shard->status()));
    }
    RIEGELI_ASSERT_GE(shard->pos(), pos_before)
        << "Reader::CopyTo() decreased pos()";
    const Position length_read = shard->pos() - pos_before;
    RIEGELI_ASSERT_LE(length_read, length)
        << "Reader::CopyTo() read more than requested";
    move_limit_pos(length_read);
    length -= length_read;
    if (ABSL_PREDICT_FALSE(!CloseShardInternal())) return false;
    if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return false;
    shard = shard_reader();
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
  if (ABSL_PREDICT_FALSE(!healthy())) return;
  Reader* shard = shard_reader();
  if (shard_is_open(shard)) {
    SyncBuffer(*shard);
  } else {
    if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return;
    shard = shard_reader();
  }
  shard->ReadHint(min_length, recommended_length);
  MakeBuffer(*shard);
}

}  // namespace riegeli
