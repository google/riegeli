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

#include "riegeli/bytes/splitting_writer.h"

#include <stddef.h>

#include <limits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/buffering.h"
#include "riegeli/base/byte_fill.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/status.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/cord_reader.h"
#include "riegeli/bytes/pushable_writer.h"
#include "riegeli/bytes/string_reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void SplittingWriterBase::DoneBehindScratch() {
  if (ABSL_PREDICT_TRUE(ok())) {
    Writer* shard = ShardWriter();
    if (shard_is_open(shard)) {
      SyncBuffer(*shard);
      CloseShardInternal();
    }
  }
}

bool SplittingWriterBase::CloseShardImpl() {
  RIEGELI_ASSERT(ok())
      << "Failed precondition of SplittingWriterBase::CloseShardImpl(): "
      << status();
  RIEGELI_ASSERT(shard_is_open())
      << "Failed precondition of SplittingWriterBase::CloseShardImpl(): "
         "shard already closed";
  Writer* shard = ShardWriter();
  if (ABSL_PREDICT_FALSE(!shard->Close())) {
    return FailWithoutAnnotation(AnnotateOverShard(shard->status()));
  }
  return true;
}

inline bool SplittingWriterBase::OpenShardInternal() {
  RIEGELI_ASSERT(ok())
      << "Failed precondition of SplittingWriterBase::OpenShardInternal(): "
      << status();
  RIEGELI_ASSERT(!shard_is_open())
      << "Failed precondition of SplittingWriterBase::OpenShardInternal(): "
         "shard already opened";
  if (ABSL_PREDICT_FALSE(start_pos() == std::numeric_limits<Position>::max())) {
    return FailOverflow();
  }
  const absl::optional<Position> size_limit = OpenShardImpl();
  if (ABSL_PREDICT_FALSE(size_limit == absl::nullopt)) {
    RIEGELI_ASSERT(!ok())
        << "Failed postcondition of SplittingWriterBase::OpenShardImpl(): "
           "zero returned but SplittingWriterBase OK";
    return false;
  }
  RIEGELI_ASSERT(ok())
      << "Failed postcondition of SplittingWriterBase::OpenShardImpl(): "
      << status();
  RIEGELI_ASSERT(shard_is_open())
      << "Failed postcondition of SplittingWriterBase::OpenShardImpl(): "
         "shard not opened";
  shard_pos_limit_ = SaturatingAdd(start_pos(), *size_limit);
  Writer* shard = ShardWriter();
  Position limit_hint = shard_pos_limit_;
  if (size_hint_ != absl::nullopt) {
    limit_hint = UnsignedMin(limit_hint, *size_hint_);
  }
  shard->SetWriteSizeHint(SaturatingSub(limit_hint, start_pos()));
  return true;
}

inline bool SplittingWriterBase::CloseShardInternal() {
  RIEGELI_ASSERT(ok())
      << "Failed precondition of SplittingWriterBase::CloseShardInternal(): "
      << status();
  RIEGELI_ASSERT(shard_is_open())
      << "Failed precondition of SplittingWriterBase::CloseShardInternal(): "
         "shard already closed";
  const bool close_shard_ok = CloseShardImpl();
  RIEGELI_ASSERT(!shard_is_open())
      << "Failed postcondition of SplittingWriterBase::CloseShardImpl(): "
         "shard not closed";
  if (ABSL_PREDICT_FALSE(!close_shard_ok)) {
    RIEGELI_ASSERT(!ok())
        << "Failed postcondition of SplittingWriterBase::CloseShardImpl(): "
           "SplittingWriterBase OK";
    return false;
  }
  RIEGELI_ASSERT(ok())
      << "Failed postcondition of SplittingWriterBase::CloseShardImpl(): "
      << status();
  return true;
}

bool SplittingWriterBase::OpenShard() {
  RIEGELI_ASSERT(ok())
      << "Failed precondition of SplittingWriterBase::OpenShard(): "
      << status();
  RIEGELI_ASSERT(!shard_is_open())
      << "Failed precondition of SplittingWriterBase::OpenShard(): "
         "shard already opened";
  if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return false;
  Writer* shard = ShardWriter();
  MakeBuffer(*shard);
  return true;
}

bool SplittingWriterBase::CloseShard() {
  RIEGELI_ASSERT(ok())
      << "Failed precondition of SplittingWriterBase::CloseShard(): "
      << status();
  RIEGELI_ASSERT(shard_is_open())
      << "Failed precondition of SplittingWriterBase::CloseShard(): "
         "shard already closed";
  Writer* shard = ShardWriter();
  SyncBuffer(*shard);
  return CloseShardInternal();
}

absl::Status SplittingWriterBase::AnnotateStatusImpl(absl::Status status) {
  Writer* shard = ShardWriter();
  if (shard_is_open(shard)) status = shard->AnnotateStatus(std::move(status));
  // The status might have been annotated by `*shard` with the position within
  // the shard. Clarify that the current position is the position across shards
  // instead of delegating to `PushableWriter::AnnotateStatusImpl()`.
  return AnnotateOverShard(std::move(status));
}

absl::Status SplittingWriterBase::AnnotateOverShard(absl::Status status) {
  if (is_open()) {
    return Annotate(status, absl::StrCat("across shards at byte ", pos()));
  }
  return status;
}

void SplittingWriterBase::SetWriteSizeHintImpl(
    absl::optional<Position> write_size_hint) {
  if (ABSL_PREDICT_FALSE(!ok())) return;
  if (write_size_hint == absl::nullopt) {
    size_hint_ = absl::nullopt;
  } else {
    size_hint_ = SaturatingAdd(pos(), *write_size_hint);
  }
  Writer* shard = ShardWriter();
  if (!shard_is_open(shard)) return;
  BehindScratch behind_scratch(this);
  SyncBuffer(*shard);
  Position limit_hint = shard_pos_limit_;
  if (size_hint_ != absl::nullopt) {
    limit_hint = UnsignedMin(limit_hint, *size_hint_);
  }
  shard->SetWriteSizeHint(SaturatingSub(limit_hint, start_pos()));
  MakeBuffer(*shard);
}

bool SplittingWriterBase::PushBehindScratch(size_t recommended_length) {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of PushableWriter::PushBehindScratch(): "
         "some space available, use Push() instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableWriter::PushBehindScratch(): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  RIEGELI_ASSERT_LE(pos(), shard_pos_limit_)
      << "Failed invariant of SplittingWriter: "
         "current position exceeds the shard limit";
  Writer* shard = ShardWriter();
  if (!shard_is_open(shard)) {
    if (!AllowEmptyShards()) return ForcePushUsingScratch();
    if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return false;
    shard = ShardWriter();
  } else {
    SyncBuffer(*shard);
    while (start_pos() == shard_pos_limit_) {
      if (ABSL_PREDICT_FALSE(!CloseShardInternal())) return false;
      if (!AllowEmptyShards()) return ForcePushUsingScratch();
      if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return false;
      shard = ShardWriter();
    }
  }
  const bool push_ok = shard->Push(1, recommended_length);
  MakeBuffer(*shard);
  return push_ok;
}

bool SplittingWriterBase::WriteBehindScratch(absl::string_view src) {
  RIEGELI_ASSERT_LT(available(), src.size())
      << "Failed precondition of "
         "PushableWriter::WriteBehindScratch(string_view): "
         "enough space available, use Write(string_view) instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of "
         "PushableWriter::WriteBehindScratch(string_view): "
         "scratch used";
  return WriteInternal<StringReader<const absl::string_view*>>(src);
}

bool SplittingWriterBase::WriteBehindScratch(const Chain& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of PushableWriter::WriteBehindScratch(Chain): "
         "enough space available, use Write(Chain) instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableWriter::WriteBehindScratch(Chain): "
         "scratch used";
  return WriteInternal<ChainReader<>>(src);
}

bool SplittingWriterBase::WriteBehindScratch(Chain&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of PushableWriter::WriteBehindScratch(Chain&&): "
         "enough space available, use Write(Chain&&) instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableWriter::WriteBehindScratch(Chain&&): "
         "scratch used";
  return WriteInternal<ChainReader<>>(std::move(src));
}

bool SplittingWriterBase::WriteBehindScratch(const absl::Cord& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of PushableWriter::WriteBehindScratch(Cord): "
         "enough space available, use Write(Cord) instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableWriter::WriteBehindScratch(Cord): "
         "scratch used";
  return WriteInternal<CordReader<>>(src);
}

bool SplittingWriterBase::WriteBehindScratch(absl::Cord&& src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of PushableWriter::WriteBehindScratch(Cord&&): "
         "enough space available, use Write(Cord&&) instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableWriter::WriteBehindScratch(Cord&&): "
         "scratch used";
  return WriteInternal<CordReader<>>(std::move(src));
}

template <typename SrcReader, typename Src>
inline bool SplittingWriterBase::WriteInternal(Src&& src) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  RIEGELI_ASSERT_LE(pos(), shard_pos_limit_)
      << "Failed invariant of SplittingWriter: "
         "current position exceeds the shard limit";
  Writer* shard = ShardWriter();
  if (shard_is_open(shard)) {
    SyncBuffer(*shard);
  } else {
    if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return false;
    shard = ShardWriter();
  }
  Position length_to_write = shard_pos_limit_ - start_pos();
  bool write_ok;
  size_t length = src.size();
  if (ABSL_PREDICT_TRUE(length <= length_to_write)) {
    if (ABSL_PREDICT_FALSE(!shard->Write(std::forward<Src>(src)))) {
      write_ok = false;
    } else {
      move_start_pos(length);
      write_ok = true;
    }
  } else {
    SrcReader reader(&src);
    for (;;) {
      if (ABSL_PREDICT_FALSE(!reader.Copy(length_to_write, *shard))) {
        RIEGELI_ASSERT(!shard->ok()) << "Reading failed";
        write_ok = false;
        break;
      }
      move_start_pos(length_to_write);
      length -= length_to_write;
      if (length == 0) {
        write_ok = true;
        break;
      }
      if (ABSL_PREDICT_FALSE(!CloseShardInternal())) return false;
      if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return false;
      shard = ShardWriter();
      length_to_write = UnsignedMin(length, shard_pos_limit_ - start_pos());
    }
  }
  MakeBuffer(*shard);
  return write_ok;
}

bool SplittingWriterBase::WriteBehindScratch(ByteFill src) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), src.size())
      << "Failed precondition of "
         "PushableWriter::WriteBehindScratch(ByteFill): "
         "enough space available, use Write(ByteFill) instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of "
         "PushableWriter::WriteBehindScratch(ByteFill): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  RIEGELI_ASSERT_LE(pos(), shard_pos_limit_)
      << "Failed invariant of SplittingWriter: "
         "current position exceeds the shard limit";
  Writer* shard = ShardWriter();
  if (shard_is_open(shard)) {
    SyncBuffer(*shard);
  } else {
    if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return false;
    shard = ShardWriter();
  }
  bool write_ok;
  for (;;) {
    const Position length_to_write =
        UnsignedMin(src.size(), shard_pos_limit_ - start_pos());
    if (ABSL_PREDICT_FALSE(!shard->Write(src.Extract(length_to_write)))) {
      write_ok = false;
      break;
    }
    move_start_pos(length_to_write);
    if (src.empty()) {
      write_ok = true;
      break;
    }
    if (ABSL_PREDICT_FALSE(!CloseShardInternal())) return false;
    if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return false;
    shard = ShardWriter();
  }
  MakeBuffer(*shard);
  return write_ok;
}

bool SplittingWriterBase::FlushBehindScratch(FlushType flush_type) {
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableWriter::FlushBehindScratch(): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  RIEGELI_ASSERT_LE(pos(), shard_pos_limit_)
      << "Failed invariant of SplittingWriter: "
         "current position exceeds the shard limit";
  Writer* const shard = ShardWriter();
  if (shard_is_open(shard)) {
    SyncBuffer(*shard);
    if (flush_type != FlushType::kFromObject) {
      if (ABSL_PREDICT_FALSE(!shard->Flush(flush_type))) return false;
    }
    if (ABSL_PREDICT_FALSE(!CloseShardInternal())) return false;
  }
  return true;
}

}  // namespace riegeli
