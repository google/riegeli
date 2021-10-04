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
#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/cord_reader.h"
#include "riegeli/bytes/pushable_writer.h"
#include "riegeli/bytes/string_reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

void SplittingWriterBase::DoneBehindScratch() {
  if (ABSL_PREDICT_TRUE(healthy())) {
    Writer* shard = shard_writer();
    if (shard_is_open(shard)) {
      SyncBuffer(*shard);
      CloseShardInternal();
    }
  }
}

bool SplittingWriterBase::CloseShardImpl() {
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of SplittingWriterBase::CloseShardImpl(): "
      << status();
  RIEGELI_ASSERT(shard_is_open())
      << "Failed precondition of SplittingWriterBase::CloseShardImpl(): "
         "shard already closed";
  Writer* shard = shard_writer();
  if (ABSL_PREDICT_FALSE(!shard->Close())) return Fail(*shard);
  return true;
}

inline bool SplittingWriterBase::OpenShardInternal() {
  RIEGELI_ASSERT(healthy())
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
    RIEGELI_ASSERT(!healthy())
        << "Failed postcondition of SplittingWriterBase::OpenShardImpl(): "
           "zero returned but SplittingWriterBase healthy";
    return false;
  }
  RIEGELI_ASSERT(healthy())
      << "Failed postcondition of SplittingWriterBase::OpenShardImpl(): "
      << status();
  RIEGELI_ASSERT(shard_is_open())
      << "Failed postcondition of SplittingWriterBase::OpenShardImpl(): "
         "shard not opened";
  shard_pos_limit_ = SaturatingAdd(start_pos(), *size_limit);
  return true;
}

inline bool SplittingWriterBase::CloseShardInternal() {
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of SplittingWriterBase::CloseShardInternal(): "
      << status();
  RIEGELI_ASSERT(shard_is_open())
      << "Failed precondition of SplittingWriterBase::CloseShardInternal(): "
         "shard already closed";
  const bool ok = CloseShardImpl();
  RIEGELI_ASSERT(!shard_is_open())
      << "Failed postcondition of SplittingWriterBase::CloseShardImpl(): "
         "shard not closed";
  if (ABSL_PREDICT_FALSE(!ok)) {
    RIEGELI_ASSERT(!healthy())
        << "Failed postcondition of SplittingWriterBase::CloseShardImpl(): "
           "SplittingWriterBase healthy";
    return false;
  }
  RIEGELI_ASSERT(healthy())
      << "Failed postcondition of SplittingWriterBase::CloseShardImpl(): "
      << status();
  return true;
}

bool SplittingWriterBase::OpenShard() {
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of SplittingWriterBase::OpenShard(): "
      << status();
  RIEGELI_ASSERT(!shard_is_open())
      << "Failed precondition of SplittingWriterBase::OpenShard(): "
         "shard already opened";
  if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return false;
  Writer* shard = shard_writer();
  MakeBuffer(*shard);
  return true;
}

bool SplittingWriterBase::CloseShard() {
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of SplittingWriterBase::CloseShard(): "
      << status();
  RIEGELI_ASSERT(shard_is_open())
      << "Failed precondition of SplittingWriterBase::CloseShard(): "
         "shard already closed";
  Writer* shard = shard_writer();
  SyncBuffer(*shard);
  return CloseShardInternal();
}

void SplittingWriterBase::DefaultAnnotateStatus() {
  RIEGELI_ASSERT(!not_failed())
      << "Failed precondition of Object::DefaultAnnotateStatus(): "
         "Object not failed";
  if (is_open()) AnnotateStatus(absl::StrCat("across shards at byte ", pos()));
}

bool SplittingWriterBase::PushBehindScratch() {
  RIEGELI_ASSERT_EQ(available(), 0u)
      << "Failed precondition of PushableWriter::PushBehindScratch(): "
         "some space available, use Push() instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableWriter::PushBehindScratch():"
         "scratch used";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  RIEGELI_ASSERT_LE(pos(), shard_pos_limit_)
      << "Failed invariant of SplittingWriter: "
         "current position exceeds the shard limit";
  Writer* shard = shard_writer();
  if (!shard_is_open(shard)) return ForcePushUsingScratch();
  SyncBuffer(*shard);
  if (start_pos() == shard_pos_limit_) {
    if (ABSL_PREDICT_FALSE(!CloseShardInternal())) return false;
    return ForcePushUsingScratch();
  }
  const bool ok = shard->Push();
  MakeBuffer(*shard);
  return ok;
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
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  RIEGELI_ASSERT_LE(pos(), shard_pos_limit_)
      << "Failed invariant of SplittingWriter: "
         "current position exceeds the shard limit";
  Writer* shard = shard_writer();
  if (shard_is_open(shard)) {
    SyncBuffer(*shard);
  } else {
    if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return false;
    shard = shard_writer();
  }
  Position length_to_write = shard_pos_limit_ - start_pos();
  bool ok;
  size_t length = src.size();
  if (ABSL_PREDICT_TRUE(length <= length_to_write)) {
    if (ABSL_PREDICT_FALSE(!shard->Write(std::forward<Src>(src)))) {
      ok = false;
    } else {
      move_start_pos(length);
      ok = true;
    }
  } else {
    SrcReader reader(&src);
    for (;;) {
      if (ABSL_PREDICT_FALSE(!reader.Copy(length_to_write, *shard))) {
        RIEGELI_ASSERT(!shard->healthy()) << "Reading failed";
        ok = false;
        break;
      }
      move_start_pos(length_to_write);
      length -= length_to_write;
      if (length == 0) {
        ok = true;
        break;
      }
      if (ABSL_PREDICT_FALSE(!CloseShardInternal())) return false;
      if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return false;
      shard = shard_writer();
      length_to_write = UnsignedMin(length, shard_pos_limit_ - start_pos());
    }
  }
  MakeBuffer(*shard);
  return ok;
}

bool SplittingWriterBase::WriteZerosBehindScratch(Position length) {
  RIEGELI_ASSERT_LT(UnsignedMin(available(), kMaxBytesToCopy), length)
      << "Failed precondition of PushableWriter::WriteZerosBehindScratch(): "
         "enough space available, use WriteZeros() instead";
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableWriter::WriteZerosBehindScratch(): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  RIEGELI_ASSERT_LE(pos(), shard_pos_limit_)
      << "Failed invariant of SplittingWriter: "
         "current position exceeds the shard limit";
  Writer* shard = shard_writer();
  if (shard_is_open(shard)) {
    SyncBuffer(*shard);
  } else {
    if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return false;
    shard = shard_writer();
  }
  bool ok;
  for (;;) {
    const Position length_to_write =
        UnsignedMin(length, shard_pos_limit_ - start_pos());
    if (ABSL_PREDICT_FALSE(!shard->WriteZeros(length_to_write))) {
      ok = false;
      break;
    }
    move_start_pos(length_to_write);
    length -= length_to_write;
    if (length == 0) {
      ok = true;
      break;
    }
    if (ABSL_PREDICT_FALSE(!CloseShardInternal())) return false;
    if (ABSL_PREDICT_FALSE(!OpenShardInternal())) return false;
    shard = shard_writer();
  }
  MakeBuffer(*shard);
  return ok;
}

bool SplittingWriterBase::FlushBehindScratch(FlushType flush_type) {
  RIEGELI_ASSERT(!scratch_used())
      << "Failed precondition of PushableWriter::FlushBehindScratch(): "
         "scratch used";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  RIEGELI_ASSERT_LE(pos(), shard_pos_limit_)
      << "Failed invariant of SplittingWriter: "
         "current position exceeds the shard limit";
  Writer* const shard = shard_writer();
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
