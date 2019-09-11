// Copyright 2017 Google LLC
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

#include "riegeli/records/record_position.h"

#include <stddef.h>
#include <stdint.h>

#include <cstring>
#include <future>
#include <limits>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/variant.h"
#include "riegeli/base/base.h"
#include "riegeli/base/endian.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/records/block.h"

namespace riegeli {

std::string RecordPosition::ToString() const {
  return absl::StrCat(chunk_begin_, "/", record_index_);
}

bool RecordPosition::FromString(absl::string_view serialized) {
  const size_t sep = serialized.find('/');
  if (ABSL_PREDICT_FALSE(sep == absl::string_view::npos)) return false;
  uint64_t chunk_begin;
  if (ABSL_PREDICT_FALSE(
          !absl::SimpleAtoi(serialized.substr(0, sep), &chunk_begin))) {
    return false;
  }
  uint64_t record_index;
  if (ABSL_PREDICT_FALSE(
          !absl::SimpleAtoi(serialized.substr(sep + 1), &record_index))) {
    return false;
  }
  if (ABSL_PREDICT_FALSE(record_index >
                         std::numeric_limits<uint64_t>::max() - chunk_begin)) {
    return false;
  }
  chunk_begin_ = chunk_begin;
  record_index_ = record_index;
  return true;
}

std::string RecordPosition::ToBytes() const {
  const uint64_t words[2] = {WriteBigEndian64(chunk_begin_),
                             WriteBigEndian64(record_index_)};
  return std::string(reinterpret_cast<const char*>(words), sizeof(words));
}

bool RecordPosition::FromBytes(absl::string_view serialized) {
  uint64_t words[2];
  if (ABSL_PREDICT_FALSE(serialized.size() != sizeof(words))) return false;
  std::memcpy(words, serialized.data(), sizeof(words));
  const uint64_t chunk_begin = ReadBigEndian64(words[0]);
  const uint64_t record_index = ReadBigEndian64(words[1]);
  if (ABSL_PREDICT_FALSE(record_index >
                         std::numeric_limits<uint64_t>::max() - chunk_begin)) {
    return false;
  }
  chunk_begin_ = chunk_begin;
  record_index_ = record_index;
  return true;
}

std::ostream& operator<<(std::ostream& out, RecordPosition pos) {
  return out << pos.ToString();
}

inline FutureRecordPosition::FutureChunkBegin::FutureChunkBegin(
    Position pos_before_chunks, std::vector<Action> actions)
    : pos_before_chunks_(pos_before_chunks), actions_(std::move(actions)) {}

void FutureRecordPosition::FutureChunkBegin::Resolve() const {
  struct Visitor {
    void operator()(const std::shared_future<ChunkHeader>& chunk_header) {
      // Matches DefaultChunkWriterBase::WriteChunk().
      pos = internal::ChunkEnd(chunk_header.get(), pos);
    }
    void operator()(const PadToBlockBoundary&) {
      // Matches DefaultChunkWriterBase::PadToBlockBoundary().
      Position length = internal::RemainingInBlock(pos);
      if (length == 0) return;
      if (length < ChunkHeader::size()) length += internal::kBlockSize;
      pos += length;
    }

    Position pos;
  };
  Visitor visitor{pos_before_chunks_};
  for (const Action& action : actions_) {
    absl::visit(visitor, action);
  }
  pos_before_chunks_ = visitor.pos;
  actions_ = std::vector<Action>();
}

FutureRecordPosition::FutureRecordPosition(Position pos_before_chunks,
                                           std::vector<Action> actions,
                                           uint64_t record_index)
    : future_chunk_begin_(actions.empty()
                              ? nullptr
                              : std::make_unique<FutureChunkBegin>(
                                    pos_before_chunks, std::move(actions))),
      chunk_begin_(pos_before_chunks),
      record_index_(record_index) {}

}  // namespace riegeli
