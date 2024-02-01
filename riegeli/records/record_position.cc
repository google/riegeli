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

#include <future>
#include <limits>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/variant.h"
#include "riegeli/base/intrusive_ref_count.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/string_reader.h"
#include "riegeli/bytes/string_writer.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/endian/endian_reading.h"
#include "riegeli/ordered_varint/ordered_varint_reading.h"
#include "riegeli/ordered_varint/ordered_varint_writing.h"
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
  std::string serialized;
  StringWriter<> writer(&serialized);
  WriteOrderedVarint64(chunk_begin_, writer);
  WriteOrderedVarint64(record_index_, writer);
  writer.Close();
  return serialized;
}

bool RecordPosition::FromBytes(absl::string_view serialized) {
  if (serialized.size() == 2 * sizeof(uint64_t)) {
    // Reading the old format is temporarily supported too.
    const uint64_t chunk_begin = ReadBigEndian64(&serialized[0]);
    const uint64_t record_index =
        ReadBigEndian64(&serialized[sizeof(uint64_t)]);
    if (ABSL_PREDICT_FALSE(record_index > std::numeric_limits<uint64_t>::max() -
                                              chunk_begin)) {
      return false;
    }
    chunk_begin_ = chunk_begin;
    record_index_ = record_index;
    return true;
  }
  StringReader<> reader(serialized);
  uint64_t chunk_begin, record_index;
  if (ABSL_PREDICT_FALSE(!ReadOrderedVarint64(reader, chunk_begin))) {
    return false;
  }
  if (ABSL_PREDICT_FALSE(!ReadOrderedVarint64(reader, record_index))) {
    return false;
  }
  if (ABSL_PREDICT_FALSE(record_index >
                         std::numeric_limits<uint64_t>::max() - chunk_begin)) {
    return false;
  }
  if (ABSL_PREDICT_FALSE(!reader.VerifyEndAndClose())) return false;
  chunk_begin_ = chunk_begin;
  record_index_ = record_index;
  return true;
}

void RecordPosition::OutputImpl(std::ostream& out) const { out << ToString(); }

namespace records_internal {

inline FutureChunkBegin::Unresolved::Unresolved(Position pos_before_chunks,
                                                std::vector<Action> actions)
    : pos_before_chunks_(pos_before_chunks), actions_(std::move(actions)) {}

void FutureChunkBegin::Unresolved::Resolve() const {
  struct Visitor {
    void operator()(const std::shared_future<ChunkHeader>& chunk_header) {
      // Matches `ChunkWriter::PosAfterWriteChunk()`.
      pos = records_internal::ChunkEnd(chunk_header.get(), pos);
    }
    void operator()(const PadToBlockBoundary&) {
      // Matches `ChunkWriter::PosAfterPadToBlockBoundary()`.
      Position length = records_internal::RemainingInBlock(pos);
      if (length == 0) return;
      if (length < ChunkHeader::size()) {
        // Not enough space for a padding chunk in this block. Write one more
        // block.
        length += records_internal::kBlockSize;
      }
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

FutureChunkBegin::FutureChunkBegin(Position pos_before_chunks,
                                   std::vector<Action> actions)
    : unresolved_(actions.empty() ? nullptr
                                  : MakeRefCounted<const Unresolved>(
                                        pos_before_chunks, std::move(actions))),
      resolved_(pos_before_chunks) {}

}  // namespace records_internal

}  // namespace riegeli
