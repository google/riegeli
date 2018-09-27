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

#include <stdint.h>
#include <cstring>
#include <future>
#include <limits>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/memory/memory.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/endian.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/records/block.h"

namespace riegeli {

std::string RecordPosition::Serialize() const {
  const uint64_t words[2] = {WriteBigEndian64(chunk_begin_),
                             WriteBigEndian64(record_index_)};
  return std::string(reinterpret_cast<const char*>(words), sizeof(words));
}

bool RecordPosition::Parse(absl::string_view serialized) {
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
  return out << pos.chunk_begin() << "/" << pos.record_index();
}

inline FutureRecordPosition::FutureChunkBegin::FutureChunkBegin(
    Position pos_before_chunks,
    std::vector<std::shared_future<ChunkHeader>> chunk_headers)
    : pos_before_chunks_(pos_before_chunks),
      chunk_headers_(std::move(chunk_headers)) {}

void FutureRecordPosition::FutureChunkBegin::Resolve() const {
  Position pos = pos_before_chunks_;
  for (const std::shared_future<ChunkHeader>& chunk_header : chunk_headers_) {
    pos = internal::ChunkEnd(chunk_header.get(), pos);
  }
  pos_before_chunks_ = pos;
  chunk_headers_ = std::vector<std::shared_future<ChunkHeader>>();
}

FutureRecordPosition::FutureRecordPosition(
    Position pos_before_chunks,
    std::vector<std::shared_future<ChunkHeader>> chunk_headers,
    uint64_t record_index)
    : future_chunk_begin_(
          chunk_headers.empty()
              ? nullptr
              : absl::make_unique<FutureChunkBegin>(pos_before_chunks,
                                                    std::move(chunk_headers))),
      chunk_begin_(pos_before_chunks),
      record_index_(record_index) {}

}  // namespace riegeli
