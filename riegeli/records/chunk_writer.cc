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

#include "riegeli/records/chunk_writer.h"

#include <stddef.h>
#include <stdint.h>

#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/string_reader.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/chunk_encoding/constants.h"
#include "riegeli/chunk_encoding/hash.h"
#include "riegeli/records/block.h"

namespace riegeli {

ChunkWriter::~ChunkWriter() {}

void DefaultChunkWriterBase::Initialize(Writer* dest, Position pos) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of DefaultChunkWriter: null Writer pointer";
  if (ABSL_PREDICT_FALSE(!records_internal::IsPossibleChunkBoundary(pos))) {
    const Position length = records_internal::RemainingInBlock(pos);
    dest->WriteZeros(length);
    pos += length;
  }
  ChunkWriter::Initialize(pos);
  if (ABSL_PREDICT_FALSE(!dest->ok())) FailWithoutAnnotation(dest->status());
}

absl::Status DefaultChunkWriterBase::AnnotateStatusImpl(absl::Status status) {
  if (is_open()) {
    Writer& dest = *DestWriter();
    return dest.AnnotateStatus(std::move(status));
  }
  return status;
}

bool DefaultChunkWriterBase::WriteChunk(const Chunk& chunk) {
  RIEGELI_ASSERT_EQ(chunk.header.data_hash(),
                    chunk_encoding_internal::Hash(chunk.data))
      << "Failed precondition of ChunkWriter::WriteChunk(): "
         "Wrong chunk data hash";
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  // Matches `FutureRecordPosition::FutureChunkBegin::Resolve()`.
  Writer& dest = *DestWriter();
  StringReader<> header_reader(chunk.header.bytes(), chunk.header.size());
  ChainReader<> data_reader(&chunk.data);
  const Position chunk_begin = pos_;
  const Position chunk_end =
      records_internal::ChunkEnd(chunk.header, chunk_begin);
  if (ABSL_PREDICT_FALSE(
          !WriteSection(header_reader, chunk_begin, chunk_end, dest))) {
    return false;
  }
  if (ABSL_PREDICT_FALSE(
          !WriteSection(data_reader, chunk_begin, chunk_end, dest))) {
    return false;
  }
  if (ABSL_PREDICT_FALSE(!WritePadding(chunk_begin, chunk_end, dest))) {
    return false;
  }
  RIEGELI_ASSERT_EQ(pos_, chunk_end)
      << "Unexpected position after writing chunk";
  return true;
}

inline bool DefaultChunkWriterBase::WriteSection(Reader& src,
                                                 Position chunk_begin,
                                                 Position chunk_end,
                                                 Writer& dest) {
  const absl::optional<Position> size = src.Size();
  if (size == absl::nullopt) {
    RIEGELI_ASSERT_UNREACHABLE()
        << "Getting section size failed: " << src.status();
  }
  RIEGELI_ASSERT_EQ(src.pos(), 0u) << "Non-zero section reader position";
  while (src.pos() < *size) {
    if (records_internal::IsBlockBoundary(pos_)) {
      records_internal::BlockHeader block_header(
          IntCast<uint64_t>(pos_ - chunk_begin),
          IntCast<uint64_t>(chunk_end - pos_));
      if (ABSL_PREDICT_FALSE(!dest.Write(
              absl::string_view(block_header.bytes(), block_header.size())))) {
        return FailWithoutAnnotation(dest.status());
      }
      pos_ += block_header.size();
    }
    const Position length = UnsignedMin(
        *size - src.pos(), records_internal::RemainingInBlock(pos_));
    if (ABSL_PREDICT_FALSE(!src.Copy(length, dest))) {
      return FailWithoutAnnotation(dest.status());
    }
    pos_ += length;
  }
  if (!src.Close()) {
    RIEGELI_ASSERT_UNREACHABLE()
        << "Closing section reader failed: " << src.status();
  }
  return true;
}

inline bool DefaultChunkWriterBase::WritePadding(Position chunk_begin,
                                                 Position chunk_end,
                                                 Writer& dest) {
  while (pos_ < chunk_end) {
    if (records_internal::IsBlockBoundary(pos_)) {
      records_internal::BlockHeader block_header(
          IntCast<uint64_t>(pos_ - chunk_begin),
          IntCast<uint64_t>(chunk_end - pos_));
      if (ABSL_PREDICT_FALSE(!dest.Write(
              absl::string_view(block_header.bytes(), block_header.size())))) {
        return FailWithoutAnnotation(dest.status());
      }
      pos_ += block_header.size();
    }
    const Position length =
        UnsignedMin(chunk_end - pos_, records_internal::RemainingInBlock(pos_));
    if (ABSL_PREDICT_FALSE(!dest.WriteZeros(length))) {
      return FailWithoutAnnotation(dest.status());
    }
    pos_ += length;
  }
  return true;
}

bool DefaultChunkWriterBase::PadToBlockBoundary() {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  // Matches `FutureRecordPosition::FutureChunkBegin::Resolve()`.
  size_t length = IntCast<size_t>(records_internal::RemainingInBlock(pos_));
  if (length == 0) return true;
  if (length < ChunkHeader::size()) {
    // Not enough space for a padding chunk in this block. Write one more block.
    length += size_t{records_internal::kUsableBlockSize};
  }
  length -= ChunkHeader::size();
  Chunk chunk;
  chunk.data = ChainOfZeros(length);
  chunk.header = ChunkHeader(chunk.data, ChunkType::kPadding, 0, 0);
  return WriteChunk(chunk);
}

}  // namespace riegeli
