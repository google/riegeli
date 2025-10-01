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

#include <optional>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/assert.h"
#include "riegeli/base/byte_fill.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/types.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/string_reader.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/chunk_encoding/constants.h"
#include "riegeli/chunk_encoding/hash.h"
#include "riegeli/records/block.h"

namespace riegeli {

namespace records_internal {

Position PosAfterPadding(Position pos, Position padding) {
  if (padding <= 1) return pos;
  const Position remainder = pos % padding;
  if (remainder == 0) return pos;
  Position length = padding - remainder;
  while (length < ChunkHeader::size()) {
    // Not enough space for the chunk header.
    length += padding;
  }
  Position end_pos = pos + length;
  while (!records_internal::IsPossibleChunkBoundary(end_pos)) {
    // `end_pos` falls inside a block header.
    end_pos += padding;
  }
  return end_pos;
}

}  // namespace records_internal

ChunkWriter::~ChunkWriter() {}

void DefaultChunkWriterBase::Initialize(Writer* dest, Position pos) {
  RIEGELI_ASSERT_NE(dest, nullptr)
      << "Failed precondition of DefaultChunkWriter: null Writer pointer";
  if (ABSL_PREDICT_FALSE(!records_internal::IsPossibleChunkBoundary(pos))) {
    const Position length = records_internal::RemainingInBlock(pos);
    dest->Write(ByteFill(length));
    pos += length;
  }
  set_pos(pos);
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
  Writer& dest = *DestWriter();
  StringReader<> header_reader(chunk.header.bytes(), chunk.header.size());
  ChainReader<> data_reader(&chunk.data);
  const Position chunk_begin = pos();
  const Position chunk_end = PosAfterWriteChunk(chunk.header);
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
  RIEGELI_ASSERT_EQ(pos(), chunk_end)
      << "Unexpected position after writing chunk";
  return true;
}

inline bool DefaultChunkWriterBase::WriteSection(Reader& src,
                                                 Position chunk_begin,
                                                 Position chunk_end,
                                                 Writer& dest) {
  const std::optional<Position> size = src.Size();
  RIEGELI_ASSERT(size != std::nullopt) << src.status();
  RIEGELI_ASSERT_EQ(src.pos(), 0u) << "Non-zero section reader position";
  while (src.pos() < *size) {
    if (records_internal::IsBlockBoundary(pos())) {
      records_internal::BlockHeader block_header(
          IntCast<uint64_t>(pos() - chunk_begin),
          IntCast<uint64_t>(chunk_end - pos()));
      if (ABSL_PREDICT_FALSE(!dest.Write(
              absl::string_view(block_header.bytes(), block_header.size())))) {
        return FailWithoutAnnotation(dest.status());
      }
      move_pos(block_header.size());
    }
    const Position length = UnsignedMin(
        *size - src.pos(), records_internal::RemainingInBlock(pos()));
    if (ABSL_PREDICT_FALSE(!src.Copy(length, dest))) {
      return FailWithoutAnnotation(dest.status());
    }
    move_pos(length);
  }
  RIEGELI_EVAL_ASSERT(src.Close()) << src.status();
  return true;
}

inline bool DefaultChunkWriterBase::WritePadding(Position chunk_begin,
                                                 Position chunk_end,
                                                 Writer& dest) {
  while (pos() < chunk_end) {
    if (records_internal::IsBlockBoundary(pos())) {
      records_internal::BlockHeader block_header(
          IntCast<uint64_t>(pos() - chunk_begin),
          IntCast<uint64_t>(chunk_end - pos()));
      if (ABSL_PREDICT_FALSE(!dest.Write(
              absl::string_view(block_header.bytes(), block_header.size())))) {
        return FailWithoutAnnotation(dest.status());
      }
      move_pos(block_header.size());
    }
    const Position length = UnsignedMin(
        chunk_end - pos(), records_internal::RemainingInBlock(pos()));
    if (ABSL_PREDICT_FALSE(!dest.Write(ByteFill(length)))) {
      return FailWithoutAnnotation(dest.status());
    }
    move_pos(length);
  }
  return true;
}

bool DefaultChunkWriterBase::PadToBlockBoundary() {
  return WritePadding(records_internal::kBlockSize);
}

bool DefaultChunkWriterBase::WritePadding(Position padding) {
  if (ABSL_PREDICT_FALSE(!ok())) return false;
  const Position pos_after_pad =
      records_internal::PosAfterPadding(pos(), padding);
  if (pos_after_pad == pos()) return true;

  // Excludes the chunk header and any intervening block headers.
  const size_t padding_data_length =
      records_internal::DistanceWithoutOverhead(pos(), pos_after_pad) -
      ChunkHeader::size();
  Chunk chunk;
  chunk.data = Chain(ByteFill(padding_data_length));
  chunk.header = ChunkHeader(chunk.data, ChunkType::kPadding, 0, 0);
  return WriteChunk(chunk);
}

}  // namespace riegeli
