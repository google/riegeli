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

#include <cstring>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/status.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/string_reader.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/bytes/writer_utils.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/chunk_encoding/constants.h"
#include "riegeli/chunk_encoding/hash.h"
#include "riegeli/records/block.h"

namespace riegeli {

ChunkWriter::~ChunkWriter() {}

void DefaultChunkWriterBase::Initialize(Writer* dest, Position pos) {
  RIEGELI_ASSERT(dest != nullptr)
      << "Failed precondition of DefaultChunkWriter: null Writer pointer";
  if (ABSL_PREDICT_FALSE(!internal::IsPossibleChunkBoundary(pos))) {
    const Position length = internal::RemainingInBlock(pos);
    WriteZeros(dest, length);
    pos += length;
  }
  ChunkWriter::Initialize(pos);
  if (ABSL_PREDICT_FALSE(!dest->healthy())) Fail(*dest);
}

bool DefaultChunkWriterBase::WriteChunk(const Chunk& chunk) {
  RIEGELI_ASSERT_EQ(chunk.header.data_hash(), internal::Hash(chunk.data))
      << "Failed precondition of ChunkWriter::WriteChunk(): "
         "Wrong chunk data hash";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  // Matches `FutureRecordPosition::FutureChunkBegin::Resolve()`.
  Writer* const dest = dest_writer();
  StringReader<> header_reader(
      absl::string_view(chunk.header.bytes(), chunk.header.size()));
  ChainReader<> data_reader(&chunk.data);
  const Position chunk_begin = pos_;
  const Position chunk_end = internal::ChunkEnd(chunk.header, chunk_begin);
  if (ABSL_PREDICT_FALSE(
          !WriteSection(&header_reader, chunk_begin, chunk_end, dest))) {
    return false;
  }
  if (ABSL_PREDICT_FALSE(
          !WriteSection(&data_reader, chunk_begin, chunk_end, dest))) {
    return false;
  }
  if (ABSL_PREDICT_FALSE(!WritePadding(chunk_begin, chunk_end, dest))) {
    return false;
  }
  RIEGELI_ASSERT_EQ(pos_, chunk_end)
      << "Unexpected position after writing chunk";
  return true;
}

inline bool DefaultChunkWriterBase::WriteSection(Reader* src,
                                                 Position chunk_begin,
                                                 Position chunk_end,
                                                 Writer* dest) {
  Position size;
  if (!src->Size(&size)) {
    RIEGELI_ASSERT_UNREACHABLE()
        << "Getting section size failed: " << src->status();
  }
  RIEGELI_ASSERT_EQ(src->pos(), 0u) << "Non-zero section reader position";
  while (src->pos() < size) {
    if (internal::IsBlockBoundary(pos_)) {
      internal::BlockHeader block_header(IntCast<uint64_t>(pos_ - chunk_begin),
                                         IntCast<uint64_t>(chunk_end - pos_));
      if (ABSL_PREDICT_FALSE(!dest->Write(
              absl::string_view(block_header.bytes(), block_header.size())))) {
        return Fail(*dest);
      }
      pos_ += block_header.size();
    }
    const Position length =
        UnsignedMin(size - src->pos(), internal::RemainingInBlock(pos_));
    if (ABSL_PREDICT_FALSE(!src->CopyTo(dest, length))) return Fail(*dest);
    pos_ += length;
  }
  if (!src->Close()) {
    RIEGELI_ASSERT_UNREACHABLE()
        << "Closing section reader failed: " << src->status();
  }
  return true;
}

inline bool DefaultChunkWriterBase::WritePadding(Position chunk_begin,
                                                 Position chunk_end,
                                                 Writer* dest) {
  while (pos_ < chunk_end) {
    if (internal::IsBlockBoundary(pos_)) {
      internal::BlockHeader block_header(IntCast<uint64_t>(pos_ - chunk_begin),
                                         IntCast<uint64_t>(chunk_end - pos_));
      if (ABSL_PREDICT_FALSE(!dest->Write(
              absl::string_view(block_header.bytes(), block_header.size())))) {
        return Fail(*dest);
      }
      pos_ += block_header.size();
    }
    const Position length =
        UnsignedMin(chunk_end - pos_, internal::RemainingInBlock(pos_));
    if (ABSL_PREDICT_FALSE(!WriteZeros(dest, length))) return Fail(*dest);
    pos_ += length;
  }
  return true;
}

bool DefaultChunkWriterBase::PadToBlockBoundary() {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  // Matches `FutureRecordPosition::FutureChunkBegin::Resolve()`.
  size_t length = IntCast<size_t>(internal::RemainingInBlock(pos_));
  if (length == 0) return true;
  if (length < ChunkHeader::size()) {
    // Not enough space for a padding chunk in this block. Write one more block.
    length += size_t{internal::kUsableBlockSize};
  }
  length -= ChunkHeader::size();
  Chunk chunk;
  const absl::Span<char> buffer = chunk.data.AppendFixedBuffer(
      length, Chain::Options().set_size_hint(length));
  std::memset(buffer.data(), '\0', buffer.size());
  chunk.header = ChunkHeader(chunk.data, ChunkType::kPadding, 0, 0);
  return WriteChunk(chunk);
}

bool DefaultChunkWriterBase::Flush(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Writer* const dest = dest_writer();
  if (ABSL_PREDICT_FALSE(!dest->Flush(flush_type))) return Fail(*dest);
  return true;
}

}  // namespace riegeli
