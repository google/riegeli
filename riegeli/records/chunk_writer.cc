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

#include <stdint.h>
#include <memory>

#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/string_reader.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/bytes/writer_utils.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/chunk_encoding/hash.h"
#include "riegeli/records/block.h"

namespace riegeli {

ChunkWriter::~ChunkWriter() {}

bool DefaultChunkWriterBase::WriteChunk(const Chunk& chunk) {
  RIEGELI_ASSERT_EQ(chunk.header.data_hash(), internal::Hash(chunk.data))
      << "Failed precondition of ChunkWriter::WriteChunk(): "
         "Wrong chunk data hash";
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
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
        << "Getting section size failed: " << src->message();
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
    if (ABSL_PREDICT_FALSE(!src->CopyTo(dest, length))) {
      return Fail(*dest);
    }
    pos_ += length;
  }
  if (!src->Close()) {
    RIEGELI_ASSERT_UNREACHABLE()
        << "Closing section reader failed: " << src->message();
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
    if (ABSL_PREDICT_FALSE(!WriteZeros(dest, length))) {
      return Fail(*dest);
    }
    pos_ += length;
  }
  return true;
}

bool DefaultChunkWriterBase::Flush(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  Writer* const dest = dest_writer();
  if (ABSL_PREDICT_FALSE(!dest->Flush(flush_type))) {
    if (ABSL_PREDICT_FALSE(!dest->healthy())) return Fail(*dest);
    return false;
  }
  return true;
}

template class DefaultChunkWriter<Writer*>;
template class DefaultChunkWriter<std::unique_ptr<Writer>>;

}  // namespace riegeli
