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

#include <memory>
#include <utility>

#include "riegeli/base/assert.h"
#include "riegeli/base/base.h"
#include "riegeli/base/string_view.h"
#include "riegeli/bytes/chain_reader.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/string_reader.h"
#include "riegeli/bytes/writer.h"
#include "riegeli/bytes/writer_utils.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/chunk_encoding/hash.h"
#include "riegeli/records/block.h"

namespace riegeli {

ChunkWriter::~ChunkWriter() = default;

DefaultChunkWriter::DefaultChunkWriter(std::unique_ptr<Writer> byte_writer)
    : DefaultChunkWriter(byte_writer.get()) {
  owned_byte_writer_ = std::move(byte_writer);
}

DefaultChunkWriter::DefaultChunkWriter(Writer* byte_writer)
    : byte_writer_(RIEGELI_ASSERT_NOTNULL(byte_writer)) {}

DefaultChunkWriter::~DefaultChunkWriter() = default;

void DefaultChunkWriter::Done() {
  if (owned_byte_writer_ != nullptr) {
    if (RIEGELI_LIKELY(healthy())) {
      if (RIEGELI_UNLIKELY(!owned_byte_writer_->Close())) {
        Fail(*owned_byte_writer_);
      }
    }
    owned_byte_writer_.reset();
  }
  byte_writer_ = nullptr;
}

bool DefaultChunkWriter::WriteChunk(const Chunk& chunk) {
  RIEGELI_ASSERT_EQ(chunk.header.data_hash(), internal::Hash(chunk.data));
  StringReader header_reader(chunk.header.bytes(), chunk.header.size());
  ChainReader data_reader(&chunk.data);
  const Position chunk_begin = byte_writer_->pos();
  const Position chunk_end = internal::ChunkEnd(chunk.header, chunk_begin);
  if (RIEGELI_UNLIKELY(!WriteSection(&header_reader, chunk_begin, chunk_end))) {
    return false;
  }
  if (RIEGELI_UNLIKELY(!WriteSection(&data_reader, chunk_begin, chunk_end))) {
    return false;
  }
  if (RIEGELI_UNLIKELY(!WritePadding(chunk_begin, chunk_end))) {
    return false;
  }
  RIEGELI_ASSERT_EQ(byte_writer_->pos(), chunk_end);
  return true;
}

inline bool DefaultChunkWriter::WriteSection(Reader* src, Position chunk_begin,
                                             Position chunk_end) {
  while (src->Pull()) {
    if (internal::IsBlockBoundary(byte_writer_->pos())) {
      internal::BlockHeader block_header(byte_writer_->pos() - chunk_begin,
                                         chunk_end - byte_writer_->pos());
      if (RIEGELI_UNLIKELY(!byte_writer_->Write(
              string_view(block_header.bytes(), block_header.size())))) {
        return Fail(*byte_writer_);
      }
    }
    if (!src->CopyTo(byte_writer_,
                     internal::RemainingInBlock(byte_writer_->pos()))) {
      if (RIEGELI_LIKELY(byte_writer_->healthy())) break;
      return Fail(*byte_writer_);
    }
  }
  return src->Close();
}

inline bool DefaultChunkWriter::WritePadding(Position chunk_begin,
                                             Position chunk_end) {
  while (byte_writer_->pos() < chunk_end) {
    if (internal::IsBlockBoundary(byte_writer_->pos())) {
      internal::BlockHeader block_header(byte_writer_->pos() - chunk_begin,
                                         chunk_end - byte_writer_->pos());
      if (RIEGELI_UNLIKELY(!byte_writer_->Write(
              string_view(block_header.bytes(), block_header.size())))) {
        return Fail(*byte_writer_);
      }
    }
    const Position slice_size =
        UnsignedMin(chunk_end - byte_writer_->pos(),
                    internal::RemainingInBlock(byte_writer_->pos()));
    if (RIEGELI_UNLIKELY(!WriteZeros(byte_writer_, slice_size))) {
      return Fail(*byte_writer_);
    }
  }
  return true;
}

bool DefaultChunkWriter::Flush(FlushType flush_type) {
  return byte_writer_->Flush(flush_type);
}

Position DefaultChunkWriter::pos() const { return byte_writer_->pos(); }

}  // namespace riegeli
