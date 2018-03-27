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
#include <utility>

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

DefaultChunkWriter::DefaultChunkWriter(std::unique_ptr<Writer> byte_writer)
    : DefaultChunkWriter(byte_writer.get()) {
  owned_byte_writer_ = std::move(byte_writer);
}

DefaultChunkWriter::DefaultChunkWriter(Writer* byte_writer)
    : ChunkWriter(State::kOpen),
      byte_writer_(RIEGELI_ASSERT_NOTNULL(byte_writer)) {
  pos_ = byte_writer_->pos();
}

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
  ChunkWriter::Done();
}

bool DefaultChunkWriter::WriteChunk(const Chunk& chunk) {
  RIEGELI_ASSERT_EQ(chunk.header.data_hash(), internal::Hash(chunk.data))
      << "Failed precondition of ChunkWriter::WriteChunk(): "
         "Wrong chunk data hash";
  StringReader header_reader(chunk.header.bytes(), chunk.header.size());
  ChainReader data_reader(&chunk.data);
  const Position chunk_begin = pos_;
  const Position chunk_end = internal::ChunkEnd(chunk.header, chunk_begin);
  RIEGELI_ASSERT_EQ(byte_writer_->pos(), chunk_begin)
      << "Unexpected position before writing chunk";
  if (RIEGELI_UNLIKELY(!WriteSection(&header_reader, chunk_begin, chunk_end))) {
    return false;
  }
  if (RIEGELI_UNLIKELY(!WriteSection(&data_reader, chunk_begin, chunk_end))) {
    return false;
  }
  if (RIEGELI_UNLIKELY(!WritePadding(chunk_begin, chunk_end))) {
    return false;
  }
  RIEGELI_ASSERT_EQ(byte_writer_->pos(), chunk_end)
      << "Unexpected position after writing chunk";
  pos_ = chunk_end;
  return true;
}

inline bool DefaultChunkWriter::WriteSection(Reader* src, Position chunk_begin,
                                             Position chunk_end) {
  while (src->Pull()) {
    if (internal::IsBlockBoundary(IntCast<uint64_t>(byte_writer_->pos()))) {
      internal::BlockHeader block_header(
          IntCast<uint64_t>(byte_writer_->pos() - chunk_begin),
          IntCast<uint64_t>(chunk_end - byte_writer_->pos()));
      if (RIEGELI_UNLIKELY(!byte_writer_->Write(
              absl::string_view(block_header.bytes(), block_header.size())))) {
        return Fail(*byte_writer_);
      }
    }
    if (!src->CopyTo(byte_writer_, internal::RemainingInBlock(IntCast<uint64_t>(
                                       byte_writer_->pos())))) {
      if (RIEGELI_LIKELY(byte_writer_->healthy())) break;
      return Fail(*byte_writer_);
    }
  }
  if (!src->Close()) {
    RIEGELI_ASSERT_UNREACHABLE()
        << "Closing section reader failed: " << src->Message();
  }
  return true;
}

inline bool DefaultChunkWriter::WritePadding(Position chunk_begin,
                                             Position chunk_end) {
  while (byte_writer_->pos() < chunk_end) {
    if (internal::IsBlockBoundary(IntCast<uint64_t>(byte_writer_->pos()))) {
      internal::BlockHeader block_header(
          IntCast<uint64_t>(byte_writer_->pos() - chunk_begin),
          IntCast<uint64_t>(chunk_end - byte_writer_->pos()));
      if (RIEGELI_UNLIKELY(!byte_writer_->Write(
              absl::string_view(block_header.bytes(), block_header.size())))) {
        return Fail(*byte_writer_);
      }
    }
    const Position slice_size = UnsignedMin(
        chunk_end - byte_writer_->pos(),
        internal::RemainingInBlock(IntCast<uint64_t>(byte_writer_->pos())));
    if (RIEGELI_UNLIKELY(!WriteZeros(byte_writer_, slice_size))) {
      return Fail(*byte_writer_);
    }
  }
  return true;
}

bool DefaultChunkWriter::Flush(FlushType flush_type) {
  if (RIEGELI_UNLIKELY(!byte_writer_->Flush(flush_type))) {
    if (byte_writer_->healthy()) return false;
    return Fail(*byte_writer_);
  }
  return true;
}

}  // namespace riegeli
