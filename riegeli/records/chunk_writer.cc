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

DefaultChunkWriter::DefaultChunkWriter(std::unique_ptr<Writer> byte_writer,
                                       Options options)
    : DefaultChunkWriter(byte_writer.get(), options) {
  owned_byte_writer_ = std::move(byte_writer);
}

DefaultChunkWriter::DefaultChunkWriter(Writer* byte_writer, Options options)
    : ChunkWriter(State::kOpen),
      byte_writer_(RIEGELI_ASSERT_NOTNULL(byte_writer)) {
  pos_ = options.has_assumed_pos_ ? options.assumed_pos_ : byte_writer_->pos();
}

void DefaultChunkWriter::Done() {
  if (owned_byte_writer_ != nullptr) {
    if (ABSL_PREDICT_TRUE(healthy())) {
      if (ABSL_PREDICT_FALSE(!owned_byte_writer_->Close())) {
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
  if (ABSL_PREDICT_FALSE(
          !WriteSection(&header_reader, chunk_begin, chunk_end))) {
    return false;
  }
  if (ABSL_PREDICT_FALSE(!WriteSection(&data_reader, chunk_begin, chunk_end))) {
    return false;
  }
  if (ABSL_PREDICT_FALSE(!WritePadding(chunk_begin, chunk_end))) {
    return false;
  }
  RIEGELI_ASSERT_EQ(pos_, chunk_end)
      << "Unexpected position after writing chunk";
  return true;
}

inline bool DefaultChunkWriter::WriteSection(Reader* src, Position chunk_begin,
                                             Position chunk_end) {
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
      if (ABSL_PREDICT_FALSE(!byte_writer_->Write(
              absl::string_view(block_header.bytes(), block_header.size())))) {
        return Fail(*byte_writer_);
      }
      pos_ += block_header.size();
    }
    const Position length =
        UnsignedMin(size - src->pos(), internal::RemainingInBlock(pos_));
    if (ABSL_PREDICT_FALSE(!src->CopyTo(byte_writer_, length))) {
      return Fail(*byte_writer_);
    }
    pos_ += length;
  }
  if (!src->Close()) {
    RIEGELI_ASSERT_UNREACHABLE()
        << "Closing section reader failed: " << src->message();
  }
  return true;
}

inline bool DefaultChunkWriter::WritePadding(Position chunk_begin,
                                             Position chunk_end) {
  while (pos_ < chunk_end) {
    if (internal::IsBlockBoundary(pos_)) {
      internal::BlockHeader block_header(IntCast<uint64_t>(pos_ - chunk_begin),
                                         IntCast<uint64_t>(chunk_end - pos_));
      if (ABSL_PREDICT_FALSE(!byte_writer_->Write(
              absl::string_view(block_header.bytes(), block_header.size())))) {
        return Fail(*byte_writer_);
      }
      pos_ += block_header.size();
    }
    const Position length =
        UnsignedMin(chunk_end - pos_, internal::RemainingInBlock(pos_));
    if (ABSL_PREDICT_FALSE(!WriteZeros(byte_writer_, length))) {
      return Fail(*byte_writer_);
    }
    pos_ += length;
  }
  return true;
}

bool DefaultChunkWriter::Flush(FlushType flush_type) {
  if (ABSL_PREDICT_FALSE(!byte_writer_->Flush(flush_type))) {
    if (byte_writer_->healthy()) return false;
    return Fail(*byte_writer_);
  }
  return true;
}

}  // namespace riegeli
