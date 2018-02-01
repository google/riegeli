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

#include "riegeli/records/chunk_reader.h"

#include <stddef.h>
#include <memory>
#include <new>
#include <utility>

#include "riegeli/base/assert.h"
#include "riegeli/base/base.h"
#include "riegeli/base/object.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/chunk_encoding/chunk.h"
#include "riegeli/chunk_encoding/hash.h"
#include "riegeli/records/block.h"

namespace riegeli {

inline void ChunkReader::Reading::Reset() {
  chunk.Reset();
  chunk_header_read = 0;
}

inline void ChunkReader::Recovering::Reset() {
  in_progress = false;
  chunk_begin_known = false;
  chunk_begin = 0;
}

ChunkReader::ChunkReader(std::unique_ptr<Reader> byte_reader, Options options)
    : ChunkReader(byte_reader.get(), options) {
  owned_byte_reader_ = std::move(byte_reader);
}

ChunkReader::ChunkReader(Reader* byte_reader, Options options)
    : Object(State::kOpen),
      byte_reader_(RIEGELI_ASSERT_NOTNULL(byte_reader)),
      skip_corruption_(options.skip_corruption_),
      pos_(byte_reader_->pos()),
      is_recovering_(internal::IsBlockBoundary(pos_)) {
  if (is_recovering_) {
    // pos_ is a block boundary and we will find the next chunk using this block
    // header (this can be a chunk whose boundary coincides with the block
    // boundary).
    new (&recovering_) Recovering();
    recovering_.in_progress = true;
  } else {
    new (&reading_) Reading();
  }
}

ChunkReader::~ChunkReader() {
  if (is_recovering_) {
    recovering_.~Recovering();
  } else {
    reading_.~Reading();
  }
}

void ChunkReader::Done() {
  if (!skip_corruption_ && RIEGELI_UNLIKELY(byte_reader_->pos() > pos_)) {
    Fail("Truncated Riegeli/records file");
  }
  if (owned_byte_reader_ != nullptr) {
    if (RIEGELI_LIKELY(healthy())) {
      if (RIEGELI_UNLIKELY(!owned_byte_reader_->Close())) {
        Fail(*owned_byte_reader_);
      }
    }
    owned_byte_reader_.reset();
  }
  byte_reader_ = nullptr;
  skip_corruption_ = false;
  pos_ = 0;
}

inline bool ChunkReader::ReadingFailed() {
  if (RIEGELI_LIKELY(byte_reader_->HopeForMore())) return false;
  if (byte_reader_->healthy()) {
    if (skip_corruption_) return false;
    return Fail("Truncated Riegeli/records file");
  }
  return Fail(*byte_reader_);
}

bool ChunkReader::ReadChunk(Chunk* chunk, Position* chunk_begin) {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  if (is_recovering_ && !Recover()) return false;
again:
  RIEGELI_ASSERT(!is_recovering_);

  if (reading_.chunk_header_read < reading_.chunk.header.size()) {
    if (RIEGELI_UNLIKELY(!ReadChunkHeader())) {
      if (is_recovering_ && Recover()) goto again;
      return false;
    }
  }

  while (reading_.chunk.data.size() < reading_.chunk.header.data_size()) {
    if (RIEGELI_UNLIKELY(!ReadBlockHeader())) {
      if (is_recovering_ && Recover()) goto again;
      return false;
    }
    if (RIEGELI_UNLIKELY(!byte_reader_->Read(
            &reading_.chunk.data,
            static_cast<size_t>(UnsignedMin(
                reading_.chunk.header.data_size() - reading_.chunk.data.size(),
                internal::RemainingInBlock(byte_reader_->pos())))))) {
      return ReadingFailed();
    }
  }

  const Position chunk_end = internal::ChunkEnd(reading_.chunk.header, pos_);
  if (RIEGELI_UNLIKELY(!byte_reader_->Seek(chunk_end))) return ReadingFailed();

  if (RIEGELI_UNLIKELY(internal::Hash(reading_.chunk.data) !=
                       reading_.chunk.header.data_hash())) {
    pos_ = chunk_end;
    // Reading, not Recovering, because only chunk data were corrupted; chunk
    // header had a correct hash and thus the next chunk is believed to be
    // present after this chunk.
    PrepareForReading();
    if (skip_corruption_) goto again;
    return Fail("Corrupted Riegeli/records file");
  }

  if (chunk_begin != nullptr) *chunk_begin = pos_;
  *chunk = std::move(reading_.chunk);
  pos_ = chunk_end;
  PrepareForReading();
  return true;
}

inline bool ChunkReader::ReadChunkHeader() {
  RIEGELI_ASSERT(healthy());
  RIEGELI_ASSERT(!is_recovering_);
  RIEGELI_ASSERT_LT(reading_.chunk_header_read, reading_.chunk.header.size());

  if (RIEGELI_UNLIKELY(!internal::IsPossibleChunkBoundary(pos_))) {
    PrepareForRecovering();
    return false;
  }
  if (byte_reader_->pos() == pos_ && !byte_reader_->Pull()) {
    // byte_reader_ ends between chunks. Any other place implies that the data
    // are truncated.
    if (byte_reader_->healthy()) return false;
    return Fail(*byte_reader_);
  }

  do {
    if (RIEGELI_UNLIKELY(!ReadBlockHeader())) return false;
    const size_t length =
        UnsignedMin(reading_.chunk.header.size() - reading_.chunk_header_read,
                    internal::RemainingInBlock(byte_reader_->pos()));
    const Position pos_before = byte_reader_->pos();
    if (RIEGELI_UNLIKELY(!byte_reader_->Read(
            reading_.chunk.header.bytes() + reading_.chunk_header_read,
            length))) {
      reading_.chunk_header_read += byte_reader_->pos() - pos_before;
      return ReadingFailed();
    }
    reading_.chunk_header_read += length;
  } while (reading_.chunk_header_read < reading_.chunk.header.size());

  if (RIEGELI_UNLIKELY(reading_.chunk.header.computed_header_hash() !=
                       reading_.chunk.header.stored_header_hash())) {
    PrepareForRecovering();
    return false;
  }
  return true;
}

inline bool ChunkReader::ReadBlockHeader() {
  const size_t remaining_length =
      internal::RemainingInBlockHeader(byte_reader_->pos());
  if (remaining_length == 0) return true;
  if (RIEGELI_UNLIKELY(!byte_reader_->Read(
          block_header_.bytes() + block_header_.size() - remaining_length,
          remaining_length))) {
    return ReadingFailed();
  }
  if (RIEGELI_UNLIKELY(block_header_.computed_header_hash() !=
                       block_header_.stored_header_hash())) {
    pos_ = byte_reader_->pos();
    PrepareForRecovering();
    return false;
  }
  return true;
}

inline void ChunkReader::PrepareForReading() {
  if (is_recovering_) {
    recovering_.~Recovering();
    is_recovering_ = false;
    new (&reading_) Reading();
  } else {
    reading_.Reset();
  }
}

inline void ChunkReader::PrepareForFindingChunk() {
  PrepareForRecovering();
  recovering_.in_progress = true;
}

inline void ChunkReader::PrepareForRecovering() {
  if (is_recovering_) {
    recovering_.Reset();
  } else {
    reading_.~Reading();
    is_recovering_ = true;
    new (&recovering_) Recovering();
  }
}

inline bool ChunkReader::Recover() {
  RIEGELI_ASSERT(healthy());
  RIEGELI_ASSERT(is_recovering_);
  if (!recovering_.in_progress) {
  again:
    pos_ += internal::RemainingInBlock(pos_);
    if (!skip_corruption_) return Fail("Corrupted Riegeli/records file");
    recovering_.in_progress = true;
  }
  RIEGELI_ASSERT(internal::IsBlockBoundary(pos_));

  if (byte_reader_->pos() < pos_) {
    if (RIEGELI_UNLIKELY(!byte_reader_->Seek(pos_))) {
      if (byte_reader_->healthy()) return false;
      return Fail(*byte_reader_);
    }
  }

  if (!recovering_.chunk_begin_known) {
    if (RIEGELI_UNLIKELY(!ReadBlockHeader())) {
      if (!recovering_.in_progress) {
        // Block header is invalid and ReadBlockHeader() started new recovery.
        // To detect this, check whether recovery is no longer in progress;
        // it would be incorrect to check just is_recovering_ because the mode
        // was already Recovering before.
        goto again;
      }
      return false;
    }
    if (block_header_.previous_chunk() == 0) {
      // Chunk boundary coincides with block boundary. Use the chunk which
      // begins here instead of the next chunk.
      PrepareForReading();
      return true;
    }
    recovering_.chunk_begin_known = true;
    recovering_.chunk_begin = pos_ + block_header_.next_chunk();
  }

  if (RIEGELI_UNLIKELY(!byte_reader_->Seek(recovering_.chunk_begin))) {
    if (byte_reader_->healthy()) return false;
    return Fail(*byte_reader_);
  }
  pos_ = recovering_.chunk_begin;
  PrepareForReading();
  return true;
}

bool ChunkReader::Seek(Position new_pos) {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  if (RIEGELI_UNLIKELY(!byte_reader_->Seek(new_pos))) {
    if (byte_reader_->healthy()) {
      pos_ = byte_reader_->pos();
      PrepareForRecovering();
      return false;
    }
    return Fail(*byte_reader_);
  }
  pos_ = new_pos;
  if (internal::IsBlockBoundary(pos_)) {
    // pos_ is a block boundary and we will find the next chunk using this block
    // header (this can be a chunk whose boundary coincides with the block
    // boundary).
    PrepareForFindingChunk();
    return Recover();
  }
  PrepareForReading();
  return true;
}

bool ChunkReader::SeekToChunkContaining(Position new_pos) {
  return SeekToChunk(new_pos, true);
}

bool ChunkReader::SeekToChunkAfter(Position new_pos) {
  return SeekToChunk(new_pos, false);
}

inline bool ChunkReader::SeekToChunk(Position new_pos, bool containing) {
  if (RIEGELI_UNLIKELY(!healthy())) return false;
  Position block_begin = new_pos;
  if (new_pos % internal::kBlockSize() != 0) {
    block_begin -= new_pos % internal::kBlockSize();
  } else if (new_pos != 0) {
    Position file_size;
    if (RIEGELI_UNLIKELY(!byte_reader_->Size(&file_size))) return false;
    if (file_size == new_pos) {
      // new_pos is a block boundary and the file ends there. Start with the
      // previous block to be able to distinguish whether new_pos is a valid
      // chunk boundary or the file is truncated.
      block_begin -= internal::kBlockSize();
    }
  }

  Position chunk_begin;
  if (!is_recovering_ && pos_ <= new_pos) {
    // The current chunk begins at or before new_pos. If it also ends at or
    // after block_begin, it is better to start searching from the current
    // position than to seek back to block_begin.
    if (pos_ == new_pos) return true;
    if (reading_.chunk_header_read < reading_.chunk.header.size()) {
      if (RIEGELI_UNLIKELY(!ReadChunkHeader())) {
        return is_recovering_ && Recover();
      }
    }
    const Position chunk_end = internal::ChunkEnd(reading_.chunk.header, pos_);
    if (chunk_end < block_begin) {
      // The current chunk ends too early. Skip to block_begin.
      goto read_block_header;
    }
    if (containing && pos_ + reading_.chunk.header.num_records() > new_pos) {
      return true;
    }
    chunk_begin = chunk_end;
  } else {
  read_block_header:
    pos_ = block_begin;
    PrepareForFindingChunk();
    if (RIEGELI_UNLIKELY(!byte_reader_->Seek(block_begin))) {
      if (byte_reader_->healthy()) return false;
      return Fail(*byte_reader_);
    }
    if (RIEGELI_UNLIKELY(!ReadBlockHeader())) {
      if (!recovering_.in_progress) {
        // Block header is invalid and ReadBlockHeader() started new recovery.
        // To detect this, check whether recovery is no longer in progress;
        // it would be incorrect to check just is_recovering_ because the mode
        // was already Recovering before.
        return Recover();
      }
      return false;
    }
    if (block_header_.previous_chunk() == 0) {
      // Chunk boundary coincides with block boundary. The current position is
      // already before the chunk header; start searching from this chunk,
      // skipping seeking back and reading the block header again.
      goto check_current_chunk;
    }
    chunk_begin = block_begin + block_header_.next_chunk();
    if (containing && chunk_begin > new_pos) {
      // new_pos is inside the chunk which contains this block boundary, so
      // start the search from this chunk instead of the next chunk.
      if (RIEGELI_UNLIKELY(block_header_.previous_chunk() > block_begin)) {
        // Invalid block header: previous chunk begins before the beginning of
        // the file.
        return Recover();
      }
      chunk_begin = block_begin - block_header_.previous_chunk();
    }
  }

  for (;;) {
    if (RIEGELI_UNLIKELY(!byte_reader_->Seek(chunk_begin))) {
      if (byte_reader_->healthy()) return false;
      return Fail(*byte_reader_);
    }
    pos_ = chunk_begin;
  check_current_chunk:
    PrepareForReading();
    if (pos_ >= new_pos) return true;
    if (RIEGELI_UNLIKELY(!ReadChunkHeader())) {
      return is_recovering_ && Recover();
    }
    if (containing && pos_ + reading_.chunk.header.num_records() > new_pos) {
      return true;
    }
    chunk_begin = internal::ChunkEnd(reading_.chunk.header, pos_);
  }
}

}  // namespace riegeli
