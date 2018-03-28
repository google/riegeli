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

#include "absl/base/optimization.h"
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
  corrupted = false;
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
      skip_errors_(options.skip_errors_),
      pos_(byte_reader_->pos()),
      is_recovering_(
          internal::IsBlockBoundary(pos_) ||
          ABSL_PREDICT_FALSE(!internal::IsPossibleChunkBoundary(pos_))) {
  if (is_recovering_) {
    // pos_ is a block boundary and we will find the next chunk using this block
    // header (this can be a chunk whose boundary coincides with the block
    // boundary).
    new (&recovering_) Recovering();
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
  if (ABSL_PREDICT_TRUE(healthy()) &&
      ABSL_PREDICT_FALSE(current_chunk_is_incomplete_)) {
    // Current chunk is incomplete. This was not reported yet as a truncated
    // file because HopeForMore() is true, so the caller could have retried
    // reading a growing file. It turned out that the caller did not retry
    // reading, so Close() reports the incomplete chunk as corruption.
    if (!skip_errors_) {
      Fail("Truncated Riegeli/records file");
    } else {
      PrepareForRecovering();
      recovering_.corrupted = true;
      SeekOverCorruption(byte_reader_->pos());
    }
  }
  if (owned_byte_reader_ != nullptr) {
    if (ABSL_PREDICT_TRUE(healthy())) {
      if (ABSL_PREDICT_FALSE(!owned_byte_reader_->Close())) {
        Fail(*owned_byte_reader_);
      }
    }
    owned_byte_reader_.reset();
  }
  byte_reader_ = nullptr;
  skip_errors_ = false;
  pos_ = 0;
  current_chunk_is_incomplete_ = false;
  // bytes_skipped_ is not cleared.
  if (is_recovering_) {
    recovering_ = Recovering();
  } else {
    reading_ = Reading();
  }
}

inline bool ChunkReader::ReadingFailed() {
  if (ABSL_PREDICT_TRUE(byte_reader_->HopeForMore())) {
    current_chunk_is_incomplete_ = true;
    return false;
  }
  if (byte_reader_->healthy()) {
    if (!skip_errors_) return Fail("Truncated Riegeli/records file");
    PrepareForRecovering();
    recovering_.corrupted = true;
    SeekOverCorruption(byte_reader_->pos());
    return false;
  }
  return Fail(*byte_reader_);
}

bool ChunkReader::CheckFileFormat() {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  current_chunk_is_incomplete_ = false;
  if (is_recovering_ && !Recover()) return false;
again:
  RIEGELI_ASSERT(!is_recovering_)
      << "ChunkReader::Recover() did not complete recovering";

  if (reading_.chunk_header_read < reading_.chunk.header.size()) {
    if (ABSL_PREDICT_FALSE(!ReadChunkHeader())) {
      if (is_recovering_ && Recover()) goto again;
      return false;
    }
  }

  if (pos_ == 0) {
    // Verify file signature.
    if (ABSL_PREDICT_FALSE(reading_.chunk.header.data_size() != 0 ||
                           reading_.chunk.header.num_records() != 0 ||
                           reading_.chunk.header.decoded_data_size() != 0)) {
      return Fail("Invalid Riegeli/records file: missing file signature");
    }
  }
  return true;
}

bool ChunkReader::ReadChunk(Chunk* chunk, Position* chunk_begin) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  current_chunk_is_incomplete_ = false;
  if (is_recovering_ && !Recover()) return false;
again:
  RIEGELI_ASSERT(!is_recovering_)
      << "ChunkReader::Recover() did not complete recovering";

  if (reading_.chunk_header_read < reading_.chunk.header.size()) {
    if (ABSL_PREDICT_FALSE(!ReadChunkHeader())) {
      if (is_recovering_ && Recover()) goto again;
      return false;
    }
  }

  while (reading_.chunk.data.size() < reading_.chunk.header.data_size()) {
    if (ABSL_PREDICT_FALSE(!ReadBlockHeader())) {
      if (is_recovering_ && Recover()) goto again;
      return false;
    }
    if (ABSL_PREDICT_FALSE(!byte_reader_->Read(
            &reading_.chunk.data,
            IntCast<size_t>(UnsignedMin(
                reading_.chunk.header.data_size() - reading_.chunk.data.size(),
                internal::RemainingInBlock(byte_reader_->pos())))))) {
      return ReadingFailed();
    }
  }

  const Position chunk_end = internal::ChunkEnd(reading_.chunk.header, pos_);
  if (ABSL_PREDICT_FALSE(!byte_reader_->Seek(chunk_end))) {
    return ReadingFailed();
  }

  if (ABSL_PREDICT_FALSE(internal::Hash(reading_.chunk.data) !=
                         reading_.chunk.header.data_hash())) {
    if (!skip_errors_) return Fail("Corrupted Riegeli/records file");
    PrepareForRecovering();
    recovering_.corrupted = true;
    SeekOverCorruption(chunk_end);
    // Chunk data were corrupted but chunk header had a correct hash. Recovery
    // using block headers is not needed, the next chunk is believed to be
    // present after this chunk.
    PrepareForReading();
    goto again;
  }

  if (chunk_begin != nullptr) *chunk_begin = pos_;
  *chunk = std::move(reading_.chunk);
  pos_ = chunk_end;
  PrepareForReading();
  return true;
}

inline bool ChunkReader::ReadChunkHeader() {
  RIEGELI_ASSERT(healthy())
      << "Failed precondigion of ChunkReader::ReadChunkReader(): "
         "object unhealthy";
  RIEGELI_ASSERT(!is_recovering_)
      << "Failed precondition of ChunkReader::ReadChunkHeader(): recovering";
  RIEGELI_ASSERT(internal::IsPossibleChunkBoundary(pos_))
      << "Failed invariant of ChunkReader: invalid chunk boundary";
  RIEGELI_ASSERT_LT(reading_.chunk_header_read, reading_.chunk.header.size())
      << "Failed precondition of ChunkReader::ReadChunkHeader(): "
         "chunk header already read";
  if (byte_reader_->pos() == pos_ &&
      ABSL_PREDICT_FALSE(!byte_reader_->Pull())) {
    if (ABSL_PREDICT_TRUE(byte_reader_->healthy())) {
      // The file ends between chunks. This is a valid end of file.
      //
      // If the file ends at any other place than between chunks, the file is
      // truncated, hence other reading failures call ReadingFailed() which
      // interprets the end of file as an incomplete chunk.
      return false;
    }
    return Fail(*byte_reader_);
  }

  do {
    if (ABSL_PREDICT_FALSE(!ReadBlockHeader())) return false;
    const size_t length =
        UnsignedMin(reading_.chunk.header.size() - reading_.chunk_header_read,
                    internal::RemainingInBlock(byte_reader_->pos()));
    const Position pos_before = byte_reader_->pos();
    if (ABSL_PREDICT_FALSE(!byte_reader_->Read(
            reading_.chunk.header.bytes() + reading_.chunk_header_read,
            length))) {
      RIEGELI_ASSERT_GE(byte_reader_->pos(), pos_before)
          << "Reader::Read(char*) decreased pos()";
      const Position length_read = byte_reader_->pos() - pos_before;
      RIEGELI_ASSERT_LE(length_read, length)
          << "Reader::Read(char*) read more than requested";
      reading_.chunk_header_read += IntCast<size_t>(length_read);
      return ReadingFailed();
    }
    reading_.chunk_header_read += length;
  } while (reading_.chunk_header_read < reading_.chunk.header.size());

  if (ABSL_PREDICT_FALSE(reading_.chunk.header.computed_header_hash() !=
                         reading_.chunk.header.stored_header_hash())) {
    if (!skip_errors_) return Fail("Corrupted Riegeli/records file");
    PrepareForRecovering();
    recovering_.corrupted = true;
    return false;
  }
  return true;
}

inline bool ChunkReader::ReadBlockHeader() {
  const size_t remaining_length =
      internal::RemainingInBlockHeader(byte_reader_->pos());
  if (remaining_length == 0) return true;
  if (ABSL_PREDICT_FALSE(!byte_reader_->Read(
          block_header_.bytes() + block_header_.size() - remaining_length,
          remaining_length))) {
    return ReadingFailed();
  }
  if (ABSL_PREDICT_FALSE(block_header_.computed_header_hash() !=
                         block_header_.stored_header_hash())) {
    if (!skip_errors_) return Fail("Corrupted Riegeli/records file");
    PrepareForRecovering();
    recovering_.corrupted = true;
    SeekOverCorruption(byte_reader_->pos());
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

inline void ChunkReader::PrepareForRecovering() {
  if (is_recovering_) {
    recovering_.Reset();
  } else {
    reading_.~Reading();
    is_recovering_ = true;
    new (&recovering_) Recovering();
  }
}

inline void ChunkReader::SeekOverCorruption(Position new_pos) {
  RIEGELI_ASSERT(is_recovering_)
      << "Failed precondition of ChunkReader::SeekOverCorruption(): "
         "not recovering";
  RIEGELI_ASSERT_GE(new_pos, pos_)
      << "Failed precondition of ChunkReader::SeekOverCorruption(): "
         "seeking backwards";
  if (recovering_.corrupted) {
    bytes_skipped_ = SaturatingAdd(bytes_skipped_, new_pos - pos_);
  }
  pos_ = new_pos;
}

inline bool ChunkReader::Recover() {
  RIEGELI_ASSERT(healthy())
      << "Failed precondition of ChunkReader::Recover(): Object unhealthy";
  RIEGELI_ASSERT(is_recovering_)
      << "Failed precondition of ChunkReader::Recover(): not recovering";
again:
  const Position block_begin = pos_ + internal::RemainingInBlock(pos_);
  if (ABSL_PREDICT_FALSE(!byte_reader_->Seek(block_begin))) {
    if (ABSL_PREDICT_TRUE(byte_reader_->healthy())) {
      SeekOverCorruption(byte_reader_->pos());
      return false;
    }
    return Fail(*byte_reader_);
  }
  SeekOverCorruption(block_begin);

  if (!recovering_.chunk_begin_known) {
    if (ABSL_PREDICT_FALSE(!ReadBlockHeader())) {
      if (pos_ != block_begin) {
        // Block header is invalid and ReadBlockHeader() started new recovery.
        // To detect this, check whether pos_ changed; it would be incorrect to
        // check just is_recovering_ because it was already true before.
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
    recovering_.chunk_begin = block_begin + block_header_.next_chunk();
    if (ABSL_PREDICT_FALSE(
            !internal::IsPossibleChunkBoundary(recovering_.chunk_begin))) {
      if (!InvalidChunkBoundary()) return false;
      goto again;
    }
  }

  if (ABSL_PREDICT_FALSE(!byte_reader_->Seek(recovering_.chunk_begin))) {
    if (ABSL_PREDICT_TRUE(byte_reader_->healthy())) {
      SeekOverCorruption(byte_reader_->pos());
      return false;
    }
    return Fail(*byte_reader_);
  }
  SeekOverCorruption(recovering_.chunk_begin);
  PrepareForReading();
  return true;
}

inline bool ChunkReader::InvalidChunkBoundary() {
  if (!skip_errors_) {
    return Fail("Invalid Riegeli/records file: invalid chunk boundary");
  }
  PrepareForRecovering();
  recovering_.corrupted = true;
  SeekOverCorruption(byte_reader_->pos());
  return true;
}

bool ChunkReader::Seek(Position new_pos) {
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  current_chunk_is_incomplete_ = false;
  if (ABSL_PREDICT_FALSE(!byte_reader_->Seek(new_pos))) {
    if (ABSL_PREDICT_TRUE(byte_reader_->healthy())) {
      PrepareForRecovering();
      pos_ = byte_reader_->pos();
      return false;
    }
    return Fail(*byte_reader_);
  }
  pos_ = new_pos;
  if (internal::IsBlockBoundary(pos_) ||
      ABSL_PREDICT_FALSE(!internal::IsPossibleChunkBoundary(pos_))) {
    // pos_ is a block boundary and we will find the next chunk using this block
    // header (this can be a chunk whose boundary coincides with the block
    // boundary).
    PrepareForRecovering();
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
  if (ABSL_PREDICT_FALSE(!healthy())) return false;
  current_chunk_is_incomplete_ = false;
  const Position block_begin = new_pos - new_pos % internal::kBlockSize();
  Position chunk_begin;
  if (!is_recovering_ && pos_ <= new_pos) {
    // The current chunk begins at or before new_pos. If it also ends at or
    // after block_begin, it is better to start searching from the current
    // position than to seek back to block_begin.
    if (pos_ == new_pos) return true;
    if (reading_.chunk_header_read < reading_.chunk.header.size()) {
      if (ABSL_PREDICT_FALSE(!ReadChunkHeader())) {
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
    PrepareForRecovering();
    if (ABSL_PREDICT_FALSE(!byte_reader_->Seek(block_begin))) {
      if (ABSL_PREDICT_TRUE(byte_reader_->healthy())) return false;
      return Fail(*byte_reader_);
    }
    if (ABSL_PREDICT_FALSE(!ReadBlockHeader())) {
      if (pos_ != block_begin) {
        // Block header is invalid and ReadBlockHeader() started new recovery.
        // To detect this, check whether pos_ changed; it would be incorrect to
        // check just is_recovering_ because it was already true before.
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
      if (ABSL_PREDICT_FALSE(block_header_.previous_chunk() > block_begin)) {
        if (!InvalidChunkBoundary()) return false;
        return Recover();
      }
      chunk_begin = block_begin - block_header_.previous_chunk();
    }
    if (ABSL_PREDICT_FALSE(!internal::IsPossibleChunkBoundary(chunk_begin))) {
      if (!InvalidChunkBoundary()) return false;
      return Recover();
    }
  }

  for (;;) {
    if (ABSL_PREDICT_FALSE(!byte_reader_->Seek(chunk_begin))) {
      if (ABSL_PREDICT_TRUE(byte_reader_->healthy())) return false;
      return Fail(*byte_reader_);
    }
    pos_ = chunk_begin;
  check_current_chunk:
    PrepareForReading();
    if (pos_ >= new_pos) return true;
    if (ABSL_PREDICT_FALSE(!ReadChunkHeader())) {
      return is_recovering_ && Recover();
    }
    if (containing && pos_ + reading_.chunk.header.num_records() > new_pos) {
      return true;
    }
    chunk_begin = internal::ChunkEnd(reading_.chunk.header, pos_);
  }
}

}  // namespace riegeli
